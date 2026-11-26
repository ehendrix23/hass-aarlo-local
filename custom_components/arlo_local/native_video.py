"""Loopback-only native HA media transport through the normalized RTSP fence.

HTTP avoids HA's fixed RTSP socket deadline, including on cached-worker retry.
Each reader stream-copies already normalized video; it never encodes video,
opens an additional SmartHub session, stores media or exposes an admin API.
"""

import asyncio
from contextlib import suppress

from aiohttp import web

from .const import COLD_START_TIMEOUT_US

FIRST_MEDIA_SECONDS = 12.0
MEDIA_IDLE_SECONDS = 3.0
MAX_READERS = 4


async def _settle[T](task: asyncio.Task[T]) -> tuple[T, bool]:
    """Retain ownership through repeated cancellation of a subprocess operation."""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    return task.result(), cancelled


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    async def drain() -> None:
        assert process.stdout is not None
        while await process.stdout.read(65536):
            pass

    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.terminate()
    # Waiting without draining a PIPE can deadlock when a client leaves
    # while the remuxer is blocked on a full stdout buffer.
    try:
        await asyncio.wait_for(asyncio.gather(drain(), process.wait()), 2)
    except TimeoutError:
        with suppress(ProcessLookupError):
            process.kill()
        await asyncio.gather(drain(), process.wait())


class NativeVideo:
    def __init__(self, binary: str) -> None:
        self.binary = binary
        self.port = 0
        self.runner: web.AppRunner | None = None
        self.sources: dict[str, str] = {}
        self.blocked: set[str] = set()
        self.clients: dict[
            str, dict[asyncio.Task[web.StreamResponse], asyncio.Transport]
        ] = {}

    async def start(self) -> None:
        if self.runner is not None:
            return
        app = web.Application()
        app.router.add_get("/{name}/video.ts", self._media, allow_head=False)
        runner = web.AppRunner(
            app, handler_cancellation=True, access_log=None, shutdown_timeout=1
        )
        self.runner = runner
        try:
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", self.port).start()
            self.port = int(runner.addresses[0][1])
        except BaseException:
            await self.close()
            raise

    def register(self, name: str, source: str) -> None:
        self.sources[name] = source
        self.blocked.discard(name)

    def url(self, name: str) -> str:
        if self.runner is None or name not in self.sources:
            raise RuntimeError("Native video source unavailable")
        return f"http://127.0.0.1:{self.port}/{name}/video.ts"

    def has_clients(self, name: str) -> bool:
        return bool(self.clients.get(name))

    def invalidate(self, name: str) -> None:
        """End the old media generation synchronously before relay release."""
        if name not in self.sources:
            return
        self.blocked.add(name)
        for task, transport in tuple(self.clients.get(name, {}).items()):
            transport.abort()
            if not task.cancelling():
                task.cancel()

    def resume(self, name: str) -> None:
        if name in self.sources:
            self.blocked.discard(name)

    async def remove(self, name: str) -> None:
        self.invalidate(name)
        self.sources.pop(name, None)
        await asyncio.gather(*tuple(self.clients.get(name, {})), return_exceptions=True)
        self.blocked.discard(name)

    async def close(self) -> None:
        runner, self.runner = self.runner, None
        for name in tuple(self.sources):
            await self.remove(name)
        if runner is not None:
            await runner.cleanup()

    async def _media(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        source = self.sources.get(name)
        if source is None or request.query_string or request.can_read_body:
            return web.Response(status=404)
        if (
            self.runner is None
            or name in self.blocked
            or len(self.clients.get(name, {})) >= MAX_READERS
        ):
            return web.Response(status=503)
        transport = request.transport
        task = asyncio.current_task()
        assert task is not None
        if transport is None:
            return web.Response(status=503)
        self.clients.setdefault(name, {})[task] = transport
        process: asyncio.subprocess.Process | None = None
        response: web.StreamResponse | None = None
        try:
            spawn = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    self.binary,
                    "-nostdin",
                    "-v",
                    "error",
                    "-rtsp_transport",
                    "tcp",
                    "-timeout",
                    str(COLD_START_TIMEOUT_US),
                    "-analyzeduration",
                    "100000",
                    "-probesize",
                    "32768",
                    "-i",
                    source,
                    "-map",
                    "0:v:0",
                    "-an",
                    "-c:v",
                    "copy",
                    "-mpegts_flags",
                    "+resend_headers",
                    "-muxdelay",
                    "0",
                    "-muxpreload",
                    "0",
                    "-flush_packets",
                    "1",
                    "-f",
                    "mpegts",
                    "pipe:1",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
            )
            process, cancelled = await _settle(spawn)
            if cancelled:
                raise asyncio.CancelledError
            assert process is not None and process.stdout is not None
            first = await asyncio.wait_for(
                process.stdout.read(65536), FIRST_MEDIA_SECONDS
            )
            if not first:
                return web.Response(status=503)
            response = web.StreamResponse(
                headers={"Content-Type": "video/mp2t", "Cache-Control": "no-store"}
            )
            response.force_close()
            await asyncio.wait_for(response.prepare(request), MEDIA_IDLE_SECONDS)
            await asyncio.wait_for(response.write(first), MEDIA_IDLE_SECONDS)
            while chunk := await asyncio.wait_for(
                process.stdout.read(65536), MEDIA_IDLE_SECONDS
            ):
                await asyncio.wait_for(response.write(chunk), MEDIA_IDLE_SECONDS)
            return response
        except (OSError, TimeoutError):
            if response is None:
                return web.Response(status=503)
            transport.abort()
            return response
        finally:
            try:
                if process is not None:
                    _, cancelled = await _settle(
                        asyncio.create_task(_stop_process(process))
                    )
                    if cancelled:
                        raise asyncio.CancelledError
            finally:
                clients = self.clients[name]
                clients.pop(task, None)
                if not clients:
                    self.clients.pop(name)
