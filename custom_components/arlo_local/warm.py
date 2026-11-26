"""Bounded, explicitly requested, video-only warm consumers."""

import asyncio
from collections.abc import Callable
from contextlib import suppress

from .const import COLD_START_TIMEOUT_US
from .retry import RetryBackoff
from .stream_fence import StreamFence

BACKOFF_RESET_SECONDS = 10


class WarmLease:
    """Own a stream-copy consumer; no extra encoder or retained media."""

    def __init__(
        self,
        fence: StreamFence,
        binary: str,
        duration: float,
        rollover: Callable[[], None],
    ) -> None:
        self.fence = fence
        self.binary = binary
        self.deadline = asyncio.get_running_loop().time() + duration
        self.rollover = rollover
        self.task: asyncio.Task[None] | None = None

    def extend(self, duration: float) -> None:
        self.deadline = asyncio.get_running_loop().time() + duration

    async def start(self) -> None:
        await self.fence.start()
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        clock = asyncio.get_running_loop().time
        retry = RetryBackoff()
        try:
            while clock() < self.deadline:
                started = clock()
                proc: asyncio.subprocess.Process | None = None
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
                            f"rtsp://127.0.0.1:{self.fence.port}/{self.fence.stream_name}?video",
                            "-map",
                            "0:v:0",
                            "-an",
                            "-c:v",
                            "copy",
                            "-f",
                            "null",
                            "-",
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                    )
                    try:
                        proc = await asyncio.shield(spawn)
                    except asyncio.CancelledError:
                        proc = await spawn
                        raise
                    while proc.returncode is None and clock() < self.deadline:
                        self.rollover()
                        try:
                            await asyncio.wait_for(
                                proc.wait(),
                                min(1.0, max(0.01, self.deadline - clock())),
                            )
                        except TimeoutError:
                            pass
                except OSError:
                    # A failed process launch must not create an unbounded retry loop.
                    pass
                finally:
                    if proc is not None and proc.returncode is None:
                        with suppress(ProcessLookupError):
                            proc.terminate()
                        try:
                            await asyncio.wait_for(proc.wait(), 2)
                        except TimeoutError:
                            with suppress(ProcessLookupError):
                                proc.kill()
                            await proc.wait()
                if clock() < self.deadline:
                    if clock() - started >= BACKOFF_RESET_SECONDS:
                        retry.reset()
                    delay = retry.failed(clock())
                    await asyncio.sleep(min(delay, max(0.0, self.deadline - clock())))
        finally:
            await self.fence.close()

    async def close(self) -> None:
        if self.task is not None and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.fence.close()
