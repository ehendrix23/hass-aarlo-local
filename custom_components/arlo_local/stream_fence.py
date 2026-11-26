"""Per-camera RTSP consumer lifetime boundary around the shared distributor.

go2rtc can reconnect its producer without ending consumers. End the affected
consumer TCP sessions before the upstream relay is released so their decoders
must establish a new timestamp history. The listener and URL stay stable.
"""

import asyncio
from collections.abc import Callable
from urllib.parse import urlsplit

from .rtsp_transport import (
    HEADER_LIMIT,
    METHODS,
    TIMEOUT,
    Message,
    close_writer,
    read_unit,
)
from .video_timeline import TimelineDiscontinuity, VideoTimeline


class StreamFence:
    def __init__(
        self,
        backend_port: int,
        stream_name: str,
        port: int = 0,
        *,
        on_timeline_fault: Callable[[], None] | None = None,
    ) -> None:
        self.backend_port = backend_port
        self.stream_name = stream_name
        self.port = port
        self.server: asyncio.Server | None = None
        self.blocked = False
        self.on_timeline_fault = on_timeline_fault
        self.timeline_faults = 0
        self.clients: dict[asyncio.Task[None], list[asyncio.StreamWriter]] = {}

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._client, "127.0.0.1", self.port, limit=HEADER_LIMIT
        )
        address = self.server.sockets[0].getsockname()
        self.port = int(address[1])

    def invalidate(self) -> None:
        """Fence the prior generation synchronously, before upstream cleanup."""
        self.blocked = True
        for task, writers in tuple(self.clients.items()):
            for writer in writers:
                writer.transport.abort()
            task.cancel()

    def resume(self) -> None:
        if self.server is not None and self.server.is_serving():
            self.blocked = False

    async def close(self) -> None:
        server = self.server
        if server is not None:
            server.close()
        self.server = None
        self.invalidate()
        tasks = tuple(self.clients)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if server is not None:
            await server.wait_closed()

    def _check(self, msg: Message, *, first: bool = False) -> None:
        parts = msg.start.split()
        if len(parts) != 3 or parts[0] not in METHODS or parts[2] != "RTSP/1.0":
            raise ValueError("Invalid consumer request")
        if parts[0] == "OPTIONS" and parts[1] == "*" and not first:
            return
        url = urlsplit(parts[1])
        root = "/" + self.stream_name
        if (
            url.scheme != "rtsp"
            or url.netloc not in (f"127.0.0.1:{self.port}", f"localhost:{self.port}")
            or not (url.path == root or url.path.startswith(root + "/trackID="))
            or url.fragment
        ):
            raise ValueError("Consumer target outside this camera")
        if first and (
            parts[0] not in ("OPTIONS", "DESCRIBE")
            or url.path != root
            or url.query not in ("", "video")
        ):
            raise ValueError("Invalid initial consumer request")

    async def _client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self.blocked or self.server is None:
            await close_writer(writer)
            return
        task = asyncio.current_task()
        assert task is not None
        writers = [writer]
        self.clients[task] = writers
        pumps: list[asyncio.Task[None]] = []
        try:
            first = await asyncio.wait_for(read_unit(reader), TIMEOUT)
            if not isinstance(first, Message):
                raise ValueError("Expected RTSP request")
            # go2rtc records the first request URL as its stream identity.
            if first.start == "OPTIONS * RTSP/1.0":
                seq = first.get("CSeq")
                if seq is None:
                    raise ValueError("Missing CSeq")
                writer.write(
                    Message(
                        "RTSP/1.0 200 OK",
                        [("CSeq", seq), ("Public", ", ".join(sorted(METHODS)))],
                    ).encode()
                )
                await asyncio.wait_for(writer.drain(), TIMEOUT)
                first = await asyncio.wait_for(read_unit(reader), TIMEOUT)
                if not isinstance(first, Message):
                    raise ValueError("Expected RTSP request")
            self._check(first, first=True)
            backend_reader, backend_writer = await asyncio.wait_for(
                asyncio.open_connection(
                    "127.0.0.1", self.backend_port, limit=HEADER_LIMIT
                ),
                TIMEOUT,
            )
            writers.append(backend_writer)
            backend_writer.write(first.encode())
            await asyncio.wait_for(backend_writer.drain(), TIMEOUT)

            async def upstream() -> None:
                while True:
                    unit = await read_unit(reader)
                    if isinstance(unit, Message):
                        self._check(unit)
                        payload = unit.encode()
                    else:
                        payload = unit
                    backend_writer.write(payload)
                    await asyncio.wait_for(backend_writer.drain(), TIMEOUT)

            async def downstream() -> None:
                timeline = VideoTimeline()
                # Inspect only the RTP headers at the normalized video boundary.
                # No frame decoding, retention or unbounded queue.
                while True:
                    unit = await read_unit(backend_reader)
                    if isinstance(unit, Message):
                        timeline.response(unit)
                        payload = unit.encode()
                    else:
                        timeline.packet(unit)
                        payload = unit
                    writer.write(payload)
                    await asyncio.wait_for(writer.drain(), TIMEOUT)

            pumps = [asyncio.create_task(upstream()), asyncio.create_task(downstream())]
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for item in done:
                item.result()
        except TimelineDiscontinuity:
            self.timeline_faults += 1
            if self.on_timeline_fault is not None:
                self.on_timeline_fault()
            else:
                self.invalidate()
        except (
            OSError,
            ValueError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ):
            pass
        finally:
            # Close both transports before awaiting anything, including when
            # the task is cancelled again during teardown.
            for endpoint in writers:
                endpoint.close()
            try:
                for pump in pumps:
                    pump.cancel()
                if pumps:
                    await asyncio.gather(*pumps, return_exceptions=True)
                await asyncio.gather(*(close_writer(endpoint) for endpoint in writers))
            finally:
                self.clients.pop(task, None)
