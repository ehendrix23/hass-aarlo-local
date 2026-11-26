"""One private go2rtc distributor per hub; one upstream relay per camera."""

import asyncio
import hashlib
import json
import secrets
import shutil
import socket
import ssl
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path

import aiohttp
import psutil

from .inventory import safe_id
from .process_owner import capture_owner, stop_owned_process
from .rtsp_transport import Relay
from .stream_fence import StreamFence
from .warm import WarmLease

MAX_ACTIVE_CAMERAS = 3
MAX_WARM_CAMERAS = 2
MAX_WARM_SECONDS = 120
ROLLOVER_BASE_SECONDS = 1200
ROLLOVER_STAGGER_SECONDS = 150


class Distributor:
    """Own only our process and loopback relays; never manage HA's go2rtc."""

    def __init__(
        self,
        binary: str,
        host: str,
        hostname: str,
        context: ssl.SSLContext,
        pin: bytes,
        *,
        port: int = 554,
        ffmpeg_binary: str | None = None,
    ) -> None:
        self.binary = binary
        self.ffmpeg_binary = ffmpeg_binary or shutil.which("ffmpeg")
        if not self.ffmpeg_binary:
            raise RuntimeError("FFmpeg is required for Arlo RTSP normalization")
        self.host = host
        self.hostname = hostname
        self.context = context
        self.pin = pin
        self.port = port
        self.process: asyncio.subprocess.Process | None = None
        self._process_owner: psutil.Process | None = None
        self.session: aiohttp.ClientSession | None = None
        self.directory: tempfile.TemporaryDirectory[str] | None = None
        self.relays: dict[str, Relay] = {}
        self.fences: dict[str, StreamFence] = {}
        self.warm_leases: dict[str, WarmLease] = {}
        self._output_ports: dict[str, int] = {}
        self.names: dict[str, str] = {}
        self._drain: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._admission = asyncio.BoundedSemaphore(MAX_ACTIVE_CAMERAS)
        self._closed = False
        self.rtsp_port: int | None = None
        self._prefix = "arlo_" + secrets.token_hex(8)
        self._started = False
        self._log_tail: list[str] = []

    @property
    def healthy(self) -> bool:
        return (
            self.process is not None
            and self.process.returncode is None
            and self._started
        )

    @staticmethod
    def _allocate_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            address = sock.getsockname()
            if not isinstance(address, tuple) or not isinstance(address[1], int):
                raise TypeError("Invalid distributor socket address")
            return address[1]

    async def _request(
        self, method: str, path: str, params: Mapping[str, str] | None = None
    ) -> object:
        session = self.session
        if session is None:
            raise RuntimeError("Distributor HTTP session unavailable")
        async with session.request(
            method, "http://localhost" + path, params=params
        ) as resp:
            resp.raise_for_status()
            return (
                await resp.json(content_type=None)
                if method == "GET"
                else await resp.read()
            )

    def _prepare_directory(self) -> tempfile.TemporaryDirectory[str]:
        """Prepare the process configuration outside the HA event loop."""
        directory = tempfile.TemporaryDirectory(prefix="arlolocal-", dir="/tmp")
        root = Path(directory.name)
        sock = root / "api.sock"
        # JSON is valid YAML; go2rtc accepts this configuration.
        config = root / "go2rtc.yaml"
        config.write_text(
            json.dumps(
                {
                    "app": {"modules": ["api", "rtsp", "exec", "ffmpeg"]},
                    "api": {
                        "listen": "",
                        "unix_listen": str(sock),
                        "allow_paths": ["/api", "/api/streams"],
                    },
                    "rtsp": {
                        "listen": f"127.0.0.1:{self.rtsp_port}",
                        "default_query": "video",
                    },
                    "exec": {"allow_paths": [self.ffmpeg_binary]},
                    "ffmpeg": {
                        "bin": self.ffmpeg_binary,
                        "rtsp": "-allowed_media_types video+audio -rtsp_transport tcp -timeout 5000000 -analyzeduration 100000 -probesize 32768 -threads 2 -i {input}",
                        # Preserve input frame cadence and timestamp precision.
                        # Do not synthesize frames to match a guessed FPS.
                        "h264": "-c:v libx264 -preset:v ultrafast -tune:v zerolatency -g:v 15 -threads:v 2 -pix_fmt:v yuv420p -profile:v baseline -fps_mode:v passthrough -enc_time_base:v demux",
                    },
                    "streams": {},
                }
            )
        )
        config.chmod(0o600)
        return directory

    async def _start_locked(self) -> None:
        if self._closed:
            raise RuntimeError("Distributor closed")
        if self.healthy:
            return
        if self.process or self.session or self.directory:
            await self._stop_locked()
        if self.rtsp_port is None:
            self.rtsp_port = self._allocate_port()
        prepare = asyncio.create_task(asyncio.to_thread(self._prepare_directory))
        try:
            self.directory = await asyncio.shield(prepare)
        except asyncio.CancelledError:
            self.directory = await prepare
            await self._stop_locked()
            raise
        root = Path(self.directory.name)
        sock = root / "api.sock"
        config = root / "go2rtc.yaml"
        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(
                self.binary,
                "-c",
                str(config),
                cwd=root,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        )
        try:
            try:
                self.process = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                self.process = await spawn
                self._process_owner = await asyncio.to_thread(
                    capture_owner, self.process.pid
                )
                raise
            self._process_owner = await asyncio.to_thread(
                capture_owner, self.process.pid
            )
            # HA's shared session helper creates a TCP connector. This local
            # administrative API must remain on our private Unix socket.
            self.session = aiohttp.ClientSession(
                connector=aiohttp.UnixConnector(path=str(sock)),
                timeout=aiohttp.ClientTimeout(total=5),
            )
            assert self.process is not None and self.process.stdout is not None
            # The unique Unix socket and the actual owned process startup log
            # jointly prove that the local RTSP port is ours.
            async with asyncio.timeout(10):
                while True:
                    line = await self.process.stdout.readline()
                    if not line:
                        raise RuntimeError("go2rtc exited during startup")
                    if (
                        b"[rtsp] listen" in line
                        and f"127.0.0.1:{self.rtsp_port}".encode() in line
                    ):
                        break
            for _ in range(30):
                try:
                    info = await self._request("GET", "/api")
                    rtsp = info.get("rtsp") if isinstance(info, dict) else None
                    if (
                        not isinstance(rtsp, dict)
                        or rtsp.get("listen") != f"127.0.0.1:{self.rtsp_port}"
                    ):
                        raise RuntimeError("Unexpected distributor RTSP endpoint")
                    break
                except (TimeoutError, aiohttp.ClientError, OSError):
                    await asyncio.sleep(0.1)
            else:
                raise RuntimeError("go2rtc API not ready")
            self._started = True
            self._drain = asyncio.create_task(self._drain_logs())
        except BaseException:
            await self._stop_locked()
            raise

    async def _drain_logs(self) -> None:
        while self.process:
            assert self.process.stdout is not None
            line = await self.process.stdout.readline()
            if not line:
                break
            # Retain a bounded, locally held tail only. Do not emit raw URLs.
            self._log_tail.append(line.decode(errors="replace"))
            del self._log_tail[:-20]

    async def start(self) -> None:
        async with self._lock:
            await self._start_locked()

    def output_url(self, camera_id: str) -> str:
        if not self.healthy or camera_id not in self.fences:
            raise RuntimeError("Camera distributor unavailable")
        return f"rtsp://127.0.0.1:{self.fences[camera_id].port}/{self.names[camera_id]}?video"

    async def add(self, camera_id: str) -> str:
        camera_id = safe_id(camera_id)
        async with self._lock:
            await self._start_locked()
            if camera_id in self.relays:
                return self.output_url(camera_id)
            name = self.names.setdefault(
                camera_id, self._prefix + "_" + secrets.token_hex(8)
            )
            assert self.rtsp_port is not None
            fence = StreamFence(
                self.rtsp_port,
                name,
                self._output_ports.get(camera_id, 0),
                on_timeline_fault=lambda: self._timeline_fault(camera_id),
            )
            relay = Relay(
                self.host,
                self.port,
                self.hostname,
                camera_id,
                self.context,
                self.pin,
                playback_authorized=True,
                codec="avc",
                admission=self._admission,
                on_ending=lambda: self._invalidate_generation(camera_id, fence),
                on_ended=lambda: self._resume_generation(camera_id, fence),
                on_capacity=lambda: self._reclaim_warm_capacity(camera_id),
            )
            try:
                source = await relay.start()
                # Arlo's SDP is not interoperable with the native go2rtc RTSP
                # producer. FFmpeg demuxes the validated Arlo session. Live
                # stream-copy tests also produced corrupt downstream H264;
                # normalize video once, then distribute the same encoded feed.
                # Negotiate both tracks upstream via our RTSP input template,
                # overriding go2rtc's automatic video-only input selection.
                # Publish video only: copying AAC into the normalized stream
                # caused reproducible concurrent startup failures on the hub.
                await self._request(
                    "PUT",
                    "/api/streams",
                    {
                        "name": name,
                        "src": "ffmpeg:" + source + "#video=h264",
                    },
                )
                await fence.start()
            except BaseException:
                await fence.close()
                await relay.close()
                # An incomplete registration must not disrupt other cameras.
                # The new source URL was never published to a consumer.
                with suppress(Exception):
                    if self.healthy:
                        await self._request("DELETE", "/api/streams", {"src": name})
                self.names.pop(camera_id, None)
                raise
            self.relays[camera_id] = relay
            self.fences[camera_id] = fence
            self._output_ports[camera_id] = fence.port
            return self.output_url(camera_id)

    def _invalidate_generation(self, camera_id: str, fence: StreamFence) -> None:
        fence.invalidate()
        if lease := self.warm_leases.get(camera_id):
            lease.fence.invalidate()

    def _resume_generation(self, camera_id: str, fence: StreamFence) -> None:
        fence.resume()
        if lease := self.warm_leases.get(camera_id):
            lease.fence.resume()

    def _timeline_fault(self, camera_id: str) -> None:
        relay = self.relays.get(camera_id)
        fence = self.fences.get(camera_id)
        if relay is None or fence is None:
            return
        self._invalidate_generation(camera_id, fence)
        # Terminate only this producer epoch. New clients must reconnect;
        # neither a stale packet nor the socket's health authorizes a frame.
        if relay.tasks:
            for task in tuple(relay.tasks):
                task.cancel()
        else:
            self._resume_generation(camera_id, fence)

    def _rollover_idle(self, camera_id: str) -> None:
        relay = self.relays.get(camera_id)
        fence = self.fences.get(camera_id)
        if (
            relay is None
            or fence is None
            or fence.clients
            or relay.connected_since is None
        ):
            return
        stagger = (
            int.from_bytes(hashlib.sha256(camera_id.encode()).digest()[:2], "big")
            % ROLLOVER_STAGGER_SECONDS
        )
        if (
            asyncio.get_running_loop().time() - relay.connected_since
            >= ROLLOVER_BASE_SECONDS + stagger
        ):
            # No await between checking real demand and cancelling the current
            # generation. New viewers either join before this decision or reopen
            # after the fence; never proactively interrupt an established viewer.
            self._invalidate_generation(camera_id, fence)
            for task in tuple(relay.tasks):
                task.cancel()

    async def _reclaim_warm_capacity(self, camera_id: str) -> None:
        """A validated real viewer may reclaim one idle warm lease, never a viewer."""
        async with self._lock:
            demand = self.fences.get(camera_id)
            if demand is None or not demand.clients or not self._admission.locked():
                return
            for key, lease in tuple(self.warm_leases.items()):
                if key == camera_id:
                    continue
                relay = self.relays.get(key)
                fence = self.fences.get(key)
                if relay is None or fence is None or fence.clients or not relay.active:
                    continue
                # Fence before yielding. The warm worker cannot reopen the
                # retired generation, and no real viewer is connected to it.
                self.warm_leases.pop(key)
                lease.fence.invalidate()
                fence.invalidate()
                tasks = tuple(relay.tasks)
                for task in tasks:
                    task.cancel()
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                finally:
                    await lease.close()
                    fence.resume()
                return

    async def prewarm(self, camera_id: str, duration: float = 90) -> bool:
        """Request a finite, renewable warm lease, never a permanent stream."""
        if not 5 <= duration <= MAX_WARM_SECONDS:
            raise ValueError("Warm duration must be between 5 and 120 seconds")
        async with self._lock:
            if (
                not self.healthy
                or camera_id not in self.relays
                or not self.relays[camera_id].accepting
            ):
                return False
            # Retire expired holders. Only two cameras may have a warm lease,
            # leaving the third admission slot available for ordinary demand.
            for key, previous in tuple(self.warm_leases.items()):
                if previous.task is not None and previous.task.done():
                    await previous.close()
                    self.warm_leases.pop(key)
            if lease := self.warm_leases.get(camera_id):
                lease.extend(duration)
                return True
            if len(self.warm_leases) >= MAX_WARM_CAMERAS:
                return False
            if self._admission.locked() and not self.relays[camera_id].active:
                return False
            assert self.rtsp_port is not None
            assert self.ffmpeg_binary is not None
            lease = WarmLease(
                StreamFence(
                    self.rtsp_port,
                    self.names[camera_id],
                    on_timeline_fault=lambda: self._timeline_fault(camera_id),
                ),
                self.ffmpeg_binary,
                duration,
                lambda: self._rollover_idle(camera_id),
            )
            self.warm_leases[camera_id] = lease
            try:
                await lease.start()
            except BaseException:
                self.warm_leases.pop(camera_id, None)
                await lease.close()
                raise
            return True

    async def release_prewarm(self, camera_id: str) -> None:
        async with self._lock:
            await self._release_warm_locked(camera_id)

    async def _release_warm_locked(self, camera_id: str) -> None:
        if lease := self.warm_leases.pop(camera_id, None):
            await lease.close()

    async def remove(self, camera_id: str) -> None:
        async with self._lock:
            await self._release_warm_locked(camera_id)
            relay = self.relays.pop(camera_id, None)
            if relay is None:
                return
            try:
                await self.fences.pop(camera_id).close()
                if self.healthy:
                    await self._request(
                        "DELETE", "/api/streams", {"src": self.names[camera_id]}
                    )
            finally:
                await relay.close()
                self.names.pop(camera_id, None)
                self._output_ports.pop(camera_id, None)

    def pause_if_idle(self) -> bool:
        """Atomically reject new producer admission while rotating credentials."""
        if any(relay.active for relay in self.relays.values()):
            return False
        for relay in self.relays.values():
            relay.accepting = False
        return True

    def resume(self) -> None:
        for relay in self.relays.values():
            relay.accepting = True

    async def stats(self, camera_id: str) -> object:
        async with self._lock:
            if camera_id not in self.relays or not self.healthy:
                return None
            return await self._request(
                "GET", "/api/streams", {"src": self.names[camera_id]}
            )

    async def _stop_locked(self) -> None:
        self._started = False
        process, self.process = self.process, None
        try:
            for camera_id in tuple(self.warm_leases):
                await self._release_warm_locked(camera_id)
            # Stop all downstream consumers first. The shared go2rtc process
            # must not retain an orphaned viewer when its upstreams close.
            for fence in self.fences.values():
                await fence.close()
            self.fences.clear()
            # Prevent any new upstream admissions before shutting down the
            # distributor. A stale PID must never receive a group signal.
            for relay in self.relays.values():
                await relay.close()
            self.relays.clear()
            if process:
                await stop_owned_process(process, self._process_owner)
                self._process_owner = None
        finally:
            # Signal errors must not abandon relay sockets, the private HTTP
            # session or drain tasks. Keep ownership of a still-live process
            # so recovery cannot start a second distributor over it.
            if process and (
                process.returncode is None or self._process_owner is not None
            ):
                self.process = process
            drain = self._drain
            if drain:
                drain.cancel()
                await asyncio.gather(drain, return_exceptions=True)
                self._drain = None
            try:
                for relay in self.relays.values():
                    await relay.close()
                self.relays.clear()
            finally:
                if self.session:
                    await self.session.close()
                    self.session = None
                if self.directory and self.process is None:
                    await asyncio.to_thread(self.directory.cleanup)
                    self.directory = None

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            await self._stop_locked()
