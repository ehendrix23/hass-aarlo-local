"""Inactive direct-SmartHub RTSP transport POC (no cloud code or HA imports).

Use pyaarlo's cached peer/combined/device/ICA certificate files. A live TLS
probe does not send RTSP requests. The relay requires an explicit playback
gate; do not enable until production coexistence is established.
"""

import asyncio
import hashlib
import re
import ssl
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .budget import AdmissionBudget
from .retry import RetryBackoff

HEADER_LIMIT = 65536
BODY_LIMIT = 4 * 1024 * 1024
METHODS = {"OPTIONS", "DESCRIBE", "SETUP", "PLAY", "GET_PARAMETER", "TEARDOWN"}
TIMEOUT = 5.0


@dataclass
class Message:
    start: str
    headers: list[tuple[str, str]]
    body: bytes = b""

    def get(self, name: str) -> str | None:
        matches = [v for k, v in self.headers if k.lower() == name.lower()]
        if len(matches) > 1:
            raise ValueError("Duplicate RTSP header")
        return matches[0] if matches else None

    def set(self, name: str, value: str | int) -> None:
        self.headers = [(k, v) for k, v in self.headers if k.lower() != name.lower()]
        self.headers.append((name, str(value)))

    def encode(self) -> bytes:
        if self.body or self.get("Content-Length") is not None:
            self.set("Content-Length", len(self.body))
        return (
            "\r\n".join([self.start] + [f"{k}: {v}" for k, v in self.headers])
            + "\r\n\r\n"
        ).encode("ascii") + self.body


async def read_unit(reader: asyncio.StreamReader) -> Message | bytes:
    """Read one complete RTSP message or one complete interleaved RTP/RTCP frame."""
    first = await reader.readexactly(1)
    if first == b"$":
        header = await reader.readexactly(3)
        size = int.from_bytes(header[1:], "big")
        return first + header + await reader.readexactly(size)
    raw = first + await reader.readuntil(b"\r\n\r\n")
    if len(raw) > HEADER_LIMIT:
        raise ValueError("RTSP header too large")
    lines = raw[:-4].decode("ascii").split("\r\n")
    headers = []
    for line in lines[1:]:
        name, sep, val = line.partition(":")
        if not sep or not re.fullmatch(r"[A-Za-z0-9!#$%&'*+.^_`|~-]+", name):
            raise ValueError("Invalid RTSP header")
        headers.append((name, val.strip()))
    msg = Message(lines[0], headers)
    length = msg.get("Content-Length")
    if length is not None:
        if not re.fullmatch(r"[0-9]+", length) or int(length) > BODY_LIMIT:
            raise ValueError("Invalid RTSP content length")
        msg.body = await reader.readexactly(int(length))
    return msg


def _rewrite_url(value: str, sources: Sequence[str], target: str) -> str:
    # Only change an exact authority boundary; never match a host prefix.
    for src in sources:
        value = re.sub(
            r"(?i)rtsp://" + re.escape(src) + r"(?=[/\s?]|$)",
            lambda _: "rtsp://" + target,
            value,
        )
    return value


def rewrite_message(msg: Message, sources: Sequence[str], target: str) -> Message:
    msg.start = _rewrite_url(msg.start, sources, target)
    msg.headers = [(k, _rewrite_url(v, sources, target)) for k, v in msg.headers]
    if (msg.get("Content-Type") or "").split(";")[
        0
    ].strip().lower() == "application/sdp":
        msg.body = _rewrite_url(msg.body.decode("ascii"), sources, target).encode(
            "ascii"
        )
    return msg


class Nonce:
    def __init__(self) -> None:
        self.value: int | None = None

    def receive(self, msg: Message) -> None:
        value = msg.get("Nonce")
        if value is not None:
            if not re.fullmatch(r"[0-9]{1,20}", value):
                raise ValueError("Invalid RTSP nonce")
            self.value = int(value)

    def send(self, msg: Message) -> None:
        if self.value is not None:
            self.value += 1
            msg.set("Nonce", self.value)


def response_status(msg: Message) -> str:
    """Validate the response line before inspecting its status."""
    parts = msg.start.split(maxsplit=2)
    if (
        len(parts) < 2
        or parts[0] != "RTSP/1.0"
        or not re.fullmatch(r"[0-9]{3}", parts[1])
    ):
        raise ValueError("Expected RTSP response")
    return parts[1]


def tls_context(
    *, peer_chain: Path, private_key: Path, device_cert: Path, ica_cert: Path
) -> tuple[ssl.SSLContext, bytes]:
    """Trust only the provisioned Arlo identity; no unverified TLS fallback."""
    expected = ssl.PEM_cert_to_DER_cert(Path(device_cert).read_text())
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # Certificate DNS names vary; use exact provisioned leaf pin instead.
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    if not hasattr(ssl, "VERIFY_X509_PARTIAL_CHAIN"):
        raise RuntimeError("Runtime lacks partial-chain trust support")
    context.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cafile=str(ica_cert))
    context.load_verify_locations(cafile=str(device_cert))
    context.load_cert_chain(str(peer_chain), str(private_key))
    return context, hashlib.sha256(expected).digest()


async def connect_hub(
    host: str, port: int, hostname: str, context: ssl.SSLContext, expected_pin: bytes
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(
            host,
            port,
            ssl=context,
            server_hostname=hostname,
            ssl_handshake_timeout=TIMEOUT,
            limit=HEADER_LIMIT,
        ),
        TIMEOUT,
    )
    try:
        ssl_object = writer.get_extra_info("ssl_object")
        if not isinstance(ssl_object, ssl.SSLObject):
            raise ssl.SSLCertVerificationError("SmartHub TLS session missing")
        cert = ssl_object.getpeercert(binary_form=True)
        if not cert or hashlib.sha256(cert).digest() != expected_pin:
            raise ssl.SSLCertVerificationError("SmartHub certificate pin mismatch")
    except BaseException:
        await close_writer(writer)
        raise
    return reader, writer


async def _settle_close[T](future: asyncio.Future[T]) -> T:
    """Wait for owned cleanup even if the caller is cancelled again."""
    cancelled = False
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            cancelled = True
    result = future.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


async def close_writer(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    # Relay/fence teardown may already have initiated close. In CPython
    # a second TLS transport.close() can detach the SSL protocol before
    # the underlying TCP connection has closed, defeating abort().
    if not writer.is_closing():
        writer.close()

    # TLS close() only initiates the close-notify exchange. Cancelling the
    # StreamWriter's close waiter would discard the actual connection-lost
    # acknowledgement while leaving its TCP socket established. Preserve the
    # waiter, abort an unresponsive transport, and settle it before returning.
    async def wait_closed() -> None:
        # A connection-lost error is still a closure acknowledgement.
        with suppress(OSError):
            await writer.wait_closed()

    closed = asyncio.create_task(wait_closed())
    try:
        done, _ = await asyncio.wait({closed}, timeout=1)
        if not done:
            writer.transport.abort()
        await _settle_close(closed)
    except asyncio.CancelledError:
        if not closed.done():
            writer.transport.abort()
            await _settle_close(closed)
        raise


async def probe_tls(
    host: str, port: int, hostname: str, context: ssl.SSLContext, expected_pin: bytes
) -> dict[str, str | bool | int | None]:
    """Authenticate then disconnect without issuing any RTSP command."""
    _, writer = await connect_hub(host, port, hostname, context, expected_pin)
    try:
        obj = writer.get_extra_info("ssl_object")
        if not isinstance(obj, ssl.SSLObject):
            raise ssl.SSLCertVerificationError("SmartHub TLS session missing")
        return {
            "tls_version": obj.version(),
            "peer_pin_verified": True,
            "rtsp_commands_sent": 0,
        }
    finally:
        await close_writer(writer)


class Relay:
    """Single-consumer loopback relay; one fixed hub and camera, no cloud fallback."""

    def __init__(
        self,
        host: str,
        port: int,
        hostname: str,
        camera_id: str,
        context: ssl.SSLContext,
        expected_pin: bytes,
        *,
        playback_authorized: bool = False,
        codec: str = "avc",
        admission: asyncio.BoundedSemaphore | AdmissionBudget | None = None,
        on_ending: Callable[[], None] | None = None,
        on_ended: Callable[[], None] | None = None,
        on_capacity: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if not playback_authorized:
            raise PermissionError("Playback safety gate is closed")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", camera_id) or camera_id in (".", ".."):
            raise ValueError("Invalid camera ID")
        if codec not in ("avc", "hevc"):
            raise ValueError("Unsupported codec")
        self.host, self.port, self.hostname = host, port, hostname
        self.camera_id, self.codec = camera_id, codec
        self.context, self.expected_pin = context, expected_pin
        self.server: asyncio.Server | None = None
        self.local_port: int
        self.active = False
        self.accepting = True
        self.admission = admission
        self.on_ending = on_ending
        self.on_ended = on_ended
        self.on_capacity = on_capacity
        self.retry = RetryBackoff()
        self.upstream_sessions = 0
        self.connected_since: float | None = None
        self.tasks: set[asyncio.Task[None]] = set()
        self.errors: list[str] = []
        self.protocol_trace: list[dict[str, str | bool | int | list[str]]] = []

    def _trace(self, event: dict[str, str | bool | int | list[str]]) -> None:
        self.protocol_trace.append(event)
        del self.protocol_trace[:-40]

    async def start(self) -> str:
        if self.server is not None:
            raise RuntimeError("Relay already started")
        self.server = await asyncio.start_server(
            self._client, "127.0.0.1", 0, limit=HEADER_LIMIT
        )
        address = self.server.sockets[0].getsockname()
        if not isinstance(address, tuple) or not isinstance(address[1], int):
            raise TypeError("Invalid loopback socket address")
        self.local_port = address[1]
        return f"rtsp://127.0.0.1:{self.local_port}/{self.camera_id}/tcp/{self.codec}"

    async def close(self) -> None:
        self.accepting = False
        if self.server:
            self.server.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.server:
            await self.server.wait_closed()
            self.server = None

    def _check_request(self, msg: Message) -> None:
        parts = msg.start.split()
        if len(parts) != 3 or parts[0] not in METHODS or parts[2] != "RTSP/1.0":
            raise ValueError("Unsupported RTSP request")
        if parts[1] == "*" and parts[0] == "OPTIONS":
            return
        url = urlsplit(parts[1])
        if url.scheme != "rtsp" or url.netloc not in self.local_authorities:
            raise ValueError("RTSP target outside this relay")
        path = unquote(url.path)
        if any(segment in (".", "..") for segment in path.split("/")) or "\\" in path:
            raise ValueError("Invalid RTSP path")
        if not (
            url.path == "/" + self.camera_id
            or url.path.startswith("/" + self.camera_id + "/")
        ):
            raise ValueError("RTSP target outside this camera")

    @property
    def local_authorities(self) -> tuple[str, str]:
        return (f"127.0.0.1:{self.local_port}", f"localhost:{self.local_port}")

    async def _client(
        self, local_reader: asyncio.StreamReader, local_writer: asyncio.StreamWriter
    ) -> None:
        if self.active or not self.accepting:
            await close_writer(local_writer)
            return
        self.active = True
        host, port, hostname = self.host, self.port, self.hostname
        context, expected_pin = self.context, self.expected_pin
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        upstream_writer: asyncio.StreamWriter | None = None
        pumps: list[asyncio.Task[None]] = []
        nonce = Nonce()
        admitted = False
        starting = failed = media_seen = False
        clock = asyncio.get_running_loop().time
        try:
            # Do not contact the hub until a validated RTSP request arrives.
            first = await asyncio.wait_for(read_unit(local_reader), TIMEOUT)
            if not isinstance(first, Message):
                raise TypeError("Expected RTSP request")
            self._check_request(first)
            if not self.retry.ready(clock()):
                self._trace({"admission": "retry_cooldown"})
                return
            if self.admission is not None:
                # A demand request may reclaim a warm-only slot. The admission
                # semaphore is still authoritative after the awaited callback.
                if self.admission.locked() and self.on_capacity is not None:
                    await self.on_capacity()
                if not self.accepting:
                    return
                if self.admission.locked():
                    self._trace({"admission": "capacity"})
                    return
                await self.admission.acquire()
                admitted = True
            starting = True
            upstream_reader, upstream_writer = await connect_hub(
                host, port, hostname, context, expected_pin
            )
            self.upstream_sessions += 1
            self.connected_since = asyncio.get_running_loop().time()
            hub_authorities = (hostname + ":" + str(port), hostname)
            # go2rtc begins with DESCRIBE. Arlo requires OPTIONS to establish
            # its per-connection nonce before any subsequent RTSP command.
            # Shift CSeq only for a client that did not initiate OPTIONS.
            offset = 0
            if first.start.split()[0] != "OPTIONS":
                seq = first.get("CSeq")
                if seq is None or not re.fullmatch(r"[0-9]{1,10}", seq):
                    raise ValueError("Invalid initial RTSP CSeq")
                hello = Message(
                    "OPTIONS " + first.start.split()[1] + " RTSP/1.0", [("CSeq", seq)]
                )
                rewrite_message(hello, self.local_authorities, hostname)
                self._trace({"request": "OPTIONS", "bootstrap": True})
                upstream_writer.write(hello.encode())
                await asyncio.wait_for(upstream_writer.drain(), TIMEOUT)
                reply = await asyncio.wait_for(read_unit(upstream_reader), TIMEOUT)
                if isinstance(reply, Message):
                    self._trace(
                        {
                            "bootstrap_reply": response_status(reply),
                            "cseq_matches": reply.get("CSeq") == seq,
                            "nonce": reply.get("Nonce") is not None,
                        }
                    )
                if (
                    not isinstance(reply, Message)
                    or response_status(reply) != "200"
                    or reply.get("CSeq") != seq
                ):
                    raise ValueError("SmartHub OPTIONS bootstrap rejected")
                nonce.receive(reply)
                if nonce.value is None:
                    raise ValueError("SmartHub OPTIONS nonce missing")
                self._trace({"response": "200", "nonce": True, "bootstrap": True})
                offset = 1

            async def send_request(msg: Message) -> None:
                assert upstream_writer is not None
                self._check_request(msg)
                if offset:
                    seq = msg.get("CSeq")
                    if seq is None or not re.fullmatch(r"[0-9]{1,10}", seq):
                        raise ValueError("Invalid RTSP CSeq")
                    msg.set("CSeq", int(seq) + offset)
                nonce.send(msg)
                self._trace(
                    {
                        "request": msg.start.split()[0],
                        "nonce": msg.get("Nonce") is not None,
                        "backchannel": msg.get("Require") is not None,
                    }
                )
                rewrite_message(msg, self.local_authorities, hostname)
                upstream_writer.write(msg.encode())
                await asyncio.wait_for(upstream_writer.drain(), TIMEOUT)

            await send_request(first)
            starting = False

            async def up() -> None:
                assert upstream_writer is not None
                while True:
                    msg = await read_unit(local_reader)
                    if isinstance(msg, Message):
                        await send_request(msg)
                    else:
                        upstream_writer.write(msg)
                        await asyncio.wait_for(upstream_writer.drain(), TIMEOUT)

            async def down() -> None:
                nonlocal failed, media_seen
                while True:
                    try:
                        msg = await read_unit(upstream_reader)
                    except (
                        OSError,
                        ValueError,
                        asyncio.IncompleteReadError,
                        asyncio.LimitOverrunError,
                    ):
                        failed = not media_seen
                        raise
                    if isinstance(msg, Message):
                        status = response_status(msg)
                        if not media_seen and int(status) >= 400:
                            failed = True
                        nonce.receive(msg)
                        self._trace(
                            {
                                "response": status,
                                "nonce": msg.get("Nonce") is not None,
                            }
                        )
                        if (msg.get("Content-Type") or "").split(";")[
                            0
                        ].strip().lower() == "application/sdp":
                            lines = msg.body.decode("ascii").splitlines()
                            self._trace(
                                {
                                    "sdp_media": [
                                        x for x in lines if x.startswith("m=")
                                    ],
                                    "sdp_rtpmap": [
                                        x for x in lines if x.startswith("a=rtpmap:")
                                    ],
                                    "sdp_direction": [
                                        x
                                        for x in lines
                                        if x
                                        in (
                                            "a=sendonly",
                                            "a=recvonly",
                                            "a=sendrecv",
                                            "a=inactive",
                                        )
                                    ],
                                    "sdp_controls": sum(
                                        x.startswith("a=control:") for x in lines
                                    ),
                                }
                            )
                        if offset:
                            seq = msg.get("CSeq")
                            if (
                                seq is None
                                or not re.fullmatch(r"[0-9]{1,11}", seq)
                                or int(seq) < offset
                            ):
                                raise ValueError("Invalid response CSeq")
                            msg.set("CSeq", int(seq) - offset)
                        rewrite_message(msg, hub_authorities, self.local_authorities[0])
                        payload = msg.encode()
                    else:
                        if not media_seen:
                            media_seen = True
                            failed = False
                            self.retry.reset()
                        payload = msg
                    local_writer.write(payload)
                    await asyncio.wait_for(local_writer.drain(), TIMEOUT)

            pumps = [asyncio.create_task(up()), asyncio.create_task(down())]
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for item in done:
                item.result()
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            failed = failed or starting
        except asyncio.CancelledError:
            starting = False
            raise
        except (
            OSError,
            ValueError,
            TypeError,
            RuntimeError,
            asyncio.LimitOverrunError,
        ) as exc:
            failed = failed or starting
            self.errors.append(type(exc).__name__)
            del self.errors[:-20]
        finally:
            if failed:
                delay = self.retry.failed(clock())
                self._trace({"retry_delay_s": int(delay)})
            connected = upstream_writer is not None
            try:
                if connected and self.on_ending is not None:
                    self.on_ending()
                # Start closing both transports before awaiting cleanup.
                # TLS shutdown is not itself proof of socket release.
                if not local_writer.is_closing():
                    local_writer.close()
                if upstream_writer is not None and not upstream_writer.is_closing():
                    upstream_writer.close()
                for pump in pumps:
                    pump.cancel()
                if pumps:
                    await asyncio.gather(*pumps, return_exceptions=True)
                await close_writer(upstream_writer)
                await close_writer(local_writer)
            except BaseException:
                # Cancellation can also interrupt the pump join, a fence
                # callback, or the local close. Retain the admission until
                # both actual transports and the pumps have settled, rather
                # than relying on is_closing() or on logical relay counters.
                writers = [local_writer]
                if upstream_writer is not None:
                    writers.append(upstream_writer)
                for writer in writers:
                    writer.transport.abort()
                for pump in pumps:
                    pump.cancel()
                await _settle_close(
                    asyncio.gather(
                        *pumps,
                        *(writer.wait_closed() for writer in writers),
                        return_exceptions=True,
                    )
                )
                raise
            finally:
                if admitted and self.admission is not None:
                    self.admission.release()
                self.active = False
                self.connected_since = None
                self.tasks.discard(task)
                if connected and self.on_ended is not None:
                    self.on_ended()
