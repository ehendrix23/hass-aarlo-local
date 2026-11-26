"""Check the normalized, video-only RTP timeline within a decoder session."""

import re
import struct

from .rtsp_transport import Message


class TimelineDiscontinuity(ValueError):
    """An established normalized video timeline can no longer be trusted."""


class VideoTimeline:
    """Observe negotiated RTP (not RTCP) after H.264 baseline normalization.

    Equal timestamps are normal for fragmented access units. The unsigned
    modular delta permits 32-bit RTP timestamp wrap, not backward motion.
    The encoder has no B-frames; do not apply this rule to raw camera media.
    """

    def __init__(self) -> None:
        self.channel: int | None = None
        self.ssrc: int | None = None
        self.timestamp: int | None = None

    def response(self, message: Message) -> None:
        transport = message.get("Transport")
        if not transport:
            return
        # The private go2rtc publishes only normalized H.264 over TCP.
        # Use the server's negotiated channel rather than assuming channel 0.
        match = re.search(
            r"(?:^|;)\s*interleaved=(\d{1,3})-(\d{1,3})(?:;|$)", transport, re.I
        )
        if not match or not 0 <= int(match[1]) < int(match[2]) <= 255:
            raise ValueError("Unsupported video transport")
        channel = int(match[1])
        if self.channel is not None and self.channel != channel:
            raise TimelineDiscontinuity("Video channel changed")
        self.channel = channel

    def packet(self, unit: bytes) -> None:
        if self.channel is None or unit[1] != self.channel:
            return
        if len(unit) < 16 or unit[4] >> 6 != 2:
            raise TimelineDiscontinuity("Malformed video RTP")
        timestamp, ssrc = struct.unpack_from("!II", unit, 8)
        if self.ssrc is not None and self.ssrc != ssrc:
            raise TimelineDiscontinuity("Video source changed")
        if (
            self.timestamp is not None
            and (timestamp - self.timestamp) & 0xFFFFFFFF >= 0x80000000
        ):
            raise TimelineDiscontinuity("Video timestamp moved backward")
        self.ssrc, self.timestamp = ssrc, timestamp
