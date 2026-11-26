"""RTP header discontinuity checks for the normalized video boundary."""

import struct
import unittest

from custom_components.arlo_local.rtsp_transport import Message
from custom_components.arlo_local.video_timeline import TimelineDiscontinuity, VideoTimeline


def rtp(timestamp, *, ssrc=123, channel=4, seq=1):
    payload=struct.pack("!BBHII",0x80,96,seq,timestamp,ssrc)+b"\x65"
    return bytes((36,channel))+len(payload).to_bytes(2,"big")+payload


def setup(channels="4-5"):
    return Message("RTSP/1.0 200 OK",[("CSeq","2"),("Transport",f"RTP/AVP/TCP;unicast;interleaved={channels}")])


class TimelineTests(unittest.TestCase):
    def test_wrap_fragment_and_feedback_are_not_faults(self):
        clock=VideoTimeline()
        clock.response(Message("RTSP/1.0 200 OK",[("CSeq","1")]))
        clock.packet(rtp(100))  # No media channel established yet.
        self.assertIsNone(clock.timestamp)
        clock.response(setup())
        clock.response(setup())
        for ts,seq in ((0xfffffff0,65535),(0xfffffff0,0),(0x00000010,1),(0x1000,2)):
            clock.packet(rtp(ts,seq=seq))
        clock.packet(rtp(1,channel=5,ssrc=456))  # RTCP is not RTP.
        self.assertEqual(clock.timestamp,0x1000)
        with self.assertRaisesRegex(TimelineDiscontinuity,"backward"):
            clock.packet(rtp(1))

    def test_changed_source_and_channel_and_malformed_packets(self):
        clock=VideoTimeline()
        for channels in ("256-257","5-4","invalid"):
            with self.assertRaises(ValueError):clock.response(setup(channels))
        clock.response(setup())
        with self.assertRaises(TimelineDiscontinuity):clock.response(setup("0-1"))
        with self.assertRaises(TimelineDiscontinuity):clock.packet(b"$\x04\x00\x01x")
        clock.packet(rtp(1))
        with self.assertRaisesRegex(TimelineDiscontinuity,"source changed"):
            clock.packet(rtp(2,ssrc=456))
