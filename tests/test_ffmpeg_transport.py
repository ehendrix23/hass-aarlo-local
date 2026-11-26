"""Synthetic H.264 decode through the POC's real TLS/RTSP relay.
Run explicitly with an isolated FFmpeg binary path in ARLO_POC_FFMPEG.
No camera access, no credentials from Arlo, no cloud calls.
"""
import asyncio
import base64
import os
import re
import struct
import unittest

from custom_components.arlo_local.rtsp_transport import Message, read_unit, close_writer
from test_rtsp_transport import TLSTests


class DecoderTest(TLSTests):
    async def mock_hub(self, reader, writer):
        self.connections += 1
        self.finished.clear()
        ssrc = 0x12345678
        seq = 1000
        session = "synthetic-session"
        async def send_video():
            nonlocal seq
            await asyncio.sleep(getattr(self,"video_start_delay",0))
            for frame_no in range(getattr(self,"frame_count",12)):
                # Each generated access unit is a single IDR plus repeated SPS/PPS.
                for nal_no, nal in enumerate(self.nals):
                    if not nal:
                        continue
                    parts = []
                    if len(nal) <= 1200:
                        parts = [nal]
                    else:
                        indicator = (nal[0] & 0xE0) | 28
                        typ = nal[0] & 0x1F
                        chunks = [nal[i:i+1198] for i in range(1,len(nal),1198)]
                        for i, chunk in enumerate(chunks):
                            flags = (0x80 if i==0 else 0) | (0x40 if i==len(chunks)-1 else 0)
                            parts.append(bytes((indicator, flags|typ))+chunk)
                    for i, part in enumerate(parts):
                        marker = nal_no == len(self.nals)-1 and i == len(parts)-1
                        timestamp = self.video_timestamp(frame_no) if hasattr(self, "video_timestamp") else frame_no*18000
                        packet = struct.pack("!BBHII", 0x80, 96|(0x80 if marker else 0),
                                             seq & 0xFFFF, timestamp, ssrc)+part
                        writer.write(b"$\x00"+len(packet).to_bytes(2,"big")+packet)
                        seq += 1
                await writer.drain()
                self.video_sent = getattr(self,"video_sent",0)+1
                await asyncio.sleep(getattr(self,"video_interval",0.08))
        video_task = None
        try:
            while True:
                m = await read_unit(reader)
                self.requests.append(m)
                if not isinstance(m, Message): continue
                method = m.start.split()[0]
                reply = Message("RTSP/1.0 200 OK", [("CSeq",m.get("CSeq"))])
                if method == "OPTIONS":
                    reply.set("Public","OPTIONS, DESCRIBE, SETUP, PLAY, GET_PARAMETER, TEARDOWN")
                    reply.set("Nonce","100")
                elif method == "DESCRIBE":
                    reply.set("Content-Type","application/sdp")
                    reply.set("Content-Base",m.start.split()[1].rstrip("/")+"/")
                    reply.body = ("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=POC\r\n"
                                  "c=IN IP4 127.0.0.1\r\nt=0 0\r\na=control:*\r\n"
                                  "m=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n"
                                  "a=fmtp:96 packetization-mode=1; sprop-parameter-sets="
                                  + self.sprop + "\r\na=control:trackID=0\r\n").encode("ascii")
                elif method == "SETUP":
                    reply.set("Session",session+";timeout=60")
                    reply.set("Transport","RTP/AVP/TCP;unicast;interleaved=0-1;ssrc=12345678")
                elif method == "PLAY":
                    reply.set("Session",session)
                    reply.set("RTP-Info","url="+m.start.split()[1].rstrip("/")+"/trackID=0;seq=1000;rtptime=0")
                elif method == "TEARDOWN":
                    reply.set("Session",session)
                writer.write(reply.encode())
                await writer.drain()
                if method == "PLAY":
                    video_task = asyncio.create_task(send_video())
                if method == "TEARDOWN": break
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            if video_task:
                video_task.cancel()
                await asyncio.gather(video_task,return_exceptions=True)
            await close_writer(writer)
            self.finished.set()

    async def test_ffmpeg_real_decode(self):
        ffmpeg = os.environ["ARLO_POC_FFMPEG"]
        proc = await asyncio.create_subprocess_exec(ffmpeg, "-v","error",
            "-f","lavfi","-i","color=c=black:s=160x120:r=5","-frames:v","1",
            "-c:v","libx264","-preset","ultrafast","-tune","zerolatency",
            "-x264-params","keyint=1:repeat-headers=1","-f","h264","pipe:1",
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
        raw, err = await asyncio.wait_for(proc.communicate(),10)
        self.assertEqual(proc.returncode,0,err.decode())
        self.nals = [n for n in re.split(b"\\x00\\x00(?:\\x00)?\\x01",raw) if n]
        sps = next(n for n in self.nals if n[0]&31==7)
        pps = next(n for n in self.nals if n[0]&31==8)
        self.sprop = ",".join(base64.b64encode(n).decode() for n in (sps,pps))
        r = self.relay(playback_authorized=True)
        url = await r.start()
        proc = await asyncio.create_subprocess_exec(ffmpeg,"-v","error","-rtsp_transport","tcp",
            "-analyzeduration","100000","-probesize","32768","-i",url,
            "-frames:v","1","-an","-pix_fmt","rgb24","-f","rawvideo","pipe:1",
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
        try:
            decoded, err = await asyncio.wait_for(proc.communicate(),15)
        except asyncio.TimeoutError:
            proc.kill();await proc.communicate();raise
        self.assertEqual(proc.returncode,0,err.decode())
        self.assertEqual(len(decoded),160*120*3)
        self.assertLess(max(decoded),16)
        self.assertEqual(r.errors,[])
        methods = [m.start.split()[0] for m in self.requests if isinstance(m,Message)]
        for method in ("OPTIONS","DESCRIBE","SETUP","PLAY"):
            self.assertIn(method,methods)
        print("Decoded synthetic RGB frame:",len(decoded),"bytes; RTSP methods:",methods)


if __name__ == "__main__":
    unittest.main(defaultTest="DecoderTest.test_ffmpeg_real_decode",verbosity=2)
