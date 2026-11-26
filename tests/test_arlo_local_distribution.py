"""Actual shared go2rtc transport test; generated video only."""
import asyncio
import base64
import json
import os
import re
import unittest
from pathlib import Path
from unittest.mock import patch

from test_ffmpeg_transport import DecoderTest
from custom_components.arlo_local.distribution import Distributor


class SharedStreamTests(DecoderTest):
    # Unlike the direct one-frame test, the encoder must keep receiving
    # frames through initial probing and two downstream joins.
    frame_count=150
    async def _video_fixture(self):
        ffmpeg=os.environ["ARLO_POC_FFMPEG"]
        proc=await asyncio.create_subprocess_exec(ffmpeg,"-v","error","-f","lavfi",
                "-i","color=c=black:s=160x120:r=5","-frames:v","1","-c:v","libx264",
                "-preset","ultrafast","-tune","zerolatency","-x264-params",
                "keyint=1:repeat-headers=1","-f","h264","pipe:1",
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
        raw,err=await asyncio.wait_for(proc.communicate(),10)
        self.assertEqual(proc.returncode,0,err.decode())
        self.nals=[n for n in re.split(b"\\x00\\x00(?:\\x00)?\\x01",raw) if n]
        self.sprop=",".join(base64.b64encode(next(n for n in self.nals if n[0]&31==t)).decode() for t in (7,8))
        return ffmpeg

    async def test_two_consumers_one_hub_connection(self):
        ffmpeg = await self._video_fixture()
        broker=Distributor(os.environ["ARLO_POC_GO2RTC"],"127.0.0.1","hub.local",
                           self.context,self.pin,port=self.port,ffmpeg_binary=ffmpeg)
        self.addAsyncCleanup(broker.close)
        await broker.start()
        url=await broker.add("cam")
        self.assertEqual(await broker.add("cam"),url)
        self.assertEqual(self.connections,0)
        # The input must override go2rtc's video-only track selection, while
        # publication must contain video only (no copied upstream AAC track).
        # go2rtc appends its stream YAML to the original JSON-compatible config.
        config,_=json.JSONDecoder().raw_decode((Path(broker.directory.name)/"go2rtc.yaml").read_text())
        self.assertIn("-allowed_media_types video+audio ",config["ffmpeg"]["rtsp"])
        stream=await broker.stats("cam")
        self.assertEqual(len(stream["producers"]),1)
        self.assertTrue(stream["producers"][0]["url"].endswith("#video=h264"))
        self.assertNotIn("#audio=",stream["producers"][0]["url"])
        async def view():
            p=await asyncio.create_subprocess_exec(ffmpeg,"-nostdin","-v","error",
                "-rtsp_transport","tcp","-analyzeduration","100000","-probesize","32768",
                "-threads","2","-i",url,"-frames:v","3","-an","-pix_fmt","rgb24",
                "-fps_mode","passthrough","-f","rawvideo","pipe:1",
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            try:
                data,err=await asyncio.wait_for(p.communicate(),12)
                self.assertEqual(p.returncode,0,err.decode())
                self.assertEqual(len(data),3*160*120*3)
            finally:
                if p.returncode is None:p.kill();await p.communicate()
        await asyncio.gather(view(),view())
        self.assertEqual(self.connections,1)
        methods=[m.start.split()[0] for m in self.requests if hasattr(m,"start")]
        self.assertEqual(methods[:2],["OPTIONS","DESCRIBE"])
        self.assertEqual(self.requests[1].get("Nonce"),"101")
        self.assertIsNone(self.requests[1].get("Require"))
        self.assertEqual(int(self.requests[1].get("CSeq")),int(self.requests[0].get("CSeq"))+1)
        await asyncio.wait_for(self.finished.wait(),5)
        await broker.close()
        self.assertFalse(broker.relays)
        self.assertIsNone(broker.process)

    async def test_repeated_upstream_loss_fences_actual_decoder(self):
        ffmpeg = await self._video_fixture()
        broker = Distributor(os.environ["ARLO_POC_GO2RTC"], "127.0.0.1", "hub.local",
                             self.context, self.pin, port=self.port, ffmpeg_binary=ffmpeg)
        self.addAsyncCleanup(broker.close)
        url = await broker.add("cam")
        relay = broker.relays["cam"]
        for generation in range(1, 4):
            proc = await asyncio.create_subprocess_exec(
                ffmpeg, "-nostdin", "-v", "error", "-rtsp_transport", "tcp", "-timeout", "5000000",
                "-analyzeduration", "100000", "-probesize", "32768", "-i", url,
                "-an", "-pix_fmt", "rgb24", "-fps_mode", "passthrough", "-f", "rawvideo", "pipe:1",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            try:
                self.assertEqual(len(await asyncio.wait_for(proc.stdout.readexactly(160*120*3*3), 12)), 160*120*3*3)
                self.assertEqual(relay.upstream_sessions, generation)
                self.assertEqual(len(relay.tasks), 1)
                task = next(iter(relay.tasks))
                task.cancel()
                await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 4)
                await asyncio.wait_for(proc.communicate(), 5)
                self.assertIsNotNone(proc.returncode)
                self.assertFalse(relay.active)
                self.assertFalse(broker.fences["cam"].clients)
                self.assertEqual(broker.output_url("cam"), url)
            finally:
                if proc.returncode is None:
                    proc.kill()
                    await proc.communicate()

    async def test_warm_lease_rollover_and_release(self):
        ffmpeg = await self._video_fixture()
        broker = Distributor(os.environ["ARLO_POC_GO2RTC"], "127.0.0.1", "hub.local",
                             self.context, self.pin, port=self.port, ffmpeg_binary=ffmpeg)
        self.addAsyncCleanup(broker.close)
        url = await broker.add("cam")
        relay = broker.relays["cam"]
        async def wait_for(check, timeout=10):
            async with asyncio.timeout(timeout):
                while not check():
                    await asyncio.sleep(.05)
        # Accelerate the same idle-only rollover path used at 20–22.5 min.
        with patch("custom_components.arlo_local.distribution.ROLLOVER_BASE_SECONDS", 2), patch("custom_components.arlo_local.distribution.ROLLOVER_STAGGER_SECONDS", 1):
            self.assertTrue(await broker.prewarm("cam", 15))
            lease = broker.warm_leases["cam"]
            await wait_for(lambda: relay.upstream_sessions >= 2)
        self.assertTrue(await broker.prewarm("cam", 15))
        self.assertIs(broker.warm_leases["cam"], lease)
        proc = await asyncio.create_subprocess_exec(
            ffmpeg,"-nostdin","-v","error","-rtsp_transport","tcp","-timeout","5000000",
            "-analyzeduration","100000","-probesize","32768","-i",url,
            "-an","-pix_fmt","rgb24","-fps_mode","passthrough","-f","rawvideo","pipe:1",
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
        try:
            await asyncio.wait_for(proc.stdout.readexactly(160*120*3), 10)
            generation = relay.upstream_sessions
            with patch("custom_components.arlo_local.distribution.ROLLOVER_BASE_SECONDS", 0), patch("custom_components.arlo_local.distribution.ROLLOVER_STAGGER_SECONDS", 1):
                broker._rollover_idle("cam")
                await asyncio.wait_for(proc.stdout.readexactly(160*120*3*3), 5)
                self.assertEqual(relay.upstream_sessions, generation)
            await broker.release_prewarm("cam")
            self.assertNotIn("cam", broker.warm_leases)
            self.assertTrue(lease.task.done())
            self.assertIsNone(lease.fence.server)
            await asyncio.wait_for(proc.stdout.readexactly(160*120*3*3), 5)
            self.assertEqual(relay.upstream_sessions, generation)
        finally:
            if proc.returncode is None:
                proc.kill()
            await proc.communicate()
        await wait_for(lambda: not relay.active)
        self.assertTrue(await broker.prewarm("cam", 5))
        expiring = broker.warm_leases["cam"]
        await asyncio.wait_for(asyncio.shield(expiring.task), 9)
        self.assertIsNone(expiring.fence.server)
        await wait_for(lambda: not relay.active)
        self.assertEqual(broker.output_url("cam"), url)
