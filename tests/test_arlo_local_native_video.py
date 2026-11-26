"""Loopback HTTP media ownership, cancellation and bounded resource tests."""
import asyncio
import socket
import unittest
from unittest.mock import AsyncMock, Mock, patch

import aiohttp

from custom_components.arlo_local import native_video as mod


class FakeProcess:
    def __init__(self, initial=b"", *, eof=False, stubborn=False):
        self.stdout=asyncio.StreamReader()
        self.returncode=None
        self.done=asyncio.Event()
        self.stubborn=stubborn
        self.terminated=self.killed=False
        self.stdout.feed_data(initial)
        if eof:self.finish(0)

    def finish(self, code):
        self.returncode=code
        self.stdout.feed_eof()
        self.done.set()

    def terminate(self):
        self.terminated=True
        if not self.stubborn:self.finish(-15)

    def kill(self):
        self.killed=True
        self.finish(-9)

    async def wait(self):
        await self.done.wait()
        return self.returncode


class NativeVideoTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.media=mod.NativeVideo("ffmpeg")
        await self.media.start()
        self.addAsyncCleanup(self.media.close)
        self.media.register("one","rtsp://127.0.0.1:10000/one?video")
        self.media.register("two","rtsp://127.0.0.1:10001/two?video")
        self.session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        self.addAsyncCleanup(self.session.close)

    async def test_restricted_routes_and_rebind(self):
        url=self.media.url("one")
        with self.assertRaises(RuntimeError):self.media.url("absent")
        with patch.object(mod.asyncio,"create_subprocess_exec",new_callable=AsyncMock) as spawn:
            for target in (url+"?src=other",url.replace("/one/","/absent/")):
                async with self.session.get(target) as response:self.assertEqual(response.status,404)
            async with self.session.get(url,data=b"bad") as response:self.assertEqual(response.status,404)
            async with self.session.post(url) as response:self.assertEqual(response.status,405)
            self.media.invalidate("one")
            self.media.invalidate("absent")
            async with self.session.get(url) as response:self.assertEqual(response.status,503)
            spawn.assert_not_awaited()
        self.media.resume("absent")
        self.media.resume("one")
        await self.media.start()
        self.assertEqual(self.media.url("one"),url)
        await self.media.close()
        with self.assertRaises(RuntimeError):self.media.url("one")
        await self.media.start()
        self.media.register("one","rtsp://127.0.0.1:10000/one?video")
        self.assertEqual(self.media.url("one"),url)

    async def test_reader_limit_and_generation_isolation(self):
        first,second,new=(FakeProcess(b"one"),FakeProcess(b"two"),FakeProcess(b"new"))
        with patch.object(mod,"MAX_READERS",1),patch.object(mod.asyncio,"create_subprocess_exec",new_callable=AsyncMock,side_effect=[first,second,new]) as spawn:
            r1=await self.session.get(self.media.url("one"))
            r2=await self.session.get(self.media.url("two"))
            self.assertEqual(r1.status,200)
            self.assertEqual(r1.headers["Content-Type"],"video/mp2t")
            self.assertEqual(r1.headers["Cache-Control"],"no-store")
            self.assertEqual(await r1.content.readexactly(3),b"one")
            self.assertEqual(await r2.content.readexactly(3),b"two")
            async with self.session.get(self.media.url("one")) as refused:self.assertEqual(refused.status,503)
            self.media.invalidate("one")
            async with asyncio.timeout(3):
                while self.media.has_clients("one"):await asyncio.sleep(.01)
            self.assertTrue(first.terminated)
            self.assertFalse(second.terminated)
            self.assertTrue(self.media.has_clients("two"))
            with self.assertRaises(aiohttp.ClientPayloadError):await r1.content.read()
            r1.close()
            self.media.resume("one")
            r3=await self.session.get(self.media.url("one"))
            self.assertEqual(await r3.content.readexactly(3),b"new")
            self.assertEqual(spawn.await_count,3)
            self.assertIn("copy",spawn.call_args.args)
            await self.media.remove("one")
            self.assertTrue(new.terminated)
            self.assertFalse(second.terminated)
            r2.close();r3.close()

    async def test_empty_launch_failure_and_first_media_timeout(self):
        empty=FakeProcess(eof=True)
        waiting=FakeProcess()
        with patch.object(mod,"FIRST_MEDIA_SECONDS",.03),patch.object(mod.asyncio,"create_subprocess_exec",new_callable=AsyncMock,side_effect=[empty,OSError("launch"),waiting]):
            for _ in range(3):
                async with self.session.get(self.media.url("one")) as r:self.assertEqual(r.status,503)
            self.assertTrue(waiting.terminated)
        self.assertFalse(self.media.has_clients("one"))

    async def test_established_media_timeout_and_eof(self):
        stalled=FakeProcess(b"data")
        ended=FakeProcess(b"done",eof=True)
        with patch.object(mod,"MEDIA_IDLE_SECONDS",.05),patch.object(mod.asyncio,"create_subprocess_exec",new_callable=AsyncMock,side_effect=[stalled,ended]):
            async with self.session.get(self.media.url("one")) as r:
                self.assertEqual(await r.content.readexactly(4),b"data")
                with self.assertRaises(aiohttp.ClientPayloadError):await r.content.read()
            async with self.session.get(self.media.url("one")) as r:self.assertEqual(await r.read(),b"done")
        self.assertTrue(stalled.terminated)

    async def test_cancelled_spawn_keeps_process_ownership(self):
        entered,release=asyncio.Event(),asyncio.Event()
        p=FakeProcess(b"data")
        async def spawn(*args,**kwargs):
            entered.set();await release.wait();return p
        with patch.object(mod.asyncio,"create_subprocess_exec",side_effect=spawn):
            req=asyncio.create_task(self.session.get(self.media.url("one")))
            await asyncio.wait_for(entered.wait(),2)
            self.media.invalidate("one")
            self.media.invalidate("one")
            release.set()
            await asyncio.gather(req,return_exceptions=True)
            async with asyncio.timeout(3):
                while self.media.has_clients("one"):await asyncio.sleep(.01)
        self.assertTrue(p.terminated)

    async def test_stop_escalation_drains_output(self):
        p=FakeProcess(b"x"*200000,stubborn=True)
        await mod._stop_process(p)
        self.assertTrue(p.terminated and p.killed)
        self.assertTrue(p.stdout.at_eof())

    async def test_occupied_port_cleanup(self):
        other=mod.NativeVideo("ffmpeg")
        with socket.socket() as s:
            s.bind(("127.0.0.1",0));s.listen(1)
            other.port=s.getsockname()[1]
            with self.assertRaises(OSError):await other.start()
        self.assertIsNone(other.runner)

    async def test_missing_transport_refused(self):
        request=Mock(match_info={"name":"one"},query_string="",can_read_body=False,transport=None)
        self.assertEqual((await self.media._media(request)).status,503)

    async def test_settle_cancellation_during_cleanup(self):
        release=asyncio.Event()
        task=asyncio.create_task(release.wait())
        waiter=asyncio.create_task(mod._settle(task))
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.sleep(0)
        waiter.cancel()
        release.set()
        self.assertEqual(await waiter,(True,True))
