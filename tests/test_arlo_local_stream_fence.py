"""A disconnected upstream must fence only the corresponding output consumers."""

import asyncio
import unittest
from unittest.mock import patch

from custom_components.arlo_local.rtsp_transport import Message, close_writer, read_unit
from custom_components.arlo_local.stream_fence import StreamFence
from test_arlo_local_video_timeline import rtp, setup


class StreamFenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend_clients = set()
        self.requests = []
        self.inject_reset = False
        self.inject_stall = False

        async def backend(reader, writer):
            self.backend_clients.add(writer)
            try:
                while True:
                    msg = await read_unit(reader)
                    if isinstance(msg, Message):
                        self.requests.append(msg.start)
                        if self.inject_reset and msg.start.startswith("SETUP "):
                            writer.write(setup().encode()+rtp(100)+rtp(200)+rtp(50))
                            await writer.drain()
                            continue
                        if self.inject_stall and msg.start.startswith("SETUP "):
                            writer.write(setup().encode()+rtp(100)+rtp(100))
                            await writer.drain()
                            continue
                        writer.write(Message("RTSP/1.0 200 OK", [("CSeq", msg.get("CSeq"))]).encode())
                        await writer.drain()
                        if msg.get("CSeq") == "99":
                            return
                    else:
                        self.requests.append(msg)
            except (OSError, asyncio.IncompleteReadError):
                pass
            finally:
                self.backend_clients.discard(writer)
                await close_writer(writer)

        self.backend = await asyncio.start_server(backend, "127.0.0.1", 0)
        port = self.backend.sockets[0].getsockname()[1]
        self.fences = {n: StreamFence(port, n) for n in ("one", "two")}
        self.clients = []
        for fence in self.fences.values():
            await fence.start()

    async def asyncTearDown(self):
        for fence in self.fences.values():
            await fence.close()
        await asyncio.gather(*(close_writer(w) for w in self.clients))
        self.backend.close()
        await asyncio.gather(*(close_writer(w) for w in list(self.backend_clients)))
        await self.backend.wait_closed()

    async def client(self, name, other=None):
        fence = self.fences[name]
        reader, writer = await asyncio.open_connection("127.0.0.1", fence.port)
        self.clients.append(writer)
        writer.write(Message(f"OPTIONS rtsp://127.0.0.1:{fence.port}/{other or name}?video RTSP/1.0", [("CSeq", "1")]).encode())
        await writer.drain()
        return reader, writer

    async def test_invalidated_output_closes_only_its_viewers_and_reopens(self):
        first, second, other = await asyncio.gather(self.client("one"), self.client("one"), self.client("two"))
        for reader, _ in (first, second, other):
            self.assertEqual((await asyncio.wait_for(read_unit(reader), 2)).get("CSeq"), "1")
        fence = self.fences["one"]
        fence.invalidate()
        for reader, _ in (first, second):
            self.assertEqual(await asyncio.wait_for(reader.read(), 2), b"")
        denied, _ = await self.client("one")
        try:
            self.assertEqual(await asyncio.wait_for(denied.read(), 2), b"")
        except ConnectionResetError:
            pass  # A rejected TCP connection may end with RST instead of EOF.
        other[1].write(Message("OPTIONS * RTSP/1.0", [("CSeq", "2")]).encode())
        await other[1].drain()
        self.assertEqual((await asyncio.wait_for(read_unit(other[0]), 2)).get("CSeq"), "2")
        fence.resume()
        reopened, _ = await self.client("one")
        self.assertEqual((await asyncio.wait_for(read_unit(reopened), 2)).get("CSeq"), "1")

    async def test_cross_camera_request_never_reaches_backend(self):
        reader, _ = await self.client("one", "two")
        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b"")
        self.assertEqual(self.requests, [])

    async def test_established_video_stall_closes_only_affected_consumer(self):
        first,other=await asyncio.gather(self.client("one"),self.client("two"))
        for r,_ in (first,other):await asyncio.wait_for(read_unit(r),2)
        fence=self.fences["one"]
        self.inject_stall=True
        with patch("custom_components.arlo_local.stream_fence.ESTABLISHED_VIDEO_TIMEOUT",.05):
            first[1].write(Message(f"SETUP rtsp://127.0.0.1:{fence.port}/one/trackID=0 RTSP/1.0",[("CSeq","2")]).encode())
            await first[1].drain()
            data=await asyncio.wait_for(first[0].read(),2)
        self.assertIn(rtp(100),data)
        self.assertEqual(fence.stalls,1)
        self.assertFalse(fence.blocked)
        other[1].write(Message("OPTIONS * RTSP/1.0",[("CSeq","3")]).encode());await other[1].drain()
        self.assertEqual((await asyncio.wait_for(read_unit(other[0]),2)).get("CSeq"),"3")

    async def test_same_socket_timestamp_reset_closes_camera_viewers(self):
        first,second,other=await asyncio.gather(self.client("one"),self.client("one"),self.client("two"))
        for reader,_ in (first,second,other):
            await asyncio.wait_for(read_unit(reader),2)
        fence=self.fences["one"]
        callbacks=[]
        def fault():
            callbacks.append(True)
            fence.invalidate()
        fence.on_timeline_fault=fault
        self.inject_reset=True
        first[1].write(Message(f"SETUP rtsp://127.0.0.1:{fence.port}/one/trackID=0 RTSP/1.0",[("CSeq","2")]).encode())
        await first[1].drain()
        try:
            data=await asyncio.wait_for(first[0].read(),2)
        except ConnectionResetError:
            data=b""
        self.assertNotIn(rtp(50),data)
        self.assertEqual(fence.timeline_faults,1)
        self.assertEqual(callbacks,[True])
        self.assertEqual(await asyncio.wait_for(second[0].read(),2),b"")
        other[1].write(Message("OPTIONS * RTSP/1.0",[("CSeq","3")]).encode())
        await other[1].drain()
        self.assertEqual((await asyncio.wait_for(read_unit(other[0]),2)).get("CSeq"),"3")

    async def test_protocol_rejection_and_initial_options(self):
        fence = self.fences["one"]
        url = f"rtsp://127.0.0.1:{fence.port}/one?video"
        binary = b"$\x00\x00\x01x"
        async def connect():
            r,w = await asyncio.open_connection("127.0.0.1", fence.port)
            self.clients.append(w)
            return r,w
        async def rejected(payload):
            r,w = await connect()
            w.write(payload)
            await w.drain()
            try:
                self.assertEqual(await asyncio.wait_for(r.read(),2),b"")
            except ConnectionResetError:
                pass
        for payload in (binary, Message(f"BOGUS {url} RTSP/1.0",[("CSeq","1")]).encode(),
                        Message(f"PLAY {url} RTSP/1.0",[("CSeq","1")]).encode(),
                        Message("OPTIONS * RTSP/1.0",[]).encode()):
            await rejected(payload)
        self.assertEqual(self.requests,[])
        r,w=await connect()
        w.write(Message("OPTIONS * RTSP/1.0",[("CSeq","1")]).encode());await w.drain()
        self.assertEqual((await asyncio.wait_for(read_unit(r),2)).get("CSeq"),"1")
        w.write(binary);await w.drain()
        self.assertEqual(await asyncio.wait_for(r.read(),2),b"")
        r,w=await connect()
        w.write(Message("OPTIONS * RTSP/1.0",[("CSeq","1")]).encode());await w.drain()
        self.assertEqual((await asyncio.wait_for(read_unit(r),2)).get("CSeq"),"1")
        w.write(Message(f"DESCRIBE {url} RTSP/1.0",[("CSeq","2")]).encode());await w.drain()
        self.assertEqual((await asyncio.wait_for(read_unit(r),2)).get("CSeq"),"2")
        w.write(binary+Message("OPTIONS * RTSP/1.0",[("CSeq","99")]).encode());await w.drain()
        self.assertEqual((await asyncio.wait_for(read_unit(r),2)).get("CSeq"),"99")
        self.assertEqual(await asyncio.wait_for(r.read(),2),b"")
        self.assertIn(binary,self.requests)
