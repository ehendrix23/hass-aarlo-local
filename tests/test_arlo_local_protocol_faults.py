"""Protocol failure cases against the production relay and synthetic TLS hub."""
import asyncio
import ssl
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from custom_components.arlo_local import rtsp_transport as r
from test_packaged_poc import PackagedTLSTests


class ProtocolFaultTests(IsolatedAsyncioTestCase):
    async def test_invalid_headers_and_response_lines(self):
        for wire in (b"RTSP/1.0 200 OK\r\nInvalid\r\n\r\n",
                     b"RTSP/1.0 200 OK\r\nBad Name: test\r\n\r\n",
                     b"RTSP/1.0 200 OK\r\nContent-Length: -1\r\n\r\n"):
            reader=asyncio.StreamReader()
            reader.feed_data(wire);reader.feed_eof()
            with self.assertRaises(ValueError):await r.read_unit(reader)
        with patch.object(r,"HEADER_LIMIT",16):
            reader=asyncio.StreamReader()
            reader.feed_data(b"RTSP/1.0 200 OK\r\nLong: long\r\n\r\n")
            with self.assertRaises(ValueError):await r.read_unit(reader)
        for start in ("RTSP/1.0","garbage","OPTIONS * RTSP/1.0","RTSP/1.0 bad"):
            with self.assertRaises(ValueError):r.response_status(r.Message(start,[]))
        self.assertEqual(r.response_status(r.Message("RTSP/1.0 200 OK",[])),"200")

    async def test_relay_rejects_non_camera_and_traversal_requests(self):
        ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        for cam,codec in (("../bad","avc"),("cam","unsupported")):
            with self.assertRaises(ValueError):
                r.Relay("127.0.0.1",554,"hub.local",cam,ctx,b"0"*32,playback_authorized=True,codec=codec)
        relay=r.Relay("127.0.0.1",554,"hub.local","cam",ctx,b"0"*32,playback_authorized=True)
        self.addAsyncCleanup(relay.close)
        url=await relay.start()
        with self.assertRaises(RuntimeError):await relay.start()
        for start in ("POST * RTSP/1.0","OPTIONS *","PLAY rtsp://other/cam RTSP/1.0",
                      "PLAY "+url+"/../other RTSP/1.0","PLAY "+url+"/%2e%2e/other RTSP/1.0",
                      "PLAY "+url+"/%5cother RTSP/1.0"):
            with self.assertRaises(ValueError):relay._check_request(r.Message(start,[]))
        relay._check_request(r.Message("DESCRIBE "+url+" RTSP/1.0",[]))
        relay.accepting=False
        rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
        self.assertEqual(await asyncio.wait_for(rd.read(),2),b"")
        await r.close_writer(wr)
        await relay.close()
        self.assertFalse(relay.tasks)

    async def test_unverified_transport_is_closed(self):
        writer=Mock()
        writer.get_extra_info.return_value=None
        writer.wait_closed=AsyncMock()
        with patch.object(asyncio,"open_connection",new_callable=AsyncMock,return_value=(asyncio.StreamReader(),writer)):
            with self.assertRaises(ssl.SSLCertVerificationError):
                await r.connect_hub("127.0.0.1",554,"hub.local",ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),b"0"*32)
        writer.close.assert_called_once()
        writer.close.reset_mock()
        with patch.object(r,"connect_hub",new_callable=AsyncMock,return_value=(asyncio.StreamReader(),writer)):
            with self.assertRaises(ssl.SSLCertVerificationError):
                await r.probe_tls("127.0.0.1",554,"hub.local",ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),b"0"*32)
        writer.close.assert_called_once()


class BootstrapFaultTests(PackagedTLSTests):
    async def mock_hub(self,reader,writer):
        self.connections+=1
        self.finished.clear()
        try:
            first=await r.read_unit(reader)
            self.requests.append(first)
            mode=getattr(self,"mode","ok")
            reply=r.Message("RTSP/1.0 200 OK",[("CSeq",first.get("CSeq") or "1"),("Nonce","100")])
            if mode=="reject":
                reply.start="RTSP/1.0 400 Bad Request"
            elif mode=="wrong_seq":
                reply.set("CSeq",999)
            elif mode=="no_nonce":
                reply.headers=[("CSeq",first.get("CSeq") or "1")]
            elif mode=="bad_status":
                reply.start="RTSP/1.0"
            if mode=="rtp_first":
                writer.write(b"$\x00\x00\x01x")
            else:
                writer.write(reply.encode())
            await writer.drain()
            if mode in ("ok","bad_cseq_response","bad_response"):
                second=await r.read_unit(reader)
                self.requests.append(second)
                response=r.Message("RTSP/1.0 200 OK",[("CSeq",second.get("CSeq") or "2")])
                if mode=="bad_cseq_response":
                    response.set("CSeq","wrong")
                if mode=="bad_response":
                    response.start="garbage"
                writer.write(response.encode())
                await writer.drain()
                if mode=="ok":
                    third=await r.read_unit(reader)
                    self.requests.append(third)
        except (OSError,asyncio.IncompleteReadError):
            pass
        finally:
            await r.close_writer(writer)
            self.finished.set()

    async def test_bootstrap_success_cseq_mapping_and_feedback(self):
        relay=self.relay(playback_authorized=True)
        url=await relay.start()
        rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
        wr.write(r.Message("DESCRIBE "+url+" RTSP/1.0",[("CSeq","1")]).encode());await wr.drain()
        response=await asyncio.wait_for(r.read_unit(rd),3)
        self.assertEqual(response.get("CSeq"),"1")
        self.assertEqual(self.requests[0].start.split()[0],"OPTIONS")
        self.assertEqual(self.requests[1].get("CSeq"),"2")
        self.assertEqual(self.requests[1].get("Nonce"),"101")
        wr.write(b"$\x01\x00\x01x");await wr.drain()
        await asyncio.wait_for(self.finished.wait(),3)
        self.assertEqual(self.requests[2],b"$\x01\x00\x01x")
        await r.close_writer(wr)
        self.assertEqual(self.connections,1)

    async def test_bootstrap_failure_closes_connection(self):
        for mode in ("reject","wrong_seq","no_nonce","bad_status","rtp_first","bad_cseq_response","bad_response"):
            self.mode=mode
            relay=self.relay(playback_authorized=True)
            url=await relay.start()
            rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
            wr.write(r.Message("DESCRIBE "+url+" RTSP/1.0",[("CSeq","1")]).encode());await wr.drain()
            self.assertEqual(await asyncio.wait_for(rd.read(),3),b"",mode)
            await r.close_writer(wr)
            await relay.close()
            self.assertFalse(relay.active)
            self.assertTrue(relay.errors,mode)

    async def test_invalid_initial_or_subsequent_cseq(self):
        for header in ([],[("CSeq","bad")]):
            relay=self.relay(playback_authorized=True)
            url=await relay.start()
            rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
            wr.write(r.Message("DESCRIBE "+url+" RTSP/1.0",header).encode());await wr.drain()
            self.assertEqual(await asyncio.wait_for(rd.read(),3),b"")
            await r.close_writer(wr)
            await relay.close()
            self.assertTrue(relay.errors)
        relay=self.relay(playback_authorized=True)
        url=await relay.start()
        rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
        wr.write(r.Message("DESCRIBE "+url+" RTSP/1.0",[("CSeq","1")]).encode());await wr.drain()
        await asyncio.wait_for(r.read_unit(rd),3)
        wr.write(r.Message("PLAY "+url+" RTSP/1.0",[("CSeq","bad")]).encode());await wr.drain()
        self.assertEqual(await asyncio.wait_for(rd.read(),3),b"")
        await r.close_writer(wr)
        await relay.close()
        self.assertTrue(relay.errors)

    async def test_invalid_first_packet_does_not_connect_and_cancel_releases(self):
        relay=self.relay(playback_authorized=True)
        await relay.start()
        rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
        wr.write(b"$\x00\x00\x01x");await wr.drain()
        self.assertEqual(await asyncio.wait_for(rd.read(),3),b"")
        self.assertEqual(self.connections,0)
        await r.close_writer(wr)
        rd,wr=await asyncio.open_connection("127.0.0.1",relay.local_port)
        async with asyncio.timeout(2):
            while not relay.tasks:
                await asyncio.sleep(.005)
        await relay.close()
        self.assertEqual(await asyncio.wait_for(rd.read(),3),b"")
        self.assertFalse(relay.active)
        self.assertFalse(relay.tasks)
        await r.close_writer(wr)
