"""Offline TLS/RTSP tests; no Arlo, Home Assistant, or production network access."""
import asyncio
import base64
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import ssl
import tempfile
import unittest

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from pyaarlo.security_utils import SecurityUtils

from custom_components.arlo_local.rtsp_transport import (Message, Nonce, Relay, read_unit, rewrite_message,
                            tls_context, probe_tls, close_writer)


class ParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_coalesced_messages(self):
        reader = asyncio.StreamReader(limit=65536)
        message = Message("RTSP/1.0 200 OK", [("Content-Type", "application/sdp")],
                          b"v=0\r\na=control:rtsp://hub.local/cam/tcp/avc/track1\r\n")
        frame = b"$\x00\x00\x05abcde"
        wire = message.encode() + frame + Message("RTSP/1.0 200 OK", [("CSeq", "2")]).encode()
        async def feed():
            for i in range(0, len(wire), 3):
                reader.feed_data(wire[i:i+3])
                await asyncio.sleep(0)
            reader.feed_eof()
        task = asyncio.create_task(feed())
        msg = await read_unit(reader)
        self.assertEqual(msg.body, message.body)
        self.assertEqual(await read_unit(reader), frame)
        self.assertEqual((await read_unit(reader)).get("CSeq"), "2")
        await task
    async def test_duplicate_length_rejected(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"RTSP/1.0 200 OK\r\nContent-Length: 0\r\ncontent-length: 2\r\n\r\n")
        reader.feed_eof()
        with self.assertRaises(ValueError): await read_unit(reader)
    async def test_truncated_rtp_rejected(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"$\x00\x00\x05abc")
        reader.feed_eof()
        with self.assertRaises(asyncio.IncompleteReadError): await read_unit(reader)
    async def test_oversize_length_rejected(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"RTSP/1.0 200 OK\r\nContent-Length: 99999999\r\n\r\n")
        reader.feed_eof()
        with self.assertRaises(ValueError): await read_unit(reader)
    def test_url_boundaries_and_sdp_length(self):
        m = Message("RTSP/1.0 200 OK",
                    [("Content-Type", "application/sdp"),
                     ("Content-Base", "rtsp://hub.local/cam/"),
                     ("X-Other", "rtsp://hub.local.evil/cam")],
                    b"a=control:rtsp://hub.local/cam/track1\r\n")
        rewrite_message(m, ("hub.local",), "127.0.0.1:12345")
        wire = m.encode()
        self.assertIn(b"rtsp://127.0.0.1:12345/cam/track1", wire)
        self.assertIn(b"rtsp://hub.local.evil/cam", wire)
        self.assertEqual(int(m.get("Content-Length")), len(m.body))
    def test_nonce(self):
        n = Nonce()
        m = Message("PLAY * RTSP/1.0", [])
        n.send(m)
        self.assertIsNone(m.get("Nonce"))
        n.receive(Message("RTSP/1.0 200 OK", [("nonce", "42")]))
        n.send(m)
        self.assertEqual(m.get("Nonce"), "43")
        n.send(m)
        self.assertEqual(m.get("Nonce"), "44")
        with self.assertRaises(ValueError):
            n.receive(Message("RTSP/1.0 200 OK", [("Nonce", "invalid")]))


class TLSTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.security = SecurityUtils(str(root))
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        client_public = serialization.load_pem_public_key(self.security.public_key.encode())
        now = datetime.now(timezone.utc)
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Synthetic CA")])
        def issue(name, key, is_ca=False, usage=None):
            builder = (x509.CertificateBuilder()
                       .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
                       .issuer_name(ca_name).public_key(key)
                       .serial_number(x509.random_serial_number())
                       .not_valid_before(now-timedelta(days=1))
                       .not_valid_after(now+timedelta(days=1))
                       .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), True))
            if usage:
                builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), False)
            return builder.sign(ca_key, hashes.SHA256())
        ca = issue("Synthetic CA", ca_key.public_key(), is_ca=True)
        server_cert = issue("Synthetic hub", server_key.public_key(),
                            usage=ExtendedKeyUsageOID.SERVER_AUTH)
        client_cert = issue("Synthetic peer", client_public, usage=ExtendedKeyUsageOID.CLIENT_AUTH)
        def b64(c):
            return base64.b64encode(c.public_bytes(serialization.Encoding.DER)).decode()
        self.security.save_device_certs("test-hub", {"certsData":[
            {"peerCert":b64(client_cert), "deviceCert":b64(server_cert)}], "icaCert":b64(ca)})
        hub = Path(self.security.device_certs_path("test-hub"))
        ica = Path(self.security.certs_path)/"ica.crt"
        server_key_file = root/"server-key.pem"
        server_key_file.write_bytes(server_key.private_bytes(serialization.Encoding.PEM,
                       serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        self.context, self.pin = tls_context(peer_chain=hub/"combined.crt",
                private_key=self.security.private_key_path, device_cert=hub/"device.crt",
                ica_cert=ica)
        self.server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.server_context.load_cert_chain(str(hub/"device.crt"), str(server_key_file))
        self.server_context.load_verify_locations(cafile=str(ica))
        self.server_context.verify_mode = ssl.CERT_REQUIRED
        self.server_context.maximum_version = ssl.TLSVersion.TLSv1_2
        self.requests = []
        self.connections = 0
        self.finished = asyncio.Event()
        self.frame = b"$\x00\x00\x08\x80\x60\x00\x01DATA"
        self.mock = await asyncio.start_server(self.mock_hub, "127.0.0.1", 0,
                                      ssl=self.server_context, ssl_handshake_timeout=3)
        self.port = self.mock.sockets[0].getsockname()[1]
        self.relays = []
    async def asyncTearDown(self):
        for r in self.relays: await r.close()
        self.mock.close()
        await self.mock.wait_closed()
    async def mock_hub(self, reader, writer):
        self.connections += 1
        self.finished.clear()
        try:
            while True:
                m = await read_unit(reader)
                self.requests.append(m)
                if not isinstance(m, Message): continue
                method = m.start.split()[0]
                reply = Message("RTSP/1.0 200 OK", [("CSeq", m.get("CSeq") or "0")])
                if method == "OPTIONS": reply.set("Nonce", "100")
                if method == "DESCRIBE":
                    reply.set("Content-Type", "application/sdp")
                    reply.set("Content-Base", "rtsp://hub.local/cam/tcp/avc/")
                    reply.body = b"v=0\r\na=control:rtsp://hub.local/cam/tcp/avc/track1\r\n"
                if method == "SETUP": reply.set("Session", "synthetic-session")
                if method == "PLAY": reply.set("Session", "synthetic-session")
                wire = reply.encode() + (self.frame if method == "PLAY" else b"")
                for i in range(0, len(wire), 5):
                    writer.write(wire[i:i+5])
                    await writer.drain()
                    await asyncio.sleep(0)
                if method == "TEARDOWN": break
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            await close_writer(writer)
            self.finished.set()
    def relay(self, **kwargs):
        r = Relay("127.0.0.1", self.port, "hub.local", "cam",
                  self.context, self.pin, **kwargs)
        self.relays.append(r)
        return r
    async def test_tls_only_cached_credentials_twice(self):
        for _ in range(2):
            result = await probe_tls("127.0.0.1", self.port, "hub.local", self.context, self.pin)
            self.assertEqual(result, {"tls_version":"TLSv1.2","peer_pin_verified":True,
                                      "rtsp_commands_sent":0})
            await asyncio.wait_for(self.finished.wait(), 3)
        self.assertEqual(self.connections, 2)
        self.assertEqual(self.requests, [])
    async def test_wrong_pin_rejected(self):
        with self.assertRaises(ssl.SSLCertVerificationError):
            await probe_tls("127.0.0.1", self.port, "hub.local", self.context, b"0"*32)
        self.assertEqual(self.requests, [])
    async def test_missing_client_certificate_rejected(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.load_verify_locations(cafile=str(Path(self.security.certs_path)/"ica.crt"))
        context.maximum_version = ssl.TLSVersion.TLSv1_2
        with self.assertRaises((ssl.SSLError, ConnectionResetError, OSError)):
            await probe_tls("127.0.0.1", self.port, "hub.local", context, self.pin)
        self.assertEqual(self.requests, [])
    async def test_default_playback_gate(self):
        with self.assertRaises(PermissionError): self.relay()
        self.assertEqual(self.connections, 0)
    async def test_wrong_camera_rejected_before_hub_connect(self):
        r = self.relay(playback_authorized=True)
        url = await r.start()
        rd, wr = await asyncio.open_connection("127.0.0.1", r.local_port)
        wr.write(Message("DESCRIBE "+url.replace("/cam/","/other/")+" RTSP/1.0",
                         [("CSeq","1")]).encode()); await wr.drain()
        self.assertEqual(await asyncio.wait_for(rd.read(),3), b"")
        await close_writer(wr)
        self.assertEqual(self.connections, 0)
    async def test_rtsp_session_and_frames(self):
        r = self.relay(playback_authorized=True)
        url = await r.start()
        self.assertEqual(r.server.sockets[0].getsockname()[0], "127.0.0.1")
        rd, wr = await asyncio.open_connection("127.0.0.1", r.local_port)
        async def call(method, uri, seq, headers=()):
            req = Message(f"{method} {uri} RTSP/1.0", [("CSeq", str(seq)), *headers])
            wire = req.encode()
            for i in range(0,len(wire),4):
                wr.write(wire[i:i+4]);await wr.drain()
            return await asyncio.wait_for(read_unit(rd),3)
        await call("OPTIONS", "*", 1)
        sdp = await call("DESCRIBE", url, 2)
        self.assertIn((url+"/track1").encode(), sdp.body)
        self.assertEqual(sdp.get("Content-Base"),url+"/")
        await call("SETUP",url+"/track1",3,[("Transport","RTP/AVP/TCP;unicast;interleaved=0-1")])
        await call("PLAY",url,4,[("Session","synthetic-session")])
        self.assertEqual(await asyncio.wait_for(read_unit(rd),3), self.frame)
        await call("TEARDOWN",url,5,[("Session","synthetic-session")])
        await close_writer(wr)
        await asyncio.wait_for(self.finished.wait(),3)
        self.assertEqual([x.get("Nonce") for x in self.requests],[None,"101","102","103","104"])
        self.assertEqual(self.requests[1].start,"DESCRIBE rtsp://hub.local/cam/tcp/avc RTSP/1.0")
        self.assertEqual(r.errors,[])
        self.assertEqual(self.connections,1)
    async def test_second_local_consumer_rejected(self):
        r = self.relay(playback_authorized=True)
        await r.start()
        rd, wr = await asyncio.open_connection("127.0.0.1", r.local_port)
        wr.write(Message("OPTIONS * RTSP/1.0",[("CSeq","1")]).encode()); await wr.drain()
        await asyncio.wait_for(read_unit(rd),3)
        rd2,wr2 = await asyncio.open_connection("127.0.0.1",r.local_port)
        self.assertEqual(await asyncio.wait_for(rd2.read(),3), b"")
        await close_writer(wr2)
        await close_writer(wr)
        await asyncio.wait_for(self.finished.wait(),3)
        self.assertEqual(self.connections,1)


if __name__=="__main__":unittest.main(verbosity=2)
