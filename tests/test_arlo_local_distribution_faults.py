"""Distributor process, cancellation and per-camera failure tests."""
import asyncio
import os
import ssl
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from custom_components.arlo_local.distribution import Distributor
from custom_components.arlo_local.rtsp_transport import Message, close_writer, read_unit
from test_packaged_poc import PackagedTLSTests


class DistributionAdmissionTests(PackagedTLSTests):
    async def asyncTearDown(self):
        await asyncio.gather(*(close_writer(w) for w in getattr(self, "clients", ())))
        if hasattr(self, "broker"):
            await self.broker.close()
        self.mock.abort_clients()
        await super().asyncTearDown()

    async def make_broker(self):
        broker = Distributor(
            os.environ["ARLO_POC_GO2RTC"], "127.0.0.1", "hub.local",
            self.context, self.pin, port=self.port,
            ffmpeg_binary=os.environ["ARLO_POC_FFMPEG"],
        )
        self.broker = broker
        self.clients = []
        return broker

    async def open_client(self, camera):
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", self.broker.relays[camera].local_port
        )
        self.clients.append(writer)
        writer.write(Message("OPTIONS * RTSP/1.0", [("CSeq", "1")]).encode())
        await writer.drain()
        return reader, writer

    async def test_hub_limit_releases_capacity_without_disrupting_active_relays(self):
        broker = await self.make_broker()
        for camera in ("one", "two", "three", "four"):
            await broker.add(camera)
        clients = await asyncio.gather(*(self.open_client(c) for c in ("one", "two", "three")))
        for reader, _ in clients:
            self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).start, "RTSP/1.0 200 OK")
        self.assertEqual(self.connections, 3)
        denied, _ = await self.open_client("four")
        try:
            result = await asyncio.wait_for(read_unit(denied), 3)
        except asyncio.IncompleteReadError:
            pass
        else:
            self.fail(f"Fourth camera was admitted: {result.start}")
        self.assertEqual(self.connections, 3)
        self.assertTrue(all(broker.relays[c].active for c in ("one", "two", "three")))

        # Closing one actual producer releases its hub slot; the fourth may
        # then establish a session without rebuilding the shared distributor.
        await asyncio.wait_for(broker.remove("one"), 3)
        reader, _ = await self.open_client("four")
        self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).start, "RTSP/1.0 200 OK")
        self.assertEqual(self.connections, 4)
        for reader, writer in clients[1:]:
            writer.write(Message("OPTIONS * RTSP/1.0", [("CSeq", "2")]).encode())
            await writer.drain()
            self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).get("CSeq"), "2")
        await asyncio.wait_for(broker.close(), 6)
        self.assertFalse(broker.relays)

    async def test_active_relay_close_terminates_client_before_waiting_for_server(self):
        broker = await self.make_broker()
        await broker.add("one")
        reader, _ = await self.open_client("one")
        self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).get("CSeq"), "1")
        relay = broker.relays["one"]
        await asyncio.wait_for(relay.close(), 3)
        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b"")
        self.assertFalse(relay.active)
        self.assertFalse(relay.tasks)

    async def test_upstream_disconnect_reconnects_only_the_failed_camera(self):
        broker = await self.make_broker()
        for camera in ("one", "two", "three"):
            await broker.add(camera)
        clients = await asyncio.gather(*(self.open_client(c) for c in ("one", "two", "three")))
        for reader, _ in clients:
            self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).get("CSeq"), "1")
        affected = broker.relays["one"]
        unaffected = {c: broker.relays[c] for c in ("two", "three")}
        task = next(iter(affected.tasks))
        task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 3)
        self.assertEqual(await asyncio.wait_for(clients[0][0].read(), 2), b"")
        self.assertFalse(affected.active)
        reader, _ = await self.open_client("one")
        self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).get("CSeq"), "1")
        self.assertEqual(affected.upstream_sessions, 2)
        self.assertEqual(self.connections, 4)
        for camera, (reader, writer) in zip(("two", "three"), clients[1:]):
            self.assertIs(broker.relays[camera], unaffected[camera])
            self.assertEqual(broker.relays[camera].upstream_sessions, 1)
            writer.write(Message("OPTIONS * RTSP/1.0", [("CSeq", "2")]).encode())
            await writer.drain()
            self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).get("CSeq"), "2")

    async def test_timeline_fault_releases_only_affected_generation(self):
        broker=await self.make_broker()
        for camera in ("one","two"):
            await broker.add(camera)
        # Late callbacks for a removed camera must not touch other streams.
        broker._timeline_fault("absent")
        broker._timeline_fault("one")
        self.assertFalse(broker.fences["one"].blocked)
        a,b=await asyncio.gather(self.open_client("one"),self.open_client("two"))
        for reader,_ in (a,b):await asyncio.wait_for(read_unit(reader),3)
        broker._timeline_fault("one")
        self.assertEqual(await asyncio.wait_for(a[0].read(),3),b"")
        async with asyncio.timeout(3):
            while broker.relays["one"].active:await asyncio.sleep(.01)
        self.assertFalse(broker.fences["one"].blocked)
        self.assertTrue(broker.relays["two"].active)
        self.assertEqual(broker.relays["two"].upstream_sessions,1)
        reopened,_=await self.open_client("one")
        self.assertEqual((await asyncio.wait_for(read_unit(reopened),3)).get("CSeq"),"1")
        self.assertEqual(broker.relays["one"].upstream_sessions,2)

    async def test_relay_close_during_cleanup_still_closes_local_client(self):
        broker = await self.make_broker()
        await broker.add("one")
        reader, writer = await self.open_client("one")
        self.assertEqual((await asyncio.wait_for(read_unit(reader), 3)).get("CSeq"), "1")
        relay = broker.relays["one"]
        entered = asyncio.Event()

        async def delayed_close(stream):
            if stream is not None:
                stream.close()
                entered.set()
                await asyncio.Future()

        with patch("custom_components.arlo_local.rtsp_transport.close_writer", side_effect=delayed_close):
            # A protocol error exits the pump while the downstream socket is open.
            writer.write(Message("INVALID * RTSP/1.0", [("CSeq", "2")]).encode())
            await writer.drain()
            await asyncio.wait_for(entered.wait(), 3)
            await asyncio.wait_for(relay.close(), 3)
        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b"")
        self.assertFalse(relay.active)
        self.assertFalse(relay.tasks)

class DistributorFaultTests(unittest.IsolatedAsyncioTestCase):
    def distributor(self,binary=None):
        broker=Distributor(binary or os.environ["ARLO_POC_GO2RTC"],"127.0.0.1","hub.local",
            ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),b"0"*32,ffmpeg_binary=os.environ["ARLO_POC_FFMPEG"])
        self.addAsyncCleanup(broker.close)
        return broker

    async def test_missing_binary_and_closed_state(self):
        with patch("custom_components.arlo_local.distribution.shutil.which",return_value=None):
            with self.assertRaises(RuntimeError):Distributor("go2rtc","127.0.0.1","hub.local",ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),b"0"*32)
        b=self.distributor()
        with self.assertRaises(RuntimeError):await b._request("GET","/api")
        with self.assertRaises(RuntimeError):b.output_url("cam")
        self.assertIsNone(await b.stats("cam"))
        await b.remove("absent")
        await b.close()
        with self.assertRaises(RuntimeError):await b.start()

    async def test_warm_capacity_expiry_and_failed_start(self):
        from custom_components.arlo_local.warm import WarmLease
        b=self.distributor()
        with self.assertRaises(ValueError):await b.prewarm("cam",121)
        self.assertFalse(await b.prewarm("cam"))
        for name in ("first","second","third"):
            await b.add(name)
        self.assertFalse(await b.prewarm("unknown"))
        b.relays["first"].accepting=False
        self.assertFalse(await b.prewarm("first"))
        b.relays["first"].accepting=True
        with patch.object(WarmLease,"start",new_callable=AsyncMock):
            self.assertTrue(await b.prewarm("first"))
            self.assertTrue(await b.prewarm("second"))
            self.assertFalse(await b.prewarm("third"))
            finished=asyncio.create_task(asyncio.sleep(0));await finished
            b.warm_leases["first"].task=finished
            self.assertTrue(await b.prewarm("third"))
            self.assertNotIn("first",b.warm_leases)
        await b.release_prewarm("third")
        with patch.object(WarmLease,"start",new_callable=AsyncMock,side_effect=OSError("synthetic start failure")):
            with self.assertRaises(OSError):await b.prewarm("third")
        self.assertNotIn("third",b.warm_leases)

    async def test_failed_process_start_and_untrusted_api_endpoint(self):
        b=self.distributor("/usr/bin/false")
        with self.assertRaisesRegex(RuntimeError,"exited"):
            await b.start()
        self.assertIsNone(b.process)
        self.assertIsNone(b.directory)
        self.assertIsNone(b.session)
        b=self.distributor()
        with patch.object(b,"_request",new_callable=AsyncMock,return_value={"rtsp":{"listen":"0.0.0.0:554"}}):
            with self.assertRaisesRegex(RuntimeError,"Unexpected distributor"):
                await b.start()
        self.assertIsNone(b.process)
        self.assertIsNone(b.directory)
        self.assertFalse(b.healthy)

    async def test_private_api_startup_exhaustion_releases_process(self):
        b=self.distributor()
        with patch.object(b,"_request",new_callable=AsyncMock,side_effect=OSError("synthetic API unavailable")) as request:
            with self.assertRaisesRegex(RuntimeError,"API not ready"):
                await b.start()
        self.assertEqual(request.await_count,30)
        self.assertIsNone(b.process)
        self.assertIsNone(b.directory)
        self.assertIsNone(b.session)

    async def test_registration_failure_does_not_disrupt_existing_relay(self):
        b=self.distributor()
        await b.start()
        first=await b.add("first")
        relay=b.relays["first"]
        self.assertIsNotNone(await b.stats("first"))
        original=b._request
        async def fail(method,path,params=None):
            if method=="PUT":raise OSError("synthetic registration failure")
            return await original(method,path,params)
        with patch.object(b,"_request",side_effect=fail):
            with self.assertRaises(OSError):await b.add("second")
        self.assertEqual(b.output_url("first"),first)
        self.assertIs(b.relays["first"],relay)
        self.assertNotIn("second",b.names)
        self.assertEqual(set(b.relays),{"first"})
        self.assertIsNone(await b.stats("second"))
        self.assertTrue(b.healthy)
        relay.active=True
        self.assertFalse(b.pause_if_idle())
        relay.active=False
        self.assertTrue(b.pause_if_idle())
        self.assertFalse(relay.accepting)
        b.resume()
        self.assertTrue(relay.accepting)
        b.process.kill()
        await b.process.wait()
        self.assertIsNone(await b.stats("first"))
        await b.remove("first")
        self.assertFalse(b.relays)
        self.assertIsNone(relay.server)

    async def test_cancel_during_config_preparation_cleans_directory(self):
        b=self.distributor()
        entered=threading.Event()
        release=threading.Event()
        original=b._prepare_directory
        paths=[]
        def prepare():
            entered.set()
            if not release.wait(3):raise TimeoutError("test preparation release")
            d=original()
            paths.append(Path(d.name))
            return d
        with patch.object(b,"_prepare_directory",side_effect=prepare):
            task=asyncio.create_task(b.start())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait,2))
                task.cancel()
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(paths)
        self.assertFalse(paths[0].exists())
        self.assertIsNone(b.directory)
        self.assertIsNone(b.process)

    async def test_cancel_during_subprocess_spawn_releases_owned_process(self):
        b=self.distributor()
        entered=asyncio.Event()
        release=asyncio.Event()
        original=asyncio.create_subprocess_exec
        processes=[]
        async def spawn(*args,**kwargs):
            entered.set()
            await release.wait()
            p=await original(*args,**kwargs)
            processes.append(p)
            return p
        with patch.object(asyncio,"create_subprocess_exec",side_effect=spawn):
            task=asyncio.create_task(b.start())
            await asyncio.wait_for(entered.wait(),3)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(processes)
        self.assertIsNotNone(processes[0].returncode)
        self.assertIsNone(b.process)
        self.assertIsNone(b.directory)

    async def test_exited_leader_never_receives_group_signal(self):
        b=self.distributor()
        await b.start()
        await b.add("cam")
        relay=b.relays["cam"]
        session=b.session
        directory=Path(b.directory.name)
        process=b.process
        process.terminate()
        await process.wait()
        with patch("os.killpg",side_effect=AssertionError("Unverified process-group signal")):
            await b.close()
        self.assertIsNotNone(process.returncode)
        self.assertIsNone(b.process)
        self.assertIsNone(b.session)
        self.assertTrue(session.closed)
        self.assertFalse(b.relays)
        self.assertIsNone(relay.server)
        self.assertIsNone(b.directory)
        self.assertFalse(directory.exists())

    async def test_cleanup_failure_retains_ownership_and_can_retry(self):
        b=self.distributor()
        await b.add("cam")
        process=b.process
        owner=b._process_owner
        self.assertIsNotNone(owner)
        with patch("custom_components.arlo_local.distribution.stop_owned_process",new_callable=AsyncMock,side_effect=PermissionError("synthetic denial")):
            with self.assertRaises(PermissionError):await b.close()
        self.assertIs(b.process,process)
        self.assertIs(b._process_owner,owner)
        self.assertFalse(b.fences)
        self.assertFalse(b.relays)
        self.assertIsNone(b.session)
        await b.close()
        self.assertIsNone(b.process)
        self.assertIsNone(b._process_owner)
        self.assertIsNone(b.directory)
