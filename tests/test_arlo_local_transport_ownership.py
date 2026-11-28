"""Real mutual-TLS relay ownership under an unresponsive close-notify peer.

The TCP proxy is exclusively loopback. It selectively drops encrypted data
without closing TCP, reproducing a peer that does not acknowledge TLS shutdown.
No physical hub, credentials, or production endpoints are used.
"""
import asyncio
import os
import unittest
from unittest.mock import patch

from custom_components.arlo_local import rtsp_transport as rtsp
from custom_components.arlo_local.distribution import Distributor
from test_packaged_poc import PackagedTLSTests


class TransportOwnershipTests(PackagedTLSTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.blackholes = set()
        self.proxy_clients = {}
        self.upstream_writers = []
        self.clients = []
        self.proxy = await asyncio.start_server(self.proxy_client, '127.0.0.1', 0)
        self.proxy_port = self.proxy.sockets[0].getsockname()[1]
        real_connect = rtsp.connect_hub

        async def connect(*args, **kwargs):
            reader, writer = await real_connect(*args, **kwargs)
            self.upstream_writers.append((writer, writer.get_extra_info('socket')))
            return reader, writer

        self.enterContext(patch.object(rtsp, 'connect_hub', connect))
        self.broker = Distributor(os.environ['ARLO_POC_GO2RTC'], '127.0.0.1', 'hub.local',
                                  self.context, self.pin, port=self.proxy_port,
                                  ffmpeg_binary=os.environ['ARLO_POC_FFMPEG'], max_streams=2)
        for camera in ('one', 'two'):
            await self.broker.add(camera)

    async def proxy_client(self, reader, writer):
        port = writer.get_extra_info('peername')[1]
        hub_reader, hub_writer = await asyncio.open_connection('127.0.0.1', self.port)
        self.proxy_clients[port] = (writer, hub_writer)

        async def pump(source, dest):
            while data := await source.read(65536):
                if port not in self.blackholes:
                    dest.write(data)
                    await dest.drain()

        tasks = [asyncio.create_task(pump(reader, hub_writer)),
                 asyncio.create_task(pump(hub_reader, writer))]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            writer.transport.abort()
            hub_writer.transport.abort()
            self.proxy_clients.pop(port, None)

    async def asyncTearDown(self):
        self.blackholes.clear()
        try:
            await self.broker.close()
        finally:
            for writer, sock in self.upstream_writers:
                if sock.fileno() >= 0:
                    writer.transport.abort()
            for writer in self.clients:
                writer.transport.abort()
            for writer, peer in tuple(self.proxy_clients.values()):
                writer.transport.abort()
                peer.transport.abort()
            self.proxy.close()
            await self.proxy.wait_closed()
            await super().asyncTearDown()

    async def open_client(self, name):
        relay = self.broker.relays[name]
        reader, writer = await asyncio.open_connection('127.0.0.1', relay.local_port)
        self.clients.append(writer)
        writer.write(rtsp.Message('OPTIONS * RTSP/1.0', [('CSeq', '1')]).encode())
        await writer.drain()
        reply = await asyncio.wait_for(rtsp.read_unit(reader), 3)
        self.assertEqual(reply.get('CSeq'), '1')
        return reader, writer

    async def two_clients(self):
        first = await self.open_client('one')
        second = await self.open_client('two')
        self.assertEqual(self.broker._admission.in_use, 2)
        self.assertEqual(len(self.upstream_writers), 2)
        return first, second

    async def exercise_recovery(self, *, repeated_cancel, already_closing=False):
        _, (other_reader, other_writer) = await self.two_clients()
        relay = self.broker.relays['one']
        old_writer, old_socket = self.upstream_writers[0]
        self.blackholes.add(old_socket.getsockname()[1])
        self.assertGreaterEqual(old_socket.fileno(), 0)
        if already_closing:
            old_writer.close()
            self.assertTrue(old_writer.is_closing())
            self.assertGreaterEqual(old_socket.fileno(), 0)
        task = next(iter(relay.tasks))
        task.cancel()
        async with asyncio.timeout(2):
            while not old_writer.is_closing():
                await asyncio.sleep(.001)
        if repeated_cancel:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 4)
        self.assertEqual(self.broker._admission.in_use, 1)
        self.assertFalse(relay.active)
        # Immediately replace the failed generation; test actual descriptors,
        # not just application counters or the transport's is_closing flag.
        await self.open_client('one')
        self.assertEqual(relay.upstream_sessions, 2)
        self.assertEqual(self.broker._admission.in_use, 2)
        self.assertEqual(sum(sock.fileno() >= 0 for _, sock in self.upstream_writers), 2,
                         'replacement opened while the old TLS socket was still owned')
        self.assertEqual(old_socket.fileno(), -1)
        other_writer.write(rtsp.Message('OPTIONS * RTSP/1.0', [('CSeq', '2')]).encode())
        await other_writer.drain()
        self.assertEqual((await asyncio.wait_for(rtsp.read_unit(other_reader), 3)).get('CSeq'), '2')
        self.assertEqual(self.broker.relays['two'].upstream_sessions, 1)
        await self.broker.close()
        self.assertEqual(self.broker._admission.in_use, 0)
        self.assertTrue(all(sock.fileno() == -1 for _, sock in self.upstream_writers))

    async def test_unresponsive_tls_shutdown_cannot_oversubscribe_budget(self):
        await self.exercise_recovery(repeated_cancel=False)

    async def test_repeated_cancel_cannot_release_live_tls_socket(self):
        await self.exercise_recovery(repeated_cancel=True)

    async def test_already_closing_tls_socket_retains_abort_ownership(self):
        await self.exercise_recovery(repeated_cancel=False, already_closing=True)

    async def test_repeated_simultaneous_generation_loss_never_leaks_sockets(self):
        # Exercise both slots across repeated losses, not only one recovery
        # while the other camera is healthy. Every generation uses real TLS.
        for generation in range(1, 9):
            clients = await asyncio.gather(self.open_client('one'), self.open_client('two'))
            self.assertEqual(self.broker._admission.in_use, 2)
            live = [(w, s) for w, s in self.upstream_writers if s.fileno() >= 0]
            self.assertEqual(len(live), 2)
            ports = {s.getsockname()[1] for _, s in live}
            self.blackholes.update(ports)
            tasks = [next(iter(self.broker.relays[name].tasks)) for name in ('one', 'two')]
            for task in tasks:
                task.cancel()
            async with asyncio.timeout(2):
                while not all(w.is_closing() for w, _ in live):
                    await asyncio.sleep(.001)
            if generation % 2 == 0:
                for task in tasks:
                    task.cancel()
                await asyncio.sleep(0)
                for task in tasks:
                    task.cancel()
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 4)
            self.assertEqual(self.broker._admission.in_use, 0)
            self.assertTrue(all(s.fileno() == -1 for _, s in self.upstream_writers))
            self.assertEqual([self.broker.relays[k].upstream_sessions for k in ('one', 'two')],
                             [generation, generation])
            for reader, writer in clients:
                self.assertEqual(await asyncio.wait_for(reader.read(), 2), b'')
                await rtsp.close_writer(writer)
            async with asyncio.timeout(2):
                while ports.intersection(self.proxy_clients):
                    await asyncio.sleep(.001)
            self.blackholes.difference_update(ports)
        self.assertEqual(len(self.upstream_writers), 16)
        await self.broker.close()
        self.assertEqual(self.broker._admission.in_use, 0)
