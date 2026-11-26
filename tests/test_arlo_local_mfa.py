"""Manual MFA bridge tests: no Arlo request and no stored OTP."""
import asyncio
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from pyaarlo.backend import ArloBackEnd
from custom_components.arlo_local.manual_mfa import ManualMFA
from custom_components.arlo_local.owner_session import IsolatedBackend, OwnerConfiguration

class ManualMFATests(unittest.IsolatedAsyncioTestCase):
    async def test_upstream_console_contract_without_terminal(self):
        bridge = ManualMFA(timeout=2)
        self.addCleanup(bridge.close)
        with tempfile.TemporaryDirectory() as root:
            config = {"username": "synthetic@example.invalid", "password": "synthetic",
                      "mfa": "manual_email"}
            with patch.object(ArloBackEnd, "_login", return_value=True):
                backend = IsolatedBackend(OwnerConfiguration(Path(root), config, bridge))
            helper = backend._get_tfa()
            self.assertTrue(helper.start())
            worker = asyncio.create_task(asyncio.to_thread(helper.get))
            await asyncio.wait_for(bridge.challenge.wait(), 1)
            self.assertFalse(bridge.submit("invalid"))
            self.assertTrue(bridge.submit("123456"))
            self.assertEqual(await worker, "123456")
            self.assertFalse(bridge.pending)
            helper.stop()

    async def test_cancel_releases_worker(self):
        bridge = ManualMFA(timeout=2)
        worker = asyncio.create_task(asyncio.to_thread(bridge.request_code))
        await asyncio.wait_for(bridge.challenge.wait(), 1)
        bridge.close()
        self.assertIsNone(await worker)
        self.assertFalse(bridge.pending)
        self.assertFalse(bridge.submit("123456"))
        self.assertIsNone(await asyncio.to_thread(bridge.request_code))

    async def test_timeout_does_not_accept_late_code(self):
        bridge = ManualMFA(timeout=.05)
        worker = asyncio.create_task(asyncio.to_thread(bridge.request_code))
        await asyncio.wait_for(bridge.challenge.wait(), 1)
        self.assertIsNone(await worker)
        self.assertFalse(bridge.submit("123456"))
        bridge.close()
