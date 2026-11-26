"""Verify that diagnostic exports are restricted to non-secret operational state."""
import json
from types import MappingProxyType, SimpleNamespace
import tempfile
import unittest

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from custom_components.arlo_local.diagnostics import async_get_config_entry_diagnostics
from custom_components.arlo_local.inventory import CameraInfo
from custom_components.arlo_local.runtime import Runtime

class DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_allowlist_does_not_export_sensitive_fields(self):
        with tempfile.TemporaryDirectory() as root:
            hass=HomeAssistant(root)
            try:
                entry=ConfigEntry(domain="arlo_local",title="Synthetic",version=1,minor_version=1,
                    data={"host":"192.0.2.42","hostname":"secret-host.local","hub_id":"secret-hub",
                          "storage_id":"secret-storage","username":"private@example.invalid",
                          "password":"secret-password","mfa":"imap","imap_password":"secret-imap"},
                    source="user",unique_id="secret-hub",options={},discovery_keys=MappingProxyType({}),
                    subentries_data=None)
                self.assertFalse((await async_get_config_entry_diagnostics(hass,entry))["runtime_loaded"])
                runtime=Runtime(hass,entry)
                runtime.cameras={"secret-camera":CameraInfo("secret-camera","secret-unique","private name","VMC2030B",interface_version="i002")}
                entry.runtime_data=runtime
                result=await async_get_config_entry_diagnostics(hass,entry)
                self.assertEqual(result["camera_count"],1)
                self.assertEqual(result["camera_models"],["VMC2030B"])
                self.assertEqual(result["camera_interface_versions"],[{"model":"VMC2030B","interface_version":"i002"}])
                runtime.broker=SimpleNamespace(healthy=True,relays={"secret-camera":SimpleNamespace(connected_since=1.0),"pending":SimpleNamespace(connected_since=None)},admitted_count=2,native=SimpleNamespace(clients={"secret-camera":{1:object()}}),warm_leases={},fences={"secret-camera":SimpleNamespace(stalls=1)})
                result=await async_get_config_entry_diagnostics(hass,entry)
                self.assertEqual(result["active_upstream_count"],1)
                self.assertEqual(result["admitted_stream_count"],2)
                self.assertEqual(result["native_reader_count"],1)
                self.assertEqual(result["output_stalls"],1)
                serialized=json.dumps(result)
                for forbidden in ("secret","private","192.0.2.42","password","username"):
                    self.assertNotIn(forbidden,serialized)
            finally:
                await hass.async_stop(force=True)
