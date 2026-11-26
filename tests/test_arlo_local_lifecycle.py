"""Exercise actual HA config-entry setup, device registry and camera services."""
import asyncio
import base64
from pathlib import Path
import os
import re
import shutil
import socket
import tempfile
from types import MappingProxyType
from unittest.mock import patch

from homeassistant import bootstrap, loader
from homeassistant.components.camera import async_get_image
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr, entity_registry as er

from custom_components.arlo_local import runtime as integration_runtime
from custom_components.arlo_local.inventory import Hub, CameraInfo
from custom_components.arlo_local.owner_session import OwnerSession
from custom_components.arlo_local.storage import CredentialStore, serialize_inventory
from test_ffmpeg_transport import DecoderTest


class LifecycleTests(DecoderTest):
    frame_count = 150

    async def test_native_entry_registry_snapshot_reload_and_unload(self):
        ffmpeg = os.environ["ARLO_POC_FFMPEG"]
        proc = await asyncio.create_subprocess_exec(
            ffmpeg, "-v", "error", "-f", "lavfi",
            "-i", "color=c=black:s=160x120:r=5", "-frames:v", "1",
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-x264-params", "keyint=1:repeat-headers=1", "-f", "h264", "pipe:1",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        raw, err = await asyncio.wait_for(proc.communicate(), 10)
        self.assertEqual(proc.returncode, 0, err.decode())
        self.nals = [n for n in re.split(b"\\x00\\x00(?:\\x00)?\\x01", raw) if n]
        self.sprop = ",".join(
            base64.b64encode(next(n for n in self.nals if n[0] & 31 == t)).decode()
            for t in (7, 8)
        )
        with tempfile.TemporaryDirectory(prefix="arlo-native-lifecycle-") as conf:
            config = Path(conf)
            shutil.copytree(
                Path(__file__).resolve().parents[1] / "custom_components" / "arlo_local",
                config / "custom_components" / "arlo_local",
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
            hub = Hub("hub-device", "test-hub", "Synthetic Hub", "VMB5000")
            cameras = {"cam": CameraInfo("cam", "camera-one", "Camera A", "VMC2030B")}
            store = CredentialStore(config / ".arlo_local" / "teststore")
            generation = store.root / "arlo-cert-poc-synthetic"
            generation.mkdir(mode=0o700)
            shutil.copytree(Path(self.tmp.name) / "certs", generation / "certs")
            store.activate({"directory": generation.name, **serialize_inventory(hub, cameras)})
            hass = HomeAssistant(conf)
            loader.async_setup(hass)
            hass.config.skip_pip = True
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                http_port = listener.getsockname()[1]
            # Keep this ephemeral test out of unrelated LAN discovery.
            with patch.multiple(loader, ZEROCONF={}, HOMEKIT={}), patch.multiple(
                integration_runtime, PORT=self.port, INVENTORY_INTERVAL=3600
            ), patch.object(
                integration_runtime.shutil, "which",
                side_effect=lambda name: os.environ["ARLO_POC_GO2RTC"] if name == "go2rtc" else ffmpeg
            ), patch.object(OwnerSession, "inventory", return_value=(hub, cameras)):
                try:
                    result = await bootstrap.async_from_config_dict({
                        "homeassistant": {"name": "Synthetic Arlo Lifecycle", "time_zone": "UTC",
                                          "latitude": 0, "longitude": 0, "elevation": 0,
                                          "unit_system": "metric"},
                        "http": {"server_host": "127.0.0.1", "server_port": http_port},
                        "ffmpeg": {"ffmpeg_bin": ffmpeg},
                        "stream": {},
                        "camera": {},
                    }, hass)
                    self.assertIs(result, hass)
                    self.assertTrue({"http", "stream", "camera"} <= hass.config.components)
                    await hass.async_start()
                    entry = ConfigEntry(
                        domain="arlo_local", title="Synthetic Arlo Local", version=1, minor_version=1,
                        data={"host": "127.0.0.1", "hostname": "hub.local", "hub_id": "hub-device",
                              "storage_id": "teststore", "username": "synthetic@example.invalid",
                              "password": "synthetic", "mfa": "push"},
                        source="user", unique_id="hub-device", options={},
                        discovery_keys=MappingProxyType({}), subentries_data=None,
                    )
                    await hass.config_entries.async_add(entry)
                    await hass.async_block_till_done()
                    if entry.state != ConfigEntryState.LOADED:
                        print("Setup state:", entry.state, "component loaded:",
                              "arlo_local" in hass.config.components,
                              "entries:", len(hass.config_entries.async_entries("arlo_local")),
                              "reason:", entry.reason, flush=True)
                    self.assertEqual(entry.state, ConfigEntryState.LOADED)
                    original_runtime = entry.runtime_data
                    device_registry = dr.async_get(hass)
                    hub_device = device_registry.async_get_device_by_identifier(("arlo_local", "test-hub"), entry.entry_id)
                    camera_device = device_registry.async_get_device_by_identifier(("arlo_local", "camera-one"), entry.entry_id)
                    self.assertIsNotNone(hub_device)
                    self.assertIsNotNone(camera_device)
                    self.assertEqual(camera_device.via_device_id, hub_device.id)
                    entity_id = er.async_get(hass).async_get_entity_id("camera", "arlo_local", "test-hub_camera-one")
                    self.assertIsNotNone(entity_id)
                    self.assertTrue(hass.services.has_service("arlo_local", "prewarm"))
                    self.assertTrue(hass.services.has_service("arlo_local", "release_prewarm"))
                    # The synthetic certificate lasts one day, so maintenance
                    # must reject a warm lease unless the test lowers the
                    # renewal window. No real credentials are changed.
                    from homeassistant.exceptions import HomeAssistantError
                    with self.assertRaises(HomeAssistantError):
                        await hass.services.async_call("arlo_local", "prewarm", {"entity_id": entity_id, "duration": 5}, blocking=True)
                    with patch.object(integration_runtime, "RENEW_DAYS", 0):
                        await hass.services.async_call("arlo_local", "prewarm", {"entity_id": entity_id, "duration": 15}, blocking=True)
                    self.assertIn("cam", original_runtime.broker.warm_leases)
                    image = await async_get_image(hass, entity_id, timeout=10)
                    self.assertTrue(image.content.startswith(bytes.fromhex("ffd8")))
                    from io import BytesIO
                    from PIL import Image
                    resized = await async_get_image(hass, entity_id, timeout=10, width=80, height=60)
                    self.assertEqual(Image.open(BytesIO(resized.content)).size,(80,60))
                    await hass.services.async_call("arlo_local", "release_prewarm", {"entity_id": entity_id}, blocking=True)
                    self.assertFalse(original_runtime.broker.warm_leases)
                    # Real device-registry metadata reconciliation preserves
                    # the user's chosen name and the entity ID.
                    device_registry.async_update_device(camera_device.id,name_by_user="My camera")
                    renamed={"cam":CameraInfo("cam","camera-one","Renamed","VMC2030B")}
                    await original_runtime.apply_inventory(hub,renamed)
                    updated=device_registry.async_get_device_by_identifier(("arlo_local","camera-one"),entry.entry_id)
                    self.assertEqual(updated.name,"Arlo Local Renamed")
                    self.assertEqual(updated.name_by_user,"My camera")
                    self.assertTrue(await hass.config_entries.async_reload(entry.entry_id))
                    await hass.async_block_till_done()
                    self.assertEqual(entry.state, ConfigEntryState.LOADED)
                    self.assertIsNone(original_runtime.broker.process)
                    self.assertIsNot(original_runtime, entry.runtime_data)
                    self.assertEqual(er.async_get(hass).async_get_entity_id("camera", "arlo_local", "test-hub_camera-one"), entity_id)
                    current_runtime = entry.runtime_data
                    from custom_components.arlo_local import async_remove_config_entry_device
                    from types import SimpleNamespace
                    self.assertFalse(await async_remove_config_entry_device(hass,entry,SimpleNamespace(identifiers=set())))
                    self.assertFalse(await async_remove_config_entry_device(hass,entry,SimpleNamespace(identifiers={("arlo_local","camera-one")},id="wrong")))
                    await current_runtime.apply_inventory(hub, cameras)
                    self.assertFalse(await async_remove_config_entry_device(hass, entry, hub_device))
                    self.assertFalse(await async_remove_config_entry_device(hass, entry, camera_device))
                    await current_runtime.apply_inventory(hub, {})
                    self.assertTrue(await async_remove_config_entry_device(hass, entry, camera_device))
                    device_registry.async_remove_device(camera_device.id)
                    await hass.async_block_till_done()
                    await current_runtime.apply_inventory(hub, cameras)
                    await hass.async_block_till_done()
                    self.assertIsNotNone(device_registry.async_get_device_by_identifier(("arlo_local", "camera-one"), entry.entry_id))
                    self.assertTrue(await hass.config_entries.async_unload(entry.entry_id))
                    self.assertEqual(entry.state, ConfigEntryState.NOT_LOADED)
                    self.assertIsNone(current_runtime.broker.process)
                    self.assertFalse(current_runtime.broker.relays)
                    self.assertTrue(await hass.config_entries.async_setup(entry.entry_id))
                    shutdown_runtime = entry.runtime_data
                finally:
                    await hass.async_stop(force=True)
                self.assertIsNone(shutdown_runtime.broker.process)
