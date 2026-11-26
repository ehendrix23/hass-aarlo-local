"""Synthetic hub plus actual Home Assistant 2026.11.3 camera entity and runtime."""
import asyncio
import base64
from io import BytesIO
from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import re
import shutil
import tempfile
from types import MappingProxyType
from unittest.mock import patch

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntries,ConfigEntry
from homeassistant.helpers import issue_registry as ir
from homeassistant.components.camera import Camera,CameraEntityFeature
from PIL import Image

from custom_components.arlo_local import camera as platform
from custom_components.arlo_local import runtime as integration_runtime
from custom_components.arlo_local.inventory import Hub,CameraInfo
from custom_components.arlo_local.owner_session import OwnerSession,AuthRequired
from custom_components.arlo_local.storage import CredentialStore,serialize_inventory
from test_ffmpeg_transport import DecoderTest


class NativeRuntimeTests(DecoderTest):
    frame_count=150
    async def test_native_entities_shared_snapshot_stream_and_inventory(self):
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
        with tempfile.TemporaryDirectory() as conf:
            hass=HomeAssistant(conf)
            hass.config_entries=ConfigEntries(hass,{})
            await hass.config_entries.async_initialize()
            await ir.async_load(hass,load_empty=True)
            hub=Hub("hub-device","test-hub","Synthetic Hub","VMB5000")
            initial={"cam":CameraInfo("cam","camera-one","Camera 1","VMC2030B")}
            current=dict(initial)
            store=CredentialStore(Path(conf)/".arlo_local"/"teststore")
            generation=store.root/"arlo-cert-poc-synthetic"
            generation.mkdir(mode=0o700)
            shutil.copytree(Path(self.tmp.name)/"certs",generation/"certs")
            store.activate({"directory":generation.name,**serialize_inventory(hub,initial)})
            entry=ConfigEntry(domain="arlo_local",title="Arlo Local",version=1,minor_version=1,
                data={"host":"127.0.0.1","hostname":"hub.local","hub_id":"hub-device",
                      "storage_id":"teststore"},source="user",unique_id="hub-device",options={},
                discovery_keys=MappingProxyType({}),subentries_data=None)
            # Register the synthetic entry through HA's public entry manager.
            # The integration runtime is invoked explicitly below, not via
            # HA's unrelated HTTP and discovery dependency startup.
            with patch.object(ConfigEntries,"async_setup",return_value=True):
                await hass.config_entries.async_add(entry)
            rt=integration_runtime.Runtime(hass,entry)
            entry.runtime_data=rt
            counts=[]
            def inventory(*args):
                counts.append(1)
                return hub,dict(current)
            with patch.multiple(integration_runtime,PORT=self.port,INVENTORY_INTERVAL=0.2),patch.object(
                integration_runtime.shutil,"which",side_effect=lambda name: os.environ["ARLO_POC_GO2RTC"] if name=="go2rtc" else ffmpeg),patch.object(
                OwnerSession,"inventory",side_effect=inventory):
                try:
                    await rt.setup()
                    self.assertTrue(rt.available)
                    entities=[]
                    await platform.async_setup_entry(hass,entry,entities.extend)
                    self.assertEqual(len(entities),1)
                    entity=entities[0]
                    self.assertIsInstance(entity,Camera)
                    self.assertEqual(entity.supported_features,CameraEntityFeature.STREAM)
                    self.assertEqual(entity.unique_id,"test-hub_camera-one")
                    entity.hass=hass
                    url=await entity.stream_source()
                    self.assertEqual(url,await entity.stream_source())
                    # Startup probes use TLS but no video. Count only new sessions.
                    await asyncio.sleep(.05)
                    before=self.connections
                    async def viewer():
                        p=await asyncio.create_subprocess_exec(ffmpeg,"-nostdin","-v","error",
                            "-rtsp_transport","tcp","-analyzeduration","100000","-probesize","32768",
                            "-threads","2","-i",url,"-frames:v","3","-an","-pix_fmt","rgb24",
                            "-fps_mode","passthrough","-f","rawvideo","pipe:1",
                            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
                        try:
                            out,err=await asyncio.wait_for(p.communicate(),12)
                            self.assertEqual(p.returncode,0,err.decode())
                            self.assertEqual(len(out),3*160*120*3)
                        finally:
                            if p.returncode is None:p.kill();await p.communicate()
                    with patch.object(platform.av,"open",wraps=platform.av.open) as opened:
                        image,small,unused=await asyncio.gather(entity.async_camera_image(),entity.async_camera_image(80,60),viewer())
                    self.assertEqual(opened.call_count,1)
                    self.assertIsNotNone(image)
                    self.assertTrue(image.startswith(b"\xff\xd8"))
                    self.assertEqual(Image.open(BytesIO(image)).size,(160,120))
                    self.assertEqual(Image.open(BytesIO(small)).size,(80,60))
                    self.assertEqual(self.connections-before,1)
                    await asyncio.wait_for(self.finished.wait(),5)
                    # Periodic inventory refresh uses the real runtime and entity callback.
                    current["cam"]=CameraInfo("cam","camera-one","Renamed","VMC2030B")
                    current["cam2"]=CameraInfo("cam2","camera-two","New camera","VMC5040")
                    async with asyncio.timeout(5):
                        while len(entities)<2:await asyncio.sleep(.05)
                    self.assertTrue(entity.has_entity_name)
                    self.assertIsNone(entity.name)
                    self.assertEqual(entity.device_info.name,"Arlo Local Renamed")
                    self.assertEqual(await entity.stream_source(),url)
                    self.assertGreaterEqual(len(counts),2)
                    second=next(e for e in entities if e.info.device_id=="cam2")
                    self.assertTrue(second.available)
                    self.assertNotEqual(await second.stream_source(),url)
                    current.pop("cam")
                    async with asyncio.timeout(5):
                        while entity.available:await asyncio.sleep(.05)
                    self.assertIsNone(await entity.stream_source())
                    self.assertTrue(second.available)
                    self.assertEqual(set(rt.broker.relays),{"cam2"})
                    # Renew an expired-due generation only when the distributor
                    # can pause. A failed TLS probe must retain the active cache.
                    from custom_components.arlo_local.owner_session import InventoryUnavailable
                    active = rt.record
                    active["validated_utc"] = (datetime.now(UTC)-timedelta(days=8)).isoformat()
                    candidate_dir = store.root/"arlo-cert-poc-renewal"
                    candidate_dir.mkdir(mode=0o700)
                    shutil.copytree(Path(self.tmp.name)/"certs",candidate_dir/"certs")
                    candidate = {"directory": candidate_dir.name, **serialize_inventory(hub, {})}
                    due = {"validity": {"device": {"not_after": datetime.now(UTC).isoformat()}}}
                    # Avoid a background refresh between these controlled checks.
                    async with rt._refresh_lock:
                        with patch.object(CredentialStore,"inspect",return_value=due),patch.object(
                            OwnerSession,"provision",return_value=candidate) as provision:
                            with patch.object(rt.broker,"pause_if_idle",return_value=False):
                                await rt._renew_if_due({})
                                provision.assert_not_called()
                        original_inspect=CredentialStore.inspect
                        def inspect_due(self, record):
                            return due if record is active else original_inspect(self, record)
                        with patch.object(CredentialStore,"inspect",inspect_due),patch.object(
                            OwnerSession,"provision",return_value=candidate),patch.object(
                            integration_runtime,"probe_tls",side_effect=OSError("synthetic")):
                            await rt._renew_if_due({})
                            self.assertEqual(store.current()["directory"],active["directory"])
                        rt._renew_not_before=datetime.min.replace(tzinfo=UTC)
                        with patch.object(CredentialStore,"inspect",inspect_due),patch.object(
                            OwnerSession,"provision",return_value=candidate):
                            await rt._renew_if_due({})
                            self.assertEqual(store.current()["directory"],candidate_dir.name)
                            self.assertEqual(set(rt.cameras),{"cam2"})
                            self.assertEqual(store.current()["cameras"][0]["device_id"],"cam2")
                    with patch.object(OwnerSession,"inventory",side_effect=InventoryUnavailable("synthetic")):
                        self.assertFalse(await rt.refresh(force_login=True))
                        self.assertFalse(await rt.forget_stale_camera("camera-one"))
                        self.assertTrue(rt.available)
                    # Owner authentication failure must not invalidate the working local camera.
                    with patch.object(OwnerSession,"inventory",side_effect=AuthRequired("synthetic")),patch.object(
                        ConfigEntry,"async_start_reauth") as reauth:
                        self.assertFalse(await rt.refresh(force_login=True))
                        self.assertTrue(rt.available)
                        self.assertTrue(second.available)
                        self.assertTrue(reauth.called)
                    # Owned process recovery preserves the surviving consumer URL.
                    remaining=await second.stream_source()
                    rt.broker.process.kill()
                    await rt.broker.process.wait()
                    await rt.ensure_broker()
                    self.assertEqual(await second.stream_source(),remaining)
                    self.assertEqual(set(rt.broker.relays),{"cam2"})
                finally:
                    await rt.close()
                    await hass.async_stop(force=True)
                self.assertFalse(rt.broker.relays)
                self.assertIsNone(rt.broker.process)
