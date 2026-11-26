"""Camera entity and snapshot failure contracts."""
import asyncio
from io import BytesIO
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch
import numpy as np
from PIL import Image

from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from custom_components.arlo_local.camera import ArloLocalCamera, fresh_jpeg, async_setup_entry
from custom_components.arlo_local.inventory import Hub, CameraInfo
from custom_components.arlo_local.runtime import Runtime


class CameraFaultTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp=TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.hass=HomeAssistant(tmp.name)
        dr.async_setup(self.hass)
        await dr.async_load(self.hass,load_empty=True)
        self.cam=CameraInfo("cam","uid","Camera","VMC2030B")
        self.rt=Mock(spec=Runtime)
        self.rt.hub=Hub("hub","hub-uid","Hub","VMB5000")
        self.rt.hub_registry_id=None
        self.rt.entry=SimpleNamespace(entry_id="synthetic")
        self.rt.cameras={"cam":self.cam}
        self.rt.available=True
        self.rt.broker=SimpleNamespace(relays={"cam":object()})
        self.rt.source=AsyncMock(return_value=None)
        self.rt.listeners=[]

    async def asyncTearDown(self):
        await self.hass.async_stop(force=True)

    async def test_no_source_or_decode_failure_never_returns_stale_image(self):
        camera=ArloLocalCamera(self.rt,self.cam)
        camera.hass=self.hass
        self.assertIsNone(await camera.async_camera_image())
        self.rt.source.return_value="rtsp://127.0.0.1:1/synthetic"
        with patch("custom_components.arlo_local.camera.av.open",side_effect=OSError("synthetic")) as opened:
            self.assertIsNone(await camera.async_camera_image())
            self.assertEqual(opened.call_count, 2)
        stream=Mock()
        stream.streams.video=[Mock()]
        stream.decode.return_value=iter(())
        manager=Mock()
        manager.__enter__=Mock(return_value=stream)
        manager.__exit__=Mock(return_value=False)
        with patch("custom_components.arlo_local.camera.av.open",return_value=manager):
            self.assertIsNone(fresh_jpeg("rtsp://127.0.0.1:1/synthetic"))
        manager.__exit__.assert_called_once()

    async def test_snapshot_retries_initial_decode_once_then_requests_fresh_frame(self):
        camera=ArloLocalCamera(self.rt,self.cam)
        camera.hass=self.hass
        self.rt.source.return_value="rtsp://127.0.0.1:1/synthetic"
        frame=Mock()
        frame.to_ndarray.return_value=np.zeros((120,160,3),dtype=np.uint8)
        stream=Mock()
        stream.streams.video=[Mock()]
        stream.decode.return_value=iter([frame])
        manager=Mock()
        manager.__enter__=Mock(return_value=stream)
        manager.__exit__=Mock(return_value=False)
        with patch("custom_components.arlo_local.camera.av.open",side_effect=[OSError("initial startup failure"),manager]) as opened:
            jpeg=await camera.async_camera_image(width=80,height=60)
            self.assertIsNotNone(jpeg)
            self.assertEqual(Image.open(BytesIO(jpeg)).size,(80,60))
            self.assertEqual(opened.call_count,2)
        with patch("custom_components.arlo_local.camera.av.open",side_effect=OSError("new request failed")) as opened:
            self.assertIsNone(await camera.async_camera_image(width=80,height=60))
            self.assertEqual(opened.call_count,2)

    async def test_cancelled_snapshot_waiter_does_not_cancel_shared_capture(self):
        camera=ArloLocalCamera(self.rt,self.cam)
        camera.hass=self.hass
        entered=asyncio.Event()
        release=asyncio.Event()
        async def source(_device):
            entered.set()
            await release.wait()
            return "rtsp://127.0.0.1:1/synthetic"
        self.rt.source.side_effect=source
        with patch("custom_components.arlo_local.camera.fresh_image",return_value=Image.new("RGB",(160,120))) as decode:
            first=asyncio.create_task(camera.async_camera_image())
            await asyncio.wait_for(entered.wait(),2)
            second=asyncio.create_task(camera.async_camera_image(80,60))
            await asyncio.sleep(0)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):await first
            release.set()
            jpeg=await asyncio.wait_for(second,3)
            self.assertIsNotNone(jpeg)
            self.assertEqual(Image.open(BytesIO(jpeg)).size,(80,60))
            self.assertEqual(decode.call_count,1)
        self.assertIsNone(camera._capture_task)

    async def test_abandoned_capture_does_not_retry(self):
        camera=ArloLocalCamera(self.rt,self.cam)
        camera.hass=self.hass
        self.rt.source.return_value="rtsp://127.0.0.1:1/synthetic"
        entered=asyncio.Event()
        release=asyncio.Event()
        async def executor(*args):
            entered.set()
            await release.wait()
            raise OSError("synthetic")
        with patch.object(self.hass,"async_add_executor_job",side_effect=executor) as decode:
            waiter=asyncio.create_task(camera.async_camera_image())
            await asyncio.wait_for(entered.wait(),2)
            capture=camera._capture_task
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):await waiter
            release.set()
            self.assertIsNone(await asyncio.wait_for(capture,3))
            self.assertEqual(decode.call_count,1)
        self.assertIsNone(camera._capture_task)

    async def test_metadata_change_with_missing_registry_device_and_forget_nonmatching(self):
        camera=ArloLocalCamera(self.rt,self.cam)
        camera.hass=self.hass
        camera.entity_id="camera.synthetic"
        camera.update_inventory_metadata(CameraInfo("cam","uid","Renamed","VMC2030B"))
        self.assertEqual(camera.device_info.name,"Arlo Local Renamed")
        entities=[]
        entry=SimpleNamespace(runtime_data=self.rt,async_on_unload=Mock())
        await async_setup_entry(self.hass,entry,entities.extend)
        self.assertEqual(len(entities),1)
        self.rt.forget_camera_listener("not-the-camera")
        self.rt.listeners[0]()
        self.assertEqual(len(entities),1)
