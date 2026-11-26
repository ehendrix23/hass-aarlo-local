"""Native HA camera entities using the shared distributor, never raw hub URLs."""

import asyncio
from io import BytesIO

import av
from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.device_registry import async_get as async_get_device_registry
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from PIL import Image

from .const import DOMAIN
from .inventory import CameraInfo
from .runtime import Runtime

PARALLEL_UPDATES = 0


def fresh_image(url: str) -> Image.Image | None:
    # A snapshot consumes the existing local go2rtc producer.
    with av.open(
        url,
        mode="r",
        options={
            "rtsp_transport": "tcp",
            "analyzeduration": "100000",
            "probesize": "32768",
        },
        timeout=(6.0, 3.0),
    ) as stream:
        video = stream.streams.video[0]
        video.codec_context.thread_count = 2
        for frame in stream.decode(video):
            return Image.fromarray(frame.to_ndarray(format="rgb24"), mode="RGB")
    return None


def encode_jpeg(image: Image.Image, width: int | None, height: int | None) -> bytes:
    image = image.copy()
    if width or height:
        image.thumbnail((width or image.width, height or image.height))
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=85)
    return buffer.getvalue()


def fresh_jpeg(
    url: str, width: int | None = None, height: int | None = None
) -> bytes | None:
    image = fresh_image(url)
    return encode_jpeg(image, width, height) if image is not None else None


class ArloLocalCamera(Camera):
    _attr_supported_features = CameraEntityFeature.STREAM
    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_name = None
    _attr_brand = "Arlo"

    def __init__(self, runtime: Runtime, info: CameraInfo) -> None:
        super().__init__()
        self.runtime = runtime
        self.info = info
        self._capture_task: asyncio.Task[Image.Image | None] | None = None
        self._capture_waiters = 0
        self._attr_unique_id = runtime.hub.unique_id + "_" + info.unique_id
        self._attr_model = info.model
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, info.unique_id)},
            via_device_id=runtime.hub_registry_id,
            manufacturer="Arlo",
            name="Arlo Local " + info.name,
            model=info.model,
        )

    @property
    def available(self) -> bool:
        return (
            self.info.device_id in self.runtime.cameras
            and self.runtime.available
            and self.runtime.broker is not None
            and self.info.device_id in self.runtime.broker.relays
        )

    @callback
    def update_inventory_metadata(self, info: CameraInfo) -> None:
        """Refresh device metadata while preserving registry identity and user name."""
        if info == self.info:
            return
        self.info = info
        self._attr_model = info.model
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, info.unique_id)},
            via_device_id=self.runtime.hub_registry_id,
            manufacturer="Arlo",
            name="Arlo Local " + info.name,
            model=info.model,
        )
        hass = getattr(self, "hass", None)
        if hass is not None and getattr(self, "entity_id", None) is not None:
            registry = async_get_device_registry(hass)
            device = registry.async_get_device_by_identifier(
                (DOMAIN, info.unique_id), self.runtime.entry.entry_id
            )
            if device is not None:
                registry.async_update_device(
                    device.id, name="Arlo Local " + info.name, model=info.model
                )

    async def stream_source(self) -> str | None:
        return await self.runtime.source(self.info.device_id)

    async def async_prewarm(self, duration: int = 90) -> None:
        if not await self.runtime.prewarm(self.info.device_id, duration):
            raise HomeAssistantError(
                "Prewarm unavailable: capacity, connection or credential maintenance"
            )

    async def async_release_prewarm(self) -> None:
        await self.runtime.release_prewarm(self.info.device_id)

    async def _capture_image(self) -> Image.Image | None:
        for _ in range(2):
            if not self._capture_waiters:
                return None
            url = await self.stream_source()
            if not url:
                return None
            try:
                image = await self.hass.async_add_executor_job(fresh_image, url)
            except (av.error.FFmpegError, OSError, IndexError):
                continue
            if image is not None:
                return image
        return None

    def _capture_finished(self, task: asyncio.Task[Image.Image | None]) -> None:
        if self._capture_task is task:
            self._capture_task = None

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        self._capture_waiters += 1
        task = self._capture_task
        if task is None or task.done():
            task = self.hass.async_create_task(self._capture_image())
            self._capture_task = task
            task.add_done_callback(self._capture_finished)
        try:
            image = await asyncio.shield(task)
            if image is None or not self.available:
                return None
            jpeg = await self.hass.async_add_executor_job(
                encode_jpeg, image, width, height
            )
            return jpeg if self.available else None
        except (av.error.FFmpegError, OSError, IndexError):
            return None
        finally:
            self._capture_waiters -= 1


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry[Runtime],
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    runtime = entry.runtime_data
    entities: dict[str, ArloLocalCamera] = {}

    @callback
    def reconcile_entities() -> None:
        added = []
        for camera_id, info in runtime.cameras.items():
            if camera_id not in entities:
                entities[camera_id] = ArloLocalCamera(runtime, info)
                added.append(entities[camera_id])
            else:
                camera = entities[camera_id]
                camera.update_inventory_metadata(info)
                if (
                    getattr(camera, "hass", None) is not None
                    and getattr(camera, "entity_id", None) is not None
                ):
                    camera.async_write_ha_state()
        for camera_id, camera in entities.items():
            if (
                camera_id not in runtime.cameras
                and getattr(camera, "hass", None) is not None
                and getattr(camera, "entity_id", None) is not None
            ):
                camera.async_write_ha_state()
        if added:
            async_add_entities(added)

    runtime.listeners.append(reconcile_entities)

    @callback
    def forget_camera(uid: str) -> None:
        for camera_id, camera in list(entities.items()):
            if camera.info.unique_id == uid:
                entities.pop(camera_id)

    runtime.forget_camera_listener = forget_camera
    entry.async_on_unload(
        lambda: (
            runtime.listeners.remove(reconcile_entities)
            if reconcile_entities in runtime.listeners
            else None
        )
    )
    reconcile_entities()
