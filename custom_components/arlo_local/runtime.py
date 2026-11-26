"""HA-facing inventory, certificate, and owned-distributor lifecycle."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import shutil
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiohttp
from homeassistant.components.zeroconf import async_get_async_instance
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from zeroconf.asyncio import AsyncServiceInfo

from .budget import stream_budget
from .const import DOMAIN, INVENTORY_INTERVAL, PORT, RENEW_DAYS, SERVICE_TYPE
from .distribution import Distributor
from .inventory import CameraInfo, Hub, reconcile
from .manual_mfa import ManualMFA
from .owner_session import (
    AuthRequired,
    InventoryUnavailable,
    IssuanceBlocked,
    OwnerSession,
)
from .rtsp_transport import probe_tls
from .storage import CredentialRecord, CredentialStore, deserialize_inventory

_LOGGER = logging.getLogger(__name__)
LOCAL_ERRORS = (OSError, RuntimeError, ValueError, TypeError, aiohttp.ClientError)


class Runtime:
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry[Runtime]) -> None:
        self.hass = hass
        self.entry = entry
        self.host: str = entry.data["host"]
        self.hostname: str = entry.data["hostname"]
        self.store: CredentialStore
        self.owner: OwnerSession | None = None
        self.record: CredentialRecord
        self.hub: Hub
        self.hub_registry_id: str | None = None
        self.cameras: dict[str, CameraInfo] = {}
        self.broker: Distributor | None = None
        self.listeners: list[Callable[[], None]] = []
        self.forget_camera_listener: Callable[[str], None] | None = None
        self._lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._stopping = False
        self._tasks: list[asyncio.Task[None]] = []
        self._tls_ok = False
        self._renew_not_before = datetime.min.replace(tzinfo=UTC)
        self._binary: str | None = None
        self._ffmpeg_binary: str | None = None
        self._auth_blocked = False
        self._reported_connection: bool | None = None
        self._registration_failures: set[str] = set()
        self._inventory_verified = False

    @property
    def available(self) -> bool:
        return bool(
            not self._stopping and self._tls_ok and self.broker and self.broker.healthy
        )

    async def setup(self) -> None:
        root = Path(self.hass.config.path(".arlo_local", self.entry.data["storage_id"]))
        self.store = await self.hass.async_add_executor_job(CredentialStore, root)
        self.owner = OwnerSession(self.store)
        record = await self.hass.async_add_executor_job(self.store.current)
        if record is None:
            raise ValueError("No provisioned Arlo Local cache")
        self.record = record
        self.hub, self.cameras = deserialize_inventory(self.record)
        if self.hub.device_id != self.entry.unique_id:
            raise ValueError("Cached hub identity mismatch")
        self._binary = await self.hass.async_add_executor_job(shutil.which, "go2rtc")
        if not self._binary:
            raise RuntimeError("go2rtc binary not installed")
        self._ffmpeg_binary = await self.hass.async_add_executor_job(
            shutil.which, "ffmpeg"
        )
        if not self._ffmpeg_binary:
            raise RuntimeError("FFmpeg binary not installed")
        await self.resolve_hub()
        try:
            await self.ensure_broker()
        except LOCAL_ERRORS:
            self._connection_state(False)
        self._tasks = [
            self.hass.async_create_background_task(
                self._health_loop(), "Arlo Local health"
            ),
            self.hass.async_create_background_task(
                self._inventory_loop(), "Arlo Local inventory"
            ),
        ]

    def _issue(self, suffix: str, key: str) -> None:
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self.entry.entry_id + "_" + suffix,
            config_entry_id=self.entry.entry_id,
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key=key,
        )

    def _clear(self, suffix: str) -> None:
        ir.async_delete_issue(self.hass, DOMAIN, self.entry.entry_id + "_" + suffix)

    def notify(self) -> None:
        for callback in list(self.listeners):
            callback()

    def _connection_state(self, connected: bool) -> None:
        """Publish hub transport state and log only transitions."""
        self._tls_ok = connected
        if self._reported_connection is connected:
            return
        previous = self._reported_connection
        self._reported_connection = connected
        if connected:
            self._clear("local_connection")
            if previous is False:
                _LOGGER.info("Arlo Local SmartHub connection recovered")
        else:
            self._issue("local_connection", "local_connection")
            _LOGGER.warning("Arlo Local SmartHub connection unavailable")
        self.notify()

    async def _register_missing(self) -> None:
        """Register missing cameras independently without disrupting existing ones."""
        broker = self.broker
        if broker is None:
            return
        for camera_id in self.cameras:
            if camera_id in broker.relays:
                continue
            try:
                await broker.add(camera_id)
            except LOCAL_ERRORS as exc:
                if camera_id not in self._registration_failures:
                    _LOGGER.warning(
                        "Arlo Local camera stream registration failed (%s)",
                        type(exc).__name__,
                    )
                    self._registration_failures.add(camera_id)
            else:
                if camera_id in self._registration_failures:
                    _LOGGER.info("Arlo Local camera stream registration recovered")
                    self._registration_failures.discard(camera_id)

    async def check_idle_connection(self) -> None:
        """Verify the cached mutual TLS connection without waking any camera."""
        async with self._lock:
            if self._stopping or self.broker is None or not self.broker.healthy:
                return
            if any(relay.active for relay in self.broker.relays.values()):
                return
            context, pin = await self._executor(self.store.context, self.record)
            try:
                await probe_tls(self.host, PORT, self.hostname, context, pin)
            except LOCAL_ERRORS:
                self._connection_state(False)
            else:
                self._connection_state(True)

    async def resolve_hub(self) -> None:
        name = self.entry.data.get("service_name")
        if not name:
            return
        try:
            zc = await async_get_async_instance(self.hass)
            service = AsyncServiceInfo(SERVICE_TYPE, name)
            if not await service.async_request(zc.zeroconf, 3000):
                return
            raw = service.properties.get(b"deviceid")
            if (
                not isinstance(raw, bytes)
                or raw.decode("ascii") != self.hub.device_id
                or service.port != PORT
            ):
                return
            addresses = service.parsed_addresses()
            address = next(
                (
                    a
                    for a in addresses
                    if ipaddress.ip_address(a).version == 4
                    and ipaddress.ip_address(a).is_private
                ),
                None,
            )
            if not address:
                return
            server = service.server
            if not server or not server.endswith(".local."):
                return
            async with self._lock:
                self.host, self.hostname = address, server.rstrip(".")
                if self.broker:
                    self.broker.host, self.broker.hostname = self.host, self.hostname
                    for relay in self.broker.relays.values():
                        relay.host, relay.hostname = self.host, self.hostname
        except (TimeoutError, OSError, ValueError, UnicodeError):
            return

    async def ensure_broker(self) -> None:
        async with self._lock:
            if self._stopping:
                return
            if self.broker and self.broker.healthy and self._tls_ok:
                await self._register_missing()
                return
            context, pin = await self.hass.async_add_executor_job(
                self.store.context, self.record
            )
            # Never replace an active distributor with another instance.
            if self.broker is None:
                if self._binary is None or self._ffmpeg_binary is None:
                    raise RuntimeError("Distributor binaries unavailable")
                self.broker = Distributor(
                    self._binary,
                    self.host,
                    self.hostname,
                    context,
                    pin,
                    port=PORT,
                    ffmpeg_binary=self._ffmpeg_binary,
                    max_streams=stream_budget(self.entry.options),
                )
            else:
                self.broker.context, self.broker.pin = context, pin
                for relay in self.broker.relays.values():
                    relay.context, relay.expected_pin = context, pin
            await probe_tls(self.host, PORT, self.hostname, context, pin)
            await self.broker.start()
            await self._register_missing()
            self._connection_state(True)
            self.notify()

    async def update_options(
        self, hass: HomeAssistant, entry: ConfigEntry[Runtime]
    ) -> None:
        """Apply local streaming options without restart or owner authentication."""
        async with self._lock:
            if self.broker is not None:
                self.broker.resize_capacity(stream_budget(entry.options))

    async def source(self, camera_id: str, *, native: bool = False) -> str | None:
        if camera_id not in self.cameras or self._stopping:
            return None
        try:
            await self.ensure_broker()
            async with self._lock:
                if camera_id not in self.cameras or self._stopping:
                    return None
                if self.broker is None:
                    return None
                return (
                    self.broker.native_url(camera_id)
                    if native
                    else self.broker.output_url(camera_id)
                )
        except LOCAL_ERRORS:
            if not self.broker or not self.broker.healthy:
                self._connection_state(False)
            self.notify()
            return None

    async def prewarm(self, camera_id: str, duration: float) -> bool:
        """Warm a selected camera only when credentials are not due for renewal."""
        if await self.source(camera_id) is None:
            return False
        async with self._lock:
            if not self.available or not self.broker or camera_id not in self.cameras:
                return False
            report = await self._executor(self.store.inspect, self.record)
            values = report.get("validity", {})
            if report.get("status") != "locally_usable_candidate" or not values:
                return False
            if min(
                datetime.fromisoformat(v["not_after"]) for v in values.values()
            ) <= datetime.now(UTC) + timedelta(days=RENEW_DAYS):
                return False
            return await self.broker.prewarm(camera_id, duration)

    async def release_prewarm(self, camera_id: str) -> None:
        async with self._lock:
            if self.broker:
                await self.broker.release_prewarm(camera_id)

    async def apply_inventory(self, hub: Hub, cameras: dict[str, CameraInfo]) -> None:
        self._inventory_verified = False
        if hub.unique_id != self.hub.unique_id or hub.device_id != self.hub.device_id:
            raise ValueError("Unexpected hub identity")
        async with self._lock:
            delta = reconcile(self.cameras, cameras)
            if any(
                self.cameras[k].unique_id != cameras[k].unique_id for k in delta.changed
            ):
                raise ValueError("Camera identity reassignment requires review")
            self.record = await self.hass.async_add_executor_job(
                self.store.update_inventory, hub, cameras
            )
            self.hub, self.cameras = hub, cameras
            self._inventory_verified = True
            if self.broker:
                for camera_id in delta.removed:
                    try:
                        await self.broker.remove(camera_id)
                    except LOCAL_ERRORS as exc:
                        _LOGGER.warning(
                            "Arlo Local camera stream removal failed (%s)",
                            type(exc).__name__,
                        )
                    self._registration_failures.discard(camera_id)
                if self.broker.healthy:
                    await self._register_missing()
            self.notify()

    async def forget_stale_camera(self, uid: str) -> bool:
        async with self._lock:
            if (
                self._stopping
                or not self._inventory_verified
                or uid == self.hub.unique_id
                or any(cam.unique_id == uid for cam in self.cameras.values())
            ):
                return False
            if self.forget_camera_listener is not None:
                self.forget_camera_listener(uid)
            return True

    async def _executor[T, *Ts](self, func: Callable[[*Ts], T], *args: *Ts) -> T:
        # Do not abandon a worker that may be completing an issuance.
        task = asyncio.ensure_future(self.hass.async_add_executor_job(func, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def refresh(
        self,
        *,
        force_login: bool = False,
        config: Mapping[str, str] | None = None,
        manual_mfa: ManualMFA | None = None,
    ) -> bool:
        async with self._refresh_lock:
            if self._stopping:
                return False
            if self._auth_blocked and not force_login:
                return False
            cfg: dict[str, str] = dict(config or self.entry.data)
            if self.owner is None:
                return False
            try:
                hub, cameras = await self._executor(
                    self.owner.inventory,
                    cfg,
                    self.hub.device_id,
                    force_login,
                    manual_mfa,
                )
            except AuthRequired:
                self._inventory_verified = False
                self._auth_blocked = True
                self.entry.async_start_reauth(self.hass)
                return False
            except InventoryUnavailable:
                self._inventory_verified = False
                self._issue("inventory", "inventory")
                return False
            await self.apply_inventory(hub, cameras)
            self._auth_blocked = False
            self._clear("inventory")
            await self._renew_if_due(cfg, manual_mfa)
            return True

    async def validate_reconfigure(
        self,
        config: Mapping[str, str],
        host: str,
        hostname: str,
        manual_mfa: ManualMFA | None = None,
    ) -> None:
        """Validate owner and pinned local endpoint before updating config."""
        async with self._refresh_lock:
            if self._stopping or self.owner is None:
                raise InventoryUnavailable(
                    "Integration is not available for reconfiguration"
                )
            hub, _ = await self._executor(
                self.owner.inventory, config, self.hub.device_id, True, manual_mfa
            )
            if hub.unique_id != self.hub.unique_id:
                raise InventoryUnavailable("Configured hub identity changed")
            async with self._lock:
                context, pin = await self._executor(self.store.context, self.record)
                await probe_tls(host, PORT, hostname, context, pin)

    async def _renew_if_due(
        self, cfg: Mapping[str, str], manual_mfa: ManualMFA | None = None
    ) -> None:
        # Review before issuance. Existing valid credentials remain authoritative.
        if datetime.now(UTC) < self._renew_not_before:
            return
        report = await self.hass.async_add_executor_job(self.store.inspect, self.record)
        values = report.get("validity", {})
        due = not values or min(
            datetime.fromisoformat(v["not_after"]) for v in values.values()
        ) <= datetime.now(UTC) + timedelta(days=RENEW_DAYS)
        if not due:
            return
        validated = self.record.get("validated_utc")
        if validated and datetime.fromisoformat(validated) > datetime.now(
            UTC
        ) - timedelta(days=7):
            self._issue("certificate", "certificate")
            return
        async with self._lock:
            if self.owner is None:
                return
            if self.broker and not self.broker.pause_if_idle():
                return
            # Defer actual renewal when a viewer connects. A new candidate does
            # not replace active material until the new TLS identity is verified.
            self._renew_not_before = datetime.now(UTC) + timedelta(days=1)
            try:
                candidate = await self._executor(
                    self.owner.provision, cfg, self.hub.device_id, False, manual_mfa
                )
                hub, _ = deserialize_inventory(candidate)
                if (
                    hub.unique_id != self.hub.unique_id
                    or hub.device_id != self.hub.device_id
                ):
                    raise IssuanceBlocked("Renewal hub identity changed")
                candidate["hub"] = self.record["hub"]
                candidate["cameras"] = self.record["cameras"]
                context, pin = await self.hass.async_add_executor_job(
                    self.store.context, candidate
                )
                for _ in range(2):
                    await probe_tls(self.host, PORT, self.hostname, context, pin)
                self.record = await self.hass.async_add_executor_job(
                    self.store.activate, candidate
                )
                if self.broker:
                    self.broker.context, self.broker.pin = context, pin
                    for r in self.broker.relays.values():
                        r.context, r.expected_pin = context, pin
                self._clear("certificate")
            except AuthRequired:
                self._auth_blocked = True
                self.entry.async_start_reauth(self.hass)
            except (TimeoutError, IssuanceBlocked, ValueError, OSError, RuntimeError):
                self._issue("certificate", "certificate")
            finally:
                if self.broker:
                    self.broker.resume()

    async def _health_loop(self) -> None:
        counter = 0
        while not self._stopping:
            await asyncio.sleep(15)
            counter += 1
            await self._health_tick(counter)

    async def _health_tick(self, counter: int) -> None:
        """One health cycle, independent of the scheduler."""
        try:
            if counter % 20 == 0:
                await self.resolve_hub()
            if counter % 4 == 0:
                await self.check_idle_connection()
            if counter % 20 == 0:
                report = await self.hass.async_add_executor_job(
                    self.store.inspect, self.record
                )
                if report["status"] != "locally_usable_candidate":
                    self._connection_state(False)
                    self._issue("certificate", "certificate")
            if (
                not self.available
                or self.broker is None
                or any(k not in self.broker.relays for k in self.cameras)
            ):
                await self.ensure_broker()
                self.notify()
        except LOCAL_ERRORS:
            self._connection_state(False)

    async def _inventory_loop(self) -> None:
        # Use the persisted cache immediately; refresh in background.
        while not self._stopping:
            try:
                await self.refresh()
            except (
                InventoryUnavailable,
                IssuanceBlocked,
                AuthRequired,
                *LOCAL_ERRORS,
            ) as exc:
                self._inventory_verified = False
                _LOGGER.debug(
                    "Arlo Local inventory refresh failed (%s)", type(exc).__name__
                )
                self._issue("inventory", "inventory")
            await asyncio.sleep(INVENTORY_INTERVAL)

    async def close(self) -> None:
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        async with self._refresh_lock, self._lock:
            if self.broker:
                await self.broker.close()
            if self.owner:
                await self._executor(self.owner.close)
        self.listeners.clear()
        self.forget_camera_listener = None
