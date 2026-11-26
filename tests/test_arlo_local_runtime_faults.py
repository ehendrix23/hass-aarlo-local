"""Exercise runtime fault and recovery paths without contacting real Arlo devices."""
import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
import ssl
from tempfile import TemporaryDirectory
from types import MappingProxyType, SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from homeassistant.config_entries import ConfigEntries, ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from custom_components.arlo_local import runtime as mod
from custom_components.arlo_local.distribution import Distributor
from custom_components.arlo_local.inventory import Hub, CameraInfo
from custom_components.arlo_local.owner_session import OwnerSession, AuthRequired, InventoryUnavailable, IssuanceBlocked
from custom_components.arlo_local.storage import CredentialStore, atomic_json, serialize_inventory

class RuntimeFaultTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp=TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.hass=HomeAssistant(temp.name)
        self.hass.config_entries=ConfigEntries(self.hass,{})
        await self.hass.config_entries.async_initialize()
        await ir.async_load(self.hass,load_empty=True)
        self.entry=ConfigEntry(domain="arlo_local",title="Synthetic",version=1,minor_version=1,
            data={"host":"192.0.2.10","hostname":"hub.local","hub_id":"hub",
                  "storage_id":"test","username":"synthetic@example.invalid","password":"synthetic","mfa":"push",
                  "service_name":"Synthetic._arlo-video._tcp.local."},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(self.entry)
        self.rt=mod.Runtime(self.hass,self.entry)
        self.entry.runtime_data=self.rt
        self.rt.hub=Hub("hub","uid","Synthetic","VMB5000")
        self.cam=CameraInfo("cam","cam-uid","Camera","VMC2030B")
        self.rt.cameras={"cam":self.cam}
        self.rt.store=CredentialStore(Path(temp.name)/".arlo_local"/"test")
        self.rt.record={"directory":"arlo-cert-poc-synthetic",**serialize_inventory(self.rt.hub,self.rt.cameras)}
        atomic_json(self.rt.store.root/"active.json",self.rt.record)
        self.context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.pin=b"0"*32
        self.relay=SimpleNamespace(active=False,accepting=True,host=self.rt.host,hostname=self.rt.hostname,
                                   context=self.context,expected_pin=self.pin)
        self.broker=Mock(spec=Distributor)
        self.broker.healthy=True
        self.broker.relays={"cam":self.relay}
        self.broker.pause_if_idle.return_value=True
        self.broker.output_url.return_value="rtsp://127.0.0.1:5000/cam"
        self.broker.close=AsyncMock()
        self.broker.add=AsyncMock()
        self.broker.remove=AsyncMock()
        self.broker.start=AsyncMock()
        self.rt.broker=self.broker
        self.rt._tls_ok=True

    async def asyncTearDown(self):
        await self.rt.close()
        await self.hass.async_stop(force=True)

    def issue(self,key):
        return (mod.DOMAIN,self.entry.entry_id+"_"+key) in ir.async_get(self.hass).issues

    async def test_options_update_without_broker_and_with_broker(self):
        self.hass.config_entries.async_update_entry(self.entry,options={"max_streams":5})
        await self.rt.update_options(self.hass,self.entry)
        self.broker.resize_capacity.assert_called_once_with(5)
        self.rt.broker=None
        await self.rt.update_options(self.hass,self.entry)

    async def test_prewarm_respects_source_and_credential_maintenance(self):
        self.assertFalse(await self.rt.prewarm("unknown",90))
        with patch.object(self.rt,"source",new_callable=AsyncMock,return_value="rtsp://test"):
            self.rt.broker=None
            self.assertFalse(await self.rt.prewarm("cam",90))
            await self.rt.release_prewarm("cam")
            self.rt.broker=self.broker
            self.broker.prewarm=AsyncMock(return_value=True)
            self.broker.release_prewarm=AsyncMock()
            for report in ({"status":"invalid","validity":{}},{"status":"locally_usable_candidate","validity":{}},
                           {"status":"locally_usable_candidate","validity":{"peer":{"not_after":datetime.now(UTC).isoformat()}}}):
                with patch.object(self.rt.store,"inspect",return_value=report):
                    self.assertFalse(await self.rt.prewarm("cam",90))
            self.broker.prewarm.assert_not_awaited()
            far={"status":"locally_usable_candidate","validity":{"peer":{"not_after":(datetime.now(UTC)+timedelta(days=365)).isoformat()}}}
            with patch.object(self.rt.store,"inspect",return_value=far):
                self.assertTrue(await self.rt.prewarm("cam",90))
            self.broker.prewarm.assert_awaited_once_with("cam",90)
            await self.rt.release_prewarm("cam")
            self.broker.release_prewarm.assert_awaited_once_with("cam")

    async def test_setup_rejects_missing_cache_wrong_identity_and_missing_binaries(self):
        self.rt.broker=None
        original_record=self.rt.record
        (self.rt.store.root/"active.json").unlink()
        with self.assertRaisesRegex(ValueError,"No provisioned"):
            await self.rt.setup()
        record={**self.rt.record,"hub":{**self.rt.record["hub"],"device_id":"other"}}
        atomic_json(self.rt.store.root/"active.json",record)
        with self.assertRaisesRegex(ValueError,"identity mismatch"):
            await self.rt.setup()
        atomic_json(self.rt.store.root/"active.json",original_record)
        with patch.object(mod.shutil,"which",return_value=None):
            with self.assertRaisesRegex(RuntimeError,"go2rtc"):
                await self.rt.setup()
        with patch.object(mod.shutil,"which",side_effect=["/synthetic/go2rtc",None]):
            with self.assertRaisesRegex(RuntimeError,"FFmpeg"):
                await self.rt.setup()

    async def test_registration_failure_isolated_and_recovery(self):
        rt=self.rt
        rt.cameras["other"]=CameraInfo("other","other-uid","Other","VMC5040")
        rt.broker=None
        await rt._register_missing()
        rt.broker=self.broker
        self.broker.add.side_effect=OSError("synthetic")
        with self.assertLogs(mod.__name__,"WARNING") as log:
            await rt._register_missing()
            await rt._register_missing()
        self.assertEqual(len(log.output),1)
        self.assertEqual(rt._registration_failures,{"other"})
        async def add(camera_id):
            self.broker.relays[camera_id]=SimpleNamespace(active=False)
        self.broker.add.side_effect=add
        with self.assertLogs(mod.__name__,"INFO"):
            await rt._register_missing()
        self.assertEqual(rt._registration_failures,set())
        self.assertEqual(set(self.broker.relays),{"cam","other"})

    async def test_connection_state_transitions_and_idle_health(self):
        rt=self.rt
        rt.broker=None
        await rt.check_idle_connection()
        rt.broker=self.broker
        with patch.object(CredentialStore,"context",return_value=(self.context,self.pin)),patch.object(
            mod,"probe_tls",new_callable=AsyncMock) as probe:
            self.relay.active=True
            await rt.check_idle_connection()
            probe.assert_not_called()
            self.relay.active=False
            probe.side_effect=OSError("synthetic disconnect")
            with self.assertLogs(mod.__name__,"WARNING") as log:
                await rt.check_idle_connection()
                await rt.check_idle_connection()
            self.assertEqual(len(log.output),1)
            self.assertFalse(rt.available)
            self.assertTrue(self.issue("local_connection"))
            probe.side_effect=None
            with self.assertLogs(mod.__name__,"INFO"):
                await rt.check_idle_connection()
            self.assertTrue(rt.available)
            self.assertFalse(self.issue("local_connection"))

    async def test_discovery_rejects_invalid_records_and_updates_known_hub(self):
        rt=self.rt
        original=rt.host
        service=SimpleNamespace(async_request=AsyncMock(return_value=True),
            properties={b"deviceid":b"hub"},port=554,
            parsed_addresses=lambda:["192.0.2.99"],server="NewHub.local.")
        with patch.object(mod,"async_get_async_instance",new_callable=AsyncMock,return_value=SimpleNamespace(zeroconf=object())),patch.object(
            mod,"AsyncServiceInfo",return_value=service):
            for update in (
                {"properties":{}},{"properties":{b"deviceid":b"other"}},
                {"properties":{b"deviceid":b"\xff"}},
                {"port":555},{"parsed_addresses":lambda:["8.8.8.8"]},
                {"server":"untrusted.example"},
            ):
                defaults={k:getattr(service,k) for k in update}
                for k,v in update.items():setattr(service,k,v)
                await rt.resolve_hub()
                self.assertEqual(rt.host,original)
                for k,v in defaults.items():setattr(service,k,v)
            service.async_request.return_value=False
            await rt.resolve_hub()
            self.assertEqual(rt.host,original)
            service.async_request.return_value=True
            await rt.resolve_hub()
            self.assertEqual(rt.host,"192.0.2.99")
            self.assertEqual(rt.hostname,"NewHub.local")
            self.assertEqual(self.relay.host,rt.host)
            self.assertEqual(self.relay.hostname,rt.hostname)

    async def test_source_and_inventory_reject_invalid_state(self):
        rt=self.rt
        self.assertIsNone(await rt.source("absent"))
        with self.assertRaises(ValueError):
            await rt.apply_inventory(Hub("other","uid","Other","VMB5000"),{})
        with self.assertRaises(ValueError):
            await rt.apply_inventory(rt.hub,{"cam":CameraInfo("cam","different","Other","VMC2030B")})
        self.assertEqual(rt.cameras,{"cam":self.cam})
        self.assertFalse(await rt.forget_stale_camera("absent"))
        with patch.object(rt,"ensure_broker",side_effect=OSError("synthetic")):
            self.assertIsNone(await rt.source("cam"))
            self.broker.healthy=False
            self.assertIsNone(await rt.source("cam"))
            self.assertFalse(rt.available)
        rt.broker=None
        await rt.apply_inventory(rt.hub,{})
        self.assertTrue(await rt.forget_stale_camera("cam-uid"))
        self.assertEqual(rt.store.current()["cameras"],[])

    async def test_runtime_renewal_and_auth_failure_preserve_existing_state(self):
        rt=self.rt
        self.assertFalse(await rt.refresh())
        rt.owner=Mock(spec=OwnerSession)
        rt.owner.close=Mock()
        rt.owner.inventory.side_effect=AuthRequired("synthetic")
        with patch.object(ConfigEntry,"async_start_reauth") as reauth:
            self.assertFalse(await rt.refresh(force_login=True))
            reauth.assert_called_once_with(self.hass)
            self.assertFalse(await rt.refresh())
        self.assertTrue(rt.available)
        rt.owner.inventory.side_effect=InventoryUnavailable("synthetic")
        self.assertFalse(await rt.refresh(force_login=True))
        self.assertTrue(self.issue("inventory"))
        rt.owner.inventory.side_effect=None
        rt.owner.inventory.return_value=(rt.hub,rt.cameras)
        far={"status":"locally_usable_candidate","validity":{"peer":{"not_after":(datetime.now(UTC)+timedelta(days=365)).isoformat()}}}
        with patch.object(CredentialStore,"inspect",return_value=far):
            self.assertTrue(await rt.refresh(force_login=True))
        self.assertFalse(self.issue("inventory"))
        self.assertFalse(rt._auth_blocked)
        rt.owner.provision.assert_not_called()
        rt.owner.inventory.return_value=(Hub("hub","unexpected","Other","VMB5000"),{})
        with self.assertRaises(InventoryUnavailable):
            await rt.validate_reconfigure({},rt.host,rt.hostname)
        rt.owner.inventory.return_value=(rt.hub,rt.cameras)
        with patch.object(CredentialStore,"context",return_value=(self.context,self.pin)),patch.object(
            mod,"probe_tls",new_callable=AsyncMock) as probe:
            await rt.validate_reconfigure({},"192.0.2.100","hub.local")
            probe.assert_awaited_once()
        # A failed reconfigure does not mutate the active endpoint.
        self.assertEqual(rt.host,"192.0.2.10")

    async def test_health_tick_and_shutdown_guards(self):
        rt=self.rt
        valid={"status":"locally_usable_candidate"}
        invalid={"status":"invalid"}
        with patch.object(rt,"resolve_hub",new_callable=AsyncMock) as resolve,patch.object(
            rt,"check_idle_connection",new_callable=AsyncMock) as check,patch.object(
            rt,"ensure_broker",new_callable=AsyncMock) as ensure,patch.object(
            CredentialStore,"inspect",return_value=valid):
            await rt._health_tick(1)
            ensure.assert_not_called()
            await rt._health_tick(20)
            resolve.assert_awaited_once()
            check.assert_awaited_once()
            ensure.assert_not_called()
        with patch.object(CredentialStore,"inspect",return_value=invalid),patch.object(
            rt,"resolve_hub",new_callable=AsyncMock),patch.object(rt,"check_idle_connection",new_callable=AsyncMock),patch.object(
            rt,"ensure_broker",side_effect=OSError("synthetic")):
            await rt._health_tick(20)
        self.assertFalse(rt.available)
        self.assertTrue(self.issue("certificate"))
        rt._stopping=True
        self.assertFalse(await rt.refresh(force_login=True))
        self.assertIsNone(await rt.source("cam"))
        self.assertFalse(await rt.forget_stale_camera("absent"))
        with self.assertRaises(InventoryUnavailable):
            await rt.validate_reconfigure({},rt.host,rt.hostname)
        await rt.ensure_broker()
        await rt._health_loop()
        await rt._inventory_loop()

    async def test_entrypoint_failed_setup_cancel_and_failed_unload(self):
        from custom_components.arlo_local import async_setup_entry,async_unload_entry
        from homeassistant.exceptions import ConfigEntryNotReady
        with patch.object(mod.Runtime,"setup",side_effect=OSError("synthetic")),patch.object(
            mod.Runtime,"close",new_callable=AsyncMock) as close:
            with self.assertRaises(ConfigEntryNotReady):
                await async_setup_entry(self.hass,self.entry)
            close.assert_awaited_once()
        with patch.object(mod.Runtime,"setup",side_effect=asyncio.CancelledError),patch.object(
            mod.Runtime,"close",new_callable=AsyncMock) as close:
            with self.assertRaises(asyncio.CancelledError):
                await async_setup_entry(self.hass,self.entry)
            close.assert_awaited_once()
        with patch.object(ConfigEntries,"async_unload_platforms",return_value=False),patch.object(
            self.rt,"close",new_callable=AsyncMock) as close:
            self.assertFalse(await async_unload_entry(self.hass,self.entry))
            close.assert_not_called()

    async def test_renewal_rejects_changed_identity_and_handles_absent_owner(self):
        rt=self.rt
        rt.record["validated_utc"]=(datetime.now(UTC)-timedelta(days=8)).isoformat()
        due={"status":"locally_usable_candidate","validity":{"device":{"not_after":datetime.now(UTC).isoformat()}}}
        with patch.object(CredentialStore,"inspect",return_value=due):
            await rt._renew_if_due({})
            self.assertFalse(self.issue("certificate"))
            rt.owner=Mock(spec=OwnerSession)
            rt.owner.close=Mock()
            self.broker.pause_if_idle.return_value=False
            await rt._renew_if_due({})
            rt.owner.provision.assert_not_called()
            self.broker.pause_if_idle.return_value=True
            rt.owner.provision.return_value={"directory":"arlo-cert-poc-wrong",**serialize_inventory(
                Hub("hub","wrong","Wrong","VMB5000"),{})}
            await rt._renew_if_due({})
            self.assertTrue(self.issue("certificate"))
            self.broker.resume.assert_called_once()
            self.assertEqual(rt.store.current()["hub"]["unique_id"],"uid")
            rt._renew_not_before=datetime.min.replace(tzinfo=UTC)
            rt.owner.provision.side_effect=AuthRequired("synthetic")
            with patch.object(ConfigEntry,"async_start_reauth") as reauth:
                await rt._renew_if_due({})
                self.assertTrue(rt._auth_blocked)
                reauth.assert_called_once()

    async def test_broker_absence_and_source_races(self):
        rt=self.rt
        rt.broker=None
        with patch.object(CredentialStore,"context",return_value=(self.context,self.pin)):
            with self.assertRaisesRegex(RuntimeError,"binaries unavailable"):await rt.ensure_broker()
        async def removed():
            rt.cameras.clear()
        with patch.object(rt,"ensure_broker",side_effect=removed):
            self.assertIsNone(await rt.source("cam"))
        rt.cameras={"cam":self.cam}
        with patch.object(rt,"ensure_broker",new_callable=AsyncMock):
            self.assertIsNone(await rt.source("cam"))
        rt.broker=self.broker
        self.broker.healthy=False
        await rt.apply_inventory(rt.hub,{"cam":self.cam,"other":CameraInfo("other","other-uid","Other","VMC5040")})
        self.broker.add.assert_not_called()
        self.broker.remove.side_effect=OSError("synthetic")
        await rt.apply_inventory(rt.hub,{})
        self.assertEqual(rt.cameras,{})
