"""Use the real HA config-entry manager to test discovery and native Repairs."""
import asyncio
import ipaddress
from pathlib import Path
import shutil
import tempfile
from types import MappingProxyType,SimpleNamespace
import unittest
from unittest.mock import AsyncMock,patch

from homeassistant.core import HomeAssistant
from homeassistant import loader
from homeassistant.config_entries import ConfigEntries,ConfigEntry
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo
from custom_components.arlo_local.config_flow import ArloLocalConfigFlow, credential_config, validate_endpoint
from custom_components.arlo_local.owner_session import AuthRequired, InventoryUnavailable, IssuanceBlocked, OwnerSession
from custom_components.arlo_local.inventory import Hub, CameraInfo
from custom_components.arlo_local.storage import CredentialStore, serialize_inventory, atomic_json
from custom_components.arlo_local.runtime import Runtime

class ConfigFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name)
        shutil.copytree(Path(__file__).resolve().parents[1]/"custom_components"/"arlo_local",
            root/"custom_components"/"arlo_local",ignore=shutil.ignore_patterns("__pycache__","*.pyc"))
        self.hass=HomeAssistant(str(root))
        loader.async_setup(self.hass)
        # Load the custom flow with HA's real loader; avoid starting the
        # unrelated HTTP server and mDNS advertiser in this isolated test.
        integration=await loader.async_get_integration(self.hass,"arlo_local")
        await integration.async_get_platform("config_flow")
        self.hass.config_entries=ConfigEntries(self.hass,{})
        await self.hass.config_entries.async_initialize()
        await ir.async_load(self.hass,load_empty=True)
    async def asyncTearDown(self):
        await self.hass.async_stop(force=True)
    async def test_discovered_hub_form_and_duplicate(self):
        info=ZeroconfServiceInfo(ip_address=ipaddress.ip_address("192.0.2.13"),
                ip_addresses=[ipaddress.ip_address("192.0.2.13")],port=554,
                hostname="VMB5000.local.",type="_arlo-video._tcp.local.",
                name="Synthetic._arlo-video._tcp.local.",properties={"deviceid":"hub"})
        flow=await self.hass.config_entries.flow.async_init("arlo_local",
            context={"source":"zeroconf"},data=info)
        self.assertEqual(flow["type"],"form",flow)
        self.assertEqual(flow["step_id"],"user")
        self.assertIn("hub_id",str(flow["data_schema"]))
        self.hass.config_entries.flow.async_abort(flow["flow_id"])
    async def test_native_reauth_repair_clears_after_update_without_reload(self):
        entry=ConfigEntry(domain="arlo_local",title="Synthetic Arlo Local",version=1,minor_version=1,
            data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub","storage_id":"test",
                  "username":"old@example.invalid","password":"old-synthetic","mfa":"push"},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),
            subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        rt=SimpleNamespace(refresh=AsyncMock(return_value=True))
        entry.runtime_data=rt
        await entry._async_init_reauth(self.hass)
        key=("homeassistant",f"config_entry_reauth_arlo_local_{entry.entry_id}")
        issues=ir.async_get(self.hass).issues
        self.assertIn(key,issues)
        flow_id=issues[key].data["flow_id"]
        data={"username":"new@example.invalid","password":"new-synthetic","mfa":"push"}
        with patch.object(ConfigEntries,"async_reload",return_value=True) as reload:
            result=await self.hass.config_entries.flow.async_configure(flow_id,data)
            async with asyncio.timeout(5):
                while result["type"]!="abort":
                    await asyncio.sleep(.05)
                    result=await self.hass.config_entries.flow.async_configure(flow_id)
            self.assertEqual(result["reason"],"reauth_successful")
            self.assertFalse(reload.called)
        self.assertEqual(entry.data["username"],"new@example.invalid")
        self.assertNotIn(key,ir.async_get(self.hass).issues)
        rt.refresh.assert_awaited_once()
    async def test_reconfigure_validates_endpoint_and_preserves_entry_identity(self):
        entry=ConfigEntry(domain="arlo_local",title="Synthetic Arlo Local",version=1,minor_version=1,
            data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub","storage_id":"test",
                  "username":"old@example.invalid","password":"old-synthetic","mfa":"push"},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),
            subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        rt=Runtime(self.hass,entry)
        entry.runtime_data=rt
        data={"host":"192.0.2.14","hostname":"hub.local",
              "username":"new@example.invalid","password":"new-synthetic","mfa":"push"}
        with patch.object(ConfigEntries,"async_reload",return_value=True) as reload, patch.object(
            rt,"validate_reconfigure",new_callable=AsyncMock) as validate:
            result=await self.hass.config_entries.flow.async_init("arlo_local",
                context={"source":"reconfigure","entry_id":entry.entry_id},data=dict(entry.data))
            self.assertEqual(result["type"],"form")
            flow_id=result["flow_id"]
            result=await self.hass.config_entries.flow.async_configure(flow_id,data)
            async with asyncio.timeout(5):
                while result["type"]!="abort":
                    await asyncio.sleep(.05)
                    result=await self.hass.config_entries.flow.async_configure(flow_id)
            self.assertEqual(result["reason"],"reconfigure_successful")
            await self.hass.async_block_till_done()
            reload.assert_awaited_once_with(entry.entry_id)
        self.assertEqual(entry.unique_id,"hub")
        self.assertEqual(entry.data["host"],"192.0.2.14")
        self.assertEqual(entry.data["storage_id"],"test")
        validate.assert_awaited_once_with(
            {"username":"new@example.invalid","password":"new-synthetic","mfa":"push"},
            "192.0.2.14","hub.local")

    async def test_manual_mfa_native_flow_and_no_otp_persistence(self):
        async def authenticate(flow):
            self.assertIsNotNone(flow._manual_mfa)
            code = await asyncio.to_thread(flow._manual_mfa.request_code)
            self.assertEqual(code, "123456")
            return {"storage_id": "synthetic-store", "title": "Synthetic"}
        with patch.object(ArloLocalConfigFlow, "_authenticate", authenticate), patch.object(
            ConfigEntries, "async_setup", return_value=True
        ):
            result = await self.hass.config_entries.flow.async_init(
                "arlo_local", context={"source": "user"}
            )
            flow_id = result["flow_id"]
            result = await self.hass.config_entries.flow.async_configure(flow_id, {
                "host": "192.0.2.13", "hostname": "hub.local", "hub_id": "synthetic-hub",
                "username": "synthetic@example.invalid", "password": "synthetic", "mfa": "manual_email",
            })
            async with asyncio.timeout(5):
                while result["type"] != "form":
                    await asyncio.sleep(.05)
                    result = await self.hass.config_entries.flow.async_configure(flow_id)
            self.assertEqual(result["step_id"], "mfa")
            result = await self.hass.config_entries.flow.async_configure(flow_id, {"code": "wrong"})
            self.assertEqual(result["errors"]["base"], "invalid_mfa")
            result = await self.hass.config_entries.flow.async_configure(flow_id, {"code": "123456"})
            async with asyncio.timeout(5):
                while result["type"] != "create_entry":
                    await asyncio.sleep(.05)
                    result = await self.hass.config_entries.flow.async_configure(flow_id)
            self.assertNotIn("code", result["result"].data)
            self.assertNotIn("123456", str(result["result"].data))

    async def test_initial_setup_and_cached_retry_validate_before_entry_creation(self):
        import hashlib
        from ssl import SSLContext, PROTOCOL_TLS_CLIENT
        from custom_components.arlo_local import config_flow
        hub=Hub("hub","hub-uid","Synthetic Hub","VMB5000")
        cameras={"cam":CameraInfo("cam","cam-uid","Camera B","VMC5040")}
        candidate={"directory":"arlo-cert-poc-synthetic",**serialize_inventory(hub,cameras)}
        root=Path(self.tmp.name)/".arlo_local"/hashlib.sha256(b"hub").hexdigest()
        data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub",
              "username":"synthetic@example.invalid","password":"synthetic","mfa":"imap",
              "imap_host":"imap.example.invalid","imap_username":"synthetic@example.invalid","imap_password":"synthetic"}
        async def finish(result):
            async with asyncio.timeout(5):
                while result["type"] not in ("form","create_entry","abort"):
                    await asyncio.sleep(.01)
                    result=await self.hass.config_entries.flow.async_configure(result["flow_id"])
            return result
        with patch.object(OwnerSession,"inventory",return_value=(hub,cameras)),patch.object(
            OwnerSession,"provision",return_value=candidate) as provision,patch.object(
            CredentialStore,"context",return_value=(SSLContext(PROTOCOL_TLS_CLIENT),b"0"*32)),patch.object(
            config_flow,"probe_tls",new_callable=AsyncMock) as probe,patch.object(
            ConfigEntries,"async_setup",return_value=True):
            # A connection failure must leave no active certificate or config entry.
            probe.side_effect=OSError("synthetic unavailable")
            f=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            result=await finish(await self.hass.config_entries.flow.async_configure(f["flow_id"],data))
            self.assertEqual(result["errors"]["base"],"cannot_connect")
            self.assertFalse((root/"active.json").exists())
            self.assertEqual(self.hass.config_entries.async_entries("arlo_local"),[])
            # Success commits only after both TLS checks.
            probe.side_effect=None
            result=await finish(await self.hass.config_entries.flow.async_configure(f["flow_id"],data))
            self.assertEqual(result["type"],"create_entry")
            self.assertEqual(result["result"].unique_id,"hub")
            self.assertEqual(CredentialStore(root).current()["directory"],candidate["directory"])
            self.assertEqual(probe.await_count,3)
            self.assertEqual(provision.call_count,2)

    async def test_form_error_recovery_and_cancellation(self):
        data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub",
              "username":"synthetic@example.invalid","password":"synthetic","mfa":"push"}
        flow=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
        fid=flow["flow_id"]
        result=await self.hass.config_entries.flow.async_configure(fid,dict(data,host="127.0.0.1"))
        self.assertEqual(result["errors"]["base"],"invalid_config")
        for exception,error in [(AuthRequired("synthetic"),"invalid_auth"),
                                (InventoryUnavailable("synthetic"),"inventory_unavailable"),
                                (IssuanceBlocked("synthetic"),"issuance_unresolved")]:
            with patch.object(ArloLocalConfigFlow,"_authenticate",side_effect=exception):
                result=await self.hass.config_entries.flow.async_configure(fid,data)
                async with asyncio.timeout(5):
                    while result["type"]!="form":
                        await asyncio.sleep(.01)
                        result=await self.hass.config_entries.flow.async_configure(fid)
                self.assertEqual(result["errors"]["base"],error)
        self.hass.config_entries.flow.async_abort(fid)
        self.assertEqual(self.hass.config_entries.async_entries("arlo_local"),[])

    async def test_invalid_discovery_and_duplicate(self):
        def discovery(port=554,props=None):
            return ZeroconfServiceInfo(ip_address=ipaddress.ip_address("192.0.2.13"),
                ip_addresses=[ipaddress.ip_address("192.0.2.13")],port=port,
                hostname="VMB5000.local.",type="_arlo-video._tcp.local.",
                name="Synthetic._arlo-video._tcp.local.",properties=props if props is not None else {"deviceid":"hub"})
        for info in (discovery(port=123),discovery(props={})):
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"zeroconf"},data=info)
            self.assertEqual(result["reason"],"invalid_discovery")
        entry=ConfigEntry(domain="arlo_local",title="Synthetic",version=1,minor_version=1,
            data={"host":"192.0.2.14","hostname":"hub.local","hub_id":"hub","storage_id":"test"},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"zeroconf"},data=discovery())
        self.assertEqual(result["reason"],"already_configured")
        self.assertEqual(entry.data["host"],"192.0.2.13")
        self.assertEqual(entry.data["storage_id"],"test")

    def test_endpoint_and_auth_input_contracts(self):
        for host,hostname,uid in [(None,"hub.local","hub"),("8.8.8.8","hub.local","hub"),
                                  ("::1","hub.local","hub"),("0.0.0.0","hub.local","hub"),
                                  ("192.0.2.13","bad/name","hub"),("192.0.2.13","hub.local","../bad")]:
            with self.subTest(host=host,hostname=hostname,uid=uid):
                with self.assertRaises((TypeError,ValueError)):validate_endpoint(host,hostname,uid)
        self.assertEqual(validate_endpoint("192.0.2.13","hub.local.","hub"),("192.0.2.13","hub.local","hub"))
        for values in ({},{"username":"synthetic","password":"synthetic","mfa":"unsupported"},
                       {"username":"synthetic","password":"synthetic","mfa":"imap"}):
            with self.assertRaises(ValueError):credential_config(values)
        self.assertEqual(credential_config({"username":"synthetic","password":"synthetic","mfa":"manual_sms","code":"123456"}),
                         {"username":"synthetic","password":"synthetic","mfa":"manual_sms"})

    async def test_unloaded_reconfigure_checks_cached_hub_and_never_issues(self):
        from ssl import SSLContext, PROTOCOL_TLS_CLIENT
        from custom_components.arlo_local import config_flow
        hub=Hub("hub","uid","Synthetic","VMB5000")
        entry=ConfigEntry(domain="arlo_local",title="Synthetic",version=1,minor_version=1,
            data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub","storage_id":"test",
                  "username":"old@example.invalid","password":"synthetic","mfa":"push"},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        store=CredentialStore(Path(self.tmp.name)/".arlo_local"/"test")
        flow=ArloLocalConfigFlow()
        flow.hass=self.hass
        flow._reconfigure=entry
        flow.endpoint={"host":"192.0.2.14","hostname":"hub.local","hub_id":"hub"}
        flow._config={"username":"synthetic","password":"synthetic","mfa":"imap",
                      "imap_host":"imap.example.invalid","imap_username":"synthetic","imap_password":"synthetic"}
        with patch.object(OwnerSession,"inventory",return_value=(hub,{})),patch.object(
            OwnerSession,"provision") as provision,patch.object(CredentialStore,"context",
            return_value=(SSLContext(PROTOCOL_TLS_CLIENT),b"0"*32)),patch.object(config_flow,"probe_tls",new_callable=AsyncMock) as probe:
            with self.assertRaises(IssuanceBlocked):await flow._authenticate()
            record={"directory":"arlo-cert-poc-synthetic",**serialize_inventory(hub,{})}
            atomic_json(store.root/"active.json",record)
            self.assertEqual(await flow._authenticate(),{})
            probe.assert_awaited_once()
            with patch.object(OwnerSession,"inventory",return_value=(Hub("hub","other","Other","VMB5000"),{})):
                with self.assertRaises(IssuanceBlocked):await flow._authenticate()
            provision.assert_not_called()

    async def test_cached_setup_does_not_change_inventory_until_tls_validation(self):
        import hashlib
        from ssl import SSLContext, PROTOCOL_TLS_CLIENT
        from custom_components.arlo_local import config_flow
        hub=Hub("hub","uid","Synthetic","VMB5000")
        root=Path(self.tmp.name)/".arlo_local"/hashlib.sha256(b"hub").hexdigest()
        store=CredentialStore(root)
        record={"directory":"arlo-cert-poc-synthetic",**serialize_inventory(hub,{})}
        atomic_json(root/"active.json",record)
        flow=ArloLocalConfigFlow();flow.hass=self.hass
        flow.endpoint={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub"}
        flow._config={"username":"synthetic","password":"synthetic","mfa":"push"}
        cameras={"cam":CameraInfo("cam","cam-uid","Camera","VMC2030B")}
        with patch.object(OwnerSession,"inventory",return_value=(hub,cameras)),patch.object(
            OwnerSession,"provision") as provision,patch.object(CredentialStore,"inspect",
            return_value={"status":"locally_usable_candidate"}),patch.object(CredentialStore,"context",
            return_value=(SSLContext(PROTOCOL_TLS_CLIENT),b"0"*32)),patch.object(config_flow,"probe_tls",new_callable=AsyncMock) as probe:
            probe.side_effect=OSError("synthetic")
            with self.assertRaises(OSError):await flow._authenticate()
            self.assertEqual(store.current(),record)
            probe.side_effect=None
            result=await flow._authenticate()
            self.assertEqual(result["storage_id"],root.name)
            self.assertEqual(store.current()["cameras"][0]["unique_id"],"cam-uid")
            provision.assert_not_called()
            with patch.object(OwnerSession,"inventory",return_value=(Hub("hub","other","Other","VMB5000"),{})):
                with self.assertRaises(IssuanceBlocked):await flow._authenticate()

    async def test_cancel_active_manual_mfa_flow(self):
        started=asyncio.Event()
        finished=asyncio.Event()
        async def authenticate(flow):
            started.set()
            try:
                code=await asyncio.to_thread(flow._manual_mfa.request_code)
                self.assertIsNone(code)
            finally:
                finished.set()
            raise AuthRequired("cancelled")
        with patch.object(ArloLocalConfigFlow,"_authenticate",authenticate):
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            fid=result["flow_id"]
            result=await self.hass.config_entries.flow.async_configure(fid,{"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub",
                "username":"synthetic","password":"synthetic","mfa":"manual_email"})
            await asyncio.wait_for(started.wait(),2)
            self.hass.config_entries.flow.async_abort(fid)
            await asyncio.wait_for(finished.wait(),2)
        self.assertEqual(self.hass.config_entries.async_entries("arlo_local"),[])

    async def test_discovery_identity_cannot_be_overridden(self):
        info=ZeroconfServiceInfo(ip_address=ipaddress.ip_address("192.0.2.13"),
            ip_addresses=[ipaddress.ip_address("192.0.2.13")],port=554,
            hostname="VMB5000.local.",type="_arlo-video._tcp.local.",
            name="Synthetic._arlo-video._tcp.local.",properties={"deviceid":"hub"})
        result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"zeroconf"},data=info)
        fid=result["flow_id"]
        result=await self.hass.config_entries.flow.async_configure(fid,{"host":"192.0.2.13",
            "hostname":"hub.local","hub_id":"other","username":"synthetic","password":"synthetic","mfa":"push"})
        self.assertEqual(result["errors"]["base"],"invalid_config")
        self.hass.config_entries.flow.async_abort(fid)

    async def test_loaded_manual_reauth_and_reconfigure_pass_challenge(self):
        from custom_components.arlo_local.manual_mfa import ManualMFA
        entry=ConfigEntry(domain="arlo_local",title="Synthetic",version=1,minor_version=1,
            data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub","storage_id":"test",
                  "username":"old@example.invalid","password":"synthetic","mfa":"push"},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        rt=Runtime(self.hass,entry);entry.runtime_data=rt
        flow=ArloLocalConfigFlow();flow.hass=self.hass
        flow._config={"username":"synthetic","password":"synthetic","mfa":"manual_email"}
        flow.endpoint={"host":"192.0.2.14","hostname":"hub.local","hub_id":"hub"}
        flow._manual_mfa=ManualMFA();self.addCleanup(flow._manual_mfa.close)
        flow._reconfigure=entry
        with patch.object(rt,"validate_reconfigure",new_callable=AsyncMock) as validate:
            self.assertEqual(await flow._authenticate(),{})
            self.assertIs(validate.call_args.kwargs["manual_mfa"],flow._manual_mfa)
        flow._reconfigure=None;flow._reauth=entry
        with patch.object(rt,"refresh",new_callable=AsyncMock,return_value=True) as refresh:
            self.assertEqual(await flow._authenticate(),{})
            self.assertIs(refresh.call_args.kwargs["manual_mfa"],flow._manual_mfa)
            refresh.return_value=False
            with self.assertRaises(AuthRequired):await flow._authenticate()

    async def test_unloaded_reauth_reuses_entry_store_and_reloads(self):
        from ssl import SSLContext,PROTOCOL_TLS_CLIENT
        from custom_components.arlo_local import config_flow
        hub=Hub("hub","uid","Synthetic","VMB5000")
        entry=ConfigEntry(domain="arlo_local",title="Synthetic",version=1,minor_version=1,
            data={"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub","storage_id":"existing-store",
                  "username":"old@example.invalid","password":"synthetic","mfa":"push"},
            source="user",unique_id="hub",options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        root=Path(self.tmp.name)/".arlo_local"/"existing-store"
        store=CredentialStore(root)
        atomic_json(root/"active.json",{"directory":"arlo-cert-poc-synthetic",**serialize_inventory(hub,{})})
        data={"username":"synthetic@example.invalid","password":"synthetic","mfa":"imap",
              "imap_host":"imap.example.invalid","imap_username":"synthetic","imap_password":"synthetic"}
        with patch.object(OwnerSession,"inventory",return_value=(hub,{})),patch.object(
            OwnerSession,"provision") as provision,patch.object(CredentialStore,"inspect",
            return_value={"status":"locally_usable_candidate"}),patch.object(CredentialStore,"context",
            return_value=(SSLContext(PROTOCOL_TLS_CLIENT),b"0"*32)),patch.object(config_flow,"probe_tls",new_callable=AsyncMock),patch.object(
            ConfigEntries,"async_reload",return_value=True) as reload:
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"reauth","entry_id":entry.entry_id},data=dict(entry.data))
            fid=result["flow_id"]
            result=await self.hass.config_entries.flow.async_configure(fid,data)
            async with asyncio.timeout(5):
                while result["type"]!="abort":
                    await asyncio.sleep(.01)
                    result=await self.hass.config_entries.flow.async_configure(fid)
            self.assertEqual(result["reason"],"reauth_successful")
            await self.hass.async_block_till_done()
            reload.assert_awaited_once_with(entry.entry_id)
            provision.assert_not_called()
        self.assertEqual(entry.data["storage_id"],"existing-store")
        self.assertEqual(entry.data["mfa"],"imap")

    async def test_manual_progress_cancellation_before_challenge(self):
        started=asyncio.Event()
        stopped=asyncio.Event()
        async def authenticate(flow):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()
        with patch.object(ArloLocalConfigFlow,"_authenticate",authenticate):
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            fid=result["flow_id"]
            result=await self.hass.config_entries.flow.async_configure(fid,{"host":"192.0.2.13","hostname":"hub.local","hub_id":"hub",
                "username":"synthetic","password":"synthetic","mfa":"manual_email"})
            await asyncio.wait_for(started.wait(),2)
            result=await self.hass.config_entries.flow.async_configure(fid)
            try:
                self.assertEqual(result["type"],"progress")
            finally:
                self.hass.config_entries.flow.async_abort(fid)
            await asyncio.wait_for(stopped.wait(),2)

    async def test_executor_cancellation_waits_for_owned_worker(self):
        import threading
        flow=ArloLocalConfigFlow();flow.hass=self.hass
        entered=threading.Event();release=threading.Event();done=threading.Event()
        def worker():
            entered.set()
            if not release.wait(3):raise TimeoutError("synthetic")
            done.set()
            return True
        task=asyncio.create_task(flow._executor(worker))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel()
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(done.is_set())

    async def test_manual_mfa_form_and_expired_challenge(self):
        from custom_components.arlo_local.manual_mfa import ManualMFA
        flow=ArloLocalConfigFlow();flow.hass=self.hass
        bridge=ManualMFA();flow._manual_mfa=bridge
        worker=asyncio.create_task(asyncio.to_thread(bridge.request_code))
        try:
            await asyncio.wait_for(bridge.challenge.wait(),2)
            result=await flow.async_step_mfa()
            self.assertEqual(result['type'],'form')
            self.assertEqual(result['step_id'],'mfa')
            self.assertEqual(result['errors'],{})
            bridge.close()
            self.assertIsNone(await asyncio.wait_for(worker,2))
            with patch.object(flow,'async_step_authenticate',new_callable=AsyncMock,
                              return_value={'type':'progress_done','step_id':'finish'}) as resume:
                result=await flow.async_step_mfa({'code':'123456'})
                resume.assert_awaited_once_with()
                self.assertEqual(result['step_id'],'finish')
        finally:
            bridge.close()
            await worker

    async def test_imap_reconfigure_preserves_mail_settings(self):
        entry=ConfigEntry(domain='arlo_local',title='Synthetic',version=1,minor_version=1,
            data={'host':'192.0.2.13','hostname':'hub.local','hub_id':'hub','storage_id':'test',
                  'username':'old@example.invalid','password':'synthetic','mfa':'push'},
            source='user',unique_id='hub',options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,'async_setup',return_value=True):
            await self.hass.config_entries.async_add(entry)
        rt=Runtime(self.hass,entry);entry.runtime_data=rt
        data={'host':'192.0.2.14','hostname':'hub.local','username':'synthetic@example.invalid',
              'password':'synthetic','mfa':'imap','imap_host':'imap.example.invalid',
              'imap_username':'synthetic','imap_password':'synthetic'}
        with patch.object(ConfigEntries,'async_reload',return_value=True) as reload,patch.object(
            rt,'validate_reconfigure',new_callable=AsyncMock):
            result=await self.hass.config_entries.flow.async_init('arlo_local',
                context={'source':'reconfigure','entry_id':entry.entry_id},data=dict(entry.data))
            fid=result['flow_id']
            result=await self.hass.config_entries.flow.async_configure(fid,data)
            async with asyncio.timeout(5):
                while result['type']!='abort':
                    await asyncio.sleep(.01)
                    result=await self.hass.config_entries.flow.async_configure(fid)
            self.assertEqual(result['reason'],'reconfigure_successful')
            await self.hass.async_block_till_done()
            reload.assert_awaited_once_with(entry.entry_id)
        self.assertEqual(entry.data['storage_id'],'test')
        for key in ('mfa','imap_host','imap_username','imap_password'):
            self.assertEqual(entry.data[key],data[key])
