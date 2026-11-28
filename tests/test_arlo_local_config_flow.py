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
from homeassistant.components import zeroconf as ha_zeroconf
from zeroconf import ServiceStateChange
from zeroconf import asyncio as zc_asyncio
from custom_components.arlo_local import config_flow
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
        # Discovery is an external LAN boundary; existing auth/entry tests
        # must not accidentally discover the developer's physical SmartHub.
        self.mdns={}
        self.browser=SimpleNamespace(async_cancel=AsyncMock())
        async def resolve(service_type,name,timeout):
            self.assertEqual(service_type,"_arlo-video._tcp.local.")
            return self.mdns[name]
        self.zc=SimpleNamespace(zeroconf=object(),async_get_service_info=AsyncMock(side_effect=resolve))
        self.shared_zc=AsyncMock(return_value=self.zc)
        def browse(zc,service_type,handlers):
            self.assertIs(zc,self.zc.zeroconf)
            self.assertEqual(service_type,"_arlo-video._tcp.local.")
            for name in self.mdns:
                handlers[0](zc,service_type,name,ServiceStateChange.Added)
            return self.browser
        self.browse=unittest.mock.Mock(side_effect=browse)
        self.enterContext(patch.object(ha_zeroconf,"async_get_async_instance",self.shared_zc))
        self.enterContext(patch.object(zc_asyncio,"AsyncServiceBrowser",self.browse))
        self.enterContext(patch.object(config_flow,"DISCOVERY_WINDOW",0,create=True))
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

    def advertise(self,hub_id="hub",host="192.168.50.13",hostname="VMB5000.local.",port=554,properties=None):
        service_type="_arlo-video._tcp.local."
        name=f"{hub_id}.{service_type}"
        info=zc_asyncio.AsyncServiceInfo(service_type,name,addresses=[ipaddress.ip_address(host).packed],
            port=port,server=hostname,properties=properties if properties is not None else {"DeviceID":hub_id})
        self.mdns[name]=info
        return name

    def filled(self,result,**overrides):
        return result["data_schema"]({"username":"synthetic@example.invalid","password":"synthetic",
            "mfa":"push",**overrides})

    async def test_manual_add_prefills_one_hub_and_persists_discovery(self):
        from ssl import SSLContext, PROTOCOL_TLS_CLIENT
        name=self.advertise()
        hub=Hub("hub","hub-uid","Synthetic","VMB5000")
        candidate={"directory":"arlo-cert-poc-synthetic",**serialize_inventory(hub,{})}
        with patch.object(OwnerSession,"inventory",return_value=(hub,{})) as inventory,patch.object(
            OwnerSession,"provision",return_value=candidate),patch.object(CredentialStore,"context",
            return_value=(SSLContext(PROTOCOL_TLS_CLIENT),b"0"*32)),patch.object(
            config_flow,"probe_tls",new_callable=AsyncMock) as probe,patch.object(ConfigEntries,"async_setup",return_value=True):
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            self.assertEqual(result["step_id"],"user")
            data=self.filled(result)
            self.assertEqual({k:data[k] for k in ("host","hostname","hub_id")},
                {"host":"192.168.50.13","hostname":"VMB5000.local","hub_id":"hub"})
            self.browser.async_cancel.assert_awaited_once()
            result=await self.hass.config_entries.flow.async_configure(result["flow_id"],data)
            async with asyncio.timeout(5):
                while result["type"]!="create_entry":
                    await asyncio.sleep(.01)
                    result=await self.hass.config_entries.flow.async_configure(result["flow_id"])
            self.assertEqual(result["result"].unique_id,"hub")
            self.assertEqual(result["result"].data["service_name"],name)
            self.assertEqual(result["result"].data["host"],"192.168.50.13")
            self.assertEqual(result["result"].data["hostname"],"VMB5000.local")
            self.assertEqual(inventory.call_args.args[1],"hub")
            self.assertEqual(probe.await_count,2)
            for call in probe.await_args_list:
                self.assertEqual(call.args[:3],("192.168.50.13",554,"VMB5000.local"))
        self.shared_zc.assert_awaited_once()

    async def test_manual_add_multiple_hubs_select_or_manual(self):
        self.advertise("hub-a","192.168.50.13","a.local.")
        self.advertise("hub-b","192.168.50.14","b.local.")
        for choice,expected in (("hub-b",("192.168.50.14","b.local","hub-b")),
                                ("manual:",("","VMB5000.local",""))):
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            fid=result["flow_id"]
            self.assertEqual(result["step_id"],"select_hub")
            result=await self.hass.config_entries.flow.async_configure(fid,{"hub":choice})
            self.assertEqual(result["step_id"],"user")
            data=self.filled(result)
            self.assertEqual(tuple(data[k] for k in ("host","hostname","hub_id")),expected)
            self.hass.config_entries.flow.async_abort(fid)

    async def test_manual_discovery_fallback_excludes_invalid_and_existing_hubs(self):
        self.advertise("configured")
        self.advertise("wrong-port",port=123)
        self.advertise("missing-id",properties={})
        self.advertise("public-address",host="8.8.8.8")
        self.mdns["unresolved._arlo-video._tcp.local."]=None
        entry=ConfigEntry(domain="arlo_local",title="Existing",version=1,minor_version=1,
            data={"host":"192.168.50.99","hostname":"configured.local","hub_id":"configured","storage_id":"existing"},
            source="user",unique_id="configured",options={},discovery_keys=MappingProxyType({}),subentries_data=None)
        with patch.object(ConfigEntries,"async_setup",return_value=True):
            await self.hass.config_entries.async_add(entry)
        result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
        self.assertEqual(result["step_id"],"user")
        self.assertEqual(self.filled(result)["host"],"")
        self.assertEqual(self.filled(result)["hub_id"],"")
        self.assertEqual(entry.data["host"],"192.168.50.99")
        self.assertEqual(entry.data["storage_id"],"existing")
        self.browser.async_cancel.assert_awaited_once()
        self.hass.config_entries.flow.async_abort(result["flow_id"])

    async def test_manual_discovery_error_and_retry_does_not_rescan(self):
        self.shared_zc.side_effect=OSError("mDNS unavailable")
        result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
        fid=result["flow_id"]
        self.assertEqual(self.filled(result)["host"],"")
        result=await self.hass.config_entries.flow.async_configure(fid,self.filled(result))
        self.assertEqual(result["errors"]["base"],"invalid_config")
        self.shared_zc.assert_awaited_once()
        self.browser.async_cancel.assert_not_awaited()
        self.hass.config_entries.flow.async_abort(fid)

    async def test_discovery_removed_conflicting_ipv6_and_unresolved_records(self):
        self.advertise("ipv6-only","fd00::1")
        self.advertise("wrong-id",properties={"deviceid":"../invalid"})
        self.advertise("ambiguous-a",properties={"deviceid":"conflict"})
        self.advertise("ambiguous-b","192.168.50.14",properties={"deviceid":"conflict"})
        removed=self.advertise("removed")
        dual=self.advertise("dual")
        self.mdns[dual].addresses=[ipaddress.ip_address("192.168.50.13").packed,ipaddress.ip_address("fd00::2").packed]
        self.mdns["incomplete._arlo-video._tcp.local."]=zc_asyncio.AsyncServiceInfo("_arlo-video._tcp.local.","incomplete._arlo-video._tcp.local.")
        real_browse=self.browse.side_effect
        def browse(zc,service_type,handlers):
            browser=real_browse(zc,service_type,handlers)
            handlers[0](zc,service_type,removed,ServiceStateChange.Removed)
            return browser
        self.browse.side_effect=browse
        result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
        self.assertEqual(result["step_id"],"user")
        data=self.filled(result)
        self.assertEqual((data["host"],data["hub_id"]),("192.168.50.13","dual"))
        self.assertNotIn(removed,[c.args[1] for c in self.zc.async_get_service_info.await_args_list])
        self.browser.async_cancel.assert_awaited_once()
        self.hass.config_entries.flow.async_abort(result["flow_id"])

    async def test_user_can_correct_discovery_defaults_without_storing_wrong_service(self):
        self.advertise()
        async def authenticate(flow):
            self.assertEqual(flow.endpoint,{"host":"192.168.50.15","hostname":"other.local","hub_id":"other"})
            return {"storage_id":"synthetic","title":"Synthetic"}
        with patch.object(ArloLocalConfigFlow,"_authenticate",authenticate),patch.object(ConfigEntries,"async_setup",return_value=True):
            result=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            data=self.filled(result,host="192.168.50.15",hostname="other.local",hub_id="other")
            result=await self.hass.config_entries.flow.async_configure(result["flow_id"],data)
            async with asyncio.timeout(5):
                while result["type"]!="create_entry":
                    await asyncio.sleep(.01)
                    result=await self.hass.config_entries.flow.async_configure(result["flow_id"])
            self.assertEqual(result["result"].unique_id,"other")
            self.assertNotIn("service_name",result["result"].data)

    async def test_manual_setup_supersedes_idle_discovery_but_not_started_setup(self):
        name=self.advertise()
        info=ZeroconfServiceInfo(ip_address=ipaddress.ip_address("192.168.50.13"),
            ip_addresses=[ipaddress.ip_address("192.168.50.13")],port=554,hostname="VMB5000.local.",
            type="_arlo-video._tcp.local.",name=name,properties={"deviceid":"hub"})
        # A submitted but invalid discovered setup is already user-owned.
        # Manual add must not cancel it.
        discovery=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"zeroconf"},data=info)
        fid=discovery["flow_id"]
        discovery=await self.hass.config_entries.flow.async_configure(fid,self.filled(discovery,hub_id="wrong"))
        self.assertEqual(discovery["errors"]["base"],"invalid_config")
        manual=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
        manual=await self.hass.config_entries.flow.async_configure(manual["flow_id"],self.filled(manual))
        self.assertEqual(manual["reason"],"already_in_progress")
        self.assertIn(fid,[f["flow_id"] for f in self.hass.config_entries.flow.async_progress_by_handler("arlo_local")])
        self.hass.config_entries.flow.async_abort(fid)
        # A passive discovery notification may be replaced by explicit manual setup.
        discovery=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"zeroconf"},data=info)
        fid=discovery["flow_id"]
        with patch.object(ArloLocalConfigFlow,"_authenticate",new_callable=AsyncMock,
            return_value={"storage_id":"synthetic","title":"Synthetic"}),patch.object(ConfigEntries,"async_setup",return_value=True):
            manual=await self.hass.config_entries.flow.async_init("arlo_local",context={"source":"user"})
            manual=await self.hass.config_entries.flow.async_configure(manual["flow_id"],self.filled(manual))
            async with asyncio.timeout(5):
                while manual["type"]!="create_entry":
                    await asyncio.sleep(.01)
                    manual=await self.hass.config_entries.flow.async_configure(manual["flow_id"])
        self.assertNotIn(fid,[f["flow_id"] for f in self.hass.config_entries.flow.async_progress_by_handler("arlo_local")])
        self.assertEqual(manual["result"].data["service_name"],name)

    async def test_scan_cancellation_releases_only_its_browser(self):
        self.advertise()
        entered=asyncio.Event()
        async def resolve(*args):
            entered.set()
            await asyncio.Event().wait()
        self.zc.async_get_service_info.side_effect=resolve
        task=asyncio.create_task(config_flow.discover_hubs(self.hass))
        await asyncio.wait_for(entered.wait(),2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.browser.async_cancel.assert_awaited_once()

    async def test_select_hub_rejects_unknown_selection(self):
        self.advertise("hub-a")
        self.advertise("hub-b")
        flow=ArloLocalConfigFlow()
        flow.hass=self.hass
        flow._discovered_hubs=await config_flow.discover_hubs(self.hass)
        result=await flow.async_step_select_hub({"hub":"not-advertised"})
        self.assertEqual(result["errors"]["base"],"invalid_config")
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
