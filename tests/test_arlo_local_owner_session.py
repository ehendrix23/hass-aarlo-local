import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyaarlo.backend import ArloBackEnd
from custom_components.arlo_local.owner_session import (IsolatedBackend,OwnerConfiguration,
    OwnerSession,AuthRequired,InventoryUnavailable,IssuanceBlocked)
from custom_components.arlo_local.storage import CredentialStore
from custom_components.arlo_local.inventory import Hub,CameraInfo

CONFIG={"username":"synthetic@example.invalid","password":"synthetic","mfa":"push"}

class OwnerSessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
    def test_backend_does_not_share_session_state_or_start_event_workers(self):
        before=ArloBackEnd._session_info
        first=self.root/"first";second=self.root/"second"
        first.mkdir(mode=0o700);second.mkdir(mode=0o700)
        with patch.object(ArloBackEnd,"_login",return_value=True):
            a=IsolatedBackend(OwnerConfiguration(first,CONFIG))
            b=IsolatedBackend(OwnerConfiguration(second,CONFIG))
            c=IsolatedBackend(OwnerConfiguration(first,CONFIG))
        self.assertIs(ArloBackEnd._session_info,before)
        self.assertNotEqual(a._user_device_id,b._user_device_id)
        self.assertEqual(a._user_device_id,c._user_device_id)
        self.assertTrue(a.is_connected)
        self.assertFalse(a._relogin())
        self.assertIsNone(a._event_thread)
        self.assertFalse(a._arlo.cfg.save_session)
        self.assertFalse((first/"session.pickle").exists())
    def test_imap_config_uses_upstream_helper(self):
        cfg=OwnerConfiguration(self.root,dict(CONFIG,mfa="imap",
            imap_host="imap.example.invalid",imap_username="synthetic@example.invalid",
            imap_password="synthetic"))
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=IsolatedBackend(cfg)
        from pyaarlo.tfa import Arlo2FAImap
        self.assertIsInstance(backend._get_tfa(),Arlo2FAImap)
        self.assertEqual(cfg.cfg.tfa_type,"email")
        self.assertFalse(cfg.cfg.save_session)
    def test_auth_expiry_only_one_relogin(self):
        store=CredentialStore(self.root/"store")
        owner=OwnerSession(store)
        self.addCleanup(owner.close)
        first_root=self.root/"first"
        second_root=self.root/"second"
        first_root.mkdir(mode=0o700)
        second_root.mkdir(mode=0o700)
        with patch.object(ArloBackEnd,"_login",return_value=True):
            first=IsolatedBackend(OwnerConfiguration(first_root,CONFIG))
            second=IsolatedBackend(OwnerConfiguration(second_root,CONFIG))
        owner.backend=first;owner._config=dict(CONFIG)
        with patch.object(owner,"_read_inventory",side_effect=AuthRequired("synthetic")),patch.object(
            owner,"_login",return_value=second) as login:
            with self.assertRaises(AuthRequired):owner.inventory(CONFIG,"hub")
            self.assertEqual(login.call_count,1)
    def test_uncertain_issuance_cannot_repeat(self):
        store=CredentialStore(self.root/"store")
        owner=OwnerSession(store)
        from custom_components.arlo_local.storage import atomic_json
        atomic_json(store.root/"issuance.json",{"status":"request_pending","directory":"arlo-cert-poc-synthetic"})
        hub=Hub("hub","hub-uid","Hub","VMB5000")
        with patch.object(owner,"inventory",return_value=(hub,{})):
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
        self.assertIsNone(store.current())
        self.assertEqual(list(store.root.glob("arlo-cert-poc-*")),[])

    def test_auth_session_reuse_ignores_unrelated_entry_fields(self):
        owner=OwnerSession(CredentialStore(self.root/"store"))
        self.addCleanup(owner.close)
        hub=Hub("hub","hub-uid","Hub","VMB5000")
        with patch.object(ArloBackEnd,"_login",return_value=True) as login, patch.object(
            OwnerSession,"_read_inventory",return_value=(hub,{})):
            self.assertEqual(owner.inventory(CONFIG,"hub"),(hub,{}))
            self.assertEqual(owner.inventory(dict(CONFIG,host="192.0.2.4",storage_id="test"),"hub"),(hub,{}))
            self.assertEqual(login.call_count,1)
            self.assertEqual(owner.inventory(dict(CONFIG,password="changed"),"hub"),(hub,{}))
            self.assertEqual(login.call_count,2)

    def test_owner_inventory_response_validation(self):
        owner=OwnerSession(CredentialStore(self.root/"store"))
        self.addCleanup(owner.close)
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=owner._login(CONFIG)
        backend._user_id="owner"
        devices=[
            {"deviceId":"hub","uniqueId":"uid","deviceName":"Hub","modelId":"VMB5000","owner":{"ownerId":"owner"}},
            {"deviceId":"cam","uniqueId":"c","deviceType":"camera","parentId":"hub","modelId":"VMC2030B","owner":{"ownerId":"owner"}},
        ]
        for response in ((200,{"success":True,"data":devices}), (200,{"meta":{"code":200},"data":devices})):
            with patch.object(backend,"_request_tuple",return_value=response) as request:
                hub,cameras=owner._read_inventory(backend,"hub")
                self.assertEqual(hub.unique_id,"uid")
                self.assertEqual(set(cameras),{"cam"})
                self.assertFalse(request.call_args.kwargs["retry_401"])
        for response,exception in (
            ((401,None),AuthRequired),
            ((200,{"meta":{"code":"401"}}),AuthRequired),
            ((200,{"meta":{"error":9204}}),AuthRequired),
            ((503,None),InventoryUnavailable),
            ((200,[]),InventoryUnavailable),
            ((200,{"success":False}),InventoryUnavailable),
            ((200,{"success":True,"data":[]}),InventoryUnavailable),
        ):
            with self.subTest(response=response),patch.object(backend,"_request_tuple",return_value=response):
                with self.assertRaises(exception):owner._read_inventory(backend,"hub")
        backend._user_id=None
        with patch.object(backend,"_request_tuple",return_value=(200,{"success":True,"data":devices})):
            with self.assertRaises(AuthRequired):owner._read_inventory(backend,"hub")

    def test_login_rejection_and_transient_failures_distinguished(self):
        owner=OwnerSession(CredentialStore(self.root/"store"))
        self.addCleanup(owner.close)
        for message,exception in (
            ("login failed: 401 - synthetic",AuthRequired),
            ("login failed: 2fa: startup failed",AuthRequired),
            ("login failed: finish failed: 403 - synthetic",AuthRequired),
            ("login failed: 503 - possible cloudflare issue",InventoryUnavailable),
        ):
            def rejected(backend):
                backend._arlo.error(message)
                return False
            with self.subTest(message=message),patch.object(ArloBackEnd,"_login",rejected):
                with self.assertRaises(exception):owner._login(CONFIG)
                self.assertIsNone(owner.backend)
        with patch.object(ArloBackEnd,"_login",side_effect=OSError("synthetic")):
            with self.assertRaises(InventoryUnavailable):owner._login(CONFIG)
        with patch("custom_components.arlo_local.owner_session.pyaarlo.__version__","unsupported"):
            with self.assertRaises(RuntimeError):owner._login(CONFIG)

    def test_manual_background_login_does_not_start_email_challenge(self):
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=IsolatedBackend(OwnerConfiguration(self.root,dict(CONFIG,mfa="manual_email")))
        helper=backend._get_tfa()
        self.assertFalse(helper.start())
        self.assertIsNone(helper.get())

    def test_provision_valid_candidate_and_reuse(self):
        import base64
        import json
        from datetime import UTC,datetime,timedelta
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes,serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        from custom_components.arlo_local import arlo_cert_poc
        store=CredentialStore(self.root/"store")
        owner=OwnerSession(store)
        self.addCleanup(owner.close)
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=owner._login(CONFIG)
        backend._user_id="owner"
        hub=Hub("hub","hub-uid","Hub","VMB5000")
        cam=CameraInfo("cam","cam-uid","Camera","VMC2030B")
        ca=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,"Synthetic")])
        now=datetime.now(UTC)
        def response(backend,params,headers,**kwargs):
            self.assertEqual(params["uuid"],backend._user_device_id)
            self.assertEqual(params["uniqueIds"],["hub-uid"])
            self.assertEqual(headers["x-forwarded-user"],"owner")
            public=serialization.load_der_public_key(base64.b64decode(params["publicKey"]))
            def issue(key):
                c=x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key).serial_number(
                    x509.random_serial_number()).not_valid_before(now-timedelta(days=1)).not_valid_after(
                    now+timedelta(days=1)).sign(ca,hashes.SHA256())
                return base64.b64encode(c.public_bytes(serialization.Encoding.DER)).decode()
            return {"certsData":[{"peerCert":issue(public),"deviceCert":issue(ca.public_key())}],"icaCert":issue(ca.public_key())}
        with patch.object(owner,"inventory",return_value=(hub,{"cam":cam})),patch.object(
            arlo_cert_poc,"request_once",side_effect=response) as request:
            candidate=owner.provision(CONFIG,"hub")
            self.assertIsNone(store.current())
            self.assertEqual(store.inspect(candidate)["status"],"locally_usable_candidate")
            self.assertEqual(owner.provision(CONFIG,"hub"),candidate)
            self.assertEqual(request.call_count,1)
            self.assertEqual(json.loads((store.root/"issuance.json").read_text())["status"],"candidate")
        self.assertEqual(candidate["cameras"][0]["unique_id"],"cam-uid")

    def test_provision_failure_preserves_previous_generation_and_blocks_repeat(self):
        import json
        from custom_components.arlo_local import arlo_cert_poc
        from custom_components.arlo_local.storage import atomic_json,serialize_inventory
        store=CredentialStore(self.root/"store")
        owner=OwnerSession(store)
        self.addCleanup(owner.close)
        hub=Hub("hub","hub-uid","Hub","VMB5000")
        previous={"directory":"arlo-cert-poc-prior",**serialize_inventory(hub,{})}
        atomic_json(store.root/"active.json",previous)
        atomic_json(store.root/"issuance.json",{"status":"committed","directory":previous["directory"]})
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=owner._login(CONFIG)
        backend._user_id="owner"
        with patch.object(owner,"inventory",return_value=(hub,{})),patch.object(
            arlo_cert_poc,"request_once",side_effect=arlo_cert_poc.CertificateRequestError({"backend_status":200,"api_success":False})) as request:
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
            self.assertEqual(request.call_count,1)
        self.assertEqual(store.current(),previous)
        ledger=json.loads((store.root/"issuance.json").read_text())
        self.assertEqual(ledger["status"],"failed_or_uncertain")
        self.assertEqual(ledger["diagnostic"]["api_success"],False)

    def test_invalid_client_state_and_backend_cleanup(self):
        import json
        from unittest.mock import Mock
        from custom_components.arlo_local.owner_session import authentication_settings
        owner=OwnerSession(CredentialStore(self.root/"store"))
        self.addCleanup(owner.close)
        with self.assertRaises(ValueError):OwnerConfiguration(self.root,dict(CONFIG,mfa="unknown"))
        imap=dict(CONFIG,mfa="imap",imap_host="imap.example.invalid",imap_username="synthetic",imap_password="synthetic")
        self.assertEqual(authentication_settings(dict(imap,host="ignored")),imap)
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=owner._login(CONFIG)
        backend._session=Mock()
        session=backend._session
        owner.close()
        session.close.assert_called_once()
        root=owner.store.root/"owner_auth"
        (root/"client_id.json").write_text(json.dumps({"id":42}))
        with self.assertRaises(InventoryUnavailable):owner._login(CONFIG)

    def test_login_exception_cleanup_and_no_second_attempt(self):
        from unittest.mock import Mock
        owner=OwnerSession(CredentialStore(self.root/"store"))
        self.addCleanup(owner.close)
        session=Mock()
        def failed(backend):
            backend._session=session
            backend._arlo.error("login failed: 401 - synthetic")
            raise OSError("synthetic")
        with patch.object(ArloBackEnd,"_login",failed):
            with self.assertRaises(AuthRequired):owner._login(CONFIG)
        session.close.assert_called_once()
        def interrupted(backend):
            backend._session=session
            raise KeyboardInterrupt
        with patch.object(ArloBackEnd,"_login",interrupted):
            with self.assertRaises(KeyboardInterrupt):owner._login(CONFIG)
        self.assertEqual(session.close.call_count,2)
        with patch.object(ArloBackEnd,"_login",return_value=True) as login,patch.object(
            OwnerSession,"_read_inventory",side_effect=AuthRequired("synthetic")):
            with self.assertRaises(AuthRequired):owner.inventory(CONFIG,"hub",True)
            self.assertEqual(login.call_count,1)

    def test_provision_invalid_ledgers_identity_and_key_material(self):
        from custom_components.arlo_local import arlo_cert_poc
        from custom_components.arlo_local.storage import atomic_json,serialize_inventory
        store=CredentialStore(self.root/"store")
        owner=OwnerSession(store)
        self.addCleanup(owner.close)
        hub=Hub("hub","hub-uid","Hub","VMB5000")
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=owner._login(CONFIG)
        backend._user_id="owner"
        candidate={"directory":"arlo-cert-poc-candidate",**serialize_inventory(hub,{})}
        wrong={"directory":"arlo-cert-poc-wrong",**serialize_inventory(Hub("hub","wrong","Wrong","VMB5000"),{})}
        ledger=store.root/"issuance.json"
        with patch.object(owner,"inventory",return_value=(hub,{})),patch.object(
            arlo_cert_poc,"request_once") as request:
            atomic_json(store.root/"active.json",wrong)
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
            (store.root/"active.json").unlink()
            ledger.write_text("[]")
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
            atomic_json(ledger,{"status":"candidate","candidate":wrong})
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
            ledger.unlink()
            backend._user_device_id=None
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
            request.assert_not_called()
        self.assertIsNone(store.current())

    def test_activated_candidate_ledger_recovers_without_reusing_stale_generation(self):
        from datetime import UTC,datetime
        import json
        from custom_components.arlo_local import arlo_cert_poc
        from custom_components.arlo_local.storage import atomic_json,serialize_inventory
        store=CredentialStore(self.root/"store")
        owner=OwnerSession(store)
        self.addCleanup(owner.close)
        hub=Hub("hub","hub-uid","Hub","VMB5000")
        candidate={"directory":"arlo-cert-poc-candidate",**serialize_inventory(hub,{})}
        atomic_json(store.root/"active.json",dict(candidate,validated_utc=datetime.now(UTC).isoformat()))
        atomic_json(store.root/"issuance.json",{"status":"candidate","candidate":candidate})
        with patch.object(ArloBackEnd,"_login",return_value=True):
            backend=owner._login(CONFIG)
        backend._user_id="owner"
        with patch.object(owner,"inventory",return_value=(hub,{})),patch.object(
            arlo_cert_poc,"request_once",return_value={}) as request,patch.object(
            arlo_cert_poc,"validate_and_save",return_value={"status":"invalid"}):
            with self.assertRaises(IssuanceBlocked):owner.provision(CONFIG,"hub")
        request.assert_called_once()
        self.assertEqual(store.current()["directory"],candidate["directory"])
        self.assertEqual(json.loads((store.root/"issuance.json").read_text())["status"],"failed_or_uncertain")
