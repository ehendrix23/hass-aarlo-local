"""Credential-store validation, crash consistency and private storage contracts."""
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from custom_components.arlo_local import storage
from custom_components.arlo_local.inventory import Hub, CameraInfo
from custom_components.arlo_local.storage import CredentialStore, atomic_json, parse_record, serialize_inventory, deserialize_inventory, private_dir

class StorageTests(TestCase):
    def setUp(self):
        temp=TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.store=CredentialStore(self.root/"store")
        self.hub=Hub("hub","hub-uid","Synthetic","VMB5000")
        self.cam=CameraInfo("cam","cam-uid","Camera","VMC2030B")
        self.record={"directory":"arlo-cert-poc-candidate",**serialize_inventory(self.hub,{"cam":self.cam})}

    def test_private_directory_and_pointer_safeguards(self):
        self.assertEqual(self.store.root.stat().st_mode & 0o777,0o700)
        self.assertIsNone(self.store.current())
        wrong=self.root/"wrong";wrong.mkdir(mode=0o755)
        with self.assertRaises(ValueError):private_dir(wrong)
        link=self.root/"link";link.symlink_to(self.store.root)
        with self.assertRaises(ValueError):private_dir(link)
        f=self.root/"file";f.write_text("synthetic")
        with self.assertRaises(ValueError):private_dir(f)
        with patch.object(os,"geteuid",return_value=self.store.root.stat().st_uid+1):
            with self.assertRaises(ValueError):private_dir(self.store.root)
        pointer=self.store.root/"active.json";pointer.symlink_to(self.root/"absent")
        with self.assertRaises(ValueError):self.store.current()
        pointer.unlink()
        with self.assertRaises(ValueError):self.store.candidate(dict(self.record,directory="../escape"))
        with self.assertRaises(ValueError):self.store.candidate(dict(self.record,directory="arlo-cert-poc-/escape"))

    def test_record_validation(self):
        self.assertEqual(parse_record(self.record),self.record)
        for invalid in (None,[],dict(self.record,directory=5),dict(self.record,cameras=None),
                        dict(self.record,hub=None),dict(self.record,hub=dict(self.record["hub"],name=1)),
                        dict(self.record,validated_utc=1),dict(self.record,validated_utc="2026-11-25T12:00:00")):
            with self.subTest(invalid=invalid):
                with self.assertRaises((TypeError,ValueError)):parse_record(invalid)
        d=dict(self.record,validated_utc=datetime.now(UTC).isoformat())
        self.assertEqual(parse_record(d),d)
        for cameras in ([self.record["cameras"][0]]*2,
                        [self.record["cameras"][0],dict(self.record["cameras"][0],device_id="different")]):
            with self.assertRaises(ValueError):deserialize_inventory(dict(self.record,cameras=cameras))
        self.assertEqual(deserialize_inventory(self.record),(self.hub,{"cam":self.cam}))

    def test_failed_atomic_write_leaves_previous_pointer(self):
        path=self.store.root/"active.json"
        atomic_json(path,self.record)
        self.assertEqual(path.stat().st_mode & 0o777,0o600)
        with patch.object(os,"replace",side_effect=OSError("synthetic")):
            with self.assertRaises(OSError):atomic_json(path,{"invalid":"replacement"})
        self.assertEqual(json.loads(path.read_text()),self.record)
        self.assertEqual(list(path.parent.glob(".record-*")),[])

    def test_generation_commit_survives_secondary_ledger_failure(self):
        prior=dict(self.record,directory="arlo-cert-poc-previous")
        atomic_json(self.store.root/"active.json",prior)
        atomic_json(self.store.root/"issuance.json",{"status":"candidate","candidate":self.record})
        def persist(path,data):
            if path.name=="issuance.json":raise OSError("synthetic")
            return atomic_json(path,data)
        with patch.object(CredentialStore,"context"),patch.object(storage,"atomic_json",side_effect=persist):
            with self.assertLogs(storage.__name__,level="WARNING"):
                result=self.store.activate(self.record)
        self.assertEqual(self.store.current(),result)
        self.assertEqual(result["directory"],self.record["directory"])
        self.assertIn("validated_utc",result)
        self.assertEqual(json.loads((self.store.root/"issuance.json").read_text())["status"],"candidate")

    def test_inventory_update_refuses_hub_mismatch(self):
        with self.assertRaises(ValueError):self.store.update_inventory(self.hub,{})
        atomic_json(self.store.root/"active.json",self.record)
        for hub in (Hub("other","hub-uid","Synthetic","VMB5000"),Hub("hub","other","Synthetic","VMB5000")):
            with self.assertRaises(ValueError):self.store.update_inventory(hub,{})
        self.assertEqual(self.store.current(),self.record)
        self.assertEqual(self.store.update_inventory(self.hub,{})["cameras"],[])
        self.assertEqual(self.store.current()["directory"],self.record["directory"])

    def test_missing_credential_material_never_yields_tls_context(self):
        (self.store.root/self.record["directory"]).mkdir(mode=0o700)
        with self.assertRaisesRegex(ValueError,"Invalid or expired"):
            self.store.context(self.record)

    def test_cross_instance_transaction_lock(self):
        other=CredentialStore(self.store.root)
        with self.store.exclusive():
            with self.assertRaises(BlockingIOError):
                with other.exclusive():
                    self.fail("Concurrent credential transaction admitted")
        with other.exclusive():
            self.assertEqual((self.store.root/".transaction.lock").stat().st_mode & 0o777,0o600)
        path=self.store.root/".transaction.lock"
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            with self.store.exclusive():
                self.fail("Unsafe credential lock admitted")
