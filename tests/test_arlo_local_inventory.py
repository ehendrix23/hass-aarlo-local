import unittest
from custom_components.arlo_local.inventory import Hub, parse_inventory, reconcile

class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.devices=[
            {"deviceId":"hub","uniqueId":"uid","deviceName":"SmartHub","modelId":"VMB5000","deviceType":"basestation","owner":{"ownerId":"owner"}},
            {"deviceId":"cam1","uniqueId":"c1","deviceName":"Camera A","modelId":"VMC2030B","deviceType":"camera","parentId":"hub","owner":{"ownerId":"owner"}},
            {"deviceId":"cam2","uniqueId":"c2","deviceName":"Camera B","modelId":"VMC5040","deviceType":"camera","parentId":"hub","owner":{"ownerId":"owner"}},
            {"deviceId":"other","uniqueId":"other","deviceName":"Other","modelId":"VMC5040","deviceType":"camera","parentId":"otherhub","owner":{"ownerId":"owner"}},
        ]
    def test_owner_and_children_no_capability_filter(self):
        hub,cameras=parse_inventory(self.devices,"owner","hub")
        self.assertEqual(hub.unique_id,"uid")
        self.assertEqual(set(cameras),{"cam1","cam2"})
    def test_interface_version_is_transient_allowlisted_metadata(self):
        from custom_components.arlo_local.storage import serialize_inventory, deserialize_inventory
        self.devices[1]["interfaceVersion"]="i002"
        self.devices[2]["interfaceVersion"]="../../private-token"
        hub,cameras=parse_inventory(self.devices,"owner","hub")
        self.assertEqual(cameras["cam1"].interface_version,"i002")
        self.assertIsNone(cameras["cam2"].interface_version)
        record=serialize_inventory(hub,cameras)
        self.assertNotIn("interface_version",record["cameras"][0])
        _,restored=deserialize_inventory(record)
        self.assertIsNone(restored["cam1"].interface_version)
        del self.devices[1]["interfaceVersion"]
        _,cameras=parse_inventory(self.devices,"owner","hub")
        self.assertIsNone(cameras["cam1"].interface_version)
    def test_nonowner_fails(self):
        with self.assertRaises(ValueError):parse_inventory(self.devices,"admin","hub")
    def test_remove_rename_add_and_no_changes_on_failure(self):
        hub,old=parse_inventory(self.devices,"owner","hub")
        self.devices[1]["deviceName"]="Camera renamed"
        self.devices.pop(2)
        self.devices.append({"deviceId":"cam3","uniqueId":"c3","deviceName":"New","modelId":"X","deviceType":"camera","parentId":"hub","owner":{"ownerId":"owner"}})
        _,new=parse_inventory(self.devices,"owner","hub")
        delta=reconcile(old,new)
        self.assertEqual(delta.added,{"cam3"})
        self.assertEqual(delta.removed,{"cam2"})
        self.assertEqual(delta.changed,{"cam1"})
        self.assertEqual(reconcile(new,new).changed,set())
    def test_missing_hub_not_empty_inventory(self):
        with self.assertRaises(ValueError):parse_inventory(self.devices[1:],"owner","hub")
    def test_duplicate_identity_fails(self):
        with self.assertRaises(ValueError):parse_inventory(self.devices+[self.devices[1]],"owner","hub")

    def test_unverified_inventory_and_child_owner_rejected(self):
        for devices,owner in ((None,"owner"),(self.devices,"")):
            with self.assertRaises(ValueError):parse_inventory(devices,owner,"hub")
        self.devices[1]["owner"]={"ownerId":"different"}
        with self.assertRaises(ValueError):parse_inventory(self.devices,"owner","hub")
