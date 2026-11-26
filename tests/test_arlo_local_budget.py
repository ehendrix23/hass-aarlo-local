"""A per-hub admission budget must never preempt a running producer."""
import unittest

from custom_components.arlo_local.budget import AdmissionBudget, stream_budget


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_resize_drains_without_revoking_existing_admission(self):
        a=AdmissionBudget(5)
        b=AdmissionBudget(1)
        for _ in range(5):
            await a.acquire()
        self.assertEqual(a.in_use,5)
        self.assertTrue(a.locked())
        self.assertFalse(b.locked())
        a.resize(2)
        self.assertEqual(a.in_use,5)
        for _ in range(3):
            a.release()
            self.assertTrue(a.locked())
        a.release()
        self.assertFalse(a.locked())
        await a.acquire()
        self.assertTrue(a.locked())
        with self.assertRaises(RuntimeError):await a.acquire()
        a.resize(5)
        self.assertFalse(a.locked())
        a.release();a.release()
        with self.assertRaises(ValueError):a.release()

    async def test_validation(self):
        self.assertEqual(stream_budget({}),3)
        for v in (1,2,3,4,5):
            self.assertEqual(stream_budget({'max_streams':v}),v)
        for v in (0,6,True,False,'5',2.5,None):
            with self.assertRaises(ValueError):stream_budget({'max_streams':v})
            with self.assertRaises(ValueError):AdmissionBudget(v)
