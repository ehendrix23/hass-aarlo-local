"""Bounded retry timing; no external dependencies or sleeps."""
import unittest

from custom_components.arlo_local.retry import RetryBackoff


class RetryTests(unittest.TestCase):
    def test_exponential_cap_and_reset(self):
        retry = RetryBackoff()
        now = 100.0
        self.assertTrue(retry.ready(now))
        for expected in (1, 2, 4, 8, 15, 15):
            self.assertEqual(retry.failed(now), expected)
            self.assertFalse(retry.ready(now + expected - 0.001))
            now += expected
            self.assertTrue(retry.ready(now))
        retry.reset()
        self.assertEqual(retry.failures, 0)
        self.assertTrue(retry.ready(now))
        self.assertEqual(retry.failed(now), 1)
