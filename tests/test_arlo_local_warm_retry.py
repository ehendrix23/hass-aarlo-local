"""Deterministic warm-worker retry and lease expiry tests."""

import unittest
from unittest.mock import AsyncMock, Mock, patch

from custom_components.arlo_local import warm


class WarmRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_stable_run_resets_failure_backoff(self):
        fence = Mock(port=1234, stream_name="camera")
        fence.close = AsyncMock()
        lease = warm.WarmLease(fence, "ffmpeg", 60, Mock())
        delays = []

        async def sleep(delay):
            delays.append(delay)
            if len(delays) == 2:
                warm.BACKOFF_RESET_SECONDS = 0
            elif len(delays) == 3:
                warm.BACKOFF_RESET_SECONDS = 3600
            elif len(delays) == 4:
                lease.deadline = 0

        with (
            patch.object(warm, "BACKOFF_RESET_SECONDS", 3600),
            patch.object(warm.asyncio, "create_subprocess_exec", new_callable=AsyncMock, return_value=Mock(returncode=1)) as spawn,
            patch.object(warm.asyncio, "sleep", side_effect=sleep),
        ):
            await lease._run()
        self.assertEqual(delays, [1, 2, 1, 2])
        self.assertEqual(spawn.await_count, 4)
        fence.close.assert_awaited_once()

    async def test_expiry_during_running_process_terminates_without_retry(self):
        fence = Mock(port=1234, stream_name="camera")
        fence.close = AsyncMock()
        lease = warm.WarmLease(fence, "ffmpeg", 60, Mock())
        proc = Mock(returncode=None)

        async def wait():
            if proc.returncode is None:
                lease.deadline = 0
                raise TimeoutError
            return proc.returncode

        proc.wait = AsyncMock(side_effect=wait)
        proc.terminate.side_effect = lambda: setattr(proc, "returncode", -15)
        with (
            patch.object(warm.asyncio, "create_subprocess_exec", new_callable=AsyncMock, return_value=proc) as spawn,
            patch.object(warm.asyncio, "sleep", new_callable=AsyncMock) as sleep,
        ):
            await lease._run()
        spawn.assert_awaited_once()
        proc.terminate.assert_called_once()
        proc.kill.assert_not_called()
        sleep.assert_not_awaited()
        fence.close.assert_awaited_once()
