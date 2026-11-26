"""Process-session ownership and exact-child cleanup on POSIX."""

import asyncio
import os
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

import psutil

from custom_components.arlo_local.process_owner import capture_owner, session_members, signal_members, stop_owned_process


class ProcessOwnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_already_exited_process_without_captured_identity(self):
        proc=Mock(returncode=0)
        proc.wait=AsyncMock(return_value=0)
        with patch("custom_components.arlo_local.process_owner.session_members",side_effect=AssertionError("No verified owner")),patch("custom_components.arlo_local.process_owner.signal_members") as signals:
            await stop_owned_process(proc,None)
        proc.terminate.assert_not_called()
        proc.kill.assert_not_called()
        self.assertEqual(signals.call_args_list[0].args,([],))
        self.assertEqual(signals.call_args_list[1].args,([],))

    async def test_orphaned_child_is_removed_without_group_signal(self):
        # The child deliberately survives SIGTERM and inherits the leader's
        # private session. A separate process must remain untouched.
        script = "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"
        leader = await asyncio.create_subprocess_exec(sys.executable, "-u", "-c", "import subprocess,sys; p=subprocess.Popen([sys.executable,'-u','-c',sys.argv[1]],stdout=subprocess.PIPE); p.stdout.readline(); print(p.pid,flush=True); sys.stdin.buffer.read(1)",script,stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,start_new_session=True)
        other = await asyncio.create_subprocess_exec(sys.executable,"-c","import time; time.sleep(60)",start_new_session=True)
        owner = capture_owner(leader.pid)
        self.assertIsNotNone(owner)
        child = None
        try:
            child=psutil.Process(int(await asyncio.wait_for(leader.stdout.readline(),5)))
            self.assertIn(child,session_members(owner))
            self.assertNotIn(other.pid,[p.pid for p in session_members(owner)])
            leader.stdin.write(b"x");await leader.stdin.drain()
            await asyncio.wait_for(leader.wait(),5)
            with patch("os.killpg",side_effect=AssertionError("Unverified group signal")):
                await stop_owned_process(leader,owner)
            self.assertFalse(session_members(owner))
            self.assertIsNone(other.returncode)
        finally:
            if child is not None and child.is_running():
                try: child.kill()
                except psutil.NoSuchProcess: pass
            for p in (leader,other):
                if p.returncode is None:p.kill()
                await p.wait()

    async def test_recycled_leader_is_not_scanned(self):
        old=Mock(pid=123)
        new=Mock(pid=123)
        with patch("custom_components.arlo_local.process_owner.capture_owner",return_value=new),patch("psutil.pids",side_effect=AssertionError("Reused session scanned")):
            self.assertEqual(session_members(old),[])

    async def test_owner_missing_and_signal_failure_not_suppressed(self):
        with patch("psutil.Process",side_effect=psutil.NoSuchProcess(123)):
            self.assertIsNone(capture_owner(123))
        child=Mock()
        child.terminate.side_effect=psutil.NoSuchProcess(123)
        signal_members([child],force=False)
        child.kill.side_effect=psutil.AccessDenied(123)
        with self.assertRaises(psutil.AccessDenied):signal_members([child],force=True)

    async def test_session_scan_rejects_old_or_disappeared_processes(self):
        owner=Mock(pid=10);owner.create_time.return_value=100
        stale=Mock(pid=11);stale.create_time.return_value=50
        zombie=Mock(pid=12);zombie.create_time.return_value=110;zombie.status.return_value=psutil.STATUS_ZOMBIE
        valid=Mock(pid=13);valid.create_time.return_value=110;valid.status.return_value=psutil.STATUS_RUNNING
        def sid(pid):
            if pid==14:raise PermissionError
            if pid==15:raise ProcessLookupError
            return 10
        with patch("custom_components.arlo_local.process_owner.capture_owner",return_value=owner),patch("psutil.pids",return_value=[10,11,12,13,14,15]),patch("os.getsid",side_effect=sid),patch("psutil.Process",side_effect={11:stale,12:zombie,13:valid}.__getitem__):
            self.assertEqual(session_members(owner),[valid])
        with patch("custom_components.arlo_local.process_owner.capture_owner",side_effect=[owner,Mock(pid=10)]),patch("psutil.pids",return_value=[]):
            self.assertEqual(session_members(owner),[])

    async def test_stubborn_leader_escalates_and_waits_for_owned_child(self):
        owner=Mock(pid=10)
        child=Mock(pid=11)
        proc=Mock(returncode=None)
        proc.wait=AsyncMock(side_effect=[TimeoutError(),0])
        with patch("custom_components.arlo_local.process_owner.session_members",side_effect=[[child],[],[child],[]]),patch("custom_components.arlo_local.process_owner.signal_members") as signals:
            await stop_owned_process(proc,owner)
        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(signals.call_count,2)
