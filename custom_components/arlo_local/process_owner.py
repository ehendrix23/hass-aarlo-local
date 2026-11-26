"""Stop the exact owned process session, never a recycled process group."""

import asyncio
import os
from contextlib import suppress

import psutil


def capture_owner(pid: int) -> psutil.Process | None:
    """Capture PID and creation-time identity for a freshly spawned leader."""
    try:
        return psutil.Process(pid)
    except psutil.NoSuchProcess:
        return None


def session_members(owner: psutil.Process) -> list[psutil.Process]:
    """Find surviving members of the original session, not another process tree."""
    current = capture_owner(owner.pid)
    if current is not None and current != owner:
        return []  # The original session ID cannot be assumed after PID reuse.
    members = []
    for pid in psutil.pids():
        if pid == owner.pid:
            continue
        try:
            if os.getsid(pid) != owner.pid:
                continue
            child = psutil.Process(pid)
            if child.create_time() < owner.create_time():
                continue
            if (
                child.is_running()
                and os.getsid(pid) == owner.pid
                and child.status() != psutil.STATUS_ZOMBIE
            ):
                members.append(child)
        except (ProcessLookupError, psutil.NoSuchProcess):
            continue
        except PermissionError:
            # Other users' processes may not expose their session on every OS.
            # Never attempt to signal an unverified process.
            continue
    current = capture_owner(owner.pid)
    if current is not None and current != owner:
        return []
    return members


def signal_members(members: list[psutil.Process], *, force: bool) -> None:
    """Signal only PID+creation-time identities captured in the owned session."""
    for child in members:
        with suppress(psutil.NoSuchProcess):
            if force:
                child.kill()
            else:
                child.terminate()


async def stop_owned_process(
    process: asyncio.subprocess.Process, owner: psutil.Process | None
) -> None:
    """Stop the leader, then reap verified children; no post-exit group kill."""
    children = await asyncio.to_thread(session_members, owner) if owner else []
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.terminate()
    await asyncio.to_thread(signal_members, children, force=False)
    try:
        await asyncio.wait_for(process.wait(), 3)
    except TimeoutError:
        with suppress(ProcessLookupError):
            process.kill()
        await asyncio.wait_for(process.wait(), 3)
    if owner is not None:
        # The leader may have exited before cleanup or spawned a child just
        # before termination. Re-scan its original isolated session once.
        children.extend(await asyncio.to_thread(session_members, owner))
    await asyncio.to_thread(signal_members, list(set(children)), force=True)
    if owner is not None:
        async with asyncio.timeout(3):
            while await asyncio.to_thread(session_members, owner):
                await asyncio.sleep(0.1)
