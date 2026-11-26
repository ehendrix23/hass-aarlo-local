"""Bounded, thread-safe bridge between pyaarlo console MFA and the HA flow."""

import asyncio
import re
import threading
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout


class ManualMFA:
    """One active one-time challenge; never persist or log the verification code."""

    def __init__(self, timeout: float = 120.0) -> None:
        self.challenge = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        self._timeout = timeout
        self._lock = threading.Lock()
        self._pending: Future[str | None] | None = None
        self._closed = False

    @property
    def pending(self) -> bool:
        with self._lock:
            return self._pending is not None and not self._pending.done()

    def request_code(self) -> str | None:
        """Called only from the synchronous authentication worker."""
        with self._lock:
            if self._closed or self._pending is not None:
                return None
            pending: Future[str | None] = Future()
            self._pending = pending
            self._loop.call_soon_threadsafe(self.challenge.set)
        try:
            return pending.result(timeout=self._timeout)
        except FutureTimeout:
            return None
        finally:
            with self._lock:
                # Only this request can clear the pending slot. Concurrent
                # request_code calls are rejected and close() resolves it.
                self._pending = None
            self._loop.call_soon_threadsafe(self.challenge.clear)

    def submit(self, code: str) -> bool:
        """Submit one valid OTP from the event loop."""
        if not re.fullmatch(r"[0-9]{6}", code):
            return False
        with self._lock:
            if self._closed or self._pending is None or self._pending.done():
                return False
            self._pending.set_result(code)
        self.challenge.clear()
        return True

    def close(self) -> None:
        """Release a waiting worker when the flow is cancelled or completes."""
        with self._lock:
            self._closed = True
            if self._pending is not None and not self._pending.done():
                self._pending.set_result(None)
        self.challenge.clear()

    async def wait_for_challenge_or_completion(
        self, job: asyncio.Task[dict[str, str]]
    ) -> None:
        waiter = asyncio.create_task(self.challenge.wait())
        try:
            await asyncio.wait((waiter, job), return_when=asyncio.FIRST_COMPLETED)
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
