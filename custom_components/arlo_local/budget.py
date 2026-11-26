"""Per-hub, non-preemptive admission control on the HA event loop."""

from collections.abc import Mapping

DEFAULT_STREAMS = 3
MAX_STREAMS = 5
CONF_MAX_STREAMS = "max_streams"


def stream_budget(options: Mapping[str, object]) -> int:
    value = options.get(CONF_MAX_STREAMS, DEFAULT_STREAMS)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= MAX_STREAMS
    ):
        raise ValueError("Stream budget must be an integer from 1 to 5")
    return value


class AdmissionBudget:
    """A resize changes future admission only, not existing ownership.

    Like an unlocked semaphore, acquire completes without yielding. A full
    budget is explicitly rejected instead of holding an unbounded wait queue.
    """

    def __init__(self, limit: int = DEFAULT_STREAMS) -> None:
        self.in_use = 0
        self.limit = stream_budget({CONF_MAX_STREAMS: limit})

    def resize(self, limit: int) -> None:
        self.limit = stream_budget({CONF_MAX_STREAMS: limit})

    def locked(self) -> bool:
        return self.in_use >= self.limit

    async def acquire(self) -> bool:
        if self.locked():
            raise RuntimeError("Stream budget exhausted")
        self.in_use += 1
        return True

    def release(self) -> None:
        if not self.in_use:
            raise ValueError("No admission to release")
        self.in_use -= 1
