"""Bounded monotonic backoff for failed producer and optional warm starts."""


class RetryBackoff:
    def __init__(self) -> None:
        self.failures = 0
        self.not_before = 0.0

    def ready(self, now: float) -> bool:
        return now >= self.not_before

    def failed(self, now: float) -> float:
        self.failures = min(self.failures + 1, 5)
        delay = float(min(2 ** (self.failures - 1), 15))
        self.not_before = now + delay
        return delay

    def reset(self) -> None:
        self.failures = 0
        self.not_before = 0.0
