"""Retry policy: exponential backoff with jitter for transient failures.

HTTP 429 (rate limited) gets its own policy: more attempts and much longer
backoff, honouring the server's Retry-After header when present.
"""
from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

T = TypeVar("T")


class RetryableError(Exception):
    """Transient failure: worth retrying (timeouts, 5xx, corrupt payload)."""


class RateLimitError(RetryableError):
    """HTTP 429: the server asks us to slow down; needs much longer backoff."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentError(Exception):
    """Failure that retrying cannot fix (unexpected 4xx, bad instrument)."""


class RetryManager:
    def __init__(
        self,
        max_attempts: int,
        base_seconds: float,
        max_seconds: float,
        *,
        fast: bool = False,
        rate_limit_attempts: int = 6,
        rate_limit_base_seconds: float = 5.0,
        rate_limit_max_seconds: float = 120.0,
    ):
        self.max_attempts = max_attempts
        self.base_seconds = base_seconds
        self.max_seconds = max_seconds
        self.fast = fast
        self.rate_limit_attempts = rate_limit_attempts
        self.rate_limit_base_seconds = rate_limit_base_seconds
        self.rate_limit_max_seconds = rate_limit_max_seconds

    def run(self, fn: Callable[[], T]) -> T:
        attempt = 0
        rate_attempt = 0
        while True:
            try:
                return fn()
            except RateLimitError as exc:
                rate_attempt += 1
                if rate_attempt >= self.rate_limit_attempts:
                    raise
                time.sleep(self._rate_limit_backoff(rate_attempt, exc.retry_after))
            except RetryableError:
                attempt += 1
                if attempt >= self.max_attempts:
                    raise
                delay = self._backoff(attempt)
                if delay > 0:
                    time.sleep(delay)

    def _backoff(self, attempt: int) -> float:
        if self.fast and attempt == 1:
            return 0.0
        cap = min(self.max_seconds, 4.0) if self.fast else self.max_seconds
        delay = min(cap, self.base_seconds * (2 ** (attempt - 1)))
        return delay * (0.5 + random.random())  # jitter: 0.5x .. 1.5x

    def _rate_limit_backoff(self, attempt: int, retry_after: float | None) -> float:
        """Long backoff for HTTP 429; server hint (Retry-After) wins when sane."""
        if retry_after is not None and retry_after > 0:
            return min(self.rate_limit_max_seconds, retry_after)
        delay = min(
            self.rate_limit_max_seconds,
            self.rate_limit_base_seconds * (2 ** (attempt - 1)),
        )
        return delay * (0.75 + random.random() / 2)  # jitter: 0.75x .. 1.25x
