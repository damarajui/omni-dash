"""Token bucket rate limiter for Omni API (60 requests/minute)."""

from __future__ import annotations

import threading
import time


class RateLimiter:
    """Thread-safe token bucket rate limiter.

    Omni enforces 60 requests per minute per API key. This limiter
    proactively throttles requests to avoid 429 errors.

    The bucket holds up to `max_tokens` tokens and refills at `refill_rate`
    tokens per second. Each request consumes one token. If no tokens are
    available, the caller blocks until one is refilled.
    """

    def __init__(self, max_tokens: int = 60, refill_rate: float = 1.0):
        """Initialize the rate limiter.

        Args:
            max_tokens: Maximum burst capacity. Defaults to 60 (Omni's per-minute limit).
            refill_rate: Tokens added per second. Defaults to 1.0 (60/min).
        """
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self._tokens = float(max_tokens)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Add tokens based on elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.refill_rate
        self._tokens = min(self.max_tokens, self._tokens + new_tokens)
        self._last_refill = now

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a single token, blocking until one is available.

        Args:
            timeout: Maximum seconds to wait. None means wait forever.

        Returns:
            True if a token was acquired, False if timed out.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                # Calculate wait time until next token
                wait_time = (1.0 - self._tokens) / self.refill_rate

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait_time = min(wait_time, remaining)

            time.sleep(min(wait_time, 0.1))  # Sleep in small increments for responsiveness

    @property
    def available_tokens(self) -> float:
        """Current number of available tokens (approximate)."""
        with self._lock:
            self._refill()
            return self._tokens

    def __enter__(self) -> RateLimiter:
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        pass
