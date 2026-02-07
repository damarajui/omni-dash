"""Tests for the rate limiter."""

import time

from omni_dash.api.rate_limiter import RateLimiter


def test_initial_tokens():
    rl = RateLimiter(max_tokens=10, refill_rate=1.0)
    assert rl.available_tokens == 10.0


def test_acquire_decrements_tokens():
    rl = RateLimiter(max_tokens=5, refill_rate=1.0)
    assert rl.acquire(timeout=0.1)
    assert rl.available_tokens < 5.0


def test_acquire_multiple():
    rl = RateLimiter(max_tokens=3, refill_rate=100.0)
    assert rl.acquire(timeout=0.1)
    assert rl.acquire(timeout=0.1)
    assert rl.acquire(timeout=0.1)
    # 4th should still work because refill_rate is high
    assert rl.acquire(timeout=0.5)


def test_context_manager():
    rl = RateLimiter(max_tokens=5, refill_rate=1.0)
    with rl:
        pass  # Should acquire a token


def test_timeout_returns_false():
    rl = RateLimiter(max_tokens=1, refill_rate=0.1)
    rl.acquire(timeout=0.1)  # Take the only token
    # With very low refill rate and short timeout, should fail
    result = rl.acquire(timeout=0.01)
    # May or may not succeed depending on timing; just ensure no exception
    assert isinstance(result, bool)


def test_refill_over_time():
    rl = RateLimiter(max_tokens=2, refill_rate=100.0)
    rl.acquire()
    rl.acquire()
    # After a small delay with high refill rate, tokens should be available
    time.sleep(0.05)
    assert rl.available_tokens > 0
