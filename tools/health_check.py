#!/usr/bin/env python3
"""
Health check module with configurable timeout, rate limiting, and circuit breaker.
"""

import argparse
import asyncio
import json
import logging
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, burst: Optional[int] = None):
        self.rate = rate
        self.burst = burst if burst is not None else int(rate)
        self.tokens = float(self.burst)
        self.last_refill = time.monotonic()
        self.throttled_count = 0

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if allowed, False if throttled."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        self.throttled_count += 1
        return False

    def get_stats(self) -> Dict:
        return {
            "current_rate": self.rate,
            "current_tokens": self.tokens,
            "throttled_requests": self.throttled_count,
        }


class CircuitBreaker:
    """Simple circuit breaker with three states: CLOSED, OPEN, HALF_OPEN."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = "CLOSED"
        self.failure_count = 0
        self.last_failure_time = 0.0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def allow_request(self) -> bool:
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if time.monotonic() - self.last_failure_time >= self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        elif self.state == "HALF_OPEN":
            return True
        return False

    def get_state(self) -> str:
        return self.state


async def probe_endpoint(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float,
    rate_limiter: TokenBucket,
    circuit_breaker: CircuitBreaker,
) -> Tuple[bool, float, Optional[str]]:
    """Probe a single endpoint with rate limiting and circuit breaker."""
    if not circuit_breaker.allow_request():
        return False, 0.0, "Circuit breaker open"

    # Adjust rate for half-open state
    effective_rate = rate_limiter.rate
    if circuit_breaker.get_state() == "HALF_OPEN":
        effective_rate = rate_limiter.rate * 0.5
        # Temporarily adjust bucket rate for this probe
        original_rate = rate_limiter.rate
        rate_limiter.rate = effective_rate
        allowed = rate_limiter.consume()
        rate_limiter.rate = original_rate
    else:
        allowed = rate_limiter.consume()

    if not allowed:
        return False, 0.0, "Rate limited"

    start = time.monotonic()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            elapsed = time.monotonic() - start
            if response.status < 400:
                circuit_breaker.record_success()
                return True, elapsed, None
            else:
                circuit_breaker.record_failure()
                return False, elapsed, f"HTTP {response.status}"
    except asyncio.TimeoutError:
        elapsed = time.monotonic() - start
        circuit_breaker.record_failure()
        return False, elapsed, "Timeout"
    except Exception as e:
        elapsed = time.monotonic() - start
        circuit_breaker.record_failure()
        return False, elapsed, str(e)


async def run_health_checks(
    endpoints: List[str],
    timeout: float,
    probe_rate: float,
    circuit_breaker: CircuitBreaker,
) -> Dict:
    """Run health checks against all endpoints with rate limiting."""
    rate_limiter = TokenBucket(rate=probe_rate)
    results = []
    async with aiohttp.ClientSession() as session:
        for url in endpoints:
            success, elapsed, error = await probe_endpoint(
                session, url, timeout, rate_limiter, circuit_breaker
            )
            results.append({
                "url": url,
                "success": success,
                "elapsed": elapsed,
                "error": error,
            })
    return {
        "results": results,
        "rate_limiter_stats": rate_limiter.get_stats(),
        "circuit_breaker_state": circuit_breaker.get_state(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Health check tool")
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-endpoint timeout in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--probe-rate",
        type=float,
        default=10.0,
        help="Maximum probes per second globally (default: 10.0)",
    )
    parser.add_argument(
        "endpoints",
        nargs="+",
        help="One or more endpoint URLs to check",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    circuit_breaker = CircuitBreaker()
    report = await run_health_checks(
        args.endpoints, args.timeout, args.probe_rate, circuit_breaker
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
