#!/usr/bin/env python3
"""
Health check module with retry/backoff, circuit breaker, configurable timeout,
and rate limiting.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import aiohttp

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT = 5.0          # seconds
DEFAULT_PROBE_RATE = 10        # probes per second
HALF_OPEN_RATE_FACTOR = 0.5    # reduce rate to 50% in HALF_OPEN

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Circuit Breaker States
# ---------------------------------------------------------------------------
class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

# ---------------------------------------------------------------------------
# Rate Limiter (Token Bucket)
# ---------------------------------------------------------------------------
class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, burst: Optional[int] = None):
        self.rate = rate
        self.burst = burst if burst is not None else int(rate)
        self.tokens = float(self.burst)
        self.last_refill = time.monotonic()
        self.throttled_count = 0
        self.total_requests = 0

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

    def acquire(self) -> bool:
        """Try to acquire a token. Returns True if allowed, False if throttled."""
        self._refill()
        self.total_requests += 1
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        else:
            self.throttled_count += 1
            return False

    async def wait_acquire(self):
        """Block until a token is available."""
        while not self.acquire():
            await asyncio.sleep(1.0 / self.rate)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "rate": self.rate,
            "burst": self.burst,
            "current_tokens": round(self.tokens, 2),
            "throttled": self.throttled_count,
            "total_requests": self.total_requests,
        }

# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------
class CircuitBreaker:
    """Simple circuit breaker with half-open state."""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0

    def record_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
        elif self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN

    def allow_request(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if time.monotonic() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        elif self.state == CircuitState.HALF_OPEN:
            return True
        return True

    def get_state(self) -> CircuitState:
        return self.state

# ---------------------------------------------------------------------------
# Health Check Probe
# ---------------------------------------------------------------------------
@dataclass
class ProbeResult:
    endpoint: str
    success: bool
    status_code: Optional[int] = None
    response_time: float = 0.0
    error: Optional[str] = None

class HealthProbe:
    """Per-endpoint health probe with configurable timeout."""

    def __init__(self, endpoint: str, timeout: float = DEFAULT_TIMEOUT):
        self.endpoint = endpoint
        self.timeout = timeout
        self.circuit_breaker = CircuitBreaker()

    async def check(self, session: aiohttp.ClientSession) -> ProbeResult:
        start = time.monotonic()
        try:
            async with session.get(self.endpoint, timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                elapsed = time.monotonic() - start
                if resp.status < 500:
                    self.circuit_breaker.record_success()
                    return ProbeResult(endpoint=self.endpoint, success=True, status_code=resp.status, response_time=elapsed)
                else:
                    self.circuit_breaker.record_failure()
                    return ProbeResult(endpoint=self.endpoint, success=False, status_code=resp.status, response_time=elapsed)
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start
            self.circuit_breaker.record_failure()
            return ProbeResult(endpoint=self.endpoint, success=False, error=f"Timeout after {self.timeout}s", response_time=elapsed)
        except Exception as e:
            elapsed = time.monotonic() - start
            self.circuit_breaker.record_failure()
            return ProbeResult(endpoint=self.endpoint, success=False, error=str(e), response_time=elapsed)

# ---------------------------------------------------------------------------
# Health Check Runner
# ---------------------------------------------------------------------------
class HealthCheckRunner:
    """Orchestrates health checks with rate limiting and circuit breaker integration."""

    def __init__(self, endpoints: List[str], timeout: float = DEFAULT_TIMEOUT,
                 probe_rate: float = DEFAULT_PROBE_RATE):
        self.endpoints = endpoints
        self.timeout = timeout
        self.probe_rate = probe_rate
        self.rate_limiter = TokenBucket(rate=probe_rate)
        self.probes = [HealthProbe(endpoint=ep, timeout=timeout) for ep in endpoints]
        self.results: List[ProbeResult] = []

    def _get_effective_rate(self) -> float:
        """Return the effective probe rate based on circuit breaker states."""
        half_open_count = sum(1 for p in self.probes if p.circuit_breaker.get_state() == CircuitState.HALF_OPEN)
        if half_open_count > 0:
            return self.probe_rate * HALF_OPEN_RATE_FACTOR
        return self.probe_rate

    async def run_checks(self) -> List[ProbeResult]:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for probe in self.probes:
                # Apply rate limiting
                effective_rate = self._get_effective_rate()
                self.rate_limiter.rate = effective_rate
                await self.rate_limiter.wait_acquire()

                if not probe.circuit_breaker.allow_request():
                    self.results.append(ProbeResult(endpoint=probe.endpoint, success=False,
                                                    error="Circuit breaker open"))
                    continue

                task = asyncio.create_task(probe.check(session))
                tasks.append(task)

            if tasks:
                done, _ = await asyncio.wait(tasks)
                for task in done:
                    result = task.result()
                    self.results.append(result)
        return self.results

    def get_aggregate_report(self) -> Dict[str, Any]:
        total = len(self.results)
        successes = sum(1 for r in self.results if r.success)
        failures = total - successes
        avg_response_time = sum(r.response_time for r in self.results) / total if total > 0 else 0.0
        rate_limiter_stats = self.rate_limiter.get_stats()
        circuit_states = {}
        for probe in self.probes:
            circuit_states[probe.endpoint] = probe.circuit_breaker.get_state().value
        return {
            "total_probes": total,
            "successes": successes,
            "failures": failures,
            "average_response_time": round(avg_response_time, 3),
            "rate_limiter": rate_limiter_stats,
            "circuit_states": circuit_states,
        }

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Health check tool with rate limiting and circuit breaker.")
    parser.add_argument("endpoints", nargs="+", help="URL endpoints to check")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help=f"Per-probe timeout in seconds (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--probe-rate", type=float, default=DEFAULT_PROBE_RATE,
                        help=f"Max probes per second (default: {DEFAULT_PROBE_RATE})")
    parser.add_argument("--output", choices=["text", "json"], default="text",
                        help="Output format")
    return parser.parse_args(argv)

def main(argv: Optional[List[str]] = None):
    args = parse_args(argv)
    runner = HealthCheckRunner(endpoints=args.endpoints, timeout=args.timeout,
                               probe_rate=args.probe_rate)
    asyncio.run(runner.run_checks())
    report = runner.get_aggregate_report()
    if args.output == "json":
        print(json.dumps(report, indent=2))
    else:
        print(f"Total probes: {report['total_probes']}")
        print(f"Successes: {report['successes']}")
        print(f"Failures: {report['failures']}")
        print(f"Average response time: {report['average_response_time']}s")
        print(f"Rate limiter stats: {report['rate_limiter']}")
        print(f"Circuit states: {report['circuit_states']}")

if __name__ == "__main__":
    main()
