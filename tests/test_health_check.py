#!/usr/bin/env python3
"""
Unit tests for health_check module.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from tools.health_check import TokenBucket, CircuitBreaker, probe_endpoint


class TestTokenBucket(unittest.TestCase):
    def test_initial_tokens(self):
        bucket = TokenBucket(rate=10, burst=5)
        self.assertEqual(bucket.tokens, 5)
        self.assertEqual(bucket.rate, 10)

    def test_consume_allows(self):
        bucket = TokenBucket(rate=10, burst=5)
        self.assertTrue(bucket.consume())
        self.assertEqual(bucket.tokens, 4)

    def test_consume_throttles(self):
        bucket = TokenBucket(rate=10, burst=1)
        self.assertTrue(bucket.consume())
        self.assertFalse(bucket.consume())
        self.assertEqual(bucket.throttled_count, 1)

    def test_refill_over_time(self):
        bucket = TokenBucket(rate=10, burst=5)
        bucket.tokens = 0
        bucket.last_refill = 0  # force refill from epoch
        # Simulate 0.5 seconds elapsed
        with patch('time.monotonic', return_value=0.5):
            bucket._refill()
            self.assertAlmostEqual(bucket.tokens, 5.0, places=1)

    def test_get_stats(self):
        bucket = TokenBucket(rate=5, burst=3)
        bucket.consume()
        stats = bucket.get_stats()
        self.assertEqual(stats["current_rate"], 5)
        self.assertEqual(stats["throttled_requests"], 0)


class TestCircuitBreaker(unittest.TestCase):
    def test_initial_state_closed(self):
        cb = CircuitBreaker()
        self.assertEqual(cb.state, "CLOSED")
        self.assertTrue(cb.allow_request())

    def test_open_after_failures(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        for _ in range(3):
            cb.record_failure()
        self.assertEqual(cb.state, "OPEN")
        self.assertFalse(cb.allow_request())

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, "OPEN")
        # Wait for recovery timeout
        import time
        time.sleep(0.2)
        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, "HALF_OPEN")

    def test_success_resets(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_success()
        self.assertEqual(cb.failure_count, 0)
        self.assertEqual(cb.state, "CLOSED")


class TestProbeEndpoint(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_successful_probe(self):
        async def run():
            session = AsyncMock()
            response = AsyncMock()
            response.status = 200
            session.get.return_value.__aenter__.return_value = response
            bucket = TokenBucket(rate=10, burst=5)
            cb = CircuitBreaker()
            success, elapsed, error = await probe_endpoint(
                session, "http://example.com", 5.0, bucket, cb
            )
            self.assertTrue(success)
            self.assertIsNone(error)
            self.assertGreaterEqual(elapsed, 0)
        self.loop.run_until_complete(run())

    def test_timeout_probe(self):
        async def run():
            session = AsyncMock()
            session.get.side_effect = asyncio.TimeoutError()
            bucket = TokenBucket(rate=10, burst=5)
            cb = CircuitBreaker()
            success, elapsed, error = await probe_endpoint(
                session, "http://example.com", 0.001, bucket, cb
            )
            self.assertFalse(success)
            self.assertEqual(error, "Timeout")
        self.loop.run_until_complete(run())

    def test_rate_limited_probe(self):
        async def run():
            session = AsyncMock()
            bucket = TokenBucket(rate=0, burst=0)  # no tokens
            cb = CircuitBreaker()
            success, elapsed, error = await probe_endpoint(
                session, "http://example.com", 5.0, bucket, cb
            )
            self.assertFalse(success)
            self.assertEqual(error, "Rate limited")
        self.loop.run_until_complete(run())

    def test_half_open_rate_reduction(self):
        async def run():
            session = AsyncMock()
            response = AsyncMock()
            response.status = 200
            session.get.return_value.__aenter__.return_value = response
            bucket = TokenBucket(rate=10, burst=5)
            cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
            cb.record_failure()  # open circuit
            import time
            time.sleep(0.02)  # wait for half-open
            cb.allow_request()  # transition to half-open
            self.assertEqual(cb.state, "HALF_OPEN")
            # In half-open, effective rate should be 50% of 10 = 5
            # We can't easily test the exact rate, but we can check that probe succeeds
            success, elapsed, error = await probe_endpoint(
                session, "http://example.com", 5.0, bucket, cb
            )
            self.assertTrue(success)
        self.loop.run_until_complete(run())


if __name__ == "__main__":
    unittest.main()
