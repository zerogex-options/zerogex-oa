"""Tests for the DB-write circuit-breaker backoff policy.

`_compute_db_backoff_seconds` lives at module scope in
`src.ingestion.main_engine` so both `_upsert_underlying_quote` and
`_write_option_rows` share the exact same exponential-with-jitter
policy. Tests live here so the policy stays correct without needing to
mock the engine, asyncpg, or the streaming loop.
"""

from __future__ import annotations

from src.ingestion.main_engine import _compute_db_backoff_seconds


class TestExponentialBaseAndCap:
    def test_first_failure(self):
        # 2 ** 1 == 2; jitter in [0, 0.2)
        for _ in range(50):
            assert 2.0 <= _compute_db_backoff_seconds(1) < 2.2

    def test_third_failure(self):
        # 2 ** 3 == 8; jitter in [0, 0.8)
        for _ in range(50):
            assert 8.0 <= _compute_db_backoff_seconds(3) < 8.8

    def test_capped_at_sixty_seconds_plus_jitter(self):
        # Once 2 ** N > 60, base saturates at 60. Jitter in [0, 6).
        for _ in range(50):
            v = _compute_db_backoff_seconds(20)
            assert 60.0 <= v < 66.0

    def test_zero_failures_returns_minimum(self):
        # 2 ** 0 == 1; jitter in [0, 0.1)
        v = _compute_db_backoff_seconds(0)
        assert 1.0 <= v < 1.1


class TestJitterIsActuallyRandom:
    """Without jitter, `min(2**N, 60)` is deterministic for fixed N. Confirm
    the helper produces distinct values across repeated calls — that's the
    whole point of W3.6, breaking lockstep retry storms."""

    def test_repeated_calls_are_not_all_identical(self):
        samples = [_compute_db_backoff_seconds(5) for _ in range(20)]
        assert len(set(samples)) > 1, "expected jitter to introduce variation"

    def test_jitter_never_decreases_below_base(self):
        # Floor: jitter is uniform on [0, base * 0.1) — never negative.
        for failures in (1, 2, 5, 10, 100):
            for _ in range(20):
                base = min(2**failures, 60)
                assert _compute_db_backoff_seconds(failures) >= base
