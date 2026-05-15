"""Regression tests: computed-but-unpersisted option aggregates must not
be lost on a DB write failure or a circuit-breaker skip.

Defect (pre-fix): on a DB write failure (or a skip while the circuit
breaker is backing off) at/after a 1-minute bucket rollover,
``_prepare_option_agg`` has *already* cleared/seeded the per-symbol
buffer and advanced the volume baseline. The only "recovery" was
invalidating the baseline cache, but the post-rollover buffer is a
``_SEED_FLAG`` snapshot whose path never reconsults the baseline — so
that bucket's classified ask/mid/bid flow was dropped permanently.

Fix: ``_write_option_rows`` retains the exact agg dicts it failed to
persist and prepends them to the next attempt. This is provably exact:
a rolled-back / skipped transaction commits nothing, and the upsert sums
flow fields additively, so each agg is applied exactly once when it
finally succeeds. These tests pin: (1) loss is repaired, (2) no
double-count across retries, (3) the happy path neither retains nor
re-writes, (4) the retain buffer is bounded.
"""

from __future__ import annotations

import contextlib
import threading
from collections import OrderedDict
from datetime import datetime

import pytz

import src.ingestion.main_engine as me
from src.ingestion.main_engine import IngestionEngine
from src.validation import bucket_timestamp

ET = pytz.timezone("US/Eastern")
SYM = "SPY260515P00739000"


# --------------------------------------------------------------------------
# Fake DB layer: records the rows that actually reach the upsert, and can be
# toggled "down" so the write raises (caught by _write_option_rows).
# --------------------------------------------------------------------------
class _FakeCursor:
    pass


class _FakeConn:
    def __init__(self):
        self.committed = False

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        self.committed = True


class _FakeDB:
    """Holds state for the patched db_connection / execute_values."""

    def __init__(self):
        self.up = True
        # Each element is the list of value-tuples one execute_values saw.
        self.persisted_batches: list[list[tuple]] = []

    @contextlib.contextmanager
    def connection(self):
        if not self.up:
            raise RuntimeError("simulated DB down (connect)")
        yield _FakeConn()

    def execute_values(self, cursor, sql, values, page_size=None):
        if not self.up:
            raise RuntimeError("simulated DB down (execute)")
        self.persisted_batches.append(list(values))


def _install_fake_db(monkeypatch) -> _FakeDB:
    fake = _FakeDB()
    monkeypatch.setattr(me, "db_connection", fake.connection)
    monkeypatch.setattr(me, "execute_values", fake.execute_values)
    return fake


def _write_engine() -> IngestionEngine:
    """Minimal stub exercising the real _write_option_rows / retain path."""
    e = IngestionEngine.__new__(IngestionEngine)
    e._pending_failed_option_rows = []
    e._pending_failed_option_rows_max = 20000
    e._db_backoff_until = 0.0
    e._db_consecutive_failures = 0
    e.errors_count = 0
    e.option_quotes_stored = 0
    e.last_flush_time = None
    e._obs_batches_written = 0
    e._obs_rows_written = 0
    e._obs_write_time_ms = 0.0
    e._obs_last_log = 0.0
    # Baseline invalidation is exercised by its own test; here it would
    # only add unrelated state, so make it a no-op for these focused tests.
    e._invalidate_option_volume_baseline = lambda _sym: None  # type: ignore
    return e


def _agg(option_symbol: str, bucket: datetime, ask_volume: int, volume: int) -> dict:
    return {
        "option_symbol": option_symbol,
        "timestamp": bucket,
        "underlying": "SPY",
        "strike": 739.0,
        "expiration": "2026-05-15",
        "option_type": "P",
        "last": 5.0,
        "bid": 4.9,
        "ask": 5.1,
        "mid": 5.0,
        "volume": volume,
        "open_interest": 100,
        "implied_volatility": 0.3,
        "ask_volume": ask_volume,
        "mid_volume": 0,
        "bid_volume": 0,
        "delta": -0.5,
        "gamma": 0.01,
        "theta": -0.1,
        "vega": 0.2,
    }


def _flow_for(fake: _FakeDB, option_symbol: str, ts: datetime) -> int:
    """Total classified flow (ask+mid+bid) persisted across ALL batches.

    The real upsert is additive across separate writes, so summing every
    batch that carried this (symbol, timestamp) mirrors the DB columns.
    """
    total = 0
    for batch in fake.persisted_batches:
        for row in batch:
            # values tuple order: option_symbol@0, timestamp@1, ...,
            # ask_volume@13, mid_volume@14, bid_volume@15
            if row[0] == option_symbol and row[1] == ts:
                total += row[13] + row[14] + row[15]
    return total


B = bucket_timestamp(ET.localize(datetime(2026, 5, 15, 10, 15, 0)), 60)
B_NEXT = bucket_timestamp(ET.localize(datetime(2026, 5, 15, 10, 16, 0)), 60)


def test_failed_write_retains_and_resubmits_exactly_once(monkeypatch):
    fake = _install_fake_db(monkeypatch)
    e = _write_engine()

    # Bucket B's residual classified flow (the rollover agg). DB is down.
    fake.up = False
    e._write_option_rows([_agg(SYM, B, ask_volume=70, volume=1070)])

    # Nothing persisted; the agg is retained, not lost.
    assert fake.persisted_batches == []
    assert len(e._pending_failed_option_rows) == 1
    assert _flow_for(fake, SYM, B) == 0

    # DB recovers and the breaker's backoff window has elapsed; next write
    # carries a *different* bucket's agg.
    fake.up = True
    e._db_backoff_until = 0.0
    e._write_option_rows([_agg(SYM, B_NEXT, ask_volume=12, volume=1082)])

    # The previously-lost B flow is now persisted exactly once, alongside
    # the new bucket — and the pending buffer is drained.
    assert _flow_for(fake, SYM, B) == 70
    assert _flow_for(fake, SYM, B_NEXT) == 12
    assert e._pending_failed_option_rows == []


def test_circuit_breaker_skip_retains_instead_of_dropping(monkeypatch):
    fake = _install_fake_db(monkeypatch)
    e = _write_engine()

    # Breaker open: the write is skipped. Pre-fix this silently dropped rows.
    e._db_backoff_until = me._time.monotonic() + 999.0
    e._write_option_rows([_agg(SYM, B, ask_volume=55, volume=1055)])
    assert fake.persisted_batches == []
    assert len(e._pending_failed_option_rows) == 1

    # Breaker clears; even an empty new batch flushes the retained agg.
    e._db_backoff_until = 0.0
    e._write_option_rows([])
    assert _flow_for(fake, SYM, B) == 55
    assert e._pending_failed_option_rows == []


def test_repeated_failures_coalesce_additively_no_double_count(monkeypatch):
    fake = _install_fake_db(monkeypatch)
    e = _write_engine()
    fake.up = False

    # Two failed attempts add more residual flow to the SAME (symbol,bucket).
    e._write_option_rows([_agg(SYM, B, ask_volume=70, volume=1070)])
    e._db_backoff_until = 0.0  # keep the next call on the failure path
    e._write_option_rows([_agg(SYM, B, ask_volume=30, volume=1100)])

    fake.up = True
    e._db_backoff_until = 0.0
    e._write_option_rows([])  # drain

    # Persisted exactly once with the summed flow (70 + 30), not doubled
    # and not lost. (Coalesced to a single row for the key.)
    assert _flow_for(fake, SYM, B) == 100
    assert sum(len(b) for b in fake.persisted_batches) == 1
    assert e._pending_failed_option_rows == []


def test_successful_write_neither_retains_nor_rewrites(monkeypatch):
    fake = _install_fake_db(monkeypatch)
    e = _write_engine()

    e._write_option_rows([_agg(SYM, B, ask_volume=42, volume=1042)])
    assert _flow_for(fake, SYM, B) == 42
    assert e._pending_failed_option_rows == []

    # A subsequent empty/no-op write must not phantom-resubmit anything.
    e._write_option_rows([])
    assert _flow_for(fake, SYM, B) == 42  # still 42, not 84
    assert sum(len(b) for b in fake.persisted_batches) == 1


def test_pending_buffer_is_bounded_drops_oldest(monkeypatch, caplog):
    fake = _install_fake_db(monkeypatch)
    e = _write_engine()
    e._pending_failed_option_rows_max = 3
    fake.up = False
    # Pin the breaker open so all five calls deterministically take the
    # retain path (failure vs skip both retain identically; pinning just
    # removes any wall-clock timing dependence from the test).
    e._db_backoff_until = me._time.monotonic() + 9999.0

    # Five distinct buckets fail; only the newest 3 are retained.
    buckets = [
        bucket_timestamp(ET.localize(datetime(2026, 5, 15, 10, m, 0)), 60) for m in range(10, 15)
    ]
    for i, bkt in enumerate(buckets):
        e._write_option_rows([_agg(SYM, bkt, ask_volume=i + 1, volume=1000 + i)])

    pending = e._pending_failed_option_rows
    assert len(pending) == 3
    kept_ts = {r["timestamp"] for r in pending}
    assert kept_ts == set(buckets[2:])  # oldest two dropped
    assert any("Pending failed-write buffer exceeded" in r.message for r in caplog.records)


def test_rollover_residual_survives_db_failure_end_to_end(monkeypatch):
    """Higher-level: drive the REAL _prepare_option_agg rollover, fail the
    write, then recover — the bucket-B residual must reach the DB once."""
    fake = _install_fake_db(monkeypatch)

    e = IngestionEngine.__new__(IngestionEngine)
    e.options_buffer = {}
    e._option_volume_baseline = {}
    e._option_volume_baseline_lock = threading.Lock()
    e._option_volume_baseline_ttl = 1800.0
    e._option_last_quote = OrderedDict()
    e._option_last_quote_lock = threading.Lock()
    e._option_last_quote_max = 10000
    e._option_bucket_last_write = {}
    e._classify_fallback_count = 0
    e.errors_count = 0
    e._pending_failed_option_rows = []
    e._pending_failed_option_rows_max = 20000
    e._db_backoff_until = 0.0
    e._db_consecutive_failures = 0
    e.option_quotes_stored = 0
    e.last_flush_time = None
    e._obs_batches_written = 0
    e._obs_rows_written = 0
    e._obs_write_time_ms = 0.0
    e._obs_last_log = 0.0

    ts1 = ET.localize(datetime(2026, 5, 15, 10, 15, 5))
    ts1b = ET.localize(datetime(2026, 5, 15, 10, 15, 40))

    def _snap(ts, volume):
        return {
            "option_symbol": SYM,
            "timestamp": ts,
            "underlying": "SPY",
            "strike": 739.0,
            "expiration": "2026-05-15",
            "option_type": "P",
            "last": 5.0,
            "bid": 4.9,
            "ask": 5.1,
            "mid": 5.0,
            "volume": volume,
            "open_interest": 100,
            "delta": -0.5,
            "gamma": 0.01,
            "theta": -0.1,
            "vega": 0.2,
            "implied_volatility": 0.3,
        }

    # Seed B with a prior cumulative of 1000 so first obs isn't a cold start.
    e._option_volume_baseline[e._baseline_cache_key(SYM, B)] = (1000, me._time.monotonic())

    # First throttled flush of B (keep seed) — persists (1000 -> 1000? no:
    # snap vol 1000 == baseline so 0); add a real first delta.
    e.options_buffer[SYM] = [_snap(ts1, volume=1000)]
    a1 = e._prepare_option_agg(SYM, B, keep_last_snapshot=True)
    e._write_option_rows([a1])  # DB up: persists agg1 (0 classified here)

    # More ticks in B, then the bucket-closing rollover flush yields the
    # residual (1080 - 1000 = 80 classified) — but the DB is now DOWN.
    e.options_buffer[SYM].append(_snap(ts1b, volume=1080))
    a_resid = e._prepare_option_agg(SYM, B, keep_last_snapshot=False)
    assert a_resid["ask_volume"] + a_resid["mid_volume"] + a_resid["bid_volume"] == 80

    fake.up = False
    e._write_option_rows([a_resid])
    assert _flow_for(fake, SYM, B) == 0  # lost? no — retained
    assert len(e._pending_failed_option_rows) >= 1

    # Recover (breaker window elapsed): the residual is re-submitted and
    # lands exactly once.
    fake.up = True
    e._db_backoff_until = 0.0
    e._write_option_rows([])
    assert _flow_for(fake, SYM, B) == 80
    assert e._pending_failed_option_rows == []
