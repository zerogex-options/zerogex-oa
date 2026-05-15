"""Regression tests for the option volume-baseline / seed accumulation bug.

These pin the fix for a ~30-minute sawtooth in
``option_chains.ask_volume / mid_volume / bid_volume`` (and therefore the
Live Options Quotes chart). Two defects combined to produce it:

* (a) ``_prepare_option_agg`` advanced the volume-baseline cache under a
  bare ``option_symbol`` string key while ``_get_option_volume_baseline``
  read it under a ``(option_symbol, session_date)`` tuple key, so the
  per-aggregation refresh was a silent no-op and the only refresh left was
  the ``OPTION_VOLUME_BASELINE_TTL_SECONDS`` (default 1800s = 30 min) TTL.

* (b) the single-snapshot path re-derived a whole-bucket delta from that
  (stale) baseline even when the lone buffered snapshot was a retained
  seed whose volume had already been classified and persisted — and the
  accumulating upsert added it again.
"""

import threading
from collections import OrderedDict
from datetime import datetime
import time as _time

import pytz

from src.ingestion.main_engine import IngestionEngine, _SEED_FLAG
from src.validation import bucket_timestamp

ET = pytz.timezone("US/Eastern")


def _agg_engine() -> IngestionEngine:
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
    return e


def _snap(option_symbol: str, ts: datetime, volume: int) -> dict:
    # last == mid so classification is deterministic (lands in mid_volume);
    # the tests assert on the ask+mid+bid total so the bucket doesn't matter.
    return {
        "option_symbol": option_symbol,
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


SYM = "SPY260515P00739000"
# 10:15 ET — deliberately not the 09:30 opening-auction bucket.
TS = ET.localize(datetime(2026, 5, 15, 10, 15, 5))
BUCKET = bucket_timestamp(TS, 60)


def _classified(agg: dict) -> int:
    return agg["ask_volume"] + agg["mid_volume"] + agg["bid_volume"]


def test_baseline_advance_uses_read_path_key_and_tags_seed():
    """Fix (a): the optimistic advance must land under the SAME key the
    reader uses (and never as a stray bare-string key), and the retained
    snapshot must be tagged as a seed."""
    e = _agg_engine()
    # Fresh cached baseline so _get_option_volume_baseline returns it
    # without a DB hit. Cold-start (untagged) single snapshot.
    key = e._baseline_cache_key(SYM, BUCKET)
    e._option_volume_baseline[key] = (1000, _time.monotonic())
    e.options_buffer[SYM] = [_snap(SYM, TS, volume=1500)]

    agg = e._prepare_option_agg(SYM, BUCKET, keep_last_snapshot=True)

    assert agg is not None
    # Cold start: vol_delta = 1500 - 1000 = 500.
    assert _classified(agg) == 500
    # The advance landed under the reader's tuple key, set to the
    # just-aggregated cumulative volume...
    assert e._option_volume_baseline[key][0] == 1500
    # ...and NOT under the old buggy bare-string key.
    assert SYM not in e._option_volume_baseline
    # Retained snapshot is tagged so a later lone flush won't re-count it.
    assert e.options_buffer[SYM][0].get(_SEED_FLAG) is True


def test_lone_seed_snapshot_contributes_zero_volume():
    """Fix (b): a lone carried seed must classify zero volume and must not
    even consult the baseline (its volume is already persisted)."""
    e = _agg_engine()

    def _boom(*_a, **_k):
        raise AssertionError("baseline must not be consulted for a seed snapshot")

    e._get_option_volume_baseline = _boom  # type: ignore[assignment]

    seed = _snap(SYM, TS, volume=1500)
    seed[_SEED_FLAG] = True
    e.options_buffer[SYM] = [seed]

    agg = e._prepare_option_agg(SYM, BUCKET, keep_last_snapshot=False)

    assert agg is not None
    assert agg["ask_volume"] == 0
    assert agg["mid_volume"] == 0
    assert agg["bid_volume"] == 0
    # Quote / volume fields still populated so the upsert refreshes them.
    assert agg["volume"] == 1500


def test_no_sawtooth_across_buckets_with_stale_baseline():
    """End-to-end: with a deliberately TTL-stale baseline, the classified
    volume of a *subsequent* bucket is the true per-minute delta — not the
    inflated baseline-relative value that produced the 30-minute sawtooth.
    """
    e = _agg_engine()
    ts1 = ET.localize(datetime(2026, 5, 15, 10, 15, 5))
    ts1b = ET.localize(datetime(2026, 5, 15, 10, 15, 40))
    ts2 = ET.localize(datetime(2026, 5, 15, 10, 16, 3))
    b1 = bucket_timestamp(ts1, 60)
    b2 = bucket_timestamp(ts2, 60)

    # Stale baseline: contract first seen earlier this session at cum vol 100.
    e._option_volume_baseline[e._baseline_cache_key(SYM, b1)] = (
        100,
        _time.monotonic(),
    )

    # --- bucket b1: first observation (cold start, untagged) ---
    e.options_buffer[SYM] = [_snap(SYM, ts1, volume=1000)]
    a1 = e._prepare_option_agg(SYM, b1, keep_last_snapshot=True)
    b1_total = _classified(a1)
    # Cold start legitimately attributes pre-observation session volume
    # (1000 - 100) to the first observed bucket.
    assert b1_total == 900
    seed_b1 = e.options_buffer[SYM][0]
    assert seed_b1.get(_SEED_FLAG) is True

    # --- more ticks in b1, throttled re-flush (multi-snapshot) ---
    e.options_buffer[SYM].append(_snap(SYM, ts1b, volume=1100))
    a1b = e._prepare_option_agg(SYM, b1, keep_last_snapshot=True)
    b1_total += _classified(a1b)
    # Accumulated b1 == 1100 - 100 (the upsert sums per-flush contributions).
    assert b1_total == 1000

    # --- cross into b2 exactly as _store_option_batch does ---
    prev_snap = e.options_buffer[SYM][-1]  # lone tagged seed (vol 1100)
    a1_final = e._prepare_option_agg(SYM, b1, keep_last_snapshot=False)
    # The bucket-closing flush sees only the seed → zero (already counted).
    assert _classified(a1_final) == 0

    e.options_buffer[SYM] = [prev_snap]
    e.options_buffer[SYM].append(_snap(SYM, ts2, volume=1130))
    a2 = e._prepare_option_agg(SYM, b2, keep_last_snapshot=True)

    # REGRESSION: b2 == true per-minute delta (1130 - 1100 = 30), NOT the
    # stale-baseline-relative 1130 - 100 = 1030 that drove the sawtooth.
    assert _classified(a2) == 30
