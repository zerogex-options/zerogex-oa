"""Bucket-boundary flow attribution must not leak across the minute.

`_store_option_batch` finalizes the previous 1-minute bucket on a
rollover and stamps the row with the accumulator's session-cumulative
totals.  Previously it advanced the accumulator with the *new* bucket's
snapshot BEFORE finalizing the previous bucket, so the previous bucket's
stored cumulative absorbed the first tick of the new minute.  The
downstream LAG-delta reader (api/database.py) then attributed that first
tick to the prior minute (overcount t-1, undercount t).

This pins the fix: the finalized previous-bucket row carries only the
cumulative as of its own boundary.
"""

import threading
from collections import defaultdict
from datetime import date, datetime

import pytz

from src.ingestion.main_engine import IngestionEngine, _FlowAccumulator
from src.validation import bucket_timestamp

ET = pytz.timezone("US/Eastern")
SYM = "SPY260515P00739000"


def _engine():
    e = IngestionEngine.__new__(IngestionEngine)
    e.options_buffer = defaultdict(list)
    e._option_flow = {}
    e._option_flow_lock = threading.Lock()
    e._option_bucket_last_write = {}
    e._classify_fallback_count = 0
    e.errors_count = 0
    e._written_rows = []

    # Identity Greeks enrichment + capture writes; never touch the DB.
    e._enrich_with_greeks = lambda data: data
    e._write_option_rows = lambda rows: e._written_rows.append([dict(r) for r in rows])
    # First-sight hydrate returns a zeroed accumulator (no DB).
    e._hydrate_flow_accumulator = lambda sym, sd: _FlowAccumulator(
        session_date=sd, last_volume_cum=0, ask_cum=0, mid_cum=0, bid_cum=0
    )
    return e


def _snap(ts, volume):
    # last == mid -> all classified flow lands in mid_volume.
    return {
        "option_symbol": SYM,
        "timestamp": ts,
        "underlying": "SPY",
        "strike": 739.0,
        "expiration": date(2026, 5, 15),
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


def test_previous_bucket_excludes_next_minute_first_tick():
    e = _engine()
    ts1 = ET.localize(datetime(2026, 5, 15, 10, 15, 5))
    ts2 = ET.localize(datetime(2026, 5, 15, 10, 16, 3))
    b1 = bucket_timestamp(ts1, 60)
    b2 = bucket_timestamp(ts2, 60)

    # Minute 1: cumulative 1000.
    e._store_option_batch([_snap(ts1, 1000)])
    # Minute 2: cumulative 1080 (80 traded across the boundary).
    e._store_option_batch([_snap(ts2, 1080)])

    # Find the finalized b1 row (keep_last_snapshot=False) written during
    # the second batch's rollover handling.
    b1_rows = [r for batch in e._written_rows for r in batch if r["timestamp"] == b1]
    b2_rows = [r for batch in e._written_rows for r in batch if r["timestamp"] == b2]
    assert b1_rows, "expected a finalized previous-bucket row"
    assert b2_rows, "expected a current-bucket row"

    # The previous bucket's cumulative must be 1000 — NOT 1080. If the
    # accumulator had advanced before the finalize, this would be 1080.
    assert b1_rows[-1]["volume"] == 1000
    assert b1_rows[-1]["mid_volume"] == 1000

    # The new bucket carries the full cumulative; LAG-delta = 1080-1000 = 80
    # correctly lands in minute 2, not minute 1.
    assert b2_rows[-1]["volume"] == 1080
    assert b2_rows[-1]["mid_volume"] == 1080
