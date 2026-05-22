"""Regression tests for per-contract session-cumulative classified flow.

The prior design wrote ``option_chains.ask_volume / mid_volume / bid_volume``
as per-bucket additive values via an additive upsert
(``ask_volume = option_chains.ask_volume + EXCLUDED.ask_volume``).  That
stack required a TTL-backed baseline cache, a SEED_FLAG marker on
carried-over snapshots, an optimistic-advance step, and a pre-commit /
commit-phase failure fork.  When any link broke (the original
sawtooth bug: bare-string optimistic-advance key vs. tuple read key)
the columns could inflate by ~30 minutes of double-counted flow.

The replacement keeps a single ``_FlowAccumulator`` per
``(option_symbol, ET session date)`` holding running session-cumulative
totals (matching what ``flow_contract_facts`` already derived via
``LAG()`` deltas — and what the cumulative ``volume`` column already
used).  These tests pin the invariants that fall out of that:

* Each snapshot advances the cumulative once and only once
  (idempotent under replay).
* The bucket-rollover ``keep_last_snapshot`` path no longer needs a
  marker — the watermark in the accumulator already records the
  carried snapshot's volume.
* Aggregated rows expose the accumulator's cumulative, so the
  downstream LAG-delta consumer recovers the correct per-bucket flow.
* Cross-bucket flush attribution uses the buffered timestamp, not
  wall-clock.
"""

import threading
from datetime import date, datetime
import time as _time

import pytz

from src.ingestion.main_engine import IngestionEngine, _FlowAccumulator
from src.validation import bucket_timestamp

ET = pytz.timezone("US/Eastern")


def _agg_engine() -> IngestionEngine:
    e = IngestionEngine.__new__(IngestionEngine)
    e.options_buffer = {}
    e._option_flow = {}
    e._option_flow_lock = threading.Lock()
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


def _seed_accumulator(
    e: IngestionEngine,
    sym: str,
    bucket: datetime,
    *,
    last_volume_cum: int = 0,
    ask: int = 0,
    mid: int = 0,
    bid: int = 0,
) -> _FlowAccumulator:
    """Install a hydrated accumulator without hitting the DB."""
    acc = _FlowAccumulator(
        session_date=e._bucket_session_date(bucket),
        last_volume_cum=last_volume_cum,
        ask_cum=ask,
        mid_cum=mid,
        bid_cum=bid,
    )
    with e._option_flow_lock:
        e._option_flow[sym] = acc
    return acc


def test_agg_row_exposes_session_cumulative_classified_flow():
    """Agg row's ask/mid/bid_volume == accumulator's cumulative totals.
    This is the contract the downstream LAG-delta query in
    flow_contract_facts already assumes for these columns."""
    e = _agg_engine()
    # Pre-existing session state from a hydrate or prior buckets.
    _seed_accumulator(e, SYM, BUCKET, last_volume_cum=1000, ask=120, mid=80, bid=40)
    e.options_buffer[SYM] = [_snap(SYM, TS, volume=1500)]

    acc = e._get_flow_accumulator(SYM, BUCKET)
    # Simulate _store_option_batch's per-snapshot classify-on-arrival.
    e._ingest_snapshot_into_accumulator(acc, e.options_buffer[SYM][-1], BUCKET)

    agg = e._prepare_option_agg(SYM, BUCKET, keep_last_snapshot=True)
    assert agg is not None
    # 1500 - 1000 = 500 new classified flow this snapshot, routed to mid
    # (last==mid by construction).  Cumulative becomes 80 + 500 = 580.
    assert agg["mid_volume"] == 580
    assert agg["ask_volume"] == 120
    assert agg["bid_volume"] == 40
    # Volume column carries the same session-cumulative semantics.
    assert agg["volume"] == 1500
    # ask + mid + bid + opening-auction carve-out invariant: in a
    # non-opening bucket with last==mid, all new flow lands in mid_cum,
    # so cumulative classified sum equals the cumulative volume only
    # when all prior flow was also classified (here: 580+120+40=740,
    # 1500-740=760 was pre-hydrate unclassified volume).
    assert _classified(agg) == 740


def test_replaying_same_snapshot_does_not_double_count():
    """The watermark in the accumulator makes ingest idempotent: replaying
    the same snapshot is a no-op for the cumulative.  This is what makes
    retain-and-retry safe under the unified failure path."""
    e = _agg_engine()
    _seed_accumulator(e, SYM, BUCKET)
    snap = _snap(SYM, TS, volume=1000)
    e.options_buffer[SYM] = [snap]

    acc = e._get_flow_accumulator(SYM, BUCKET)
    e._ingest_snapshot_into_accumulator(acc, snap, BUCKET)
    first_mid = acc.mid_cum

    # Replay the same snapshot (same cumulative volume) — vol_delta = 0.
    e._ingest_snapshot_into_accumulator(acc, snap, BUCKET)
    assert acc.mid_cum == first_mid
    assert acc.last_volume_cum == 1000


def test_bucket_rollover_no_double_count_without_seed_flag():
    """Carrying the previous bucket's last snapshot into the new bucket
    must not re-classify its volume.  With the in-memory watermark, this
    falls out automatically — no SEED_FLAG marker required."""
    e = _agg_engine()
    _seed_accumulator(e, SYM, BUCKET)
    ts1 = ET.localize(datetime(2026, 5, 15, 10, 15, 5))
    ts2 = ET.localize(datetime(2026, 5, 15, 10, 16, 3))
    b1 = bucket_timestamp(ts1, 60)
    b2 = bucket_timestamp(ts2, 60)

    snap1 = _snap(SYM, ts1, volume=1000)
    snap2 = _snap(SYM, ts2, volume=1080)

    # Ingest snap1 (b1) then snap2 (b2). The carried-over snap1 in the
    # b2 buffer doesn't get re-ingested — _store_option_batch only calls
    # _ingest_snapshot_into_accumulator for each arriving snapshot, not
    # for re-scanned buffer contents.
    acc = e._get_flow_accumulator(SYM, b1)
    e._ingest_snapshot_into_accumulator(acc, snap1, b1)
    e.options_buffer[SYM] = [snap1]
    agg1 = e._prepare_option_agg(SYM, b1, keep_last_snapshot=True)
    # b1 row: cumulative is 1000, last==mid so all in mid.
    assert agg1["mid_volume"] == 1000

    # b2: ingest snap2 into the same (now-rollover) accumulator.
    e.options_buffer[SYM].append(snap2)
    acc2 = e._get_flow_accumulator(SYM, b2)
    e._ingest_snapshot_into_accumulator(acc2, snap2, b2)
    agg2 = e._prepare_option_agg(SYM, b2, keep_last_snapshot=True)
    # b2 row: cumulative now 1080. LAG-delta downstream = 1080 - 1000 = 80
    # — the true per-bucket flow.  No double counting from the carried seed.
    assert agg2["mid_volume"] == 1080
    assert agg2["volume"] == 1080


def test_session_rollover_resets_accumulator():
    """A bucket in a new ET session date triggers a fresh hydrate.  Stub
    out the DB call to simulate a cold start (no prior rows today)."""
    e = _agg_engine()
    monday_ts = ET.localize(datetime(2026, 5, 18, 10, 15, 5))
    tuesday_ts = ET.localize(datetime(2026, 5, 19, 10, 15, 5))
    b_mon = bucket_timestamp(monday_ts, 60)
    b_tue = bucket_timestamp(tuesday_ts, 60)

    _seed_accumulator(e, SYM, b_mon, last_volume_cum=5000, ask=2000, mid=1000, bid=2000)

    # Make _hydrate_flow_accumulator return zeros (no DB) for the Tuesday call.
    def _zero_hydrate(_sym: str, sd: date) -> _FlowAccumulator:
        return _FlowAccumulator(
            session_date=sd,
            last_volume_cum=0,
            ask_cum=0,
            mid_cum=0,
            bid_cum=0,
        )

    e._hydrate_flow_accumulator = _zero_hydrate  # type: ignore[method-assign]

    mon_acc = e._get_flow_accumulator(SYM, b_mon)
    assert mon_acc.last_volume_cum == 5000

    tue_acc = e._get_flow_accumulator(SYM, b_tue)
    # Different session date → fresh hydrate, NOT the Monday state.
    assert tue_acc.last_volume_cum == 0
    assert tue_acc.ask_cum == 0


def test_flush_all_buffers_buckets_by_buffered_timestamp_not_wallclock():
    """The timeout/shutdown safety flush must attribute volume to the
    minute the ticks actually traded, not the wall-clock minute the flush
    fires in (mirrors the buffer-overflow path)."""
    e = _agg_engine()
    e.underlying_buffer = []
    e.last_flush_time = datetime.now(ET)
    written: list = []
    e._write_option_rows = lambda rows: written.extend(rows)  # type: ignore[method-assign]

    # Buffered ticks traded at 10:15 ET; the flush "fires" whenever the
    # test runs (definitely not 2026-05-15 10:15 ET).
    ts_a = ET.localize(datetime(2026, 5, 15, 10, 15, 10))
    ts_b = ET.localize(datetime(2026, 5, 15, 10, 15, 50))
    minute_1015 = bucket_timestamp(ts_a, 60)
    snap_a = _snap(SYM, ts_a, volume=1000)
    snap_b = _snap(SYM, ts_b, volume=1080)
    e.options_buffer[SYM] = [snap_a, snap_b]

    # Simulate the in-memory state _store_option_batch would have
    # produced by classify-on-arrival for both snapshots.
    _seed_accumulator(e, SYM, minute_1015)
    acc = e._get_flow_accumulator(SYM, minute_1015)
    e._ingest_snapshot_into_accumulator(acc, snap_a, minute_1015)
    e._ingest_snapshot_into_accumulator(acc, snap_b, minute_1015)

    e._flush_all_buffers()

    assert len(written) == 1
    agg = written[0]
    # Bucketed to the minute the ticks traded, NOT datetime.now().
    assert agg["timestamp"] == minute_1015
    # Cumulative classified flow for the bucket: 1080 (all in mid).
    assert _classified(agg) == 1080
