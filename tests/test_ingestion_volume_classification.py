"""Unit tests for volume classification (Lee-Ready prior-tick + mid-band).

Covers ``IngestionEngine._classify_volume_chunk`` and the opening-auction
helper. These pin the classification semantics that drive ask/bid/mid
volume in ``option_chains``.
"""

import threading
from datetime import datetime

import pytz

from src.ingestion.main_engine import IngestionEngine

ET = pytz.timezone("US/Eastern")


def _engine() -> IngestionEngine:
    return IngestionEngine.__new__(IngestionEngine)


def test_classify_at_ask_is_ask_volume():
    av, mv, bv = _engine()._classify_volume_chunk(100, last=5.58, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (100, 0, 0)


def test_classify_at_bid_is_bid_volume():
    av, mv, bv = _engine()._classify_volume_chunk(100, last=5.53, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (0, 0, 100)


def test_classify_user_5_57_case_routes_to_mid_with_default_band():
    # The reported case: user sold 250 contracts at 5.57 with bid 5.53,
    # ask 5.58, mid 5.555. Half-spread = 0.025; default band 0.70 puts
    # the ask threshold at mid + 0.70*0.025 = 5.5725, so a 5.57 fill is
    # below the threshold and is routed to mid_volume rather than getting
    # full ask credit.
    av, mv, bv = _engine()._classify_volume_chunk(250, last=5.57, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (0, 250, 0)


def test_classify_clear_ask_print_outside_band():
    # 5.578 is well above the 5.5700 threshold -> still ask_volume.
    av, mv, bv = _engine()._classify_volume_chunk(100, last=5.578, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (100, 0, 0)


def test_classify_band_zero_is_pure_lee_ready():
    # band_pct=0 => any print strictly above mid is ask_volume, below is bid.
    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.557, bid=5.53, ask=5.58, mid=5.555, band_pct=0.0
    )
    assert (av, mv, bv) == (100, 0, 0)

    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.553, bid=5.53, ask=5.58, mid=5.555, band_pct=0.0
    )
    assert (av, mv, bv) == (0, 0, 100)


def test_classify_band_one_only_counts_at_quote_as_ask_bid():
    # band_pct=1 => mid zone covers the full inside spread; only prints at
    # or beyond the quote count as ask/bid.
    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.575, bid=5.53, ask=5.58, mid=5.555, band_pct=1.0
    )
    assert (av, mv, bv) == (0, 100, 0)

    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.585, bid=5.53, ask=5.58, mid=5.555, band_pct=1.0
    )
    assert (av, mv, bv) == (100, 0, 0)


def test_classify_band_clamped_to_unit_interval():
    # Out-of-range band shouldn't invert the zones; >1 behaves like 1.
    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.575, bid=5.53, ask=5.58, mid=5.555, band_pct=5.0
    )
    assert (av, mv, bv) == (0, 100, 0)


def test_classify_zero_volume_returns_zeros():
    assert _engine()._classify_volume_chunk(0, 5.0, 4.95, 5.05, 5.0) == (0, 0, 0)


def test_classify_missing_last_defaults_to_mid():
    av, mv, bv = _engine()._classify_volume_chunk(50, last=None, bid=4.95, ask=5.05, mid=5.0)
    assert (av, mv, bv) == (0, 50, 0)


def test_classify_missing_bid_and_ask_defaults_to_mid():
    av, mv, bv = _engine()._classify_volume_chunk(50, last=5.0, bid=None, ask=None, mid=None)
    assert (av, mv, bv) == (0, 50, 0)


def test_classify_falls_back_to_nearest_neighbor_when_only_one_side_known():
    # Only ask known, last very close to it -> still classifies as ask.
    av, mv, bv = _engine()._classify_volume_chunk(50, last=5.05, bid=None, ask=5.05, mid=5.0)
    assert (av, mv, bv) == (50, 0, 0)


def test_opening_auction_bucket_detected_at_0930_et():
    bucket_open = ET.localize(datetime(2026, 4, 28, 9, 30))
    bucket_post_open = ET.localize(datetime(2026, 4, 28, 9, 31))
    bucket_pre_open = ET.localize(datetime(2026, 4, 28, 9, 29))
    assert IngestionEngine._is_opening_auction_bucket(bucket_open) is True
    assert IngestionEngine._is_opening_auction_bucket(bucket_post_open) is False
    assert IngestionEngine._is_opening_auction_bucket(bucket_pre_open) is False


def test_opening_auction_helper_handles_utc_input():
    # 13:30 UTC == 09:30 ET during US daylight time.
    bucket_utc = pytz.UTC.localize(datetime(2026, 4, 28, 13, 30))
    assert IngestionEngine._is_opening_auction_bucket(bucket_utc) is True


def test_opening_auction_helper_naive_datetime_treated_as_et():
    naive = datetime(2026, 4, 28, 9, 30)
    assert IngestionEngine._is_opening_auction_bucket(naive) is True


def test_accumulator_prior_tick_carries_across_snapshots():
    """The flow accumulator stores the most recent NBBO and reuses it as
    the prior-tick quote for the next classification — same behavior the
    deleted ``_option_last_quote`` cache provided, now consolidated into
    the per-contract accumulator."""
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    engine._option_flow = {}
    engine._option_flow_lock = threading.Lock()

    bucket = ET.localize(datetime(2026, 4, 28, 10, 15))
    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )

    # First snapshot: no prior NBBO yet, classifier falls through to the
    # snapshot's own quote (degraded but unavoidable cold-start path).
    # last=5.58 sits at the ask, above the default mid-band threshold
    # (5.555 + 0.7*0.025 = 5.5725 → 5.58 is above → ask_volume).
    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 100, "last": 5.58, "bid": 5.53, "ask": 5.58, "mid": 5.555},
        bucket,
    )
    assert acc.last_volume_cum == 100
    assert acc.ask_cum == 100  # first 100 classified as ask
    # After ingesting, the accumulator captured the snapshot's NBBO as
    # the prior tick for next time.
    assert acc.last_bid == 5.53
    assert acc.last_ask == 5.58
    assert acc.last_mid == 5.555

    # Second snapshot with a new quote and another at-ask print:
    # classification uses the *prior* NBBO (5.53/5.58), so the 50 new
    # contracts classify as ask, bringing ask_cum to 150.
    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 150, "last": 5.58, "bid": 5.54, "ask": 5.59, "mid": 5.565},
        bucket,
    )
    assert acc.last_volume_cum == 150
    assert acc.ask_cum == 150  # cumulative: 100 + 50
    # Prior tick advanced to the new NBBO for the *next* classification.
    assert acc.last_bid == 5.54
    assert acc.last_ask == 5.59


def test_accumulator_ingest_is_idempotent_for_same_cumulative():
    """Replaying the same snapshot (same TS-reported cumulative volume)
    contributes zero new flow. This is what makes retain-and-retry safe
    without the prior design's pre-commit / commit-phase fork."""
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    bucket = ET.localize(datetime(2026, 4, 28, 10, 15))
    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )

    snap = {"volume": 100, "last": 5.58, "bid": 5.53, "ask": 5.58, "mid": 5.555}
    engine._ingest_snapshot_into_accumulator(acc, snap, bucket)
    ask_after_first = acc.ask_cum
    mid_after_first = acc.mid_cum
    bid_after_first = acc.bid_cum

    # Replay the same snapshot — cumulative watermark doesn't advance,
    # so no new flow is attributed.
    engine._ingest_snapshot_into_accumulator(acc, snap, bucket)
    assert acc.ask_cum == ask_after_first
    assert acc.mid_cum == mid_after_first
    assert acc.bid_cum == bid_after_first
