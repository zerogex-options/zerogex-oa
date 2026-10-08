"""Unit tests for volume classification (Lee-Ready prior-tick + mid-band).

Covers ``IngestionEngine._classify_volume_chunk`` and the opening-auction
helper. These pin the classification semantics that drive ask/bid/mid
volume in ``option_chains``.
"""

import threading
from datetime import date, datetime
from unittest.mock import patch

import pytz

from src.ingestion import main_engine
from src.ingestion.main_engine import IngestionEngine, _FlowAccumulator

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


def test_classify_locked_quote_above_routes_to_ask_volume():
    # bid == ask is a legitimate locked market (common on tight ATM /
    # illiquid contracts), not a degraded quote.  A print ABOVE the
    # locked price is a buyer lifting the locked offer -> ask_volume.
    # Before the fix, ``ask <= bid`` swept this into the nearest-neighbor
    # fallback, where all three distances were equal and every locked
    # print degenerated to mid_volume regardless of trade direction --
    # and every first-of-process locked print also fired the WARN.
    engine = _engine()
    av, mv, bv = engine._classify_volume_chunk(100, last=2.55, bid=2.5, ask=2.5, mid=2.5)
    assert (av, mv, bv) == (100, 0, 0)
    # The fallback counter MUST NOT increment for a locked market.
    assert getattr(engine, "_classify_fallback_count", 0) == 0


def test_classify_locked_quote_below_routes_to_bid_volume():
    # Print BELOW the locked price is a seller hitting the locked bid.
    engine = _engine()
    av, mv, bv = engine._classify_volume_chunk(100, last=1.35, bid=1.38, ask=1.38, mid=1.38)
    assert (av, mv, bv) == (0, 0, 100)
    assert getattr(engine, "_classify_fallback_count", 0) == 0


def test_classify_locked_quote_at_price_routes_to_mid_volume():
    # Print AT the locked price is ambiguous direction => mid_volume.
    engine = _engine()
    av, mv, bv = engine._classify_volume_chunk(100, last=2.5, bid=2.5, ask=2.5, mid=2.5)
    assert (av, mv, bv) == (0, 100, 0)
    assert getattr(engine, "_classify_fallback_count", 0) == 0


def test_classify_truly_crossed_quote_still_fires_fallback():
    # ask < bid (genuinely crossed -- stale or corrupt feed) MUST still
    # trip the fallback so the data-quality WARN remains visible.
    engine = _engine()
    av, mv, bv = engine._classify_volume_chunk(100, last=5.0, bid=5.10, ask=4.90, mid=5.0)
    # Crossed: dist_to_ask=0.10, dist_to_mid=0.0, dist_to_bid=0.10 ->
    # nearest is mid, so mid_volume wins under nearest-neighbor.
    assert (av, mv, bv) == (0, 100, 0)
    assert engine._classify_fallback_count == 1


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


def test_accumulator_reanchors_watermark_on_vendor_cumulative_reset():
    """TradeStation resets per-contract cumulative volume at 09:30 ET.
    The accumulator's ET-calendar-day session means the pre-cash watermark
    (seeded from prior-day residual at 00:00 ET) would otherwise swallow
    every cash-session trade whose post-reset cumulative is below it.
    Regression test for the user-reported case: 1214-volume residual at
    midnight ET, then 200 contracts traded at 09:35/09:48 ET — under the
    pre-fix writer those 200 trades produced vol_delta=0 forever and
    storage stayed pinned at 1214 (GREATEST upsert)."""
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()

    # Accumulator seeded with prior-day residual the way the first
    # 00:00 ET snapshot would have left it: 1214 cumulative volume, all
    # classified as ask (last==ask at that pre-cash snapshot).
    pre_cash_bucket = ET.localize(datetime(2026, 4, 28, 0, 0))
    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=1214,
        ask_cum=1214,
        mid_cum=0,
        bid_cum=0,
        last_bid=5.66,
        last_ask=5.70,
        last_mid=5.68,
    )
    assert engine._is_opening_auction_bucket(pre_cash_bucket) is False

    # 09:35 ET: user trades 100 contracts.  TS has reset cumulative to 0
    # at 09:30 ET, so the snapshot's volume is now 100 (not 1314).
    # Pre-fix: vol_delta = max(100 - 1214, 0) = 0 -> trade dropped.
    # Post-fix: watermark re-anchors to 0, vol_delta = 100 -> classified.
    bucket_0935 = ET.localize(datetime(2026, 4, 28, 9, 35))
    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 100, "last": 5.70, "bid": 5.66, "ask": 5.70, "mid": 5.68},
        bucket_0935,
    )
    assert acc.last_volume_cum == 100, "watermark must re-anchor on vendor reset"
    # Classified totals stay monotonic across the reset so the reader's
    # LAG against the pre-reset bar surfaces the correct per-bar delta
    # (1314 - 1214 = 100).  Resetting them would zero the LAG delta.
    assert acc.ask_cum == 1314, "classified ask_cum must stay monotonic across reset"
    assert acc.mid_cum == 0
    assert acc.bid_cum == 0

    # 09:48 ET: user closes the position, +100 more contracts.  TS cum
    # advances 100 -> 200.  Normal post-reset delta path.
    bucket_0948 = ET.localize(datetime(2026, 4, 28, 9, 48))
    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 200, "last": 5.70, "bid": 5.66, "ask": 5.70, "mid": 5.68},
        bucket_0948,
    )
    assert acc.last_volume_cum == 200
    assert acc.ask_cum == 1414, "second 100 contracts must accumulate"


def test_accumulator_reset_detection_does_not_fire_on_monotonic_advance():
    """Sanity: reset detection must only trigger on a true decrease.
    A normal session advance (curr_vol > watermark) leaves classified
    totals untouched and just advances the watermark, same as before."""
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    bucket = ET.localize(datetime(2026, 4, 28, 10, 15))
    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=500,
        ask_cum=300,
        mid_cum=100,
        bid_cum=100,
        last_bid=5.53,
        last_ask=5.58,
        last_mid=5.555,
    )

    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 600, "last": 5.58, "bid": 5.53, "ask": 5.58, "mid": 5.555},
        bucket,
    )
    assert acc.last_volume_cum == 600
    # 100-contract advance classified as ask -> ask_cum 300 -> 400.
    assert acc.ask_cum == 400
    assert acc.mid_cum == 100
    assert acc.bid_cum == 100


def test_stale_prior_tick_falls_back_to_contemporaneous_quote():
    """Regression for the user-reported SELL-tagged-BUY inversion.

    Reconstructs the reported 10:06->10:07 ET option_chains rows for
    SPY 260605P752: the contract was quiet (volume 560, last NBBO recorded
    at 10:06 = 2.59/2.61), then the price gapped up and the user's 50-lot
    SELL hit the 2.65 bid at 10:07 (quote now 2.65/2.67).

    Against the STALE 10:06 prior tick (mid 2.60) the 2.65 print reads as a
    lift (2.65 > 2.60) -> ask_volume -> "BUY": exactly the inversion the
    user saw (ask_volume rose 70 -> 120 in the DB).  With the staleness
    guard the minute-old prior tick is rejected and the trade is scored
    against the contemporaneous 2.65/2.67 quote (mid 2.66), where 2.65 is
    below mid -> bid_volume -> SELL.
    """
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    # Accumulator state at the end of the 10:06 ET bucket (from the DB row).
    acc = _FlowAccumulator(
        session_date=date(2026, 5, 29),
        last_volume_cum=560,
        ask_cum=70,
        mid_cum=76,
        bid_cum=2836,
        last_bid=2.59,
        last_ask=2.61,
        last_mid=2.60,
        last_quote_ts=ET.localize(datetime(2026, 5, 29, 10, 6)),
    )

    # 10:07 ET: user's 50-lot SELL prints at the 2.65 bid; NBBO has gapped
    # up to 2.65 x 2.67.  The prior tick is ~60s stale.
    engine._ingest_snapshot_into_accumulator(
        acc,
        {
            "volume": 610,
            "last": 2.65,
            "bid": 2.65,
            "ask": 2.67,
            "mid": 2.66,
            "timestamp": ET.localize(datetime(2026, 5, 29, 10, 7)),
        },
        ET.localize(datetime(2026, 5, 29, 10, 7)),
    )

    # The +50 must land in bid_volume (SELL), NOT ask_volume (BUY).
    assert acc.bid_cum == 2886, "stale prior tick must not credit a bid-hit as ask"
    assert acc.ask_cum == 70
    assert acc.mid_cum == 76


def test_fresh_prior_tick_is_still_used_for_marketable_lift():
    """A RECENT prior tick is still preferred over the contemporaneous quote.

    This preserves the anti-contamination purpose of the prior-tick rule:
    a marketable BUY that lifts the offer and bumps the NBBO up within the
    same drain must be scored against the PRE-trade quote, not the
    post-trade quote it just created.

    Pre-trade quote 2.55/2.57 (recorded 1s ago); the buy lifts the 2.57
    offer and the NBBO moves to 2.57/2.59.  Against the fresh prior tick
    (mid 2.56) the 2.57 print is a lift -> ask_volume -> BUY (correct).
    Against the contemporaneous post-trade quote (mid 2.58) it would read
    2.57 < mid -> bid_volume -> SELL (the contamination the guard avoids).
    """
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    acc = _FlowAccumulator(
        session_date=date(2026, 5, 29),
        last_volume_cum=100,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
        last_bid=2.55,
        last_ask=2.57,
        last_mid=2.56,
        last_quote_ts=ET.localize(datetime(2026, 5, 29, 10, 0, 0)),
    )

    engine._ingest_snapshot_into_accumulator(
        acc,
        {
            "volume": 150,
            "last": 2.57,
            "bid": 2.57,
            "ask": 2.59,
            "mid": 2.58,
            "timestamp": ET.localize(datetime(2026, 5, 29, 10, 0, 1)),  # 1s later
        },
        ET.localize(datetime(2026, 5, 29, 10, 0, 1)),
    )

    # Fresh prior tick -> the lift is correctly ask_volume (BUY).
    assert acc.ask_cum == 50, "fresh prior tick must score the lift as ask (buy)"
    assert acc.bid_cum == 0
    assert acc.mid_cum == 0


def test_prior_tick_age_threshold_is_configurable(monkeypatch):
    """Setting the max-age to 0 disables the guard (legacy prior-tick)."""
    import src.ingestion.main_engine as me
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    monkeypatch.setattr(me, "FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS", 0.0)

    engine = _engine()
    acc = _FlowAccumulator(
        session_date=date(2026, 5, 29),
        last_volume_cum=560,
        ask_cum=70,
        mid_cum=76,
        bid_cum=2836,
        last_bid=2.59,
        last_ask=2.61,
        last_mid=2.60,
        last_quote_ts=ET.localize(datetime(2026, 5, 29, 10, 6)),
    )
    engine._ingest_snapshot_into_accumulator(
        acc,
        {
            "volume": 610,
            "last": 2.65,
            "bid": 2.65,
            "ask": 2.67,
            "mid": 2.66,
            "timestamp": ET.localize(datetime(2026, 5, 29, 10, 7)),
        },
        ET.localize(datetime(2026, 5, 29, 10, 7)),
    )
    # Guard disabled -> legacy stale-prior-tick behavior (the bug): ask.
    assert acc.ask_cum == 120
    assert acc.bid_cum == 2836


def test_missing_volume_snapshot_does_not_re_anchor_watermark():
    """Regression for the 06:40 ET phantom 'reset' on SPY 260605P752.

    On a stream reconnect the merger's ``_state`` is cleared; if the first
    message back lacks a ``Volume`` field, ``snap.get("volume")`` is None
    and ``int(None or 0) == 0`` reaches the accumulator.  Before the fix,
    ``curr_vol < last_volume_cum`` zeroed the watermark and the next
    written row carried ``volume = 0`` while ``bid_volume`` kept its 2422
    baseline by design — the exact cliff the user saw.

    The re-anchor must only fire on a *positive* below-watermark
    cumulative (a real vendor reset), so a missing Volume preserves the
    watermark instead of dropping a phantom zero into storage.
    """
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    acc = _FlowAccumulator(
        session_date=date(2026, 5, 29),
        last_volume_cum=2422,
        ask_cum=0,
        mid_cum=0,
        bid_cum=2422,
        last_bid=3.37,
        last_ask=3.39,
        last_mid=3.38,
    )
    bucket = ET.localize(datetime(2026, 5, 29, 6, 40))

    # Snapshot from a fresh post-reconnect merge: NBBO present, Volume absent.
    engine._ingest_snapshot_into_accumulator(
        acc,
        {"last": 3.37, "bid": 3.37, "ask": 3.39, "mid": 3.38},
        bucket,
    )

    # Watermark must hold; no phantom reset.
    assert acc.last_volume_cum == 2422, "missing Volume must not trigger a re-anchor"
    assert acc.ask_cum == 0
    assert acc.mid_cum == 0
    assert acc.bid_cum == 2422


def test_explicit_zero_volume_snapshot_does_not_re_anchor_watermark():
    """Defensive: even if a literal Volume=0 slips past the merger's
    skip-when-zero gate (stream_manager._merge_single_quote), the
    accumulator must not treat it as a vendor reset.  Only a *positive*
    below-watermark cumulative counts."""
    from src.ingestion.main_engine import _FlowAccumulator
    from datetime import date

    engine = _engine()
    acc = _FlowAccumulator(
        session_date=date(2026, 5, 29),
        last_volume_cum=2422,
        ask_cum=0,
        mid_cum=0,
        bid_cum=2422,
        last_bid=3.37,
        last_ask=3.39,
        last_mid=3.38,
    )
    bucket = ET.localize(datetime(2026, 5, 29, 6, 40))

    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 0, "last": 3.37, "bid": 3.37, "ask": 3.39, "mid": 3.38},
        bucket,
    )

    assert acc.last_volume_cum == 2422
    assert acc.bid_cum == 2422


# ---------------------------------------------------------------------------
# FLOW_CLASSIFIER=tick
#
# The tick test grades a trade against the PREVIOUS TRADE PRICE and reads no
# quote, so the Market Value penny randomisation cannot reach it (F10/F11).
# The classifier itself is already tested; what is new and untested is the
# STATE THREADING -- carrying the prior trade and the inherited direction
# across snapshots, and surviving a mid-session restart.
# ---------------------------------------------------------------------------


def _tick_engine(monkeypatch) -> IngestionEngine:
    engine = IngestionEngine.__new__(IngestionEngine)
    engine._option_flow_lock = threading.Lock()
    monkeypatch.setattr(main_engine, "FLOW_CLASSIFIER", "tick")
    return engine


def _acc(**kw) -> "_FlowAccumulator":
    base = dict(
        session_date=date(2026, 10, 9),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )
    base.update(kw)
    return _FlowAccumulator(**base)


def test_quote_classifier_is_the_default():
    """Production must not change behaviour because this flag exists."""
    from src.config import FLOW_CLASSIFIER

    assert FLOW_CLASSIFIER == "quote"


def test_tick_uptick_is_ask_volume(monkeypatch):
    engine = _tick_engine(monkeypatch)
    bucket = ET.localize(datetime(2026, 10, 9, 10, 15))
    acc = _acc(last_trade_price=5.57)

    engine._ingest_snapshot_into_accumulator(
        acc,
        # Quote deliberately contradicts the tick: at 5.53/5.58 a 5.58 print
        # is at the ask either way, so instead put the trade BELOW the mid
        # where Lee-Ready would call it a sell. The uptick must win.
        {"volume": 100, "last": 5.58, "bid": 9.00, "ask": 9.10, "mid": 9.05},
        bucket,
    )

    assert (acc.ask_cum, acc.mid_cum, acc.bid_cum) == (100, 0, 0)
    assert acc.tick_direction == 1
    assert acc.last_trade_price == 5.58, "the prior trade must advance"


def test_tick_downtick_is_bid_volume(monkeypatch):
    engine = _tick_engine(monkeypatch)
    bucket = ET.localize(datetime(2026, 10, 9, 10, 15))
    acc = _acc(last_trade_price=5.58)

    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 100, "last": 5.53, "bid": 0.01, "ask": 0.02, "mid": 0.015},
        bucket,
    )

    assert (acc.ask_cum, acc.mid_cum, acc.bid_cum) == (0, 0, 100)
    assert acc.tick_direction == -1


def test_a_zero_tick_inherits_the_carried_direction(monkeypatch):
    """The whole reason direction is state and not a local."""
    engine = _tick_engine(monkeypatch)
    bucket = ET.localize(datetime(2026, 10, 9, 10, 15))
    acc = _acc(last_trade_price=5.50)

    # Uptick establishes direction...
    engine._ingest_snapshot_into_accumulator(
        acc, {"volume": 100, "last": 5.58, "bid": 5.53, "ask": 5.58, "mid": 5.555}, bucket
    )
    assert acc.tick_direction == 1
    # ...and a flat print at the same price inherits it rather than going mid.
    engine._ingest_snapshot_into_accumulator(
        acc, {"volume": 180, "last": 5.58, "bid": 5.53, "ask": 5.58, "mid": 5.555}, bucket
    )

    assert acc.ask_cum == 180, "80 more at the same price keep the up direction"
    assert acc.mid_cum == 0
    assert acc.tick_direction == 1


def test_the_prior_trade_advances_even_when_no_volume_traded(monkeypatch):
    """A hole in the tick sequence carries the wrong direction forward.

    A contract whose cumulative volume did not move can still print a new
    last between polls. If the accumulator only advanced on classified
    snapshots, the NEXT trade would be graded against a stale price and the
    direction would be wrong from there on.
    """
    engine = _tick_engine(monkeypatch)
    bucket = ET.localize(datetime(2026, 10, 9, 10, 15))
    acc = _acc(last_volume_cum=100, last_trade_price=5.50)

    # No volume delta: nothing classifies, but the print is newer.
    engine._ingest_snapshot_into_accumulator(
        acc, {"volume": 100, "last": 5.60, "bid": 5.55, "ask": 5.60, "mid": 5.575}, bucket
    )
    assert (acc.ask_cum, acc.mid_cum, acc.bid_cum) == (0, 0, 0)
    assert acc.last_trade_price == 5.60

    # Now a trade at 5.55 -- a DOWNtick from 5.60. Graded against the stale
    # 5.50 it would have read as an uptick and been credited to buyers.
    engine._ingest_snapshot_into_accumulator(
        acc, {"volume": 150, "last": 5.55, "bid": 5.50, "ask": 5.55, "mid": 5.525}, bucket
    )
    assert acc.bid_cum == 50
    assert acc.ask_cum == 0


def test_the_per_row_invariant_holds_on_the_tick_path(monkeypatch):
    """ask + mid + bid == volume, which schema.sql:181 declares per row."""
    engine = _tick_engine(monkeypatch)
    bucket = ET.localize(datetime(2026, 10, 9, 10, 15))
    acc = _acc(last_trade_price=5.00)

    for cum, last in ((100, 5.10), (250, 5.10), (400, 4.90), (400, 4.90), (900, 5.30)):
        engine._ingest_snapshot_into_accumulator(
            acc, {"volume": cum, "last": last, "bid": 4.80, "ask": 4.85, "mid": 4.825}, bucket
        )
        assert (
            acc.ask_cum + acc.mid_cum + acc.bid_cum == acc.last_volume_cum
        ), f"invariant broken at cumulative={cum}"


def test_a_cold_start_with_no_prior_trade_goes_to_mid(monkeypatch):
    """Nothing to tick against is not a buy and not a sell."""
    engine = _tick_engine(monkeypatch)
    bucket = ET.localize(datetime(2026, 10, 9, 10, 15))
    acc = _acc()

    engine._ingest_snapshot_into_accumulator(
        acc, {"volume": 100, "last": 5.58, "bid": 5.53, "ask": 5.58, "mid": 5.555}, bucket
    )

    assert (acc.ask_cum, acc.mid_cum, acc.bid_cum) == (0, 100, 0)
    assert acc.last_trade_price == 5.58, "but the next trade has something to grade against"


def test_hydration_restores_the_prior_trade_price(monkeypatch):
    """A restart mid-session must resume against the right prior trade.

    This is the ONLY reason last_trade_price is persisted rather than kept
    in memory. Hydrate it as None and every contract's first trade after a
    restart falls to mid_volume instead of being classified -- silently,
    with no error and no log line, on every contract at once.
    """
    from unittest.mock import MagicMock

    engine = IngestionEngine.__new__(IngestionEngine)
    cursor = MagicMock()
    # volume, ask_volume, mid_volume, bid_volume, bid, ask, mid, timestamp, last
    cursor.fetchone.return_value = (
        4200,
        1500,
        700,
        2000,
        5.53,
        5.58,
        5.555,
        ET.localize(datetime(2026, 10, 9, 10, 14, 55)),
        5.57,
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = False

    with patch.object(main_engine, "db_connection", return_value=ctx):
        acc = engine._hydrate_flow_accumulator("SPY   261009C00775000", date(2026, 10, 9))

    assert acc.last_volume_cum == 4200
    assert (acc.ask_cum, acc.mid_cum, acc.bid_cum) == (1500, 700, 2000)
    assert acc.last_trade_price == 5.57, "the prior TRADE, not just the prior quote"
    assert acc.last_bid == 5.53 and acc.last_ask == 5.58
    # row[8] is a POSITIONAL read, so the SELECT's column list is the actual
    # contract here and a mocked cursor cannot see it. Drop `last` from the
    # query and the index silently reads the wrong column or raises into the
    # except, which degrades to zeros with only a warning.
    sql = cursor.execute.call_args[0][0]
    assert "last" in sql, "the hydration query must actually select the trade price"


def test_a_hydrated_accumulator_classifies_its_first_trade(monkeypatch):
    """The restart case end to end: hydrate, then tick against what came back."""
    from unittest.mock import MagicMock

    engine = IngestionEngine.__new__(IngestionEngine)
    engine._option_flow_lock = threading.Lock()
    monkeypatch.setattr(main_engine, "FLOW_CLASSIFIER", "tick")

    cursor = MagicMock()
    cursor.fetchone.return_value = (
        4200,
        1500,
        700,
        2000,
        5.53,
        5.58,
        5.555,
        ET.localize(datetime(2026, 10, 9, 10, 14, 55)),
        5.57,
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = False

    with patch.object(main_engine, "db_connection", return_value=ctx):
        acc = engine._hydrate_flow_accumulator("SPY   261009C00775000", date(2026, 10, 9))

    # First print after the restart is 5.60 -- an UPTICK from the hydrated
    # 5.57, so it is buyer-initiated. With last_trade_price lost it would
    # have gone to mid.
    engine._ingest_snapshot_into_accumulator(
        acc,
        {"volume": 4500, "last": 5.60, "bid": 5.55, "ask": 5.60, "mid": 5.575},
        ET.localize(datetime(2026, 10, 9, 10, 15)),
    )

    assert acc.ask_cum == 1500 + 300, "300 new contracts credited to buyers"
    assert acc.mid_cum == 700, "and none fell through to mid"
