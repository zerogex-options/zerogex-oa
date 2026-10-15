"""The tick test, and the two questions it has to answer before it ships.

The quote test cannot survive ThetaData's crossed Market Value quotes
(compliance F9): against a crossed book, "is this price nearer the bid or
the ask" has no answer. The tick test reads no quote at all — only the
sequence of trade prices — so the defect cannot reach it.

That is not a free win. Swapping the classifier changes the published
figure whether or not the feed changes, so these tests pin BOTH things the
measurement has to separate: what the method change does on its own, and
whether the tick test is genuinely feed-insensitive.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import pytz

from src.ingestion import main_engine
from src.ingestion.main_engine import IngestionEngine, _FlowAccumulator

ET = pytz.timezone("US/Eastern")


def _engine() -> IngestionEngine:
    """A bare engine: these tests drive the accumulator, not a live session."""
    return IngestionEngine.__new__(IngestionEngine)


from src.ingestion.providers.base import OptionQuote
from src.tools.feed_compare import FeedSample, compare_tick_test, new_tick_state

tick = IngestionEngine._classify_volume_chunk_tick

EXP = date(2026, 10, 6)
NOW = datetime(2026, 10, 6, 14, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# The classifier
# ---------------------------------------------------------------------------


def test_an_uptick_is_buyer_initiated():
    assert tick(100, 1.36, 1.35) == (100, 0, 0, 1)


def test_a_downtick_is_seller_initiated():
    assert tick(100, 1.34, 1.35) == (0, 0, 100, -1)


@pytest.mark.parametrize(
    "prev_direction, expected",
    [(1, (100, 0, 0, 1)), (-1, (0, 0, 100, -1))],
)
def test_a_zero_tick_carries_the_last_non_zero_direction(prev_direction, expected):
    """Lee & Ready 1991. A flat print is not a mid print.

    Dropping this rule would route every repeated price to mid, and on a
    penny-wide 0DTE contract most consecutive prints ARE the same price —
    so the imbalance would collapse toward zero and stop saying anything.
    """
    assert tick(100, 1.35, 1.35, prev_direction) == expected


def test_a_zero_tick_with_no_history_is_genuinely_unknown():
    """Nothing to carry, so mid — recorded as "cannot tell", not invented."""
    assert tick(100, 1.35, 1.35, 0) == (0, 100, 0, 0)


def test_no_prior_trade_means_no_side():
    assert tick(100, 1.35, None) == (0, 100, 0, 0)
    assert tick(100, 1.35, 0.0) == (0, 100, 0, 0)


def test_no_volume_classifies_nothing_and_keeps_the_direction():
    """Direction must survive a quiet interval, or the next zero tick loses it."""
    assert tick(0, 1.35, 1.34, 1) == (0, 0, 0, 1)
    assert tick(-5, 1.35, 1.34, -1) == (0, 0, 0, -1)


def test_a_missing_last_price_classifies_nothing_as_a_side():
    assert tick(100, None, 1.35, 1) == (0, 100, 0, 1)


def test_the_tick_test_reads_no_quote_at_all():
    """Its whole reason for existing. Signature carries no bid, ask or mid.

    The exact-set assertion is deliberate and is NOT to be relaxed into the
    denylist above. A denylist only catches a quote arriving under a name
    somebody already thought of; pinning the whole signature forces any new
    parameter to be justified in a diff, which is the actual guarantee.

    ``prev_trade_age_seconds`` was added 2026-10-09 for
    FLOW_TICK_MAX_CARRY_SECONDS (F12), and is admissible on exactly one
    ground: it is a DURATION, derived from two observation timestamps, and
    carries no price information of any kind. A parameter that could encode
    a price -- however it were named -- would break the premise that the
    Market Value randomisation cannot reach this classifier, and with it the
    licensing argument in F9 for not paying OPRA non-display fees.
    """
    import inspect

    params = set(inspect.signature(tick).parameters)
    assert not params & {"bid", "ask", "mid", "band_pct"}
    assert params == {
        "volume_delta",
        "last",
        "prev_last",
        "prev_direction",
        "prev_trade_age_seconds",
    }


# ---------------------------------------------------------------------------
# The measurement
# ---------------------------------------------------------------------------


def _q(last, volume, *, bid=1.35, ask=1.36) -> OptionQuote:
    return OptionQuote(option_symbol="X", timestamp=NOW, bid=bid, ask=ask, last=last, volume=volume)


def _pair(inc_quotes, cand_quotes):
    """Two samples over one contract, each keyed its own vendor's way."""
    meta = {"strike": 765.0, "expiration": EXP, "option_type": "C"}
    inc = FeedSample(
        provider="tradestation",
        captured_at=NOW,
        spot=765.0,
        quotes={"SPY 261006C765": inc_quotes},
        metadata={"SPY 261006C765": dict(meta)},
    )
    cand = FeedSample(
        provider="thetadata_mv",
        captured_at=NOW,
        spot=765.0,
        quotes={"SPY   261006C00765000": cand_quotes},
        metadata={"SPY   261006C00765000": dict(meta)},
    )
    return inc, cand


def test_the_first_sample_compares_nothing_and_seeds_the_next():
    """A tick test with no previous trade has nothing to say, and must say so."""
    state = new_tick_state()
    out = compare_tick_test(*_pair(_q(1.35, 1000), _q(1.35, 1000)), state)
    assert out["contracts_compared"] == 0
    assert state["inc"], "the first sample has to leave state behind"


def test_volume_is_a_delta_not_the_cumulative():
    """Weighting by cumulative day volume would inflate every interval.

    compare_flow_classification does weight by the cumulative, which is why
    its numbers and these are not comparable. This block differences it.
    """
    state = new_tick_state()
    compare_tick_test(*_pair(_q(1.35, 1000), _q(1.35, 1000)), state)
    out = compare_tick_test(*_pair(_q(1.36, 1250), _q(1.36, 1250)), state)
    assert out["volume_compared"] == 250, "classified the cumulative, not the delta"
    assert out["net_tick_test"] == 250


def test_the_method_change_is_measured_with_the_feed_held_constant():
    """An uptick INTO the bid is where the two methods part company.

    Quote test: 1.35 sits at the bid of 1.35/1.36, so seller-initiated.
    Tick test: 1.35 is above the previous print of 1.34, so buyer-initiated.
    Same feed, same trade, opposite side — that is the product risk, and it
    exists with no vendor migration at all.
    """
    state = new_tick_state()
    compare_tick_test(*_pair(_q(1.34, 1000), _q(1.34, 1000)), state)
    out = compare_tick_test(*_pair(_q(1.35, 1100), _q(1.35, 1100)), state)
    assert out["contracts_compared"] == 1
    assert out["method_contracts_differ"] == 1
    assert out["net_quote_test"] == -100
    assert out["net_tick_test"] == +100
    assert out["shifts"] == {"bid->ask": 1}


def test_the_tick_test_is_feed_insensitive_when_the_tapes_agree():
    """The reason it is a candidate: no quote reaches it, crossed or not.

    The candidate's quote here is CROSSED (bid above ask), exactly as F9
    describes. The tick test does not care.
    """
    state = new_tick_state()
    compare_tick_test(*_pair(_q(1.34, 1000), _q(1.34, 1000, bid=1.36, ask=1.35)), state)
    out = compare_tick_test(*_pair(_q(1.36, 1100), _q(1.36, 1100, bid=1.38, ask=1.37)), state)
    assert out["feed_contracts_differ"] == 0
    assert out["net_tick_test"] == out["net_tick_candidate"]


def test_a_tape_disagreement_between_feeds_is_caught_not_hidden():
    """Not a tautology: the two last-trade sequences are different vendors'.

    If the candidate's print lags, its tick points the other way, and this
    block has to show that rather than assume the tapes match.
    """
    state = new_tick_state()
    compare_tick_test(*_pair(_q(1.35, 1000), _q(1.35, 1000)), state)
    out = compare_tick_test(*_pair(_q(1.36, 1100), _q(1.34, 1100)), state)
    assert out["feed_contracts_differ"] == 1
    assert out["net_tick_test"] == +100
    assert out["net_tick_candidate"] == -100


def test_direction_survives_an_interval_with_no_trades():
    """Otherwise a quiet minute wipes the memory the zero-tick rule needs."""
    state = new_tick_state()
    compare_tick_test(*_pair(_q(1.34, 1000), _q(1.34, 1000)), state)
    compare_tick_test(*_pair(_q(1.36, 1100), _q(1.36, 1100)), state)  # uptick, dir=+1
    compare_tick_test(*_pair(_q(1.36, 1100), _q(1.36, 1100)), state)  # no volume
    out = compare_tick_test(*_pair(_q(1.36, 1200), _q(1.36, 1200)), state)  # zero tick
    assert out["net_tick_test"] == +100, "the carried direction was lost"


def test_an_unmatched_contract_is_counted_not_skipped():
    """Same discipline as the flow join: a silent miss is what cost five days."""
    state = new_tick_state()
    inc, cand = _pair(_q(1.35, 1000), _q(1.35, 1000))
    cand.quotes.clear()
    out = compare_tick_test(inc, cand, state)
    assert out["contracts_unmatched"] == 1
    assert out["contracts_compared"] == 0


# --- FLOW_TICK_MAX_CARRY_SECONDS, end to end -------------------------------
#
# The guard was built on 2026-10-09 for F12's NDX hypothesis: a contract that
# prints rarely credits a whole chunk to a direction set minutes ago. It was
# never exercised through the accumulator, only as a parameter handed straight
# to the classifier -- and through the accumulator it could not fire AT ALL.
#
# `last` is the last TRADED price and it persists in every later snapshot, so a
# contract that has not traded since 10:00 still reports one at 15:00. The
# accumulator stamped the observation time on each of those, so the carry
# always looked one poll old and no threshold could ever be exceeded. A session
# run with the guard on would have shown no change and been read as "the guard
# does not help NDX".
#
# These drive the real path: an uptick, a long quiet stretch of unchanged
# prices, then a large zero-tick chunk.


def _quiet_then_big_chunk(carry_seconds: float) -> _FlowAccumulator:
    """Uptick, ten minutes of unchanged price, then 1,000 contracts."""
    engine = _engine()
    engine._option_flow = {}
    engine._option_flow_lock = threading.Lock()
    bucket = ET.localize(datetime(2026, 10, 14, 10, 0))
    acc = _FlowAccumulator(
        session_date=date(2026, 10, 14),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )
    t0 = ET.localize(datetime(2026, 10, 14, 10, 0, 0))
    quote = {"bid": 10.9, "ask": 11.1, "mid": 11.0}
    with (
        patch.object(main_engine, "FLOW_CLASSIFIER", "tick"),
        patch.object(main_engine, "FLOW_TICK_MAX_CARRY_SECONDS", carry_seconds),
    ):
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 100, "last": 10.0, "bid": 9.9, "ask": 10.1, "mid": 10.0, "timestamp": t0},
            bucket,
        )
        # Uptick -> buyer-initiated, direction carried from here.
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 200, "last": 11.0, **quote, "timestamp": t0 + timedelta(seconds=5)},
            bucket,
        )
        # Ten minutes of polls at an unchanged price and unchanged volume.
        t = t0 + timedelta(seconds=5)
        for _ in range(120):
            t += timedelta(seconds=5)
            engine._ingest_snapshot_into_accumulator(
                acc,
                {"volume": 200, "last": 11.0, **quote, "timestamp": t},
                bucket,
            )
        # A big zero-tick chunk, ten minutes after the price last moved.
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 1200, "last": 11.0, **quote, "timestamp": t + timedelta(seconds=5)},
            bucket,
        )
    return acc


def test_the_guard_fires_through_the_accumulator():
    """A chunk ten minutes past the last price change must route to mid."""
    acc = _quiet_then_big_chunk(60.0)
    assert acc.mid_cum == 1100, (
        "the stale chunk inherited its direction instead of routing to mid; "
        "FLOW_TICK_MAX_CARRY_SECONDS is not reaching the classifier"
    )
    assert acc.ask_cum == 100  # only the original uptick


def test_disabled_is_the_behaviour_measured_on_2026_10_09():
    """0 means carry forever -- the configuration F12 and F16 were measured on."""
    acc = _quiet_then_big_chunk(0.0)
    assert acc.ask_cum == 1100
    assert acc.mid_cum == 100


def test_the_timestamp_tracks_the_last_PRICE_CHANGE_not_the_last_observation():
    """The bug, pinned directly.

    `last` persists between trades, so stamping every snapshot that carries one
    made the carry permanently one poll old and the guard unreachable.
    """
    engine = _engine()
    engine._option_flow = {}
    engine._option_flow_lock = threading.Lock()
    bucket = ET.localize(datetime(2026, 10, 14, 10, 0))
    acc = _FlowAccumulator(
        session_date=date(2026, 10, 14),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )
    t0 = ET.localize(datetime(2026, 10, 14, 10, 0, 0))
    quote = {"bid": 10.9, "ask": 11.1, "mid": 11.0}
    with patch.object(main_engine, "FLOW_CLASSIFIER", "tick"):
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 100, "last": 11.0, **quote, "timestamp": t0},
            bucket,
        )
        assert acc.last_trade_ts == t0
        # Same price, five minutes later: the stamp must NOT move.
        later = t0 + timedelta(minutes=5)
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 100, "last": 11.0, **quote, "timestamp": later},
            bucket,
        )
        assert acc.last_trade_ts == t0, "an unchanged price refreshed the carry age"
        # A NEW price does move it.
        moved = t0 + timedelta(minutes=6)
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 150, "last": 11.5, **quote, "timestamp": moved},
            bucket,
        )
        assert acc.last_trade_ts == moved
        assert acc.last_trade_price == 11.5


def test_a_prompt_zero_tick_still_inherits():
    """The guard must not punish a contract that is simply trading flat."""
    engine = _engine()
    engine._option_flow = {}
    engine._option_flow_lock = threading.Lock()
    bucket = ET.localize(datetime(2026, 10, 14, 10, 0))
    acc = _FlowAccumulator(
        session_date=date(2026, 10, 14),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )
    t0 = ET.localize(datetime(2026, 10, 14, 10, 0, 0))
    quote = {"bid": 10.9, "ask": 11.1, "mid": 11.0}
    with (
        patch.object(main_engine, "FLOW_CLASSIFIER", "tick"),
        patch.object(main_engine, "FLOW_TICK_MAX_CARRY_SECONDS", 60.0),
    ):
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 100, "last": 10.0, "bid": 9.9, "ask": 10.1, "mid": 10.0, "timestamp": t0},
            bucket,
        )
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 200, "last": 11.0, **quote, "timestamp": t0 + timedelta(seconds=5)},
            bucket,
        )
        # Zero tick five seconds later -- well inside the window.
        engine._ingest_snapshot_into_accumulator(
            acc,
            {"volume": 300, "last": 11.0, **quote, "timestamp": t0 + timedelta(seconds=10)},
            bucket,
        )
    # 100 to mid (first chunk, no prior trade to compare against), 100 to ask
    # on the uptick, and the prompt zero tick INHERITS that direction for
    # another 100 -- which is the behaviour the guard must not disturb.
    assert acc.ask_cum == 200, "a prompt zero tick was wrongly treated as stale"
    assert acc.mid_cum == 100


def test_the_threshold_boundary_is_exclusive():
    """An age exactly equal to the threshold is NOT yet stale.

    Pinned because it is the one part of the guard no behavioural test reaches:
    both sides look identical in a session, and a silent flip to `>=` would
    change which chunks route to mid at the margin with nothing to catch it.
    """
    engine = _engine()
    with patch.object(main_engine, "FLOW_TICK_MAX_CARRY_SECONDS", 60.0):
        at_the_boundary = engine._classify_volume_chunk_tick(
            100,
            last=11.0,
            prev_last=11.0,
            prev_direction=1,
            prev_trade_age_seconds=60.0,
        )
        assert at_the_boundary == (
            100,
            0,
            0,
            1,
        ), "an age equal to the threshold was treated as stale"

        just_past = engine._classify_volume_chunk_tick(
            100,
            last=11.0,
            prev_last=11.0,
            prev_direction=1,
            prev_trade_age_seconds=60.000001,
        )
        assert just_past == (0, 100, 0, 1), "an age past the threshold still inherited"
