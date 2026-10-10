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

from datetime import date, datetime, timezone

import pytest

from src.ingestion.main_engine import IngestionEngine
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
