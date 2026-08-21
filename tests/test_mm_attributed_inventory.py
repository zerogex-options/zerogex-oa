"""Market Maker inventory reconstruction — hand-verifiable cases.

Every number in this file is small enough to check by eye.  That is the point:
the inventory recursion is the load-bearing assumption of the whole experiment,
so its behavior is pinned against arithmetic a reader can redo, not against a
golden file.

Covers requirement 15's inventory items: opening buys, opening sells, closing a
long, closing a short, multi-session carry, expiration retirement, missing
starting history, missing intervals, calls, puts, and the clean vs partially
reconstructed distinction.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest

from research.mm_attributed_gex.inventory import (
    InventoryReconstructor,
    expected_sessions,
    settlement_instant,
)
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
)

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")

SYMBOL = "SPX"
EXPIRATION = date(2026, 6, 19)  # a Friday
STRIKE = 6000.0


def _et(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm), tzinfo=ET).astimezone(timezone.utc)


def rec(
    trading_date: date,
    side: Side,
    effect: PositionEffect,
    contracts: int,
    *,
    option_type: str = "C",
    hour: int = 10,
    minute: int = 0,
    participant: ParticipantType = ParticipantType.MARKET_MAKER,
    expiration: date = EXPIRATION,
    strike: float = STRIKE,
    option_root: str = "SPXW",
) -> ParticipantActivity:
    return ParticipantActivity(
        exchange=Exchange.CBOE_C1,
        symbol=SYMBOL,
        expiration=expiration,
        strike=strike,
        option_type=option_type,
        participant=participant,
        side=side,
        position_effect=effect,
        contracts=contracts,
        timestamp=_et(trading_date, hour, minute),
        trading_date=trading_date,
        interval=Interval.MINUTE_30,
        option_root=option_root,
    )


def _build(records, **kwargs) -> InventoryReconstructor:
    r = InventoryReconstructor(symbol=SYMBOL, **kwargs)
    r.consume(records).finalize()
    return r


# ---------------------------------------------------------------------------
# The four legs of the recursion
# ---------------------------------------------------------------------------


def test_opening_buys_build_long_inventory():
    """MM buys to open 100 -> long 100, short 0, net +100."""
    r = _build([rec(date(2026, 6, 15), Side.BUY, PositionEffect.OPEN, 100)])
    pos = r.positions()[0]
    assert pos.long_contracts == 100
    assert pos.short_contracts == 0
    assert pos.net_contracts == 100
    assert pos.opening_buys == 100


def test_opening_sells_build_short_inventory():
    """MM sells to open 80 -> short 80, net -80."""
    r = _build([rec(date(2026, 6, 15), Side.SELL, PositionEffect.OPEN, 80)])
    pos = r.positions()[0]
    assert pos.short_contracts == 80
    assert pos.long_contracts == 0
    assert pos.net_contracts == -80


def test_closing_sells_retire_long_inventory():
    """Open long 100, then sell-to-close 40 -> long 60, net +60."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100, hour=10),
            rec(day, Side.SELL, PositionEffect.CLOSE, 40, hour=11),
        ]
    )
    pos = r.positions()[0]
    assert pos.long_contracts == 60
    assert pos.short_contracts == 0
    assert pos.net_contracts == 60
    assert pos.long_floor_breaches == 0


def test_closing_buys_retire_short_inventory():
    """Open short 80, then buy-to-close 30 -> short 50, net -50."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.SELL, PositionEffect.OPEN, 80, hour=10),
            rec(day, Side.BUY, PositionEffect.CLOSE, 30, hour=11),
        ]
    )
    pos = r.positions()[0]
    assert pos.short_contracts == 50
    assert pos.net_contracts == -50


def test_long_and_short_legs_are_tracked_separately():
    """Long 100 and short 40 nets to +60 while gross stays 140."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100, hour=10),
            rec(day, Side.SELL, PositionEffect.OPEN, 40, hour=11),
        ]
    )
    pos = r.positions()[0]
    assert (pos.long_contracts, pos.short_contracts) == (100, 40)
    assert pos.net_contracts == 60
    assert pos.gross_contracts == 140


# ---------------------------------------------------------------------------
# The open/close invariance property
# ---------------------------------------------------------------------------


def test_net_position_is_invariant_to_open_close_misclassification():
    """net = buys − sells regardless of how open/close is labelled.

    This is the property that makes the experiment robust to the least
    trustworthy field in an Open-Close feed. Two record sets with identical
    buy/sell totals but *opposite* position-effect labels must produce the
    same net, even though their long/short decompositions differ.
    """
    day = date(2026, 6, 15)
    as_labelled = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100, hour=10),
            rec(day, Side.SELL, PositionEffect.CLOSE, 40, hour=11),
        ]
    ).positions()[0]
    mislabelled = _build(
        [
            rec(day, Side.BUY, PositionEffect.CLOSE, 100, hour=10),
            rec(day, Side.SELL, PositionEffect.OPEN, 40, hour=11),
        ]
    ).positions()[0]

    assert as_labelled.net_contracts == mislabelled.net_contracts == 60
    # ...but the decomposition is NOT the same, which is why long/short is
    # reported as a diagnostic and never fed to the gamma calculation.
    assert as_labelled.long_contracts != mislabelled.long_contracts


def test_unknown_effect_volume_reaches_net_flow_but_not_the_recursion():
    """UNKNOWN effect is excluded from long/short and included in net-flow."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100, hour=10),
            rec(day, Side.SELL, PositionEffect.UNKNOWN, 25, hour=11),
        ]
    )
    pos = r.positions()[0]
    assert pos.net_contracts == 100  # recursion ignores the unknown bucket
    assert pos.net_flow_contracts == 75  # 100 bought − 25 sold
    assert pos.estimator_disagreement == 25
    assert pos.unknown_effect_share == pytest.approx(25 / 125)


# ---------------------------------------------------------------------------
# Multi-session carry
# ---------------------------------------------------------------------------


def test_inventory_carries_across_multiple_sessions():
    """+100 Monday, −30 Tuesday, +50 Wednesday -> net +120 with 3 sessions seen."""
    mon, tue, wed = date(2026, 6, 15), date(2026, 6, 16), date(2026, 6, 17)
    r = _build(
        [
            rec(mon, Side.BUY, PositionEffect.OPEN, 100),
            rec(tue, Side.SELL, PositionEffect.CLOSE, 30),
            rec(wed, Side.BUY, PositionEffect.OPEN, 50),
        ]
    )
    pos = r.positions()[0]
    assert pos.net_contracts == 120
    assert len(pos.active_sessions) == 3
    assert pos.first_activity_date == mon
    assert pos.last_activity_date == wed


def test_calls_and_puts_are_separate_series():
    """A call and a put at the same strike never share an inventory."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100, option_type="C"),
            rec(day, Side.SELL, PositionEffect.OPEN, 70, option_type="P"),
        ]
    )
    by_type = {p.option_type: p for p in r.positions()}
    assert by_type["C"].net_contracts == 100
    assert by_type["P"].net_contracts == -70


def test_non_market_maker_records_are_excluded_by_default():
    """Customer flow is consumed for reconciliation but never enters MM inventory."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100),
            rec(day, Side.SELL, PositionEffect.OPEN, 100, participant=ParticipantType.CUSTOMER),
        ]
    )
    assert len(r.positions()) == 1
    assert r.positions()[0].net_contracts == 100
    assert r.report.records_consumed == 2
    assert r.report.mm_records_consumed == 1
    assert "CUSTOMER" in r.report.participants_seen


# ---------------------------------------------------------------------------
# Left-censoring
# ---------------------------------------------------------------------------


def test_closing_more_than_observed_opens_proves_prior_inventory():
    """Selling to close 150 against 100 observed opens implies ≥50 pre-existed."""
    day = date(2026, 6, 15)
    r = _build(
        [
            rec(day, Side.BUY, PositionEffect.OPEN, 100, hour=10),
            rec(day, Side.SELL, PositionEffect.CLOSE, 150, hour=11),
        ]
    )
    pos = r.positions()[0]
    assert pos.long_contracts == -50
    assert pos.long_floor_breaches == 1
    assert pos.implied_prior_long == 50
    assert pos.left_censored is True
    assert pos.left_censor_reason == "closing_volume_exceeded_observed_opens"


def test_series_first_seen_at_the_window_edge_is_marked_censored():
    """A contract already trading on day 1 cannot be assumed to start at zero."""
    d1, d2 = date(2026, 6, 15), date(2026, 6, 16)
    r = _build(
        [
            rec(d1, Side.BUY, PositionEffect.OPEN, 100),
            rec(d2, Side.BUY, PositionEffect.OPEN, 10, strike=6100.0),
        ]
    )
    by_strike = {p.strike: p for p in r.positions()}
    assert by_strike[6000.0].left_censored is True
    assert by_strike[6000.0].left_censor_reason == "first_trade_at_window_edge"
    # The 6100 series first traded on day 2 — inside the window — so its
    # inventory genuinely starts from zero.
    assert by_strike[6100.0].left_censored is False
    assert by_strike[6100.0].left_censor_reason == "first_trade_inside_window"


def test_listing_date_evidence_overrides_the_window_heuristic():
    """A confirmed listing date inside the window makes a day-1 series clean."""
    d1, d2 = date(2026, 6, 15), date(2026, 6, 16)
    key = (SYMBOL, EXPIRATION, STRIKE, "C")
    r = _build(
        [
            rec(d1, Side.BUY, PositionEffect.OPEN, 100),
            rec(d2, Side.BUY, PositionEffect.OPEN, 20),
        ],
        listing_dates={key: d1},
    )
    pos = r.positions()[0]
    assert pos.left_censored is False
    assert pos.left_censor_reason == "observed_from_listing_date"


def test_listing_date_before_the_window_marks_the_series_censored():
    d1 = date(2026, 6, 15)
    key = (SYMBOL, EXPIRATION, STRIKE, "C")
    r = _build(
        [rec(d1, Side.BUY, PositionEffect.OPEN, 100)],
        listing_dates={key: date(2026, 5, 1)},
    )
    pos = r.positions()[0]
    assert pos.left_censored is True
    assert pos.left_censor_reason == "listed_before_data_window"


def test_clean_and_censored_series_are_reported_separately():
    d1, d2, d3 = date(2026, 6, 15), date(2026, 6, 16), date(2026, 6, 17)
    r = _build(
        [
            rec(d1, Side.BUY, PositionEffect.OPEN, 100, strike=5900.0),
            rec(d2, Side.BUY, PositionEffect.OPEN, 50, strike=6000.0),
            rec(d3, Side.BUY, PositionEffect.OPEN, 25, strike=6100.0),
        ]
    )
    summary = r.summary()
    assert summary["series_total"] == 3
    assert summary["series_left_censored"] == 1  # only the day-1 series
    assert summary["series_clean"] == 2
    assert summary["clean_share"] == pytest.approx(2 / 3)
    assert {p.strike for p in r.clean_positions()} == {6000.0, 6100.0}


# ---------------------------------------------------------------------------
# Missing intervals / sessions
# ---------------------------------------------------------------------------


def test_missing_trading_session_is_detected_as_a_gap():
    """Mon and Wed present, Tue absent -> Tuesday is reported as a gap."""
    mon, wed = date(2026, 6, 15), date(2026, 6, 17)
    r = _build(
        [
            rec(mon, Side.BUY, PositionEffect.OPEN, 100),
            rec(wed, Side.BUY, PositionEffect.OPEN, 40),
        ]
    )
    assert r.report.session_gaps == (date(2026, 6, 16),)
    assert r.positions()[0].missing_sessions == (date(2026, 6, 16),)


def test_weekend_is_not_a_gap():
    """Friday then Monday is contiguous; the calendar knows there is no session."""
    fri, mon = date(2026, 6, 12), date(2026, 6, 15)
    r = _build(
        [
            rec(fri, Side.BUY, PositionEffect.OPEN, 100),
            rec(mon, Side.BUY, PositionEffect.OPEN, 40),
        ]
    )
    assert r.report.session_gaps == ()


def test_expected_sessions_skips_weekends_and_holidays():
    sessions = expected_sessions(date(2026, 6, 12), date(2026, 6, 16))
    assert sessions == [date(2026, 6, 12), date(2026, 6, 15), date(2026, 6, 16)]


# ---------------------------------------------------------------------------
# Expiration
# ---------------------------------------------------------------------------


def test_settlement_instant_is_am_for_spx_third_friday_and_pm_for_spxw():
    """SPX monthlies settle at the 09:30 SOQ; SPXW weeklies at 16:00."""
    am = settlement_instant("SPX", EXPIRATION, option_root=None)
    pm = settlement_instant("SPX", EXPIRATION, option_root="SPXW")
    assert am.astimezone(ET).time() == time(9, 30)
    assert pm.astimezone(ET).time() == time(16, 0)
    assert am < pm


def test_expired_series_are_retired_and_stop_contributing():
    """SPX is European and cash-settled: past settlement the position is gone."""
    day = date(2026, 6, 15)
    r = _build([rec(day, Side.BUY, PositionEffect.OPEN, 100)])
    before = _et(EXPIRATION, 15, 0)
    after = _et(EXPIRATION + timedelta(days=1), 10, 0)

    assert len(r.book.live_positions(before)) == 1
    assert len(r.book.live_positions(after)) == 0

    retired = r.book.retire_expired(after)
    assert retired == 1
    assert len(r.book) == 0


def test_am_settled_monthly_retires_at_the_open_not_the_close():
    """An SPX (no SPXW root) 3rd-Friday position is gone by 10:00 that morning."""
    day = date(2026, 6, 15)
    r = _build([rec(day, Side.BUY, PositionEffect.OPEN, 100, option_root=None)])
    assert len(r.book.live_positions(_et(EXPIRATION, 9, 0))) == 1
    assert len(r.book.live_positions(_et(EXPIRATION, 10, 0))) == 0
