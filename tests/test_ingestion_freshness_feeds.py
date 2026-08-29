"""Every TradeStation stream, not just the underlying bars.

The freshness check was built after a QQQ underlying worker died silently for
six hours. It watched one of four stream families: a dead option-chain worker,
a dead VIX stream, or a dead ES/NQ futures stream produced exactly the same
silence with nothing looking at it.

The hard part is not the query, it is the WINDOW. Each family delivers on a
different calendar, and a check that gets that wrong either cries wolf every
evening — which trains an operator to ignore it, the worst outcome — or stays
quiet through a real outage.
"""

import datetime as dt

import pytz

from src.market_calendar import option_chain_feed_expected
from src.tools.ingestion_freshness_healthcheck import (
    FEED_CHAINS,
    FEED_FUTURES,
    FEED_UNDERLYING,
    FEED_VOLATILITY,
    evaluate_feed,
)

ET = pytz.timezone("US/Eastern")
MAX_STALE = dt.timedelta(minutes=15)


def _et(y, m, d, hh, mm):
    return ET.localize(dt.datetime(y, m, d, hh, mm))


NOW = _et(2026, 8, 27, 11, 0)  # Thursday, mid cash session


def _feed(last, *, expected=True, anchor=None, name=FEED_UNDERLYING):
    return evaluate_feed(
        name, "SPY", last, NOW, expected=expected, session_anchor=anchor, max_stale=MAX_STALE
    )


# --- the failure this exists to catch --------------------------------------


def test_a_stream_silent_through_the_session_is_stale():
    """The QQQ shape: process alive, systemd green, no data for hours."""
    assert _feed(_et(2026, 8, 27, 9, 35), anchor=_et(2026, 8, 27, 9, 30)).status == "stale"


def test_a_stream_writing_normally_is_fresh():
    assert _feed(_et(2026, 8, 27, 10, 59), anchor=_et(2026, 8, 27, 9, 30)).status == "fresh"


def test_a_feed_outside_its_window_never_alerts():
    """Crying wolf every evening is worse than not checking: it teaches the
    operator to skip the one morning it matters."""
    result = _feed(_et(2026, 8, 26, 15, 59), expected=False)
    assert result.status == "not_expected"
    assert result.stale_minutes is None


# --- the anchor ------------------------------------------------------------


def test_staleness_is_measured_from_the_open_not_the_overnight_gap():
    """First check after an open must not report the whole night as an
    outage — every session would start with a spurious alert."""
    just_opened = _et(2026, 8, 27, 9, 32)
    result = evaluate_feed(
        FEED_UNDERLYING,
        "SPY",
        _et(2026, 8, 26, 19, 59),  # last night's final bar
        just_opened,
        expected=True,
        session_anchor=_et(2026, 8, 27, 9, 30),
        max_stale=MAX_STALE,
    )
    assert result.status == "fresh"
    assert result.stale_minutes == 2.0


def test_a_feed_that_has_never_written_still_measures_from_the_open():
    """A worker that died BEFORE the open leaves no bar at all. Anchoring on
    `now` would call that fresh forever."""
    assert _feed(None, anchor=_et(2026, 8, 27, 9, 30)).status == "stale"


# --- each family's own calendar --------------------------------------------


def test_futures_are_live_when_the_equity_market_is_shut():
    """CME runs Sun 18:00 -> Fri 17:00 ET. A futures stream silent at 22:00 on
    a Wednesday is a real outage, and an equity-session window would miss it
    entirely."""
    late = _et(2026, 8, 26, 22, 0)
    result = evaluate_feed(
        FEED_FUTURES,
        "SPX",  # futures_quotes keys ES rows under its backing index
        _et(2026, 8, 26, 20, 0),
        late,
        expected=True,
        session_anchor=_et(2026, 8, 26, 18, 0),
        max_stale=MAX_STALE,
    )
    assert result.status == "stale"


def test_cash_index_chains_stopping_at_the_bell_is_not_an_outage():
    """SPX/NDX option rows stop at 15:59 every day — Greeks are refused once
    the index stops printing. Verified across three sessions in production;
    alerting on it would fire daily and mean nothing."""
    evening = _et(2026, 8, 26, 22, 0)
    result = evaluate_feed(
        FEED_CHAINS,
        "SPX",
        _et(2026, 8, 26, 15, 59),
        evening,
        expected=False,  # feed_session_window clamps cash indexes to 09:30-16:00
        session_anchor=None,
        max_stale=MAX_STALE,
    )
    assert result.status == "not_expected"


def test_volatility_feeds_are_checked_inside_the_cash_session():
    result = evaluate_feed(
        FEED_VOLATILITY,
        "VIX",
        _et(2026, 8, 27, 9, 40),
        NOW,
        expected=True,
        session_anchor=_et(2026, 8, 27, 9, 30),
        max_stale=MAX_STALE,
    )
    assert result.status == "stale"  # last bar 09:40, now 11:00


def test_the_feed_name_travels_with_the_result():
    """Four families report per symbol, so 'SPX is stale' is ambiguous —
    the operator needs to know WHICH stream to go restart."""
    assert _feed(None, anchor=NOW, name=FEED_CHAINS).feed == FEED_CHAINS
    assert _feed(None, anchor=NOW, name=FEED_FUTURES).feed == FEED_FUTURES


# --- bars stamped in the future --------------------------------------------


def test_a_close_stamped_bar_never_reports_negative_staleness():
    """vix_bars/vxn_bars store TradeStation's raw TimeStamp — the bar's CLOSE —
    on a 5-minute interval, so the 09:30-09:35 bar is stamped 09:35 and is
    genuinely 'in the future' at 09:31. Production printed
    'VIX: fresh (-3.1 min since last write)', which reads as broken and
    undermines every other line in the report."""
    now = _et(2026, 8, 27, 9, 31)
    result = evaluate_feed(
        FEED_VOLATILITY,
        "VIX",
        _et(2026, 8, 27, 9, 35),  # bar close, four minutes ahead of now
        now,
        expected=True,
        session_anchor=_et(2026, 8, 27, 9, 30),
        max_stale=MAX_STALE,
    )
    assert result.status == "fresh"
    assert result.stale_minutes == 0.0


def test_clamping_does_not_hide_a_genuinely_dead_feed():
    """The clamp must apply to the future case only — a feed silent for hours
    still has to alert."""
    result = evaluate_feed(
        FEED_VOLATILITY,
        "VIX",
        _et(2026, 8, 27, 9, 35),
        _et(2026, 8, 27, 13, 0),  # three and a half hours later
        expected=True,
        session_anchor=_et(2026, 8, 27, 9, 30),
        max_stale=MAX_STALE,
    )
    assert result.status == "stale"
    assert result.stale_minutes == 205.0


# --- the option-chain window ------------------------------------------------
#
# Chains were originally graded against the underlying's delivery window
# (04:00-20:00 under USEQ24Hour). Option quotes only tick while options trade,
# so both tails of that window are full of legitimate multi-minute gaps: on
# 2026-08-28 production logged QQQ chains "STALE - nothing written for 18.4
# min" at 19:40 ET and 28.3 min at 19:50, and the same shape appears every
# pre-market. Each firing emailed, every 10 minutes, for a feed that was fine.


def _chain_expected(when, symbol="SPY"):
    return option_chain_feed_expected(when, symbol)


def test_chains_are_not_graded_before_the_options_open():
    """04:00-09:30: the bar feed is live, the options market is not."""
    assert _chain_expected(_et(2026, 8, 27, 4, 15)) is False
    assert _chain_expected(_et(2026, 8, 27, 9, 29)) is False


def test_chains_are_not_graded_after_the_options_close():
    """The 19:40 ET page that started this: real gap, shut market."""
    assert _chain_expected(_et(2026, 8, 28, 16, 30)) is False
    assert _chain_expected(_et(2026, 8, 28, 19, 40)) is False


def test_chains_are_graded_through_the_options_session():
    assert _chain_expected(_et(2026, 8, 27, 9, 30)) is True
    assert _chain_expected(_et(2026, 8, 27, 12, 0)) is True
    assert _chain_expected(_et(2026, 8, 27, 16, 15)) is True


def test_cash_index_chains_stop_at_the_index_close():
    """SPX/NDX chains stop at 16:00: no underlying print, no Greeks, no rows."""
    assert option_chain_feed_expected(_et(2026, 8, 27, 16, 10), "SPX") is False
    assert option_chain_feed_expected(_et(2026, 8, 27, 15, 59), "SPX") is True


def test_chains_never_graded_at_weekends_or_holidays(monkeypatch):
    """NYSE_HOLIDAYS is env-loaded, so pin it rather than testing the box."""
    import src.market_calendar as mc

    assert _chain_expected(_et(2026, 8, 29, 12, 0)) is False  # Saturday
    monkeypatch.setattr(mc, "NYSE_HOLIDAYS", {dt.date(2026, 9, 7)})
    assert mc.option_chain_feed_expected(_et(2026, 9, 7, 12, 0), "SPY") is False  # Labor Day


def test_half_day_chains_stop_at_the_early_close(monkeypatch):
    """An early close must not page for the three hours after it."""
    import src.market_calendar as mc

    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {dt.date(2026, 11, 27)})
    assert mc.option_chain_feed_expected(_et(2026, 11, 27, 12, 0), "SPY") is True
    assert mc.option_chain_feed_expected(_et(2026, 11, 27, 14, 0), "SPY") is False


def test_a_chain_stream_dead_inside_the_session_is_still_stale():
    """The narrowing must not blind the check to the failure it exists for."""
    result = evaluate_feed(
        FEED_CHAINS,
        "QQQ",
        _et(2026, 8, 27, 9, 35),
        _et(2026, 8, 27, 14, 0),
        expected=option_chain_feed_expected(_et(2026, 8, 27, 14, 0), "QQQ"),
        session_anchor=_et(2026, 8, 27, 9, 30),
        max_stale=MAX_STALE,
    )
    assert result.status == "stale"
