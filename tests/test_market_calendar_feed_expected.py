"""Regression coverage for ``underlying_feed_expected``.

This predicate decides whether underlying-bar silence is a real anomaly
(WARN + stream self-heal) or expected (quiet DEBUG, watchdog idle). The
load-bearing invariant — and the bug that flooded the journal for hours
every afternoon — is that a cash index (SPX) has an underlying print
ONLY during the regular cash session (09:30-16:00 ET) regardless of the
TradeStation session template, even though its options trade extended
hours.
"""

from datetime import datetime

import pytz

from src.market_calendar import underlying_feed_expected

ET = pytz.timezone("US/Eastern")


def _et(y, mo, d, h, mi):
    return ET.localize(datetime(y, mo, d, h, mi))


# 2026-05-15 is a Friday; 2026-05-16 a Saturday.
FRI_AFTER_HOURS = _et(2026, 5, 15, 16, 35)  # the incident time
FRI_REGULAR = _et(2026, 5, 15, 11, 0)
FRI_PREMARKET = _et(2026, 5, 15, 7, 0)
SAT = _et(2026, 5, 16, 11, 0)


def test_cash_index_clamped_to_regular_session_regardless_of_template():
    # SPX has no pre/after-hours print even under a 24h template.
    assert underlying_feed_expected(FRI_AFTER_HOURS, "Default", "SPX") is False
    assert underlying_feed_expected(FRI_AFTER_HOURS, "USEQ24Hour", "SPX") is False
    assert underlying_feed_expected(FRI_PREMARKET, "USEQ24Hour", "SPX") is False
    # ...but it IS expected during the regular cash session, so a genuine
    # regular-hours stall still alerts and self-heals.
    assert underlying_feed_expected(FRI_REGULAR, "Default", "SPX") is True
    assert underlying_feed_expected(FRI_REGULAR, "USEQ24Hour", "SPX") is True


def test_equity_follows_session_template_window():
    # The reported flood: SPY under the Default (regular-only) template
    # after the 16:00 close — expected silence, must not warn.
    assert underlying_feed_expected(FRI_AFTER_HOURS, "Default", "SPY") is False
    assert underlying_feed_expected(FRI_PREMARKET, "Default", "SPY") is False
    # An ETF under a 24h template genuinely should be live after-hours,
    # so staleness there is a real anomaly worth alerting.
    assert underlying_feed_expected(FRI_AFTER_HOURS, "USEQ24Hour", "SPY") is True
    assert underlying_feed_expected(FRI_REGULAR, "Default", "SPY") is True


def test_unknown_template_fails_safe_wide_for_equities():
    # Unrecognised template -> widest window so a real stall is never
    # silenced; the cash-index clamp still restricts SPX.
    assert underlying_feed_expected(FRI_AFTER_HOURS, "SomeCustomTpl", "SPY") is True
    assert underlying_feed_expected(FRI_AFTER_HOURS, "SomeCustomTpl", "SPX") is False


def test_weekend_is_never_expected():
    assert underlying_feed_expected(SAT, "USEQ24Hour", "SPY") is False
    assert underlying_feed_expected(SAT, "Default", "SPX") is False


def test_missing_symbol_uses_template_window_only():
    # No symbol -> can't apply the cash-index clamp; fall back to the
    # template window (don't crash).
    assert underlying_feed_expected(FRI_REGULAR, "Default", None) is True
    assert underlying_feed_expected(FRI_AFTER_HOURS, "Default", None) is False
