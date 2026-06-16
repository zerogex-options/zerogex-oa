"""Regression coverage for ``is_rth_settled``.

This predicate gates "no data yet" diagnostics in the signals engine
(``no flow_contract_facts rows in last 30 minutes`` and the iv_rank
NULL-components warning). The 04:00–20:00 ET extended-hours window the
previous gate used is too permissive: SPY/QQQ options legitimately have
no last-30-min rows pre-market, and the analytics engine has not yet
UPSERTed today's ``daily_atm_iv`` row before the first Greek-bearing
cycle lands — so any worker restart in that 5½-hour window false-fired
the diagnostics.

``is_rth_settled`` tightens the gate to (RTH ∧ at least 30 min past
09:30 ET) by default, so a 04:19 ET restart stays silent while a
genuine ingestion gap at 11:30 ET still surfaces.
"""

from datetime import datetime

import pytz

from src.market_calendar import is_rth_settled

ET = pytz.timezone("US/Eastern")


def _et(y, mo, d, h, mi):
    return ET.localize(datetime(y, mo, d, h, mi))


def test_rth_settled_true_during_settled_rth_weekday():
    # Tue 11:00 ET — 90 min past open, well inside the gate.
    assert is_rth_settled(_et(2026, 6, 16, 11, 0)) is True


def test_rth_settled_false_before_open():
    # Tue 04:19 ET — pre-market extended hours; the precise restart
    # window that previously false-fired the flow-missing diagnostic.
    assert is_rth_settled(_et(2026, 6, 16, 4, 19)) is False


def test_rth_settled_false_inside_open_grace_window():
    # Tue 09:45 ET — 15 min past open, still inside the default 30-min
    # settle grace. SPY/QQQ may legitimately have no last-30-min flow
    # rows here because most of the look-back is pre-market.
    assert is_rth_settled(_et(2026, 6, 16, 9, 45)) is False


def test_rth_settled_true_exactly_at_settle_boundary():
    # 10:00 ET = 09:30 + 30 min. The boundary is inclusive.
    assert is_rth_settled(_et(2026, 6, 16, 10, 0)) is True


def test_rth_settled_true_at_close():
    # 16:00 ET — close is inclusive (matches is_market_hours).
    assert is_rth_settled(_et(2026, 6, 16, 16, 0)) is True


def test_rth_settled_false_after_close():
    # Tue 16:30 ET — past cash close.
    assert is_rth_settled(_et(2026, 6, 16, 16, 30)) is False


def test_rth_settled_false_on_weekend():
    # Sat 11:00 ET — gate must stay closed across both weekend days
    # so a Saturday operator restart doesn't fire the diagnostic.
    assert is_rth_settled(_et(2026, 6, 13, 11, 0)) is False


def test_rth_settled_honours_custom_settle_minutes():
    """Callers can widen or narrow the grace; gate respects the override."""
    ts = _et(2026, 6, 16, 10, 15)  # 45 min past open
    # 30-min grace (default) — inside the window.
    assert is_rth_settled(ts, settle_minutes=30) is True
    # 60-min grace — still inside the grace, gate stays closed.
    assert is_rth_settled(ts, settle_minutes=60) is False
    # 0-min grace — equivalent to plain RTH (09:30–16:00 ET).
    assert is_rth_settled(ts, settle_minutes=0) is True
