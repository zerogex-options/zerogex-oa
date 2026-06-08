"""Cash-session keying tests.

Cash-session keying is now unconditional in production: the Item 8
``USE_CASH_SESSION_KEYING`` rollout flag was removed once the staged
cutover (Phase 5) completed.  Both the in-memory _FlowAccumulator
session key (main_engine.py) and the SQL LAG-CASE date-equality clause
(api/database.py) partition by the cash-session date — the date the
09:30 ET cash open belongs to — so pre-09:30 ET rows belong to the PRIOR
session, matching the TradeStation vendor's cumulative-volume reset
boundary exactly.

What this file pins:
* the in-memory ``_bucket_session_date`` cash-session semantics;
* the SQL ``_flow_lag_same_session_clause`` builder.  It retains a
  ``use_cash_keying`` parameter whose ``False`` (legacy calendar-date)
  formulation is used only as the comparison baseline by the Phase 4
  parity regression harness — production always passes ``True``.

Related:
* ``cash_session_date`` semantics themselves -> test_cash_session_date.py
* end-to-end legacy-vs-cash parity on a real day of option_chains data
  -> the Phase 4 parity harness (test_cash_session_keying_parity.py).
"""

from datetime import date, datetime

import pytz

from src.api.database import _flow_lag_same_session_clause
from src.ingestion.main_engine import IngestionEngine

_ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# In-memory _bucket_session_date (cash-session keying, unconditional)
# ---------------------------------------------------------------------------


def test_bucket_session_date_pre_cash_belongs_to_prior_session():
    """09:29 ET on day D belongs to cash session D-1 (pre-09:30)."""
    ts = _ET.localize(datetime(2025, 4, 15, 9, 29))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 14)


def test_bucket_session_date_at_open_belongs_to_today():
    """09:30 ET on day D is the FIRST instant of cash session D (>=, not >)."""
    ts = _ET.localize(datetime(2025, 4, 15, 9, 30))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


def test_bucket_session_date_during_rth_returns_today():
    """During RTH the cash-session date IS the calendar date."""
    ts = _ET.localize(datetime(2025, 4, 15, 12, 0))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


def test_bucket_session_date_late_evening_still_today():
    """23:59 ET on day D is still cash session D (pre-midnight extended hrs)."""
    ts = _ET.localize(datetime(2025, 4, 15, 23, 59))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


# ---------------------------------------------------------------------------
# SQL LAG-CASE clause builder.  Production always uses the cash formulation
# (use_cash_keying=True); the legacy formulation is retained as the parity
# harness's comparison baseline, so both are pinned here.
# ---------------------------------------------------------------------------


def test_lag_clause_legacy_uses_bare_calendar_date():
    """Legacy baseline clause: bare AT TIME ZONE on both sides, no time shift."""
    clause = _flow_lag_same_session_clause(use_cash_keying=False)
    assert "AT TIME ZONE 'America/New_York'" in clause
    assert "interval" not in clause
    assert "9 hours 30 minutes" not in clause


def test_lag_clause_cash_shifts_both_sides_by_cash_open():
    """Cash-keying clause: subtract 9h30m on BOTH sides before truncating."""
    clause = _flow_lag_same_session_clause(use_cash_keying=True)
    # The shift must apply to BOTH the LAG and the current timestamp;
    # otherwise the equality misclassifies pre-09:30 rows on one side.
    # The shift expression is inlined four times per side after the
    # weekend roll-back was added (once for EXTRACT(DOW), three for
    # the CASE branches), so both shift markers appear >= 2 with the
    # same count across LAG and current.
    assert clause.count("interval '9 hours 30 minutes'") >= 2
    assert clause.count("AT TIME ZONE 'America/New_York'") >= 2
    assert "::date" in clause
    # Both sides of the equality must be present.
    assert "LAG(s.timestamp) OVER w" in clause
    assert "s.timestamp AT TIME ZONE 'America/New_York'" in clause


def test_lag_clause_cash_rolls_weekend_back_to_a_trading_day():
    """Cash-keying clause must roll Sat/Sun (and NYSE holidays) back
    to the most recent trading day so a gap-bridge timestamp (Mon
    00:00 ET, or Tue 06:00 ET after a Mon holiday) doesn't get
    assigned to a phantom "non-trading session" with no 09:30 ET
    cash open to anchor against.

    Regression for the 2026-06-01 phantom-midnight bug where every
    contract that traded the prior Friday generated a spurious flow
    event at exactly Mon 00:00 ET because both formulations agreed
    that the weekend-bridge was a session boundary.
    """
    clause = _flow_lag_same_session_clause(use_cash_keying=True)
    assert "EXTRACT(DOW" in clause
    # The walk-back is now expressed as COALESCE over a chain of
    # ``CASE WHEN <is_trading_day(candidate)> THEN candidate END``
    # branches; the DOW check uses ``NOT IN (0, 6)`` to exclude both
    # Sunday and Saturday simultaneously.
    assert "NOT IN (0, 6)" in clause
    assert "COALESCE" in clause


def test_lag_clause_cash_rolls_holidays_back_when_present(monkeypatch):
    """When ``NYSE_HOLIDAYS`` is non-empty, the cash-keying clause
    must also exclude those dates from the trading-day predicate so a
    Tue-after-Mon-holiday timestamp lands on the prior Friday's
    session rather than the holiday Monday."""
    import src.api.database as db_mod
    from datetime import date

    monkeypatch.setattr(db_mod, "NYSE_HOLIDAYS", {date(2026, 7, 3), date(2026, 11, 26)})
    clause = db_mod._flow_lag_same_session_clause(use_cash_keying=True)
    assert "NOT IN (DATE '2026-07-03', DATE '2026-11-26')" in clause


def test_lag_clause_cash_falls_back_to_weekend_only_with_no_holidays(monkeypatch):
    """An empty ``NYSE_HOLIDAYS`` set must degrade to weekend-only
    walk-back without producing an ``IN ()`` empty-list syntax error."""
    import src.api.database as db_mod

    monkeypatch.setattr(db_mod, "NYSE_HOLIDAYS", set())
    clause = db_mod._flow_lag_same_session_clause(use_cash_keying=True)
    # No DATE literal -> the holiday predicate is omitted entirely.
    assert "DATE '" not in clause
    # But the weekend predicate must still be there.
    assert "NOT IN (0, 6)" in clause


def test_lag_clause_both_forms_compare_lag_to_current():
    """Sanity: both forms compare LAG(s.timestamp) OVER w to s.timestamp."""
    for use_cash_keying in (False, True):
        clause = _flow_lag_same_session_clause(use_cash_keying=use_cash_keying)
        assert "LAG(s.timestamp) OVER w" in clause
        assert "s.timestamp" in clause
        # Single equality, not a range
        assert clause.count("=") == 1
