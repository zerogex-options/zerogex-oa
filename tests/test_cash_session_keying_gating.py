"""Phase 2+3 gating tests for the ``USE_CASH_SESSION_KEYING`` feature flag.

The flag is the routing switch for Item 8's cash-session refactor.  When
on, both the in-memory _FlowAccumulator session key (main_engine.py) and
the SQL LAG-CASE date-equality clause (api/database.py) partition by
the cash-session date (the date the 09:30 ET cash open belongs to)
rather than the ET calendar date.  Pre-09:30 ET rows belong to the
PRIOR session, matching the TradeStation vendor's cumulative-volume
reset boundary exactly.

What this file pins: ONLY the flag-routing behavior on both sites.

* ``cash_session_date`` semantics themselves -> test_cash_session_date.py
* legacy ET-calendar-date in-memory behavior -> the
  test_ingestion_session_open_stale_volume.py / test_ingestion_volume_*
  suites (flag default off, so those continue to gate the legacy path)
* end-to-end parity between flag-off and flag-on across a real day of
  option_chains data -> Phase 4 parity harness (separate commit)
"""

from datetime import date, datetime

import pytest
import pytz

from src import config
from src.api.database import _flow_lag_same_session_clause
from src.ingestion.main_engine import IngestionEngine

_ET = pytz.timezone("US/Eastern")


@pytest.fixture
def flag_off(monkeypatch):
    monkeypatch.setattr(config, "USE_CASH_SESSION_KEYING", False)


@pytest.fixture
def flag_on(monkeypatch):
    monkeypatch.setattr(config, "USE_CASH_SESSION_KEYING", True)


# ---------------------------------------------------------------------------
# In-memory _bucket_session_date routing
# ---------------------------------------------------------------------------


def test_bucket_session_date_flag_off_returns_et_calendar_date(flag_off):
    """09:29 ET on day D is calendar D under legacy keying."""
    ts = _ET.localize(datetime(2025, 4, 15, 9, 29))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


def test_bucket_session_date_flag_on_pre_cash_belongs_to_prior_session(flag_on):
    """09:29 ET on day D belongs to cash session D-1 under cash keying."""
    ts = _ET.localize(datetime(2025, 4, 15, 9, 29))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 14)


def test_bucket_session_date_flag_on_at_open_belongs_to_today(flag_on):
    """09:30 ET on day D is the FIRST instant of cash session D (>=, not >)."""
    ts = _ET.localize(datetime(2025, 4, 15, 9, 30))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


def test_bucket_session_date_flag_on_during_rth_returns_today(flag_on):
    """During RTH the cash-session date IS the calendar date."""
    ts = _ET.localize(datetime(2025, 4, 15, 12, 0))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


def test_bucket_session_date_flag_on_late_evening_still_today(flag_on):
    """23:59 ET on day D is still cash session D (pre-midnight extended hrs)."""
    ts = _ET.localize(datetime(2025, 4, 15, 23, 59))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)


def test_bucket_session_date_flag_routing_is_per_call(flag_off, monkeypatch):
    """Toggling the flag mid-process must take effect on the next call.

    The accumulator path reads the flag at call time (not at import),
    so a runtime flip via env-watcher / config reload would apply
    immediately without a process restart.
    """
    ts = _ET.localize(datetime(2025, 4, 15, 9, 29))
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 15)
    monkeypatch.setattr(config, "USE_CASH_SESSION_KEYING", True)
    assert IngestionEngine._bucket_session_date(ts) == date(2025, 4, 14)


# ---------------------------------------------------------------------------
# SQL LAG-CASE clause routing
# ---------------------------------------------------------------------------


def test_lag_clause_flag_off_uses_bare_calendar_date():
    """Legacy clause: bare AT TIME ZONE on both sides, no time shift."""
    clause = _flow_lag_same_session_clause(use_cash_keying=False)
    assert "AT TIME ZONE 'America/New_York'" in clause
    assert "interval" not in clause
    assert "9 hours 30 minutes" not in clause


def test_lag_clause_flag_on_shifts_both_sides_by_cash_open():
    """Cash-keying clause: subtract 9h30m on BOTH sides before truncating."""
    clause = _flow_lag_same_session_clause(use_cash_keying=True)
    # The shift must apply to BOTH the LAG and the current timestamp;
    # otherwise the equality misclassifies pre-09:30 rows on one side.
    assert clause.count("interval '9 hours 30 minutes'") == 2
    assert clause.count("AT TIME ZONE 'America/New_York'") == 2
    assert "::date" in clause


def test_lag_clause_both_forms_compare_lag_to_current():
    """Sanity: both forms compare LAG(s.timestamp) OVER w to s.timestamp."""
    for use_cash_keying in (False, True):
        clause = _flow_lag_same_session_clause(use_cash_keying=use_cash_keying)
        assert "LAG(s.timestamp) OVER w" in clause
        assert "s.timestamp" in clause
        # Single equality, not a range
        assert clause.count("=") == 1
