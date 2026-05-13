"""Regression tests for the Event Timeline two-session window.

The Event Timeline endpoint (``GET /api/signals/{signal_name}/events``) is
backed by :func:`SignalsQueriesMixin.get_signal_component_events`. Each
signal's response must cover a *consistent* window regardless of how often
that signal emits scores:

* If live in an open session: the current session plus the previous session.
* Otherwise: the two most-recent fully-elapsed sessions.

Previously the read was bounded only by a row ``LIMIT`` (default 100) with
no time filter, so dense signals returned a few hours and sparse signals
returned days — that's the inconsistency this guards against.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, time
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.api.queries.signals import _two_session_cutoff


_ET = ZoneInfo("America/New_York")


class _CapturingConn:
    """Records the parameters passed to the last ``fetch`` call."""

    def __init__(self):
        self.last_query = None
        self.last_args = None

    async def fetch(self, query, *args):
        self.last_query = query
        self.last_args = args
        return []


# ---------------------------------------------------------------------------
# _two_session_cutoff: spec coverage for the four scenarios in the user spec
# ---------------------------------------------------------------------------


def test_two_session_cutoff_during_open_session_returns_prior_session_open():
    # Wednesday 10:00 ET — market is live. Two sessions = Tue (prior) + Wed (current).
    now = datetime(2026, 4, 22, 10, 0, tzinfo=_ET)
    cutoff = _two_session_cutoff(now=now)
    assert cutoff == datetime(2026, 4, 21, 9, 30, tzinfo=_ET)


def test_two_session_cutoff_after_hours_weekday_returns_prior_session_open():
    # Wednesday 20:00 ET — after the close. Today's session is the "most recent"
    # fully-elapsed one, prior = Tuesday.
    now = datetime(2026, 4, 22, 20, 0, tzinfo=_ET)
    cutoff = _two_session_cutoff(now=now)
    assert cutoff == datetime(2026, 4, 21, 9, 30, tzinfo=_ET)


def test_two_session_cutoff_premarket_monday_skips_weekend():
    # Monday 08:00 ET — before today's open. Most recent session is Friday;
    # prior session is Thursday, so the cutoff must straddle the weekend.
    now = datetime(2026, 4, 27, 8, 0, tzinfo=_ET)
    cutoff = _two_session_cutoff(now=now)
    assert cutoff == datetime(2026, 4, 23, 9, 30, tzinfo=_ET)


def test_two_session_cutoff_weekend_returns_thursday_open():
    # Sunday afternoon — most recent session is Friday, prior is Thursday.
    now = datetime(2026, 4, 26, 13, 0, tzinfo=_ET)
    cutoff = _two_session_cutoff(now=now)
    assert cutoff == datetime(2026, 4, 23, 9, 30, tzinfo=_ET)


def test_two_session_cutoff_at_open_boundary_uses_today():
    # Exactly 09:30 ET on a weekday — today counts as the current session,
    # so the cutoff is yesterday's open (matches the during-session case).
    now = datetime(2026, 4, 22, 9, 30, tzinfo=_ET)
    cutoff = _two_session_cutoff(now=now)
    assert cutoff == datetime(2026, 4, 21, 9, 30, tzinfo=_ET)


# ---------------------------------------------------------------------------
# get_signal_component_events: the cutoff must be bound into the query
# ---------------------------------------------------------------------------


def test_get_signal_component_events_query_includes_session_cutoff_bound():
    db = DatabaseManager()
    conn = _CapturingConn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    asyncio.run(db.get_signal_component_events("SPY", "vol_expansion", limit=500))

    assert conn.last_query is not None, "fetch was never called"
    # The session cutoff is bind parameter $4 and the predicate must filter
    # rows newer than that timestamp.
    assert "scs.timestamp >= $4" in conn.last_query
    # And the matching argument must be a tz-aware datetime at 09:30 ET.
    symbol, component, limit, cutoff = conn.last_args
    assert (symbol, component, limit) == ("SPY", "vol_expansion", 500)
    assert isinstance(cutoff, datetime)
    assert cutoff.tzinfo is not None
    assert cutoff.astimezone(_ET).time() == time(9, 30)


def test_get_signal_component_events_cutoff_matches_helper():
    """The cutoff bound into the query must equal ``_two_session_cutoff()``.

    Different signals call the same method, so pinning equality here is how
    we prove every signal — Basic or Advanced — gets the *same* window.
    """
    db = DatabaseManager()
    conn = _CapturingConn()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    before = _two_session_cutoff()
    asyncio.run(db.get_signal_component_events("SPY", "tape_flow_bias"))
    after = _two_session_cutoff()

    _symbol, _component, _limit, cutoff = conn.last_args
    # Allow for a clock tick between calls — but the cutoff must land on the
    # same 09:30 ET boundary as the helper at the moment of the call.
    assert before <= cutoff <= after
