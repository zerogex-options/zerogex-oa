"""Freshness of the open-position book: ``/api/signals/trades-live``.

Two wrong answers were possible here and the fix has to avoid both.

**The one that shipped.** The path fell under the ``/api/signals/trades*``
glob alongside ``trades-history``, so it was graded ``historical`` — the
profile whose whole meaning is "immutable once written, age is never a
fault." A dead signal engine therefore reported ``static``: the endpoint
that exists to show what the engine is holding *right now* was structurally
incapable of reporting that the engine had stopped.

**The one the obvious fix would have introduced.** Reclassifying the path to
``signals_cycle`` and stopping there grades it on the newest row timestamp —
and every row's ``signal_timestamp``/``opened_at`` is the ENTRY instant. A
position opened at 09:35 and held all day would read hours stale at 13:00 on
a perfectly healthy engine, which is the same false-positive class an
integrator already reported against ``/api/flow/smart-money``.

The engine marks every open position to market each cycle and bumps
``updated_at`` doing it, so the response now carries a top-level
``last_refreshed_at`` = newest ``updated_at`` across the book, and the
envelope grades against that. Entry age stops mattering; refresh age is what
gets measured.

The tests below drive the real route and grade the body it actually returned
(there is no freezegun here, so the clock is supplied to ``build_freshness``
rather than faked globally). Hand-writing the payload would let the router
and the envelope drift apart silently.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.api import freshness as fr

# Friday, mid regular session. Chosen deliberately: outside market hours
# every feed-backed profile reports ``session_closed``, which masks exactly
# the bug this file is about — that is why it survived every smoke run.
NOW = datetime(2026, 8, 21, 17, 0, tzinfo=timezone.utc)  # 13:00 ET
ENTRY = datetime(2026, 8, 21, 13, 35, tzinfo=timezone.utc)  # 09:35 ET

PROFILE = fr.resolve_profile("/api/signals/trades-live")


def _trade(updated_at):
    """A position entered near the open. Both stamps the payload carried
    before this fix are the ENTRY instant; only ``updated_at`` moves."""
    return {
        "id": 1,
        "underlying": "SPY",
        "signal_timestamp": ENTRY,
        "opened_at": ENTRY,
        "updated_at": updated_at,
        "status": "open",
        "direction": "long",
        "entry_price": 1.85,
        "current_price": 2.10,
        "quantity_open": 10,
    }


@pytest.fixture
def book(monkeypatch: pytest.MonkeyPatch):
    """Returns a callable: rows -> (response body, freshness graded at NOW)."""
    monkeypatch.setenv("ENVIRONMENT", "development")
    from src.api import database as dbmod

    monkeypatch.setattr(dbmod.DatabaseManager, "connect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "disconnect", AsyncMock(return_value=None))

    from src.api.main import app
    from src.api.routers.trade_signals import get_db

    def run(rows):
        class FakeDB:
            async def get_live_signal_trades(self):
                return rows

        app.dependency_overrides[get_db] = FakeDB
        try:
            with TestClient(app) as c:
                resp = c.get("/api/v2/signals/trades-live")
        finally:
            app.dependency_overrides.pop(get_db, None)
        assert resp.status_code == 200
        body = resp.json()
        return body, fr.build_freshness(body["data"], profile=PROFILE, now=NOW)

    return run


# ---------------------------------------------------------------------------
# The two wrong answers
# ---------------------------------------------------------------------------


def test_a_stopped_engine_is_reported_stale_not_static(book):
    """The shipped bug. Graded ``historical`` this returned ``static`` — the
    envelope's way of saying "age here is never a fault" — while the engine
    had been dead for three and a half hours."""
    _, f = book([_trade(ENTRY)])
    assert f.freshness_status is fr.FreshnessStatus.STALE
    assert f.age_seconds == pytest.approx((NOW - ENTRY).total_seconds())


def test_a_position_held_since_the_open_is_fresh_on_a_healthy_engine(book):
    """The false positive the naive fix would have introduced. The entry is
    3.5h old and stays 3.5h old all day; the mark is seconds old, because the
    reconcile loop bumps ``updated_at`` on every open position every cycle."""
    marked = NOW - timedelta(seconds=5)
    _, f = book([_trade(marked)])
    assert f.freshness_status is fr.FreshnessStatus.FRESH
    assert f.latest_event_at == marked


def test_the_entry_instant_is_never_read_as_the_refresh(book):
    """Load-bearing: ``signal_timestamp`` is a source key, so without a
    response-level stamp outranking it the envelope grades entry age."""
    marked = NOW - timedelta(seconds=5)
    body, f = book([_trade(marked)])
    assert f.latest_event_at == marked != ENTRY
    assert body["data"]["last_refreshed_at"] is not None


def test_the_newest_mark_across_the_book_wins(book):
    """One unresolvable mark on an old position must not drag the whole
    view's freshness down while the rest of the book is being marked."""
    marked = NOW - timedelta(seconds=5)
    _, f = book([_trade(ENTRY), _trade(marked), _trade(NOW - timedelta(minutes=40))])
    assert f.latest_event_at == marked
    assert f.freshness_status is fr.FreshnessStatus.FRESH


def test_an_empty_book_makes_no_freshness_claim(book):
    """A healthy engine holding nothing and a dead engine holding nothing
    produce identical payloads. ``unknown`` is the honest answer; ``fresh``
    would be a fabrication and ``stale`` a false alarm."""
    body, f = book([])
    assert body["data"] == {"trades": [], "count": 0, "last_refreshed_at": None}
    assert f.freshness_status is fr.FreshnessStatus.UNKNOWN
    assert f.latest_event_at is None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_the_live_book_is_not_filed_under_history():
    """``trades-live`` and ``trades-history`` differ by one hyphenated word
    and sit under the same glob. Order matters: the specific entry has to
    precede ``/api/signals/trades*`` or it is unreachable."""
    assert fr.resolve_profile("/api/signals/trades-live") is fr.SIGNALS_CYCLE
    assert fr.resolve_profile("/api/signals/trades-history") is fr.HISTORICAL


def test_realised_outcomes_are_still_history():
    """The sibling repair. Realised per-signal outcomes hang off the
    component name — ``/api/signals/{signal_name}/events`` — so the old
    ``/api/signals/events*`` pattern matched nothing at all and these fell
    through to the live signals cadence, which grades sparse triggered
    events on recency and calls a quiet tape stale."""
    assert fr.resolve_profile("/api/signals/{signal_name}/events") is fr.HISTORICAL
    # The dead pattern must not come back: it looks plausible and matches
    # zero real paths, so nothing else in the suite would notice.
    assert not any(p == "/api/signals/events*" for p, _ in fr.ENDPOINT_CADENCE)


def test_no_registry_pattern_is_unreachable():
    """Generalises the dead-glob bug: a pattern that matches no route in the
    published surface is either a typo or dead weight, and either way it is
    silently not doing the job it looks like it is doing.

    Scoped to the signals family, where the failure actually occurred and
    where every path is enumerable without standing up the app.
    """
    from src.api.routers import trade_signals

    paths = [
        r.path
        for r in trade_signals.router.routes
        if getattr(r, "path", "").startswith("/api/signals")
    ]
    assert len(paths) > 5, "router shape changed — this guard is watching nothing"

    import fnmatch

    for pattern, _ in fr.ENDPOINT_CADENCE:
        if not pattern.startswith("/api/signals"):
            continue
        assert any(
            fnmatch.fnmatchcase(p, pattern) for p in paths
        ), f"{pattern!r} matches none of the {len(paths)} /api/signals routes"


# ---------------------------------------------------------------------------
# The envelope side of the contract
# ---------------------------------------------------------------------------


def test_a_response_level_refresh_stamp_outranks_row_bookkeeping():
    """``updated_at`` on a ROW is bookkeeping — a write time, consulted only
    when nothing observed a market (an option_chains re-write of an old
    snapshot bumps it to now). Promoting it wholesale would have regressed
    that. ``last_refreshed_at`` is a separate, response-level key in the
    source tier, so it grades without disturbing the row rules."""
    assert "last_refreshed_at" in fr._SOURCE_KEYS
    assert "last_refreshed_at" not in fr._BOOKKEEPING_KEYS
    assert "updated_at" in fr._BOOKKEEPING_KEYS

    # A row write time still loses to a real observation.
    older = NOW - timedelta(minutes=30)
    _, latest = fr._scan_timestamps({"rows": [{"timestamp": older, "updated_at": NOW}]}, NOW)
    assert latest == older


def test_payloads_without_the_new_key_are_unaffected():
    """Every other endpoint keeps its existing grading."""
    marked = NOW - timedelta(seconds=10)
    f = fr.build_freshness([{"timestamp": marked}], profile=PROFILE, now=NOW)
    assert f.freshness_status is fr.FreshnessStatus.FRESH
    assert f.latest_event_at == marked
