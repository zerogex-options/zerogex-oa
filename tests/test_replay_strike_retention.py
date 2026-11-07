"""Replay sessions whose per-strike rows have aged out say so.

``gex_by_strike`` is pruned at DATA_RETENTION_DAYS (60 in production) while
``gex_summary`` is retention-exempt, so ``/api/replay/sessions`` lists sessions
that replay their levels and candles over an EMPTY strike profile.  Seen on the
live site on 2026-11-07: ``/replay?symbol=ES`` listed Aug 14 to Nov 6, and
every session before Sep 8 rendered with no strike profile under a "Full
session" label.

``has_strikes`` on both the session list and the range payload is how the
website tells those sessions apart.  Pinned here:

* it is ``False`` exactly for a session that ended before the oldest per-strike
  row still on file, and ``True`` for one that ended at or after it;
* it is ``None`` -- never ``False`` -- when that lookup fails, so a slow
  database cannot relabel every session as aged out;
* on ``/range`` it is whole-chain whatever the scope, because an aged-out
  session under 0DTE and a session with no 0DTE book both come back as empty
  ladders and the page must not tell one story for the other;
* it survives the ES / NQ projection unchanged.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.api.database import DatabaseManager

from tests.test_replay_endpoint import _build_app

# The oldest gex_by_strike row on file: the 2026-09-08 open.
HORIZON = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)


def _session(day: date, first: datetime, last: datetime, bars: int = 390) -> dict:
    return {"session_date": day, "bar_count": bars, "first_ts": first, "last_ts": last}


SESSIONS = [
    # Inside the window.
    _session(
        date(2026, 9, 9),
        datetime(2026, 9, 9, 13, 30, tzinfo=timezone.utc),
        datetime(2026, 9, 9, 19, 59, tzinfo=timezone.utc),
    ),
    # The oldest retained session: it starts ON the horizon.
    _session(
        date(2026, 9, 8),
        datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc),
        datetime(2026, 9, 8, 19, 59, tzinfo=timezone.utc),
    ),
    # Aged out: the whole session is older than the oldest strike row.
    _session(
        date(2026, 9, 4),
        datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc),
        datetime(2026, 9, 4, 19, 59, tzinfo=timezone.utc),
    ),
]


def _frames(day_ts: list[datetime], with_strikes: bool) -> list[dict]:
    strikes = (
        [
            {
                "strike": Decimal("6600"),
                "net_gex": Decimal("1.5"),
                "call_gex": Decimal("2.0"),
                "put_gex": Decimal("-0.5"),
            }
        ]
        if with_strikes
        else []
    )
    return [
        {
            "timestamp": ts,
            "gamma_flip": Decimal("6590"),
            "call_wall": Decimal("6650"),
            "put_wall": Decimal("6550"),
            "max_pain": Decimal("6600"),
            "pin_strike": Decimal("6600"),
            "pin_confidence": Decimal("0.4"),
            "max_gamma_strike": Decimal("6600"),
            "strikes": list(strikes),
        }
        for ts in day_ts
    ]


# --------------------------------------------------------------------------
# The lookup
# --------------------------------------------------------------------------


def _db_with_fetchval(result):
    db = DatabaseManager()
    calls: list = []

    class _Conn:
        async def fetchval(self, query, *args):
            calls.append((query, args))
            if isinstance(result, Exception):
                raise result
            return result

    @asynccontextmanager
    async def _acquire():
        yield _Conn()

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db, calls


def test_lookup_returns_the_oldest_strike_row():
    db, calls = _db_with_fetchval(HORIZON)
    assert asyncio.run(db.get_strike_history_start("SPX")) == {"oldest_ts": HORIZON}
    query, args = calls[0]
    assert args == ("SPX",)
    flat = " ".join(query.split())
    # One probe down the (underlying, timestamp, ...) primary key.
    assert "FROM gex_by_strike WHERE underlying = $1 ORDER BY timestamp ASC LIMIT 1" in flat


def test_lookup_reports_an_empty_table_as_no_rows_not_a_failure():
    db, _ = _db_with_fetchval(None)
    assert asyncio.run(db.get_strike_history_start("SPX")) == {"oldest_ts": None}


def test_lookup_reports_a_failed_read_as_none():
    db, _ = _db_with_fetchval(TimeoutError("statement timeout"))
    assert asyncio.run(db.get_strike_history_start("SPX")) is None


def test_lookup_is_cached_including_the_no_rows_answer():
    for value in (HORIZON, None):
        db, calls = _db_with_fetchval(value)
        first = asyncio.run(db.get_strike_history_start("SPX"))
        second = asyncio.run(db.get_strike_history_start("SPX"))
        assert first == second == {"oldest_ts": value}
        assert len(calls) == 1


def test_a_failed_lookup_is_not_cached():
    db, calls = _db_with_fetchval(TimeoutError("statement timeout"))
    asyncio.run(db.get_strike_history_start("SPX"))
    asyncio.run(db.get_strike_history_start("SPX"))
    assert len(calls) == 2


# --------------------------------------------------------------------------
# /api/replay/sessions
# --------------------------------------------------------------------------


def _sessions_body(monkeypatch, history, sessions=SESSIONS, symbol="SPX"):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_replay_session_dates = AsyncMock(return_value=sessions)
    probe = AsyncMock(return_value=history)
    dbmod.DatabaseManager.get_strike_history_start = probe
    with TestClient(app) as client:
        r = client.get(f"/api/replay/sessions?symbol={symbol}&limit=60")
    assert r.status_code == 200, r.text
    return r.json(), probe


def test_sessions_flag_the_ones_whose_strikes_aged_out(monkeypatch):
    body, probe = _sessions_body(monkeypatch, {"oldest_ts": HORIZON})
    flags = {s["date"]: s["has_strikes"] for s in body["sessions"]}
    assert flags == {"2026-09-09": True, "2026-09-08": True, "2026-09-04": False}
    # One lookup labels the whole list.
    probe.assert_awaited_once_with("SPX")
    # Everything the list carried before is unchanged.
    old = body["sessions"][2]
    assert old["bar_count"] == 390
    assert old["first_ts"] == "2026-09-04T13:30:00+00:00"


def test_a_partly_pruned_session_still_has_strikes(monkeypatch):
    """The prune runs at 02:00, outside the cash session, but a manual
    ``make db-prune`` can cut through one. That session keeps its later
    minutes' strikes, so it is not labelled as having none."""
    mid_session = datetime(2026, 9, 8, 16, 0, tzinfo=timezone.utc)
    body, _ = _sessions_body(monkeypatch, {"oldest_ts": mid_session})
    flags = {s["date"]: s["has_strikes"] for s in body["sessions"]}
    assert flags["2026-09-08"] is True
    assert flags["2026-09-04"] is False


def test_no_strike_rows_at_all_means_every_session_lacks_them(monkeypatch):
    body, _ = _sessions_body(monkeypatch, {"oldest_ts": None})
    assert [s["has_strikes"] for s in body["sessions"]] == [False, False, False]


def test_a_failed_lookup_labels_nothing(monkeypatch):
    body, _ = _sessions_body(monkeypatch, None)
    assert [s["has_strikes"] for s in body["sessions"]] == [None, None, None]
    assert body["count"] == 3


def test_an_empty_list_skips_the_lookup(monkeypatch):
    body, probe = _sessions_body(monkeypatch, {"oldest_ts": HORIZON}, sessions=[])
    assert body["sessions"] == []
    probe.assert_not_awaited()


# --------------------------------------------------------------------------
# /api/replay/range
# --------------------------------------------------------------------------


def _range_body(monkeypatch, frames, history, query="symbol=SPX&date=2026-09-04"):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=frames)
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    dbmod.DatabaseManager.get_gex_expiration_shares_for_session = AsyncMock(
        return_value={"expirations": [], "far_bucket": False, "rows": {}}
    )
    probe = AsyncMock(return_value=history)
    dbmod.DatabaseManager.get_strike_history_start = probe
    with TestClient(app) as client:
        r = client.get(f"/api/replay/range?{query}")
    assert r.status_code == 200, r.text
    return r.json(), probe


OLD_DAY = [
    datetime(2026, 9, 4, 13, 30, tzinfo=timezone.utc),
    datetime(2026, 9, 4, 19, 59, tzinfo=timezone.utc),
]
NEW_DAY = [
    datetime(2026, 9, 9, 13, 30, tzinfo=timezone.utc),
    datetime(2026, 9, 9, 19, 59, tzinfo=timezone.utc),
]


def test_range_says_an_aged_out_session_has_no_strikes(monkeypatch):
    body, probe = _range_body(monkeypatch, _frames(OLD_DAY, False), {"oldest_ts": HORIZON})
    assert body["has_strikes"] is False
    probe.assert_awaited_once_with("SPX")
    # The levels the session still has ride along untouched.
    assert body["frames"][0]["call_wall"] == pytest.approx(6650.0)
    assert body["frames"][0]["strikes"] == []


def test_range_says_a_retained_session_has_strikes(monkeypatch):
    body, _ = _range_body(
        monkeypatch,
        _frames(NEW_DAY, True),
        {"oldest_ts": HORIZON},
        query="symbol=SPX&date=2026-09-09",
    )
    assert body["has_strikes"] is True
    assert len(body["frames"][0]["strikes"]) == 1


@pytest.mark.parametrize("scope", ["0dte", "2026-09-11"])
def test_range_flag_is_whole_chain_under_a_scope(monkeypatch, scope):
    """Under a scope, an aged-out session and a session with no contracts in
    that scope both return empty ladders. The flag is what separates them,
    so it must not follow the scope."""
    aged, _ = _range_body(
        monkeypatch,
        _frames(OLD_DAY, False),
        {"oldest_ts": HORIZON},
        query=f"symbol=SPX&date=2026-09-04&expirations={scope}",
    )
    assert aged["has_strikes"] is False
    no_book, _ = _range_body(
        monkeypatch,
        _frames(NEW_DAY, False),
        {"oldest_ts": HORIZON},
        query=f"symbol=SPX&date=2026-09-09&expirations={scope}",
    )
    assert no_book["has_strikes"] is True


def test_range_failed_lookup_is_unknown_not_aged_out(monkeypatch):
    body, _ = _range_body(monkeypatch, _frames(OLD_DAY, False), None)
    assert body["has_strikes"] is None


def test_range_with_no_frames_skips_the_lookup(monkeypatch):
    body, probe = _range_body(monkeypatch, [], {"oldest_ts": HORIZON})
    assert body["count"] == 0
    assert body["has_strikes"] is None
    probe.assert_not_awaited()


# --------------------------------------------------------------------------
# ES / NQ: answered from SPX / NDX, the flag carried across unchanged
# --------------------------------------------------------------------------


def test_es_sessions_read_the_spx_horizon_and_keep_the_flag(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    fm = sys.modules["src.api.futures_middleware"]
    from src.jobs.futures_projection import FuturesBasis

    basis = FuturesBasis(
        index_symbol="SPX",
        futures_symbol="ES",
        ratio=1.0067,
        source="measured",
        observed_at=datetime(2026, 9, 4, 20, 0, tzinfo=timezone.utc),
        sample_count=5,
        feed_symbol="@ES",
    )

    async def fake_resolve_basis(db, symbol, **kwargs):
        return basis if symbol.upper() in ("ES", "SPX") else None

    async def fake_live_spot(index_symbol):
        return 6650.25

    monkeypatch.setattr(fm, "resolve_basis", fake_resolve_basis)
    monkeypatch.setattr(fm, "_live_futures_spot", fake_live_spot)
    monkeypatch.setattr(fm, "_db_manager", lambda: object())
    dbmod.DatabaseManager.get_replay_session_dates = AsyncMock(return_value=SESSIONS)
    probe = AsyncMock(return_value={"oldest_ts": HORIZON})
    dbmod.DatabaseManager.get_strike_history_start = probe
    with TestClient(app) as client:
        r = client.get("/api/replay/sessions?symbol=ES&limit=60")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["symbol"] == "ES"
    probe.assert_awaited_once_with("SPX")
    assert [s["has_strikes"] for s in body["sessions"]] == [True, True, False]
