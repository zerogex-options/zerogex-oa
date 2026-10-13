"""``GET /api/v1/levels/{symbol}?expiration=`` -- levels for ONE expiration.

Asked for by a NinjaTrader customer: draw the 0DTE levels, or a chosen
expiration's, instead of the whole chain.  These pin what that integration
relies on:

  * ``all`` (and no parameter) is the whole-chain payload, untouched;
  * ``0dte`` is today's expiry until the 16:00 ET settlement, then the next
    one listed, and the response says which date it resolved to;
  * the scoped walls, flip and profile come from the website's own
    expiration-filtered strike-profile read, max pain from that expiration's
    own settlement, and the Pin Strike is not scoped;
  * every way a scope can fail is a 404 or 400 with a sentence a trader can
    act on, never whole-chain levels drawn under a 0DTE label.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.api.database import DatabaseManager

ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_expiration_accepts_the_three_forms_case_insensitively():
    from src.api.routers.levels import _parse_expiration

    assert _parse_expiration(None) == "all"
    assert _parse_expiration("") == "all"
    assert _parse_expiration(" ALL ") == "all"
    assert _parse_expiration("0DTE") == "0dte"
    assert _parse_expiration("0dte") == "0dte"
    assert _parse_expiration("2026-10-16") == date(2026, 10, 16)


@pytest.mark.parametrize("raw", ["1dte", "20261016", "2026-13-01", "10/16/2026", "next"])
def test_parse_expiration_rejects_anything_else_with_a_400(raw):
    from src.api.routers.levels import _parse_expiration

    with pytest.raises(HTTPException) as exc:
        _parse_expiration(raw)
    assert exc.value.status_code == 400


def _et(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=ET)


LISTED = [date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 16)]


def test_zero_dte_is_today_until_the_settlement():
    from src.api.routers.levels import _resolve_zero_dte

    assert _resolve_zero_dte(LISTED, _et(2026, 10, 13, 9, 31)) == date(2026, 10, 13)
    assert _resolve_zero_dte(LISTED, _et(2026, 10, 13, 15, 59)) == date(2026, 10, 13)


def test_zero_dte_rolls_to_the_next_expiry_after_the_settlement():
    """An ES chart in the evening session trades toward the NEXT expiry."""
    from src.api.routers.levels import _resolve_zero_dte

    assert _resolve_zero_dte(LISTED, _et(2026, 10, 13, 16, 0)) == date(2026, 10, 14)
    assert _resolve_zero_dte(LISTED, _et(2026, 10, 13, 23, 30)) == date(2026, 10, 14)


def test_zero_dte_skips_days_with_no_expiry_and_accepts_utc_clocks():
    from src.api.routers.levels import _resolve_zero_dte

    # Thursday the 15th lists nothing in this chain: the next one is Friday.
    assert _resolve_zero_dte(LISTED, _et(2026, 10, 15, 10, 0)) == date(2026, 10, 16)
    # 03:00 UTC on the 14th is still 23:00 ET on the 13th, after settlement.
    utc = datetime(2026, 10, 14, 3, 0, tzinfo=timezone.utc)
    assert _resolve_zero_dte(LISTED, utc) == date(2026, 10, 14)


def test_zero_dte_is_none_when_nothing_upcoming_is_listed():
    from src.api.routers.levels import _resolve_zero_dte

    assert _resolve_zero_dte(LISTED, _et(2026, 10, 16, 16, 30)) is None
    assert _resolve_zero_dte([], _et(2026, 10, 13, 10, 0)) is None


def test_latest_live_bucket_skips_a_tip_with_no_positioning():
    from src.api.routers.levels import _latest_live_bucket

    live = {"strikes": [{"strike": 100, "net_gamma": 5.0}]}
    empty = {"strikes": [{"strike": 100, "net_gamma": 0}]}
    assert _latest_live_bucket([live, empty, {"strikes": []}]) is live
    assert _latest_live_bucket([empty]) is None
    assert _latest_live_bucket(None) is None


# ---------------------------------------------------------------------------
# DB: per-expiration max pain
# ---------------------------------------------------------------------------


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    async def fetch(self, query, *args, timeout=None):
        self.calls += 1
        assert "max_pain_by_expiration" in query
        assert args == ("SPX",)
        return list(self.rows)


def _db_with(conn):
    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


@pytest.mark.parametrize(
    "stored",
    [
        '{"2026-10-13": 6650.0, "2026-10-16": 6700.0}',  # jsonb read as text
        {"2026-10-13": 6650.0, "2026-10-16": 6700.0},  # jsonb with a codec
    ],
)
def test_max_pain_for_expiration_reads_that_expirations_own_settlement(stored):
    conn = _Conn([{"max_pain_by_expiration": stored}])
    db = _db_with(conn)
    assert asyncio.run(db.get_latest_max_pain_for_expiration("spx", date(2026, 10, 16))) == 6700.0
    # A different expiration on the next poll is answered from the same read.
    assert asyncio.run(db.get_latest_max_pain_for_expiration("SPX", date(2026, 10, 13))) == 6650.0
    assert conn.calls == 1


def test_max_pain_for_an_unlisted_expiration_or_no_breakdown_is_none():
    db = _db_with(_Conn([{"max_pain_by_expiration": {"2026-10-13": 6650.0}}]))
    assert asyncio.run(db.get_latest_max_pain_for_expiration("SPX", date(2026, 10, 14))) is None
    db = _db_with(_Conn([{"max_pain_by_expiration": None}]))
    assert asyncio.run(db.get_latest_max_pain_for_expiration("SPX", date(2026, 10, 14))) is None
    db = _db_with(_Conn([]))
    assert asyncio.run(db.get_latest_max_pain_for_expiration("SPX", date(2026, 10, 14))) is None


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

# Expirations far enough out that a rolling 0DTE resolves to NEAR whatever the
# wall clock says when the suite runs.
NEAR = datetime.now(ET).date() + timedelta(days=3)
FAR = NEAR + timedelta(days=7)


def _summary(**overrides):
    base = {
        "timestamp": datetime(2026, 10, 13, 15, 0, tzinfo=timezone.utc),
        "symbol": "SPX",
        "spot_price": 6660.0,
        "net_gex_at_spot": -1.2e9,
        "gamma_flip": 6600.0,
        "gamma_flip_reason": None,
        "call_wall": 6800.0,
        "put_wall": 6500.0,
        "max_pain": 6650.0,
        "pin_strike": 6655.0,
        "pin_score": 1.5e9,
        "pin_confidence": 0.4,
        "pin_strike_reason": None,
    }
    base.update(overrides)
    return base


def _strike(strike, call, put):
    return {
        "strike": strike,
        "call_gamma": call,
        "put_gamma": put,
        "net_gamma": call + put,
        "call_oi": 10,
        "put_oi": 10,
    }


def _bucket(**overrides):
    base = {
        "timestamp": datetime(2026, 10, 13, 14, 55, tzinfo=timezone.utc),
        "symbol": "SPX",
        "close": 6661.0,
        "gamma_flip": 6640.0,
        "call_wall": 6700.0,
        "put_wall": 6620.0,
        "pin_strike": 6655.0,
        "strikes": [
            _strike(6600.0, 1e8, -9e8),
            _strike(6650.0, 4e8, -3e8),
            _strike(6660.0, 6e8, -1e8),
            _strike(6670.0, 7e8, -1e8),
            _strike(6750.0, 2e8, 0.0),
        ],
    }
    base.update(overrides)
    return base


def _build_app(monkeypatch, *, summary=None, timeseries=None, listed=None, max_pain=6700.0):
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    mocks = {
        "get_latest_gex_summary": AsyncMock(return_value=summary or _summary()),
        "get_latest_strike_gamma_profile": AsyncMock(return_value=[]),
        "get_gex_expirations": AsyncMock(return_value=[NEAR, FAR] if listed is None else listed),
        "get_strike_profile_timeseries": AsyncMock(
            side_effect=timeseries or (lambda *a, **k: [_bucket()])
        ),
        "get_latest_max_pain_for_expiration": AsyncMock(return_value=max_pain),
    }
    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    for name, mock in mocks.items():
        setattr(dbmod.DatabaseManager, name, mock)

    from src.api.main import app

    return TestClient(app), mocks


def test_no_parameter_and_all_serve_the_whole_chain_untouched(monkeypatch):
    client, mocks = _build_app(monkeypatch)
    with client:
        bare = client.get("/api/v1/levels/SPX").json()
        explicit = client.get("/api/v1/levels/SPX?expiration=ALL").json()
    for body in (bare, explicit):
        assert body["expiration"] is None
        assert body["levels"]["call_wall"] == 6800.0
        assert body["levels"]["max_pain"] == 6650.0
        assert body["net_gex_at_spot"] == -1.2e9
    mocks["get_strike_profile_timeseries"].assert_not_called()
    mocks["get_gex_expirations"].assert_not_called()


def test_zero_dte_scopes_walls_flip_max_pain_and_profile(monkeypatch):
    client, mocks = _build_app(monkeypatch)
    with client:
        r = client.get("/api/v1/levels/spx?expiration=0DTE&strikes=3")
    assert r.status_code == 200
    body = r.json()
    assert body["expiration"] == NEAR.isoformat()
    assert body["levels"] == {
        "gamma_flip": 6640.0,
        "gamma_flip_reason": None,
        "gamma_flip_label": None,
        "call_wall": 6700.0,
        "put_wall": 6620.0,
        # That expiration's own settlement, not the front-month scalar.
        "max_pain": 6700.0,
        # 0DTE by construction: never scoped, exactly as on the website.
        "pin_strike": 6655.0,
    }
    # The spot-shift curve cannot be rebuilt for one expiration.
    assert body["net_gex_at_spot"] is None
    # Nearest three strikes to spot, drawn ascending, puts negative.
    assert [bar["strike"] for bar in body["profile"]] == [6650.0, 6660.0, 6670.0]
    assert body["profile"][1] == {
        "strike": 6660.0,
        "net_gex": 5e8,
        "call_gex": 6e8,
        "put_gex": -1e8,
    }
    # The website ladder's own read, so the two share a cache entry.
    mocks["get_strike_profile_timeseries"].assert_awaited_once_with("SPX", "5min", 3, [NEAR])
    mocks["get_latest_max_pain_for_expiration"].assert_awaited_once_with("SPX", NEAR)


def test_an_explicit_date_is_served_for_that_date(monkeypatch):
    client, mocks = _build_app(monkeypatch)
    with client:
        body = client.get(f"/api/v1/levels/SPX?expiration={FAR.isoformat()}").json()
    assert body["expiration"] == FAR.isoformat()
    mocks["get_strike_profile_timeseries"].assert_awaited_once_with("SPX", "5min", 3, [FAR])


def test_an_empty_tip_reaches_back_one_session(monkeypatch):
    """An ETF after its options close: the newest buckets carry no book."""
    calls = []

    def series(symbol, timeframe, units, exps):
        calls.append(units)
        if units == 3:
            return [_bucket(strikes=[]), _bucket(strikes=[_strike(6660.0, 0.0, 0.0)])]
        return [_bucket(call_wall=6710.0), _bucket(strikes=[])]

    client, _ = _build_app(monkeypatch, timeseries=series)
    with client:
        body = client.get("/api/v1/levels/SPX?expiration=0dte").json()
    assert calls == [3, 78]
    assert body["levels"]["call_wall"] == 6710.0


def test_an_unresolved_scoped_flip_says_so(monkeypatch):
    client, _ = _build_app(monkeypatch, timeseries=lambda *a, **k: [_bucket(gamma_flip=None)])
    with client:
        levels = client.get("/api/v1/levels/SPX?expiration=0dte").json()["levels"]
    assert levels["gamma_flip"] is None
    assert levels["gamma_flip_label"] == "Flip unresolved"


@pytest.mark.parametrize(
    "query, listed, timeseries, needle",
    [
        ("0dte", [], None, "No upcoming SPX expiration"),
        ("2020-01-17", None, None, "has expired"),
        (
            (NEAR + timedelta(days=1)).isoformat(),
            None,
            None,
            "no " + (NEAR + timedelta(days=1)).isoformat(),
        ),
        ("0dte", None, lambda *a, **k: [], "positioning"),
    ],
)
def test_a_scope_that_cannot_be_served_is_a_404_that_says_why(
    monkeypatch, query, listed, timeseries, needle
):
    client, _ = _build_app(monkeypatch, listed=listed, timeseries=timeseries)
    with client:
        r = client.get(f"/api/v1/levels/SPX?expiration={query}")
    assert r.status_code == 404
    assert needle in r.json()["detail"]


def test_junk_expiration_is_a_400_before_any_read(monkeypatch):
    client, mocks = _build_app(monkeypatch)
    with client:
        r = client.get("/api/v1/levels/SPX?expiration=tomorrow")
    assert r.status_code == 400
    mocks["get_latest_gex_summary"].assert_not_called()


def test_a_futures_path_keeps_its_expiration_through_the_rewrite():
    from urllib.parse import parse_qs

    from src.api import futures_middleware as fm

    scope = {"type": "http", "path": "/api/v1/levels/ES", "query_string": b"expiration=0dte"}
    fm._rewrite_scope(scope, "ES", "SPX")
    assert scope["path"] == "/api/v1/levels/SPX"
    assert parse_qs(scope["query_string"].decode()) == {"expiration": ["0dte"]}
