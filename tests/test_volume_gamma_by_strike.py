"""/api/gex/volume-gamma-by-strike: the volume view of the gamma ladders.

Pins three things:

* the arithmetic: each (strike, expiration) side's stored ``Σ γ × OI`` is
  turned back into per-contract gamma and re-weighted by today's volume, in
  the open-interest ladder's units and signs (calls +, puts −);
* the read: the newest snapshot, every row of it (no row limit), the same
  after-close expiry cutoff as /api/gex/by-strike, cached per cutoff;
* the HTTP contract the ladder's volume view depends on.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

import src.market_calendar as market_calendar
from src.api.database import DatabaseManager
from src.api.volume_gamma import volume_weighted_strike_gamma

_ET = ZoneInfo("America/New_York")

# Spot 100 makes the dollar scale a round number: 100 × 100² × 0.01.
SPOT = 100.0
SCALE = 10_000.0


def _row(
    strike, *, call_gamma=0.0, call_oi=0, call_volume=0, put_gamma=0.0, put_oi=0, put_volume=0
):
    return {
        "strike": strike,
        "call_gamma": call_gamma,
        "call_oi": call_oi,
        "call_volume": call_volume,
        "put_gamma": put_gamma,
        "put_oi": put_oi,
        "put_volume": put_volume,
    }


# ---------------------------------------------------------------------------
# The arithmetic
# ---------------------------------------------------------------------------


def test_gamma_is_reweighted_from_open_interest_to_volume():
    # call: Σγ×OI = 0.5 over 100 contracts -> γ = 0.005; 40 traded today.
    # put:  Σγ×OI = 0.3 over 60 contracts  -> γ = 0.005; 10 traded today.
    rows = [
        _row(
            100,
            call_gamma=0.5,
            call_oi=100,
            call_volume=40,
            put_gamma=0.3,
            put_oi=60,
            put_volume=10,
        )
    ]

    (strike,) = volume_weighted_strike_gamma(rows, SPOT)

    assert strike["strike"] == 100.0
    assert strike["call_gex"] == pytest.approx(0.005 * 40 * SCALE)  # 2,000
    assert strike["put_gex"] == pytest.approx(-0.005 * 10 * SCALE)  # -500
    assert strike["net_gex"] == pytest.approx(1_500.0)
    assert (strike["call_volume"], strike["put_volume"]) == (40, 10)


def test_volume_equal_to_open_interest_reproduces_the_open_interest_ladder():
    """Same units and signs as /api/gex/by-strike: if today's volume equals
    the open interest, the volume ladder IS the open-interest ladder."""
    rows = [
        _row(
            105,
            call_gamma=1.2,
            call_oi=300,
            call_volume=300,
            put_gamma=0.7,
            put_oi=250,
            put_volume=250,
        )
    ]

    (strike,) = volume_weighted_strike_gamma(rows, SPOT)

    assert strike["call_gex"] == pytest.approx(1.2 * SCALE)
    assert strike["put_gex"] == pytest.approx(-0.7 * SCALE)


def test_expirations_at_a_strike_are_summed_and_strikes_come_back_ascending():
    rows = [
        _row(110, call_gamma=0.2, call_oi=10, call_volume=5),
        _row(100, call_gamma=0.4, call_oi=20, call_volume=10),  # 0DTE at 100
        _row(100, call_gamma=0.1, call_oi=50, call_volume=100),  # a later expiry at 100
    ]

    out = volume_weighted_strike_gamma(rows, SPOT)

    assert [s["strike"] for s in out] == [100.0, 110.0]
    at_100 = out[0]
    assert at_100["call_gex"] == pytest.approx((0.02 * 10 + 0.002 * 100) * SCALE)
    assert at_100["call_volume"] == 110


def test_a_side_without_open_interest_adds_volume_but_no_gamma():
    # Traded today on a contract with no open interest: there is no stored
    # gamma to recover, so it moves the volume and nothing else.
    rows = [
        _row(100, call_gamma=0.0, call_oi=0, call_volume=25, put_gamma=0.4, put_oi=40, put_volume=8)
    ]

    (strike,) = volume_weighted_strike_gamma(rows, SPOT)

    assert strike["call_gex"] == 0.0
    assert strike["call_volume"] == 25
    assert strike["put_gex"] == pytest.approx(-0.01 * 8 * SCALE)


def test_database_types_and_nulls_are_read_as_numbers():
    rows = [
        {
            "strike": Decimal("100"),
            "call_gamma": Decimal("0.5"),
            "call_oi": 100,
            "call_volume": None,
            "put_gamma": None,
            "put_oi": None,
            "put_volume": 7,
        }
    ]

    (strike,) = volume_weighted_strike_gamma(rows, SPOT)

    assert strike["call_gex"] == 0.0  # no volume today
    assert strike["put_gex"] == 0.0  # no open interest
    assert (strike["call_volume"], strike["put_volume"]) == (0, 7)


@pytest.mark.parametrize("spot", [None, 0.0, -5.0])
def test_no_usable_spot_means_nothing_to_dollarize(spot):
    assert (
        volume_weighted_strike_gamma([_row(100, call_gamma=0.5, call_oi=1, call_volume=1)], spot)
        == []
    )


# ---------------------------------------------------------------------------
# The read
# ---------------------------------------------------------------------------

FRIDAY = date(2026, 10, 30)
SATURDAY = date(2026, 10, 31)


def _et(day: int, hhmm: str, month: int = 10) -> datetime:
    hour, minute = (int(x) for x in hhmm.split(":"))
    return datetime(2026, month, day, hour, minute, tzinfo=_ET).astimezone(timezone.utc)


def _read_at(monkeypatch, moment: datetime) -> None:
    monkeypatch.setattr(market_calendar, "_expiration_clock", lambda: moment)


class _Conn:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    async def fetch(self, query, *args, **_kwargs):
        self.calls.append((query, args))
        return self.rows


def _db(conn) -> DatabaseManager:
    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


def test_the_read_leaves_out_expired_expirations_and_caches_per_cutoff(monkeypatch):
    conn = _Conn()
    db = _db(conn)

    _read_at(monkeypatch, _et(30, "15:59"))
    asyncio.run(db.get_volume_gamma_by_strike("spx"))
    _read_at(monkeypatch, _et(30, "16:00"))
    asyncio.run(db.get_volume_gamma_by_strike("spx"))

    (sql, before), (_, after) = conn.calls
    assert "AND g.expiration >= $2::date" in sql
    # Every row of the snapshot: a strike's sum needs all its expirations.
    assert "LIMIT $" not in sql
    assert before == ("SPX", FRIDAY, None)
    # The cutoff is part of the cache key: the bell is not served from a body
    # cached a second before it.
    assert after == ("SPX", SATURDAY, None)


def test_an_expiration_selection_is_sorted_and_deduplicated(monkeypatch):
    conn = _Conn()
    db = _db(conn)
    _read_at(monkeypatch, _et(30, "11:00"))

    asyncio.run(db.get_volume_gamma_by_strike("SPX", [date(2026, 11, 2), FRIDAY, FRIDAY]))

    ((_, args),) = conn.calls
    assert args[2] == [FRIDAY, date(2026, 11, 2)]


def test_the_read_shapes_the_snapshot(monkeypatch):
    snapshot = datetime(2026, 10, 30, 15, 0, tzinfo=timezone.utc)
    rows = [
        {
            **_row(100, call_gamma=Decimal("0.5"), call_oi=100, call_volume=40),
            "timestamp": snapshot,
            "spot_price": Decimal("100"),
        },
        {
            **_row(95, put_gamma=Decimal("0.3"), put_oi=60, put_volume=10),
            "timestamp": snapshot,
            "spot_price": Decimal("100"),
        },
    ]
    db = _db(_Conn(rows))
    _read_at(monkeypatch, _et(30, "11:00"))

    out = asyncio.run(db.get_volume_gamma_by_strike("SPX"))

    assert out["symbol"] == "SPX"
    assert out["timestamp"] == snapshot
    assert out["spot_price"] == 100.0
    assert [s["strike"] for s in out["strikes"]] == [95.0, 100.0]
    assert out["strikes"][1]["call_gex"] == pytest.approx(2_000.0)


def test_no_snapshot_reads_as_empty(monkeypatch):
    db = _db(_Conn([]))
    _read_at(monkeypatch, _et(30, "11:00"))

    out = asyncio.run(db.get_volume_gamma_by_strike("SPY"))

    assert out == {"symbol": "SPY", "timestamp": None, "spot_price": None, "strikes": []}


# ---------------------------------------------------------------------------
# The HTTP contract
# ---------------------------------------------------------------------------


def _client(monkeypatch, returns) -> tuple[TestClient, AsyncMock]:
    """Reload src.api.main with get_volume_gamma_by_strike patched (the
    pattern test_strike_profile_timeseries_endpoint uses)."""
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    monkeypatch.setattr(dbmod.DatabaseManager, "connect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "disconnect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "check_health", AsyncMock(return_value=True))
    monkeypatch.setattr(dbmod.DatabaseManager, "get_latest_quote", AsyncMock(return_value=None))
    mock = AsyncMock(return_value=returns)
    monkeypatch.setattr(dbmod.DatabaseManager, "get_volume_gamma_by_strike", mock)

    from src.api.main import app

    return TestClient(app), mock


_PAYLOAD = {
    "symbol": "SPX",
    "timestamp": datetime(2026, 11, 3, 20, 59, tzinfo=timezone.utc),
    "spot_price": 7568.09,
    "strikes": [
        {
            "strike": 7565.0,
            "call_gex": 1.5e8,
            "put_gex": -2.5e8,
            "net_gex": -1.0e8,
            "call_volume": 1200,
            "put_volume": 3400,
        },
        {
            "strike": 7570.0,
            "call_gex": 3.0e8,
            "put_gex": -1.0e8,
            "net_gex": 2.0e8,
            "call_volume": 2600,
            "put_volume": 900,
        },
    ],
}


def test_endpoint_shape(monkeypatch):
    client, mock = _client(monkeypatch, _PAYLOAD)

    with client:
        resp = client.get("/api/gex/volume-gamma-by-strike?symbol=SPX")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "SPX"
    assert body["spot_price"] == 7568.09
    assert body["timestamp"].startswith("2026-11-03T20:59")
    assert body["strikes"][0] == {
        "strike": 7565.0,
        "call_gex": 1.5e8,
        "put_gex": -2.5e8,
        "net_gex": -1.0e8,
        "call_volume": 1200,
        "put_volume": 3400,
    }
    assert mock.await_args.args == ("SPX", None)


def test_endpoint_empty_snapshot_is_200(monkeypatch):
    client, _ = _client(
        monkeypatch, {"symbol": "SPY", "timestamp": None, "spot_price": None, "strikes": []}
    )

    with client:
        resp = client.get("/api/gex/volume-gamma-by-strike?symbol=SPY")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"symbol": "SPY", "timestamp": None, "spot_price": None, "strikes": []}


@pytest.mark.parametrize(
    "param,expected",
    [
        ("all", None),
        ("ALL", None),
        (",", None),  # an all-blank list collapses to All
        ("2026-11-04,2026-11-03", [date(2026, 11, 4), date(2026, 11, 3)]),
    ],
)
def test_endpoint_expirations_param(monkeypatch, param, expected):
    client, mock = _client(monkeypatch, _PAYLOAD)

    with client:
        resp = client.get(f"/api/gex/volume-gamma-by-strike?symbol=SPX&expirations={param}")

    assert resp.status_code == 200, resp.text
    assert mock.await_args.args == ("SPX", expected)


def test_endpoint_rejects_a_bad_expiration(monkeypatch):
    client, mock = _client(monkeypatch, _PAYLOAD)

    with client:
        resp = client.get("/api/gex/volume-gamma-by-strike?symbol=SPX&expirations=2026-13-01")

    assert resp.status_code == 400
    mock.assert_not_awaited()
