"""Tests for the versioned consolidated levels contract.

``GET /api/v1/levels/{symbol}`` is the stable, derived-only, ``gex``-scoped
surface that third-party charting integrations (TradingView Charting Library
widget, NinjaScript indicator) build on.  These tests pin the two things an
external integrator relies on:

  * the DB primitive ``get_latest_strike_gamma_profile`` aggregates the
    per-strike gamma profile ACROSS expirations (one row per strike, the
    same cross-expiration basis the walls use) and returns the strikes
    nearest to spot — not per-(strike, expiration) rows;
  * the endpoint returns one consolidated payload (flip + walls + max pain +
    profile), renders the profile ascending by strike, exposes freshness
    (``as_of`` / ``age_seconds``), and validates its inputs.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.api.database import DatabaseManager


class _RecordingConn:
    def __init__(self, fetch_rows=None):
        self._fetch_rows = fetch_rows or []
        self.queries = []
        self.args = []

    async def fetch(self, query, *args, timeout=None):
        self.queries.append(query)
        self.args.append(args)
        return list(self._fetch_rows)


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# DB-layer query shape
# ---------------------------------------------------------------------------


def test_profile_query_aggregates_across_expirations_and_ranks_by_distance():
    """The profile must sum gamma per strike ACROSS expirations (gex_by_strike
    is keyed (strike, expiration)); ranking per-row would double-count a
    strike and disagree with the cross-expiration walls.  It must also select
    the strikes nearest to spot, using the canonical dollar-GEX formula."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_latest_strike_gamma_profile("SPY", 40))

    sql = conn.queries[0]
    assert "FROM gex_by_strike" in sql
    assert "GROUP BY g.strike" in sql  # one row per strike
    assert "SUM(COALESCE(g.call_gamma" in sql
    assert "SUM(COALESCE(g.put_gamma" in sql
    # canonical dollar GEX: γ × OI × 100 × S² × 0.01
    assert "* 100 *" in sql and "* 0.01" in sql
    # nearest-to-spot ranking
    assert "ORDER BY ABS(ps.strike - s.close) ASC" in sql
    # strikes limit is parameterised, not hard-coded
    assert conn.args[0] == ("SPY", 40)


def test_profile_query_caches_at_analytics_ttl():
    """The profile changes only on the ~60s analytics cycle, so repeated
    polling must hit the cache rather than re-run the aggregate scan."""
    db = DatabaseManager()
    conn = _RecordingConn(
        fetch_rows=[{"strike": 670.0, "call_gex": 1.0, "put_gex": -2.0, "net_gex": -1.0}]
    )
    _install_conn(db, conn)
    asyncio.run(db.get_latest_strike_gamma_profile("SPY", 40))
    asyncio.run(db.get_latest_strike_gamma_profile("SPY", 40))
    assert len(conn.queries) == 1  # second call served from cache


def test_profile_symbol_is_upcased():
    """Symbol is normalised so /api/v1/levels/spy and /SPY hit one cache key
    and one row set."""
    db = DatabaseManager()
    conn = _RecordingConn(fetch_rows=[])
    _install_conn(db, conn)
    asyncio.run(db.get_latest_strike_gamma_profile("spy", 40))
    assert conn.args[0][0] == "SPY"


# ---------------------------------------------------------------------------
# Endpoint layer
# ---------------------------------------------------------------------------


def _summary(**overrides):
    base = {
        "timestamp": datetime(2026, 7, 6, 19, 30, tzinfo=timezone.utc),
        "symbol": "SPY",
        "spot_price": 676.04,
        "net_gex_at_spot": -1.2e9,
        "gamma_flip": 675.0,
        "call_wall": 680.0,
        "put_wall": 670.0,
        "max_pain": 676.0,
        # Pin Strike columns the summary query now returns (additive).
        "pin_strike": 676.0,
        "pin_score": 1.504e9,
        "pin_confidence": 0.31,
        "pin_strike_reason": None,
    }
    base.update(overrides)
    return base


def _profile_rows():
    # Deliberately NOT strike-sorted — the endpoint must sort ascending.
    return [
        {"strike": 680.0, "call_gex": 5e8, "put_gex": -1e8, "net_gex": 4e8},
        {"strike": 670.0, "call_gex": 1e8, "put_gex": -6e8, "net_gex": -5e8},
        {"strike": 676.0, "call_gex": 2e8, "put_gex": -2e8, "net_gex": 0.0},
    ]


def _build_app(monkeypatch, *, summary, profile):
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(return_value=summary)
    dbmod.DatabaseManager.get_latest_strike_gamma_profile = AsyncMock(return_value=profile)

    from src.api.main import app

    return TestClient(app)


def test_endpoint_returns_consolidated_contract(monkeypatch: pytest.MonkeyPatch):
    client = _build_app(monkeypatch, summary=_summary(), profile=_profile_rows())
    with client:
        r = client.get("/api/v1/levels/SPY?strikes=5")
    assert r.status_code == 200
    body = r.json()
    assert body["symbol"] == "SPY"
    assert body["spot"] == 676.04
    assert body["net_gex_at_spot"] == -1.2e9
    assert body["levels"] == {
        "gamma_flip": 675.0,
        "call_wall": 680.0,
        "put_wall": 670.0,
        "max_pain": 676.0,
        # Pin Strike is a fifth drawable level (additive; existing four unchanged).
        "pin_strike": 676.0,
    }
    # Pin Strike scalar metadata rides at the response top level.
    assert body["pin_score"] == 1.504e9
    assert body["pin_confidence"] == 0.31
    assert body["pin_strike_reason"] is None
    # freshness surfaced for the delayed-vs-live gate
    assert body["as_of"].startswith("2026-07-06T19:30:00")
    assert isinstance(body["age_seconds"], int) and body["age_seconds"] >= 0


def test_endpoint_surfaces_null_pin_with_reason(monkeypatch: pytest.MonkeyPatch):
    """No active pin ⇒ levels.pin_strike is null (hide, don't zero) and the
    REASON_* code rides in pin_strike_reason — the nullable-analytics contract."""
    summary = _summary(
        pin_strike=None,
        pin_score=None,
        pin_confidence=None,
        pin_strike_reason="NO_POSITIVE_RESTORING_GAMMA",
    )
    client = _build_app(monkeypatch, summary=summary, profile=_profile_rows())
    with client:
        r = client.get("/api/v1/levels/SPY")
    body = r.json()
    assert body["levels"]["pin_strike"] is None
    assert body["pin_score"] is None
    assert body["pin_confidence"] is None
    assert body["pin_strike_reason"] == "NO_POSITIVE_RESTORING_GAMMA"
    # The other four levels are unaffected.
    assert body["levels"]["gamma_flip"] == 675.0


def test_endpoint_profile_sorted_ascending_by_strike(monkeypatch: pytest.MonkeyPatch):
    """A histogram reads left-to-right in price order, so the profile must be
    strike-ascending regardless of the DB's nearest-to-spot ordering."""
    client = _build_app(monkeypatch, summary=_summary(), profile=_profile_rows())
    with client:
        r = client.get("/api/v1/levels/SPY")
    strikes = [bar["strike"] for bar in r.json()["profile"]]
    assert strikes == [670.0, 676.0, 680.0]


def test_endpoint_symbol_is_upcased_in_response(monkeypatch: pytest.MonkeyPatch):
    client = _build_app(monkeypatch, summary=_summary(), profile=[])
    with client:
        r = client.get("/api/v1/levels/spy")
    assert r.status_code == 200
    assert r.json()["symbol"] == "SPY"


def test_endpoint_null_level_passes_through(monkeypatch: pytest.MonkeyPatch):
    """An unresolved gamma flip is null, not zero — consumers hide the line."""
    client = _build_app(monkeypatch, summary=_summary(gamma_flip=None), profile=[])
    with client:
        r = client.get("/api/v1/levels/SPY")
    assert r.status_code == 200
    assert r.json()["levels"]["gamma_flip"] is None


def test_endpoint_404_when_no_snapshot(monkeypatch: pytest.MonkeyPatch):
    client = _build_app(monkeypatch, summary=None, profile=[])
    with client:
        r = client.get("/api/v1/levels/SPY")
    assert r.status_code == 404


def test_endpoint_rejects_out_of_range_strikes(monkeypatch: pytest.MonkeyPatch):
    """1 ≤ strikes ≤ 200 bounds the profile scan; FastAPI 422s outside it."""
    client = _build_app(monkeypatch, summary=_summary(), profile=[])
    with client:
        assert client.get("/api/v1/levels/SPY?strikes=999").status_code == 422
        assert client.get("/api/v1/levels/SPY?strikes=0").status_code == 422


def test_endpoint_rejects_bad_symbol(monkeypatch: pytest.MonkeyPatch):
    """Symbol is charset/length bounded so a junk path can't reach the DB."""
    client = _build_app(monkeypatch, summary=_summary(), profile=[])
    with client:
        assert client.get("/api/v1/levels/bad symbol").status_code == 422
