"""Regression: /api/flow/buying-pressure survives a bar whose volume is unknown.

``FlowBuyingPressurePoint.volume`` was required on the view that a bar's total
volume is always measured. It is not:

* a cash index has no share volume of its own. ThetaData's index bars are
  stored with ``volume``, ``up_volume`` and ``down_volume`` all NULL
  (``stream_index_bars``), and the futures middleware answers ES / NQ from
  SPX / NDX, so every SPX, NDX, ES and NQ request read only such bars;
* a stock minute whose running total stepped backwards is stored as unknown
  (``_VolumeDelta.delta`` returns None) rather than guessed.

The query's total, ``COALESCE(volume, up_volume + down_volume)``, is NULL for
those bars, and one of them in the window failed the whole response with a
500. Members hit that after the close on 2026-11-12.

The first tests run everywhere against a mocked database. The last runs the
real query and route against a scratch Postgres built from
``setup/database/schema.sql``, so it is ``integration``-marked and skips
unless ``NULL_READING_TEST_DSN`` is set::

    NULL_READING_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_flow_buying_pressure_unknown_volume.py -q
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

_DSN = os.getenv("NULL_READING_TEST_DSN")

_T = datetime(2026, 11, 12, 21, 0, tzinfo=timezone.utc)  # 16:00 ET

# What the query returns for ThetaData index bars: price known, volume and
# the tick split unknown, so the label is "No Tick Data".
_INDEX_ROWS = [
    {
        "timestamp": _T - timedelta(minutes=i),
        "symbol": "SPX",
        "price": Decimal("6800.25"),
        "volume": None,
        "buy_pct": None,
        "period_buy_pct": None,
        "price_chg": Decimal("0.50"),
        "momentum": "⚪ No Tick Data",
    }
    for i in range(3)
]


def _build_app(monkeypatch: pytest.MonkeyPatch, rows):
    """Reload src.api.main with get_flow_buying_pressure mocked on the class."""
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
    mock = AsyncMock(return_value=rows)
    dbmod.DatabaseManager.get_flow_buying_pressure = mock

    from src.api.main import app

    return app, mock


def test_index_bars_without_volume_serve_200_with_volume_null(monkeypatch):
    app, _ = _build_app(monkeypatch, _INDEX_ROWS)
    with TestClient(app) as client:
        response = client.get("/api/flow/buying-pressure?symbol=SPX")

    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body) == 3
    for row in body:
        assert row["volume"] is None  # unknown, never invented as 0
        assert row["buy_pct"] is None
        assert row["momentum"] == "⚪ No Tick Data"
        assert Decimal(str(row["price"])) == Decimal("6800.25")


def test_one_stock_minute_of_unknown_volume_does_not_sink_the_rest(monkeypatch):
    def bar(minute: int, volume):
        return {
            "timestamp": _T - timedelta(minutes=minute),
            "symbol": "NVDA",
            "price": Decimal("190.10"),
            "volume": volume,
            "buy_pct": None,
            "period_buy_pct": None,
            "price_chg": Decimal("0.02"),
            "momentum": "⚪ No Tick Data",
        }

    app, _ = _build_app(monkeypatch, [bar(0, 1200), bar(1, None), bar(2, 0)])
    with TestClient(app) as client:
        response = client.get("/api/flow/buying-pressure?symbol=NVDA")

    assert response.status_code == 200, response.text
    # A measured 0 stays 0, a measured total stays an integer, and only the
    # minute the feed could not establish reads null.
    assert [row["volume"] for row in response.json()] == [1200, None, 0]


def test_es_is_answered_from_spx_bars_and_keeps_volume_null(monkeypatch):
    app, mock = _build_app(monkeypatch, _INDEX_ROWS)

    from src.api import futures_middleware as fm
    from src.jobs.futures_projection import FuturesBasis

    basis = FuturesBasis(
        index_symbol="SPX",
        futures_symbol="ES",
        ratio=1.005,
        source="measured",
        observed_at=_T,
        sample_count=5,
        feed_symbol="@ES",
    )

    async def fake_resolve_basis(db, symbol, **kwargs):
        return basis if symbol.upper() in ("ES", "SPX") else None

    async def fake_live_spot(index_symbol):
        return 6834.25

    monkeypatch.setattr(fm, "resolve_basis", fake_resolve_basis)
    monkeypatch.setattr(fm, "_live_futures_spot", fake_live_spot)
    monkeypatch.setattr(fm, "_db_manager", lambda: object())

    with TestClient(app) as client:
        response = client.get("/api/flow/buying-pressure?symbol=ES")

    assert response.status_code == 200, response.text
    assert mock.await_args.args[0] == "SPX"  # the route read the index's bars
    rows = response.json()
    rows = rows["data"] if isinstance(rows, dict) and "data" in rows else rows
    assert len(rows) == 3
    for row in rows:
        assert row["symbol"] == "ES"
        assert row["volume"] is None
        assert float(row["price"]) == pytest.approx(6800.25 * 1.005, abs=0.25)


# ---------------------------------------------------------------------------
# The real query and route against Postgres
# ---------------------------------------------------------------------------

_SYMBOL = "ZZBPNULL"  # throwaway; cascade-deleted with its symbols row


async def _seed(conn, now: datetime) -> list:
    """Five recent bars stored the way ingestion stores them: the newest two
    as ThetaData index bars (no volume, no split), one stock minute whose
    total was unknown, and two with a measured total and no split."""
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'buying pressure null test', 'ETF', true)",
        _SYMBOL,
    )
    minute = now.replace(second=0, microsecond=0)
    volumes = [None, None, None, 800, 1500]  # newest first
    stamps = [minute - timedelta(minutes=i) for i in range(len(volumes))]
    for ts, volume in zip(stamps, volumes):
        await conn.execute(
            "INSERT INTO underlying_quotes"
            "(symbol, timestamp, open, high, low, close, up_volume, down_volume, volume) "
            "VALUES ($1, $2, 100, 100.5, 99.5, 100.25, NULL, NULL, $3)",
            _SYMBOL,
            ts,
            volume,
        )
    return list(zip(stamps, volumes))


@pytest.mark.integration
@pytest.mark.skipif(
    _DSN is None,
    reason="NULL_READING_TEST_DSN not set: needs a scratch Postgres with the project schema.",
)
def test_route_on_real_bars_with_no_volume():
    import asyncpg

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import main as mainmod
    from src.api.database import DatabaseManager

    async def _go():
        conn = await asyncpg.connect(_DSN)
        try:
            seeded = await _seed(conn, datetime.now(timezone.utc))
            db = DatabaseManager()

            @asynccontextmanager
            async def _acquire():
                yield conn

            db._acquire_connection = _acquire  # type: ignore[method-assign]
            mainmod.db_manager = db
            # The route itself, error wrapper and response model included:
            # before the fix this raised HTTPException(500).
            points = await mainmod.get_flow_buying_pressure(symbol=_SYMBOL, limit=20)
            return seeded, points
        finally:
            await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
            await conn.close()

    seeded, points = asyncio.run(_go())

    assert [p.timestamp for p in points] == [ts for ts, _ in seeded]
    assert [p.volume for p in points] == [v for _, v in seeded]
    assert all(p.buy_pct is None and p.momentum == "⚪ No Tick Data" for p in points)
    assert all(p.price == Decimal("100.25") for p in points)
