"""/api/gex/heatmap honors an expiration scope.

Until November 2026 the endpoint took no expiration parameter. The web
heatmap's Expiry menu sent ``&expiration=<date>``, FastAPI dropped it, and
every cell kept summing every expiration while the toolbar read "Expiry
<date> · DTE 0d". The endpoint now takes ``expirations`` with the semantics of
/api/gex/strike-profile-timeseries (``all``, or comma-joined ISO dates): each
cell sums only that scope, the scope is part of the cache key, and the flip
follows the scope the way the strike-profile read scopes it (recomputed from
the scope's strikes, never the whole-chain flip).

The first group runs everywhere against a mocked database. The
``integration`` group runs the real SQL, through the HTTP endpoint, against a
scratch Postgres built from ``setup/database/schema.sql``, and skips unless a
DSN is set::

    HEATMAP_SCOPE_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_gex_heatmap_expirations.py -m integration -q

(``REPLAY_SUM_TEST_DSN`` or ``STRIKE_PROFILE_SUM_TEST_DSN`` work too.) A
throwaway symbol is seeded and torn down, so it never touches market data.
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from src.analytics.walls import compute_gamma_flip_from_strikes

# ---------------------------------------------------------------------------
# The book. Two expirations share the strikes near the money; E1 also holds a
# position on each side of the heatmap's ±8% band, which is where the scoped
# flip and a band-only flip part ways (618.89 against 610.0).
# ---------------------------------------------------------------------------

_SPOT = 600.0
_SCALE = 100 * _SPOT * _SPOT * 0.01  # raw gamma -> dollars per 1% move
_E1 = date(2026, 8, 14)
_E2 = date(2026, 8, 21)
# Whole-chain flip as the engine stored it. A scoped bucket must never carry it.
_STORED_FLIP = 601.0

# strike -> (call_gamma, put_gamma)
_E1_BOOK = {
    540.0: (0.0, 400.0),  # below the band (600 ± 48)
    580.0: (50.0, 300.0),
    590.0: (100.0, 250.0),
    600.0: (300.0, 200.0),
    610.0: (400.0, 100.0),
    620.0: (500.0, 50.0),
    660.0: (200.0, 0.0),  # above the band
}
_E2_BOOK = {
    600.0: (250.0, 100.0),
    610.0: (100.0, 300.0),
}
# E2 also lists 630 with no position: a real zero cell inside the band.
_E2_EMPTY_STRIKE = 630.0
_IN_BAND = [580.0, 590.0, 600.0, 610.0, 620.0]


def _flip_inputs(*books: dict) -> list:
    agg: dict = {}
    for book in books:
        for strike, (c, p) in book.items():
            cc, pp = agg.get(strike, (0.0, 0.0))
            agg[strike] = (cc + c, pp + p)
    return [{"strike": k, "call_gamma": c, "put_gamma": p} for k, (c, p) in sorted(agg.items())]


def _net(book: dict, strike: float) -> float:
    c, p = book.get(strike, (0.0, 0.0))
    return (c - p) * _SCALE


def test_the_book_separates_the_flips_it_is_meant_to():
    """Guards the fixture: the scoped flip must differ from both a band-only
    flip and the stored whole-chain flip, or the flip tests below prove
    nothing."""
    scoped = compute_gamma_flip_from_strikes(_flip_inputs(_E1_BOOK), _SPOT)
    band_only = compute_gamma_flip_from_strikes(
        _flip_inputs({k: v for k, v in _E1_BOOK.items() if k in _IN_BAND}), _SPOT
    )
    assert scoped == pytest.approx(618.8889, abs=1e-3)
    assert band_only == pytest.approx(610.0)
    assert scoped != pytest.approx(_STORED_FLIP)


# ---------------------------------------------------------------------------
# Endpoint: the parameter reaches the read.
# ---------------------------------------------------------------------------


def _fresh_api_modules() -> None:
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)


def _client_with_mocked_read(monkeypatch: pytest.MonkeyPatch) -> tuple[TestClient, AsyncMock]:
    """Same pattern as tests/test_strike_profile_timeseries_endpoint.py: patch
    the class before importing main, so the module-level db_manager has it."""
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    _fresh_api_modules()

    from src.api import database as dbmod

    monkeypatch.setattr(dbmod.DatabaseManager, "connect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "disconnect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "check_health", AsyncMock(return_value=True))
    monkeypatch.setattr(dbmod.DatabaseManager, "get_latest_quote", AsyncMock(return_value=None))
    mock = AsyncMock(return_value=[])
    monkeypatch.setattr(dbmod.DatabaseManager, "get_gex_heatmap", mock)

    from src.api.main import app

    return TestClient(app), mock


def test_endpoint_passes_the_expirations_to_the_read(monkeypatch: pytest.MonkeyPatch):
    client, mock = _client_with_mocked_read(monkeypatch)
    with client:
        response = client.get(
            "/api/gex/heatmap?symbol=SPY&timeframe=5min&window_units=60"
            "&expirations=2026-08-21,2026-08-14"
        )
    assert response.status_code == 200, response.text
    mock.assert_awaited_once()
    args, _ = mock.call_args
    assert args[:3] == ("SPY", "5min", 60)
    assert args[3] == [date(2026, 8, 21), date(2026, 8, 14)]


@pytest.mark.parametrize("query", ["", "&expirations=all", "&expirations=ALL", "&expirations=,"])
def test_endpoint_all_reaches_the_read_as_none(monkeypatch: pytest.MonkeyPatch, query: str):
    client, mock = _client_with_mocked_read(monkeypatch)
    with client:
        response = client.get(f"/api/gex/heatmap?symbol=SPY{query}")
    assert response.status_code == 200, response.text
    args, _ = mock.call_args
    assert args[3] is None


@pytest.mark.parametrize("bad", ["tomorrow", "2026-08-14,tomorrow", "08/14/2026"])
def test_endpoint_rejects_an_unparseable_expiration(monkeypatch: pytest.MonkeyPatch, bad: str):
    """It used to answer 200 with the whole chain, whatever was asked."""
    client, mock = _client_with_mocked_read(monkeypatch)
    with client:
        response = client.get(f"/api/gex/heatmap?symbol=SPY&expirations={bad}")
    assert response.status_code == 400, response.text
    assert "expirations" in response.json()["detail"].lower()
    mock.assert_not_awaited()


# ---------------------------------------------------------------------------
# The read: SQL shape, parameters and cache key (mocked connection).
# ---------------------------------------------------------------------------


class _RecordingConn:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.calls: list = []

    async def fetch(self, query, *args, **_kwargs):
        self.calls.append((query, args))
        return list(self._rows)


def _db_with(conn):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return db


def _sql_only(sql: str) -> str:
    """Drop ``--`` comments so a shape assertion can't pass on prose."""
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _lateral(sql: str) -> str:
    return sql[sql.index("CROSS JOIN LATERAL") : sql.index(") cell")]


def test_scoped_read_sums_only_the_bound_expirations():
    conn = _RecordingConn()
    db = _db_with(conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60, [_E2, _E1, _E2]))

    query, args = conn.calls[0]
    # Normalized like the strike-profile read: deduped and ascending.
    assert args == ("SPY", 60, [_E1, _E2])
    lateral = _lateral(_sql_only(query))
    assert "g.expiration = ANY($3::date[])" in lateral
    # Still one SUM per strike over the probe at the bucket's representative.
    assert "SUM(g.net_gex) AS net_gex" in lateral
    assert "g.timestamp = br.rep_ts" in lateral
    assert "GROUP BY g.strike" in lateral


def test_scoped_read_binds_the_scope_after_the_holiday_list_for_a_cash_index():
    conn = _RecordingConn()
    db = _db_with(conn)
    asyncio.run(db.get_gex_heatmap("SPX", "5min", 60, [_E1]))

    query, args = conn.calls[0]
    assert args[0] == "SPX" and args[1] == 60
    assert isinstance(args[2], list)  # NYSE holidays
    assert args[3] == [_E1]
    sql = _sql_only(query)
    assert "<> ALL($3::date[])" in sql
    assert "g.expiration = ANY($4::date[])" in _lateral(sql)


def test_scoped_read_returns_every_position_for_the_flip_and_marks_the_band():
    """Cells stay inside the band; the flip reads every strike that holds a
    position. So the band moves from WHERE to HAVING (kept OR has a position),
    each row says whether it is a cell, and the bucket's close rides along."""
    conn = _RecordingConn()
    db = _db_with(conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60, [_E1]))
    sql = _sql_only(conn.calls[0][0])
    lateral = _lateral(sql)

    where = lateral[lateral.index("WHERE") : lateral.index("GROUP BY")]
    having = lateral[lateral.index("HAVING") :]
    assert "ABS(g.strike" not in where
    assert "ABS(g.strike - (SELECT spot_close FROM spot))" in having
    assert "SUM(COALESCE(g.call_oi, 0)) <> 0" in having
    assert "AS in_band" in lateral
    assert "AS has_position" in lateral
    assert "SUM(COALESCE(g.call_gamma, 0)) AS call_gamma" in lateral
    assert "SUM(COALESCE(g.put_gamma, 0)) AS put_gamma" in lateral
    assert "closes AS (" in sql
    assert "FROM underlying_quotes" in sql[sql.index("closes AS (") : sql.index("strikes AS (")]
    assert "LEFT JOIN closes c ON c.bucket_ts = s.bucket_ts" in sql


@pytest.mark.parametrize("scope", [None, []])
def test_all_read_is_unchanged(scope):
    """All keeps the query it always ran: no scope parameter, the band in the
    lateral's WHERE, no HAVING, no closes."""
    conn = _RecordingConn()
    db = _db_with(conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60, scope))

    query, args = conn.calls[0]
    assert args == ("SPY", 60)
    sql = _sql_only(query)
    lateral = _lateral(sql)
    assert "g.expiration" not in lateral
    assert "HAVING" not in lateral
    assert "ABS(g.strike - (SELECT spot_close FROM spot))" in lateral
    assert "closes AS" not in sql


def test_scope_is_part_of_the_cache_key():
    """A scoped request must not be answered from the All entry, or the other
    way round."""
    conn = _RecordingConn()
    db = _db_with(conn)
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60))
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60, [_E1]))
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60, [_E2]))
    assert len(conn.calls) == 3

    # The same scope in another order or with a repeat is the same entry.
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60, [_E1, _E1]))
    asyncio.run(db.get_gex_heatmap("SPY", "5min", 60))
    assert len(conn.calls) == 3


_TS = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)


def _scoped_rows(close=_SPOT) -> list:
    """The rows the scoped query returns for one bucket of the E1 book, plus
    E2's empty listed strike, as a [E1, E2] read would see it. The stored
    whole-chain flip rides the lowest strike's row, as the SQL projects it."""
    rows = []
    strikes = sorted(set(_E1_BOOK) | {_E2_EMPTY_STRIKE})
    for strike in strikes:
        c, p = _E1_BOOK.get(strike, (0.0, 0.0))
        rows.append(
            {
                "timestamp": _TS,
                "strike": strike,
                "net_gex": (c - p) * _SCALE,
                "call_gamma": c,
                "put_gamma": p,
                "in_band": abs(strike - _SPOT) <= _SPOT * 0.08,
                "has_position": bool(c or p),
                "close": close,
                "gamma_flip": _STORED_FLIP if strike == strikes[0] else None,
                "gamma_flip_span_used": 0.2 if strike == strikes[0] else None,
            }
        )
    return rows


def test_scoped_flip_is_recomputed_from_every_position_and_cells_stay_in_band():
    db = _db_with(_RecordingConn(_scoped_rows()))
    [bucket] = asyncio.run(db.get_gex_heatmap("SPY", "1min", 5, [_E1, _E2]))

    # Cells: the band only, the empty listed strike included as a zero.
    assert [c["strike"] for c in bucket["heatmap"]] == _IN_BAND + [_E2_EMPTY_STRIKE]
    assert bucket["heatmap"][-1]["net_gex"] == 0.0
    # Flip: every strike with a position, in or out of the band, against the
    # bucket's close; never the stored whole-chain flip or its span.
    want = compute_gamma_flip_from_strikes(_flip_inputs(_E1_BOOK), _SPOT)
    assert bucket["gamma_flip"] == pytest.approx(want)
    assert bucket["gamma_flip"] != pytest.approx(_STORED_FLIP)
    assert bucket["gamma_flip_span_used"] is None


def test_scoped_flip_is_none_without_a_close():
    """No bar for the bucket: nothing to gate a flip against. None, not the
    stored whole-chain flip."""
    db = _db_with(_RecordingConn(_scoped_rows(close=None)))
    [bucket] = asyncio.run(db.get_gex_heatmap("SPY", "1min", 5, [_E1]))
    assert bucket["gamma_flip"] is None
    assert bucket["gamma_flip_span_used"] is None
    assert len(bucket["heatmap"]) == len(_IN_BAND) + 1


def test_all_read_still_carries_the_stored_flip():
    rows = [
        {k: v for k, v in r.items() if k not in ("call_gamma", "put_gamma", "in_band")}
        for r in _scoped_rows()
        if r["in_band"]
    ]
    rows[0]["gamma_flip"], rows[0]["gamma_flip_span_used"] = _STORED_FLIP, 0.2
    db = _db_with(_RecordingConn(rows))
    [bucket] = asyncio.run(db.get_gex_heatmap("SPY", "1min", 5))
    assert bucket["gamma_flip"] == _STORED_FLIP
    assert bucket["gamma_flip_span_used"] == 0.2


# ---------------------------------------------------------------------------
# Integration: the real SQL, through the HTTP endpoint.
# ---------------------------------------------------------------------------

_DSN = (
    os.getenv("HEATMAP_SCOPE_TEST_DSN")
    or os.getenv("REPLAY_SUM_TEST_DSN")
    or os.getenv("STRIKE_PROFILE_SUM_TEST_DSN")
)
_SYMBOL = "ZZHMXP"  # throwaway; cascade-deleted with its symbols row
_T0 = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
_T1 = datetime(2026, 8, 12, 14, 1, tzinfo=timezone.utc)  # 10:01 ET

skip_without_db = pytest.mark.skipif(
    _DSN is None,
    reason="HEATMAP_SCOPE_TEST_DSN not set: needs a scratch Postgres with the project schema.",
)


async def _seed(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'heatmap scope test', 'ETF', true)",
        _SYMBOL,
    )
    rows = [(_E1, k, c, p, 1000) for k, (c, p) in _E1_BOOK.items()]
    rows += [(_E2, k, c, p, 1000) for k, (c, p) in _E2_BOOK.items()]
    rows.append((_E2, _E2_EMPTY_STRIKE, 0.0, 0.0, 0))
    for ts in (_T0, _T1):
        await conn.execute(
            "INSERT INTO gex_summary(underlying, timestamp, gamma_flip_point, "
            "gamma_flip_span_used) VALUES ($1, $2, $3, 0.2)",
            _SYMBOL,
            ts,
            _STORED_FLIP,
        )
        await conn.execute(
            "INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            _SYMBOL,
            ts,
            _SPOT,
            _SPOT + 1,
            _SPOT - 1,
            _SPOT,
        )
        for exp, strike, c, p, oi in rows:
            await conn.execute(
                "INSERT INTO gex_by_strike(underlying, timestamp, strike, expiration, "
                "call_gamma, put_gamma, net_gex, call_oi, put_oi) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                _SYMBOL,
                ts,
                strike,
                exp,
                c,
                p,
                (c - p) * _SCALE,
                oi,
                oi,
            )


async def _teardown(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


def _with_seed(body):
    """Seed the scratch database, run ``body()``, always tear down."""
    import asyncpg

    async def _once(fn):
        conn = await asyncpg.connect(_DSN)
        try:
            await fn(conn)
        finally:
            await conn.close()

    asyncio.run(_once(_seed))
    try:
        return body()
    finally:
        asyncio.run(_once(_teardown))


def _client_on_scratch_db(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """The real app, its DatabaseManager reading the scratch database. Each
    request opens its own connection, on the app's event loop."""
    import asyncpg

    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    _fresh_api_modules()

    from src.api import database as dbmod

    @asynccontextmanager
    async def _acquire(self):
        conn = await asyncpg.connect(_DSN)
        try:
            yield conn
        finally:
            await conn.close()

    monkeypatch.setattr(dbmod.DatabaseManager, "connect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "disconnect", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "check_health", AsyncMock(return_value=True))
    monkeypatch.setattr(dbmod.DatabaseManager, "get_latest_quote", AsyncMock(return_value=None))
    monkeypatch.setattr(dbmod.DatabaseManager, "_acquire_connection", _acquire)

    from src.api.main import app

    return TestClient(app)


def _cells(body: list, ts: datetime) -> dict:
    [bucket] = [b for b in body if datetime.fromisoformat(b["timestamp"]) == ts]
    return {float(c["strike"]): float(c["net_gex"]) for c in bucket["heatmap"]}


def _get(client: TestClient, scope: str) -> list:
    response = client.get(
        f"/api/gex/heatmap?symbol={_SYMBOL}&timeframe=1min&window_units=5&expirations={scope}"
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.integration
@skip_without_db
def test_api_filtered_request_sums_only_the_chosen_expirations(monkeypatch: pytest.MonkeyPatch):
    """The bug, end to end: two expirations share strikes near the money, and
    a filtered request must return different sums than All."""

    def body():
        with _client_on_scratch_db(monkeypatch) as client:
            return {
                scope: _get(client, scope)
                for scope in ("all", _E1.isoformat(), _E2.isoformat(), f"{_E2},{_E1}")
            }

    out = _with_seed(body)
    for ts in (_T0, _T1):
        everything = _cells(out["all"], ts)
        only_e1 = _cells(out[_E1.isoformat()], ts)
        only_e2 = _cells(out[_E2.isoformat()], ts)
        both = _cells(out[f"{_E2},{_E1}"], ts)

        # All sums both expirations at the shared strikes...
        assert everything[600.0] == pytest.approx(_net(_E1_BOOK, 600.0) + _net(_E2_BOOK, 600.0))
        assert everything[610.0] == pytest.approx(_net(_E1_BOOK, 610.0) + _net(_E2_BOOK, 610.0))
        # ...and a filtered request sums only its own, so the cells differ.
        assert only_e1[600.0] == pytest.approx(_net(_E1_BOOK, 600.0))
        assert only_e2[600.0] == pytest.approx(_net(_E2_BOOK, 600.0))
        assert only_e1[600.0] != pytest.approx(everything[600.0])
        assert only_e2[610.0] != pytest.approx(everything[610.0])
        # Both dates together are All again for this book.
        assert both == pytest.approx(everything)

        # Cells are the band's strikes the scope lists, zero cells included,
        # and never the out-of-band strikes that only feed the flip.
        assert sorted(only_e1) == _IN_BAND
        assert sorted(only_e2) == [600.0, 610.0, _E2_EMPTY_STRIKE]
        assert only_e2[_E2_EMPTY_STRIKE] == 0.0
        assert sorted(everything) == _IN_BAND + [_E2_EMPTY_STRIKE]


@pytest.mark.integration
@skip_without_db
def test_api_scoped_flip_matches_the_strike_profile_flip(monkeypatch: pytest.MonkeyPatch):
    """The scoped flip is the one /api/gex/strike-profile-timeseries draws for
    the same scope and minute: built from every strike holding a position
    (618.89 here, where a band-only flip would say 610), never the stored
    whole-chain flip. All keeps the stored flip."""

    def body():
        with _client_on_scratch_db(monkeypatch) as client:
            heat_e1 = _get(client, _E1.isoformat())
            heat_all = _get(client, "all")
            profile = client.get(
                f"/api/gex/strike-profile-timeseries?symbol={_SYMBOL}&timeframe=1min"
                f"&window_units=5&expirations={_E1.isoformat()}"
            )
            assert profile.status_code == 200, profile.text
            return heat_e1, heat_all, profile.json()

    heat_e1, heat_all, profile = _with_seed(body)
    want = compute_gamma_flip_from_strikes(_flip_inputs(_E1_BOOK), _SPOT)
    profile_flip = {datetime.fromisoformat(b["timestamp"]): b["gamma_flip"] for b in profile}
    for ts in (_T0, _T1):
        [scoped] = [b for b in heat_e1 if datetime.fromisoformat(b["timestamp"]) == ts]
        [whole] = [b for b in heat_all if datetime.fromisoformat(b["timestamp"]) == ts]
        assert scoped["gamma_flip"] == pytest.approx(want)
        assert scoped["gamma_flip"] == pytest.approx(profile_flip[ts])
        assert scoped["gamma_flip_span_used"] is None
        assert whole["gamma_flip"] == pytest.approx(_STORED_FLIP)
        assert whole["gamma_flip_span_used"] == pytest.approx(0.2)
