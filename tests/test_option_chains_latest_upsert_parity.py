"""The cache row is the newest history row, and the snapshot read from it is
the history snapshot.

``option_chains_latest`` replaces the analytics snapshot's DISTINCT ON over
``option_chains`` when ``ANALYTICS_USE_LATEST_CACHE`` is on, so it is only
safe while every cache row equals that contract's newest history row.  Its
cumulative columns (volume, open_interest, ask/mid/bid_volume) used a bare
``GREATEST`` across sessions, which pinned open interest to the highest value
ever seen and held yesterday's session volume until today's passed it.  These
run the real ingestion writer against a real Postgres and check the two
tables, then the two snapshot paths and their GEX, through
``src.tools.latest_cache_parity`` -- the same comparison an operator runs in
production before turning the flag on.

They are ``integration``-marked::

    LATEST_CACHE_PARITY_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_option_chains_latest_upsert_parity.py -q -m integration

Each test builds its own throwaway schema (the three tables the write and read
paths touch, with the project's column types and keys) and drops it after, so
the database needs no project schema.
"""

from __future__ import annotations

import math
import os
import random
import threading
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest

from src.analytics import main_engine as analytics_module
from src.analytics.main_engine import AnalyticsEngine
from src.ingestion import main_engine as ingestion_module
from src.ingestion.main_engine import IngestionEngine
from src.market_calendar import ET
from src.tools.latest_cache_parity import compare_snapshot
from tests.test_ingestion_option_chains_latest_dual_write import _make_engine_for_write_test

pytestmark = pytest.mark.integration

_DSN = os.getenv("LATEST_CACHE_PARITY_TEST_DSN")
_SKIP = "LATEST_CACHE_PARITY_TEST_DSN not set -- latest-cache parity tests skipped."

_OPTION_COLUMNS = """
    timestamp TIMESTAMPTZ NOT NULL,
    underlying VARCHAR(10) NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    expiration DATE NOT NULL,
    option_type CHAR(1) NOT NULL,
    last NUMERIC(12, 4),
    bid NUMERIC(12, 4),
    ask NUMERIC(12, 4),
    mid NUMERIC(12, 4),
    volume BIGINT DEFAULT 0,
    open_interest BIGINT DEFAULT 0,
    ask_volume BIGINT DEFAULT 0,
    mid_volume BIGINT DEFAULT 0,
    bid_volume BIGINT DEFAULT 0,
    implied_volatility NUMERIC(8, 6),
    delta NUMERIC(8, 6),
    gamma NUMERIC(10, 8),
    theta NUMERIC(10, 6),
    vega NUMERIC(10, 6),
    charm NUMERIC(16, 8),
    vanna NUMERIC(16, 8),
    updated_at TIMESTAMPTZ DEFAULT NOW()
"""

_DDL = f"""
CREATE TABLE option_chains (
    option_symbol VARCHAR(50) NOT NULL,
    {_OPTION_COLUMNS},
    PRIMARY KEY (option_symbol, timestamp)
);
CREATE TABLE option_chains_latest (
    option_symbol VARCHAR(50) PRIMARY KEY,
    {_OPTION_COLUMNS}
);
CREATE TABLE underlying_quotes (
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);
"""

_CUMULATIVE = ("volume", "open_interest", "ask_volume", "mid_volume", "bid_volume")

# A Thursday and the Friday after it; both NYSE sessions.
_PRIOR_SESSION = date(2026, 11, 12)
_SESSION = date(2026, 11, 13)


def _et(day: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return ET.localize(datetime(day.year, day.month, day.day, hh, mm, ss))


@pytest.fixture
def scratch_db():
    """A throwaway schema; yields a connect() that opens connections into it."""
    if not _DSN:
        pytest.skip(_SKIP)
    import psycopg2

    schema = f"zgx_latest_parity_{uuid.uuid4().hex[:10]}"
    admin = psycopg2.connect(_DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
        cur.execute(_DDL)

    def connect():
        conn = psycopg2.connect(_DSN, options=f"-c search_path={schema} -c timezone=UTC")
        return conn

    try:
        yield connect
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
        admin.close()


@contextmanager
def _patched_writer_connection(connect):
    """Route ingestion's db_connection() into the scratch schema, with the
    real context manager's commit-on-success / rollback-on-error contract."""

    @contextmanager
    def _db_connection():
        conn = connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    with patch.object(ingestion_module, "db_connection", _db_connection):
        yield


def _row(symbol: str, ts: datetime, **cumulative) -> dict:
    row = {
        "option_symbol": symbol,
        "timestamp": ts,
        "underlying": "SPY",
        "strike": 600.0,
        "expiration": date(2026, 11, 20),
        "option_type": "C",
        "last": 1.0,
        "bid": 0.95,
        "ask": 1.05,
        "mid": 1.0,
        "implied_volatility": 0.2,
        "delta": 0.5,
        "gamma": 0.02,
        "theta": -0.05,
        "vega": 0.1,
        "charm": 0.0,
        "vanna": 0.0,
    }
    row.update({col: 0 for col in _CUMULATIVE})
    row.update(cumulative)
    return row


def _cache_vs_newest_history(conn) -> list:
    """Contracts whose cache row differs from their newest history row."""
    cols = ", ".join(("timestamp",) + _CUMULATIVE)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT option_symbol, {cols} FROM option_chains_latest ORDER BY option_symbol
            """)
        cache = {r[0]: r[1:] for r in cur.fetchall()}
        cur.execute(f"""
            SELECT DISTINCT ON (option_symbol) option_symbol, {cols}
            FROM option_chains ORDER BY option_symbol, timestamp DESC
            """)
        newest = {r[0]: r[1:] for r in cur.fetchall()}
    return [
        (s, cache.get(s), newest.get(s))
        for s in cache.keys() | newest.keys()
        if cache.get(s) != newest.get(s)
    ]


def _cache_row(conn, symbol: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT timestamp, " + ", ".join(_CUMULATIVE) + " FROM option_chains_latest "
            "WHERE option_symbol = %s",
            (symbol,),
        )
        row = cur.fetchone()
    return dict(zip(("timestamp",) + _CUMULATIVE, row))


# ---------------------------------------------------------------------------
# The upsert, row by row.
# ---------------------------------------------------------------------------


def test_a_new_session_replaces_open_interest_and_volume(scratch_db):
    """Yesterday's higher OI and yesterday's session volume must not survive
    today's first rows -- the cache must read what history's newest row reads."""
    engine = _make_engine_for_write_test()
    sym = "SPY261120C00600000"
    with _patched_writer_connection(scratch_db):
        engine._write_option_rows(
            [
                _row(
                    sym,
                    _et(_PRIOR_SESSION, 15, 58),
                    volume=9000,
                    open_interest=5000,
                    ask_volume=5000,
                    mid_volume=1000,
                    bid_volume=3000,
                ),
                _row(
                    sym,
                    _et(_PRIOR_SESSION, 15, 59),
                    volume=9100,
                    open_interest=5000,
                    ask_volume=5050,
                    mid_volume=1000,
                    bid_volume=3050,
                ),
            ]
        )
        engine._write_option_rows(
            [
                _row(
                    sym,
                    _et(_SESSION, 9, 31),
                    volume=150,
                    open_interest=4200,
                    ask_volume=90,
                    mid_volume=20,
                    bid_volume=40,
                )
            ]
        )

    conn = scratch_db()
    try:
        assert _cache_row(conn, sym) == {
            "timestamp": _et(_SESSION, 9, 31),
            "volume": 150,
            "open_interest": 4200,
            "ask_volume": 90,
            "mid_volume": 20,
            "bid_volume": 40,
        }
        assert _cache_vs_newest_history(conn) == []
    finally:
        conn.close()


def test_a_retry_cannot_decrement_and_an_older_minute_cannot_overwrite(scratch_db):
    """What the old GREATEST was for, kept: a re-sent row for the minute the
    cache already holds keeps the higher cumulative (history's own GREATEST),
    and a replay of an older minute changes nothing (the WHERE guard)."""
    engine = _make_engine_for_write_test()
    sym = "SPY261120P00590000"
    t1, t2 = _et(_SESSION, 10, 0), _et(_SESSION, 10, 1)
    with _patched_writer_connection(scratch_db):
        engine._write_option_rows([_row(sym, t2, volume=600, open_interest=800, ask_volume=300)])
        # Same minute, older cumulative: a retained row from a failed batch.
        engine._write_option_rows([_row(sym, t2, volume=580, open_interest=800, ask_volume=290)])
        # Older minute arriving late, with a higher number that is not newer.
        engine._write_option_rows([_row(sym, t1, volume=999, open_interest=999, ask_volume=999)])

    conn = scratch_db()
    try:
        row = _cache_row(conn, sym)
        assert row["timestamp"] == t2
        assert (row["volume"], row["open_interest"], row["ask_volume"]) == (600, 800, 300)
        assert _cache_vs_newest_history(conn) == []
    finally:
        conn.close()


def test_randomized_replay_keeps_every_cache_row_equal_to_the_newest_history_row(scratch_db):
    """Two sessions of writes for a handful of contracts -- out-of-order and
    duplicated minutes, re-sent rows, multi-minute batches, OI moving both
    ways at the session boundary, volume resetting and re-anchoring -- checked
    after every batch."""
    rng = random.Random(20261116)
    engine = _make_engine_for_write_test()
    symbols = [f"SPY261120{'CP'[i % 2]}00{600 + i}000" for i in range(6)]
    minutes = [_et(_PRIOR_SESSION, 15, 50) + timedelta(minutes=m) for m in range(10)]
    minutes += [_et(_SESSION, 9, 30) + timedelta(minutes=m) for m in range(15)]

    state = {s: {"vol": 0, "oi": rng.randint(100, 5000), "a": 0, "m": 0, "b": 0} for s in symbols}
    rows = []
    for ts in minutes:
        new_session = ts == _et(_SESSION, 9, 30)
        for s in symbols:
            st = state[s]
            if new_session:
                st.update(vol=0, a=0, m=0, b=0, oi=max(0, int(st["oi"] * rng.uniform(0.5, 1.3))))
            if rng.random() < 0.08:
                st["vol"] = max(0, st["vol"] - rng.randint(1, 50))  # a re-anchor
            add = rng.randint(0, 40)
            st["vol"] += add
            st["a"] += add // 2
            st["b"] += add - add // 2
            for _ in range(rng.randint(1, 3)):  # several writes of one minute
                rows.append(
                    _row(
                        s,
                        ts,
                        volume=st["vol"],
                        open_interest=st["oi"],
                        ask_volume=st["a"],
                        mid_volume=st["m"],
                        bid_volume=st["b"],
                    )
                )
    # Out of order a little, then cut into uneven batches.
    for i in range(0, len(rows) - 1, 7):
        if rng.random() < 0.5:
            rows[i], rows[i + 1] = rows[i + 1], rows[i]
    conn = scratch_db()
    try:
        with _patched_writer_connection(scratch_db):
            i = 0
            while i < len(rows):
                n = rng.randint(1, 25)
                batch = rows[i : i + n]
                if rng.random() < 0.2:
                    batch = batch + rng.sample(rows[: i + 1], k=min(3, i + 1))  # re-sent rows
                engine._write_option_rows(batch)
                assert _cache_vs_newest_history(conn) == [], f"diverged after row {i}"
                i += n
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# End to end: the ingestion pipeline writes, both snapshot paths read.
# ---------------------------------------------------------------------------


def _bs_gamma(spot: float, strike: float, years: float, iv: float) -> float:
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * years) / (iv * math.sqrt(years))
    return math.exp(-0.5 * d1 * d1) / (spot * iv * math.sqrt(2 * math.pi * years))


def _pipeline_engine() -> IngestionEngine:
    """IngestionEngine with exactly the state _store_option_batch reads."""
    engine = _make_engine_for_write_test()
    engine.options_buffer = defaultdict(list)
    engine._option_flow = {}
    engine._option_flow_lock = threading.Lock()
    engine._option_bucket_last_write = {}
    engine._classify_fallback_count = 0
    engine._enrich_with_greeks = lambda data: data  # snapshots already carry Greeks
    return engine


def _chain(spot: float):
    expirations = [date(2026, 11, 13), date(2026, 11, 20), date(2026, 12, 18)]
    for exp in expirations:
        for k in range(int(spot) - 12, int(spot) + 13, 2):
            for cp in "CP":
                yield f"SPY{exp:%y%m%d}{cp}{k * 1000:08d}", float(k), exp, cp


def _snapshot(sym, strike, exp, cp, ts, spot, volume, oi) -> dict:
    years = max((_et(exp, 16, 0) - ts).total_seconds(), 600.0) / (365.0 * 86400.0)
    iv = 0.18 + 0.002 * abs(strike - spot)
    gamma = _bs_gamma(spot, strike, years, iv)
    intrinsic = max(0.0, (spot - strike) if cp == "C" else (strike - spot))
    mid = round(intrinsic + 0.4 * iv * spot * math.sqrt(years), 2)
    return {
        "option_symbol": sym,
        "timestamp": ts,
        "underlying": "SPY",
        "strike": strike,
        "expiration": exp,
        "option_type": cp,
        "last": mid,
        "bid": mid - 0.02,
        "ask": mid + 0.02,
        "mid": mid,
        "volume": volume,
        "open_interest": oi,
        "implied_volatility": round(iv, 6),
        "delta": 0.5 if cp == "C" else -0.5,
        "gamma": round(gamma, 8),
        "theta": -0.05,
        "vega": 0.1,
        "charm": 0.0,
        "vanna": 0.0,
    }


def test_two_sessions_through_the_pipeline_give_the_same_snapshot_and_gex(scratch_db):
    """The writer's real path -- per-contract accumulator, minute rollover with
    the carried-over quote, dual write -- for the last minutes of one session
    and the first half hour of the next, with open interest falling on half
    the chain overnight.  Then the parity tool reads both snapshot paths in
    one transaction: every contract, every strike and the GEX must match, and
    the cache must show TODAY's open interest, not yesterday's."""
    rng = random.Random(7)
    spot = 600.0
    chain = list(_chain(spot))
    oi_prior = {c[0]: rng.randint(500, 20000) for c in chain}
    # Half the chain loses 20-60% of its open interest overnight.
    oi_today = {
        s: (int(v * rng.uniform(0.4, 0.8)) if i % 2 else int(v * rng.uniform(1.0, 1.2)))
        for i, (s, v) in enumerate(sorted(oi_prior.items()))
    }
    volume = {c[0]: 0 for c in chain}

    engine = _pipeline_engine()
    conn = scratch_db()
    try:
        with _patched_writer_connection(scratch_db):
            steps = [(_PRIOR_SESSION, 15, m) for m in range(55, 60)]
            steps += [(_SESSION, 9 + (30 + m) // 60, (30 + m) % 60) for m in range(0, 31)]
            for day, hh, mm in steps:
                if (day, hh, mm) == (_SESSION, 9, 30):
                    volume = {s: 0 for s in volume}  # the vendor's cash-open reset
                oi = oi_prior if day == _PRIOR_SESSION else oi_today
                for second in (5, 35):
                    ts = _et(day, hh, mm, second)
                    batch = []
                    for sym, strike, exp, cp in chain:
                        if rng.random() < 0.7:
                            volume[sym] += rng.randint(1, 30)
                        batch.append(
                            _snapshot(sym, strike, exp, cp, ts, spot, volume[sym], oi[sym])
                        )
                    engine._store_option_batch(batch)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO underlying_quotes VALUES ('SPY', %s, %s)",
                        (_et(day, hh, mm), spot),
                    )
                conn.commit()

        engine_ro = AnalyticsEngine(underlying="SPY")
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            with patch.object(analytics_module, "db_connection", side_effect=AssertionError):
                result = compare_snapshot(cur, engine_ro)
        conn.rollback()
    finally:
        conn.close()

    assert result["status"] == "pass", result
    assert result["contracts_history"] == result["contracts_cache"] == len(chain)
    assert (result["oi_differs"], result["volume_differs"]) == (0, 0)
    assert (result["only_history"], result["only_cache"]) == (0, 0)
    assert (result["strikes_oi_differ"], result["strikes_volume_differ"]) == (0, 0)
    assert result["gex_history"] == result["gex_cache"]
    assert result["gex_history"]["call_wall"] is not None

    # And it is today's open interest the cache carries.
    conn = scratch_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT option_symbol, open_interest FROM option_chains_latest")
            cached_oi = dict(cur.fetchall())
    finally:
        conn.close()
    assert cached_oi == oi_today


def test_a_greeks_gap_is_reported_apart_and_does_not_fail_parity(scratch_db):
    """The one difference the paths keep by design.  When ingestion writes a
    minute without gamma (its Greeks gate), history's DISTINCT ON skips that
    row for the older gamma-bearing one while the cache keeps the newest row
    with its last good Greeks -- so the two disagree on volume.  The parity
    tool must name that as a Greeks gap rather than fail the sample, and must
    still fail an unexplained difference."""
    engine = _make_engine_for_write_test()
    exp = date(2026, 11, 20)
    t1, t2 = _et(_SESSION, 11, 0), _et(_SESSION, 11, 1)
    rows = []
    for k, cp in ((595.0, "P"), (600.0, "C"), (605.0, "C")):
        sym = f"SPY261120{cp}00{int(k) * 1000:06d}"
        for ts, vol in ((t1, 100), (t2, 140)):
            row = _row(sym, ts, volume=vol, open_interest=2000)
            row.update(strike=k, option_type=cp, expiration=exp)
            rows.append(row)
    gap_symbol = rows[-1]["option_symbol"]
    rows[-1]["gamma"] = None  # the 605 call's 11:01 row: no Greeks
    with _patched_writer_connection(scratch_db):
        # One minute per batch, as the writer sends them, so the cache holds
        # 11:00's gamma when the gamma-less 11:01 row arrives.
        engine._write_option_rows([r for r in rows if r["timestamp"] == t1])
        engine._write_option_rows([r for r in rows if r["timestamp"] == t2])

    conn = scratch_db()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO underlying_quotes VALUES ('SPY', %s, 600.0)", (t2,))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            result = compare_snapshot(cur, AnalyticsEngine(underlying="SPY"))
        conn.rollback()

        # Now an unexplained one: the cache row's volume knocked off history's.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE option_chains_latest SET volume = 1 WHERE option_symbol = %s",
                (rows[0]["option_symbol"],),
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            broken = compare_snapshot(cur, AnalyticsEngine(underlying="SPY"))
        conn.rollback()
    finally:
        conn.close()

    assert result["volume_differs"] == 1 and result["greeks_gap"] == 1
    assert result["unexplained"] == 0 and result["status"] == "pass", result
    assert broken["status"] == "fail"
    assert broken["unexplained_examples"] == [rows[0]["option_symbol"]]
    assert gap_symbol not in broken["unexplained_examples"]


def test_a_greeks_gap_that_moves_gex_passes_and_a_gamma_drift_fails(scratch_db):
    """MSFT on 2026-11-16 failed on total net GEX alone, every contract that
    differed a Greeks gap: a minute rewritten without gamma leaves history on an
    older gamma while the cache keeps the newer one, which moves GEX by design.
    That must pass, with the GEX difference still reported. A gamma that differs
    while history HAS gamma is not explained, and must fail even with open
    interest and volume identical."""
    engine = _make_engine_for_write_test()
    exp = date(2026, 11, 20)
    t1, t2 = _et(_SESSION, 11, 0), _et(_SESSION, 11, 1)

    def rows_at(ts, gamma_600=0.02):
        out = []
        for k, cp in ((595.0, "P"), (600.0, "C"), (605.0, "C")):
            row = _row(f"SPY261120{cp}00{int(k) * 1000:06d}", ts, volume=100, open_interest=2000)
            row.update(strike=k, option_type=cp, expiration=exp)
            if k == 600.0:
                row["gamma"] = gamma_600
            out.append(row)
        return out

    with _patched_writer_connection(scratch_db):
        engine._write_option_rows(rows_at(t1))
        engine._write_option_rows(rows_at(t2, gamma_600=0.06))
        # The same minute again without gamma: history's 11:01 row loses it
        # (its upsert takes the written gamma), the cache keeps 0.06.
        engine._write_option_rows(rows_at(t2, gamma_600=None))

    drifted = rows_at(t1)[0]["option_symbol"]
    conn = scratch_db()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO underlying_quotes VALUES ('SPY', %s, 600.0)", (t2,))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            result = compare_snapshot(cur, AnalyticsEngine(underlying="SPY"))
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE option_chains_latest SET gamma = 0.07 WHERE option_symbol = %s",
                (drifted,),
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            broken = compare_snapshot(cur, AnalyticsEngine(underlying="SPY"))
        conn.rollback()
    finally:
        conn.close()

    assert (result["oi_differs"], result["volume_differs"]) == (0, 0)
    assert result["greeks_differ"] == 1 and result["greeks_gap"] == 1
    assert result["unexplained"] == 0 and result["status"] == "pass", result
    assert "total_net_gex" in result["gex_differs"]  # moved, reported, not failed

    assert (broken["oi_differs"], broken["volume_differs"]) == (0, 0)
    assert broken["status"] == "fail"
    assert broken["unexplained_examples"] == [drifted]


def test_an_iv_gap_passes_and_an_unexplained_iv_change_fails(scratch_db):
    """NVDA after the 2026-11-17 close: open interest, volume and total net GEX
    identical, the flip and net GEX at spot moved. When the IV solver fails,
    ingestion prices the Greeks off its default IV and stores no IV, so the
    newest history row has gamma but no IV while the cache keeps the last
    solved IV. That is by design and must pass. An IV that differs while
    history HAS one is not explained, and must fail."""
    engine = _make_engine_for_write_test()
    exp = date(2026, 11, 20)
    t1, t2 = _et(_SESSION, 11, 0), _et(_SESSION, 11, 1)

    def rows_at(ts, iv_600=0.25):
        out = []
        for k, cp in ((595.0, "P"), (600.0, "C"), (605.0, "C")):
            row = _row(f"SPY261120{cp}00{int(k) * 1000:06d}", ts, volume=100, open_interest=2000)
            row.update(strike=k, option_type=cp, expiration=exp, implied_volatility=0.25)
            if k == 600.0:
                row["implied_volatility"] = iv_600
            out.append(row)
        return out

    with _patched_writer_connection(scratch_db):
        engine._write_option_rows(rows_at(t1))
        engine._write_option_rows(rows_at(t2, iv_600=None))  # the 11:01 IV solve failed

    changed = rows_at(t1)[0]["option_symbol"]
    conn = scratch_db()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO underlying_quotes VALUES ('SPY', %s, 600.0)", (t2,))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            result = compare_snapshot(cur, AnalyticsEngine(underlying="SPY"))
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE option_chains_latest SET implied_volatility = 0.40 "
                "WHERE option_symbol = %s",
                (changed,),
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            broken = compare_snapshot(cur, AnalyticsEngine(underlying="SPY"))
        conn.rollback()
    finally:
        conn.close()

    assert (result["oi_differs"], result["volume_differs"]) == (0, 0)
    assert result["greeks_differ"] == 1 and result["iv_gap"] == 1 and result["greeks_gap"] == 0
    assert result["unexplained"] == 0 and result["status"] == "pass", result
    assert "total_net_gex" not in result["gex_differs"]

    assert broken["status"] == "fail"
    assert broken["unexplained_examples"] == [changed]
