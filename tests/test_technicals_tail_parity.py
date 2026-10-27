"""Parity: a technicals tail read returns the same bars as the full read.

The website's charts poll ``/api/technicals`` once a second for the live tip.
Asking for ``intervals=N`` (the trailing N 5-minute buckets) instead of the
whole session cuts each answer from ~70 KB to ~2 KB, but only if a tail bar
carries the same values as that bar in the full read: the chart merges both
into one cache, so a tail that disagreed would make the VWAP jump between two
answers every time a poll or a 5-minute reload landed.

The VWAP is cumulative from the start of the ET day, so a tail has to read the
whole day even though it returns only N buckets. An earlier version read 160
minutes back, which covers the 30-bucket volume window but turns the VWAP into
an average of the last ~3 hours. It went unnoticed because the website sent
``interval=3``, which the endpoint ignores, so every poll got the full session.

Each case also checks the newest bar's VWAP against one computed here from the
seeded rows, so the full read and the tail cannot agree on a wrong answer.

Value-level SQL behaviour is only meaningful against a real server, so this is
``integration``-marked and skips unless a DSN is exported::

    TECHNICALS_TAIL_TEST_DSN=postgresql://user:pass@localhost:5432/scratch \\
        pytest tests/test_technicals_tail_parity.py -q -m integration

The DSN's database must already have ``setup/database/schema.sql`` applied.
"""

from __future__ import annotations

import asyncio
import math
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("TECHNICALS_TAIL_TEST_DSN")

_SKIP = (
    "TECHNICALS_TAIL_TEST_DSN not set — technicals tail-parity integration test "
    "skipped (needs a scratch Postgres with the project schema)."
)

_ET = ZoneInfo("America/New_York")

# Throwaway symbol: the teardown deletes it, so it must never be a real one.
# Not in the index volume-proxy map, so it is its own volume source.
_SYMBOL = "ZZTAIL"

_PRIOR_DAY = date(2026, 8, 11)
_SESSION_DAY = date(2026, 8, 12)

# The latest bar: mid-afternoon, so a 160-minute lookback starts well after
# the open for both an ETF (04:00) and a cash index (09:30).
_LAST_BAR = time(13, 57)

_SESSION_START = {"ETF": time(4, 0), "INDEX": time(9, 30)}
_SESSION_END = {"ETF": time(20, 0), "INDEX": time(16, 0)}


def _minutes(day: date, start: time, end: time) -> list[datetime]:
    """Every minute from ``start`` up to but not including ``end`` on ``day``, ET."""
    t = datetime.combine(day, start, tzinfo=_ET)
    stop = datetime.combine(day, end, tzinfo=_ET)
    out = []
    while t < stop:
        out.append(t)
        t += timedelta(minutes=1)
    return out


def _bar(i: int) -> tuple[float, int, int]:
    """Close, up volume and down volume for minute ``i``.

    The price trends up through the day, so a VWAP that starts late reads
    clearly higher than one that starts at the open. Volumes vary so the
    weighting matters.
    """
    close = round(600 + 0.05 * i + 0.3 * math.sin(i / 7), 4)
    up = 1000 + (i * 37) % 500
    down = 800 + (i * 53) % 400
    return close, up, down


def _session_minutes(asset_type: str) -> list[datetime]:
    return _minutes(
        _SESSION_DAY,
        _SESSION_START[asset_type],
        (datetime.combine(_SESSION_DAY, _LAST_BAR) + timedelta(minutes=1)).time(),
    )


async def _seed(conn, asset_type: str) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)
    await conn.execute(
        "INSERT INTO symbols(symbol, name, asset_type, is_active) "
        "VALUES ($1, 'technicals tail parity test', $2, true)",
        _SYMBOL,
        asset_type,
    )
    prior = _minutes(_PRIOR_DAY, _SESSION_START[asset_type], _SESSION_END[asset_type])
    rows = []
    for i, ts in enumerate(prior + _session_minutes(asset_type)):
        close, up, down = _bar(i)
        rows.append((_SYMBOL, ts, close, close + 0.1, close - 0.1, close, up, down, up + down))
    await conn.executemany(
        "INSERT INTO underlying_quotes(symbol, timestamp, open, high, low, close, "
        "up_volume, down_volume, volume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        rows,
    )
    # Option flow on every fourth minute, calls and puts in different sizes, so
    # the divergence fields carry real values in every bucket.
    flow = []
    for i, ts in enumerate(_session_minutes(asset_type)):
        if i % 4:
            continue
        for opt, premium in (("C", 40_000 + 900 * (i % 17)), ("P", 35_000 + 1_300 * (i % 11))):
            flow.append(
                (
                    ts,
                    _SYMBOL,
                    f"{_SYMBOL}260814{opt}00600000",
                    600,
                    date(2026, 8, 14),
                    opt,
                    10,
                    premium,
                    10,
                    premium,
                )
            )
    await conn.executemany(
        "INSERT INTO flow_contract_facts(timestamp, symbol, option_symbol, strike, expiration, "
        "option_type, volume_delta, premium_delta, signed_volume, signed_premium) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        flow,
    )


async def _teardown(conn) -> None:
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


async def _technicals(conn, intervals=None):
    from src.api.database import DatabaseManager

    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    return await db.get_technicals_timeseries(_SYMBOL, intervals=intervals)


def _expected_day_vwap(asset_type: str) -> float:
    """The newest bar's VWAP, from the seeded rows: every 5-minute bucket of the
    day so far, each bucket's last close weighted by the bucket's volume."""
    buckets: dict[datetime, list[tuple[float, int]]] = {}
    prior_count = len(_minutes(_PRIOR_DAY, _SESSION_START[asset_type], _SESSION_END[asset_type]))
    for j, ts in enumerate(_session_minutes(asset_type)):
        close, up, down = _bar(prior_count + j)
        key = ts.replace(minute=ts.minute - ts.minute % 5)
        buckets.setdefault(key, []).append((close, up + down))
    pv = sum(minutes[-1][0] * sum(v for _, v in minutes) for minutes in buckets.values())
    vol = sum(sum(v for _, v in minutes) for minutes in buckets.values())
    return pv / vol


@pytest.mark.skipif(_DSN is None, reason=_SKIP)
@pytest.mark.parametrize("asset_type", ["ETF", "INDEX"])
@pytest.mark.parametrize("intervals", [1, 3, 12])
def test_tail_bars_equal_the_full_reads(asset_type, intervals):
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _seed(conn, asset_type)
            full = await _technicals(conn)
            tail = await _technicals(conn, intervals)
        finally:
            await _teardown(conn)
            await conn.close()
        return full, tail

    full, tail = asyncio.run(_run())

    assert full["bars"], "the full read must return the seeded session"
    assert len(tail["bars"]) == intervals
    # The tail is the newest stretch of the full read, bar for bar.
    assert tail["bars"] == full["bars"][:intervals]
    for key in (
        "symbol",
        "asset_type",
        "session_date",
        "session_start_et",
        "session_end_et",
        "volume_proxy",
    ):
        assert tail[key] == full[key]

    expected = _expected_day_vwap(asset_type)
    for payload in (full, tail):
        assert payload["bars"][0]["vwap_deviation"]["vwap"] == pytest.approx(expected, abs=1e-4)
