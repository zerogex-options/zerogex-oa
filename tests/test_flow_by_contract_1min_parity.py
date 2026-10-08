"""Value-level parity: 1-minute /api/flow/by-contract rows on real rows.

``timeframe=1min`` is built from ``flow_contract_facts`` by
``_FLOW_BY_CONTRACT_1MIN_SQL`` while the default 5-minute rows are read from
``flow_by_contract``, so the claim that both report the same totals is only as
good as a run against a real Postgres. Same reason
``tests/test_flow_series_1min_parity.py`` exists.

Two claims, for windows at the open, mid-session, a single minute and the close:

  1. Every row matches the definition: per contract, SUM over the facts from
     the 09:30 ET open to the end of its minute, with every contract that has
     traded present in every later minute.
  2. For every 5-minute bucket, the row for its last minute matches the
     5-minute row, contract for contract.

The harness SEEDS a synthetic session under a sentinel symbol and removes every
row it wrote, so point it at a SCRATCH database with schema.sql applied:

    FLOW_BY_CONTRACT_1MIN_PARITY_DSN=postgresql://user@host:5432/scratch \\
        python -m pytest tests/test_flow_by_contract_1min_parity.py -m integration --no-cov -q

``flow_by_contract`` is built here the way AnalyticsEngine._refresh_flow_caches
builds it: per contract, SUM over the facts from the 09:30 ET open through
each bucket's end, contracts with no volume omitted.
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from src.api.database import _FLOW_BY_CONTRACT_1MIN_SQL

pytestmark = pytest.mark.integration

_DSN = os.getenv("FLOW_BY_CONTRACT_1MIN_PARITY_DSN")
_SYMBOL = os.getenv("FLOW_BY_CONTRACT_1MIN_PARITY_SYMBOL", "ZZTEST")
_SKIP_REASON = (
    "FLOW_BY_CONTRACT_1MIN_PARITY_DSN not set — 1-minute by-contract parity "
    "harness skipped (point it at a scratch database; it seeds data)."
)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
_DAY = date(2026, 10, 2)
_STRIKES = (660.0, 665.0, 670.0)
_EXPIRATIONS = (date(2026, 10, 2), date(2026, 10, 5))
_EARLY_ONLY = ("C", 660.0, _EXPIRATIONS[0])  # last trade before 10:00
_LATE_START = ("P", 670.0, _EXPIRATIONS[1])  # first trade at 11:30
_QUIET = {(11, m) for m in range(10, 20)}  # nobody trades
_VALUES = ("raw_volume", "raw_premium", "net_volume", "net_premium", "underlying_price")

_BUILD_FLOW_BY_CONTRACT = """
    INSERT INTO flow_by_contract (
        timestamp, symbol, option_type, strike, expiration,
        raw_volume, raw_premium, net_volume, net_premium, underlying_price
    )
    SELECT
        b.bucket_start, f.symbol, f.option_type, f.strike, f.expiration,
        SUM(f.volume_delta)::bigint,
        SUM(f.premium_delta)::numeric,
        SUM(f.buy_volume - f.sell_volume)::bigint,
        SUM(f.buy_premium - f.sell_premium)::numeric,
        MAX(f.underlying_price)::numeric
    FROM flow_contract_facts f
    CROSS JOIN generate_series($2::timestamptz, $3::timestamptz, INTERVAL '5 minutes')
        AS b(bucket_start)
    WHERE f.symbol = $1
      AND f.timestamp >= $2::timestamptz
      AND f.timestamp < b.bucket_start + INTERVAL '5 minutes'
    GROUP BY b.bucket_start, f.symbol, f.option_type, f.strike, f.expiration
    HAVING SUM(f.volume_delta) > 0
"""

# The definition, minute by minute. It rescans the session once per minute,
# which is exactly why production does not compute it this way.
_DEFINITION = """
    SELECT
        m.minute AS timestamp,
        f.option_type, f.strike, f.expiration,
        SUM(f.volume_delta)::bigint AS raw_volume,
        SUM(f.premium_delta)::numeric AS raw_premium,
        SUM(f.buy_volume - f.sell_volume)::bigint AS net_volume,
        SUM(f.buy_premium - f.sell_premium)::numeric AS net_premium,
        MAX(f.underlying_price)::numeric AS underlying_price
    FROM generate_series($3::timestamptz, $4::timestamptz, INTERVAL '1 minute') AS m(minute)
    JOIN flow_contract_facts f
      ON f.symbol = $1
     AND f.timestamp >= $2::timestamptz
     AND f.timestamp < m.minute + INTERVAL '1 minute'
    GROUP BY m.minute, f.option_type, f.strike, f.expiration
    HAVING SUM(f.volume_delta) > 0
"""


def _et(h: int, m: int, s: int = 0) -> datetime:
    return datetime(_DAY.year, _DAY.month, _DAY.day, h, m, s, tzinfo=ET).astimezone(UTC)


def _key(row) -> tuple:
    return (row["timestamp"], row["option_type"], float(row["strike"]), row["expiration"])


def _same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) < 1e-6


async def _purge(conn) -> None:
    for table in ("flow_by_contract", "flow_contract_facts"):
        await conn.execute(f"DELETE FROM {table} WHERE symbol = $1", _SYMBOL)
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


def _fact(ts: datetime, t: str, k: float, e: date, vol: int, buy: int, px: float, spot: float):
    def usd(contracts: int) -> Decimal:
        return Decimal(str(round(contracts * px * 100, 2)))

    sign = 1 if t == "C" else -1
    return (
        ts,
        _SYMBOL,
        f"{_SYMBOL}{e:%y%m%d}{t}{int(k)}",
        k,
        e,
        t,
        vol,
        usd(vol),
        sign * vol,
        usd(sign * vol),
        buy,
        vol - buy,
        usd(buy),
        usd(vol - buy),
        Decimal(str(round(spot, 4))),
    )


async def _seed(conn) -> None:
    rng = random.Random(20261008)
    await conn.execute(
        "INSERT INTO symbols (symbol, name, asset_type) VALUES ($1, $1, 'ETF')", _SYMBOL
    )
    facts, spot = [], 665.0
    for h, m in [(h, m) for h in range(9, 17) for m in range(60)]:
        if not (9, 30) <= (h, m) <= (16, 14):
            continue
        spot += rng.uniform(-0.4, 0.4)
        if (h, m) in _QUIET:
            continue
        for k in _STRIKES:
            for e in _EXPIRATIONS:
                for t in "CP":
                    contract = (t, k, e)
                    if contract == _EARLY_ONLY and (h, m) >= (10, 0):
                        continue
                    if contract == _LATE_START and (h, m) < (11, 30):
                        continue
                    if rng.random() > 0.35:
                        continue
                    vol = rng.randint(1, 200)
                    facts.append(
                        _fact(
                            _et(h, m),
                            t,
                            k,
                            e,
                            vol,
                            rng.randint(0, vol),
                            rng.uniform(0.05, 6.0),
                            spot,
                        )
                    )
    # A fact mid-minute must land in that minute, beside the one on the minute.
    facts.append(_fact(_et(12, 41, 30), "C", 665.0, _EXPIRATIONS[0], 77, 50, 2.5, 700.0))
    # Outside the session on both sides: neither timeframe may count them.
    for ts in (_et(9, 25), _et(16, 15), _et(16, 20)):
        facts.append(_fact(ts, "C", 665.0, _EXPIRATIONS[0], 999, 999, 1.0, 900.0))
    await conn.executemany(
        "INSERT INTO flow_contract_facts (timestamp, symbol, option_symbol, strike,"
        " expiration, option_type, volume_delta, premium_delta, signed_volume,"
        " signed_premium, buy_volume, sell_volume, buy_premium, sell_premium,"
        " underlying_price) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        facts,
    )
    await conn.execute(_BUILD_FLOW_BY_CONTRACT, _SYMBOL, _et(9, 30), _et(16, 10))


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_one_minute_rows_match_the_definition_and_close_every_five_minute_bucket():
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _purge(conn)
            await _seed(conn)
            today = await conn.fetchval("SELECT CURRENT_DATE")
            five = {
                _key(r): r
                for r in await conn.fetch(
                    "SELECT * FROM flow_by_contract WHERE symbol = $1", _SYMBOL
                )
            }
            opening = _et(9, 30)
            buckets_checked = 0
            for first, last in (
                (_et(9, 30), _et(10, 29)),
                (_et(11, 0), _et(11, 59)),
                (_et(12, 30), _et(12, 59)),
                (_et(13, 7), _et(13, 7)),
                (_et(15, 15), _et(16, 14)),
            ):
                rows = await conn.fetch(_FLOW_BY_CONTRACT_1MIN_SQL, _SYMBOL, opening, first, last)
                want = {
                    _key(r): r for r in await conn.fetch(_DEFINITION, _SYMBOL, opening, first, last)
                }
                got = {_key(r): r for r in rows}
                assert len(got) == len(rows), "one row per contract per minute"
                assert got.keys() == want.keys(), first
                for key, row in got.items():
                    diff = [f for f in _VALUES if not _same(row[f], want[key][f])]
                    assert not diff, (key, diff)
                    assert row["symbol"] == _SYMBOL
                    assert row["dte"] == (row["expiration"] - today).days
                stamps = [r["timestamp"] for r in rows]
                assert stamps == sorted(stamps, reverse=True), "newest first"

                closing = {k: r for k, r in got.items() if k[0].minute % 5 == 4}
                bucket_rows = {
                    k: r for k, r in five.items() if first <= k[0] + timedelta(minutes=4) <= last
                }
                assert len(closing) == len(bucket_rows), first
                for (ts, t, k, e), row in closing.items():
                    bucket = bucket_rows[(ts - timedelta(minutes=4), t, k, e)]
                    diff = [f for f in _VALUES if not _same(row[f], bucket[f])]
                    assert not diff, (ts, t, k, e, diff)
                buckets_checked += len({k[0] for k in closing})

            # 12 + 12 + 6 + 0 + 12 bucket closes across the five windows.
            assert buckets_checked == 42

            mid = {
                _key(r): r
                for r in await conn.fetch(
                    _FLOW_BY_CONTRACT_1MIN_SQL, _SYMBOL, opening, _et(11, 0), _et(11, 59)
                )
            }

            def at(minute: datetime) -> dict:
                return {k[1:]: r["raw_volume"] for k, r in mid.items() if k[0] == minute}

            # Traded only before 10:00: present in every minute, totals frozen.
            early = [r["raw_volume"] for k, r in mid.items() if k[1:] == _EARLY_ONLY]
            assert len(early) == 60 and len(set(early)) == 1
            # First trade at 11:30: absent before it.
            assert min(k[0] for k in mid if k[1:] == _LATE_START) >= _et(11, 30)
            # Nobody traded 11:10-11:19: each of those minutes carries 11:09.
            for m in range(10, 20):
                assert at(_et(11, m)) == at(_et(11, 9)), m
        finally:
            await _purge(conn)
            await conn.close()

    asyncio.run(_run())
