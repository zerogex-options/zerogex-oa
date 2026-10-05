"""Value-level parity: 1-minute flow bars vs 5-minute flow bars on real rows.

``timeframe=1min`` is read from ``flow_contract_facts`` while the default
5-minute series is read from ``flow_by_contract``, so the claim that both
report the same totals is only as good as a run against a real Postgres. Same
reason ``tests/test_flow_series_parity.py`` and
``tests/test_hedging_flow_snapshot_sql.py`` exist.

The claim: for every 5-minute bar, the session-cumulative fields equal those of
the last 1-minute bar inside it, unfiltered and under strike / expiration
filters. ``contract_count`` and ``is_synthetic`` are deliberately absent:
on 1-minute bars they describe the minute itself (see src/flow_series_sql.py).

The harness SEEDS a synthetic session under a sentinel symbol and removes every
row it wrote, so point it at a SCRATCH database with schema.sql applied:

    FLOW_SERIES_1MIN_PARITY_DSN=postgresql://user@host:5432/scratch \\
        python -m pytest tests/test_flow_series_1min_parity.py -m integration --no-cov -q

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

from src.flow_series_sql import FLOW_SERIES_1MIN_CTE_ASYNCPG, FLOW_SERIES_CTE_ASYNCPG

pytestmark = pytest.mark.integration

_DSN = os.getenv("FLOW_SERIES_1MIN_PARITY_DSN")
_SYMBOL = os.getenv("FLOW_SERIES_1MIN_PARITY_SYMBOL", "ZZTEST")
_SKIP_REASON = (
    "FLOW_SERIES_1MIN_PARITY_DSN not set — 1-minute parity harness skipped "
    "(point it at a scratch database; it seeds data)."
)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
_DAY = date(2026, 10, 2)
_STRIKES = (660.0, 665.0, 670.0)
_EXPIRATIONS = (date(2026, 10, 2), date(2026, 10, 5))
_QUIET = {(10, m) for m in range(40, 48)}  # no trades at all: carry-forward bars
_NO_PRICE = {(11, 2), (11, 3)}  # no underlying bar: price must carry

_CUMULATIVE = (
    "call_premium_cum",
    "put_premium_cum",
    "call_volume_cum",
    "put_volume_cum",
    "net_volume_cum",
    "raw_volume_cum",
    "call_position_cum",
    "put_position_cum",
    "net_premium_cum",
    "put_call_ratio",
    "underlying_price",
)

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


def _et(h: int, m: int) -> datetime:
    return datetime(_DAY.year, _DAY.month, _DAY.day, h, m, tzinfo=ET).astimezone(UTC)


async def _purge(conn) -> None:
    for table in ("flow_by_contract", "flow_contract_facts", "underlying_quotes"):
        await conn.execute(f"DELETE FROM {table} WHERE symbol = $1", _SYMBOL)
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


async def _seed(conn) -> None:
    rng = random.Random(20261002)
    await conn.execute(
        "INSERT INTO symbols (symbol, name, asset_type) VALUES ($1, $1, 'ETF')", _SYMBOL
    )
    facts, quotes, price = [], [], 665.0
    for h, m in [(h, m) for h in range(9, 17) for m in range(60)]:
        if not (9, 30) <= (h, m) <= (16, 15):
            continue
        ts = _et(h, m)
        price += rng.uniform(-0.4, 0.4)
        if (h, m) not in _NO_PRICE:
            quotes.append((_SYMBOL, ts, price, price + 0.2, price - 0.2, price))
        if (h, m) in _QUIET or (h, m) == (16, 15):
            continue
        for k in _STRIKES:
            for e in _EXPIRATIONS:
                for t in "CP":
                    if rng.random() > 0.35:
                        continue
                    vol, px = rng.randint(1, 200), rng.uniform(0.05, 6.0)
                    buy = rng.randint(0, vol)
                    sign = 1 if t == "C" else -1

                    def usd(contracts: int) -> Decimal:
                        return Decimal(str(round(contracts * px * 100, 2)))

                    facts.append(
                        (
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
                            Decimal(str(round(price, 4))),
                        )
                    )
    # Outside the session on both sides: neither series may count them.
    for ts in (_et(9, 25), _et(16, 20)):
        facts.append(
            (
                ts,
                _SYMBOL,
                f"{_SYMBOL}EDGE{ts:%H%M}",
                _STRIKES[0],
                _EXPIRATIONS[0],
                "C",
                999,
                Decimal("99900"),
                999,
                Decimal("99900"),
                999,
                0,
                Decimal("99900"),
                Decimal("0"),
                Decimal("665"),
            )
        )
    await conn.executemany(
        "INSERT INTO flow_contract_facts (timestamp, symbol, option_symbol, strike,"
        " expiration, option_type, volume_delta, premium_delta, signed_volume,"
        " signed_premium, buy_volume, sell_volume, buy_premium, sell_premium,"
        " underlying_price) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        facts,
    )
    await conn.executemany(
        "INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close)"
        " VALUES ($1,$2,$3,$4,$5,$6)",
        quotes,
    )
    await conn.execute(_BUILD_FLOW_BY_CONTRACT, _SYMBOL, _et(9, 30), _et(16, 15))


def _same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) < 1e-6


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_one_minute_bars_close_every_five_minute_bar_on_the_same_totals():
    import asyncpg

    async def _run():
        conn = await asyncpg.connect(_DSN)
        try:
            await _purge(conn)
            await _seed(conn)
            start, end = _et(9, 30), _et(16, 15)
            for strikes, expirations in (
                (None, None),
                ([665.0], None),
                (None, [_EXPIRATIONS[0]]),
                ([660.0, 670.0], [_EXPIRATIONS[1]]),
            ):
                five = await conn.fetch(
                    FLOW_SERIES_CTE_ASYNCPG, _SYMBOL, start, end, strikes, expirations
                )
                one = await conn.fetch(
                    FLOW_SERIES_1MIN_CTE_ASYNCPG, _SYMBOL, start, end, strikes, expirations
                )
                assert len(five) == 82 and len(one) == 406
                by_minute = {r["bar_start"]: r for r in one}
                for bar in five:
                    closing = by_minute[min(bar["bar_start"] + timedelta(minutes=4), end)]
                    diff = [f for f in _CUMULATIVE if not _same(bar[f], closing[f])]
                    assert not diff, (strikes, expirations, bar["bar_start"], diff)

            one = await conn.fetch(FLOW_SERIES_1MIN_CTE_ASYNCPG, _SYMBOL, start, end, None, None)
            by_et = {r["bar_start"].astimezone(ET).strftime("%H:%M"): r for r in one}
            assert all(by_et[f"{h}:{m:02d}"]["is_synthetic"] for h, m in _QUIET)
            assert by_et["11:02"]["underlying_price"] == by_et["11:01"]["underlying_price"]
        finally:
            await _purge(conn)
            await conn.close()

    asyncio.run(_run())
