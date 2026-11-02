"""Value-level parity: 1-minute hedging bars vs 5-minute hedging bars on real rows.

``timeframe=1min`` runs the hedging query with the bar formed as the minute
(``HEDGING_FLOW_1MIN_CTE_ASYNCPG``), so the claim that it reports the same
totals as the 5-minute bars is only as good as a run against a real Postgres.
The rest of the hedging suite mocks the connection. Same reason
``tests/test_flow_series_1min_parity.py`` exists for the flow series.

The claims, unfiltered and under strike / expiration filters:

* every 5-minute bar's cumulative fields equal those of the last 1-minute bar
  inside it;
* every 5-minute bar's per-bar flows equal the sum of its five minutes';
* a minute with no trades is synthetic and carries the cumulative forward,
  and a minute with no underlying quote carries the price forward;
* facts with no delta are left out of both, and trades outside the session
  window are counted by neither.

The harness SEEDS a synthetic session under a sentinel symbol and removes every
row it wrote, so point it at a SCRATCH database with schema.sql applied:

    HEDGING_FLOW_1MIN_PARITY_DSN=postgresql://user@host:5432/scratch \\
        python -m pytest tests/test_hedging_flow_1min_parity.py -m integration --no-cov -q
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from src.hedging_flow_sql import HEDGING_FLOW_1MIN_CTE_ASYNCPG, HEDGING_FLOW_CTE_ASYNCPG

pytestmark = pytest.mark.integration

_DSN = os.getenv("HEDGING_FLOW_1MIN_PARITY_DSN")
_SYMBOL = os.getenv("HEDGING_FLOW_1MIN_PARITY_SYMBOL", "ZZTEST")
_SKIP_REASON = (
    "HEDGING_FLOW_1MIN_PARITY_DSN not set — 1-minute hedging parity harness skipped "
    "(point it at a scratch database; it seeds data)."
)

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
_DAY = date(2026, 10, 2)
_STRIKES = (660.0, 665.0, 670.0)
_EXPIRATIONS = (date(2026, 10, 2), date(2026, 10, 5))
_QUIET = {(10, m) for m in range(40, 48)}  # no trades at all: carry-forward minutes
_NO_PRICE = {(11, 2), (11, 3)}  # no underlying quote: price must carry
_NO_DELTA = {(12, 10), (14, 33)}  # trades whose delta is missing: excluded

_CUMULATIVE = ("cum_call_usd", "cum_put_usd", "cum_net_usd", "underlying_price")
_PER_BAR = ("call_flow_usd", "put_flow_usd", "net_flow_usd")


def _et(h: int, m: int) -> datetime:
    return datetime(_DAY.year, _DAY.month, _DAY.day, h, m, tzinfo=ET).astimezone(UTC)


async def _purge(conn) -> None:
    for table in ("flow_contract_facts", "underlying_quotes"):
        await conn.execute(f"DELETE FROM {table} WHERE symbol = $1", _SYMBOL)
    await conn.execute("DELETE FROM symbols WHERE symbol = $1", _SYMBOL)


def _fact(ts, option_symbol, strike, expiration, option_type, vol, buy, px, delta, price):
    def usd(contracts: int) -> Decimal:
        return Decimal(str(round(contracts * px * 100, 2)))

    sign = 1 if option_type == "C" else -1
    return (
        ts,
        _SYMBOL,
        option_symbol,
        strike,
        expiration,
        option_type,
        vol,
        usd(vol),
        sign * vol,
        usd(sign * vol),
        buy,
        vol - buy,
        usd(buy),
        usd(vol - buy),
        delta,
        price,
    )


async def _seed(conn) -> None:
    rng = random.Random(20261102)
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
                    vol = rng.randint(1, 200)
                    moneyness = (price - k) / 10.0
                    call_delta = min(0.95, max(0.05, 0.5 + moneyness))
                    delta = call_delta if t == "C" else call_delta - 1.0
                    facts.append(
                        _fact(
                            ts,
                            f"{_SYMBOL}{e:%y%m%d}{t}{int(k)}",
                            k,
                            e,
                            t,
                            vol,
                            rng.randint(0, vol),
                            rng.uniform(0.05, 6.0),
                            None if (h, m) in _NO_DELTA else Decimal(str(round(delta, 6))),
                            Decimal(str(round(price, 4))),
                        )
                    )
    # Outside the session on both sides: neither bar size may count them.
    for ts in (_et(9, 25), _et(16, 20)):
        facts.append(
            _fact(
                ts,
                f"{_SYMBOL}EDGE{ts:%H%M}",
                _STRIKES[0],
                _EXPIRATIONS[0],
                "C",
                999,
                999,
                1.0,
                Decimal("0.5"),
                Decimal("665"),
            )
        )
    await conn.executemany(
        "INSERT INTO flow_contract_facts (timestamp, symbol, option_symbol, strike,"
        " expiration, option_type, volume_delta, premium_delta, signed_volume,"
        " signed_premium, buy_volume, sell_volume, buy_premium, sell_premium,"
        " delta, underlying_price) VALUES"
        " ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
        facts,
    )
    await conn.executemany(
        "INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close)"
        " VALUES ($1,$2,$3,$4,$5,$6)",
        quotes,
    )


def _same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) < 1e-6


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_one_minute_hedging_bars_add_up_to_the_five_minute_bars():
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
                    HEDGING_FLOW_CTE_ASYNCPG, _SYMBOL, start, end, strikes, expirations
                )
                one = await conn.fetch(
                    HEDGING_FLOW_1MIN_CTE_ASYNCPG, _SYMBOL, start, end, strikes, expirations
                )
                assert len(five) == 82 and len(one) == 406
                by_minute = {r["bar_start"]: r for r in one}
                for bar in five:
                    minutes = [
                        by_minute[m]
                        for m in (bar["bar_start"] + timedelta(minutes=i) for i in range(5))
                        if m in by_minute
                    ]
                    closing = minutes[-1]
                    diff = [f for f in _CUMULATIVE if not _same(bar[f], closing[f])]
                    assert not diff, (strikes, expirations, bar["bar_start"], diff)
                    summed = {f: sum(float(m[f]) for m in minutes) for f in _PER_BAR}
                    diff = [f for f in _PER_BAR if not _same(bar[f], summed[f])]
                    assert not diff, (strikes, expirations, bar["bar_start"], diff)
                    assert bar["is_synthetic"] == all(m["is_synthetic"] for m in minutes)

            one = await conn.fetch(HEDGING_FLOW_1MIN_CTE_ASYNCPG, _SYMBOL, start, end, None, None)
            by_et = {r["bar_start"].astimezone(ET).strftime("%H:%M"): r for r in one}
            for h, m in _QUIET:
                quiet = by_et[f"{h}:{m:02d}"]
                assert quiet["is_synthetic"] and quiet["net_flow_usd"] == 0
                assert quiet["cum_net_usd"] == by_et["10:39"]["cum_net_usd"]
            assert by_et["11:02"]["underlying_price"] == by_et["11:01"]["underlying_price"]
            # Every fact in a no-delta minute is dropped, as on 5-minute bars.
            for h, m in _NO_DELTA:
                traded = await conn.fetchval(
                    "SELECT COUNT(*) FROM flow_contract_facts WHERE symbol = $1 AND timestamp = $2",
                    _SYMBOL,
                    _et(h, m),
                )
                assert traded > 0, "the seed must trade in a no-delta minute"
                assert by_et[f"{h}:{m:02d}"]["is_synthetic"]
            # The session's total is the sum of every in-session trade with a
            # delta: nothing from 09:25 or 16:20, nothing double counted.
            total = await conn.fetchval(
                "SELECT SUM((buy_volume - sell_volume) * delta * 100 * underlying_price)"
                " FROM flow_contract_facts WHERE symbol = $1 AND timestamp >= $2"
                " AND timestamp < $3 AND delta IS NOT NULL",
                _SYMBOL,
                start,
                end,
            )
            assert _same(one[0]["cum_net_usd"], total)
        finally:
            await _purge(conn)
            await conn.close()

    asyncio.run(_run())
