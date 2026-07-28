"""Futures security-definition ingestion — vendor-neutral provider + fixtures.

A *security definition* is the authoritative description of a tradable
contract published by the exchange/vendor (its exact expiration instant,
multiplier, exercise/settlement style, the underlying future an option
settles into, …).  Per spec rule #14, ZeroGEX treats this metadata as
authoritative rather than parsing critical fields out of display tickers at
runtime.

This module defines:

* :class:`FuturesSecurityDefinitionProvider` — the vendor-neutral async
  Protocol the ingestion layer talks to.
* :class:`MockFuturesSecurityDefinitionProvider` — a deterministic fixture
  implementation used by tests and local development.  It **does not** hit any
  real feed; the licensed CME/vendor adapter is intentionally pending.
* Helpers to build the standard ES/NQ quarterly contract set (correct SOQ
  expiration instants) so the resolver and tests have realistic data.
* Idempotent upsert statements + functions (cursor-based, psycopg2 style —
  matching the existing ingestion write path) targeting the ``futures_*``
  tables added in ``setup/database/schema.sql``.

Nothing here fabricates *market* data (prices/OI); it only describes
*contracts*.  Volume/OI on a mock contract is left ``None`` unless a caller
sets it explicitly for a test.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Iterable, List, Optional, Protocol, Sequence
from zoneinfo import ZoneInfo

from src.futures.contracts import (
    ContractStatus,
    ExerciseStyle,
    FuturesContract,
    FuturesOptionContract,
    FuturesOptionSeries,
    FuturesProduct,
    OptionListingType,
    PutCall,
    QUARTERLY_MONTHS,
    SettlementType,
    contract_label,
)

CT = ZoneInfo("America/Chicago")

#: SOQ final-settlement instant for ES/NQ quarterlies: 3rd Friday 08:30 CT.
_SOQ_TIME = time(8, 30)

#: Static product definitions.  Economics mirror src.instruments so the two
#: registries agree (multiplier 50/20, tick 0.25).
_PRODUCTS: dict[str, FuturesProduct] = {
    "ES": FuturesProduct(
        symbol="ES",
        name="E-mini S&P 500 Futures",
        exchange="CME",
        currency="USD",
        multiplier=50,
        tick_size=0.25,
        family="sp500",
    ),
    "NQ": FuturesProduct(
        symbol="NQ",
        name="E-mini Nasdaq-100 Futures",
        exchange="CME",
        currency="USD",
        multiplier=20,
        tick_size=0.25,
        family="nasdaq100",
    ),
}


def get_product(symbol: str) -> Optional[FuturesProduct]:
    return _PRODUCTS.get((symbol or "").strip().upper())


def all_products() -> List[FuturesProduct]:
    return list(_PRODUCTS.values())


# ---------------------------------------------------------------------------
# Expiration helpers
# ---------------------------------------------------------------------------


def third_friday(year: int, month: int) -> date:
    """The 3rd Friday of a month (ES/NQ quarterly settlement date)."""
    d = date(year, month, 1)
    # weekday(): Mon=0 .. Fri=4
    first_friday_offset = (4 - d.weekday()) % 7
    return d + timedelta(days=first_friday_offset + 14)


def quarterly_expiration_instant(year: int, month: int) -> datetime:
    """SOQ final-settlement instant: 3rd Friday 08:30 CT (tz-aware)."""
    return datetime.combine(third_friday(year, month), _SOQ_TIME, tzinfo=CT)


def _next_quarterly_months(as_of: date, count: int) -> List[tuple[int, int]]:
    """The next ``count`` (year, month) quarterly cycles at/after ``as_of``.

    A quarterly contract stops being listed as front once its 3rd Friday has
    passed, so we skip any cycle whose expiration is already behind ``as_of``.
    """
    out: List[tuple[int, int]] = []
    year = as_of.year
    while len(out) < count:
        for m in QUARTERLY_MONTHS:
            exp = third_friday(year, m)
            if exp >= as_of:
                out.append((year, m))
                if len(out) >= count:
                    break
        year += 1
    return out


def generate_quarterly_futures(
    product_symbol: str, as_of: date, count: int = 4
) -> List[FuturesContract]:
    """Build the standard set of upcoming ES/NQ quarterly dated contracts.

    Deterministic and side-effect free — used by the mock provider and by
    tests.  The nearest-expiry contract is flagged front, the next is flagged
    next; ``vendor_symbol`` uses the exchange dated label (e.g. ``ESZ25``).
    """
    product = get_product(product_symbol)
    if product is None:
        raise KeyError(f"Unknown futures product: {product_symbol!r}")

    contracts: List[FuturesContract] = []
    for idx, (year, month) in enumerate(_next_quarterly_months(as_of, count)):
        exp = quarterly_expiration_instant(year, month)
        label = contract_label(product.symbol, year, month)
        contracts.append(
            FuturesContract(
                product_symbol=product.symbol,
                contract_month=month,
                contract_year=year,
                expiration=exp,
                multiplier=product.multiplier,
                tick_size=product.tick_size,
                currency=product.currency,
                exchange=product.exchange,
                exchange_symbol=label,
                vendor_symbol=label,
                last_trade_date=exp,
                settlement_date=exp,
                status=ContractStatus.ACTIVE,
                is_front_contract=(idx == 0),
                is_next_contract=(idx == 1),
                contract_id=label,
            )
        )
    return contracts


def generate_quarterly_option_series(
    product_symbol: str, underlying: FuturesContract
) -> FuturesOptionSeries:
    """The standard quarterly option series settling into ``underlying``.

    Quarterly ES/NQ options are AM-settled European-style options that expire
    with (and settle into) the quarterly future.  Additional weekly/EOM roots
    exist but are not generated here — the mock provider focuses on the
    quarterly cycle the resolver keys on.
    """
    product = get_product(product_symbol)
    assert product is not None
    return FuturesOptionSeries(
        product_symbol=product.symbol,
        option_root=product.symbol,  # quarterly root == product root
        expiration=underlying.expiration,
        underlying_contract_id=underlying.contract_id or underlying.label,
        multiplier=product.multiplier,
        listing_type=OptionListingType.QUARTERLY,
        exercise_style=ExerciseStyle.EUROPEAN,
        settlement_type=SettlementType.FUTURES,
        currency=product.currency,
        vendor_symbol=f"{product.symbol}O:{underlying.label}",
    )


def generate_option_contracts(
    series: FuturesOptionSeries,
    underlying: FuturesContract,
    *,
    center: float,
    width: int = 20,
    step: float = 25.0,
) -> List[FuturesOptionContract]:
    """A symmetric put/call strike ladder around ``center`` for a series.

    Strikes only — no prices or OI are invented (those come from a real market
    feed).  ``step`` defaults to the common 25-point ES strike spacing.
    """
    contracts: List[FuturesOptionContract] = []
    for i in range(-width, width + 1):
        strike = round(center + i * step, 4)
        if strike <= 0:
            continue
        for pc in (PutCall.CALL, PutCall.PUT):
            contracts.append(
                FuturesOptionContract(
                    series_id=series.series_id or "",
                    product_symbol=series.product_symbol,
                    strike=strike,
                    put_call=pc,
                    expiration=series.expiration,
                    underlying_contract_id=series.underlying_contract_id,
                    multiplier=series.multiplier,
                    exercise_style=series.exercise_style,
                    settlement_type=series.settlement_type,
                    vendor_contract_id=(
                        f"{series.option_root}:{underlying.label}:{pc.value}:{strike:g}"
                    ),
                )
            )
    return contracts


# ---------------------------------------------------------------------------
# Provider protocol + mock
# ---------------------------------------------------------------------------


class FuturesSecurityDefinitionProvider(Protocol):
    """Vendor-neutral async interface for fetching security definitions."""

    async def fetch_products(self) -> Sequence[FuturesProduct]: ...

    async def fetch_futures_contracts(
        self, product_symbol: str, as_of: Optional[date] = None
    ) -> Sequence[FuturesContract]: ...

    async def fetch_option_series(
        self, product_symbol: str, as_of: Optional[date] = None
    ) -> Sequence[FuturesOptionSeries]: ...

    async def fetch_option_contracts(self, series_id: str) -> Sequence[FuturesOptionContract]: ...


class MockFuturesSecurityDefinitionProvider:
    """Deterministic fixture provider — NO real feed access.

    Generates the standard quarterly contract set for ES/NQ around a fixed
    ``as_of`` date and a per-product reference price (for the option strike
    ladder).  Intended for tests, local dev, and demonstrating the ingestion
    pipeline end-to-end without licensed CME data.
    """

    def __init__(
        self,
        as_of: Optional[date] = None,
        reference_prices: Optional[dict[str, float]] = None,
        contracts_per_product: int = 4,
    ) -> None:
        self.as_of = as_of or date(2026, 1, 5)
        self.reference_prices = reference_prices or {"ES": 6000.0, "NQ": 21500.0}
        self.contracts_per_product = contracts_per_product
        self._series_index: dict[str, tuple[FuturesOptionSeries, FuturesContract]] = {}
        self._build()

    def _build(self) -> None:
        self._futures: dict[str, List[FuturesContract]] = {}
        self._series: dict[str, List[FuturesOptionSeries]] = {}
        for product in all_products():
            futures = generate_quarterly_futures(
                product.symbol, self.as_of, self.contracts_per_product
            )
            self._futures[product.symbol] = futures
            series_list: List[FuturesOptionSeries] = []
            for fut in futures:
                series = generate_quarterly_option_series(product.symbol, fut)
                series_list.append(series)
                self._series_index[series.series_id or ""] = (series, fut)
            self._series[product.symbol] = series_list

    async def fetch_products(self) -> Sequence[FuturesProduct]:
        return all_products()

    async def fetch_futures_contracts(
        self, product_symbol: str, as_of: Optional[date] = None
    ) -> Sequence[FuturesContract]:
        return list(self._futures.get(product_symbol.strip().upper(), []))

    async def fetch_option_series(
        self, product_symbol: str, as_of: Optional[date] = None
    ) -> Sequence[FuturesOptionSeries]:
        return list(self._series.get(product_symbol.strip().upper(), []))

    async def fetch_option_contracts(self, series_id: str) -> Sequence[FuturesOptionContract]:
        entry = self._series_index.get(series_id)
        if entry is None:
            return []
        series, underlying = entry
        center = self.reference_prices.get(series.product_symbol, 6000.0)
        return generate_option_contracts(series, underlying, center=center)


# ---------------------------------------------------------------------------
# Idempotent persistence (cursor-based, psycopg2 style)
# ---------------------------------------------------------------------------

UPSERT_PRODUCT_SQL = """
INSERT INTO futures_products
    (product_symbol, name, exchange, currency, multiplier, tick_size, family, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (product_symbol) DO UPDATE SET
    name = EXCLUDED.name,
    exchange = EXCLUDED.exchange,
    currency = EXCLUDED.currency,
    multiplier = EXCLUDED.multiplier,
    tick_size = EXCLUDED.tick_size,
    family = EXCLUDED.family,
    updated_at = NOW()
"""

UPSERT_CONTRACT_SQL = """
INSERT INTO futures_contracts
    (contract_id, product_symbol, exchange_symbol, vendor_symbol, contract_month,
     contract_year, expiration, first_trade_date, last_trade_date, settlement_date,
     multiplier, tick_size, currency, status, volume, open_interest,
     is_front_contract, is_next_contract, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (contract_id) DO UPDATE SET
    exchange_symbol = EXCLUDED.exchange_symbol,
    vendor_symbol = EXCLUDED.vendor_symbol,
    expiration = EXCLUDED.expiration,
    first_trade_date = EXCLUDED.first_trade_date,
    last_trade_date = EXCLUDED.last_trade_date,
    settlement_date = EXCLUDED.settlement_date,
    multiplier = EXCLUDED.multiplier,
    tick_size = EXCLUDED.tick_size,
    status = EXCLUDED.status,
    volume = COALESCE(EXCLUDED.volume, futures_contracts.volume),
    open_interest = COALESCE(EXCLUDED.open_interest, futures_contracts.open_interest),
    is_front_contract = EXCLUDED.is_front_contract,
    is_next_contract = EXCLUDED.is_next_contract,
    updated_at = NOW()
"""

UPSERT_OPTION_SERIES_SQL = """
INSERT INTO futures_option_series
    (series_id, product_symbol, option_root, vendor_symbol, expiration,
     underlying_contract_id, multiplier, exercise_style, settlement_type,
     listing_type, status, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (series_id) DO UPDATE SET
    vendor_symbol = EXCLUDED.vendor_symbol,
    expiration = EXCLUDED.expiration,
    underlying_contract_id = EXCLUDED.underlying_contract_id,
    multiplier = EXCLUDED.multiplier,
    exercise_style = EXCLUDED.exercise_style,
    settlement_type = EXCLUDED.settlement_type,
    listing_type = EXCLUDED.listing_type,
    status = EXCLUDED.status,
    updated_at = NOW()
"""

UPSERT_OPTION_CONTRACT_SQL = """
INSERT INTO futures_option_contracts
    (vendor_contract_id, series_id, product_symbol, strike, put_call, expiration,
     underlying_contract_id, multiplier, exercise_style, settlement_type, status,
     open_interest, volume, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (vendor_contract_id) DO UPDATE SET
    series_id = EXCLUDED.series_id,
    strike = EXCLUDED.strike,
    put_call = EXCLUDED.put_call,
    expiration = EXCLUDED.expiration,
    underlying_contract_id = EXCLUDED.underlying_contract_id,
    multiplier = EXCLUDED.multiplier,
    exercise_style = EXCLUDED.exercise_style,
    settlement_type = EXCLUDED.settlement_type,
    status = EXCLUDED.status,
    open_interest = COALESCE(EXCLUDED.open_interest, futures_option_contracts.open_interest),
    volume = COALESCE(EXCLUDED.volume, futures_option_contracts.volume),
    updated_at = NOW()
"""


def upsert_products(cursor: Any, products: Iterable[FuturesProduct]) -> int:
    rows = [
        (p.symbol, p.name, p.exchange, p.currency, p.multiplier, p.tick_size, p.family)
        for p in products
    ]
    if not rows:
        return 0
    cursor.executemany(UPSERT_PRODUCT_SQL, rows)
    return len(rows)


def upsert_futures_contracts(cursor: Any, contracts: Iterable[FuturesContract]) -> int:
    rows = [
        (
            c.contract_id,
            c.product_symbol,
            c.exchange_symbol,
            c.vendor_symbol,
            c.contract_month,
            c.contract_year,
            c.expiration,
            c.first_trade_date,
            c.last_trade_date,
            c.settlement_date,
            c.multiplier,
            c.tick_size,
            c.currency,
            c.status.value,
            c.volume,
            c.open_interest,
            c.is_front_contract,
            c.is_next_contract,
        )
        for c in contracts
    ]
    if not rows:
        return 0
    cursor.executemany(UPSERT_CONTRACT_SQL, rows)
    return len(rows)


def upsert_option_series(cursor: Any, series: Iterable[FuturesOptionSeries]) -> int:
    rows = [
        (
            s.series_id,
            s.product_symbol,
            s.option_root,
            s.vendor_symbol,
            s.expiration,
            s.underlying_contract_id,
            s.multiplier,
            s.exercise_style.value,
            s.settlement_type.value,
            s.listing_type.value,
            s.status.value,
        )
        for s in series
    ]
    if not rows:
        return 0
    cursor.executemany(UPSERT_OPTION_SERIES_SQL, rows)
    return len(rows)


def upsert_option_contracts(cursor: Any, contracts: Iterable[FuturesOptionContract]) -> int:
    rows = [
        (
            c.vendor_contract_id,
            c.series_id,
            c.product_symbol,
            c.strike,
            c.put_call.value,
            c.expiration,
            c.underlying_contract_id,
            c.multiplier,
            c.exercise_style.value,
            c.settlement_type.value,
            c.status.value,
            c.open_interest,
            c.volume,
        )
        for c in contracts
    ]
    if not rows:
        return 0
    cursor.executemany(UPSERT_OPTION_CONTRACT_SQL, rows)
    return len(rows)
