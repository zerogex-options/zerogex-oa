"""Futures-specific backtest P&L economics (foundation).

The existing backtester (``src/backtesting/engine.py``) hardcodes the equity
option multiplier ``× 100`` and prices share-based option round trips.  Futures
P&L is fundamentally different: it uses the contract **point value / multiplier**
($50 ES, $20 NQ), transaction costs are **per contract** (commission + exchange
fee, round trip), and slippage is naturally expressed in **ticks** (0.25 pts →
$12.50 ES / $5.00 NQ).  This module provides that economics layer as pure,
decimal-safe functions (spec Phase 13 / rule #15) so the backtest engine can
opt into futures math instead of equity-share assumptions.

It is intentionally standalone and fully unit-tested; wiring it into the live
backtest runner (and gating it on real historical CME data availability) is a
follow-up — a futures backtest must not be generated from data that does not
exist (spec Phase 13).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum

from src.futures.contracts import tick_value as _tick_value_dollars


class TradeSide(str, Enum):
    LONG = "long"
    SHORT = "short"


def _d(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass(frozen=True)
class FuturesTradeEconomics:
    """Per-contract economics for a futures product."""

    symbol: str
    multiplier: int  # point value ($50 ES / $20 NQ)
    tick_size: float  # 0.25 for ES/NQ
    commission_per_contract: float = 0.0
    exchange_fee_per_contract: float = 0.0
    slippage_ticks: float = 0.0

    @property
    def tick_value(self) -> Decimal:
        return _tick_value_dollars(self.tick_size, self.multiplier)

    @classmethod
    def for_symbol(
        cls,
        symbol: str,
        *,
        commission_per_contract: float = 0.0,
        exchange_fee_per_contract: float = 0.0,
        slippage_ticks: float = 0.0,
    ) -> "FuturesTradeEconomics":
        """Build from the instrument registry's multiplier/tick metadata."""
        from src import instruments as reg

        inst = reg.get_instrument(symbol)
        if inst is None or not inst.is_future:
            raise ValueError(f"{symbol!r} is not a registered futures instrument")
        return cls(
            symbol=inst.symbol,
            multiplier=inst.contract_multiplier,
            tick_size=inst.price_increment or 0.25,
            commission_per_contract=commission_per_contract,
            exchange_fee_per_contract=exchange_fee_per_contract,
            slippage_ticks=slippage_ticks,
        )


@dataclass(frozen=True)
class FuturesPnLResult:
    symbol: str
    side: TradeSide
    contracts: int
    entry_price: float
    exit_price: float
    gross_points: Decimal
    gross_pnl: Decimal
    slippage_cost: Decimal
    fees: Decimal
    net_pnl: Decimal

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "side": self.side.value,
            "contracts": self.contracts,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "gross_points": float(self.gross_points),
            "gross_pnl": float(self.gross_pnl),
            "slippage_cost": float(self.slippage_cost),
            "fees": float(self.fees),
            "net_pnl": float(self.net_pnl),
        }


def _q(value: Decimal) -> Decimal:
    """Quantize to cents."""
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compute_futures_pnl(
    *,
    entry_price: float,
    exit_price: float,
    contracts: int,
    side: TradeSide,
    econ: FuturesTradeEconomics,
) -> FuturesPnLResult:
    """Decimal-safe futures trade P&L.

    P&L = signed point move × multiplier × contracts, minus round-trip
    per-contract fees and tick-denominated slippage on both fills.
    """
    if contracts <= 0:
        raise ValueError("contracts must be positive")
    direction = Decimal(1) if side is TradeSide.LONG else Decimal(-1)
    gross_points = (_d(exit_price) - _d(entry_price)) * direction
    mult = _d(econ.multiplier)
    n = _d(contracts)

    gross_pnl = gross_points * mult * n
    # Slippage applies to entry AND exit (2 fills), in tick-value dollars.
    slippage_cost = _d(econ.slippage_ticks) * econ.tick_value * n * Decimal(2)
    fees = (_d(econ.commission_per_contract) + _d(econ.exchange_fee_per_contract)) * n * Decimal(2)
    net_pnl = gross_pnl - slippage_cost - fees

    return FuturesPnLResult(
        symbol=econ.symbol,
        side=side,
        contracts=contracts,
        entry_price=entry_price,
        exit_price=exit_price,
        gross_points=gross_points,
        gross_pnl=_q(gross_pnl),
        slippage_cost=_q(slippage_cost),
        fees=_q(fees),
        net_pnl=_q(net_pnl),
    )
