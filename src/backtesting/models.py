"""Typed request/result models for the backtesting engine.

These are plain dataclasses (not pydantic) so the engine can be imported and
unit-tested without pulling in the API stack. The API router validates the raw
request body and constructs a :class:`BacktestSpec` via :meth:`from_dict`,
raising :class:`SpecError` with a human-readable message on bad input.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional


class SpecError(ValueError):
    """Raised when a submitted BacktestSpec is structurally invalid."""


# Hard caps keep a single customer request from scanning an unbounded window or
# requesting absurd capital. Tuned to the 90-day live retention horizon.
_MAX_WINDOW_DAYS = 180
_MIN_CAPITAL = 500.0
_MAX_CAPITAL = 10_000_000.0


def _coerce_float(value: Any, *, field_name: str, lo: float, hi: float, default: float) -> float:
    if value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        raise SpecError(f"{field_name} must be a number")
    if out != out:  # NaN
        raise SpecError(f"{field_name} must be a number")
    return min(max(out, lo), hi)


def _coerce_date(value: Any, *, field_name: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise SpecError(f"{field_name} must be an ISO date (YYYY-MM-DD)")
    raise SpecError(f"{field_name} is required (YYYY-MM-DD)")


@dataclass
class FillModel:
    """Realistic-fill parameters applied per leg.

    ``slippage_pct`` widens each side of the quote symmetrically (the same
    knob ``src/signals/execution.py`` uses); ``commission_per_contract`` is
    charged once per contract on entry AND once on exit (round trip).
    """

    slippage_pct: float = 0.01
    commission_per_contract: float = 0.65

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "FillModel":
        raw = raw or {}
        return cls(
            slippage_pct=_coerce_float(
                raw.get("slippage_pct"), field_name="fill_model.slippage_pct",
                lo=0.0, hi=0.25, default=0.01,
            ),
            commission_per_contract=_coerce_float(
                raw.get("commission_per_contract"),
                field_name="fill_model.commission_per_contract",
                lo=0.0, hi=25.0, default=0.65,
            ),
        )


@dataclass
class Sizing:
    """Position-sizing rules.

    ``risk_per_trade_pct`` of *running realized equity* is allocated as premium
    per trade; ``max_concurrent`` caps simultaneously-open positions.
    """

    capital: float = 25_000.0
    risk_per_trade_pct: float = 2.0
    max_concurrent: int = 3

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "Sizing":
        raw = raw or {}
        try:
            max_concurrent = int(raw.get("max_concurrent", 3) or 3)
        except (TypeError, ValueError):
            raise SpecError("sizing.max_concurrent must be an integer")
        return cls(
            capital=_coerce_float(
                raw.get("capital"), field_name="sizing.capital",
                lo=_MIN_CAPITAL, hi=_MAX_CAPITAL, default=25_000.0,
            ),
            risk_per_trade_pct=_coerce_float(
                raw.get("risk_per_trade_pct"), field_name="sizing.risk_per_trade_pct",
                lo=0.1, hi=100.0, default=2.0,
            ),
            max_concurrent=min(max(max_concurrent, 1), 20),
        )


@dataclass
class ExitRules:
    """Optional overrides for per-Card exit behavior."""

    max_hold_minutes: Optional[int] = None

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "ExitRules":
        raw = raw or {}
        mhm = raw.get("max_hold_minutes")
        if mhm in (None, ""):
            return cls(max_hold_minutes=None)
        try:
            val = int(mhm)
        except (TypeError, ValueError):
            raise SpecError("exit.max_hold_minutes must be an integer or null")
        return cls(max_hold_minutes=max(1, min(val, 10_080)))


@dataclass
class BacktestSpec:
    """A fully-validated backtest request."""

    underlying: str
    start_date: date
    end_date: date
    patterns: list[str] = field(default_factory=list)
    fill_model: FillModel = field(default_factory=FillModel)
    sizing: Sizing = field(default_factory=Sizing)
    exit: ExitRules = field(default_factory=ExitRules)

    @classmethod
    def from_dict(cls, raw: dict) -> "BacktestSpec":
        if not isinstance(raw, dict):
            raise SpecError("request body must be a JSON object")
        underlying = str(raw.get("underlying") or "").strip().upper()
        if not underlying:
            raise SpecError("underlying is required")
        start = _coerce_date(raw.get("start_date"), field_name="start_date")
        end = _coerce_date(raw.get("end_date"), field_name="end_date")
        if end < start:
            raise SpecError("end_date must be on or after start_date")
        if (end - start).days > _MAX_WINDOW_DAYS:
            raise SpecError(f"window may not exceed {_MAX_WINDOW_DAYS} days")
        patterns_raw = raw.get("patterns") or []
        if not isinstance(patterns_raw, list):
            raise SpecError("patterns must be a list")
        patterns = [str(p).strip() for p in patterns_raw if str(p).strip()]
        return cls(
            underlying=underlying,
            start_date=start,
            end_date=end,
            patterns=patterns,
            fill_model=FillModel.from_dict(raw.get("fill_model")),
            sizing=Sizing.from_dict(raw.get("sizing")),
            exit=ExitRules.from_dict(raw.get("exit")),
        )

    def to_dict(self) -> dict:
        return {
            "underlying": self.underlying,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "patterns": list(self.patterns),
            "fill_model": {
                "slippage_pct": self.fill_model.slippage_pct,
                "commission_per_contract": self.fill_model.commission_per_contract,
            },
            "sizing": {
                "capital": self.sizing.capital,
                "risk_per_trade_pct": self.sizing.risk_per_trade_pct,
                "max_concurrent": self.sizing.max_concurrent,
            },
            "exit": {"max_hold_minutes": self.exit.max_hold_minutes},
        }


@dataclass
class TradeResult:
    """One simulated round-trip trade."""

    seq: int
    pattern: str
    direction: str
    tier: str
    option_symbol: str
    option_type: str
    strike: Optional[float]
    expiration: Optional[date]
    entered_at: datetime
    exited_at: Optional[datetime]
    entry_premium: float
    exit_premium: Optional[float]
    contracts: int
    gross_pnl: float
    commission: float
    net_pnl: float
    return_pct: Optional[float]
    outcome: str
    mfe_pct: Optional[float]
    mae_pct: Optional[float]
    hold_minutes: Optional[int]

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "pattern": self.pattern,
            "direction": self.direction,
            "tier": self.tier,
            "option_symbol": self.option_symbol,
            "option_type": self.option_type,
            "strike": float(self.strike) if self.strike is not None else None,
            "expiration": self.expiration.isoformat() if self.expiration else None,
            "entered_at": self.entered_at.isoformat(),
            "exited_at": self.exited_at.isoformat() if self.exited_at else None,
            "entry_premium": round(self.entry_premium, 4),
            "exit_premium": round(self.exit_premium, 4) if self.exit_premium is not None else None,
            "contracts": self.contracts,
            "gross_pnl": round(self.gross_pnl, 2),
            "commission": round(self.commission, 2),
            "net_pnl": round(self.net_pnl, 2),
            "return_pct": round(self.return_pct, 2) if self.return_pct is not None else None,
            "outcome": self.outcome,
            "hold_minutes": self.hold_minutes,
        }


@dataclass
class EquityPoint:
    t: datetime
    equity: float
    drawdown_pct: float


@dataclass
class RunResult:
    """The full output of an engine run, ready to persist."""

    trades: list[TradeResult]
    equity: list[EquityPoint]
    summary: dict
