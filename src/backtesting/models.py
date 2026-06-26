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


def _coerce_cooldown(value: Any) -> int:
    """Cooldown minutes, defaulting to the configured value when omitted/blank."""
    if value in (None, ""):
        from src import config

        return int(config.BACKTEST_SIGNAL_COOLDOWN_MINUTES)
    try:
        out = int(value)
    except (TypeError, ValueError):
        raise SpecError("cooldown_minutes must be an integer")
    return min(max(out, 0), 1440)


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
    # Greeks-aware caps (Phase 5b): cap |net position delta| / |net vega| per
    # trade (share-deltas and dollar-vega). None ⇒ that cap is off.
    max_net_delta: Optional[float] = None
    max_net_vega: Optional[float] = None

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "Sizing":
        raw = raw or {}
        try:
            max_concurrent = int(raw.get("max_concurrent", 3) or 3)
        except (TypeError, ValueError):
            raise SpecError("sizing.max_concurrent must be an integer")

        def _opt_positive(key: str) -> Optional[float]:
            v = raw.get(key)
            if v in (None, ""):
                return None
            try:
                out = float(v)
            except (TypeError, ValueError):
                raise SpecError(f"sizing.{key} must be a number or null")
            return out if out > 0 else None

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
            max_net_delta=_opt_positive("max_net_delta"),
            max_net_vega=_opt_positive("max_net_vega"),
        )


def _coerce_opt_pct(value: Any, *, field_name: str, hi: float) -> Optional[float]:
    """Optional positive fraction (e.g. 0.5 = 50%); None/blank ⇒ disabled."""
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        raise SpecError(f"{field_name} must be a number or null")
    if out != out or out <= 0:  # NaN or non-positive ⇒ treat as disabled
        return None
    return min(out, hi)


@dataclass
class ExitRules:
    """Optional overrides for per-Card exit behavior.

    ``profit_target_pct`` / ``stop_loss_pct`` are an option-premium exit overlay
    (Phase 2): take profit when the option's premium rises ``profit_target_pct``
    above the entry fill, stop out when it falls ``stop_loss_pct`` below — both
    resolved on the option's own premium series, in addition to any underlying
    level target/stop the Card carries. ``None`` disables that side.
    """

    max_hold_minutes: Optional[int] = None
    profit_target_pct: Optional[float] = None
    stop_loss_pct: Optional[float] = None

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "ExitRules":
        raw = raw or {}
        mhm = raw.get("max_hold_minutes")
        if mhm in (None, ""):
            max_hold = None
        else:
            try:
                max_hold = max(1, min(int(mhm), 10_080))
            except (TypeError, ValueError):
                raise SpecError("exit.max_hold_minutes must be an integer or null")
        return cls(
            max_hold_minutes=max_hold,
            # Take-profit can exceed 100% (a 0DTE can multiply); stop caps at
            # 100% (the option can't lose more than its premium).
            profit_target_pct=_coerce_opt_pct(
                raw.get("profit_target_pct"), field_name="exit.profit_target_pct", hi=20.0
            ),
            stop_loss_pct=_coerce_opt_pct(
                raw.get("stop_loss_pct"), field_name="exit.stop_loss_pct", hi=1.0
            ),
        )


# ---------------------------------------------------------------------------
# Custom strategy builder (Phase 3)
#
# Numeric fields compare with </<=/>/>=/==/!=; categorical fields with ==/!=.
# Every field maps to a per-minute value materialized by
# ``src/backtesting/strategy.py`` from gex_summary ⋈ signal_scores ⋈
# underlying_quotes.
# ---------------------------------------------------------------------------
STRATEGY_NUMERIC_FIELDS = {
    "price", "net_gex", "net_gex_at_spot", "flip_distance", "flip_distance_pct",
    "gamma_flip_point", "call_wall", "put_wall", "dist_to_call_wall_pct",
    "dist_to_put_wall_pct", "put_call_ratio", "max_pain", "convexity_risk", "msi",
}
STRATEGY_CATEGORICAL_FIELDS = {
    "net_gex_sign": ("positive", "negative", "zero"),
    "msi_regime": ("trend_expansion", "controlled_trend", "chop_range", "high_risk_reversal"),
}
_NUMERIC_OPS = ("<", "<=", ">", ">=", "==", "!=")
_CATEGORICAL_OPS = ("==", "!=")


@dataclass
class Condition:
    """One AND-ed entry condition: ``field op value``."""

    field: str
    op: str
    value: object  # float for numeric fields, str for categorical

    @classmethod
    def from_dict(cls, raw: dict) -> "Condition":
        if not isinstance(raw, dict):
            raise SpecError("each condition must be an object")
        fld = str(raw.get("field") or "").strip()
        op = str(raw.get("op") or "").strip()
        if fld in STRATEGY_NUMERIC_FIELDS:
            if op not in _NUMERIC_OPS:
                raise SpecError(f"condition op for {fld!r} must be one of {_NUMERIC_OPS}")
            try:
                value: object = float(raw.get("value"))
            except (TypeError, ValueError):
                raise SpecError(f"condition value for {fld!r} must be a number")
        elif fld in STRATEGY_CATEGORICAL_FIELDS:
            if op not in _CATEGORICAL_OPS:
                raise SpecError(f"condition op for {fld!r} must be == or !=")
            value = str(raw.get("value") or "").strip()
            allowed = STRATEGY_CATEGORICAL_FIELDS[fld]
            if value not in allowed:
                raise SpecError(f"condition value for {fld!r} must be one of {allowed}")
        else:
            raise SpecError(f"unknown condition field {fld!r}")
        return cls(field=fld, op=op, value=value)

    def to_dict(self) -> dict:
        return {"field": self.field, "op": self.op, "value": self.value}


@dataclass
class StrategySpec:
    """A user-defined entry rule compiled into synthetic Action Cards.

    Fires a directional ATM entry on every bar where ALL ``conditions`` hold.
    Exits combine the optional underlying-level offsets here with the
    option-premium overlay on the top-level ``exit`` (whichever triggers first).
    """

    direction: str
    conditions: list[Condition]
    dte: int = 0
    structure: str = "single"        # single|vertical|straddle|strangle|condor
    width: float = 5.0               # vertical width / strangle & condor offset, pts
    wing: float = 5.0               # condor wing width (short→long strike), pts
    target_offset_pct: Optional[float] = None  # fraction of entry price, favorable
    stop_offset_pct: Optional[float] = None    # fraction of entry price, adverse

    # Defined-risk structures supported. Directional ones take a bullish/bearish
    # direction; neutral ones (straddle/strangle/condor) are non-directional.
    DIRECTIONAL = ("single", "vertical")
    NEUTRAL = ("straddle", "strangle", "condor")

    @classmethod
    def from_dict(cls, raw: dict) -> "StrategySpec":
        if not isinstance(raw, dict):
            raise SpecError("strategy must be an object")
        structure = str(raw.get("structure") or "single").strip().lower()
        if structure not in cls.DIRECTIONAL + cls.NEUTRAL:
            raise SpecError(
                "strategy.structure must be one of "
                f"{cls.DIRECTIONAL + cls.NEUTRAL}"
            )
        direction = str(raw.get("direction") or "").strip().lower()
        if structure in cls.NEUTRAL:
            # Non-directional structures are always neutral; accept an omitted or
            # explicit 'neutral' direction, reject a directional one.
            if direction in ("", "neutral"):
                direction = "neutral"
            else:
                raise SpecError(f"{structure} is non-directional; set direction 'neutral'")
        elif direction not in ("bullish", "bearish"):
            raise SpecError(f"{structure} requires direction 'bullish' or 'bearish'")
        conds_raw = raw.get("conditions") or []
        if not isinstance(conds_raw, list) or not conds_raw:
            raise SpecError("strategy.conditions must be a non-empty list")
        conditions = [Condition.from_dict(c) for c in conds_raw]
        entry = raw.get("entry") or {}
        try:
            dte = int(entry.get("dte", 0) or 0)
        except (TypeError, ValueError):
            raise SpecError("strategy.entry.dte must be an integer")

        def _pos_pts(key: str, default: float) -> float:
            v = raw.get(key)
            if v in (None, ""):
                return default
            try:
                out = float(v)
            except (TypeError, ValueError):
                raise SpecError(f"strategy.{key} must be a number")
            if out <= 0:
                raise SpecError(f"strategy.{key} must be positive")
            return out

        return cls(
            direction=direction,
            conditions=conditions,
            dte=min(max(dte, 0), 30),
            structure=structure,
            width=_pos_pts("width", 5.0),
            wing=_pos_pts("wing", 5.0),
            target_offset_pct=_coerce_opt_pct(
                raw.get("target_offset_pct"), field_name="strategy.target_offset_pct", hi=1.0
            ),
            stop_offset_pct=_coerce_opt_pct(
                raw.get("stop_offset_pct"), field_name="strategy.stop_offset_pct", hi=1.0
            ),
        )

    def to_dict(self) -> dict:
        return {
            "direction": self.direction,
            "conditions": [c.to_dict() for c in self.conditions],
            "entry": {"dte": self.dte},
            "structure": self.structure,
            "width": self.width,
            "wing": self.wing,
            "target_offset_pct": self.target_offset_pct,
            "stop_offset_pct": self.stop_offset_pct,
        }


@dataclass
class BacktestSpec:
    """A fully-validated backtest request.

    Either ``patterns`` (built-in playbook patterns) OR ``strategy`` (a custom
    condition rule) selects the entries; ``strategy`` takes precedence.
    """

    underlying: str
    start_date: date
    end_date: date
    patterns: list[str] = field(default_factory=list)
    fill_model: FillModel = field(default_factory=FillModel)
    sizing: Sizing = field(default_factory=Sizing)
    exit: ExitRules = field(default_factory=ExitRules)
    # Minimum minutes between consecutive entries of the same pattern. Defaults
    # to BACKTEST_SIGNAL_COOLDOWN_MINUTES; 0 prices every card. Collapses the
    # continuous Action-Card stream into discrete trades.
    cooldown_minutes: int = 0
    strategy: Optional[StrategySpec] = None
    # Pattern-mode structure override (ignored when ``strategy`` is set, which
    # carries its own structure). "single" prices each Card's own legs; "vertical"
    # reshapes the directional Card into a long-ATM/short-OTM debit spread, the
    # short strike ``width_pct`` of the entry price out. Used by the structure-
    # aware calibration feed to measure a pattern as a defined-risk spread.
    structure: str = "single"
    width_pct: float = 0.01

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
        strategy = StrategySpec.from_dict(raw["strategy"]) if raw.get("strategy") else None
        exit_rules = ExitRules.from_dict(raw.get("exit"))
        structure = str(raw.get("structure") or "single").strip().lower()
        if structure not in ("single", "vertical"):
            raise SpecError("structure must be 'single' or 'vertical'")
        width_pct = _coerce_float(
            raw.get("width_pct"), field_name="width_pct", lo=0.001, hi=0.2, default=0.01
        )
        if strategy is not None:
            has_level = (
                strategy.target_offset_pct is not None or strategy.stop_offset_pct is not None
            )
            has_premium = (
                exit_rules.profit_target_pct is not None or exit_rules.stop_loss_pct is not None
            )
            if strategy.direction == "neutral" and not has_premium:
                # Level offsets are directional; neutral structures exit on the
                # premium overlay (and/or the time stop) only.
                raise SpecError(
                    f"a {strategy.structure} is non-directional — set an exit via "
                    "exit.profit_target_pct / stop_loss_pct (level offsets don't apply)"
                )
            if not has_level and not has_premium:
                raise SpecError(
                    "a custom strategy needs an exit: set strategy.target_offset_pct / "
                    "stop_offset_pct, or exit.profit_target_pct / stop_loss_pct"
                )
        return cls(
            underlying=underlying,
            start_date=start,
            end_date=end,
            patterns=patterns,
            fill_model=FillModel.from_dict(raw.get("fill_model")),
            sizing=Sizing.from_dict(raw.get("sizing")),
            exit=exit_rules,
            cooldown_minutes=_coerce_cooldown(raw.get("cooldown_minutes")),
            strategy=strategy,
            structure=structure,
            width_pct=width_pct,
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
                "max_net_delta": self.sizing.max_net_delta,
                "max_net_vega": self.sizing.max_net_vega,
            },
            "exit": {
                "max_hold_minutes": self.exit.max_hold_minutes,
                "profit_target_pct": self.exit.profit_target_pct,
                "stop_loss_pct": self.exit.stop_loss_pct,
            },
            "cooldown_minutes": self.cooldown_minutes,
            "strategy": self.strategy.to_dict() if self.strategy else None,
            "structure": self.structure,
            "width_pct": self.width_pct,
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
    structure: str = "single"
    legs: list = field(default_factory=list)
    net_delta: float = 0.0
    net_vega: float = 0.0

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "pattern": self.pattern,
            "direction": self.direction,
            "tier": self.tier,
            "structure": self.structure,
            "legs": self.legs,
            "net_delta": round(self.net_delta, 1),
            "net_vega": round(self.net_vega, 2),
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
