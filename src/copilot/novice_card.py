"""Novice Card — plain-English wrapper around the internal ``ActionCard``.

The Playbook Engine emits ``ActionCard``s for the internal trade-execution
pipeline. The Copilot surface needs a slightly different shape: dollar
risk in human units, plain-English thesis, what-could-go-wrong, and an
explicit invalidation clause. Rather than bloat ``ActionCard`` with
UX fields, we wrap.

See ``docs/design/gex_copilot_architecture.md`` §4 for the full spec.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from src.signals.playbook.types import ActionCard, ActionEnum

from .regime_narrative import RegimeNarrative


# Default novice account size in dollars. The user can override via their
# profile. Risk cap is a hard percent of this number.
DEFAULT_ACCOUNT_SIZE_USD = 10_000.0
MAX_RISK_PCT_OF_ACCOUNT = 0.01  # 1% — hard cap, never override

# A pattern is treated as "experimental" until it has at least this many
# closed trades in the rolling window.
MIN_SAMPLE_FOR_PROVEN = 10


# ---------------------------------------------------------------------------
# Card status enum
# ---------------------------------------------------------------------------


STATUS_ACTIVE = "ACTIVE"
STATUS_FILLED = "FILLED"
STATUS_STOPPED = "STOPPED"
STATUS_TARGET_HIT = "TARGET_HIT"
STATUS_INVALIDATED = "INVALIDATED"
STATUS_EXPIRED = "EXPIRED"


# ---------------------------------------------------------------------------
# Card schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NoviceCard:
    """A NoviceCard is the Copilot's atomic answer to "what should I do".

    It carries the underlying ``ActionCard`` verbatim plus the novice-only
    fields the chat surface and the UI need. Storage layer persists this
    flat — see spec §4.5.
    """

    # Identity ----------------------------------------------------------
    card_id: str
    action_card: ActionCard
    regime: RegimeNarrative

    # Plain-English ----------------------------------------------------
    one_line_thesis: str
    what_could_go_wrong: str
    invalidation: str

    # Dollar-unit risk -------------------------------------------------
    account_size: float
    risk_dollars: float
    risk_pct_of_account: float
    target_dollars: float
    payoff_ratio: float

    # Credibility ------------------------------------------------------
    historical_hit_rate: Optional[float]
    historical_sample_size: int
    historical_avg_winner_pct: Optional[float]
    historical_avg_loser_pct: Optional[float]
    experimental: bool

    # Lifecycle --------------------------------------------------------
    emitted_at: datetime
    expires_at: datetime
    status: str = STATUS_ACTIVE
    realized_pnl_dollars: Optional[float] = None
    closed_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "card_id": self.card_id,
            "status": self.status,
            "regime": self.regime.to_dict(),
            "action_card": self.action_card.to_dict(),
            "thesis": {
                "one_line": self.one_line_thesis,
                "what_could_go_wrong": self.what_could_go_wrong,
                "invalidation": self.invalidation,
            },
            "risk": {
                "account_size": self.account_size,
                "risk_dollars": round(self.risk_dollars, 2),
                "risk_pct_of_account": round(self.risk_pct_of_account, 4),
                "target_dollars": round(self.target_dollars, 2),
                "payoff_ratio": round(self.payoff_ratio, 2),
            },
            "credibility": {
                "historical_hit_rate": self.historical_hit_rate,
                "historical_sample_size": self.historical_sample_size,
                "historical_avg_winner_pct": self.historical_avg_winner_pct,
                "historical_avg_loser_pct": self.historical_avg_loser_pct,
                "experimental": self.experimental,
            },
            "emitted_at": self.emitted_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "realized_pnl_dollars": self.realized_pnl_dollars,
        }


# ---------------------------------------------------------------------------
# Pattern-history bundle (read-side input to the wrapper)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PatternHistory:
    """Aggregated outcomes for a pattern over the lookback window.

    Populated by the caller from ``signal_trades``. Kept as a value type
    so the wrapper stays a pure function and is unit-testable.
    """

    pattern_id: str
    window_days: int
    sample_size: int
    hit_rate: Optional[float]
    avg_winner_pct: Optional[float]
    avg_loser_pct: Optional[float]

    @classmethod
    def empty(cls, pattern_id: str, window_days: int = 90) -> "PatternHistory":
        return cls(
            pattern_id=pattern_id,
            window_days=window_days,
            sample_size=0,
            hit_rate=None,
            avg_winner_pct=None,
            avg_loser_pct=None,
        )


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


def wrap_action_card(
    action: ActionCard,
    regime: RegimeNarrative,
    *,
    history: PatternHistory,
    account_size: float = DEFAULT_ACCOUNT_SIZE_USD,
) -> NoviceCard:
    """Project an ``ActionCard`` into a ``NoviceCard``.

    Pure function. Caller is responsible for resolving ``PatternHistory``
    from the ``signal_trades`` table and persisting the result.
    """

    emitted_at = _ensure_utc(action.timestamp)
    expires_at = _expiry(emitted_at, action)

    if action.action == ActionEnum.STAND_DOWN:
        return _wrap_stand_down(action, regime, history, account_size, emitted_at, expires_at)

    risk_dollars, target_dollars = _risk_target_dollars(action)
    risk_pct = risk_dollars / account_size if account_size > 0 else 0.0
    payoff_ratio = target_dollars / risk_dollars if risk_dollars > 0 else 0.0

    return NoviceCard(
        card_id=str(uuid.uuid4()),
        action_card=action,
        regime=regime,
        one_line_thesis=_compose_thesis(action, regime),
        what_could_go_wrong=_compose_risks(action, regime),
        invalidation=_compose_invalidation(action),
        account_size=account_size,
        risk_dollars=risk_dollars,
        risk_pct_of_account=risk_pct,
        target_dollars=target_dollars,
        payoff_ratio=payoff_ratio,
        historical_hit_rate=history.hit_rate,
        historical_sample_size=history.sample_size,
        historical_avg_winner_pct=history.avg_winner_pct,
        historical_avg_loser_pct=history.avg_loser_pct,
        experimental=history.sample_size < MIN_SAMPLE_FOR_PROVEN,
        emitted_at=emitted_at,
        expires_at=expires_at,
        status=STATUS_ACTIVE,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _expiry(emitted_at: datetime, action: ActionCard) -> datetime:
    minutes = action.max_hold_minutes or 90
    return emitted_at + timedelta(minutes=minutes)


def _risk_target_dollars(action: ActionCard) -> tuple[float, float]:
    """Compute dollar risk and dollar target from the card's premiums.

    Single-leg debits: risk = entry_premium - stop_exit_premium; target =
    target_exit_premium - entry_premium, both per contract × 100 ×
    suggested 1 contract.

    Spreads: equivalent calculation on net premium.

    When premiums aren't populated, fall back to a level-distance proxy.
    """
    entry = action.entry
    target = action.target
    stop = action.stop

    if entry and target and stop and entry.limit_premium is not None:
        entry_prem = float(entry.limit_premium)
        target_prem = float(target.exit_premium) if target.exit_premium is not None else entry_prem
        stop_prem = float(stop.exit_premium) if stop.exit_premium is not None else 0.0

        per_contract_risk = max(entry_prem - stop_prem, 0.0) * 100.0
        per_contract_target = max(target_prem - entry_prem, 0.0) * 100.0

        contracts = sum(max(leg.qty, 1) for leg in action.legs) or 1
        return per_contract_risk * contracts, per_contract_target * contracts

    if entry and target and stop and entry.ref_price is not None:
        entry_p = float(entry.ref_price)
        target_p = float(target.ref_price) if target.ref_price is not None else entry_p
        stop_p = float(stop.ref_price) if stop.ref_price is not None else entry_p
        contracts = sum(max(leg.qty, 1) for leg in action.legs) or 1
        return abs(entry_p - stop_p) * 100.0 * contracts, abs(target_p - entry_p) * 100.0 * contracts

    return 0.0, 0.0


def _compose_thesis(action: ActionCard, regime: RegimeNarrative) -> str:
    """One sentence in plain English. Pulls directly from ActionCard.rationale
    when the engine populates it; otherwise composes from regime + pattern."""
    if action.rationale:
        return action.rationale.strip()
    return (
        f"Regime is {regime.label.replace('_', ' ').lower()}. The {action.pattern} "
        f"setup is firing, suggesting a {action.direction} bias."
    )


def _compose_risks(action: ActionCard, regime: RegimeNarrative) -> str:
    """One sentence naming the single biggest risk."""
    if regime.label == "LONG_GAMMA_PIN":
        return "If realized vol expands or price escapes the magnet level, the pin breaks and the trade reverses fast."
    if regime.label == "SHORT_GAMMA_TREND":
        return "A sudden vol-expansion or reversal back through the flip level can unwind the trend trade quickly."
    if regime.label == "VOL_EXPANSION":
        return "Wider candles cut both ways — stops can be triggered intrabar even when the eventual move agrees with the card."
    if regime.label == "VANNA_GLIDE":
        return "If VIX reverses, the vanna drag flips with it and the smooth drift breaks."
    if regime.label == "CHARM_DRIFT":
        return "A late-day news catalyst can interrupt the drift to max pain and strand 0DTE positions."
    return "Regime context is ambiguous — treat this card with reduced size."


def _compose_invalidation(action: ActionCard) -> str:
    """Where the card stops being valid as a thesis (vs. just hitting stop)."""
    stop = action.stop
    if stop and stop.level_name:
        return f"Card invalid if price closes through {stop.level_name}."
    if stop and stop.ref_price is not None:
        return f"Card invalid if price closes through {stop.ref_price:.2f}."
    return "Card invalid if the regime label changes within 30 minutes."


def _wrap_stand_down(
    action: ActionCard,
    regime: RegimeNarrative,
    history: PatternHistory,
    account_size: float,
    emitted_at: datetime,
    expires_at: datetime,
) -> NoviceCard:
    return NoviceCard(
        card_id=str(uuid.uuid4()),
        action_card=action,
        regime=regime,
        one_line_thesis="Nothing to trade right now — sit out.",
        what_could_go_wrong="Forcing a trade in this window has historically reduced expectancy.",
        invalidation="A new pattern firing on the next analytics cycle will replace this card.",
        account_size=account_size,
        risk_dollars=0.0,
        risk_pct_of_account=0.0,
        target_dollars=0.0,
        payoff_ratio=0.0,
        historical_hit_rate=None,
        historical_sample_size=history.sample_size,
        historical_avg_winner_pct=None,
        historical_avg_loser_pct=None,
        experimental=False,
        emitted_at=emitted_at,
        expires_at=expires_at,
        status=STATUS_ACTIVE,
    )
