"""Futures contract resolver and roll logic.

The continuous symbol ``ES`` must always resolve to a *dated* contract, and
analytics must be attributable to that dated contract — options referencing
different maturities are never blindly co-aggregated on one price axis (spec
Phase 3).  This module answers "which dated contract is ES right now?" under
a configurable roll policy, and describes the roll state for display.

The resolver is deliberately **pure and synchronous**: it operates on a list
of :class:`~src.futures.contracts.FuturesContract` already fetched from a
provider (or the DB), so it is trivially unit-testable across quarterly
rolls without any I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Sequence

from src.futures.contracts import FuturesContract, FuturesOptionContract


class RollPolicy(str, Enum):
    #: Roll a fixed number of days before the front contract's expiration.
    CALENDAR = "calendar"
    #: Roll when the next contract's volume overtakes the front's.
    VOLUME = "volume"
    #: Roll when the next contract's open interest overtakes the front's.
    OPEN_INTEREST = "open_interest"
    #: Default: within the calendar window, roll as soon as EITHER volume or
    #: OI crosses over; always roll by the hard fallback near expiry so a roll
    #: happens even when volume/OI data is unavailable.
    HYBRID = "hybrid"


@dataclass(frozen=True)
class RollState:
    product: str
    front_contract: FuturesContract
    next_contract: Optional[FuturesContract]
    recommended_display_contract: FuturesContract
    roll_window_active: bool
    calendar_spread: Optional[float]
    volume_ratio: Optional[float]
    open_interest_ratio: Optional[float]
    policy: RollPolicy
    reason: str
    timestamp: datetime


class ContractResolver:
    """Resolves the active dated contract for a product under a roll policy."""

    def __init__(
        self,
        contracts: Sequence[FuturesContract],
        *,
        policy: RollPolicy = RollPolicy.HYBRID,
        roll_days_before: int = 8,
        hard_roll_days_before: int = 2,
        crossover_ratio: float = 1.0,
    ) -> None:
        # Index by product, sorted by expiration ascending.
        self._by_product: Dict[str, List[FuturesContract]] = {}
        for c in contracts:
            self._by_product.setdefault(c.product_symbol.upper(), []).append(c)
        for lst in self._by_product.values():
            lst.sort(key=lambda c: c.expiration)
        self.policy = policy
        self.roll_days_before = roll_days_before
        self.hard_roll_days_before = hard_roll_days_before
        self.crossover_ratio = crossover_ratio

    # -- ordered active contracts -----------------------------------------

    def _active_sorted(self, product: str, timestamp: datetime) -> List[FuturesContract]:
        """Not-yet-expired contracts for a product, nearest expiry first."""
        return [c for c in self._by_product.get(product.upper(), []) if not c.is_expired(timestamp)]

    def resolve_front_contract(
        self, product: str, timestamp: datetime
    ) -> Optional[FuturesContract]:
        active = self._active_sorted(product, timestamp)
        return active[0] if active else None

    def resolve_next_contract(self, product: str, timestamp: datetime) -> Optional[FuturesContract]:
        active = self._active_sorted(product, timestamp)
        return active[1] if len(active) > 1 else None

    def resolve_contract(
        self, product: str, contract_id_or_symbol: str, timestamp: datetime
    ) -> Optional[FuturesContract]:
        """Resolve ``front`` / ``next`` / a specific id or label to a contract."""
        key = (contract_id_or_symbol or "").strip()
        low = key.lower()
        if low in ("", "front", "continuous"):
            return self.resolve_front_contract(product, timestamp)
        if low == "next":
            return self.resolve_next_contract(product, timestamp)
        upper = key.upper()
        for c in self._by_product.get(product.upper(), []):
            if (c.contract_id or "").upper() == upper or c.label.upper() == upper:
                return c
        return None

    def resolve_option_underlying(
        self, option: FuturesOptionContract, timestamp: Optional[datetime] = None
    ) -> Optional[FuturesContract]:
        """The exact dated future an option settles into (never guessed)."""
        target = option.underlying_contract_id.upper()
        for c in self._by_product.get(option.product_symbol.upper(), []):
            if (c.contract_id or "").upper() == target or c.label.upper() == target:
                return c
        return None

    # -- roll state --------------------------------------------------------

    def get_roll_state(
        self,
        product: str,
        timestamp: datetime,
        prices: Optional[Dict[str, float]] = None,
    ) -> Optional[RollState]:
        front = self.resolve_front_contract(product, timestamp)
        if front is None:
            return None
        nxt = self.resolve_next_contract(product, timestamp)

        days_to_expiry = (front.expiration - timestamp).total_seconds() / 86_400.0
        calendar_active = days_to_expiry <= self.roll_days_before

        vol_ratio = _ratio(nxt.volume if nxt else None, front.volume)
        oi_ratio = _ratio(nxt.open_interest if nxt else None, front.open_interest)
        vol_cross = vol_ratio is not None and vol_ratio >= self.crossover_ratio
        oi_cross = oi_ratio is not None and oi_ratio >= self.crossover_ratio

        recommend_next, reason = self._decide(
            has_next=nxt is not None,
            days_to_expiry=days_to_expiry,
            calendar_active=calendar_active,
            vol_cross=vol_cross,
            oi_cross=oi_cross,
        )
        recommended = nxt if (recommend_next and nxt is not None) else front

        calendar_spread = None
        if prices and nxt is not None:
            f_px = prices.get(front.contract_id or front.label)
            n_px = prices.get(nxt.contract_id or nxt.label)
            if f_px is not None and n_px is not None:
                calendar_spread = round(n_px - f_px, 6)

        return RollState(
            product=product.upper(),
            front_contract=front,
            next_contract=nxt,
            recommended_display_contract=recommended,
            roll_window_active=calendar_active,
            calendar_spread=calendar_spread,
            volume_ratio=vol_ratio,
            open_interest_ratio=oi_ratio,
            policy=self.policy,
            reason=reason,
            timestamp=timestamp,
        )

    def _decide(
        self,
        *,
        has_next: bool,
        days_to_expiry: float,
        calendar_active: bool,
        vol_cross: bool,
        oi_cross: bool,
    ) -> tuple[bool, str]:
        """Return (recommend_next, human-readable reason) under the policy."""
        if not has_next:
            return False, "no next contract listed; holding front"

        if self.policy is RollPolicy.CALENDAR:
            if calendar_active:
                return (
                    True,
                    f"calendar roll: {days_to_expiry:.1f}d to expiry <= {self.roll_days_before}d",
                )
            return False, f"calendar: {days_to_expiry:.1f}d to expiry, outside roll window"

        if self.policy is RollPolicy.VOLUME:
            if vol_cross:
                return True, "volume crossover: next volume >= front volume"
            return False, "volume: next has not overtaken front"

        if self.policy is RollPolicy.OPEN_INTEREST:
            if oi_cross:
                return True, "open-interest crossover: next OI >= front OI"
            return False, "open-interest: next has not overtaken front"

        # HYBRID (default)
        if not calendar_active:
            return False, f"hybrid: {days_to_expiry:.1f}d to expiry, outside roll window"
        if days_to_expiry <= self.hard_roll_days_before:
            return True, (
                f"hybrid hard roll: {days_to_expiry:.1f}d to expiry "
                f"<= {self.hard_roll_days_before}d fallback"
            )
        if vol_cross or oi_cross:
            crossed = "volume" if vol_cross else "open-interest"
            return True, f"hybrid: in roll window and {crossed} crossover"
        return False, (
            f"hybrid: in roll window ({days_to_expiry:.1f}d) but no volume/OI crossover yet"
        )


def _ratio(numerator: Optional[int], denominator: Optional[int]) -> Optional[float]:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return round(numerator / denominator, 6)
