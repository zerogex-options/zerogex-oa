"""Contract-aware, multiplier-aware futures-options GEX pipeline.

Computes true futures-options gamma exposure from CME ES/NQ option chains
using Black-76 Greeks (:mod:`src.futures.black76`) and the futures contract
multiplier (:mod:`src.instruments` / the option's own multiplier) — **never**
the hardcoded equity ``100``.

GEX convention (preserves the platform's existing "dollar gamma per 1% move"
units, but with the futures multiplier)::

    dealer_signed_gex = dealer_sign
                        × gamma            (Black-76, 1/point)
                        × open_interest
                        × contract_multiplier   (50 ES / 20 NQ)
                        × futures_price²
                        × 0.01

Every result carries explicit provenance (spec Phase 7): logical symbol,
resolved dated contract, analytics source, timestamps, multiplier, dealer-sign
policy version, calculation version, and a quality status + reasons — so a
consumer can always tell genuine CME futures-options GEX apart from anything
else, and know exactly how it was produced.

Analytics are computed **per underlying futures contract** and only then
optionally aggregated, so options on different maturities are never blindly
summed onto one price axis (the aggregation seam is
:func:`aggregate_by_underlying`).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Sequence

from src.futures import black76
from src.futures.contracts import PutCall
from src.futures.sign_policy import (
    DEFAULT_SIGN_POLICY,
    DealerPositionSignPolicy,
)

CALCULATION_VERSION = "futures_gex_v1"

#: GEX unit tag echoed in results so downstream never guesses the convention.
GEX_UNIT = "dollar_gamma_per_1pct_move"


@dataclass(frozen=True)
class OptionInput:
    """One option contract's inputs to the GEX engine.

    Either ``iv`` or ``market_price`` must be supplied; when only a market
    price is given the engine solves Black-76 IV.  ``expiration`` is the exact
    settlement instant, and ``underlying_contract_id`` binds it to a dated
    future so cross-maturity mixing is explicit.
    """

    strike: float
    put_call: PutCall
    open_interest: int
    expiration: datetime
    underlying_contract_id: str
    multiplier: int
    iv: Optional[float] = None
    market_price: Optional[float] = None


@dataclass(frozen=True)
class ContractGEX:
    strike: float
    put_call: PutCall
    open_interest: int
    underlying_contract_id: str
    expiration: datetime
    multiplier: int
    dte: float
    iv: Optional[float]
    gamma: float
    gex: float  # dealer-signed dollar gamma per 1% move
    delta_exposure: float  # dealer-signed delta * OI * multiplier
    vanna_exposure: float
    charm_exposure: float
    quality: str


@dataclass(frozen=True)
class StrikeGEX:
    strike: float
    call_gex: float
    put_gex: float
    net_gex: float


@dataclass(frozen=True)
class Wall:
    strike: Optional[float]
    strength: Optional[float]


@dataclass(frozen=True)
class GEXResult:
    # -- provenance / metadata (spec Phase 7) --------------------------------
    logical_symbol: str
    resolved_contract: str
    contract_expiration: Optional[str]
    analytics_source: str
    data_timestamp: str
    open_interest_as_of: Optional[str]
    futures_price: float
    multiplier: int
    calculation_version: str
    gex_unit: str
    dealer_sign_policy: str
    quality_status: str
    quality_reasons: List[str]
    # -- headline exposures --------------------------------------------------
    call_gex: float
    put_gex: float
    net_gex: float
    net_gex_at_spot: float
    gamma_flip: Optional[float]
    call_wall: Wall
    put_wall: Wall
    max_pain: Optional[float]
    vanna_exposure: float
    charm_exposure: float
    dealer_delta_pressure: float
    zero_dte_net_gex: float
    # -- breakdowns ----------------------------------------------------------
    by_strike: List[StrikeGEX] = field(default_factory=list)
    by_expiration: Dict[str, float] = field(default_factory=dict)
    by_underlying_contract: Dict[str, float] = field(default_factory=dict)
    dte_buckets: Dict[str, float] = field(default_factory=dict)


class FuturesGexEngine:
    """Computes futures-options GEX for one logical product's chain."""

    def __init__(
        self,
        *,
        risk_free_rate: float = 0.05,
        sign_policy: DealerPositionSignPolicy = DEFAULT_SIGN_POLICY,
        analytics_source: str = "cme_futures_options",
        profile_span_pct: float = 0.20,
        profile_step_pct: float = 0.005,
        min_minutes_to_expiry: float = 30.0,
    ) -> None:
        self.risk_free_rate = risk_free_rate
        self.sign_policy = sign_policy
        self.analytics_source = analytics_source
        self.profile_span_pct = profile_span_pct
        self.profile_step_pct = profile_step_pct
        self.min_minutes_to_expiry = min_minutes_to_expiry

    # -- per-contract ------------------------------------------------------

    def _contract_gex(
        self, opt: OptionInput, futures_price: float, now: datetime
    ) -> Optional[ContractGEX]:
        if opt.open_interest is None or opt.open_interest <= 0:
            return None
        t_years = black76.years_to_expiration(
            now, opt.expiration, min_minutes=self.min_minutes_to_expiry
        )
        is_call = opt.put_call is PutCall.CALL

        iv = opt.iv
        quality = "good"
        if iv is None:
            if opt.market_price is None:
                return None
            iv = black76.implied_volatility(
                opt.market_price,
                futures_price,
                opt.strike,
                t_years,
                is_call=is_call,
                rate=self.risk_free_rate,
            )
            if iv is None:
                quality = "iv_unresolved"
                return ContractGEX(
                    strike=opt.strike,
                    put_call=opt.put_call,
                    open_interest=opt.open_interest,
                    underlying_contract_id=opt.underlying_contract_id,
                    expiration=opt.expiration,
                    multiplier=opt.multiplier,
                    dte=t_years * 365.0,
                    iv=None,
                    gamma=0.0,
                    gex=0.0,
                    delta_exposure=0.0,
                    vanna_exposure=0.0,
                    charm_exposure=0.0,
                    quality=quality,
                )

        greeks = black76.evaluate(
            futures_price, opt.strike, t_years, iv, is_call=is_call, rate=self.risk_free_rate
        )
        sign = self.sign_policy.sign_for_contract(opt.put_call)
        mult = opt.multiplier
        f2 = futures_price * futures_price

        gex = sign * greeks.gamma * opt.open_interest * mult * f2 * 0.01
        delta_exposure = sign * greeks.delta * opt.open_interest * mult
        vanna_exposure = sign * greeks.vanna * opt.open_interest * mult * futures_price * 0.01
        charm_exposure = sign * greeks.charm * opt.open_interest * mult * futures_price
        return ContractGEX(
            strike=opt.strike,
            put_call=opt.put_call,
            open_interest=opt.open_interest,
            underlying_contract_id=opt.underlying_contract_id,
            expiration=opt.expiration,
            multiplier=opt.multiplier,
            dte=t_years * 365.0,
            iv=iv,
            gamma=greeks.gamma,
            gex=gex,
            delta_exposure=delta_exposure,
            vanna_exposure=vanna_exposure,
            charm_exposure=charm_exposure,
            quality=greeks.quality.value,
        )

    # -- profile / flip ----------------------------------------------------

    def _gamma_flip(
        self, contracts: Sequence[ContractGEX], futures_price: float, now: datetime
    ) -> Optional[float]:
        """Nearest zero-crossing of the spot-shifted dealer gamma profile.

        Reprices from the per-contract results (which carry the *solved* IV),
        so a chain quoted by price rather than IV still yields a profile.
        """
        priced = [c for c in contracts if c.iv is not None and c.open_interest > 0]
        if not priced:
            return None
        span = self.profile_span_pct
        step = max(self.profile_step_pct, 1e-4)
        lo = futures_price * (1.0 - span)
        hi = futures_price * (1.0 + span)
        n = max(4, int((hi - lo) / (futures_price * step)))
        grid = [lo + (hi - lo) * i / n for i in range(n + 1)]

        profile: List[float] = []
        for gp in grid:
            total = 0.0
            for c in priced:
                # `priced` guarantees a solved IV; assert narrows for the checker.
                assert c.iv is not None
                t_years = black76.years_to_expiration(
                    now, c.expiration, min_minutes=self.min_minutes_to_expiry
                )
                g = black76.evaluate(
                    gp,
                    c.strike,
                    t_years,
                    c.iv,
                    is_call=c.put_call is PutCall.CALL,
                    rate=self.risk_free_rate,
                ).gamma
                sign = self.sign_policy.sign_for_contract(c.put_call)
                total += sign * g * c.open_interest * c.multiplier * gp * gp * 0.01
            profile.append(total)

        best: Optional[float] = None
        best_dist = math.inf
        for i in range(len(profile) - 1):
            a, b = profile[i], profile[i + 1]
            if a == 0.0:
                cross = grid[i]
            elif a * b < 0:
                cross = grid[i] + (grid[i + 1] - grid[i]) * (-a) / (b - a)
            else:
                continue
            dist = abs(cross - futures_price)
            if dist < best_dist:
                best_dist, best = dist, cross
        return best

    # -- walls / max pain --------------------------------------------------

    @staticmethod
    def _walls(by_strike: List[StrikeGEX], futures_price: float) -> tuple[Wall, Wall]:
        call_candidates = [s for s in by_strike if s.strike >= futures_price and s.call_gex > 0]
        put_candidates = [s for s in by_strike if s.strike <= futures_price and s.put_gex < 0]
        call_wall = max(call_candidates, key=lambda s: s.call_gex, default=None)
        put_wall = min(put_candidates, key=lambda s: s.put_gex, default=None)
        return (
            Wall(
                call_wall.strike if call_wall else None, call_wall.call_gex if call_wall else None
            ),
            Wall(
                put_wall.strike if put_wall else None, abs(put_wall.put_gex) if put_wall else None
            ),
        )

    @staticmethod
    def _max_pain(options: Sequence[OptionInput]) -> Optional[float]:
        strikes = sorted({o.strike for o in options if o.open_interest and o.open_interest > 0})
        if not strikes:
            return None
        best_strike = None
        best_pay = math.inf
        for test in strikes:
            pay = 0.0
            for o in options:
                if not o.open_interest or o.open_interest <= 0:
                    continue
                if o.put_call is PutCall.CALL:
                    pay += max(0.0, test - o.strike) * o.open_interest * o.multiplier
                else:
                    pay += max(0.0, o.strike - test) * o.open_interest * o.multiplier
            if pay < best_pay:
                best_pay, best_strike = pay, test
        return best_strike

    # -- public entrypoint -------------------------------------------------

    def compute(
        self,
        *,
        logical_symbol: str,
        resolved_contract: str,
        options: Sequence[OptionInput],
        futures_price: float,
        now: datetime,
        contract_expiration: Optional[datetime] = None,
        open_interest_as_of: Optional[datetime] = None,
    ) -> GEXResult:
        reasons: List[str] = []
        if futures_price is None or futures_price <= 0:
            reasons.append("invalid futures price")
        usable = [o for o in options if o.open_interest and o.open_interest > 0]
        if not usable:
            reasons.append("no open interest in chain")

        per_contract: List[ContractGEX] = []
        if futures_price and futures_price > 0:
            for opt in usable:
                cg = self._contract_gex(opt, futures_price, now)
                if cg is not None:
                    per_contract.append(cg)

        iv_missing = sum(1 for c in per_contract if c.iv is None)
        if iv_missing:
            reasons.append(f"{iv_missing} contracts missing/unsolvable IV")

        # Aggregate by strike.
        strike_map: Dict[float, List[float]] = {}
        call_total = put_total = 0.0
        vanna_total = charm_total = delta_total = 0.0
        zero_dte_total = 0.0
        by_expiration: Dict[str, float] = {}
        by_underlying: Dict[str, float] = {}
        dte_buckets: Dict[str, float] = {"0dte": 0.0, "1-5": 0.0, "6-30": 0.0, "30+": 0.0}

        for c in per_contract:
            call_gex = c.gex if c.put_call is PutCall.CALL else 0.0
            put_gex = c.gex if c.put_call is PutCall.PUT else 0.0
            slot = strike_map.setdefault(c.strike, [0.0, 0.0])
            slot[0] += call_gex
            slot[1] += put_gex
            call_total += call_gex
            put_total += put_gex
            vanna_total += c.vanna_exposure
            charm_total += c.charm_exposure
            delta_total += c.delta_exposure
            u_key = c.underlying_contract_id
            by_underlying[u_key] = by_underlying.get(u_key, 0.0) + c.gex
            e_key = c.expiration.date().isoformat()
            by_expiration[e_key] = by_expiration.get(e_key, 0.0) + c.gex
            if c.dte <= 1.0:
                dte_buckets["0dte"] += c.gex
                zero_dte_total += c.gex
            elif c.dte <= 5.0:
                dte_buckets["1-5"] += c.gex
            elif c.dte <= 30.0:
                dte_buckets["6-30"] += c.gex
            else:
                dte_buckets["30+"] += c.gex

        by_strike = [
            StrikeGEX(strike=k, call_gex=v[0], put_gex=v[1], net_gex=v[0] + v[1])
            for k, v in sorted(strike_map.items())
        ]
        net_gex = call_total + put_total
        call_wall, put_wall = self._walls(by_strike, futures_price or 0.0)
        max_pain = self._max_pain(usable) if usable else None

        gamma_flip = None
        net_gex_at_spot = net_gex
        if per_contract and futures_price and futures_price > 0:
            # Reprice from solved-IV per-contract results.
            if any(c.iv is not None for c in per_contract):
                gamma_flip = self._gamma_flip(per_contract, futures_price, now)
            else:
                reasons.append("gamma flip skipped: no IV to reprice profile")

        quality = "good"
        if reasons:
            quality = "degraded" if per_contract else "unavailable"

        return GEXResult(
            logical_symbol=logical_symbol,
            resolved_contract=resolved_contract,
            contract_expiration=(contract_expiration.isoformat() if contract_expiration else None),
            analytics_source=self.analytics_source,
            data_timestamp=now.isoformat(),
            open_interest_as_of=(open_interest_as_of.isoformat() if open_interest_as_of else None),
            futures_price=futures_price or 0.0,
            multiplier=(usable[0].multiplier if usable else 0),
            calculation_version=CALCULATION_VERSION,
            gex_unit=GEX_UNIT,
            dealer_sign_policy=self.sign_policy.version,
            quality_status=quality,
            quality_reasons=reasons,
            call_gex=call_total,
            put_gex=put_total,
            net_gex=net_gex,
            net_gex_at_spot=net_gex_at_spot,
            gamma_flip=gamma_flip,
            call_wall=call_wall,
            put_wall=put_wall,
            max_pain=max_pain,
            vanna_exposure=vanna_total,
            charm_exposure=charm_total,
            dealer_delta_pressure=delta_total,
            zero_dte_net_gex=zero_dte_total,
            by_strike=by_strike,
            by_expiration=by_expiration,
            by_underlying_contract=by_underlying,
            dte_buckets=dte_buckets,
        )


def aggregate_by_underlying(results: Sequence[GEXResult]) -> Dict[str, float]:
    """Explicit cross-maturity aggregation seam (net GEX per dated contract).

    Kept separate from :meth:`FuturesGexEngine.compute` so that raw GEX values
    with incompatible price coordinates are never silently combined — a caller
    must opt in to aggregation and can always inspect each contract separately.
    """
    out: Dict[str, float] = {}
    for r in results:
        for contract_id, gex in r.by_underlying_contract.items():
            out[contract_id] = out.get(contract_id, 0.0) + gex
    return out
