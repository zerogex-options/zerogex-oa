"""Market-Maker Attributed GEX, computed with ZeroGEX's production kernels.

The whole experiment turns on isolating **one** variable: how option
positioning is attributed to Market Makers.  Everything else — the
Black-Scholes gamma kernel, the settlement-aware time-to-expiration, the
spot-shift grid, the DTE horizon-occupancy weight, the span ladder, the
structural-interior crossing gates, the interpolation, the ``γ·q·100·S²·0.01``
dollar convention — must be *identical* to production, or a difference in the
output tells you nothing about attribution.

So this module does not reimplement any of it.  It calls
:class:`src.analytics.main_engine.AnalyticsEngine` methods unmodified.

How the substitution works
--------------------------
Production sums, per contract, over a hypothetical-spot grid::

    contribution_i(S) = sign(type_i) · w_dte(T_i) · γ(K_i,T_i,σ_i;S) · OI_i · 100 · S² · 0.01

with ``sign = +1`` for calls and ``−1`` for puts.  That sign is the *modeled*
positioning assumption ZeroGEX documents openly: dealers long the calls
customers overwrite, short the puts customers buy.  It is a proxy for an
unobservable, and it is exactly what this experiment replaces.

The MM-attributed contribution is::

    contribution_i(S) = MM_net_i · w_dte(T_i) · γ(K_i,T_i,σ_i;S) · 100 · S² · 0.01

— no option-type sign, because the sign now comes from the observed position.
A long option position has positive gamma whether it is a call or a put.

Because Black-Scholes gamma is *identical* for a call and a put at the same
``(K, T, σ)``, a signed quantity can be encoded losslessly for the production
kernel: represent ``MM_net > 0`` as a synthetic call row and ``MM_net < 0`` as
a synthetic put row, each with ``open_interest = |MM_net|``.  The kernel's own
``sign(type)`` then reproduces the position's sign exactly::

    sign · |MM_net| · γ · … = MM_net · γ · …

This is an identity, not an approximation, and it is what lets the flip
resolver, the DTE ramp and the structural gates run over MM positioning
without a single line of production code being touched or duplicated.  The
synthetic row keeps the contract's real ``option_symbol`` so the AM-settled
``SPX`` / PM-settled ``SPXW`` distinction still resolves correctly.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Optional, Sequence

from src.analytics.main_engine import AnalyticsEngine
from src.market_calendar import calculate_time_to_expiration, settlement_close_time_for_contract

from research.mm_attributed_gex.inventory import MMPosition
from research.mm_attributed_gex.schema import SeriesKey

__all__ = [
    "CONTRACT_MULTIPLIER",
    "ChainQuote",
    "MMContract",
    "MMGexResult",
    "ExpirationUniverse",
    "build_engine",
    "join_positions_to_chain",
    "signed_profile_rows",
    "production_profile_rows",
    "strike_rows",
    "compute_mm_gex",
    "dollar_gex",
]

#: Inherited from production (``_calculate_gex_by_strike``), not re-derived.
#: Changing it here would silently break the apples-to-apples comparison.
CONTRACT_MULTIPLIER = 100.0


def dollar_gex(gamma_qty: float, spot: float) -> float:
    """``γ·qty → dollars per 1% move``, the production scaling constant.

    Mirrors ``γ × qty × 100 × S² × 0.01`` from
    ``AnalyticsEngine._calculate_gex_by_strike`` and ``src/analytics/walls.py``
    so a research figure is directly comparable to a persisted one.
    """
    return gamma_qty * CONTRACT_MULTIPLIER * spot * spot * 0.01


@dataclass(frozen=True, slots=True)
class ChainQuote:
    """The market data a series needs to be priced, from ZeroGEX's own chain.

    Sourced from ``option_chains`` / ``option_chains_latest`` /
    ``option_chains_archive`` — the experiment never invents an implied vol.
    A series with no quote cannot be priced and is reported as unpriceable
    rather than silently dropped.
    """

    option_symbol: str
    strike: float
    expiration: date
    option_type: str
    implied_volatility: Optional[float]
    gamma: Optional[float] = None
    open_interest: int = 0
    volume: int = 0

    @property
    def key(self) -> SeriesKey:
        return ("", self.expiration, float(self.strike), self.option_type)


@dataclass(slots=True)
class MMContract:
    """One MM position joined to its market quote — ready to price."""

    position: MMPosition
    quote: ChainQuote
    net_contracts: float

    @property
    def strike(self) -> float:
        return self.quote.strike

    @property
    def expiration(self) -> date:
        return self.quote.expiration

    @property
    def option_type(self) -> str:
        return self.quote.option_type

    @property
    def implied_volatility(self) -> Optional[float]:
        return self.quote.implied_volatility


@dataclass(frozen=True)
class ExpirationUniverse:
    """Which expirations enter a given MM-attributed calculation.

    ``max_dte=0`` is 0DTE only; ``nearest_only`` keeps just the front
    expiration; ``max_dte=7`` is the weekly horizon; ``None`` keeps everything
    the reconstruction covers.
    """

    name: str
    max_dte: Optional[int] = None
    min_dte: int = 0
    nearest_only: bool = False

    def select(self, contracts: Sequence[MMContract], as_of: date) -> list[MMContract]:
        live = [c for c in contracts if (c.expiration - as_of).days >= self.min_dte]
        if self.nearest_only:
            if not live:
                return []
            front = min(c.expiration for c in live)
            return [c for c in live if c.expiration == front]
        if self.max_dte is None:
            return live
        return [c for c in live if (c.expiration - as_of).days <= self.max_dte]


#: The universes requirement 4 asks for, plus an "all" arm.
DEFAULT_UNIVERSES: tuple[ExpirationUniverse, ...] = (
    ExpirationUniverse("0dte", max_dte=0),
    ExpirationUniverse("nearest", nearest_only=True),
    ExpirationUniverse("weekly", max_dte=7),
    ExpirationUniverse("near_term", max_dte=45),
    ExpirationUniverse("all", max_dte=None),
)


def build_engine(symbol: str = "SPX") -> AnalyticsEngine:
    """An :class:`AnalyticsEngine` used purely as a calculation kernel.

    ``AnalyticsEngine.__init__`` opens no database connection and starts no
    loop — it only resolves the symbol, the risk-free rate and the per-symbol
    dividend yield from config.  Constructing one here therefore gives the
    experiment the *exact* production gamma/flip/wall math (including the
    operator's configured ``r`` and ``q``) with no production side effects.
    """
    return AnalyticsEngine(underlying=symbol)


# ---------------------------------------------------------------------------
# Joining inventory to market data
# ---------------------------------------------------------------------------


def join_positions_to_chain(
    positions: Iterable[MMPosition],
    chain: Iterable[ChainQuote],
    *,
    use_net_flow: bool = False,
) -> tuple[list[MMContract], list[MMPosition]]:
    """Pair each MM position with its quote.

    Returns ``(joined, unpriceable)``.  ``unpriceable`` holds positions with no
    matching quote or no usable implied vol — reported, never silently dropped,
    because unpriceable inventory is a completeness problem and completeness is
    half of what this experiment measures.

    ``use_net_flow`` switches the quantity to the open/close-agnostic estimator
    (:attr:`MMPosition.net_flow_contracts`).  The two agree exactly unless the
    feed carried volume with an ``UNKNOWN`` position effect; see
    :mod:`~.inventory`.
    """
    by_key: dict[tuple[date, float, str], ChainQuote] = {}
    for q in chain:
        by_key[(q.expiration, float(q.strike), q.option_type)] = q

    joined: list[MMContract] = []
    unpriceable: list[MMPosition] = []
    for pos in positions:
        quote = by_key.get((pos.expiration, float(pos.strike), pos.option_type))
        if quote is None or not quote.implied_volatility or quote.implied_volatility <= 0:
            unpriceable.append(pos)
            continue
        qty = pos.net_flow_contracts if use_net_flow else pos.net_contracts
        if qty == 0.0:
            continue
        joined.append(MMContract(position=pos, quote=quote, net_contracts=float(qty)))
    return joined, unpriceable


# ---------------------------------------------------------------------------
# Row builders for the production kernels
# ---------------------------------------------------------------------------


def signed_profile_rows(contracts: Sequence[MMContract]) -> list[dict[str, Any]]:
    """Encode signed MM quantities as rows the production profile kernel eats.

    See the module docstring for why this is exact: BS gamma is option-type
    independent, so ``sign(type) × |qty|`` reproduces the signed quantity.  The
    real ``option_symbol`` rides along untouched so settlement-aware ``T``
    still distinguishes AM-settled SPX from PM-settled SPXW.
    """
    rows: list[dict[str, Any]] = []
    for c in contracts:
        qty = c.net_contracts
        if qty == 0.0:
            continue
        rows.append(
            {
                "option_symbol": c.quote.option_symbol,
                "strike": float(c.strike),
                "expiration": c.expiration,
                # Sign carrier for the production kernel — NOT the contract's
                # real option type, which stays available on ``mm_option_type``.
                "option_type": "C" if qty > 0 else "P",
                "open_interest": abs(qty),
                "volume": c.quote.volume,
                "implied_volatility": c.implied_volatility,
                "gamma": c.quote.gamma or 0.0,
                "mm_option_type": c.option_type,
                "mm_net_contracts": qty,
            }
        )
    return rows


def production_profile_rows(chain: Sequence[ChainQuote]) -> list[dict[str, Any]]:
    """Rows for the *existing* methodology: open interest with the call+/put− sign.

    Used when the production comparand has to be recomputed rather than read
    from ``gex_summary`` — same contracts, same IVs, same kernel, only the
    positioning attribution differs.  That is the controlled comparison.
    """
    return [
        {
            "option_symbol": q.option_symbol,
            "strike": float(q.strike),
            "expiration": q.expiration,
            "option_type": q.option_type,
            "open_interest": int(q.open_interest),
            "volume": int(q.volume),
            "implied_volatility": q.implied_volatility,
            "gamma": q.gamma or 0.0,
        }
        for q in chain
    ]


def strike_rows(
    contracts: Sequence[MMContract],
    engine: AnalyticsEngine,
    spot: float,
    timestamp: datetime,
    *,
    recompute_gamma: bool = True,
) -> list[dict[str, Any]]:
    """Per-``(strike, expiration)`` MM gamma rows, shaped like ``gex_by_strike``.

    Column meanings deliberately mirror the production table so the wall helper
    can consume them unchanged, but the values are MM-attributed:

    ``mm_call_gamma`` / ``mm_put_gamma``
        ``Σ γ_i · MM_net_i`` over that side.  **Signed** — negative when MM is
        net short that side, which the production columns can never be.
    ``call_gamma`` / ``put_gamma``
        the production-shaped slots, filled so that
        ``net_gex = (call_gamma − put_gamma) · scale`` equals MM net dollar
        gamma: ``call_gamma = mm_call_gamma`` and ``put_gamma = −mm_put_gamma``.
        This is what makes "existing wall definition applied to MM-attributed
        positions" (Definition A in :mod:`~.walls`) a faithful substitution
        rather than a re-derivation.
    ``net_gex``
        MM net dollar gamma per 1% move at that strike+expiration.

    ``recompute_gamma`` re-prices each contract's gamma at the CURRENT spot
    from its stored IV, matching what production does on a spot-anchored cycle;
    with ``False`` the snapshot's stored gamma is used verbatim.
    """
    buckets: dict[tuple[float, date], dict[str, Any]] = {}
    tte_cache: dict[tuple[date, str], float] = {}

    for c in contracts:
        close_t = settlement_close_time_for_contract(
            engine.db_symbol, c.quote.option_symbol, c.expiration
        )
        cache_key = (c.expiration, close_t)
        T = tte_cache.get(cache_key)
        if T is None:
            T = calculate_time_to_expiration(timestamp, c.expiration, market_close_time=close_t)
            tte_cache[cache_key] = T
        if T <= 0:
            continue

        iv = c.implied_volatility
        if recompute_gamma and iv and iv > 0:
            gamma = engine._calculate_bs_gamma(
                spot, float(c.strike), T, engine.risk_free_rate, iv, engine.dividend_yield
            )
        else:
            gamma = float(c.quote.gamma or 0.0)
        if gamma <= 0:
            continue

        key = (float(c.strike), c.expiration)
        row = buckets.get(key)
        if row is None:
            row = {
                "underlying": engine.db_symbol,
                "timestamp": timestamp,
                "strike": float(c.strike),
                "expiration": c.expiration,
                "mm_call_gamma": 0.0,
                "mm_put_gamma": 0.0,
                "mm_net_contracts_calls": 0.0,
                "mm_net_contracts_puts": 0.0,
                "mm_long_contracts": 0.0,
                "mm_short_contracts": 0.0,
                "n_series": 0,
                "n_clean_series": 0,
            }
            buckets[key] = row

        signed_gamma = gamma * c.net_contracts
        if c.option_type == "C":
            row["mm_call_gamma"] += signed_gamma
            row["mm_net_contracts_calls"] += c.net_contracts
        else:
            row["mm_put_gamma"] += signed_gamma
            row["mm_net_contracts_puts"] += c.net_contracts
        row["mm_long_contracts"] += max(0.0, c.net_contracts)
        row["mm_short_contracts"] += max(0.0, -c.net_contracts)
        row["n_series"] += 1
        if not c.position.left_censored:
            row["n_clean_series"] += 1

    scale = CONTRACT_MULTIPLIER * spot * spot * 0.01
    out: list[dict[str, Any]] = []
    for row in buckets.values():
        mm_call = row["mm_call_gamma"]
        mm_put = row["mm_put_gamma"]
        # Production-shaped slots (see docstring): call_gamma − put_gamma must
        # equal the MM net gamma aggregate.
        row["call_gamma"] = mm_call
        row["put_gamma"] = -mm_put
        row["total_gamma"] = mm_call + mm_put
        row["net_gex"] = (mm_call + mm_put) * scale
        row["mm_call_gex"] = mm_call * scale
        row["mm_put_gex"] = mm_put * scale
        row["abs_dollar_gex"] = abs(row["net_gex"])
        out.append(row)
    out.sort(key=lambda r: (r["strike"], r["expiration"]))
    return out


# ---------------------------------------------------------------------------
# The headline calculation
# ---------------------------------------------------------------------------


@dataclass
class MMGexResult:
    """MM-attributed gamma readings for one snapshot and one universe."""

    timestamp: datetime
    spot: float
    universe: str

    mm_attributed_gamma_at_spot: Optional[float] = None
    mm_attributed_gamma_flip: Optional[float] = None
    mm_attributed_gamma_flip_raw: Optional[float] = None
    mm_attributed_flip_span_used: Optional[float] = None
    mm_attributed_net_gex: Optional[float] = None
    mm_attributed_gamma_at_spot_unweighted: Optional[float] = None

    profile: list[tuple[float, float]] = field(default_factory=list)
    raw_profile: list[tuple[float, float]] = field(default_factory=list)
    strike_rows: list[dict[str, Any]] = field(default_factory=list)

    # Diagnostics.
    n_contracts: int = 0
    n_clean_contracts: int = 0
    n_unpriceable: int = 0
    mm_net_contracts_total: float = 0.0
    mm_gross_contracts_total: float = 0.0
    abs_gamma_total: float = 0.0
    abs_gamma_clean: float = 0.0
    contribution_by_bucket: dict[str, float] = field(default_factory=dict)
    flip_unresolved: bool = True

    @property
    def regime(self) -> Optional[str]:
        """``"long_gamma"`` / ``"short_gamma"`` from gamma at spot, or ``None``."""
        v = self.mm_attributed_gamma_at_spot
        if v is None:
            return None
        return "long_gamma" if v > 0 else "short_gamma"

    def as_dict(self) -> dict[str, Any]:
        return {
            "universe": self.universe,
            "mm_attributed_gamma_at_spot": self.mm_attributed_gamma_at_spot,
            "mm_attributed_gamma_flip": self.mm_attributed_gamma_flip,
            "mm_attributed_gamma_flip_raw": self.mm_attributed_gamma_flip_raw,
            "mm_attributed_flip_span_used": self.mm_attributed_flip_span_used,
            "mm_attributed_net_gex": self.mm_attributed_net_gex,
            "mm_attributed_gamma_at_spot_unweighted": (self.mm_attributed_gamma_at_spot_unweighted),
            "mm_regime": self.regime,
            "n_contracts": self.n_contracts,
            "n_clean_contracts": self.n_clean_contracts,
            "n_unpriceable": self.n_unpriceable,
            "mm_net_contracts_total": self.mm_net_contracts_total,
            "mm_gross_contracts_total": self.mm_gross_contracts_total,
            "abs_gamma_total": self.abs_gamma_total,
            "abs_gamma_clean": self.abs_gamma_clean,
            "flip_unresolved": self.flip_unresolved,
            **{f"contribution_{k}": v for k, v in self.contribution_by_bucket.items()},
        }


def _expiration_bucket(expiration: date, as_of: date) -> str:
    """Same bucketing production uses in ``_calculate_gex_by_strike``."""
    dte = max(0, (expiration - as_of).days)
    if dte == 0:
        return "0dte"
    if dte <= 7:
        return "weekly"
    if dte <= 45:
        return "monthly"
    return "leaps"


def compute_mm_gex(
    contracts: Sequence[MMContract],
    spot: float,
    timestamp: datetime,
    *,
    engine: Optional[AnalyticsEngine] = None,
    symbol: str = "SPX",
    universe: ExpirationUniverse = DEFAULT_UNIVERSES[-1],
    apply_horizon_weighting: bool = True,
    n_unpriceable: int = 0,
    compute_raw_flip: bool = True,
) -> MMGexResult:
    """Compute MM-attributed gamma at spot, gamma flip and net GEX.

    Every number here comes out of an unmodified production kernel:

    * the spot-shift curve from ``AnalyticsEngine._gamma_exposure_profile``
    * the flip from ``AnalyticsEngine._resolve_gamma_flip`` (span ladder,
      interior margin, structural p90 floor, max-distance gate, linear
      interpolation, and the honest ``None`` when nothing qualifies)
    * gamma at spot from ``AnalyticsEngine._net_gex_at_spot``, sampled off the
      *same* curve so the level and the regime can never contradict each other
    * the raw nearest-crossing variant from
      ``AnalyticsEngine._calculate_gamma_flip_point`` on the un-DTE-weighted
      curve, mirroring the production ``gamma_flip_raw`` secondary reference

    ``apply_horizon_weighting`` toggles requirement 4's two arms: ``True``
    applies ZeroGEX's existing horizon-occupancy ramp to MM-attributed
    positioning; ``False`` gives raw MM positioning with every expiry at full
    weight.
    """
    eng = engine or build_engine(symbol)
    as_of = timestamp.date()
    selected = universe.select(contracts, as_of)

    result = MMGexResult(
        timestamp=timestamp,
        spot=spot,
        universe=universe.name,
        n_unpriceable=n_unpriceable,
    )
    if not selected or spot <= 0:
        return result

    rows = signed_profile_rows(selected)
    result.n_contracts = len(selected)
    result.n_clean_contracts = sum(1 for c in selected if not c.position.left_censored)
    result.mm_net_contracts_total = sum(c.net_contracts for c in selected)
    result.mm_gross_contracts_total = sum(abs(c.net_contracts) for c in selected)

    if apply_horizon_weighting:
        profile, flip, span_used = eng._resolve_gamma_flip(rows, spot, timestamp)
    else:
        # Raw arm: one un-weighted profile at the default span; the ladder's
        # job is to rescue a *weighted* profile whose crossing is off-scale,
        # and mixing the two would make the arms non-comparable.
        profile = eng._gamma_exposure_profile(rows, spot, timestamp, apply_dte_weight=False)
        flip = eng._calculate_gamma_flip_point(profile, spot)
        span_used = None

    result.profile = profile
    result.mm_attributed_gamma_flip = flip
    result.mm_attributed_flip_span_used = span_used
    result.flip_unresolved = flip is None
    result.mm_attributed_gamma_at_spot = eng._net_gex_at_spot(profile, spot)

    if compute_raw_flip and apply_horizon_weighting:
        raw_profile = eng._gamma_exposure_profile(
            rows,
            spot,
            timestamp,
            span_pct=span_used,
            apply_dte_weight=False,
        )
        result.raw_profile = raw_profile
        result.mm_attributed_gamma_flip_raw = eng._calculate_gamma_flip_point(raw_profile, spot)
        result.mm_attributed_gamma_at_spot_unweighted = eng._net_gex_at_spot(raw_profile, spot)

    # Per-strike structure and the chain-wide aggregate.  Production's
    # ``total_net_gex`` is the UNWEIGHTED per-strike sum (distinct from the
    # DTE-weighted profile reading at spot); mirror that so the two columns
    # mean the same thing in both methodologies.
    srows = strike_rows(selected, eng, spot, timestamp)
    result.strike_rows = srows
    result.mm_attributed_net_gex = sum(r["net_gex"] for r in srows)
    result.abs_gamma_total = sum(abs(r["net_gex"]) for r in srows)

    clean_only = [c for c in selected if not c.position.left_censored]
    if clean_only:
        clean_rows = strike_rows(clean_only, eng, spot, timestamp)
        result.abs_gamma_clean = sum(abs(r["net_gex"]) for r in clean_rows)

    by_bucket: dict[str, float] = defaultdict(float)
    for row in srows:
        by_bucket[_expiration_bucket(row["expiration"], as_of)] += abs(row["net_gex"])
    total_abs = sum(by_bucket.values())
    result.contribution_by_bucket = {
        bucket: (value / total_abs if total_abs > 0 else 0.0)
        for bucket, value in sorted(by_bucket.items())
    }
    return result


def gamma_by_key(
    contracts: Sequence[MMContract],
    engine: AnalyticsEngine,
    spot: float,
    timestamp: datetime,
) -> dict[SeriesKey, float]:
    """``|dollar gamma|`` per series, for gamma-weighting the confidence score."""
    out: dict[SeriesKey, float] = {}
    tte_cache: dict[tuple[date, str], float] = {}
    for c in contracts:
        close_t = settlement_close_time_for_contract(
            engine.db_symbol, c.quote.option_symbol, c.expiration
        )
        cache_key = (c.expiration, close_t)
        T = tte_cache.get(cache_key)
        if T is None:
            T = calculate_time_to_expiration(timestamp, c.expiration, market_close_time=close_t)
            tte_cache[cache_key] = T
        iv = c.implied_volatility
        if T <= 0 or not iv or iv <= 0:
            continue
        gamma = engine._calculate_bs_gamma(
            spot, float(c.strike), T, engine.risk_free_rate, iv, engine.dividend_yield
        )
        out[c.position.key] = abs(dollar_gex(gamma * c.net_contracts, spot))
    return out
