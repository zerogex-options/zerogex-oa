"""MM-attributed strike structure: walls, nodes, concentrations, profile.

Requirement 7 asks for two things to be tested rather than one assumed, so
both are implemented and reported side by side.

Definition A — existing definition, MM-attributed inputs
--------------------------------------------------------
:func:`walls_definition_a` calls ZeroGEX's own
:func:`src.analytics.walls.compute_call_put_walls_with_strength` **unchanged**,
feeding it MM-attributed gamma in the slots the production rows use (see
``gex.strike_rows``: ``call_gamma = Σ_calls γ·MM_net``,
``put_gamma = −Σ_puts γ·MM_net``).  The structural question it asks is the same
one production asks — "above spot, where is dealer call gamma largest?" and
"below spot, where is dealer short-put gamma largest?" — with the modeled
positioning replaced by the observed one.  Ties break identically (call wall →
lowest qualifying strike, put wall → highest), and the strength figures come
out on the same ``γ·100·S²·0.01`` scale, so an MM wall strength is directly
comparable to a persisted ``call_wall_strength``.

Definition B — MM-natural
-------------------------
The production definition splits on option type because, under the modeled
convention, option type *is* the sign: dealers are assumed long calls and short
puts.  With observed attribution that link is broken — MM can be net short
calls and net long puts — so splitting on type no longer isolates the sign.

:func:`walls_definition_b` therefore ranks strikes on **total MM net dollar
gamma at the strike, calls and puts combined**:

* ``mm_wall_up`` / ``mm_wall_down`` — the strike above/below spot with the
  largest *positive* MM net gamma.  Positive MM gamma means MM hedging leans
  against the move (sell strength, buy weakness): the dampening, pinning,
  support/resistance behavior a "wall" is supposed to describe.
* ``mm_accelerant_up`` / ``mm_accelerant_down`` — the strike above/below spot
  with the most *negative* MM net gamma, where hedging amplifies the move.
  Reported under its own name because it is not a wall and calling it one
  would be exactly the "invent an indicator so the new data looks useful"
  failure the brief warns against.

Where the two definitions coincide
----------------------------------
When MM is net long calls above spot and net short puts below — the positioning
the production convention assumes — A and B select the same strikes.  Divergence
between them is therefore itself a measurement: it says the modeled convention
and the observed positioning disagree at that strike.  :func:`compare_definitions`
quantifies it.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence

from src.analytics.walls import compute_call_put_walls_with_strength

__all__ = [
    "WallSet",
    "GammaNode",
    "StrikeStructure",
    "walls_definition_a",
    "walls_definition_b",
    "dominant_nodes",
    "gamma_concentration",
    "strike_profile",
    "compare_definitions",
    "build_strike_structure",
]


@dataclass
class WallSet:
    """A pair of wall strikes plus their dollar-gamma magnitudes."""

    definition: str
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    call_wall_strength: Optional[float] = None
    put_wall_strength: Optional[float] = None

    def as_dict(self, prefix: str) -> dict[str, Any]:
        return {
            f"{prefix}_call_wall": self.call_wall,
            f"{prefix}_put_wall": self.put_wall,
            f"{prefix}_call_wall_strength": self.call_wall_strength,
            f"{prefix}_put_wall_strength": self.put_wall_strength,
        }


@dataclass
class GammaNode:
    """A strike carrying a locally dominant amount of MM gamma."""

    strike: float
    dollar_gex: float
    share_of_abs: float
    side: str  # "above" | "below" | "at"

    def as_dict(self) -> dict[str, Any]:
        return {
            "strike": self.strike,
            "dollar_gex": self.dollar_gex,
            "share_of_abs": self.share_of_abs,
            "side": self.side,
        }


@dataclass
class StrikeStructure:
    """Everything requirement 7 asks for, for one snapshot and one universe."""

    spot: float
    universe: str
    definition_a: WallSet = field(default_factory=lambda: WallSet("A"))
    definition_b: WallSet = field(default_factory=lambda: WallSet("B"))
    accelerant_up: Optional[float] = None
    accelerant_down: Optional[float] = None
    accelerant_up_strength: Optional[float] = None
    accelerant_down_strength: Optional[float] = None
    nodes: list[GammaNode] = field(default_factory=list)
    positive_gamma_share: float = 0.0
    negative_gamma_share: float = 0.0
    concentration_hhi: float = 0.0
    top5_share: float = 0.0
    profile: list[tuple[float, float]] = field(default_factory=list)
    definitions_agree_call: Optional[bool] = None
    definitions_agree_put: Optional[bool] = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "universe": self.universe,
            **self.definition_a.as_dict("mm_attributed_a"),
            **self.definition_b.as_dict("mm_attributed_b"),
            "mm_accelerant_up": self.accelerant_up,
            "mm_accelerant_down": self.accelerant_down,
            "mm_accelerant_up_strength": self.accelerant_up_strength,
            "mm_accelerant_down_strength": self.accelerant_down_strength,
            "mm_positive_gamma_share": self.positive_gamma_share,
            "mm_negative_gamma_share": self.negative_gamma_share,
            "mm_concentration_hhi": self.concentration_hhi,
            "mm_top5_strike_share": self.top5_share,
            "mm_wall_definitions_agree_call": self.definitions_agree_call,
            "mm_wall_definitions_agree_put": self.definitions_agree_put,
        }
        return out


def _aggregate_by_strike(rows: Iterable[Mapping[str, Any]]) -> dict[float, float]:
    """Sum ``net_gex`` per strike across expirations.

    Aggregating before ranking is the same choice ``src/analytics/walls.py``
    documents: dealers hedge the union of expirations at a strike, so ranking
    per ``(strike, expiration)`` row would pick a single-expiration outlier and
    disagree with every cross-expiration view of the chain.
    """
    agg: dict[float, float] = defaultdict(float)
    for row in rows:
        try:
            strike = float(row["strike"])
        except (KeyError, TypeError, ValueError):
            continue
        agg[strike] += float(row.get("net_gex") or 0.0)
    return dict(agg)


def walls_definition_a(strike_rows: Sequence[Mapping[str, Any]], spot: float) -> WallSet:
    """Existing ZeroGEX wall definition, applied to MM-attributed gamma.

    Delegates to the production helper with no modification whatsoever — the
    rows carry MM-attributed values in the ``call_gamma`` / ``put_gamma`` slots
    (built by :func:`research.mm_attributed_gex.gex.strike_rows`), so this is a
    pure input substitution.
    """
    call_wall, put_wall, call_strength, put_strength = compute_call_put_walls_with_strength(
        strike_rows, spot
    )
    return WallSet(
        definition="A",
        call_wall=call_wall,
        put_wall=put_wall,
        call_wall_strength=call_strength,
        put_wall_strength=put_strength,
    )


def walls_definition_b(
    strike_rows: Sequence[Mapping[str, Any]], spot: float
) -> tuple[WallSet, Optional[float], Optional[float], Optional[float], Optional[float]]:
    """MM-natural walls plus the negative-gamma accelerant strikes.

    Returns ``(walls, accel_up, accel_down, accel_up_strength, accel_down_strength)``.

    Ranking is on total MM net dollar gamma at the strike, so a strike where MM
    is long calls AND long puts ranks above one where the two cancel — which is
    the physically meaningful ordering once option type no longer encodes the
    sign.  Tie-breaks match production: nearest-to-spot wins.
    """
    if spot is None or spot <= 0:
        return WallSet("B"), None, None, None, None

    agg = _aggregate_by_strike(strike_rows)
    wall_up: Optional[float] = None
    wall_down: Optional[float] = None
    accel_up: Optional[float] = None
    accel_down: Optional[float] = None
    best_up = best_down = 0.0
    worst_up = worst_down = 0.0

    # Iterate ascending.  Above spot the tiebreak keeps the FIRST (lowest,
    # nearest-to-spot) strike, so the comparison is strict; below spot it keeps
    # the LAST (highest, nearest-to-spot) strike, so ties are allowed to
    # replace.  Same rule production uses in src/analytics/walls.py.
    for strike in sorted(agg):
        value = agg[strike]
        if strike >= spot:
            if value > 0 and value > best_up:
                best_up, wall_up = value, strike
            if value < worst_up:
                worst_up, accel_up = value, strike
        if strike <= spot:
            if value > 0 and value >= best_down:
                best_down, wall_down = value, strike
            if value < 0 and value <= worst_down:
                worst_down, accel_down = value, strike

    walls = WallSet(
        definition="B",
        call_wall=wall_up,
        put_wall=wall_down,
        call_wall_strength=abs(best_up) if wall_up is not None else None,
        put_wall_strength=abs(best_down) if wall_down is not None else None,
    )
    return (
        walls,
        accel_up,
        accel_down,
        abs(worst_up) if accel_up is not None else None,
        abs(worst_down) if accel_down is not None else None,
    )


def dominant_nodes(
    strike_rows: Sequence[Mapping[str, Any]],
    spot: float,
    top_k: int = 8,
) -> list[GammaNode]:
    """Top-``k`` strikes by ``|MM net dollar gamma|``, with their share."""
    agg = _aggregate_by_strike(strike_rows)
    total_abs = sum(abs(v) for v in agg.values())
    if total_abs <= 0:
        return []
    ranked = sorted(agg.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
    nodes: list[GammaNode] = []
    for strike, value in ranked:
        side = "above" if strike > spot else ("below" if strike < spot else "at")
        nodes.append(
            GammaNode(
                strike=strike,
                dollar_gex=value,
                share_of_abs=abs(value) / total_abs,
                side=side,
            )
        )
    return nodes


def gamma_concentration(strike_rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    """Positive/negative split and concentration of MM gamma across strikes.

    ``hhi`` is a Herfindahl index over each strike's share of total
    ``|dollar gamma|``: 1.0 = all gamma at one strike, → 0 = evenly spread.
    Concentration is a confounder the backtests control for (a wall test means
    something different when 60% of gamma sits on one strike than when it is
    smeared across forty).
    """
    agg = _aggregate_by_strike(strike_rows)
    total_abs = sum(abs(v) for v in agg.values())
    if total_abs <= 0:
        return {
            "positive_share": 0.0,
            "negative_share": 0.0,
            "hhi": 0.0,
            "top5_share": 0.0,
            "n_strikes": float(len(agg)),
        }
    positive = sum(v for v in agg.values() if v > 0)
    negative = -sum(v for v in agg.values() if v < 0)
    shares = sorted((abs(v) / total_abs for v in agg.values()), reverse=True)
    return {
        "positive_share": positive / total_abs,
        "negative_share": negative / total_abs,
        "hhi": sum(s * s for s in shares),
        "top5_share": sum(shares[:5]),
        "n_strikes": float(len(agg)),
    }


def strike_profile(strike_rows: Sequence[Mapping[str, Any]]) -> list[tuple[float, float]]:
    """Ascending ``[(strike, MM net dollar gamma), …]`` — the plottable profile."""
    agg = _aggregate_by_strike(strike_rows)
    return [(k, agg[k]) for k in sorted(agg)]


def compare_definitions(a: WallSet, b: WallSet) -> tuple[Optional[bool], Optional[bool]]:
    """Do A and B pick the same strikes?  ``None`` when either side is absent.

    Disagreement is a finding, not a bug: it marks strikes where the modeled
    call+/put− convention and the observed MM position point in different
    directions.
    """
    call_agree = (
        None if (a.call_wall is None or b.call_wall is None) else a.call_wall == b.call_wall
    )
    put_agree = None if (a.put_wall is None or b.put_wall is None) else a.put_wall == b.put_wall
    return call_agree, put_agree


def build_strike_structure(
    strike_rows: Sequence[Mapping[str, Any]],
    spot: float,
    universe: str = "all",
    *,
    top_k: int = 8,
) -> StrikeStructure:
    """Assemble every strike-structure reading for one snapshot."""
    struct = StrikeStructure(spot=spot, universe=universe)
    if not strike_rows or spot <= 0:
        return struct

    struct.definition_a = walls_definition_a(strike_rows, spot)
    (
        struct.definition_b,
        struct.accelerant_up,
        struct.accelerant_down,
        struct.accelerant_up_strength,
        struct.accelerant_down_strength,
    ) = walls_definition_b(strike_rows, spot)

    struct.nodes = dominant_nodes(strike_rows, spot, top_k=top_k)
    conc = gamma_concentration(strike_rows)
    struct.positive_gamma_share = conc["positive_share"]
    struct.negative_gamma_share = conc["negative_share"]
    struct.concentration_hhi = conc["hhi"]
    struct.top5_share = conc["top5_share"]
    struct.profile = strike_profile(strike_rows)
    struct.definitions_agree_call, struct.definitions_agree_put = compare_definitions(
        struct.definition_a, struct.definition_b
    )
    return struct
