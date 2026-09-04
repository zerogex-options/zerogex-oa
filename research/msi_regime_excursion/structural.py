"""Does the regime label move when only the DIRECTION of flow changes?

This asks a question about the shipped code, not about the market, so it needs
no archive and no database: it drives the real
:class:`src.signals.scoring_engine.ScoringEngine` with the real components, and
sweeps one input at a time.

The experiment holds **every gamma-structure input fixed** -- net GEX, the
gamma flip, the strike ladder, spot, VIX, the put/call ratio, the price path --
so the option book's shape, and therefore any honest read of "will moves run or
get pinned", is identical in every row. The only things that move are the two
inputs whose own docstrings describe them as directional:

* ``smart_call`` / ``smart_put`` -- aggressor-classified call vs put premium,
  feeding ``order_flow_imbalance`` (19 pts), "+1.0 = call premium dominates
  (**bullish** model output)".
* ``dealer_net_delta`` -- feeding ``dealer_delta_pressure`` (17 pts), "positive
  score => dealers are net short delta ... **bullish** for price".

If the MSI is "deliberately directionless" as ``frontend/core/impliedDirection.ts``
states, this sweep cannot change the regime band. Every band it does cross is a
customer-facing claim about how far price will travel, changed by an input that
carries no information about how far price will travel -- only about which way.

Run::

    python -m research.msi_regime_excursion.structural
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from research.msi_regime_excursion.bands import BANDS, band_for

__all__ = ["SweepRow", "run_sweep", "format_report"]


def _build_engine(underlying: str = "SPX"):
    from src.signals.components import (
        DealerDeltaPressureComponent,
        GammaAnchorComponent,
        NetGexSignComponent,
        OrderFlowImbalanceComponent,
        PutCallRatioStateComponent,
        VolatilityRegimeComponent,
    )
    from src.signals.scoring_engine import ScoringEngine

    return ScoringEngine(
        underlying,
        [
            NetGexSignComponent(),
            GammaAnchorComponent(),
            PutCallRatioStateComponent(),
            VolatilityRegimeComponent(),
            OrderFlowImbalanceComponent(),
            DealerDeltaPressureComponent(),
        ],
    )


#: Gamma-structure presets. Each fixes every MAGNITUDE-axis input to a value
#: whose meaning is unambiguous from the components' own documentation, so a
#: sweep within a preset varies direction and nothing else.
STRUCTURES: dict[str, dict[str, float]] = {
    # Every magnitude signal says "moves get damped": long gamma, low VIX,
    # dense local gamma, spot pinned to the max-gamma strike, flip far away.
    "pinned": {
        "net_gex": 6.0e9,
        "vix": 12.0,
        "local_gex_ratio": 1.0,
        "gamma_flip_offset": -400.0,
        "max_gamma_offset": 0.0,
    },
    # Mid-scale on every magnitude axis: no signal saturated, nothing
    # abstaining. This is the ordinary state of the market and the one where
    # the directional components have the most room to move the band.
    "neutral": {
        "net_gex": 6.2e8,
        "vix": 22.0,
        "local_gex_ratio": 0.5,
        "gamma_flip_offset": -50.0,
        "max_gamma_offset": -80.0,
    },
    # Every magnitude signal says "moves can run": short gamma, high VIX, thin
    # local gamma, spot far from the max-gamma strike, flip underfoot.
    "free": {
        "net_gex": -6.0e9,
        "vix": 30.0,
        "local_gex_ratio": 0.0,
        "gamma_flip_offset": 0.0,
        "max_gamma_offset": -250.0,
    },
}


def _context(
    *,
    structure: str,
    smart_call: float,
    smart_put: float,
    dealer_net_delta: float,
    close: float = 5000.0,
    put_call_ratio: float = 1.05,
):
    """A fully-specified context: only the three flow fields ever vary."""
    from src.signals.components.base import MarketContext

    spec = STRUCTURES[structure]
    net_gex = spec["net_gex"]
    # A symmetric strike ladder centred on spot -- identical across the sweep.
    gex_by_strike = [
        {
            "strike": float(close + offset),
            "call_oi": 20_000,
            "put_oi": 20_000,
            "net_gex": net_gex / 41.0,
        }
        for offset in range(-100, 101, 5)
    ]
    return MarketContext(
        timestamp=datetime(2026, 6, 1, 15, 0, tzinfo=timezone.utc),
        underlying="SPX",
        close=close,
        net_gex=net_gex,
        gamma_flip=close + spec["gamma_flip_offset"],
        put_call_ratio=put_call_ratio,
        max_pain=close,
        smart_call=smart_call,
        smart_put=smart_put,
        # A flat price path: no trend information anywhere in the context, so
        # nothing here can stand in for a directional read but the flow fields.
        recent_closes=[close] * 40,
        recent_lows=[close] * 40,
        recent_highs=[close] * 40,
        iv_rank=50.0,
        dealer_net_delta=dealer_net_delta,
        vwap=close,
        vwap_deviation_pct=0.0,
        total_oi=800_000,
        extra={
            "vix_level": spec["vix"],
            "gex_by_strike": gex_by_strike,
            "local_gex": abs(net_gex) * spec["local_gex_ratio"],
            "max_gamma_strike": close + spec["max_gamma_offset"],
        },
    )


@dataclass
class SweepRow:
    label: str
    flow_skew: float          # -1 = all put premium, +1 = all call premium
    dealer_net_delta: float
    msi: float
    band: Optional[str]
    band_label: str
    band_copy: str
    components: dict[str, float]


def run_sweep(
    *,
    structure: str = "pinned",
    premium: float = 5.0e7,
    dni_scale: float = 3.0e8,
    steps: int = 9,
) -> list[SweepRow]:
    """Sweep flow direction from maximally bearish to maximally bullish.

    Every gamma-structure input is fixed by ``structure`` and never varies
    within a sweep -- that is the point. ``steps`` is forced odd so the sweep
    lands on exactly-neutral flow, which is worth seeing: at skew 0 both
    directional components abstain (``|raw| < 1e-3``) and the engine
    renormalizes the survivors onto the full 100-point scale, so the neutral
    row is not the midpoint of its neighbours.
    """
    if structure not in STRUCTURES:
        raise SystemExit(f"unknown structure {structure!r}; known: {', '.join(STRUCTURES)}")
    if steps % 2 == 0:
        steps += 1
    engine = _build_engine()
    rows: list[SweepRow] = []
    for i in range(steps):
        # skew in [-1, +1]: -1 = every dollar of premium on the put side.
        skew = -1.0 + 2.0 * i / (steps - 1)
        call_prem = premium * (1.0 + skew) / 2.0
        put_prem = premium * (1.0 - skew) / 2.0
        # Dealer net delta carries the same directional story: a bullish tape
        # leaves dealers short delta (negative DNI).
        dni = -skew * dni_scale
        ctx = _context(
            structure=structure,
            smart_call=call_prem,
            smart_put=put_prem,
            dealer_net_delta=dni,
        )
        snapshot, results = engine.score(ctx)
        key = band_for(snapshot.composite_score)
        band = next((b for b in BANDS if b.key == key), None)
        rows.append(
            SweepRow(
                label=f"skew {skew:+.2f}",
                flow_skew=skew,
                dealer_net_delta=dni,
                msi=snapshot.composite_score,
                band=key,
                band_label=band.label if band else "—",
                band_copy=band.copy if band else "—",
                components={c.name: round(s, 4) for c, s in results},
            )
        )
    return rows


def format_report(rows: list[SweepRow]) -> str:
    lines: list[str] = []
    header = f"{'flow skew':>10} {'dealer Δ':>14} {'MSI':>7}  {'band':<20} copy"
    lines.append(header)
    lines.append("-" * (len(header) + 30))
    for r in rows:
        lines.append(
            f"{r.flow_skew:>+10.2f} {r.dealer_net_delta:>14,.0f} {r.msi:>7.2f}  "
            f"{r.band_label:<20} {r.band_copy}"
        )
    bands_seen = [r.band for r in rows]
    distinct = sorted(set(b for b in bands_seen if b))
    lines.append("")
    lines.append(
        f"Gamma structure identical in every row above. "
        f"Distinct regime bands produced by flow direction alone: {len(distinct)}"
    )
    lines.append(f"  {' -> '.join(dict.fromkeys(r.band_label for r in rows))}")
    return "\n".join(lines)


def summarize(structure: str, rows: list[SweepRow]) -> dict:
    """The one-line finding for one structure."""
    msis = [r.msi for r in rows]
    labels = list(dict.fromkeys(r.band_label for r in rows))
    return {
        "structure": structure,
        "msi_min": min(msis),
        "msi_max": max(msis),
        "msi_span": max(msis) - min(msis),
        "distinct_bands": len(set(r.band for r in rows if r.band)),
        "band_path": labels,
    }


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Sweep flow DIRECTION with gamma structure held fixed, and report how "
            "far the MSI and its regime band move."
        )
    )
    parser.add_argument("--structure", default="all",
                        choices=sorted(STRUCTURES) + ["all"],
                        help="gamma structure held fixed across the sweep")
    parser.add_argument("--steps", type=int, default=9)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    names = sorted(STRUCTURES) if args.structure == "all" else [args.structure]
    payload: dict[str, Any] = {}
    summaries: list[dict] = []
    for name in names:
        rows = run_sweep(structure=name, steps=args.steps)
        payload[name] = [r.__dict__ for r in rows]
        summaries.append(summarize(name, rows))
        if not args.json:
            print(f"MSI regime band vs flow DIRECTION — gamma structure fixed at {name!r}\n")
            print(format_report(rows))
            print()

    if args.json:
        print(json.dumps({"sweeps": payload, "summary": summaries}, indent=2, default=str))
        return 0

    print("=" * 90)
    print("SUMMARY — MSI movement caused by flow direction alone, structure held constant")
    print("=" * 90)
    print(f"{'structure':>10} {'MSI min':>9} {'MSI max':>9} {'span':>7} {'bands':>6}  band path")
    for s in summaries:
        print(
            f"{s['structure']:>10} {s['msi_min']:>9.2f} {s['msi_max']:>9.2f} "
            f"{s['msi_span']:>7.2f} {s['distinct_bands']:>6}  {' -> '.join(s['band_path'])}"
        )
    print()
    print(
        "frontend/core/impliedDirection.ts states of this same 0-100 number: \"It is\n"
        "deliberately directionless.\" Each band crossed above is a different\n"
        "customer-facing claim about how far price will travel, produced by an input\n"
        "that carries no information about how far price will travel — only which way."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
