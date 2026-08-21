"""The experiment battery: does MM attribution beat the existing model?

Structured around the one question that decides whether this is worth further
investment — *does participant-attributed Market Maker positioning carry
information ZeroGEX does not already have?* — and deliberately structured so a
negative answer is as easy to reach as a positive one.

Four families of test, matching requirements 9 and 11:

``gamma_regime``
    Does ``gamma@spot > 0`` under each methodology sort subsequent realized
    volatility, intraday range, mean reversion, trend persistence, VWAP
    behavior and large directional moves?  Run for both, compared head to head.

``gamma_flip``
    Above / below / crossing / rejecting each flip, across forward horizons.

``walls``
    Do the wall levels show rejection, pinning, stalling or break
    acceleration — and does either methodology's wall show it more?

``incremental``
    The question the other three cannot answer: fit a baseline model on the
    existing ZeroGEX variables, then the same model plus the MM-attributed
    ones, on identical rows.  Report the change in adjusted R² / McFadden R²,
    an F-test on the added terms, HAC t-statistics, AUC, Brier and calibration,
    and walk-forward out-of-sample versions of each.

Guardrails that are part of the method, not decoration:

* **Overlap.** Forward windows overlap heavily at minute resolution.  Every
  regression carries HAC standard errors and every bootstrap resamples blocks.
* **Multiplicity.** Dozens of tests run; p-values go through Benjamini-Hochberg
  as a family and the corrected verdict is what the report reads.
* **Power.** Below :data:`MIN_SAMPLE_FOR_CONCLUSION` observations a result is
  labelled ``insufficient_sample`` and no conclusion is drawn from it.
* **Confounders.** Every headline test also runs inside the control splits
  (0DTE vs not, time of day, VIX regime, OPEX, gamma concentration), so an
  effect that is really a 0DTE effect cannot be reported as an attribution
  effect.
* **Confidence gating.** Results are additionally reported restricted to
  snapshots whose inventory confidence is high, which is what separates "the
  methodology failed" from "the reconstruction was too incomplete to tell".
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping, Optional, Sequence

import numpy as np

from research.mm_attributed_gex import stats as st
from research.mm_attributed_gex.outcomes import (
    DEFAULT_HORIZONS,
    BarSeries,
    compute_outcomes,
)

__all__ = [
    "MIN_SAMPLE_FOR_CONCLUSION",
    "ExperimentConfig",
    "ExperimentResult",
    "attach_outcomes",
    "run_experiment",
    "gamma_regime_test",
    "gamma_flip_test",
    "wall_test",
    "incremental_value_test",
    "confluence_test",
]

#: Below this many observations a comparison is reported but explicitly not
#: concluded from.  Chosen so a two-sample mean difference of ~0.3σ is
#: detectable at 80% power — smaller samples cannot distinguish a real effect
#: from noise, and saying so is more useful than a p-value.
MIN_SAMPLE_FOR_CONCLUSION = 200


@dataclass(frozen=True)
class ExperimentConfig:
    horizons: tuple[int, ...] = DEFAULT_HORIZONS
    #: Proximity band (fraction of spot) for "price is at the wall".
    wall_proximity_pct: float = 0.0015
    #: Move beyond a wall that counts as a break.
    wall_break_pct: float = 0.0015
    #: Band around the flip that counts as "at the flip".
    flip_proximity_pct: float = 0.0015
    #: Inventory-confidence floor for the high-confidence arm.
    confidence_floor: float = 0.60
    #: Minimum share of the production gamma universe that must be
    #: reconstructed for a snapshot to enter the high-confidence arm.
    min_gamma_coverage: float = 0.25
    n_boot: int = 2000
    n_perm: int = 5000
    walk_forward_folds: int = 5
    seed: int = 20260821
    large_move_bps: float = 50.0


@dataclass
class ExperimentResult:
    """Every test's output plus the provenance needed to read it."""

    n_rows: int = 0
    n_scored: int = 0
    coverage: dict[str, Any] = field(default_factory=dict)
    gamma_regime: dict[str, Any] = field(default_factory=dict)
    gamma_flip: dict[str, Any] = field(default_factory=dict)
    walls: dict[str, Any] = field(default_factory=dict)
    incremental: dict[str, Any] = field(default_factory=dict)
    confluence: dict[str, Any] = field(default_factory=dict)
    controls: dict[str, Any] = field(default_factory=dict)
    multiplicity: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_rows": self.n_rows,
            "n_scored": self.n_scored,
            "coverage": self.coverage,
            "gamma_regime": self.gamma_regime,
            "gamma_flip": self.gamma_flip,
            "walls": self.walls,
            "incremental": self.incremental,
            "confluence": self.confluence,
            "controls": self.controls,
            "multiplicity": self.multiplicity,
            "warnings": self.warnings,
        }


# ---------------------------------------------------------------------------
# Joining readings to outcomes
# ---------------------------------------------------------------------------


def attach_outcomes(
    rows: Sequence[Mapping[str, Any]],
    series: BarSeries,
    *,
    config: ExperimentConfig = ExperimentConfig(),
) -> list[dict[str, Any]]:
    """Join each dataset row to its forward outcomes.

    Rows without a usable forward window are dropped and counted — silently
    keeping them would bias every horizon toward the middle of the session.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        if not isinstance(ts, datetime):
            continue
        outcome = compute_outcomes(
            series,
            ts,
            horizons=config.horizons,
            large_move_bps=config.large_move_bps,
        )
        if outcome is None:
            continue
        merged = dict(row)
        merged.update(outcome.flat(config.horizons))
        merged["timestamp_dt"] = ts
        out.append(merged)
    out.sort(key=lambda r: r["timestamp_dt"])
    return out


def _column(rows: Sequence[Mapping[str, Any]], name: str) -> list[Optional[float]]:
    return [r.get(name) for r in rows]


def _numeric(rows: Sequence[Mapping[str, Any]], name: str) -> np.ndarray:
    return np.array(
        [float(r[name]) if isinstance(r.get(name), (int, float)) else np.nan for r in rows],
        dtype=float,
    )


def _split_by_sign(
    rows: Sequence[Mapping[str, Any]], column: str
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """``(positive, negative)`` by the sign of ``column``; nulls dropped."""
    pos, neg = [], []
    for r in rows:
        v = r.get(column)
        if not isinstance(v, (int, float)) or v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        (pos if v > 0 else neg).append(r)
    return pos, neg


def _values(rows: Sequence[Mapping[str, Any]], column: str) -> list[float]:
    out: list[float] = []
    for r in rows:
        v = r.get(column)
        if isinstance(v, bool):
            out.append(1.0 if v else 0.0)
        elif (
            isinstance(v, (int, float))
            and v is not None
            and not (isinstance(v, float) and math.isnan(v))
        ):
            out.append(float(v))
    return out


def _verdict(n_a: int, n_b: int) -> Optional[str]:
    if min(n_a, n_b) < MIN_SAMPLE_FOR_CONCLUSION:
        return "insufficient_sample"
    return None


def _block_size(horizon_minutes: int, sampling_minutes: int = 5) -> int:
    """Block length that spans one overlapping forward window."""
    return max(1, int(math.ceil(horizon_minutes / max(1, sampling_minutes))))


# ---------------------------------------------------------------------------
# 1. Gamma regime
# ---------------------------------------------------------------------------

_REGIME_MEASURES: tuple[tuple[str, str], ...] = (
    ("realized_vol", "rvol_{h}m_bps"),
    ("abs_return", "abs_ret_{h}m_bps"),
    ("intraday_range", "range_{h}m_bps"),
    ("mean_reversion", "mean_reversion_{h}m"),
    ("trend_persistence", "continuation_{h}m"),
    ("vwap_reversion", "vwap_reversion_{h}m_bps"),
    ("large_move_rate", "large_move_{h}m"),
)


def _regime_arm(
    rows: Sequence[Mapping[str, Any]],
    signal_column: str,
    config: ExperimentConfig,
    sampling_minutes: int,
) -> dict[str, Any]:
    """One methodology's regime discrimination across horizons and measures."""
    pos, neg = _split_by_sign(rows, signal_column)
    arm: dict[str, Any] = {
        "signal": signal_column,
        "n_long_gamma": len(pos),
        "n_short_gamma": len(neg),
        "measures": {},
    }
    note = _verdict(len(pos), len(neg))
    if note:
        arm["note"] = note

    for label, template in _REGIME_MEASURES:
        per_h: dict[str, Any] = {}
        for h in config.horizons:
            col = template.format(h=h)
            a = _values(pos, col)
            b = _values(neg, col)
            if len(a) < 5 or len(b) < 5:
                continue
            cmp_ = st.compare_means(a, b)
            boot = st.block_bootstrap_diff(
                a,
                b,
                n_boot=config.n_boot,
                block_size=_block_size(h, sampling_minutes),
                seed=config.seed,
            )
            entry: dict[str, Any] = {
                "long_gamma": st.describe(a).as_dict(),
                "short_gamma": st.describe(b).as_dict(),
                "welch": cmp_.as_dict(),
                "block_bootstrap": boot,
            }
            # Rate-style measures get a proportion test as well.
            if label in ("trend_persistence", "large_move_rate"):
                entry["proportion"] = st.compare_proportions(
                    int(sum(a)), len(a), int(sum(b)), len(b)
                ).as_dict()
            per_h[f"{h}m"] = entry
        # Session-scale versions where they exist.
        session_col = {
            "realized_vol": "realized_vol_to_close_bps",
            "abs_return": "abs_ret_to_close_bps",
            "intraday_range": "range_to_close_bps",
        }.get(label)
        if session_col:
            a = _values(pos, session_col)
            b = _values(neg, session_col)
            if len(a) >= 5 and len(b) >= 5:
                per_h["to_close"] = {
                    "long_gamma": st.describe(a).as_dict(),
                    "short_gamma": st.describe(b).as_dict(),
                    "welch": st.compare_means(a, b).as_dict(),
                }
        if per_h:
            arm["measures"][label] = per_h
    return arm


def gamma_regime_test(
    rows: Sequence[Mapping[str, Any]],
    config: ExperimentConfig = ExperimentConfig(),
    *,
    sampling_minutes: int = 5,
) -> dict[str, Any]:
    """Compare regime discrimination: existing gamma@spot vs MM-attributed.

    The comparison that matters is not "does either separate the regimes" (both
    should) but *which separates them more*, measured on the same rows by the
    same statistic.  ``separation`` reports each arm's effect size per measure
    and horizon, and ``better`` names the winner only when the sample supports
    a call.
    """
    existing = _regime_arm(rows, "existing_dealer_gamma_at_spot", config, sampling_minutes)
    mm = _regime_arm(rows, "mm_attributed_gamma_at_spot", config, sampling_minutes)

    separation: dict[str, Any] = {}
    for label, _ in _REGIME_MEASURES:
        e_meas = existing["measures"].get(label, {})
        m_meas = mm["measures"].get(label, {})
        for horizon in sorted(set(e_meas) | set(m_meas)):
            e = e_meas.get(horizon, {}).get("welch", {})
            m = m_meas.get(horizon, {}).get("welch", {})
            e_d = e.get("cohens_d")
            m_d = m.get("cohens_d")
            if e_d is None or m_d is None:
                continue
            key = f"{label}.{horizon}"
            better: Optional[str]
            if existing.get("note") or mm.get("note"):
                better = None
            elif abs(m_d) > abs(e_d) * 1.10:
                better = "mm_attributed"
            elif abs(e_d) > abs(m_d) * 1.10:
                better = "existing"
            else:
                better = "tie"
            separation[key] = {
                "existing_effect_size": e_d,
                "mm_effect_size": m_d,
                "existing_p": e.get("p_value"),
                "mm_p": m.get("p_value"),
                "abs_effect_ratio": (abs(m_d) / abs(e_d)) if e_d else None,
                "better": better,
            }
    return {"existing": existing, "mm_attributed": mm, "separation": separation}


# ---------------------------------------------------------------------------
# 2. Gamma flip
# ---------------------------------------------------------------------------


def _flip_state(row: Mapping[str, Any], flip_col: str, config: ExperimentConfig) -> Optional[str]:
    flip = row.get(flip_col)
    spot = row.get("spot")
    if not isinstance(flip, (int, float)) or not isinstance(spot, (int, float)) or not spot:
        return None
    band = float(spot) * config.flip_proximity_pct
    if abs(float(spot) - float(flip)) <= band:
        return "at"
    return "above" if float(spot) > float(flip) else "below"


def _flip_arm(
    rows: Sequence[Mapping[str, Any]],
    flip_col: str,
    config: ExperimentConfig,
    sampling_minutes: int,
) -> dict[str, Any]:
    states: dict[str, list[Mapping[str, Any]]] = {"above": [], "below": [], "at": []}
    prev_state: Optional[str] = None
    crossings: list[Mapping[str, Any]] = []

    for row in rows:
        state = _flip_state(row, flip_col, config)
        if state is None:
            prev_state = None
            continue
        states[state].append(row)
        if prev_state and prev_state != state and "at" not in (prev_state, state):
            crossings.append({**row, "crossed_from": prev_state})
        prev_state = state

    # "At the flip" rows that carry a usable 30-minute forward path — the
    # canonical intraday window for reading rejection vs continuation.
    at_flip_rows = [r for r in states["at"] if isinstance(r.get("ret_30m_bps"), (int, float))]

    arm: dict[str, Any] = {
        "flip_column": flip_col,
        "n_above": len(states["above"]),
        "n_below": len(states["below"]),
        "n_at": len(states["at"]),
        "n_crossings": len(crossings),
        "unresolved_rate": (
            float(np.mean([1.0 if r.get(flip_col) is None else 0.0 for r in rows]))
            if rows
            else None
        ),
        "by_horizon": {},
    }
    note = _verdict(len(states["above"]), len(states["below"]))
    if note:
        arm["note"] = note

    for h in config.horizons:
        block = _block_size(h, sampling_minutes)
        above_vol = _values(states["above"], f"rvol_{h}m_bps")
        below_vol = _values(states["below"], f"rvol_{h}m_bps")
        above_ret = _values(states["above"], f"ret_{h}m_bps")
        below_ret = _values(states["below"], f"ret_{h}m_bps")
        if len(above_vol) < 5 or len(below_vol) < 5:
            continue
        arm["by_horizon"][f"{h}m"] = {
            "realized_vol": {
                "above": st.describe(above_vol).as_dict(),
                "below": st.describe(below_vol).as_dict(),
                "welch": st.compare_means(above_vol, below_vol).as_dict(),
                "block_bootstrap": st.block_bootstrap_diff(
                    above_vol, below_vol, n_boot=config.n_boot, block_size=block, seed=config.seed
                ),
            },
            "signed_return": {
                "above": st.describe(above_ret).as_dict(),
                "below": st.describe(below_ret).as_dict(),
                "welch": st.compare_means(above_ret, below_ret).as_dict(),
            },
            "continuation_above": st.describe(
                _values(states["above"], f"continuation_{h}m")
            ).as_dict(),
            "continuation_below": st.describe(
                _values(states["below"], f"continuation_{h}m")
            ).as_dict(),
        }

    if crossings:
        arm["crossings"] = {
            f"{h}m": {
                "realized_vol": st.describe(_values(crossings, f"rvol_{h}m_bps")).as_dict(),
                "abs_return": st.describe(_values(crossings, f"abs_ret_{h}m_bps")).as_dict(),
                "large_move_rate": st.describe(_values(crossings, f"large_move_{h}m")).as_dict(),
            }
            for h in config.horizons
        }
    if at_flip_rows:
        arm["at_flip"] = {
            f"{h}m": {
                "abs_return": st.describe(_values(at_flip_rows, f"abs_ret_{h}m_bps")).as_dict(),
                "mean_reversion": st.describe(
                    _values(at_flip_rows, f"mean_reversion_{h}m")
                ).as_dict(),
            }
            for h in config.horizons
        }
    return arm


def gamma_flip_test(
    rows: Sequence[Mapping[str, Any]],
    config: ExperimentConfig = ExperimentConfig(),
    *,
    sampling_minutes: int = 5,
) -> dict[str, Any]:
    """Above / below / crossing / at each flip, per horizon, both methodologies."""
    existing = _flip_arm(rows, "existing_gamma_flip", config, sampling_minutes)
    mm = _flip_arm(rows, "mm_attributed_gamma_flip", config, sampling_minutes)

    comparison: dict[str, Any] = {}
    for h in config.horizons:
        key = f"{h}m"
        e = existing["by_horizon"].get(key, {}).get("realized_vol", {}).get("welch", {})
        m = mm["by_horizon"].get(key, {}).get("realized_vol", {}).get("welch", {})
        if not e or not m:
            continue
        e_d, m_d = e.get("cohens_d"), m.get("cohens_d")
        if e_d is None or m_d is None:
            continue
        comparison[key] = {
            "existing_effect_size": e_d,
            "mm_effect_size": m_d,
            "better": (
                None
                if (existing.get("note") or mm.get("note"))
                else (
                    "mm_attributed"
                    if abs(m_d) > abs(e_d) * 1.10
                    else ("existing" if abs(e_d) > abs(m_d) * 1.10 else "tie")
                )
            ),
        }
    return {
        "existing": existing,
        "mm_attributed": mm,
        "comparison": comparison,
        "agreement": _flip_agreement(rows),
    }


def _flip_agreement(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """How often the two flips put spot on the same side, and how far apart."""
    same = 0
    total = 0
    gaps: list[float] = []
    for row in rows:
        e, m, spot = (
            row.get("existing_gamma_flip"),
            row.get("mm_attributed_gamma_flip"),
            row.get("spot"),
        )
        if not all(isinstance(v, (int, float)) for v in (e, m, spot)):
            continue
        total += 1
        if (float(spot) > float(e)) == (float(spot) > float(m)):
            same += 1
        gaps.append(10_000.0 * (float(m) - float(e)) / float(spot))
    return {
        "n": total,
        "same_side_rate": (same / total) if total else None,
        "flip_gap_bps": st.describe(gaps).as_dict() if gaps else None,
    }


# ---------------------------------------------------------------------------
# 3. Walls
# ---------------------------------------------------------------------------


def _wall_events(
    rows: Sequence[Mapping[str, Any]],
    wall_col: str,
    side: str,
    config: ExperimentConfig,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """``(at_wall, broke_wall)`` for one wall column.

    ``side`` is ``"call"`` (resistance above spot) or ``"put"`` (support
    below).  "At the wall" means spot within the proximity band; "broke" means
    the reading was at the wall and the 30-minute close finished beyond it by
    more than the break threshold.
    """
    at: list[Mapping[str, Any]] = []
    broke: list[Mapping[str, Any]] = []
    for row in rows:
        wall, spot = row.get(wall_col), row.get("spot")
        if not isinstance(wall, (int, float)) or not isinstance(spot, (int, float)) or not spot:
            continue
        spot_f, wall_f = float(spot), float(wall)
        if abs(spot_f - wall_f) / spot_f > config.wall_proximity_pct:
            continue
        at.append(row)
        ret = row.get("ret_30m_bps")
        if not isinstance(ret, (int, float)):
            continue
        end = spot_f * (1.0 + float(ret) / 10_000.0)
        threshold = wall_f * config.wall_break_pct
        if side == "call" and end > wall_f + threshold:
            broke.append(row)
        elif side == "put" and end < wall_f - threshold:
            broke.append(row)
    return at, broke


def _wall_arm(
    rows: Sequence[Mapping[str, Any]],
    wall_col: str,
    side: str,
    config: ExperimentConfig,
) -> dict[str, Any]:
    at, broke = _wall_events(rows, wall_col, side, config)
    broke_ids = {id(r) for r in broke}
    held = [r for r in at if id(r) not in broke_ids]
    arm: dict[str, Any] = {
        "wall_column": wall_col,
        "side": side,
        "n_at_wall": len(at),
        "n_broke": len(broke),
        "break_rate": (len(broke) / len(at)) if at else None,
        "hold_rate": (len(held) / len(at)) if at else None,
    }
    if len(at) < 30:
        arm["note"] = "insufficient_sample"
        return arm

    for h in config.horizons:
        arm[f"{h}m"] = {
            # Pinning / stalling: small absolute move and high retracement.
            "abs_return": st.describe(_values(at, f"abs_ret_{h}m_bps")).as_dict(),
            "range": st.describe(_values(at, f"range_{h}m_bps")).as_dict(),
            "mean_reversion": st.describe(_values(at, f"mean_reversion_{h}m")).as_dict(),
            "realized_vol": st.describe(_values(at, f"rvol_{h}m_bps")).as_dict(),
        }
        if broke and held:
            a = _values(broke, f"rvol_{h}m_bps")
            b = _values(held, f"rvol_{h}m_bps")
            if len(a) >= 5 and len(b) >= 5:
                # Break acceleration: does a break carry more realized vol
                # than a hold?
                arm[f"{h}m"]["break_acceleration"] = st.compare_means(a, b).as_dict()

    # Rejection: at a call wall, price should fall away; at a put wall, rise.
    rets = _values(at, "ret_30m_bps")
    if rets:
        rejected = [r for r in rets if (r < 0 if side == "call" else r > 0)]
        arm["rejection_rate_30m"] = len(rejected) / len(rets)
        arm["signed_return_30m"] = st.describe(rets).as_dict()
    return arm


def wall_test(
    rows: Sequence[Mapping[str, Any]], config: ExperimentConfig = ExperimentConfig()
) -> dict[str, Any]:
    """Existing walls vs MM-attributed walls (definitions A and B)."""
    arms = {
        "existing_call_wall": _wall_arm(rows, "existing_call_wall", "call", config),
        "existing_put_wall": _wall_arm(rows, "existing_put_wall", "put", config),
        "mm_attributed_call_wall_a": _wall_arm(rows, "mm_attributed_call_wall", "call", config),
        "mm_attributed_put_wall_a": _wall_arm(rows, "mm_attributed_put_wall", "put", config),
        "mm_attributed_call_wall_b": _wall_arm(rows, "mm_attributed_b_call_wall", "call", config),
        "mm_attributed_put_wall_b": _wall_arm(rows, "mm_attributed_b_put_wall", "put", config),
    }

    comparison: dict[str, Any] = {}
    for side, existing_key, a_key, b_key in (
        (
            "call",
            "existing_call_wall",
            "mm_attributed_call_wall_a",
            "mm_attributed_call_wall_b",
        ),
        ("put", "existing_put_wall", "mm_attributed_put_wall_a", "mm_attributed_put_wall_b"),
    ):
        e, a, b = arms[existing_key], arms[a_key], arms[b_key]
        entry: dict[str, Any] = {
            "existing_rejection_rate_30m": e.get("rejection_rate_30m"),
            "mm_a_rejection_rate_30m": a.get("rejection_rate_30m"),
            "mm_b_rejection_rate_30m": b.get("rejection_rate_30m"),
            "existing_n": e.get("n_at_wall"),
            "mm_a_n": a.get("n_at_wall"),
            "mm_b_n": b.get("n_at_wall"),
        }
        if (
            e.get("rejection_rate_30m") is not None
            and a.get("rejection_rate_30m") is not None
            and e.get("n_at_wall")
            and a.get("n_at_wall")
        ):
            entry["existing_vs_mm_a"] = st.compare_proportions(
                int(round(a["rejection_rate_30m"] * a["n_at_wall"])),
                a["n_at_wall"],
                int(round(e["rejection_rate_30m"] * e["n_at_wall"])),
                e["n_at_wall"],
            ).as_dict()
        comparison[side] = entry

    agreement = _wall_agreement(rows)
    return {"arms": arms, "comparison": comparison, "agreement": agreement}


def _wall_agreement(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """How often existing and MM-attributed walls pick the same strike."""
    out: dict[str, Any] = {}
    for label, existing_col, mm_col in (
        ("call", "existing_call_wall", "mm_attributed_call_wall"),
        ("put", "existing_put_wall", "mm_attributed_put_wall"),
    ):
        same = 0
        total = 0
        gaps: list[float] = []
        for row in rows:
            e, m, spot = row.get(existing_col), row.get(mm_col), row.get("spot")
            if not all(isinstance(v, (int, float)) for v in (e, m, spot)) or not spot:
                continue
            total += 1
            if float(e) == float(m):
                same += 1
            gaps.append(10_000.0 * (float(m) - float(e)) / float(spot))
        out[label] = {
            "n": total,
            "same_strike_rate": (same / total) if total else None,
            "gap_bps": st.describe(gaps).as_dict() if gaps else None,
        }
    return out


# ---------------------------------------------------------------------------
# 4. Incremental value
# ---------------------------------------------------------------------------


def _standardize(values: np.ndarray) -> np.ndarray:
    """Z-score with a zero-variance guard.

    Standardizing puts a dollar-gamma coefficient and a strike-distance
    coefficient on a comparable scale, which is the only way the regression
    output is readable side by side.
    """
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return values
    mu = float(finite.mean())
    sd = float(finite.std(ddof=1)) if finite.size > 1 else 0.0
    if sd <= 0:
        return values - mu
    return (values - mu) / sd


def _build_features(rows: Sequence[Mapping[str, Any]]) -> dict[str, np.ndarray]:
    """Baseline (existing) and MM-attributed predictor columns, standardized."""
    spot = _numeric(rows, "spot")

    def _dist(col: str) -> np.ndarray:
        level = _numeric(rows, col)
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(spot > 0, 10_000.0 * (spot - level) / spot, np.nan)

    def _signed_log(col: str) -> np.ndarray:
        """Sign-preserving log of a dollar-gamma column.

        Dollar gamma spans several orders of magnitude; a raw level would let
        a handful of extreme snapshots dominate the fit, and dropping the sign
        would discard the regime information that is the whole point.
        """
        v = _numeric(rows, col)
        return np.sign(v) * np.log1p(np.abs(v))

    features = {
        # Baseline — what ZeroGEX already knows.
        "existing_gamma_at_spot": _signed_log("existing_dealer_gamma_at_spot"),
        "existing_net_gex": _signed_log("existing_net_gex"),
        "existing_flip_distance_bps": _dist("existing_gamma_flip"),
        "existing_call_wall_distance_bps": _dist("existing_call_wall"),
        "existing_put_wall_distance_bps": _dist("existing_put_wall"),
        # Enhanced — what MM attribution adds.
        "mm_gamma_at_spot": _signed_log("mm_attributed_gamma_at_spot"),
        "mm_net_gex": _signed_log("mm_attributed_net_gex"),
        "mm_flip_distance_bps": _dist("mm_attributed_gamma_flip"),
        "mm_negative_gamma_share": _numeric(rows, "mm_negative_gamma_share"),
        "mm_concentration_hhi": _numeric(rows, "mm_concentration_hhi"),
    }
    return {k: _standardize(v) for k, v in features.items()}


_BASELINE_TERMS = (
    "existing_gamma_at_spot",
    "existing_net_gex",
    "existing_flip_distance_bps",
    "existing_call_wall_distance_bps",
    "existing_put_wall_distance_bps",
)
_MM_TERMS = (
    "mm_gamma_at_spot",
    "mm_net_gex",
    "mm_flip_distance_bps",
    "mm_negative_gamma_share",
    "mm_concentration_hhi",
)


def incremental_value_test(
    rows: Sequence[Mapping[str, Any]],
    config: ExperimentConfig = ExperimentConfig(),
    *,
    sampling_minutes: int = 5,
) -> dict[str, Any]:
    """Baseline ZeroGEX vs baseline + MM-attributed, on identical rows.

    Three questions, each answered in-sample and out-of-sample:

    * **Explanatory** — does adding the MM terms raise adjusted R² on forward
      realized volatility by more than chance (F-test + HAC t-statistics)?
    * **Classification** — does it raise AUC / lower Brier and log-loss for
      "will the next window produce a large directional move"?
    * **Predictive** — do those gains survive walk-forward, where the model
      never sees the period it is scored on?
    """
    if len(rows) < 50:
        return {"ok": False, "reason": "insufficient rows", "n": len(rows)}

    features = _build_features(rows)
    baseline = {k: features[k] for k in _BASELINE_TERMS}
    enhanced = {**baseline, **{k: features[k] for k in _MM_TERMS}}

    out: dict[str, Any] = {"ok": True, "n_rows": len(rows), "horizons": {}}
    for h in config.horizons:
        lags = _block_size(h, sampling_minutes)
        horizon_out: dict[str, Any] = {}

        y_vol = _numeric(rows, f"rvol_{h}m_bps")
        if np.isfinite(y_vol).sum() >= 50:
            horizon_out["realized_vol_regression"] = st.nested_model_test(
                y_vol, baseline, enhanced, hac_lags=lags
            )

        y_abs = _numeric(rows, f"abs_ret_{h}m_bps")
        if np.isfinite(y_abs).sum() >= 50:
            horizon_out["abs_return_regression"] = st.nested_model_test(
                y_abs, baseline, enhanced, hac_lags=lags
            )

        y_large = _numeric(rows, f"large_move_{h}m")
        if np.isfinite(y_large).sum() >= 50 and 0 < np.nansum(y_large) < np.isfinite(y_large).sum():
            horizon_out["large_move_classification"] = _classification_compare(
                y_large, baseline, enhanced
            )

        wf = _walk_forward_compare(rows, baseline, enhanced, f"rvol_{h}m_bps", config, embargo=lags)
        if wf:
            horizon_out["walk_forward_realized_vol"] = wf

        if horizon_out:
            out["horizons"][f"{h}m"] = horizon_out
    return out


def _classification_compare(
    y: np.ndarray,
    baseline: Mapping[str, np.ndarray],
    enhanced: Mapping[str, np.ndarray],
) -> dict[str, Any]:
    """In-sample classification metrics for both models on aligned rows."""
    mask = np.isfinite(y)
    for v in enhanced.values():
        mask &= np.isfinite(v)
    if mask.sum() < 50:
        return {"ok": False, "reason": "insufficient aligned rows"}
    y_m = y[mask]
    base = st.logistic_regression(y_m, {k: v[mask] for k, v in baseline.items()})
    full = st.logistic_regression(y_m, {k: v[mask] for k, v in enhanced.items()})
    if base is None or full is None:
        return {"ok": False, "reason": "logit did not fit"}

    lr_stat = 2.0 * (full.log_likelihood - base.log_likelihood)
    df = len(enhanced) - len(baseline)
    p_value = None
    if st.HAVE_SCIPY and df > 0:
        from scipy import stats as _s

        p_value = float(_s.chi2.sf(max(0.0, lr_stat), df))
    return {
        "ok": True,
        "n": int(mask.sum()),
        "positive_rate": float(y_m.mean()),
        "baseline": {
            "auc": st.auc(y_m, base.fitted),
            "brier": st.brier_score(y_m, base.fitted),
            "log_loss": st.log_loss(y_m, base.fitted),
            "mcfadden_r2": base.mcfadden_r2,
        },
        "enhanced": {
            "auc": st.auc(y_m, full.fitted),
            "brier": st.brier_score(y_m, full.fitted),
            "log_loss": st.log_loss(y_m, full.fitted),
            "mcfadden_r2": full.mcfadden_r2,
            "terms": full.as_dict()["terms"],
        },
        "delta_auc": ((st.auc(y_m, full.fitted) or 0.0) - (st.auc(y_m, base.fitted) or 0.0)),
        "likelihood_ratio": {"stat": lr_stat, "df": df, "p_value": p_value},
        "calibration_enhanced": st.calibration_bins(y_m, full.fitted),
    }


def _walk_forward_compare(
    rows: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, np.ndarray],
    enhanced: Mapping[str, np.ndarray],
    target: str,
    config: ExperimentConfig,
    *,
    embargo: int,
) -> Optional[dict[str, Any]]:
    """Out-of-sample R² for both models across expanding walk-forward folds.

    Fits on the training block only and scores on the held-out block, with an
    embargo between them so the overlapping forward window of the last training
    row cannot reach into the test period.  Out-of-sample R² is computed
    against the *training* mean, which is the honest baseline a live model
    would have had.
    """
    y = _numeric(rows, target)
    mask = np.isfinite(y)
    for v in enhanced.values():
        mask &= np.isfinite(v)
    idx = np.flatnonzero(mask)
    if idx.size < 200:
        return None
    y_m = y[idx]
    base_m = {k: v[idx] for k, v in baseline.items()}
    full_m = {k: v[idx] for k, v in enhanced.items()}

    splits = st.walk_forward_splits(
        idx.size,
        n_folds=config.walk_forward_folds,
        min_train=max(100, idx.size // 3),
        embargo=embargo,
    )
    if not splits:
        return None

    def _fit_predict(cols: Mapping[str, np.ndarray], tr: range, te: range):
        X_tr = np.column_stack([np.ones(len(tr))] + [cols[k][tr.start : tr.stop] for k in cols])
        X_te = np.column_stack([np.ones(len(te))] + [cols[k][te.start : te.stop] for k in cols])
        beta = np.linalg.pinv(X_tr.T @ X_tr) @ (X_tr.T @ y_m[tr.start : tr.stop])
        return X_te @ beta, y_m[tr.start : tr.stop].mean()

    folds: list[dict[str, Any]] = []
    for tr, te in splits:
        y_te = y_m[te.start : te.stop]
        if y_te.size < 10:
            continue
        pred_b, train_mean = _fit_predict(base_m, tr, te)
        pred_f, _ = _fit_predict(full_m, tr, te)
        ss_tot = float(((y_te - train_mean) ** 2).sum())
        if ss_tot <= 0:
            continue
        r2_b = 1.0 - float(((y_te - pred_b) ** 2).sum()) / ss_tot
        r2_f = 1.0 - float(((y_te - pred_f) ** 2).sum()) / ss_tot
        folds.append(
            {
                "n_train": len(tr),
                "n_test": int(y_te.size),
                "baseline_oos_r2": r2_b,
                "enhanced_oos_r2": r2_f,
                "delta_oos_r2": r2_f - r2_b,
            }
        )
    if not folds:
        return None
    deltas = [f["delta_oos_r2"] for f in folds]
    return {
        "folds": folds,
        "mean_delta_oos_r2": float(np.mean(deltas)),
        "folds_improved": int(sum(1 for d in deltas if d > 0)),
        "n_folds": len(folds),
        "embargo": embargo,
    }


# ---------------------------------------------------------------------------
# 5. Confluence
# ---------------------------------------------------------------------------


def confluence_test(
    rows: Sequence[Mapping[str, Any]],
    config: ExperimentConfig = ExperimentConfig(),
) -> dict[str, Any]:
    """Do the two methodologies agreeing beat either one alone?

    Requirement 9 is explicit that the strongest result may be confluence
    rather than replacement, so this is a first-class test, not an afterthought.
    Four buckets by the sign pair of the two gamma@spot readings; if agreement
    carries a larger effect than either signal alone, the answer to the central
    question is "useful as confirmation".
    """
    buckets: dict[str, list[Mapping[str, Any]]] = {
        "both_long": [],
        "both_short": [],
        "existing_long_mm_short": [],
        "existing_short_mm_long": [],
    }
    for row in rows:
        e, m = row.get("existing_dealer_gamma_at_spot"), row.get("mm_attributed_gamma_at_spot")
        if not isinstance(e, (int, float)) or not isinstance(m, (int, float)):
            continue
        if e > 0 and m > 0:
            buckets["both_long"].append(row)
        elif e <= 0 and m <= 0:
            buckets["both_short"].append(row)
        elif e > 0:
            buckets["existing_long_mm_short"].append(row)
        else:
            buckets["existing_short_mm_long"].append(row)

    out: dict[str, Any] = {
        "counts": {k: len(v) for k, v in buckets.items()},
        "agreement_rate": (
            (len(buckets["both_long"]) + len(buckets["both_short"])) / len(rows) if rows else None
        ),
        "by_horizon": {},
    }
    for h in config.horizons:
        col = f"rvol_{h}m_bps"
        per_bucket = {k: st.describe(_values(v, col)).as_dict() for k, v in buckets.items()}
        entry: dict[str, Any] = {"realized_vol": per_bucket}
        both_long = _values(buckets["both_long"], col)
        both_short = _values(buckets["both_short"], col)
        if len(both_long) >= 5 and len(both_short) >= 5:
            entry["agree_long_vs_agree_short"] = st.compare_means(both_short, both_long).as_dict()
        disagree = _values(
            buckets["existing_long_mm_short"] + buckets["existing_short_mm_long"], col
        )
        agree = both_long + both_short
        if len(disagree) >= 5 and len(agree) >= 5:
            entry["disagree_vs_agree"] = st.compare_means(disagree, agree).as_dict()
        out["by_horizon"][f"{h}m"] = entry
    return out


# ---------------------------------------------------------------------------
# Controls and orchestration
# ---------------------------------------------------------------------------


def _control_splits(
    rows: Sequence[Mapping[str, Any]], config: ExperimentConfig
) -> dict[str, list[Mapping[str, Any]]]:
    """Confounder splits every headline test is re-run inside.

    The point is falsification: an "attribution effect" that appears only in
    the 0DTE subset, or only in high-VIX sessions, is a 0DTE or volatility
    effect wearing a costume.
    """

    def _has(row: Mapping[str, Any], key: str, pred: Callable[[Any], bool]) -> bool:
        v = row.get(key)
        try:
            return v is not None and pred(v)
        except (TypeError, ValueError):
            return False

    vix_values = [r["vix_close"] for r in rows if isinstance(r.get("vix_close"), (int, float))]
    vix_median = float(np.median(vix_values)) if vix_values else None

    splits: dict[str, list[Mapping[str, Any]]] = {
        "0dte_dominant": [r for r in rows if _has(r, "contribution_0dte", lambda v: v >= 0.5)],
        "non_0dte_dominant": [r for r in rows if _has(r, "contribution_0dte", lambda v: v < 0.5)],
        "front_dte_0": [r for r in rows if r.get("dte_front") == 0],
        "front_dte_1_7": [r for r in rows if _has(r, "dte_front", lambda v: 1 <= int(v) <= 7)],
        "front_dte_8plus": [r for r in rows if _has(r, "dte_front", lambda v: int(v) >= 8)],
        "morning": [r for r in rows if _has(r, "session_minute", lambda v: 0 <= int(v) < 120)],
        "midday": [r for r in rows if _has(r, "session_minute", lambda v: 120 <= int(v) < 270)],
        "afternoon": [r for r in rows if _has(r, "session_minute", lambda v: int(v) >= 270)],
        "opex": [r for r in rows if r.get("is_opex")],
        "non_opex": [r for r in rows if not r.get("is_opex")],
        "month_end": [r for r in rows if r.get("is_month_end")],
        "high_concentration": [
            r for r in rows if _has(r, "mm_concentration_hhi", lambda v: v >= 0.15)
        ],
        "low_concentration": [
            r for r in rows if _has(r, "mm_concentration_hhi", lambda v: v < 0.15)
        ],
        "high_confidence": [
            r
            for r in rows
            if _has(r, "inventory_confidence", lambda v: v >= config.confidence_floor)
            and _has(
                r,
                "percent_of_gamma_universe_reconstructed",
                lambda v: v >= config.min_gamma_coverage,
            )
        ],
    }
    if vix_median is not None:
        splits["vix_high"] = [
            r for r in rows if _has(r, "vix_close", lambda v: float(v) >= vix_median)
        ]
        splits["vix_low"] = [
            r for r in rows if _has(r, "vix_close", lambda v: float(v) < vix_median)
        ]
    return {k: v for k, v in splits.items() if v}


def _collect_p_values(payload: Any, path: str = "") -> list[tuple[str, float]]:
    """Walk a result tree and pull out every p-value, with its path."""
    found: list[tuple[str, float]] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            child = f"{path}.{key}" if path else str(key)
            if key in ("p_value", "f_p_value") and isinstance(value, (int, float)):
                if math.isfinite(value):
                    found.append((child, float(value)))
            else:
                found.extend(_collect_p_values(value, child))
    elif isinstance(payload, (list, tuple)):
        for i, value in enumerate(payload):
            found.extend(_collect_p_values(value, f"{path}[{i}]"))
    return found


def run_experiment(
    rows: Sequence[Mapping[str, Any]],
    series: BarSeries,
    *,
    config: ExperimentConfig = ExperimentConfig(),
    sampling_minutes: int = 5,
) -> ExperimentResult:
    """Run the whole battery and return one structured result."""
    result = ExperimentResult(n_rows=len(rows))
    scored = attach_outcomes(rows, series, config=config)
    result.n_scored = len(scored)

    result.coverage = {
        "rows_in": len(rows),
        "rows_scored": len(scored),
        "scored_share": (len(scored) / len(rows)) if rows else 0.0,
        "sessions": len({r.get("trading_date") for r in scored}),
        "mean_inventory_confidence": (
            float(np.mean(_values(scored, "inventory_confidence"))) if scored else None
        ),
        "mean_gamma_coverage": (
            float(np.mean(_values(scored, "percent_of_gamma_universe_reconstructed")))
            if scored
            else None
        ),
        "mm_flip_unresolved_rate": (
            float(np.mean([1.0 if r.get("mm_flip_unresolved") else 0.0 for r in scored]))
            if scored
            else None
        ),
    }
    if not scored:
        result.warnings.append(
            "no rows could be scored against forward bars — check that the "
            "underlying_quotes window covers the dataset timestamps"
        )
        return result
    if len(scored) < MIN_SAMPLE_FOR_CONCLUSION:
        result.warnings.append(
            f"only {len(scored)} scored observations (< {MIN_SAMPLE_FOR_CONCLUSION}); "
            "results are reported but no conclusion should be drawn from them"
        )

    result.gamma_regime = gamma_regime_test(scored, config, sampling_minutes=sampling_minutes)
    result.gamma_flip = gamma_flip_test(scored, config, sampling_minutes=sampling_minutes)
    result.walls = wall_test(scored, config)
    result.incremental = incremental_value_test(scored, config, sampling_minutes=sampling_minutes)
    result.confluence = confluence_test(scored, config)

    controls: dict[str, Any] = {}
    for name, subset in _control_splits(scored, config).items():
        entry: dict[str, Any] = {"n": len(subset)}
        if len(subset) >= 60:
            entry["gamma_regime"] = gamma_regime_test(
                subset, config, sampling_minutes=sampling_minutes
            )["separation"]
        if len(subset) >= 200:
            inc = incremental_value_test(subset, config, sampling_minutes=sampling_minutes)
            entry["incremental_summary"] = _incremental_summary(inc)
        controls[name] = entry
    result.controls = controls

    p_values = _collect_p_values(
        {
            "gamma_regime": result.gamma_regime,
            "gamma_flip": result.gamma_flip,
            "walls": result.walls,
            "incremental": result.incremental,
            "confluence": result.confluence,
        }
    )
    flags = st.benjamini_hochberg([p for _, p in p_values])
    survivors = [path for (path, _), keep in zip(p_values, flags) if keep]
    result.multiplicity = {
        "n_tests": len(p_values),
        "n_significant_uncorrected": sum(1 for _, p in p_values if p < 0.05),
        "n_significant_bh": len(survivors),
        "bh_alpha": 0.05,
        "surviving_tests": survivors[:100],
        "note": (
            "Benjamini-Hochberg across the whole family. With this many tests, "
            "uncorrected counts near 5% of the total are what pure noise produces."
        ),
    }
    return result


def _incremental_summary(inc: Mapping[str, Any]) -> dict[str, Any]:
    """Compress an incremental-value block to its headline numbers."""
    if not inc.get("ok"):
        return {"ok": False, "reason": inc.get("reason")}
    out: dict[str, Any] = {"ok": True, "horizons": {}}
    for horizon, payload in (inc.get("horizons") or {}).items():
        reg = payload.get("realized_vol_regression") or {}
        cls = payload.get("large_move_classification") or {}
        wf = payload.get("walk_forward_realized_vol") or {}
        out["horizons"][horizon] = {
            "delta_adj_r2": reg.get("delta_adj_r2"),
            "f_p_value": reg.get("f_p_value"),
            "delta_auc": cls.get("delta_auc"),
            "lr_p_value": (cls.get("likelihood_ratio") or {}).get("p_value"),
            "mean_delta_oos_r2": wf.get("mean_delta_oos_r2"),
            "folds_improved": wf.get("folds_improved"),
            "n_folds": wf.get("n_folds"),
        }
    return out
