"""Render the research report from a completed experiment.

The verdict logic lives here, written down explicitly and applied mechanically,
so the conclusion follows from the numbers instead of from whoever reads them.
Requirement 17's terminology rules are enforced in the prose: the metric is
*Market-Maker Attributed GEX*, never "true dealer GEX".

Decision order — the first gate that fires wins, and the first two gates fire
*before* any effect is looked at:

1. **Not enough sample** → ``INCONCLUSIVE_SAMPLE``.  A study that cannot detect
   a moderate effect cannot report its absence as evidence.
2. **Reconstruction too incomplete** → ``INCONCLUSIVE_DATA``.  If the inventory
   covers a small share of the gamma universe, or confidence is low, the
   experiment measured the data, not the methodology.  This is the distinction
   the whole diagnostic apparatus exists to make.
3. **Incremental value survives out-of-sample** → ``YES`` (or the scoped
   variants below).
4. **Only in a subset** → ``USEFUL_FOR_0DTE`` / ``USEFUL_IN_REGIMES``.
5. **Agreement beats either alone** → ``USEFUL_AS_CONFIRMATION``.
6. **Otherwise** → ``NO``.

A negative result is a first-class outcome here, and the wording says so.

Three-arm verdict
-----------------
:func:`decide_arms` is the mechanical rule for the A / B / C comparison.  It
never forces a winner: the outcomes are ``PRODUCTION_BETTER``,
``AGGRESSOR_BETTER``, ``ATTRIBUTED_BETTER``, ``PRACTICALLY_EQUIVALENT``,
``INCONCLUSIVE`` and ``INCONCLUSIVE_DATA``.  "Materially better" is defined by
:class:`ArmThresholds` — predeclared effect-size floors on out-of-sample gain
and on the share of head-to-head regime comparisons won — and an arm that
clears them on the full sample but not on the untouched validation segment is
reported as inconclusive rather than as a winner.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from research.mm_attributed_gex.backtest import (
    ARMS,
    FLOW_ARMS,
    MIN_SAMPLE_FOR_CONCLUSION,
    ExperimentResult,
)

__all__ = [
    "Verdict",
    "decide",
    "render_markdown",
    "write_report",
    "ArmThresholds",
    "ARM_VERDICT_LABELS",
    "decide_arms",
    "render_arms_markdown",
]


VERDICT_LABELS = {
    "YES": "Yes — MM-attributed positioning adds material information",
    "NO": "No — MM-attributed positioning does not beat the existing model",
    "INCONCLUSIVE_SAMPLE": "Inconclusive — sample too small to decide",
    "INCONCLUSIVE_DATA": "Inconclusive — inventory reconstruction too incomplete to decide",
    "USEFUL_FOR_0DTE": "Useful primarily for 0DTE",
    "USEFUL_IN_REGIMES": "Useful only in certain regimes",
    "USEFUL_AS_CONFIRMATION": "Useful as confirmation rather than replacement",
}


@dataclass
class Verdict:
    code: str
    label: str
    rationale: list[str]
    evidence: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "label": self.label,
            "rationale": self.rationale,
            "evidence": self.evidence,
        }


def _finite(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        f = float(value)
        return f if math.isfinite(f) else None
    return None


def _incremental_evidence(result: ExperimentResult) -> dict[str, Any]:
    """Collapse the incremental block to the few numbers the verdict needs."""
    return _incremental_from_payload(result.incremental or {})


def _incremental_from_payload(inc: Mapping[str, Any]) -> dict[str, Any]:
    """The same collapse for any incremental-value payload (one arm's block)."""
    horizons = inc.get("horizons") or {}
    oos_deltas: list[float] = []
    in_sample_gain = 0
    in_sample_total = 0
    oos_gain = 0
    oos_total = 0
    auc_gain = 0
    auc_total = 0
    best: dict[str, Any] = {}

    for horizon, payload in horizons.items():
        reg = payload.get("realized_vol_regression") or {}
        if reg.get("ok"):
            in_sample_total += 1
            delta = _finite(reg.get("delta_adj_r2"))
            p = _finite(reg.get("f_p_value"))
            if delta is not None and delta > 0 and p is not None and p < 0.05:
                in_sample_gain += 1
            if delta is not None and (not best or delta > (best.get("delta_adj_r2") or -math.inf)):
                best = {"horizon": horizon, "delta_adj_r2": delta, "f_p_value": p}
        wf = payload.get("walk_forward_realized_vol") or {}
        if wf.get("n_folds"):
            oos_total += 1
            mean_delta = _finite(wf.get("mean_delta_oos_r2"))
            improved = wf.get("folds_improved") or 0
            n_folds = wf.get("n_folds") or 1
            if mean_delta is not None:
                oos_deltas.append(mean_delta)
            if mean_delta is not None and mean_delta > 0 and improved > n_folds / 2:
                oos_gain += 1
        cls = payload.get("large_move_classification") or {}
        if cls.get("ok"):
            auc_total += 1
            d_auc = _finite(cls.get("delta_auc"))
            lr_p = _finite((cls.get("likelihood_ratio") or {}).get("p_value"))
            if d_auc is not None and d_auc > 0.01 and lr_p is not None and lr_p < 0.05:
                auc_gain += 1

    return {
        "in_sample_significant_horizons": in_sample_gain,
        "in_sample_horizons_tested": in_sample_total,
        "out_of_sample_positive_horizons": oos_gain,
        "out_of_sample_horizons_tested": oos_total,
        "classification_gain_horizons": auc_gain,
        "classification_horizons_tested": auc_total,
        "best_in_sample": best,
        "mean_delta_oos_r2": (sum(oos_deltas) / len(oos_deltas)) if oos_deltas else None,
    }


def _regime_evidence(result: ExperimentResult) -> dict[str, Any]:
    separation = (result.gamma_regime or {}).get("separation") or {}
    verdicts = [v.get("better") for v in separation.values() if v.get("better")]
    return {
        "comparisons": len(separation),
        "mm_better": sum(1 for v in verdicts if v == "mm_attributed"),
        "existing_better": sum(1 for v in verdicts if v == "existing"),
        "tie": sum(1 for v in verdicts if v == "tie"),
        "undecided": len(separation) - len(verdicts),
    }


def _confluence_evidence(result: ExperimentResult) -> dict[str, Any]:
    conf = result.confluence or {}
    by_h = conf.get("by_horizon") or {}
    stronger = 0
    tested = 0
    for payload in by_h.values():
        cmp_ = payload.get("disagree_vs_agree") or {}
        d = _finite(cmp_.get("cohens_d"))
        p = _finite(cmp_.get("p_value"))
        if d is None or p is None:
            continue
        tested += 1
        if abs(d) > 0.15 and p < 0.05:
            stronger += 1
    return {
        "agreement_rate": conf.get("agreement_rate"),
        "horizons_where_agreement_matters": stronger,
        "horizons_tested": tested,
        "counts": conf.get("counts"),
    }


def _subset_evidence(result: ExperimentResult) -> dict[str, Any]:
    """Where in the control splits does MM attribution look better?"""
    out: dict[str, Any] = {}
    for name, payload in (result.controls or {}).items():
        separation = payload.get("gamma_regime") or {}
        verdicts = [v.get("better") for v in separation.values() if v.get("better")]
        if not verdicts:
            continue
        mm_better = sum(1 for v in verdicts if v == "mm_attributed")
        out[name] = {
            "n": payload.get("n"),
            "comparisons": len(verdicts),
            "mm_better": mm_better,
            "mm_better_share": mm_better / len(verdicts),
        }
    return out


def decide(result: ExperimentResult) -> Verdict:
    """Apply the decision rules to a completed experiment."""
    rationale: list[str] = []
    coverage = result.coverage or {}
    incremental = _incremental_evidence(result)
    regime = _regime_evidence(result)
    confluence = _confluence_evidence(result)
    subsets = _subset_evidence(result)
    evidence = {
        "coverage": coverage,
        "incremental": incremental,
        "regime": regime,
        "confluence": confluence,
        "subsets": subsets,
        "multiplicity": result.multiplicity,
    }

    # Gate 1 — power.
    n_scored = result.n_scored
    if n_scored < MIN_SAMPLE_FOR_CONCLUSION:
        rationale.append(
            f"Only {n_scored} scored observations, below the {MIN_SAMPLE_FOR_CONCLUSION} "
            "needed to detect a moderate effect. Absence of a result here is not "
            "evidence of absence."
        )
        return Verdict(
            "INCONCLUSIVE_SAMPLE", VERDICT_LABELS["INCONCLUSIVE_SAMPLE"], rationale, evidence
        )

    # Gate 2 — reconstruction completeness.
    mean_conf = _finite(coverage.get("mean_inventory_confidence")) or 0.0
    mean_cov = _finite(coverage.get("mean_gamma_coverage")) or 0.0
    if mean_cov < 0.20 or mean_conf < 0.35:
        rationale.append(
            f"Reconstructed inventory covers only {mean_cov:.1%} of the production gamma "
            f"universe at a mean confidence of {mean_conf:.2f}. At this completeness the "
            "experiment measures the Open-Close history, not the methodology. Obtain "
            "history reaching back to each contract's listing date and re-run."
        )
        return Verdict(
            "INCONCLUSIVE_DATA", VERDICT_LABELS["INCONCLUSIVE_DATA"], rationale, evidence
        )

    # Gate 3 — incremental value that survives out-of-sample.
    oos_ok = (
        incremental["out_of_sample_horizons_tested"] > 0
        and incremental["out_of_sample_positive_horizons"]
        > incremental["out_of_sample_horizons_tested"] / 2
    )
    in_sample_ok = (
        incremental["in_sample_horizons_tested"] > 0
        and incremental["in_sample_significant_horizons"]
        > incremental["in_sample_horizons_tested"] / 2
    )
    if oos_ok and in_sample_ok:
        rationale.append(
            f"Adding MM-attributed variables raised adjusted R² significantly at "
            f"{incremental['in_sample_significant_horizons']}/"
            f"{incremental['in_sample_horizons_tested']} horizons in sample, and the gain "
            f"held out of sample at {incremental['out_of_sample_positive_horizons']}/"
            f"{incremental['out_of_sample_horizons_tested']} horizons under walk-forward."
        )
        # Scope it if the effect lives in one subset.
        zero_dte = subsets.get("0dte_dominant")
        non_zero = subsets.get("non_0dte_dominant")
        if (
            zero_dte
            and non_zero
            and zero_dte["mm_better_share"] > 0.6
            and non_zero["mm_better_share"] < 0.4
        ):
            rationale.append(
                "The advantage is concentrated in 0DTE-dominant snapshots "
                f"({zero_dte['mm_better_share']:.0%} of comparisons favor MM attribution) "
                f"and absent otherwise ({non_zero['mm_better_share']:.0%})."
            )
            return Verdict(
                "USEFUL_FOR_0DTE", VERDICT_LABELS["USEFUL_FOR_0DTE"], rationale, evidence
            )
        return Verdict("YES", VERDICT_LABELS["YES"], rationale, evidence)

    # Gate 4 — subset-only effects.
    strong_subsets = {
        name: payload
        for name, payload in subsets.items()
        if payload["mm_better_share"] > 0.65 and (payload.get("n") or 0) >= 200
    }
    if strong_subsets and not in_sample_ok:
        if set(strong_subsets) <= {"0dte_dominant", "front_dte_0"}:
            rationale.append(
                "No overall incremental value, but MM attribution wins the majority of "
                "regime comparisons in 0DTE-dominant snapshots."
            )
            return Verdict(
                "USEFUL_FOR_0DTE", VERDICT_LABELS["USEFUL_FOR_0DTE"], rationale, evidence
            )
        rationale.append(
            "No overall incremental value, but MM attribution wins the majority of "
            f"regime comparisons inside: {', '.join(sorted(strong_subsets))}. Treat as a "
            "regime-scoped signal and re-test on fresh data before relying on it."
        )
        return Verdict(
            "USEFUL_IN_REGIMES", VERDICT_LABELS["USEFUL_IN_REGIMES"], rationale, evidence
        )

    # Gate 5 — confluence.
    if (
        confluence["horizons_tested"] > 0
        and confluence["horizons_where_agreement_matters"] > confluence["horizons_tested"] / 2
    ):
        rationale.append(
            "MM-attributed positioning does not replace the existing model, but snapshots "
            "where the two methodologies AGREE behave materially differently from those "
            f"where they disagree at {confluence['horizons_where_agreement_matters']}/"
            f"{confluence['horizons_tested']} horizons. The usable form of this signal is "
            "confirmation, not replacement."
        )
        return Verdict(
            "USEFUL_AS_CONFIRMATION",
            VERDICT_LABELS["USEFUL_AS_CONFIRMATION"],
            rationale,
            evidence,
        )

    rationale.append(
        "With adequate sample and reconstruction coverage, adding MM-attributed variables "
        "did not significantly improve explanatory power in sample, did not improve "
        "out-of-sample fit under walk-forward, and did not win the majority of head-to-head "
        "regime comparisons. On this evidence the existing ZeroGEX methodology is not "
        "improved on by exchange-classified MM attribution."
    )
    if regime["mm_better"]:
        rationale.append(
            f"MM attribution did win {regime['mm_better']}/{regime['comparisons']} individual "
            "regime comparisons, which is within what multiple testing produces by chance — "
            f"{result.multiplicity.get('n_significant_bh', 0)} of "
            f"{result.multiplicity.get('n_tests', 0)} tests survive FDR correction."
        )
    return Verdict("NO", VERDICT_LABELS["NO"], rationale, evidence)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt(value: Any, digits: int = 4) -> str:
    f = _finite(value)
    if f is None:
        return "—"
    if abs(f) >= 1e6:
        return f"{f:,.0f}"
    return f"{f:,.{digits}f}"


def _pct(value: Any) -> str:
    f = _finite(value)
    return "—" if f is None else f"{f:.1%}"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    if not rows:
        return "_(no rows)_\n"
    out = ["| " + " | ".join(headers) + " |"]
    out.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out) + "\n"


def render_markdown(
    result: ExperimentResult,
    *,
    provenance: Optional[Mapping[str, Any]] = None,
    reconciliation: Optional[Mapping[str, Any]] = None,
    title: str = "Market-Maker Attributed GEX — Research Report",
) -> str:
    """Full markdown report, verdict first."""
    verdict = decide(result)
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(
        f"_Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')}_ · "
        "research-only; no production metric was changed."
    )
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(f"**{verdict.label}**")
    lines.append("")
    for reason in verdict.rationale:
        lines.append(f"- {reason}")
    lines.append("")
    lines.append(
        "> Terminology: this metric is **Market-Maker Attributed GEX** — a reconstruction "
        "from exchange participant classification. It is not, and must not be described "
        'as, "true dealer GEX".'
    )
    lines.append("")

    # Data provenance.
    lines.append("## Data and coverage")
    lines.append("")
    cov = result.coverage or {}
    lines.append(
        _table(
            ["Metric", "Value"],
            [
                ["Dataset rows", str(result.n_rows)],
                ["Rows scored against forward bars", str(result.n_scored)],
                ["Scored share", _pct(cov.get("scored_share"))],
                ["Sessions", str(cov.get("sessions", "—"))],
                ["Mean inventory confidence", _fmt(cov.get("mean_inventory_confidence"), 3)],
                ["Mean gamma-universe coverage", _pct(cov.get("mean_gamma_coverage"))],
                ["MM flip unresolved rate", _pct(cov.get("mm_flip_unresolved_rate"))],
            ],
        )
    )
    if provenance:
        recon = provenance.get("reconstruction") or {}
        lines.append("### Inventory reconstruction")
        lines.append("")
        lines.append(
            _table(
                ["Metric", "Value"],
                [
                    ["Series tracked", str(recon.get("series_total", "—"))],
                    ["Cleanly reconstructed", str(recon.get("series_clean", "—"))],
                    ["Clean share", _pct(recon.get("clean_share"))],
                    ["Sessions observed", str(recon.get("observed_sessions", "—"))],
                    ["Session gaps", str(recon.get("session_gap_count", "—"))],
                    ["Window", f"{recon.get('window_start')} → {recon.get('window_end')}"],
                    ["Participants seen", ", ".join(recon.get("participants_seen", [])) or "—"],
                    ["MM contracts", _fmt(recon.get("mm_contracts"), 0)],
                    [
                        "Unknown position-effect contracts",
                        _fmt(recon.get("unknown_effect_contracts"), 0),
                    ],
                ],
            )
        )
    if reconciliation:
        lines.append("### Independent reconciliation")
        lines.append("")
        lines.append(
            _table(
                ["Check", "Value"],
                [
                    ["Verdict", str(reconciliation.get("verdict"))],
                    ["Series/sessions checked", str(reconciliation.get("n_checked"))],
                    ["ΔOI match rate", _pct(reconciliation.get("match_rate"))],
                    [
                        "Median |relative error|",
                        _fmt(reconciliation.get("median_abs_relative_error"), 4),
                    ],
                    [
                        "Zero-sum residual (share of volume)",
                        _pct(reconciliation.get("zero_sum_residual_share")),
                    ],
                ],
            )
        )
        for note in reconciliation.get("notes", []):
            lines.append(f"- ⚠ {note}")
        lines.append("")

    # Regime.
    lines.append("## 1. Gamma regime — which sign sorts subsequent behavior better?")
    lines.append("")
    separation = (result.gamma_regime or {}).get("separation") or {}
    rows = []
    for key in sorted(separation):
        entry = separation[key]
        rows.append(
            [
                key,
                _fmt(entry.get("existing_effect_size"), 3),
                _fmt(entry.get("mm_effect_size"), 3),
                _fmt(entry.get("existing_p"), 4),
                _fmt(entry.get("mm_p"), 4),
                str(entry.get("better") or "undecided"),
            ]
        )
    lines.append(
        _table(
            ["Measure · horizon", "Existing |d|", "MM |d|", "Existing p", "MM p", "Better"],
            rows[:60],
        )
    )
    reg_ev = _regime_evidence(result)
    lines.append(
        f"MM better in **{reg_ev['mm_better']}** of {reg_ev['comparisons']} comparisons; "
        f"existing better in **{reg_ev['existing_better']}**; tied in {reg_ev['tie']}."
    )
    lines.append("")

    # Flip.
    lines.append("## 2. Gamma flip")
    lines.append("")
    flip = result.gamma_flip or {}
    agreement = flip.get("agreement") or {}
    lines.append(
        _table(
            ["Metric", "Value"],
            [
                ["Snapshots with both flips", str(agreement.get("n", "—"))],
                ["Same side of spot", _pct(agreement.get("same_side_rate"))],
                [
                    "MM flip − existing flip (bps of spot), mean",
                    _fmt((agreement.get("flip_gap_bps") or {}).get("mean"), 1),
                ],
                [
                    "Existing flip unresolved rate",
                    _pct((flip.get("existing") or {}).get("unresolved_rate")),
                ],
                [
                    "MM flip unresolved rate",
                    _pct((flip.get("mm_attributed") or {}).get("unresolved_rate")),
                ],
            ],
        )
    )
    comparison = flip.get("comparison") or {}
    rows = [
        [
            horizon,
            _fmt(entry.get("existing_effect_size"), 3),
            _fmt(entry.get("mm_effect_size"), 3),
            str(entry.get("better") or "undecided"),
        ]
        for horizon, entry in sorted(comparison.items())
    ]
    lines.append("Above-vs-below realized-volatility separation:")
    lines.append("")
    lines.append(_table(["Horizon", "Existing |d|", "MM |d|", "Better"], rows))

    # Walls.
    lines.append("## 3. Walls")
    lines.append("")
    wall_cmp = (result.walls or {}).get("comparison") or {}
    rows = []
    for side, entry in sorted(wall_cmp.items()):
        rows.append(
            [
                side,
                f"{_pct(entry.get('existing_rejection_rate_30m'))} (n={entry.get('existing_n')})",
                f"{_pct(entry.get('mm_a_rejection_rate_30m'))} (n={entry.get('mm_a_n')})",
                f"{_pct(entry.get('mm_b_rejection_rate_30m'))} (n={entry.get('mm_b_n')})",
                _fmt((entry.get("existing_vs_mm_a") or {}).get("p_value"), 4),
            ]
        )
    lines.append(
        _table(
            [
                "Side",
                "Existing rejection @30m",
                "MM def-A rejection",
                "MM def-B rejection",
                "p (A vs existing)",
            ],
            rows,
        )
    )
    wall_agree = (result.walls or {}).get("agreement") or {}
    rows = [
        [
            side,
            str(payload.get("n")),
            _pct(payload.get("same_strike_rate")),
            _fmt((payload.get("gap_bps") or {}).get("mean"), 1),
        ]
        for side, payload in sorted(wall_agree.items())
    ]
    lines.append("Wall agreement between methodologies:")
    lines.append("")
    lines.append(_table(["Side", "n", "Same strike", "Mean gap (bps)"], rows))

    # Incremental.
    lines.append("## 4. Incremental value over the existing model")
    lines.append("")
    lines.append(
        "Baseline = existing ZeroGEX variables (dealer gamma @ spot, net GEX, distances to "
        "flip and both walls). Enhanced = baseline **plus** MM-attributed gamma @ spot, MM "
        "net GEX, MM flip distance, MM negative-gamma share and MM gamma concentration. "
        "Both fitted on identical rows; standard errors are Newey-West HAC sized to the "
        "forward-window overlap."
    )
    lines.append("")
    inc = result.incremental or {}
    rows = []
    for horizon, payload in sorted((inc.get("horizons") or {}).items()):
        reg = payload.get("realized_vol_regression") or {}
        cls = payload.get("large_move_classification") or {}
        wf = payload.get("walk_forward_realized_vol") or {}
        rows.append(
            [
                horizon,
                _fmt(reg.get("baseline_adj_r2"), 4),
                _fmt(reg.get("enhanced_adj_r2"), 4),
                _fmt(reg.get("delta_adj_r2"), 5),
                _fmt(reg.get("f_p_value"), 4),
                _fmt(cls.get("delta_auc"), 4),
                _fmt(wf.get("mean_delta_oos_r2"), 5),
                f"{wf.get('folds_improved', '—')}/{wf.get('n_folds', '—')}",
            ]
        )
    lines.append(
        _table(
            [
                "Horizon",
                "Baseline adj R²",
                "Enhanced adj R²",
                "Δ adj R²",
                "F p-value",
                "Δ AUC",
                "Mean Δ OOS R²",
                "Folds improved",
            ],
            rows,
        )
    )

    # Confluence.
    lines.append("## 5. Confluence")
    lines.append("")
    conf_ev = _confluence_evidence(result)
    lines.append(
        f"The two methodologies agree on the gamma regime "
        f"{_pct(conf_ev.get('agreement_rate'))} of the time. Bucket counts: "
        f"`{json.dumps(conf_ev.get('counts') or {})}`."
    )
    lines.append("")

    # Controls.
    lines.append("## 6. Controls and confounders")
    lines.append("")
    rows = [
        [
            name,
            str(payload.get("n")),
            str(payload.get("comparisons")),
            _pct(payload.get("mm_better_share")),
        ]
        for name, payload in sorted(_subset_evidence(result).items())
    ]
    lines.append(_table(["Split", "n", "Comparisons", "Share favoring MM attribution"], rows))

    # Multiplicity.
    lines.append("## 7. Multiple testing")
    lines.append("")
    mult = result.multiplicity or {}
    lines.append(
        _table(
            ["Metric", "Value"],
            [
                ["Tests run", str(mult.get("n_tests", "—"))],
                [
                    "Significant at p<0.05 (uncorrected)",
                    str(mult.get("n_significant_uncorrected", "—")),
                ],
                ["Surviving Benjamini-Hochberg", str(mult.get("n_significant_bh", "—"))],
            ],
        )
    )
    if mult.get("note"):
        lines.append(f"_{mult['note']}_")
    lines.append("")

    if result.warnings:
        lines.append("## Warnings")
        lines.append("")
        for warning in result.warnings:
            lines.append(f"- ⚠ {warning}")
        lines.append("")

    if result.arms:
        lines.append(render_arms_markdown(result))

    lines.append("## Reading this report")
    lines.append("")
    lines.append(
        "- A **negative** result is a valid and useful outcome; nothing here is tuned to "
        "make MM attribution look good.\n"
        "- Coverage and confidence gate the verdict *before* any effect is examined, so an "
        "incomplete Open-Close history reports as `INCONCLUSIVE_DATA` rather than as `NO`.\n"
        "- Effect sizes (Cohen's d, Δ adjusted R², Δ AUC) matter more than p-values at this "
        "sample size; a large-n p-value clears any threshold without the effect being worth "
        "acting on.\n"
        "- Out-of-sample walk-forward results are the ones that count. In-sample gains from "
        "five extra predictors are expected even when those predictors are noise."
    )
    lines.append("")
    return "\n".join(lines)


def write_report(
    result: ExperimentResult,
    path: str | Path,
    *,
    provenance: Optional[Mapping[str, Any]] = None,
    reconciliation: Optional[Mapping[str, Any]] = None,
) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        render_markdown(result, provenance=provenance, reconciliation=reconciliation),
        encoding="utf-8",
    )
    json_path = p.with_suffix(".json")
    json_path.write_text(
        json.dumps(
            {
                "verdict": decide(result).as_dict(),
                "arms_verdict": decide_arms(result).as_dict() if result.arms else None,
                "result": result.as_dict(),
                "provenance": provenance or {},
                "reconciliation": reconciliation or {},
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    return p


# ---------------------------------------------------------------------------
# Three-arm verdict
# ---------------------------------------------------------------------------

ARM_VERDICT_LABELS = {
    "PRODUCTION_BETTER": (
        "Production better — Production Modeled GEX is materially better than every "
        "alternative arm tested"
    ),
    "AGGRESSOR_BETTER": (
        "Aggressor better — Aggressor-Inferred MM GEX is materially better than "
        "Production Modeled GEX"
    ),
    "ATTRIBUTED_BETTER": (
        "Attributed better — Market-Maker Attributed GEX is materially better than "
        "Production Modeled GEX"
    ),
    "PRACTICALLY_EQUIVALENT": (
        "Practically equivalent — no arm is materially better on this evidence"
    ),
    "INCONCLUSIVE": "Inconclusive — the sample or the validation segment cannot support a call",
    "INCONCLUSIVE_DATA": (
        "Inconclusive — the data behind every alternative arm is too incomplete to test it"
    ),
}


@dataclass(frozen=True)
class ArmThresholds:
    """What "materially better" means.  Fixed before the validation segment is read."""

    min_scored: int = MIN_SAMPLE_FOR_CONCLUSION
    #: Mean walk-forward Δ out-of-sample R² (realized vol) an arm must add.
    material_delta_oos_r2: float = 0.005
    #: Share of decided head-to-head regime comparisons an arm must win.
    material_regime_share: float = 0.60
    #: Decided regime comparisons needed before the share is read at all.
    min_regime_comparisons: int = 6
    #: Aggressor arm data gates.
    min_aggressor_row_share: float = 0.50
    min_aggressor_classified_share: float = 0.50
    #: Attributed arm data gates (the same as the two-arm verdict).
    attributed_min_gamma_coverage: float = 0.20
    attributed_min_confidence: float = 0.35
    #: A full-sample winner must keep its direction on the validation segment.
    require_validation_agreement: bool = True

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _data_gate(result: ExperimentResult, name: str, t: ArmThresholds) -> tuple[bool, list[str]]:
    cov = result.coverage or {}
    reasons: list[str] = []
    if name == "aggressor_anchored":
        share = _finite(cov.get("aggressor_row_share")) or 0.0
        classified = _finite(cov.get("aggressor_mean_classified_share"))
        extrapolated = _finite(cov.get("aggressor_extrapolated_share"))
        if share < t.min_aggressor_row_share:
            reasons.append(
                f"aggressor arm present on {share:.0%} of scored rows "
                f"(< {t.min_aggressor_row_share:.0%})"
            )
        if classified is not None and classified < t.min_aggressor_classified_share:
            reasons.append(
                f"mean classified share {classified:.2f} < {t.min_aggressor_classified_share:.2f}"
            )
        if extrapolated:
            reasons.append(
                f"{extrapolated:.0%} of aggressor rows rest on an extrapolated buy/sell split"
            )
    elif name == "mm_attributed":
        mean_cov = _finite(cov.get("mean_gamma_coverage")) or 0.0
        mean_conf = _finite(cov.get("mean_inventory_confidence")) or 0.0
        if mean_cov < t.attributed_min_gamma_coverage:
            reasons.append(
                f"reconstructed gamma coverage {mean_cov:.1%} < "
                f"{t.attributed_min_gamma_coverage:.0%}"
            )
        if mean_conf < t.attributed_min_confidence:
            reasons.append(
                f"mean inventory confidence {mean_conf:.2f} < {t.attributed_min_confidence:.2f}"
            )
    return (not reasons), reasons


def _validation_direction(result: ExperimentResult, name: str) -> Optional[dict[str, Any]]:
    """Per-segment numbers for one arm, or ``None`` when no segment exists."""
    val = result.validation or {}
    if not val.get("available"):
        return None
    out: dict[str, Any] = {}
    for segment in ("development", "validation"):
        block = val.get(segment) or {}
        regime = (block.get("regime_summary") or {}).get(name) or {}
        inc = (block.get("incremental_summary") or {}).get(name) or {}
        deltas = [
            _finite(h.get("mean_delta_oos_r2"))
            for h in (inc.get("horizons") or {}).values()
            if _finite(h.get("mean_delta_oos_r2")) is not None
        ]
        out[segment] = {
            "regime_arm_better_share": regime.get("arm_better_share"),
            "mean_delta_oos_r2": (sum(deltas) / len(deltas)) if deltas else None,
            "n_rows": block.get("n_rows"),
        }
    return out


def _arm_evidence(result: ExperimentResult, name: str, t: ArmThresholds) -> dict[str, Any]:
    arms = result.arms or {}
    inc = _incremental_from_payload(
        ((arms.get("incremental") or {}).get("arms") or {}).get(name) or {}
    )
    regime = ((arms.get("regime") or {}).get("summary") or {}).get(name) or {}
    flip_cmp = ((arms.get("flip") or {}).get("vs_production") or {}).get(name) or {}
    flip_agree = ((arms.get("flip") or {}).get("agreement") or {}).get(name) or {}
    walls = ((arms.get("walls") or {}).get("vs_production") or {}).get(name) or {}

    flip_wins = sum(1 for v in flip_cmp.values() if v.get("better") == "arm")
    flip_losses = sum(1 for v in flip_cmp.values() if v.get("better") == "production")
    wall_wins = wall_losses = 0
    for side_block in walls.values():
        cmp_ = side_block.get("arm_vs_production") or {}
        p = _finite(cmp_.get("p_value"))
        diff = _finite(cmp_.get("diff"))
        if p is not None and p < 0.05 and diff is not None:
            if diff > 0:
                wall_wins += 1
            else:
                wall_losses += 1

    decided = (regime.get("comparisons") or 0) - (regime.get("undecided") or 0)
    share = _finite(regime.get("arm_better_share"))
    regime_decided = decided >= t.min_regime_comparisons and share is not None
    regime_ok = regime_decided and share >= t.material_regime_share
    regime_bad = regime_decided and share <= (1.0 - t.material_regime_share)

    mean_delta = _finite(inc.get("mean_delta_oos_r2"))
    oos_total = inc.get("out_of_sample_horizons_tested") or 0
    oos_gain = inc.get("out_of_sample_positive_horizons") or 0
    oos_ok = (
        oos_total > 0
        and oos_gain > oos_total / 2
        and mean_delta is not None
        and mean_delta >= t.material_delta_oos_r2
    )
    oos_bad = (
        oos_total > 0
        and oos_gain < oos_total / 2
        and mean_delta is not None
        and mean_delta <= -t.material_delta_oos_r2
    )
    materially_better = oos_ok and (regime_ok or not regime_decided)
    materially_worse = oos_bad and (regime_bad or not regime_decided)

    validation = _validation_direction(result, name)
    validation_agrees: Optional[bool] = None
    if validation is not None and (materially_better or materially_worse):
        v = validation.get("validation") or {}
        v_delta = _finite(v.get("mean_delta_oos_r2"))
        v_share = _finite(v.get("regime_arm_better_share"))
        checks: list[bool] = []
        if v_delta is not None:
            checks.append(v_delta > 0 if materially_better else v_delta < 0)
        if v_share is not None:
            checks.append(v_share > 0.5 if materially_better else v_share < 0.5)
        validation_agrees = all(checks) if checks else None

    data_ok, data_reasons = _data_gate(result, name, t)
    return {
        "label": ARMS[name].label if name in ARMS else name,
        "data_ok": data_ok,
        "data_reasons": data_reasons,
        "incremental": inc,
        "regime": regime,
        "regime_decided": regime_decided,
        "flip_wins": flip_wins,
        "flip_losses": flip_losses,
        "flip_same_side_rate": flip_agree.get("same_side_rate"),
        "wall_wins": wall_wins,
        "wall_losses": wall_losses,
        "mean_delta_oos_r2": mean_delta,
        "materially_better": materially_better,
        "materially_worse": materially_worse,
        "validation": validation,
        "validation_agrees": validation_agrees,
    }


def decide_arms(result: ExperimentResult, thresholds: ArmThresholds = ArmThresholds()) -> Verdict:
    """Mechanical A / B / C verdict.  First gate that fires wins; no winner is forced."""
    t = thresholds
    rationale: list[str] = []
    present = [a for a in (result.arms or {}).get("present", []) if a != "production"]
    evidence: dict[str, Any] = {
        "thresholds": t.as_dict(),
        "arms_present": present,
        "n_scored": result.n_scored,
        "validation_available": bool((result.validation or {}).get("available")),
        "per_arm": {},
    }

    if result.n_scored < t.min_scored:
        rationale.append(
            f"Only {result.n_scored} scored observations, below the {t.min_scored} needed to "
            "detect a moderate effect. No arm can be called better or worse on this sample."
        )
        return Verdict("INCONCLUSIVE", ARM_VERDICT_LABELS["INCONCLUSIVE"], rationale, evidence)
    if not present:
        rationale.append(
            "No alternative positioning arm is present in the dataset — neither an aggressor "
            "tape nor an exchange-classified file was supplied, so there is nothing to "
            "compare Production Modeled GEX against."
        )
        return Verdict(
            "INCONCLUSIVE_DATA", ARM_VERDICT_LABELS["INCONCLUSIVE_DATA"], rationale, evidence
        )

    per_arm = {name: _arm_evidence(result, name, t) for name in present}
    evidence["per_arm"] = per_arm
    evaluable = [n for n in present if per_arm[n]["data_ok"]]
    for n in present:
        if not per_arm[n]["data_ok"]:
            rationale.append(
                f"{per_arm[n]['label']}: not evaluable — " + "; ".join(per_arm[n]["data_reasons"])
            )
    if not evaluable:
        rationale.append(
            "Every alternative arm fails its data gate, so the comparison measures the data "
            "behind the arms rather than the positioning methodologies."
        )
        return Verdict(
            "INCONCLUSIVE_DATA", ARM_VERDICT_LABELS["INCONCLUSIVE_DATA"], rationale, evidence
        )

    for n in evaluable:
        e = per_arm[n]
        inc = e["incremental"]
        regime = e["regime"]
        rationale.append(
            f"{e['label']}: mean Δ OOS R² {_fmt(e['mean_delta_oos_r2'], 5)} "
            f"({inc.get('out_of_sample_positive_horizons', 0)}/"
            f"{inc.get('out_of_sample_horizons_tested', 0)} horizons positive), wins "
            f"{regime.get('arm_better', 0)} / loses {regime.get('production_better', 0)} / ties "
            f"{regime.get('tie', 0)} regime comparisons against production"
            + (
                f", flip same-side rate {_pct(e['flip_same_side_rate'])}"
                if e.get("flip_same_side_rate") is not None
                else ""
            )
            + "."
        )

    better = [
        n
        for n in evaluable
        if per_arm[n]["materially_better"]
        and (per_arm[n]["validation_agrees"] is not False or not t.require_validation_agreement)
    ]
    inconsistent = [
        n
        for n in evaluable
        if per_arm[n]["materially_better"]
        and per_arm[n]["validation_agrees"] is False
        and t.require_validation_agreement
    ]
    worse = [n for n in evaluable if per_arm[n]["materially_worse"]]

    if better:
        winner = max(better, key=lambda n: per_arm[n]["mean_delta_oos_r2"] or 0.0)
        code = "ATTRIBUTED_BETTER" if winner == "mm_attributed" else "AGGRESSOR_BETTER"
        rationale.append(
            f"{per_arm[winner]['label']} clears the predeclared materiality floors "
            f"(Δ OOS R² ≥ {t.material_delta_oos_r2}, regime share ≥ "
            f"{t.material_regime_share:.0%})"
            + (
                " and keeps its direction on the untouched validation segment."
                if per_arm[winner]["validation_agrees"]
                else " on the full sample; no validation segment was available to confirm it."
            )
        )
        if code == "ATTRIBUTED_BETTER":
            rationale.append(
                "On this evidence the production methodology should change toward "
                "exchange-classified attribution. The attributed measure remains a "
                "reconstruction of the market-maker population, not a dealer book."
            )
        else:
            rationale.append(
                "On this evidence the aggressor assumption adds information beyond the static "
                "convention. It is still an assumption about participant identity; the "
                "attribution report is what says how often it is right."
            )
        return Verdict(code, ARM_VERDICT_LABELS[code], rationale, evidence)

    if inconsistent:
        rationale.append(
            "An arm clears the materiality floors on the full sample but reverses direction on "
            f"the validation segment ({', '.join(per_arm[n]['label'] for n in inconsistent)}). "
            "That is the signature of an in-sample artefact; more data, not a verdict."
        )
        return Verdict("INCONCLUSIVE", ARM_VERDICT_LABELS["INCONCLUSIVE"], rationale, evidence)

    if evaluable and all(n in worse for n in evaluable):
        rationale.append(
            "Every evaluable alternative arm is materially WORSE than Production Modeled GEX "
            "on out-of-sample fit and loses the majority of head-to-head regime comparisons."
        )
        return Verdict(
            "PRODUCTION_BETTER", ARM_VERDICT_LABELS["PRODUCTION_BETTER"], rationale, evidence
        )

    rationale.append(
        "No evaluable arm clears the materiality floors in either direction: the arms are "
        "practically equivalent on this sample. That is a result — the alternative attribution "
        "did not produce a materially different read of subsequent behaviour here."
    )
    return Verdict(
        "PRACTICALLY_EQUIVALENT", ARM_VERDICT_LABELS["PRACTICALLY_EQUIVALENT"], rationale, evidence
    )


# ---------------------------------------------------------------------------
# Three-arm rendering
# ---------------------------------------------------------------------------


def render_arms_markdown(
    result: ExperimentResult, thresholds: ArmThresholds = ArmThresholds()
) -> str:
    """The A / B / C section of the report.  Categories are never collapsed."""
    arms = result.arms or {}
    present = arms.get("present") or []
    labels = arms.get("labels") or {n: ARMS[n].label for n in present if n in ARMS}
    verdict = decide_arms(result, thresholds)
    lines: list[str] = []
    lines.append("## 8. Three-arm comparison — Production vs Aggressor-Inferred vs Attributed")
    lines.append("")
    lines.append("Arms present: " + ", ".join(f"**{labels.get(n, n)}**" for n in present) + ".")
    lines.append("")
    lines.append(
        "> Categories, kept apart on purpose: **observed market data** (the chain, the tape, "
        "open interest); **aggressor-classified trade direction** (buyer- vs seller-initiated, "
        "from execution price against the NBBO); **Aggressor-Inferred MM positioning** (the "
        "passive side *assumed* to be a market maker); **Exchange-Classified MM activity** "
        "(the participant *tagged* by the exchange); **reconstructed MM inventory** (that "
        "activity folded into a position under stated assumptions); and **Production Modeled "
        "dealer positioning** (the call-positive / put-negative convention on open interest). "
        "None of these is an observed dealer book."
    )
    lines.append("")
    lines.append(f"**Verdict: {verdict.label}**")
    lines.append("")
    for reason in verdict.rationale:
        lines.append(f"- {reason}")
    lines.append("")

    # Static families.
    per_arm = verdict.evidence.get("per_arm") or {}
    rows = []
    for name in present:
        if name == "production":
            continue
        e = per_arm.get(name) or {}
        regime = e.get("regime") or {}
        inc = e.get("incremental") or {}
        rows.append(
            [
                labels.get(name, name),
                "yes" if e.get("data_ok") else "no",
                f"{regime.get('arm_better', '—')} / {regime.get('production_better', '—')} / "
                f"{regime.get('tie', '—')} ({regime.get('undecided', '—')} undecided)",
                _pct(e.get("flip_same_side_rate")),
                f"{e.get('flip_wins', 0)} / {e.get('flip_losses', 0)}",
                f"{e.get('wall_wins', 0)} / {e.get('wall_losses', 0)}",
                f"{inc.get('in_sample_significant_horizons', 0)}/"
                f"{inc.get('in_sample_horizons_tested', 0)}",
                f"{inc.get('out_of_sample_positive_horizons', 0)}/"
                f"{inc.get('out_of_sample_horizons_tested', 0)}",
                _fmt(e.get("mean_delta_oos_r2"), 5),
            ]
        )
    lines.append("### Static arms against production, identical rows")
    lines.append("")
    lines.append(
        _table(
            [
                "Arm",
                "Data gate",
                "Regime wins / losses / ties",
                "Flip same side",
                "Flip vol-separation wins / losses",
                "Wall rejection wins / losses (p<0.05)",
                "In-sample Δadj R² sig.",
                "OOS horizons positive",
                "Mean Δ OOS R²",
            ],
            rows,
        )
    )
    n_aligned = (arms.get("regime") or {}).get("n_aligned")
    if n_aligned is not None:
        lines.append(f"Rows aligned across every present arm: {n_aligned}.")
        lines.append("")

    # Hedge pressure.
    lines.append("### Dynamic hedge pressure — the flow arms")
    lines.append("")
    lines.append(
        "Flow since the cash open is a *change*, not an inventory. Positive = the market-maker "
        "population is assumed (B1) or attributed (C) to have net bought gamma so far today."
    )
    lines.append("")
    rows = []
    for name, payload in (result.hedge_pressure or {}).items():
        label = payload.get("label", FLOW_ARMS.get(name, (name, ""))[0])
        if payload.get("note") == "insufficient_sample" and not payload.get("level"):
            rows.append([label, "—", f"n={payload.get('n', 0)} (insufficient)", "—", "—", "—"])
            continue
        for horizon, entry in sorted((payload.get("level") or {}).items()):
            rv = (entry.get("realized_vol") or {}).get("welch") or {}
            sr = (entry.get("signed_return") or {}).get("welch") or {}
            chg = ((payload.get("change") or {}).get(horizon) or {}).get("realized_vol") or {}
            terms = {tm["name"]: tm for tm in chg.get("terms", [])}
            change_t = (terms.get("flow_change") or {}).get("t")
            rows.append(
                [
                    label,
                    horizon,
                    f"d={_fmt(rv.get('cohens_d'), 3)}, p={_fmt(rv.get('p_value'), 4)}",
                    f"d={_fmt(sr.get('cohens_d'), 3)}, p={_fmt(sr.get('p_value'), 4)}",
                    _fmt(change_t, 2),
                    str(payload.get("note") or ""),
                ]
            )
    lines.append(
        _table(
            [
                "Flow arm",
                "Horizon",
                "Realized vol, positive vs negative flow",
                "Signed return, positive vs negative flow",
                "HAC t of flow change on rvol",
                "Note",
            ],
            rows,
        )
    )

    # Validation split.
    val = result.validation or {}
    lines.append("### Development / validation split")
    lines.append("")
    if not val.get("available"):
        lines.append(f"_{val.get('note', 'no validation segment')}_")
        lines.append("")
    else:
        lines.append(
            f"Development sessions {val.get('development_sessions')} · validation sessions "
            f"{val.get('validation_sessions')} (share {val.get('development_share')}). Thresholds "
            "were fixed before the validation segment was read."
        )
        lines.append("")
        rows = []
        for name in present:
            if name == "production":
                continue
            v = (per_arm.get(name) or {}).get("validation") or {}
            for segment in ("development", "validation"):
                seg = v.get(segment) or {}
                rows.append(
                    [
                        labels.get(name, name),
                        segment,
                        str(seg.get("n_rows", "—")),
                        _pct(seg.get("regime_arm_better_share")),
                        _fmt(seg.get("mean_delta_oos_r2"), 5),
                    ]
                )
        lines.append(_table(["Arm", "Segment", "Rows", "Regime share won", "Mean Δ OOS R²"], rows))

    # Data gates.
    cov = result.coverage or {}
    lines.append("### Data behind the arms")
    lines.append("")
    lines.append(
        _table(
            ["Metric", "Value"],
            [
                ["Aggressor arm present on scored rows", _pct(cov.get("aggressor_row_share"))],
                [
                    "Aggressor mean classified share",
                    _fmt(cov.get("aggressor_mean_classified_share"), 3),
                ],
                [
                    "Aggressor rows on an extrapolated split",
                    _pct(cov.get("aggressor_extrapolated_share")),
                ],
                ["Attributed mean gamma-universe coverage", _pct(cov.get("mean_gamma_coverage"))],
                [
                    "Attributed mean inventory confidence",
                    _fmt(cov.get("mean_inventory_confidence"), 3),
                ],
            ],
        )
    )
    lines.append(
        "_The Aggressor-Inferred arms start every session from zero (B1) or from the "
        "production convention's open interest (B2); the attributed arm starts from a "
        "reconstructed inventory. A poorly classified tape or an under-reconstructed inventory "
        "reports here as a data gate, never as a methodology result._"
    )
    lines.append("")
    return "\n".join(lines)
