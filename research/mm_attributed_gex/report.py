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
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from research.mm_attributed_gex.backtest import MIN_SAMPLE_FOR_CONCLUSION, ExperimentResult

__all__ = ["Verdict", "decide", "render_markdown", "write_report"]


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
    inc = result.incremental or {}
    horizons = inc.get("horizons") or {}
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
