"""Turn study results into a markdown report with a verdict.

The verdict logic is written down here rather than applied by eye, so the same
numbers always produce the same conclusion and nobody has to trust a reading of
a table. Three questions, answered in this order:

1. **Does the score order excursion at all?** The rank correlation between the
   score and the measure. If ``rho`` is indistinguishable from zero once the
   interval is computed over sessions rather than minutes, the gauge does not
   order forward excursion and nothing downstream can rescue it.
2. **Do the bands beat the unconditional base rate?** Each band's mean against
   the pooled mean, with an effect size. A band that differs by a
   statistically detectable but negligible amount has not earned its copy.
3. **Do the bands run in the right ORDER?** "Trend / Expansion" must show more
   travel than "Chop / Range". A gauge whose bands are ordered backwards is
   worse than one that does nothing, because it is confidently wrong.

A verdict of ``supported`` requires all three. Anything less is reported as
what it is.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from research.msi_regime_excursion.bands import BANDS, BAND_KEYS
from research.msi_regime_excursion.excursion import REST_OF_SESSION
from research.msi_regime_excursion.study import MEASURES, StudyResult

__all__ = ["verdict_for", "render_markdown"]

#: |rho| below this is treated as "does not order excursion" regardless of
#: significance -- with tens of thousands of rows, detectable and useful are
#: very different things.
RHO_FLOOR = 0.05
#: |Cliff's delta| below this is "negligible" by the conventional labels.
DELTA_FLOOR = 0.147
#: The measure the headline verdict is read from.
HEADLINE_MEASURE = "range_bps"


def _fmt(value: Optional[float], places: int = 2, dash: str = "—") -> str:
    if value is None:
        return dash
    try:
        return f"{value:.{places}f}"
    except (TypeError, ValueError):
        return dash


def _pct(value: Optional[float], suffix: str = "%") -> str:
    """A rate in [0, 1] rendered as a percentage, or an em dash."""
    if value is None:
        return "—"
    return f"{100.0 * value:.1f}{suffix}"


def _fmt_p(p: Optional[float]) -> str:
    if p is None:
        return "—"
    if p < 0.001:
        return "<0.001"
    return f"{p:.3f}"


def _horizon_label(h: Any) -> str:
    return "rest of session" if h == REST_OF_SESSION else f"{h}m"


def verdict_for(
    result: StudyResult,
    horizon: Any,
    measure: str = HEADLINE_MEASURE,
) -> dict:
    """Verdict for one instrument at one horizon."""
    rho_finding = next(
        (
            c for c in result.correlations
            if c.horizon == horizon and c.measure == measure and c.score == "msi"
        ),
        None,
    )
    band_findings = {
        b.bucket: b for b in result.buckets
        if b.horizon == horizon and b.measure == measure and b.bucket_kind == "band"
    }

    rho = rho_finding.correlation.rho if rho_finding else None
    orders = rho is not None and abs(rho) >= RHO_FLOOR and bool(
        rho_finding and rho_finding.survives_bh
    )

    # Do any bands beat the base rate by more than a negligible effect?
    material = [
        b for b in band_findings.values()
        if b.survives_bh
        and b.comparison.cliffs_delta is not None
        and abs(b.comparison.cliffs_delta) >= DELTA_FLOOR
    ]

    # Are the band means ordered as the copy claims (weakest -> strongest)?
    means = [
        band_findings[k].comparison.mean
        for k in BAND_KEYS
        if k in band_findings and band_findings[k].comparison.mean is not None
    ]
    ordered: Optional[bool] = None
    if len(means) >= 3:
        ordered = all(a <= b for a, b in zip(means, means[1:]))
    inverted: Optional[bool] = None
    if len(means) >= 3:
        inverted = all(a >= b for a, b in zip(means, means[1:]))

    if rho is None or not band_findings:
        label = "insufficient data"
    elif orders and material and ordered:
        label = "supported"
    elif inverted and (orders or material):
        label = "INVERTED — bands run backwards"
    elif not orders and not material:
        label = "not supported — no effect beyond the base rate"
    else:
        label = "weak — detectable but not material"

    return {
        "instrument": result.instrument,
        "horizon": horizon,
        "measure": measure,
        "rho": rho,
        "rho_ci": (
            (rho_finding.correlation.ci_lo, rho_finding.correlation.ci_hi)
            if rho_finding else (None, None)
        ),
        "rho_survives": bool(rho_finding and rho_finding.survives_bh),
        "orders_excursion": orders,
        "material_bands": [b.bucket for b in material],
        "band_means": {k: band_findings[k].comparison.mean for k in band_findings},
        "ordered_as_claimed": ordered,
        "inverted": inverted,
        "verdict": label,
    }


def render_markdown(
    results: Sequence[StudyResult],
    *,
    horizons: Sequence[Any],
    title: str = "MSI regime gauge vs realized forward excursion",
    window: str = "",
) -> str:
    out: list[str] = [f"# {title}", ""]
    if window:
        out += [f"_Window: {window}_", ""]

    out += [
        "## What was tested",
        "",
        "The product bands the 0-100 Composite Score / MSI into four regimes whose",
        "copy makes explicit claims about how far price travels. This measures the",
        "excursion that actually followed each persisted reading and compares each",
        "band against **the unconditional base rate** for the same instrument and",
        "horizon, which is the baseline `content/methodology.md` commits to.",
        "",
        "Significance is a **session-level block bootstrap**. Readings land about once",
        "a minute and forward windows overlap, so row-level tests treat tens of",
        "thousands of correlated observations as independent and are wildly",
        "over-confident; both p-values are shown so the gap is visible.",
        "",
        "## Sample",
        "",
        "| instrument | rows | sessions | first | last | reconstructible |",
        "|---|---:|---:|---|---|---:|",
    ]
    for r in results:
        recon = (
            f"{100.0 * r.reconstruction_ok / r.reconstruction_total:.1f}%"
            if r.reconstruction_total else "—"
        )
        out.append(
            f"| {r.instrument} | {r.n_rows:,} | {r.n_sessions} | "
            f"{r.first:%Y-%m-%d %H:%M} | {r.last:%Y-%m-%d %H:%M} | {recon} |"
            if r.first and r.last else
            f"| {r.instrument} | {r.n_rows:,} | {r.n_sessions} | — | — | {recon} |"
        )
    out.append("")

    counts_needed = any(r.band_counts for r in results)
    if counts_needed:
        out += ["### Readings per band", "",
                "| instrument | " + " | ".join(
                    next(b.label for b in BANDS if b.key == k) for k in BAND_KEYS
                ) + " |",
                "|---|" + "---:|" * len(BAND_KEYS)]
        for r in results:
            out.append(
                f"| {r.instrument} | "
                + " | ".join(f"{r.band_counts.get(k, 0):,}" for k in BAND_KEYS)
                + " |"
            )
        out.append("")

    # ---- Verdicts -------------------------------------------------------
    out += ["## Verdict", "",
            f"Headline measure: `{HEADLINE_MEASURE}` "
            f"({MEASURES.get(HEADLINE_MEASURE, '')}).", "",
            "| instrument | horizon | Spearman rho (95% CI) | bands beating base rate | "
            "ordered? | verdict |",
            "|---|---|---|---|---|---|"]
    for r in results:
        for h in horizons:
            v = verdict_for(r, h)
            lo, hi = v["rho_ci"]
            rho_txt = _fmt(v["rho"], 3)
            if lo is not None and hi is not None:
                rho_txt += f" ({_fmt(lo, 3)}, {_fmt(hi, 3)})"
            ordered = (
                "yes" if v["ordered_as_claimed"] else
                ("BACKWARDS" if v["inverted"] else "no")
            ) if v["ordered_as_claimed"] is not None else "—"
            mats = ", ".join(v["material_bands"]) if v["material_bands"] else "none"
            out.append(
                f"| {r.instrument} | {_horizon_label(h)} | {rho_txt} | {mats} | "
                f"{ordered} | **{v['verdict']}** |"
            )
    out.append("")

    # ---- Band detail ----------------------------------------------------
    out += ["## Bands against the unconditional base rate", ""]
    for r in results:
        out += [f"### {r.instrument}", ""]
        for note in r.notes:
            out += [f"> {note}", ""]
        out += [
            "| horizon | measure | band | n | mean | base | diff | ratio | "
            "Cliff's d | effect | p (block) | p (naive) | BH |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|---|---|",
        ]
        for b in r.buckets:
            if b.bucket_kind != "band":
                continue
            c = b.comparison
            out.append(
                f"| {_horizon_label(b.horizon)} | {b.measure} | {b.bucket} | {c.n:,} | "
                f"{_fmt(c.mean)} | {_fmt(c.mean_base)} | {_fmt(c.diff)} | "
                f"{_fmt(c.ratio, 3)} | {_fmt(c.cliffs_delta, 3)} | {c.effect_label} | "
                f"{_fmt_p(c.p_block)} | {_fmt_p(c.p_naive)} | "
                f"{'yes' if b.survives_bh else 'no'} |"
            )
        out.append("")

    # ---- Variants -------------------------------------------------------
    out += [
        "## Alternative constructions",
        "",
        "Rank correlation with forward excursion for the shipped score and for the",
        "obvious alternatives (see `decompose.py`). `msi_direction` is the negative",
        "control: it is built only from the components whose own docstrings call them",
        "bullish/bearish, so if it tracks excursion as well as `msi` does, the shipped",
        "score's apparent regime content is direction wearing a regime label.",
        "",
        "| instrument | horizon | measure | score | rho | 95% CI | p (block) | BH |",
        "|---|---|---|---|---:|---|---|---|",
    ]
    for r in results:
        for c in r.correlations:
            rc = c.correlation
            ci = (
                f"({_fmt(rc.ci_lo, 3)}, {_fmt(rc.ci_hi, 3)})"
                if rc.ci_lo is not None else "—"
            )
            out.append(
                f"| {c.instrument} | {_horizon_label(c.horizon)} | {c.measure} | "
                f"{c.score} | {_fmt(rc.rho, 4)} | {ci} | {_fmt_p(rc.p_block)} | "
                f"{'yes' if c.survives_bh else 'no'} |"
            )
    out.append("")

    # ---- Point targets --------------------------------------------------
    if any(r.targets for r in results):
        out += [
            "## Point-target hit rates",
            "",
            "P(the instrument travels at least N points in the window), by band,",
            "against the pooled base rate. These are the units a scalper works in.",
            "",
            "| instrument | horizon | target | side | band | n | hit rate | "
            "base rate | diff | p | BH |",
            "|---|---|---:|---|---|---:|---:|---:|---:|---|---|",
        ]
        for r in results:
            for t in r.targets:
                res = t.result
                out.append(
                    f"| {t.instrument} | {_horizon_label(t.horizon)} | {t.target_pts:g} | "
                    f"{t.side} | {t.bucket} | {t.n:,} | "
                    f"{_pct(res['rate'])} | {_pct(res['rate_base'])} | "
                    f"{_pct(res['diff'], suffix='pp')} | "
                    f"{_fmt_p(res['p'])} | {'yes' if t.survives_bh else 'no'} |"
                )
        out.append("")

    out += [
        "## How to read a null",
        "",
        "If the bands do not beat the base rate, the finding is not that the MSI is",
        "worthless -- it is that **the copy on the bands is a claim the number does not",
        "support**. Those are different repairs. The first would mean deleting the",
        "gauge; the second means describing it as what it measures.",
        "",
    ]
    return "\n".join(out)
