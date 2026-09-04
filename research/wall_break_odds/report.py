"""Rendering — plain text, and deliberately hard to over-read.

Two rules shape everything here:

* **A number that cannot be supported is not printed.**  Every rate carries
  its ``n`` and a Wilson interval; every bucket under the reporting floor
  prints ``insufficient data`` instead of a percentage.  A study whose headline
  finding is "we do not have enough events yet" has to be able to SAY that,
  or it will say something else.
* **Out-of-sample first.**  The walk-forward block is printed above the
  coefficients, and the coefficient block is labelled as direction-only, so a
  reader skimming for a number lands on the honest one.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

__all__ = ["render_report"]

_RULE = "=" * 78
_THIN = "-" * 78


def _pct(x: Optional[float], digits: int = 1) -> str:
    return "n/a" if x is None else f"{x * 100:.{digits}f}%"


def _num(x: Optional[float], digits: int = 4) -> str:
    return "n/a" if x is None else f"{x:.{digits}f}"


def _rate_line(label: str, block: Mapping[str, Any]) -> str:
    n = block.get("n", 0)
    if not block.get("reportable"):
        return f"  {label:<10} n={n:<6} insufficient data"
    ci = block.get("ci95") or [None, None]
    return (
        f"  {label:<10} n={n:<6} breaks={block.get('breaks', 0):<5} "
        f"P(break | tested) = {_pct(block.get('rate'))}  "
        f"[95% {_pct(ci[0])} – {_pct(ci[1])}]"
    )


def _sample_block(meta: Mapping[str, Any]) -> list[str]:
    lines = ["SAMPLE", _THIN]
    lines.append(f"  symbol                {meta.get('symbol')}")
    lines.append(f"  window                {meta.get('start')} .. {meta.get('end')}")
    lines.append(f"  sessions with frames  {meta.get('sessions_seen', 0)}")
    lines.append(f"  sessions contributing {meta.get('sessions_used', 0)}")
    skipped = meta.get("skipped") or {}
    if skipped:
        for reason, count in sorted(skipped.items(), key=lambda kv: -kv[1]):
            lines.append(f"    skipped: {reason:<20} {count}")
    lines.append(f"  wall tests found      {meta.get('events_total', 0)}")
    lines.append(
        f"  censored (excluded)   {meta.get('events_censored', 0)}"
        "   — horizon ran past 16:00 ET, outcome never observable"
    )
    lines.append(f"  resolved (modelled)   {meta.get('events_resolved', 0)}")
    fetched = meta.get("flow_rows_fetched")
    if fetched is not None:
        usable = meta.get("flow_contracts_usable", 0)
        with_flow = meta.get("events_with_flow", 0)
        lines.append(
            f"  flow rows fetched     {fetched}"
            f"  (usable contracts {usable}; events with a flow value {with_flow})"
        )
        if fetched and not usable:
            lines.append(
                "    ** flow rows were fetched but NONE were usable — this is an "
                "encoding mismatch, not a quiet tape"
            )
    return lines


def _config_block(cfg: Mapping[str, Any]) -> list[str]:
    lines = ["EVENT DEFINITION", _THIN]
    lines.append(f"  tested        price within {cfg.get('touch_pct', 0) * 1e4:.1f} bp of the wall")
    lines.append(
        f"  broke         closed {cfg.get('break_buffer_pct', 0) * 1e4:.1f} bp beyond it for "
        f"{cfg.get('confirm_minutes')} consecutive minutes"
    )
    lines.append(f"  held          {cfg.get('resolution_minutes')} min elapsed without that")
    lines.append(f"  re-arm        {cfg.get('rearm_minutes')} min after a resolved test")
    lines.append("  a wall that breaks is spent — it emits no further tests that session")
    return lines


#: Horizons the survival block quotes. Chosen to bracket a 0DTE holding
#: period rather than to flatter the curve.
SURVIVAL_MARKS = (5, 15, 30, 45, 60)


def _survival_block(curve: Sequence[Any], n_obs: int, n_breaks: int) -> list[str]:
    """P(break within t) as a curve, which is the horizon-free answer.

    The point estimate this replaces moved from 15% to 34% on nothing but a
    change of horizon, so the curve is printed FIRST and the single-horizon
    rate is kept below it only as a cross-check.
    """
    from research.wall_break_odds.survival import break_probability_at

    lines = [
        "P(BREAK WITHIN t)  Kaplan-Meier, all tests including late-session",
        _THIN,
        "  Every test contributes the time it was actually watched. A test that",
        "  held 15 minutes and then hit the bell is right-censored at 15, not",
        "  discarded — so this uses the whole sample, not just the tests with",
        "  room to resolve.",
        "",
    ]
    if not curve:
        lines.append(f"  no curve — {n_breaks} breaks among {n_obs} observations")
        return lines
    lines.append(f"  observations {n_obs}   breaks {n_breaks}")
    lines.append("")
    lines.append(f"    {'within':<10}{'P(break)':>12}{'95% CI':>22}{'at risk':>10}")
    for mark in SURVIVAL_MARKS:
        point = break_probability_at(curve, mark)
        if point is None:
            lines.append(f"    {str(mark) + ' min':<10}{'no breaks yet':>12}")
            continue
        ci = f"[{point.break_lo * 100:.1f}% – {point.break_hi * 100:.1f}%]"
        lines.append(
            f"    {str(mark) + ' min':<10}{point.break_prob * 100:>11.1f}%{ci:>22}"
            f"{point.at_risk:>10}"
        )
    return lines


def _screen_block(screen: Sequence[Mapping[str, Any]]) -> list[str]:
    lines = [
        "UNIVARIATE SCREEN  (break rate above vs below a balanced split)",
        _THIN,
        "  Marginal associations only, and mutually correlated. Benjamini-Hochberg",
        "  FDR control at 5% across the family — without it roughly one in twenty",
        "  'findings' here is noise by construction.",
        "",
        f"  {'feature':<32}{'n':>6}{'below':>9}{'above':>9}{'delta':>9}  sig",
    ]
    ranked = sorted(
        [s for s in screen if s.get("reportable")],
        key=lambda s: -abs(s.get("delta") or 0.0),
    )
    if not ranked:
        lines.append("  insufficient data on every feature")
        return lines
    for s in ranked:
        flag = "  *" if s.get("significant_fdr_05") else ""
        lines.append(
            f"  {s['feature']:<32}{s['n']:>6}{_pct(s['rate_below'], 0):>9}"
            f"{_pct(s['rate_above'], 0):>9}{_pct(s['delta'], 0):>9}{flag}"
        )
    unreported = [s["feature"] for s in screen if not s.get("reportable")]
    if unreported:
        lines.append("")
        lines.append("  not enough coverage to screen:")
        # Wrapped rather than run out to one long line: a missing-coverage list
        # is usually most of the vector on a first run, and it is the part a
        # reader most needs to actually read.
        row: list[str] = []
        for name in unreported:
            row.append(name)
            if len(row) == 3:
                lines.append("    " + ", ".join(row))
                row = []
        if row:
            lines.append("    " + ", ".join(row))
    return lines


def _oos_block(ev: Mapping[str, Any]) -> list[str]:
    lines = ["OUT-OF-SAMPLE  (walk-forward, split on session boundaries)", _THIN]
    status = ev.get("status")
    if status != "ok":
        lines.append(f"  no model reported — {status}")
        lines.append(f"  events available: {ev.get('n', 0)}, required: {ev.get('required', 'n/a')}")
        lines.append("")
        lines.append("  This is the honest outcome of a short sample, not a failure to run.")
        return lines
    oos = ev.get("oos", {})
    skill = oos.get("skill")
    lines.append(f"  test observations     {ev.get('n')}")
    lines.append(f"  folds                 {len(ev.get('folds') or [])}")
    lines.append(f"  AUC                   {_num(oos.get('auc'), 3)}")
    lines.append(
        f"  Brier   model {_num(oos.get('brier_model'))}"
        f"   baseline {_num(oos.get('brier_baseline'))}"
    )
    lines.append(
        f"  LogLoss model {_num(oos.get('log_loss_model'))}"
        f"   baseline {_num(oos.get('log_loss_baseline'))}"
    )
    lines.append(f"  SKILL vs base rate    {_num(skill, 4)}")
    if skill is not None:
        verdict = (
            "the model beats knowing only the base rate"
            if skill > 0
            else "the model does NOT beat knowing only the base rate"
        )
        lines.append(f"    -> {verdict}")
    bins = oos.get("calibration") or []
    if bins:
        lines.append("")
        lines.append("  Reliability (predicted vs realised):")
        lines.append(f"    {'bin':<14}{'n':>6}{'predicted':>12}{'observed':>12}")
        for b in bins:
            lines.append(
                f"    {b['bin_low']:.1f}-{b['bin_high']:.1f}      {b['n']:>4}"
                f"{_pct(b['predicted'], 0):>12}{_pct(b['observed'], 0):>12}"
            )
    return lines


def _coef_block(fit: Optional[Mapping[str, Any]]) -> list[str]:
    lines = ["COEFFICIENT DIRECTION  (in-sample, standardised)", _THIN]
    if not fit:
        lines.append("  not fitted — insufficient data")
        return lines
    lines.append("  Signs and relative sizes only. This is NOT a performance claim;")
    lines.append("  the out-of-sample block above is the only performance claim.")
    lines.append("")
    lines.append(f"  {'term':<32}{'coef':>10}{'se':>10}{'z':>8}{'p':>10}")
    terms = sorted(fit.get("terms", []), key=lambda t: -abs(t.get("coef") or 0.0))
    for t in terms:
        lines.append(
            f"  {t['name']:<32}{t['coef']:>10.3f}{t['se']:>10.3f}{t['z']:>8.2f}{t['p']:>10.4f}"
        )
    lines.append("")
    lines.append(
        f"  McFadden R2 {_num(fit.get('mcfadden_r2'), 4)}   converged={fit.get('converged')}"
    )
    return lines


_LIMITS = (
    """LIMITS — what this study does NOT establish
"""
    + _THIN
    + """
  * Dealer sign is MODELLED, not observed. Walls are computed from the
    call-positive / put-negative open-interest convention. On a day when
    customers were net BUYERS of the wall-side options, the 'wall' was never
    resistance and the event was mislabelled at source. No feature here can
    detect that; see research/mm_attributed_gex for the attribution work.
  * P(break | tested) is not P(break). Conditioning on the test removes the
    distance term entirely, which is why distance-to-wall is absent from the
    feature set. For the unconditional question, the production forecast's
    reflection-principle touch odds are the right tool.
  * Events within a session are not fully independent. Re-arming spaces them,
    but a trending day produces correlated tests; the session-boundary
    walk-forward controls the fit, not the standard errors in the screen.
  * Labels are sensitive to confirm_minutes and break_buffer_pct. A break
    under one setting is a pierce under another. Re-run with --confirm and
    --buffer before quoting any figure as settled.
  * The single-horizon BASE RATE is horizon-dependent by construction, and
    measurably so: on SPX over 2026-06-29..09-03 it read 15.3% at a 30-minute
    horizon, 29.7% at 45 and 34.4% at 60, on non-overlapping intervals. Quote
    the curve, or quote the rate WITH its horizon; the bare number means
    nothing on its own.
  * Nothing here is calibrated for use as a trading signal, and no result in
    this report has been validated live."""
)


def render_report(
    meta: Mapping[str, Any],
    rates: Mapping[str, Any],
    screen: Sequence[Mapping[str, Any]],
    evaluation: Mapping[str, Any],
    fit: Optional[Mapping[str, Any]] = None,
    survival: Optional[tuple] = None,
) -> str:
    """The full text report."""
    lines = [
        _RULE,
        "P(BREAK | TESTED) — call and put wall break odds",
        "research only; no production behaviour depends on this",
        _RULE,
        "",
    ]
    lines += _sample_block(meta)
    lines += ["", *_config_block(meta.get("config", {}))]
    if survival is not None:
        lines += ["", *_survival_block(survival[0], survival[1], survival[2])]
    lines += ["", "BASE RATES  (single horizon — read the curve above first)", _THIN]
    lines.append(_rate_line("overall", rates.get("overall", {})))
    lines.append(_rate_line("call wall", rates.get("call", {})))
    lines.append(_rate_line("put wall", rates.get("put", {})))
    lines += ["", *_screen_block(screen)]
    lines += ["", *_oos_block(evaluation)]
    lines += ["", *_coef_block(fit)]
    lines += ["", _LIMITS, ""]
    return "\n".join(lines)
