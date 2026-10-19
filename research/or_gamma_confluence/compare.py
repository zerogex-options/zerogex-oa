"""Opening-range lengths side by side, and the pre-registered verdicts.

``compare-or`` builds one dataset per opening-range length over ONE window and
analyzes each with the unchanged report code.  This module reads those
summaries and lays them out as surfaces -- every statistic across every range
length, no ranking and no "best cell" -- and then applies a decision rule that
was written down before the 60-minute data existed.

**The decision rule** (per hypothesis, per range length):

* **build** -- all of: the cohort beats the baseline in the claimed direction
  and survives Benjamini-Hochberg across EVERY cohort of EVERY range length in
  the run; its excess over its own mechanical null clears the NQ cost hurdle
  at the near end of its session-clustered 95% interval; it keeps the claimed
  sign against its own column's baseline in all three chronological splits,
  each holding at least ``MIN_REPORTABLE_N`` touches; and the two option books
  do not disagree.
* **don't build** -- any of: the net-of-null effect against the baseline
  points the wrong way; even the far end of the interval of its excess over
  its own null falls short of the cost hurdle; or a split with at least
  ``MIN_REPORTABLE_N`` touches reverses the sign.
* **defer** -- everything else: pointing the right way, not contradicted out
  of sample, not yet resolved.
* **untestable** -- the cohort has fewer than ``MIN_REPORTABLE_N`` touches or
  spans fewer than ``MIN_TESTABLE_SESSIONS`` sessions, so it has no interval
  and no p-value (see :data:`.cohorts.MIN_TESTABLE_SESSIONS`).  Nothing can be
  built on it, and nothing can be ruled out either.

``defer`` is not praise.  A cohort too thin to say anything defers by
construction, so every verdict is printed beside the smallest effect that
cohort could have resolved (its MDE).  A deferral with an MDE of twenty points
is a statement about the sample, not about the market.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from research.msi_regime_excursion import stats
from research.or_gamma_confluence.cohorts import MIN_REPORTABLE_N, MIN_TESTABLE_SESSIONS
from research.or_gamma_confluence.power import min_detectable_effect, se_from_ci

__all__ = ["VERDICTS", "decide", "combined_bh", "build_comparison", "render_comparison"]

BUILD = "build"
DEFER = "defer"
DONT_BUILD = "don't build"
UNTESTABLE = "untestable"
VERDICTS: tuple[str, ...] = (BUILD, DEFER, DONT_BUILD, UNTESTABLE)


def decide(
    hyp: Mapping[str, Any],
    *,
    hurdle: Optional[float],
    survives_bh: bool,
) -> tuple[str, list[str]]:
    """Apply the module docstring's rule to one hypothesis at one range length.

    Returns ``(verdict, reasons)``.  Every criterion that was checked leaves a
    reason, passed or failed, so a verdict can be audited line by line.
    """
    sign = int(hyp.get("sign") or 1)
    base = hyp.get("vs_baseline") or {}
    own = hyp.get("own_null") or {}
    reasons: list[str] = []

    if not base.get("testable"):
        n_sess = base.get("cohort_sessions") or 0
        return UNTESTABLE, [
            f"{hyp.get('n') or 0} touches over {n_sess} session{'' if n_sess == 1 else 's'}; "
            f"a test needs {MIN_REPORTABLE_N}+ touches and {MIN_TESTABLE_SESSIONS}+ sessions"
        ]
    net = base.get("net_of_null_diff")
    if net is None:
        net = base.get("clustered_diff")
    if net is None:
        return UNTESTABLE, ["no estimate"]
    right_way = sign * net > 0
    reasons.append(
        f"{'right' if right_way else 'WRONG'} direction vs baseline, net of null "
        f"({net * 100:+.1f} pts)"
    )

    excess = own.get("excess")
    lo, hi = own.get("excess_ci_low"), own.get("excess_ci_high")
    near = far = None
    if lo is not None and hi is not None:
        near, far = (lo, hi) if sign > 0 else (-hi, -lo)
    pays_far = far is not None and hurdle is not None and far >= hurdle
    pays_near = near is not None and hurdle is not None and near > hurdle
    if hurdle is None:
        reasons.append("no cost hurdle (no NQ sessions in the run)")
    elif far is None:
        reasons.append("no interval on the excess over its own null")
    else:
        reasons.append(
            f"excess over own null {sign * (excess or 0) * 100:+.1f} pts "
            f"(claimed direction), interval [{near * 100:+.1f}, {far * 100:+.1f}] "
            f"vs hurdle {hurdle * 100:.2f}"
        )

    informative = [
        v
        for v in (hyp.get("oos") or {}).values()
        if (v.get("n") or 0) >= MIN_REPORTABLE_N and v.get("excess_vs_column") is not None
    ]
    reversed_oos = any(sign * v["excess_vs_column"] < 0 for v in informative)
    oos_complete = len(informative) == 3 and not reversed_oos
    if reversed_oos:
        reasons.append("REVERSES in a chronological split")
    elif len(informative) < 3:
        reasons.append(
            f"only {len(informative)} of 3 splits hold {MIN_REPORTABLE_N}+ touches: "
            "out-of-sample uninformative"
        )
    else:
        reasons.append("holds its sign in all three splits")

    pooling = hyp.get("pooling")
    reasons.append(f"books: {pooling}")
    reasons.append(f"Benjamini-Hochberg across the run: {'survives' if survives_bh else 'fails'}")

    if not right_way or (hurdle is not None and far is not None and not pays_far) or reversed_oos:
        return DONT_BUILD, reasons
    if right_way and survives_bh and pays_near and oos_complete and pooling != "books_disagree":
        return BUILD, reasons
    return DEFER, reasons


def combined_bh(summaries: Mapping[int, Mapping[str, Any]], alpha: float = 0.05) -> dict[str, Any]:
    """Benjamini-Hochberg over every testable cohort at every range length.

    Each report already corrects within its own family, as September did.  A
    run that tests 60 minutes AND 30 AND 5, and would act on any of them, has
    one family, not three; this is that family.
    """
    keys: list[tuple[int, str]] = []
    pvals: list[float] = []
    for minutes, summary in summaries.items():
        for c in summary.get("cohorts", []):
            if c.get("clustered_p") is not None:
                keys.append((minutes, c["key"]))
                pvals.append(c["clustered_p"])
    flags = stats.benjamini_hochberg(pvals, alpha=alpha)
    return {
        "family_size": len(pvals),
        "alpha": alpha,
        "min_p": min(pvals) if pvals else None,
        "survivors": [f"{m}m:{k}" for (m, k), f in zip(keys, flags) if f],
        "_flags": {f"{m}m:{k}": bool(f) for (m, k), f in zip(keys, flags)},
    }


def build_comparison(
    summaries: Mapping[int, Mapping[str, Any]],
    *,
    coverage: Optional[Mapping[str, Any]] = None,
    fresh_from: Optional[str] = None,
) -> dict[str, Any]:
    """Every number the comparison report prints, keyed by range length."""
    order = sorted(summaries)
    bh = combined_bh(summaries)
    verdicts: dict[str, dict[str, Any]] = {}
    for m in order:
        s = summaries[m]
        hurdle = (s.get("cost_hurdle") or {}).get("hurdle")
        for name, hyp in (s.get("hypotheses") or {}).items():
            survives = bh["_flags"].get(f"{m}m:{hyp['cohort']}", False)
            verdict, reasons = decide(hyp, hurdle=hurdle, survives_bh=survives)
            base = hyp.get("vs_baseline") or {}
            se = se_from_ci(base.get("clustered_ci_low"), base.get("clustered_ci_high"))
            verdicts.setdefault(name, {})[str(m)] = {
                "verdict": verdict,
                "reasons": reasons,
                "mde": min_detectable_effect(se),
                "n": hyp.get("n"),
                "claim": hyp.get("claim"),
                "label": hyp.get("label"),
            }
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "or_minutes": order,
        "fresh_from": fresh_from,
        "coverage": coverage,
        "summaries": {str(m): summaries[m] for m in order},
        "combined_bh": {k: v for k, v in bh.items() if not k.startswith("_")},
        "verdicts": verdicts,
    }


# ── Rendering ────────────────────────────────────────────────────────


def _pct(v: Optional[float], places: int = 1) -> str:
    return "—" if v is None else f"{100.0 * v:.{places}f}%"


def _pts(v: Optional[float], places: int = 1) -> str:
    return "—" if v is None else f"{100.0 * v:+.{places}f}"


def _ci(lo: Optional[float], hi: Optional[float]) -> str:
    if lo is None or hi is None:
        return "—"
    return f"[{lo * 100:+.1f}, {hi * 100:+.1f}]"


def _row(label: str, cells: Sequence[str]) -> str:
    return f"| {label} | " + " | ".join(cells) + " |"


def _coverage_md(coverage: Mapping[str, Any]) -> list[str]:
    out = []
    fresh = coverage.get("fresh_from")
    out.append(
        f"Counted at OR={coverage.get('opening_range_minutes')} min: a session needs "
        f"{coverage.get('min_session_frames')} gamma frames inside 09:30-16:00 ET and "
        f"{coverage.get('min_or_bars')} bars inside the opening-range window. An upper "
        "bound -- the build's clock and range-width gates can still drop a day.\n"
    )
    head = "| symbol | gamma | bars | usable |" + (f" since {fresh} |" if fresh else "")
    head += " window |"
    out.append(head)
    out.append("|---|---:|---:|---:|" + ("---:|" if fresh else "") + "---|")
    for sym, block in (coverage.get("symbols") or {}).items():
        u = block.get("sessions") or {}
        if not u.get("available"):
            out.append(f"| {sym} | — | — | — |" + (" — |" if fresh else "") + " unavailable |")
            continue
        line = f"| {sym} | {u['gamma_sessions']} | {u['bar_sessions']} | **{u['usable']}** |"
        if fresh:
            line += f" {u.get('usable_fresh', '—')} |"
        line += f" {u.get('first') or '—'} .. {u.get('last') or '—'} |"
        out.append(line)
    out.append("")
    return out


def render_comparison(cmp: Mapping[str, Any]) -> str:
    """The comparison report: surfaces across range lengths, then verdicts."""
    order = [str(m) for m in cmp["or_minutes"]]
    S = cmp["summaries"]
    cols = [f"{m} min" for m in order]
    out: list[str] = []
    A = out.append

    A("# Opening-range extension × gamma confluence — range lengths compared\n")
    A(f"*Generated {cmp['generated_at']}*\n")
    A(
        "> Surfaces, not a leaderboard: read each row across the range lengths. "
        "Nothing here has been validated live.\n"
    )
    first = S[order[0]]
    prov = first.get("provenance") or {}
    A("## Run\n")
    A(f"- Window: `{prov.get('start', '?')}` .. `{prov.get('end', '?')}`")
    A(f"- Symbols: {', '.join(prov.get('symbols', [])) or '—'}")
    cfg = first.get("config") or {}
    A(
        f"- Ladder: mode `{cfg.get('extension_mode')}`, step {cfg.get('extension_step')}R; "
        f"clock `{cfg.get('availability_clock')}`, lead {cfg.get('gamma_min_lead_seconds')}s"
    )
    for m in order:
        A(f"- {m} min: fingerprint `{S[m].get('config_fingerprint')}`")
    if cmp.get("fresh_from"):
        A(f"- Fresh-data slice for H2: sessions from `{cmp['fresh_from']}`")
    A("")

    if cmp.get("coverage"):
        A("## 1. Sessions available\n")
        out.extend(_coverage_md(cmp["coverage"]))

    A("## 2. Sample by range length\n")
    A("| | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))

    def overall(m: str, key: str) -> Any:
        return (S[m].get("overall") or {}).get(key)

    def sym_sessions(m: str) -> int:
        return sum(
            (v or {}).get("sessions_used", 0)
            for v in ((S[m].get("provenance") or {}).get("per_symbol") or {}).values()
        )

    A(_row("symbol-sessions used", [str(sym_sessions(m)) for m in order]))
    A(_row("calendar sessions", [str(overall(m, "n_sessions")) for m in order]))
    A(_row("touch events", [str(overall(m, "n")) for m in order]))
    A(_row("resolved", [str(overall(m, "n_resolved")) for m in order]))
    A(
        _row(
            "events per symbol-session",
            [f"{overall(m, 'n') / sym_sessions(m):.2f}" if sym_sessions(m) else "—" for m in order],
        )
    )
    A(_row("unresolved at the bell", [_pct(overall(m, "censored_rate")) for m in order]))
    A(
        _row(
            "sliced through in the touch bar",
            [str(overall(m, "n_continuation_same_bar")) for m in order],
        )
    )
    A(
        _row(
            "median opening range, NQ pts",
            [
                (
                    f"{(S[m].get('cost_hurdle') or {}).get('median_or_width_pts'):.1f}"
                    if (S[m].get("cost_hurdle") or {}).get("available")
                    else "—"
                )
                for m in order
            ],
        )
    )
    A("")
    A("Events by symbol:\n")
    syms = sorted({s for m in order for s in (S[m].get("by_symbol") or {})})
    A("| symbol | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))
    for sym in syms:
        A(
            _row(
                sym,
                [
                    f"{((S[m].get('by_symbol') or {}).get(sym) or {}).get('n_resolved', 0)} "
                    f"({((S[m].get('by_symbol') or {}).get(sym) or {}).get('n_sessions', 0)}d)"
                    for m in order
                ],
            )
        )
    skips = [((S[m].get("provenance") or {}).get("skip_reasons") or {}) for m in order]
    if any(skips):
        A("")
        A("Sessions skipped, by reason:\n")
        reasons = sorted({k for sk in skips for k in sk})
        A("| reason | " + " | ".join(cols) + " |")
        A("|---|" + "---:|" * len(order))
        for r in reasons:
            A(_row(f"`{r}`", [str(sk.get(r, 0)) for sk in skips]))
    A("")

    A("## 3. The mechanical null comes first\n")
    A(
        "Every rate below is set against what this event rule returns with no "
        "mean reversion at all: the gambler's-ruin formula from each touch's "
        "close-offset, and the surrogates (the session's own post-range bars "
        "replayed with random direction). Two nulls that agree are the check "
        "that the formula still holds at this range length.\n"
    )
    A("| | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))

    def head(m: str) -> Mapping[str, Any]:
        return ((S[m].get("mechanical_null") or {}).get("headline")) or {}

    A(_row("observed reversal-first", [_pct(head(m).get("observed")) for m in order]))
    A(
        _row(
            "median close-offset d (R)",
            [
                (
                    f"{(S[m].get('mechanical_null') or {}).get('median_offset_r'):.4f}"
                    if (S[m].get("mechanical_null") or {}).get("median_offset_r") is not None
                    else "—"
                )
                for m in order
            ],
        )
    )
    A(_row("formula null", [_pct(head(m).get("formula_null")) for m in order]))
    A(_row("surrogate null", [_pct(head(m).get("surrogate_null")) for m in order]))
    A(
        _row(
            "surrogate − formula",
            [
                (
                    _pts(head(m)["surrogate_null"] - head(m)["formula_null"], 2)
                    if head(m).get("surrogate_null") is not None
                    and head(m).get("formula_null") is not None
                    else "—"
                )
                for m in order
            ],
        )
    )
    A(
        _row(
            "**excess over null** [95% CI]",
            [
                f"**{_pts(head(m).get('excess'))}** "
                f"{_ci(head(m).get('excess_ci_low'), head(m).get('excess_ci_high'))}"
                for m in order
            ],
        )
    )
    A("")

    A("## 4. H1 — does reversion rise with depth?\n")
    A("Reversal-first rate minus its own null, by depth band (n in brackets):\n")
    bands = [b["band"] for b in (S[order[0]].get("depth_table") or [])]
    A("| depth | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))
    for i, band in enumerate(bands):
        cells = []
        for m in order:
            b = (S[m].get("depth_table") or [])[i]
            nb = b.get("null") or {}
            cells.append(f"{_pts(nb.get('excess'))} (n={b.get('n_resolved')})")
        A(_row(band, cells))
    for label, key in (("ρ(depth, reversal)", "raw"), ("ρ net of formula", "net_of_formula_null")):
        cells = []
        for m in order:
            r = ((S[m].get("depth_rho") or {}).get(key)) or {}
            cells.append(
                "—"
                if r.get("rho") is None
                else f"{r['rho']:+.3f} [{_num3(r.get('ci_lo'))}, {_num3(r.get('ci_hi'))}]"
            )
        A(_row(label, cells))
    A("")
    A("**The customer's rungs** — excess over own null [95% CI], n:\n")
    rung_labels = [r["rung"] for r in (S[order[0]].get("named_rungs") or [])]
    A("| rung | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))
    for i, label in enumerate(rung_labels):
        cells = []
        for m in order:
            r = (S[m].get("named_rungs") or [])[i]
            if not r.get("on_ladder"):
                cells.append("not a rung")
                continue
            cells.append(
                f"{_pts(r.get('excess'))} {_ci(r.get('excess_ci_low'), r.get('excess_ci_high'))}"
                f" n={r.get('n')}"
            )
        A(_row(label, cells))
    A("")

    A("## 5. H3 — does gamma confluence help?\n")
    A("Confluence minus no-confluence, by distance threshold (pooled symbols):\n")
    dists = [g["distance"] for g in (S[order[0]].get("distance_grid") or [])]
    A("| threshold | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))
    for i, d in enumerate(dists):
        cells = []
        for m in order:
            g = (S[m].get("distance_grid") or [])[i]
            cells.append(f"{_pts(g.get('gap'))} ({g.get('n_with')} / {g.get('n_without')})")
        A(_row(f"≤{d:g} pts", cells))
    A("")
    A("Cohorts against the baseline, net of their null (clustered p in brackets):\n")
    keys = [c["key"] for c in (S[order[0]].get("cohorts") or []) if c["key"] != "all"]
    labels = {c["key"]: c["label"] for c in (S[order[0]].get("cohorts") or [])}
    A("| cohort | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))
    flags = cmp["combined_bh"].get("survivors") or []
    for k in keys:
        cells = []
        for m in order:
            c = next((x for x in S[m].get("cohorts", []) if x["key"] == k), {})
            p = c.get("clustered_p")
            star = " **BH**" if f"{m}m:{k}" in flags else ""
            cells.append(
                f"{_pts(c.get('net_of_null_diff'))} "
                f"({'—' if p is None else f'{p:.3f}'}, n={c.get('n_resolved', 0)}){star}"
            )
        A(_row(labels.get(k, k), cells))
    A("")
    bh = cmp["combined_bh"]
    A(
        f"**Benjamini-Hochberg across all {bh['family_size']} cohort tests in this run "
        f"(every range length, alpha={bh['alpha']}):** "
        + (
            f"{len(bh['survivors'])} survive — `{'`, `'.join(bh['survivors'])}`."
            if bh["survivors"]
            else (
                f"nothing survives. Smallest p is {bh['min_p']:.3f} against a rank-1 "
                f"threshold of {bh['alpha'] / bh['family_size']:.4f}."
                if bh["family_size"] and bh.get("min_p") is not None
                else "no testable cohorts."
            )
        )
    )
    A("")
    for m in order:
        mult = S[m].get("multiplicity") or {}
        A(
            f"- Within {m} min alone ({mult.get('family_size')} tests): "
            f"{len(mult.get('survivors') or [])} survive"
            + (f" — `{'`, `'.join(mult['survivors'])}`" if mult.get("survivors") else "")
        )
    A("")

    A("## 6. H2 — broken ladders, and how far from resolving\n")
    A(
        "| | " + " | ".join(cols) + " |",
    )
    A("|---|" + "---:|" * len(order))

    def h2(m: str, part: str = "whole_window") -> Mapping[str, Any]:
        return ((S[m].get("h2") or {}).get(part)) or {}

    A(_row("touches in cohort", [str(h2(m).get("n", "—")) for m in order]))
    A(_row("cohort sessions", [str(h2(m).get("cohort_sessions", "—")) for m in order]))
    A(_row("vs baseline", [_pts(h2(m).get("clustered_diff")) for m in order]))
    A(_row("null shift", [_pts(h2(m).get("null_shift")) for m in order]))
    A(
        _row(
            "**net of null** [95% CI]",
            [
                f"**{_pts(h2(m).get('adjusted_diff'))}** "
                f"{_ci(h2(m).get('adjusted_ci_low'), h2(m).get('adjusted_ci_high'))}"
                for m in order
            ],
        )
    )
    A(
        _row(
            "p (session-clustered)",
            [
                "—" if h2(m).get("clustered_p") is None else f"{h2(m)['clustered_p']:.3f}"
                for m in order
            ],
        )
    )
    A(
        _row(
            "MDE at 80% power",
            [
                (
                    "—"
                    if (h2(m).get("power") or {}).get("mde") is None
                    else f"{h2(m)['power']['mde'] * 100:.1f} pts"
                )
                for m in order
            ],
        )
    )
    for scen, name in (("observed", "as measured"), ("september", "September's -6.0")):
        for what, fmt in (
            ("power_now", lambda v: f"{v:.0%}"),
            ("sessions_needed", lambda v: f"{v}"),
            ("more_months", lambda v: f"{v:.0f}"),
        ):
            cells = []
            for m in order:
                sc = next(
                    (
                        x
                        for x in (h2(m).get("power") or {}).get("scenarios", [])
                        if x["name"] == scen
                    ),
                    None,
                )
                v = None if sc is None else sc.get(what)
                cells.append("—" if v is None else fmt(v))
            label = {
                "power_now": "power now",
                "sessions_needed": "sessions for 80%",
                "more_months": "≈ months more",
            }[what]
            A(_row(f"{name}: {label}", cells))
    if cmp.get("fresh_from"):
        A(
            _row(
                f"fresh only (from {cmp['fresh_from']}): net [CI], n",
                [
                    f"{_pts(fr.get('adjusted_diff'))} "
                    f"{_ci(fr.get('adjusted_ci_low'), fr.get('adjusted_ci_high'))}"
                    f" n={fr.get('n', '—')}"
                    for fr in (h2(m, "fresh") for m in order)
                ],
            )
        )
        A(
            _row(
                "fresh only: power for September's -6.0",
                [
                    next(
                        (
                            f"{x['power_now']:.0%}"
                            for x in (h2(m, "fresh").get("power") or {}).get("scenarios", [])
                            if x["name"] == "september" and x.get("power_now") is not None
                        ),
                        "—",
                    )
                    for m in order
                ],
            )
        )
    A("")

    A("## 7. Out of sample — excess over each column's own baseline\n")
    A(
        "Chronological 60 / 20 / 20 split on session dates. Points above (+) or "
        "below (−) the same column's all-touch rate; n in brackets.\n"
    )
    A("| hypothesis | range | discovery | validation | test |")
    A("|---|---|---:|---:|---:|")
    for name in ("H1", "H2", "H3"):
        for m in order:
            hyp = (S[m].get("hypotheses") or {}).get(name) or {}
            oos = hyp.get("oos") or {}
            cells = [
                f"{_pts((oos.get(sp) or {}).get('excess_vs_column'))} "
                f"(n={(oos.get(sp) or {}).get('n', 0)})"
                for sp in ("discovery", "validation", "test")
            ]
            A(f"| {name} `{hyp.get('cohort', '?')}` | {m} min | " + " | ".join(cells) + " |")
    A("")

    A("## 8. Cost hurdle (NQ)\n")
    A("| | " + " | ".join(cols) + " |")
    A("|---|" + "---:|" * len(order))

    def hd(m: str) -> Mapping[str, Any]:
        return S[m].get("cost_hurdle") or {}

    A(
        _row(
            "step (pts)",
            [f"{hd(m)['step_pts']:.1f}" if hd(m).get("available") else "—" for m in order],
        )
    )
    A(
        _row(
            "round-turn cost (pts)",
            [f"{hd(m)['cost_pts']:.3f}" if hd(m).get("available") else "—" for m in order],
        )
    )
    A(
        _row(
            "**required excess over null**",
            [
                f"**{hd(m)['hurdle'] * 100:.2f} pts**" if hd(m).get("available") else "—"
                for m in order
            ],
        )
    )
    A("")

    A("## 9. Pre-registered verdicts\n")
    A(
        "Rule fixed before the data was read (see `compare.py`). `defer` beside "
        "a large MDE means the sample cannot see the effect, not that the "
        "effect is promising.\n"
    )
    A("| hypothesis | range | verdict | n | MDE (80%) | why |")
    A("|---|---|---|---:|---:|---|")
    for name, per in (cmp.get("verdicts") or {}).items():
        for m in order:
            v = per.get(m) or {}
            mde = v.get("mde")
            A(
                f"| {name} | {m} min | **{v.get('verdict', '—')}** | {v.get('n', '—')} | "
                f"{'—' if mde is None else f'{mde * 100:.1f} pts'} | "
                f"{'; '.join(v.get('reasons') or [])} |"
            )
    A("")
    for name in ("H1", "H2", "H3"):
        hyp = (S[order[0]].get("hypotheses") or {}).get(name) or {}
        if hyp:
            A(f"- **{name}** — {hyp.get('claim')} (cohort `{hyp.get('cohort')}`)")
    A("")
    return "\n".join(out)


def _num3(v: Optional[float]) -> str:
    return "—" if v is None else f"{v:+.3f}"
