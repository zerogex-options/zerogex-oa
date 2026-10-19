"""Machine-readable and human-readable results.

Three outputs, from one pass over a saved dataset: a JSON summary, a CSV of the
event rows, and a Markdown report that answers the brief's ten questions in
plain English.

Reporting rules that are enforced here rather than left to the reader:

* **N is never hidden.**  Every table carries the event count AND the number of
  distinct sessions.  Twelve events from one session is one observation of a
  day, not twelve observations of a level.
* **Thin cells are labelled, not deleted.**  A cohort below
  ``cohorts.MIN_REPORTABLE_N`` prints with a ``thin`` marker.  Suppressing it
  would hide the "we tried and could not tell" answers, which the brief asks
  for explicitly.
* **Sensitivity is a table, not a footnote.**  A real effect should not
  evaporate when the confluence threshold moves from 10 points to 9, so the
  distance and lead-time sweeps are printed as grids and read as such.
* **Out-of-sample is a date cut.**  Sessions are ordered and split
  60/20/20 with no shuffling; the holdout is reported alongside, never
  instead of, the discovery period.
* **Failures are printed.**  A cohort that does nothing gets a row like any
  other.  There is no path through this module that drops a negative result.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from research.or_gamma_confluence.cohorts import (
    MIN_REPORTABLE_N,
    MIN_TESTABLE_SESSIONS,
    NAMED_RUNGS,
    book_of,
    kinds_agreeing,
    build_cohorts,
    compare_to_baseline,
    has_confluence,
    on_rung,
    pooling_check,
    summarize,
)
from research.msi_regime_excursion import stats
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.events import (
    OUTCOME_CENSORED,
    OUTCOME_REVERSAL,
    RESOLVED_OUTCOMES,
)
from research.or_gamma_confluence.instruments import spec
from research.or_gamma_confluence.null import gamblers_ruin_null
from research.or_gamma_confluence.power import power_report

__all__ = ["split_chronological", "build_summary", "render_markdown", "write_csv"]

_QUESTIONS = (
    "Does OR extension distance predict reversion?",
    "Does gamma confluence improve it?",
    "Does gamma regime change the result?",
    "Does distance from the gamma flip matter?",
    "Does GEX rank / magnitude matter?",
    "Does prior extension failure predict continuation?",
    "Does a trend filter improve results?",
    "Which effects survive out-of-sample testing?",
    "Are effects stable across reasonable parameter ranges?",
    "Is any resulting strategy economically tradeable after costs?",
)


def _pct(v: Optional[float], places: int = 1) -> str:
    return "—" if v is None else f"{100.0 * v:.{places}f}%"


def _num(v: Optional[float], places: int = 2) -> str:
    return "—" if v is None else f"{v:.{places}f}"


def _session_key(row: Mapping[str, Any]) -> str:
    return str(row.get("session") or "")


def split_chronological(
    rows: Sequence[Mapping[str, Any]], cfg: ResearchConfig
) -> dict[str, list[Mapping[str, Any]]]:
    """Discovery / validation / test, cut on SESSION DATE, never shuffled.

    The cut is on distinct sessions rather than on row index: sessions differ
    in event count, so an index cut would place part of one session on each
    side of the boundary and leak a day's regime across it.
    """
    sessions = sorted({_session_key(r) for r in rows if _session_key(r)})
    n = len(sessions)
    if n == 0:
        return {"discovery": [], "validation": [], "test": []}
    d_end = int(n * cfg.discovery_frac)
    v_end = d_end + int(n * cfg.validation_frac)
    buckets = {
        "discovery": set(sessions[:d_end]),
        "validation": set(sessions[d_end:v_end]),
        "test": set(sessions[v_end:]),
    }
    return {name: [r for r in rows if _session_key(r) in keys] for name, keys in buckets.items()}


def _depth_table(rows: Sequence[Mapping[str, Any]], edges: Sequence[float]) -> list[dict[str, Any]]:
    """Reversal rate by extension depth — hypothesis 1's whole test."""
    out: list[dict[str, Any]] = []
    for i, lo in enumerate(edges):
        hi = edges[i + 1] if i + 1 < len(edges) else None
        sel = [
            r
            for r in rows
            if abs(float(r.get("extension_k") or 0.0)) >= lo
            and (hi is None or abs(float(r.get("extension_k") or 0.0)) < hi)
        ]
        stat = summarize(sel)
        stat["band"] = f">={lo:g}R" if hi is None else f"{lo:g}–{hi:g}R"
        out.append(stat)
    return out


def _distance_grid(
    rows: Sequence[Mapping[str, Any]], distances: Sequence[float]
) -> list[dict[str, Any]]:
    """Confluence effect at every distance bucket, on ONE dataset.

    Re-cohorting rather than rebuilding is what makes this honest AND cheap:
    every row of the grid sees exactly the same events, so a difference across
    the row is the threshold and nothing else.
    """
    grid: list[dict[str, Any]] = []
    for d in distances:
        with_c = [r for r in rows if has_confluence(r, d)]
        without = [r for r in rows if not has_confluence(r, d)]
        a, b = summarize(with_c), summarize(without)
        grid.append(
            {
                "distance": d,
                "n_with": a["n_resolved"],
                "rate_with": a["reversal_rate"],
                "n_without": b["n_resolved"],
                "rate_without": b["reversal_rate"],
                "gap": (
                    a["reversal_rate"] - b["reversal_rate"]
                    if a["reversal_rate"] is not None and b["reversal_rate"] is not None
                    else None
                ),
            }
        )
    return grid


def _discrimination(
    rows: Sequence[Mapping[str, Any]], distances: Sequence[float]
) -> dict[str, Any]:
    """Is the confluence cohort a filter, or does it contain everything?

    A confluence test is only informative if "has a gamma level nearby"
    actually splits the sample.  With enough level kinds and a deep enough
    rank ladder, every price in the session has a level near it, and the
    cohort becomes the population — at which point a null result says nothing
    about gamma and everything about level density.

    Also cross-tabs confluence against extension DEPTH, because the obvious
    confound runs that way: ingestion only streams strikes within
    ``INGEST_STRIKE_PCT_RANGE`` (3%) of spot, so a deep extension can sit
    where no gamma level is able to exist.  If the no-confluence group is
    systematically deeper than the confluence group, the two cohorts differ by
    distance-from-spot as much as by gamma, and the comparison is confounded.
    """
    total = [r for r in rows if r.get("gamma_available")]
    counts = [r.get("gamma_levels_total") for r in total]
    nearest = [r.get("nearest_gamma_distance") for r in total]

    coverage = []
    for d in distances:
        n_with = sum(1 for r in rows if has_confluence(r, d))
        coverage.append(
            {
                "distance": d,
                "n_with": n_with,
                "share": (n_with / len(rows)) if rows else None,
            }
        )

    # Base rate of multi-metric agreement. If four metrics land together on a
    # third of all prices, "four metrics agree" is not a selective signal.
    agreement = []
    for d in distances:
        row = {"distance": d}
        for k in (2, 3, 4):
            n = sum(1 for r in rows if kinds_agreeing(r, d) >= k)
            row[f"kinds_{k}"] = n
            row[f"kinds_{k}_share"] = (n / len(rows)) if rows else None
        agreement.append(row)

    def _depth_of(r: Mapping[str, Any]) -> Optional[float]:
        k = r.get("extension_k")
        return abs(float(k)) if k is not None else None

    by_group = {}
    for label, sel in (
        ("with confluence", [r for r in rows if has_confluence(r, 10.0)]),
        ("no confluence", [r for r in rows if not has_confluence(r, 10.0)]),
    ):
        depths = stats.describe([_depth_of(r) for r in sel])
        by_group[label] = {
            "n": len(sel),
            "median_depth_r": depths.median,
            "mean_depth_r": depths.mean,
        }

    return {
        "n_with_gamma": len(total),
        "levels_per_snapshot_median": stats.describe(counts).median,
        "nearest_distance_median": stats.describe(nearest).median,
        "coverage": coverage,
        "agreement": agreement,
        "depth_by_group": by_group,
    }


def _mechanical_null(rows: Sequence[Mapping[str, Any]], cfg: ResearchConfig) -> dict[str, Any]:
    """The reversal rate this event definition produces with NO mean reversion.

    A touch fires when the bar's extreme reaches the rung, but the forward scan
    starts from that bar's CLOSE, which has usually retreated back inside by
    some offset ``d``.  Price therefore begins the race to prev-vs-next already
    displaced toward prev.  For a driftless walk between barriers at ``-step``
    and ``+step`` starting at ``-d``, gambler's ruin gives

        P(reversal) = 0.5 + d / (2 * step)

    which is a pure artefact of the measurement and carries no information
    about the market.  Reporting the observed rate against 50% would credit
    that artefact as mean reversion; this is the honest baseline instead.

    Confirmed empirically by the parameter sweep: a single ``d = 0.026R``
    predicts the 0.25R and 0.5R cells to within 0.1 points (55.2 vs 55.1,
    52.6 vs 52.6), and a driftless simulation reproduces the whole column.
    """
    offsets = [
        r.get("touch_offset_r")
        for r in rows
        if r.get("next_exists") and r.get("touch_offset_r") is not None
    ]
    if not offsets:
        return {"available": False}
    step = cfg.extension_step
    per_event = [gamblers_ruin_null(d, step) for d in offsets]
    return {
        "available": True,
        "n": len(per_event),
        "median_offset_r": stats.describe(offsets).median,
        "expected_reversal_rate": sum(per_event) / len(per_event),
    }


def _formula_null_rate(rows: Sequence[Mapping[str, Any]], step: float) -> Optional[float]:
    """Mean gambler's-ruin null over a set of rows' RESOLVED touches.

    Over resolved touches, so it is on the same footing as the reversal rate
    it is compared with.  ``_mechanical_null`` averages over every touch with
    a next rung instead, which is what September reported; the two differ
    only by the censored touches.
    """
    vals = [
        gamblers_ruin_null(r.get("touch_offset_r"), step)
        for r in rows
        if r.get("outcome") in RESOLVED_OUTCOMES
        and r.get("next_exists")
        and r.get("touch_offset_r") is not None
    ]
    return (sum(vals) / len(vals)) if vals else None


def _surrogate_rate(
    null_rows: Sequence[Mapping[str, Any]],
    predicate: Callable[[Mapping[str, Any]], bool] = lambda r: True,
) -> dict[str, Any]:
    """Reversal-first rate among the surrogate touches a predicate selects.

    ``se`` is binomial over surrogate touches: it is the Monte Carlo error of
    the null itself, not the uncertainty of the real sample, and it is small
    by construction because every session is replayed many times.
    """
    sel = [r for r in null_rows if predicate(r)]
    resolved = [r for r in sel if r.get("outcome") in RESOLVED_OUTCOMES]
    n = len(resolved)
    if not n:
        return {"rate": None, "n": 0, "se": None, "censored_share": None}
    k = sum(1 for r in resolved if r.get("outcome") == OUTCOME_REVERSAL)
    rate = k / n
    return {
        "rate": rate,
        "n": n,
        "se": (rate * (1.0 - rate) / n) ** 0.5,
        "censored_share": sum(1 for r in sel if r.get("outcome") == OUTCOME_CENSORED) / len(sel),
    }


def _clustered_rate(
    rows: Sequence[Mapping[str, Any]], *, iterations: int = 2000, seed: int = 20261018
) -> dict[str, Any]:
    """Reversal rate with a session-block bootstrap interval.

    The interval a rate needs before it can be set against a null: touches on
    one day share that day's path, so resampling touches would claim more
    independent observations than there are days.
    """
    per: dict[str, list[int]] = {}
    for r in rows:
        if r.get("outcome") not in RESOLVED_OUTCOMES:
            continue
        cell = per.setdefault(_session_key(r), [0, 0])
        cell[0] += 1 if r.get("outcome") == OUTCOME_REVERSAL else 0
        cell[1] += 1
    cells = list(per.values())
    k = sum(c[0] for c in cells)
    n = sum(c[1] for c in cells)
    out: dict[str, Any] = {"rate": (k / n) if n else None, "n": n, "ci_low": None, "ci_high": None}
    # Same floor as a cohort test, for the same reason: with a handful of
    # sessions the resample collapses and the interval is a fiction.
    if len(cells) < MIN_TESTABLE_SESSIONS or n < MIN_REPORTABLE_N:
        return out
    rng = random.Random(seed)
    draws = []
    m = len(cells)
    for _ in range(iterations):
        kk = nn = 0
        for _ in range(m):
            c = cells[rng.randrange(m)]
            kk += c[0]
            nn += c[1]
        if nn:
            draws.append(kk / nn)
    draws.sort()
    out["ci_low"] = draws[int(0.025 * (len(draws) - 1))]
    out["ci_high"] = draws[int(0.975 * (len(draws) - 1))]
    return out


def _null_block(
    rows: Sequence[Mapping[str, Any]],
    null_rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    predicate: Callable[[Mapping[str, Any]], bool] = lambda r: True,
) -> dict[str, Any]:
    """Observed rate against BOTH nulls, for one price-only selection.

    ``excess`` is measured against the surrogate null when there is one, and
    against the formula otherwise.  The interval is the observed rate's
    clustered interval shifted by the null, which is treated as known: its
    Monte Carlo error is reported beside it and is several times smaller.
    """
    sel = [r for r in rows if predicate(r)]
    obs = _clustered_rate(sel)
    formula = _formula_null_rate(sel, cfg.extension_step)
    sur = _surrogate_rate(null_rows, predicate) if null_rows else {"rate": None, "n": 0}
    null = sur["rate"] if sur.get("rate") is not None else formula
    out = {
        "observed": obs["rate"],
        "observed_ci_low": obs["ci_low"],
        "observed_ci_high": obs["ci_high"],
        "n": obs["n"],
        "n_sessions": len({_session_key(r) for r in sel if r.get("outcome") in RESOLVED_OUTCOMES}),
        "formula_null": formula,
        "surrogate_null": sur.get("rate"),
        "surrogate_n": sur.get("n"),
        "surrogate_se": sur.get("se"),
        "surrogate_censored_share": sur.get("censored_share"),
        "null_used": "surrogate" if sur.get("rate") is not None else "formula",
        "excess": None,
        "excess_ci_low": None,
        "excess_ci_high": None,
    }
    if obs["rate"] is not None and null is not None:
        out["excess"] = obs["rate"] - null
        if obs["ci_low"] is not None:
            out["excess_ci_low"] = obs["ci_low"] - null
            out["excess_ci_high"] = obs["ci_high"] - null
    return out


def _named_rung_table(
    rows: Sequence[Mapping[str, Any]],
    null_rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    cohort_by_key: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """The customer's rungs, one row per (rung, side), against their nulls.

    ``on_ladder`` is False when the configured step does not land on the
    rung (1.5R on a 1.0R ladder): then there is nothing to measure and the
    row says so instead of printing an empty rate that reads like a result.
    """
    step = cfg.extension_step
    out = []
    for k in NAMED_RUNGS:
        on_ladder = abs(round(k / step) * step - k) < 1e-9 and k <= cfg.max_extension + 1e-9
        for side, label in (
            (None, f"±{k * 100:g}%"),
            ("down", f"-{k * 100:g}%"),
            ("up", f"+{k * 100:g}%"),
        ):

            def pred(r: Mapping[str, Any], k: float = k, side: Optional[str] = side) -> bool:
                return on_rung(r, k, side)

            row: dict[str, Any] = {
                "rung": label,
                "k": k,
                "side": side or "both",
                "on_ladder": on_ladder,
            }
            if on_ladder:
                row.update(_null_block(rows, null_rows, cfg, pred))
                key = {None: f"rung_{k * 100:g}", "down": f"rung_m{k * 100:g}"}.get(side)
                c = cohort_by_key.get(key) if key else None
                if c is not None:
                    row["clustered_p"] = c.get("clustered_p")
                    row["bh_significant"] = c.get("bh_significant")
            out.append(row)
    return out


def _depth_rho(rows: Sequence[Mapping[str, Any]], step: float) -> dict[str, Any]:
    """H1 as one number: Spearman(depth, reversal), raw and net of the formula.

    The net version correlates depth with ``outcome - formula null``, so a
    tilt produced by the close-offset (shallow rungs have a narrower step in
    points, so the same offset is a larger share of it) is taken out first.
    """
    res = [r for r in rows if r.get("outcome") in RESOLVED_OUTCOMES]
    depth = [abs(float(r.get("extension_k") or 0.0)) for r in res]
    hit = [1.0 if r.get("outcome") == OUTCOME_REVERSAL else 0.0 for r in res]
    sess = [_session_key(r) for r in res]
    raw = stats.spearman_block(depth, hit, sess).as_dict() if len(res) > 10 else None
    resid = [
        (h - n) if n is not None else None
        for h, n in zip(
            hit,
            (
                gamblers_ruin_null(r.get("touch_offset_r"), step) if r.get("next_exists") else None
                for r in res
            ),
        )
    ]
    net = stats.spearman_block(depth, resid, sess).as_dict() if len(res) > 10 else None
    return {"raw": raw, "net_of_formula_null": net}


def _cost_hurdle(
    rows: Sequence[Mapping[str, Any]], cfg: ResearchConfig, symbol: str = "NQ"
) -> dict[str, Any]:
    """The win-rate excess over the null a trade needs to pay for itself.

    Enter at the touch bar's close, target the previous rung, stop at the
    next.  With win ``s - d`` and loss ``s + d`` and a round-turn cost ``c``
    in points, break-even is ``P = 0.5 + d/(2s) + c/(2s)``: the mechanical
    null plus ``c / (2s)``.  ``s`` is ``extension_step * R``, and R is the
    MEDIAN opening range of the symbol's sessions in this dataset, so the
    hurdle falls as the range widens: a 60-minute range spreads the same
    $14.50 over a wider step.
    """
    inst = spec(symbol)
    widths: dict[str, float] = {}
    for r in rows:
        if r.get("symbol") == symbol and r.get("or_width"):
            widths[_session_key(r)] = float(r["or_width"])
    if not widths or not inst.point_value or not inst.tick:
        return {"available": False, "symbol": symbol}
    median_r = stats.describe(list(widths.values())).median
    step_pts = cfg.extension_step * median_r
    cost_pts = cfg.commission_round_turn / inst.point_value + 2.0 * cfg.slippage_ticks * inst.tick
    return {
        "available": True,
        "symbol": symbol,
        "sessions": len(widths),
        "median_or_width_pts": median_r,
        "step_pts": step_pts,
        "cost_pts": cost_pts,
        "cost_dollars": cost_pts * inst.point_value,
        "hurdle": cost_pts / (2.0 * step_pts) if step_pts > 0 else None,
    }


#: The cohort each hypothesis is judged on, and the direction it claims.
#: ``+1`` claims MORE reversion than the null; ``-1`` claims less (that is,
#: continuation).  Fixed here, before the 60-minute data was read, so the
#: verdict cannot drift toward whichever cohort happens to look best.
HYPOTHESES: dict[str, dict[str, Any]] = {
    "H1": {
        "cohort": "rung_named",
        "sign": 1,
        "claim": "the customer's marked rungs (±100/150/200% of R) revert more",
    },
    "H2": {
        "cohort": "prior_broken",
        "sign": -1,
        "claim": "after 2+ broken rungs, price continues more",
    },
    "H3": {
        "cohort": "confluence",
        "sign": 1,
        "claim": "a pre-existing gamma level at the rung raises reversion",
    },
}


def _hypothesis_stats(
    rows: Sequence[Mapping[str, Any]],
    null_rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    cohorts: Sequence[Any],
    cohort_by_key: Mapping[str, Mapping[str, Any]],
    oos: Mapping[str, Any],
) -> dict[str, Any]:
    """The numbers the pre-registered verdict reads, one block per hypothesis.

    Two different comparisons, because the verdict needs both:

    * against the BASELINE (``vs_baseline``, from the cohort table): does the
      cohort behave differently from the average touch?  That is the claim,
      and it is what the multiplicity correction is applied to;
    * against its OWN NULL (``own_null``): is the cohort's rate far enough
      from what the event rule produces on a directionless tape to pay the
      cost of a trade?  That is what the cost hurdle is compared with.
    """
    by_key = {c.key: c for c in cohorts}
    out: dict[str, Any] = {}
    for name, h in HYPOTHESES.items():
        c = by_key[h["cohort"]]
        stat = cohort_by_key[h["cohort"]]
        splits = {
            split: {
                "n": (part.get("cohorts", {}).get(c.key) or {}).get("n_resolved", 0),
                "excess_vs_column": (part.get("cohorts", {}).get(c.key) or {}).get(
                    "excess_vs_column"
                ),
            }
            for split, part in oos.items()
        }
        out[name] = {
            **h,
            "label": stat.get("label"),
            "n": stat.get("n_resolved"),
            "n_sessions": stat.get("n_sessions"),
            "vs_baseline": {
                k: stat.get(k)
                for k in (
                    "clustered_diff",
                    "clustered_ci_low",
                    "clustered_ci_high",
                    "clustered_p",
                    "bh_significant",
                    "null_shift",
                    "net_of_null_diff",
                    "testable",
                    "cohort_sessions",
                )
            },
            "own_null": _null_block(rows, null_rows if c.price_only else [], cfg, c.predicate),
            "oos": splits,
            "pooling": pooling_check(list(rows), c.predicate).get("verdict"),
        }
    return out


#: The H2 effect the September findings reported, as a rate difference.  Kept
#: as a power scenario so every run says how far it is from resolving THAT.
SEPTEMBER_H2_EFFECT = -0.060


def _h2_block(
    rows: Sequence[Mapping[str, Any]],
    null_rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    broken_predicate: Callable[[Mapping[str, Any]], bool],
    fresh_from: Optional[str],
) -> dict[str, Any]:
    """H2 -- broken ladders -> continuation -- with its power, twice over.

    Once on the whole window (the best current estimate), and once on the
    sessions from ``fresh_from`` on.  The second is the only clean test: the
    hypothesis was named after looking at the September window, so a test that
    re-uses those sessions is partly grading its own homework.
    """

    def one(subset: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        cmp = compare_to_baseline(subset, broken_predicate)
        n_cal = len({_session_key(r) for r in subset if r.get("outcome") in RESOLVED_OUTCOMES})
        shift = None
        if null_rows:
            keep = {_session_key(r) for r in subset}
            nsub = [r for r in null_rows if _session_key(r) in keep]
            a = _surrogate_rate(nsub, broken_predicate)["rate"]
            b = _surrogate_rate(nsub)["rate"]
            shift = (a - b) if (a is not None and b is not None) else None
        if shift is None:
            fa = _formula_null_rate([r for r in subset if broken_predicate(r)], cfg.extension_step)
            fb = _formula_null_rate(subset, cfg.extension_step)
            shift = (fa - fb) if (fa is not None and fb is not None) else None
        diff = cmp.get("clustered_diff")
        adj = (diff - shift) if (diff is not None and shift is not None) else diff
        lo, hi = cmp.get("clustered_ci_low"), cmp.get("clustered_ci_high")
        out = {
            **cmp,
            "n_calendar_sessions": n_cal,
            "cohort_sessions": len(
                {
                    _session_key(r)
                    for r in subset
                    if broken_predicate(r) and r.get("outcome") in RESOLVED_OUTCOMES
                }
            ),
            "null_shift": shift,
            "adjusted_diff": adj,
            "adjusted_ci_low": (lo - shift) if (lo is not None and shift is not None) else lo,
            "adjusted_ci_high": (hi - shift) if (hi is not None and shift is not None) else hi,
        }
        out["power"] = power_report(adj, lo, hi, n_cal, effects={"september": SEPTEMBER_H2_EFFECT})
        return out

    block = {"whole_window": one(rows), "fresh_from": fresh_from, "fresh": None}
    if fresh_from:
        fresh = [r for r in rows if _session_key(r) >= fresh_from]
        if fresh:
            block["fresh"] = one(fresh)
    return block


def build_summary(
    rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    meta: Optional[Mapping[str, Any]] = None,
    *,
    confluence_distance: float = 10.0,
    null_rows: Optional[Sequence[Mapping[str, Any]]] = None,
    fresh_from: Optional[str] = None,
) -> dict[str, Any]:
    """Every number the Markdown report prints, as JSON.

    ``null_rows`` are the dataset's surrogate touches (``dataset.read_null``);
    without them every null falls back to the gambler's-ruin formula and the
    report says which one it used.  ``fresh_from`` (ISO date) adds the H2 test
    on sessions from that date only.
    """
    null_rows = list(null_rows or [])
    cohorts = build_cohorts(
        confluence_distance=confluence_distance,
        min_extension=cfg.min_extension_for_reversion,
        min_broken=cfg.min_failed_extensions_for_continuation,
    )
    baseline = list(rows)
    formula_all = _formula_null_rate(baseline, cfg.extension_step)
    surrogate_all = _surrogate_rate(null_rows)["rate"] if null_rows else None
    cohort_stats: list[dict[str, Any]] = []
    for c in cohorts:
        sel = c.select(rows)
        stat = {
            "key": c.key,
            "label": c.label,
            "description": c.description,
            "price_only": c.price_only,
        }
        stat.update(summarize(sel))
        if c.key != "all":
            stat.update(compare_to_baseline(baseline, c.predicate))
        # How much of a cohort's gap to the baseline the measurement itself
        # explains. Every cohort gets the formula's answer, from its own
        # touches' close-offsets; a price-only cohort also gets the
        # surrogates', which carry the bell as well.
        f = _formula_null_rate(sel, cfg.extension_step)
        stat["formula_null_shift"] = (
            (f - formula_all) if (f is not None and formula_all is not None) else None
        )
        stat["surrogate_null_shift"] = None
        if c.price_only and surrogate_all is not None:
            sr = _surrogate_rate(null_rows, c.predicate)["rate"]
            stat["surrogate_null_shift"] = (sr - surrogate_all) if sr is not None else None
        shift = (
            stat["surrogate_null_shift"]
            if stat["surrogate_null_shift"] is not None
            else stat["formula_null_shift"]
        )
        diff = stat.get("clustered_diff")
        stat["null_shift"] = shift
        stat["net_of_null_diff"] = (
            (diff - shift) if (diff is not None and shift is not None) else None
        )
        cohort_stats.append(stat)

    # Multiplicity. Fourteen cohorts times several thresholds is a machine for
    # producing one p<0.05 by chance; the family is corrected here rather than
    # left as an instruction in the prose, because an instruction in the prose
    # is exactly what gets skipped when a number looks interesting.
    testable = [c for c in cohort_stats if c.get("clustered_p") is not None]
    flags = stats.benjamini_hochberg([c["clustered_p"] for c in testable], alpha=0.05)
    for c, flag in zip(testable, flags):
        c["bh_significant"] = bool(flag)
    for c in cohort_stats:
        c.setdefault("bh_significant", None)
    bh = {
        "family_size": len(testable),
        "alpha": 0.05,
        "survivors": [c["key"] for c in testable if c["bh_significant"]],
        "min_p": min((c["clustered_p"] for c in testable), default=None),
    }

    splits = split_chronological(rows, cfg)
    oos = {}
    for name, part in splits.items():
        per = {c.key: summarize(c.select(part)) for c in cohorts}
        base_rate = per["all"]["reversal_rate"]
        for st in per.values():
            # Against the SAME column's baseline: the baseline drifts across
            # the split, so a raw rate compared across columns mixes the two.
            st["excess_vs_column"] = (
                st["reversal_rate"] - base_rate
                if st["reversal_rate"] is not None and base_rate is not None
                else None
            )
        oos[name] = {
            "sessions": len({_session_key(r) for r in part}),
            "first": min((_session_key(r) for r in part), default=None),
            "last": max((_session_key(r) for r in part), default=None),
            "cohorts": per,
        }

    pooling = pooling_check(list(rows), lambda r: has_confluence(r, confluence_distance))

    by_book = {
        book: summarize([r for r in rows if book_of(r) == book])
        for book in sorted({b for b in (book_of(r) for r in rows) if b})
    }

    by_symbol = {
        sym: summarize([r for r in rows if r.get("symbol") == sym])
        for sym in sorted({str(r.get("symbol")) for r in rows if r.get("symbol")})
    }

    depth = _depth_table(rows, (0.5, 1.0, 2.0, 3.0, 5.0))
    for band, (lo, hi) in zip(depth, ((0.5, 1.0), (1.0, 2.0), (2.0, 3.0), (3.0, 5.0), (5.0, None))):

        def in_band(r: Mapping[str, Any], lo: float = lo, hi: Optional[float] = hi) -> bool:
            k = abs(float(r.get("extension_k") or 0.0))
            return k >= lo and (hi is None or k < hi)

        band["null"] = _null_block(rows, null_rows, cfg, in_band)

    cohort_by_key = {c["key"]: c for c in cohort_stats}
    broken = next(c for c in cohorts if c.key == "prior_broken")
    mech = _mechanical_null(rows, cfg)
    mech["headline"] = _null_block(rows, null_rows, cfg)

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "config_fingerprint": cfg.fingerprint(),
        "config": cfg.to_dict(),
        "provenance": dict(meta) if meta else None,
        "confluence_distance_used": confluence_distance,
        "overall": summarize(rows),
        "by_symbol": by_symbol,
        "by_book": by_book,
        "pooling_check": pooling,
        "cohorts": cohort_stats,
        "depth_table": depth,
        "depth_rho": _depth_rho(rows, cfg.extension_step),
        "named_rungs": _named_rung_table(rows, null_rows, cfg, cohort_by_key),
        "h2": _h2_block(rows, null_rows, cfg, broken.predicate, fresh_from),
        "hypotheses": _hypothesis_stats(rows, null_rows, cfg, cohorts, cohort_by_key, oos),
        "cost_hurdle": _cost_hurdle(rows, cfg),
        "distance_grid": _distance_grid(rows, cfg.confluence_buckets_pts),
        "discrimination": _discrimination(rows, cfg.confluence_buckets_pts),
        "mechanical_null": mech,
        "null_rows": len(null_rows),
        "out_of_sample": oos,
        "multiplicity": bh,
        "min_reportable_n": MIN_REPORTABLE_N,
    }


def _cohort_rows_md(cohorts: Sequence[Mapping[str, Any]]) -> list[str]:
    lines = [
        "| cohort | n | sessions | reversal | 95% CI | vs baseline | clustered 95% CI | p | BH "
        "| null shift | net of null |",
        "|---|---:|---:|---:|---|---:|---|---:|---|---:|---:|",
    ]
    for c in cohorts:
        ci = (
            f"[{_pct(c.get('reversal_ci_low'))} – {_pct(c.get('reversal_ci_high'))}]"
            if c.get("reversal_ci_low") is not None
            else "—"
        )
        diff = c.get("clustered_diff")
        eff = "—" if diff is None else f"{diff * 100:+.1f} pts"
        band = (
            f"[{c['clustered_ci_low'] * 100:+.1f}, {c['clustered_ci_high'] * 100:+.1f}]"
            if c.get("clustered_ci_low") is not None and c.get("clustered_ci_high") is not None
            else "—"
        )
        pval = "—" if c.get("clustered_p") is None else f"{c['clustered_p']:.3f}"
        bh = {True: "**yes**", False: "no", None: "—"}[c.get("bh_significant")]
        thin = " ⚠︎thin" if c.get("thin") else ""
        shift = c.get("null_shift")
        tag = "ˢ" if c.get("surrogate_null_shift") is not None else ""
        shift_txt = "—" if shift is None or c.get("key") == "all" else f"{shift * 100:+.1f}{tag}"
        net = c.get("net_of_null_diff")
        net_txt = "—" if net is None else f"{net * 100:+.1f} pts"
        lines.append(
            f"| {c['label']}{thin} | {c['n_resolved']} | {c['n_sessions']} | "
            f"{_pct(c.get('reversal_rate'))} | {ci} | {eff} | {band} | {pval} | {bh} | "
            f"{shift_txt} | {net_txt} |"
        )
    return lines


def render_markdown(summary: Mapping[str, Any]) -> str:
    """The human-readable report.  Prints failures as prominently as successes."""
    cfg = summary.get("config", {})
    prov = summary.get("provenance") or {}
    overall = summary["overall"]
    out: list[str] = []
    A = out.append

    A("# Opening-range extension × gamma confluence — results\n")
    A(f"*Generated {summary['generated_at']}*  ")
    A(f"*Config fingerprint `{summary['config_fingerprint']}`*\n")
    A(
        "> **Nothing here is calibrated for trading and no result has been "
        "validated live.** Read every rate WITH its N and its interval.\n"
    )

    A("## Provenance\n")
    A(f"- Window: `{prov.get('start', '?')}` .. `{prov.get('end', '?')}`")
    A(f"- Symbols: {', '.join(prov.get('symbols', [])) or '—'}")
    A(
        f"- Opening range: {cfg.get('opening_range_minutes')} min, "
        f"mode `{cfg.get('extension_mode')}`, step {cfg.get('extension_step')}R, "
        f"max {cfg.get('max_extension')}R"
    )
    A(
        f"- Availability clock: `{cfg.get('availability_clock')}` "
        f"(+{cfg.get('client_poll_lag_seconds')}s poll lag), "
        f"minimum lead {cfg.get('gamma_min_lead_seconds')}s"
    )
    A(f"- Confluence distance used for cohorts: " f"{summary['confluence_distance_used']:g} pts")
    A(
        f"- Events: **{overall['n']}** ({overall['n_resolved']} resolved) across "
        f"**{overall['n_sessions']}** calendar sessions "
        f"/ {overall.get('n_symbol_sessions', '?')} symbol-sessions"
    )
    A(
        "- Inference is clustered on the CALENDAR session: SPY, SPX and ES are "
        "one option book on one set of days, so pooling them adds events, not "
        "independent observations."
    )
    if prov.get("skip_reasons"):
        A(f"- Sessions skipped: `{json.dumps(prov['skip_reasons'])}`")
    A("")

    A("## Headline\n")
    A(
        f"- Reversal-first rate, all touches: **{_pct(overall.get('reversal_rate'))}** "
        f"(n={overall['n_resolved']})"
    )
    A(
        f"- Continuation-first: {_pct(overall.get('continuation_rate'))}, of which "
        f"{overall['n_continuation_same_bar']} sliced through inside the touch bar"
    )
    A(
        f"- Unresolved by the bell (censored): {_pct(overall.get('censored_rate'))}; "
        f"ambiguous (both rungs inside one minute): {overall['n_ambiguous']}\n"
    )

    mech = summary.get("mechanical_null") or {}
    if mech.get("available"):
        exp = mech["expected_reversal_rate"]
        obs = overall.get("reversal_rate")
        A("### The baseline is not 50%\n")
        A(
            f"A touch fires on the bar's extreme reaching the rung, but the "
            f"forward scan starts from that bar's CLOSE — typically "
            f"{_num(mech.get('median_offset_r'), 4)}R back inside the level. Price "
            f"therefore starts the prev-vs-next race already displaced toward "
            f"prev. For a driftless walk, gambler's ruin puts the reversal rate "
            f"at **{_pct(exp)}** with no mean reversion anywhere.\n"
        )
        if obs is not None:
            excess = obs - exp
            A(
                f"- Observed: {_pct(obs)}  |  mechanical null: {_pct(exp)}  |  "
                f"**excess: {excess * 100:+.1f} pts**\n"
            )
            if abs(excess) < 0.01:
                A(
                    "> **The headline rate is the artefact.** Measured against "
                    "the right baseline there is no mean reversion here at all. "
                    "Any cohort must be read against this number, never against "
                    "50%.\n"
                )
        head = mech.get("headline") or {}
        if head.get("surrogate_null") is not None:
            A(
                "**The same null, measured.** The formula assumes every race "
                "finishes; the surrogates replay each session's own post-range "
                "bars with random direction through the identical rule, so the "
                "bell and the unresolved touches are in it too.\n"
            )
            A("| | rate | n |")
            A("|---|---:|---:|")
            A(f"| observed (resolved touches) | {_pct(head.get('observed'))} | {head.get('n')} |")
            A(
                f"| formula null (same touches) | {_pct(head.get('formula_null'))} | "
                f"{head.get('n')} |"
            )
            A(
                f"| surrogate null | {_pct(head.get('surrogate_null'))} "
                f"(±{_pct(head.get('surrogate_se'), 2)} MC) | {head.get('surrogate_n')} |"
            )
            A(
                f"| surrogate touches unresolved at the bell | "
                f"{_pct(head.get('surrogate_censored_share'))} | |"
            )
            A("")
            if head.get("excess") is not None:
                band = (
                    f"[{head['excess_ci_low'] * 100:+.1f}, {head['excess_ci_high'] * 100:+.1f}]"
                    if head.get("excess_ci_low") is not None
                    else ""
                )
                A(
                    f"- **Excess over the surrogate null: {head['excess'] * 100:+.1f} pts** "
                    f"{band} (session-clustered).\n"
                )
        elif head:
            A(
                "> No surrogate rows were found beside this dataset, so every null "
                "below is the formula alone. Rebuild with `null_replicas > 0` to "
                "measure it.\n"
            )
    A("## Q1 — Does extension distance predict reversion?\n")
    A(
        "| depth | n | sessions | reversal | 95% CI | null | excess | "
        "median MFE (30m) | median MAE (30m) |"
    )
    A("|---|---:|---:|---:|---|---:|---:|---:|---:|")
    for band in summary["depth_table"]:
        ci = (
            f"[{_pct(band.get('reversal_ci_low'))} – {_pct(band.get('reversal_ci_high'))}]"
            if band.get("reversal_ci_low") is not None
            else "—"
        )
        thin = " ⚠︎thin" if band.get("thin") else ""
        nb = band.get("null") or {}
        null = (
            nb.get("surrogate_null")
            if nb.get("surrogate_null") is not None
            else nb.get("formula_null")
        )
        exc = nb.get("excess")
        A(
            f"| {band['band']}{thin} | {band['n_resolved']} | {band['n_sessions']} | "
            f"{_pct(band.get('reversal_rate'))} | {ci} | {_pct(null)} | "
            f"{'—' if exc is None else f'{exc * 100:+.1f}'} | "
            f"{_num(band.get('mfe_30_median'))} | {_num(band.get('mae_30_median'))} |"
        )
    A("")
    rho = summary.get("depth_rho") or {}
    for label, key in (("raw", "raw"), ("net of the formula null", "net_of_formula_null")):
        r = rho.get(key) or {}
        if r.get("rho") is not None:
            band = f" [{r['ci_lo']:+.3f}, {r['ci_hi']:+.3f}]" if r.get("ci_lo") is not None else ""
            A(
                f"- Spearman(depth, reversal), {label}: **{r['rho']:+.3f}**{band}, "
                f"n={r['n']}, {r['n_sessions']} sessions"
            )
    A("")
    A(
        "A monotone rise down this column is what the 'rubber band' claim predicts. "
        "A flat column falsifies it, and a rise that is inside the intervals is not "
        "a result.\n"
    )
    rungs = summary.get("named_rungs") or []
    if rungs:
        A("### The customer's rungs: -100%, -150%, -200% of the range width\n")
        A(
            "Exact rungs, not bands. `-` is below the range, the side his chart "
            "marks; `+` is the mirror above; `±` pools both. Each is set against "
            "its OWN null, because the touches that reach a rung are not a random "
            "draw of all touches. `p` and `BH` are the cohort test against the "
            "baseline (cohorts 18-24).\n"
        )
        A("| rung | n | sessions | reversal | null | excess | clustered 95% CI | p | BH |")
        A("|---|---:|---:|---:|---:|---:|---|---:|---|")
        for r in rungs:
            if not r.get("on_ladder"):
                A(f"| {r['rung']} | — | — | not a rung at this step | | | | | |")
                continue
            null = r.get("surrogate_null")
            if null is None:
                null = r.get("formula_null")
            exc = r.get("excess")
            band = (
                f"[{r['excess_ci_low'] * 100:+.1f}, {r['excess_ci_high'] * 100:+.1f}]"
                if r.get("excess_ci_low") is not None
                else "—"
            )
            pval = "—" if r.get("clustered_p") is None else f"{r['clustered_p']:.3f}"
            bh = {True: "**yes**", False: "no", None: "—"}[r.get("bh_significant")]
            thin = " ⚠︎thin" if (r.get("n") or 0) < MIN_REPORTABLE_N else ""
            A(
                f"| {r['rung']}{thin} | {r.get('n')} | {r.get('n_sessions')} | "
                f"{_pct(r.get('observed'))} | {_pct(null)} | "
                f"{'—' if exc is None else f'{exc * 100:+.1f}'} | {band} | {pval} | {bh} |"
            )
        A("")

    A("## Q2–Q7 — Cohorts\n")
    out.extend(_cohort_rows_md(summary["cohorts"]))
    A("")
    A(
        "`vs baseline` is the cohort rate minus the UNCONDITIONAL rate over all "
        "touches; the interval and p come from a bootstrap that resamples whole "
        "SESSIONS, so they reflect the number of independent days rather than the "
        "number of minutes. An interval spanning zero means the cohort is not "
        "distinguishable from the baseline, however large the point estimate looks. "
        "The `BH` column is Benjamini-Hochberg across this whole cohort family at "
        "alpha=0.05 — a row is only worth reading as a finding if it survives "
        "there, not on its own p-value.\n"
    )
    mult = summary.get("multiplicity") or {}
    if mult.get("family_size"):
        survivors = mult.get("survivors") or []
        if survivors:
            A(
                f"**Benjamini-Hochberg across all {mult['family_size']} cohorts "
                f"(alpha=0.05): {len(survivors)} survive** — "
                f"`{'`, `'.join(survivors)}`.\n"
            )
        else:
            A(
                f"**Benjamini-Hochberg across all {mult['family_size']} cohorts "
                f"(alpha=0.05): NOTHING SURVIVES.** Smallest p is "
                f"{mult.get('min_p'):.3f} against a rank-1 threshold of "
                f"{0.05 / mult['family_size']:.4f}. A single row that looks "
                f"significant on its own p-value is what this many comparisons "
                f"produces by chance.\n"
            )

    if (cfg.get("trend_filter") or "none") == "none":
        A(
            "> Cohorts 8 and 9 are empty by construction: `trend_filter` is `none`, "
            "so no trend read is selected. Re-run with `--trend-filter ema_slope` "
            "(or `hma` / `vwap_slope` / `trade_bias`) to populate them.\n"
        )

    A("## Is the confluence cohort actually a filter?\n")
    disc = summary.get("discrimination") or {}
    A(
        f"- Gamma levels published per snapshot (median): "
        f"**{_num(disc.get('levels_per_snapshot_median'), 0)}**"
    )
    A(
        f"- Distance from an extension to its nearest level (median): "
        f"**{_num(disc.get('nearest_distance_median'))} pts**\n"
    )
    A("| threshold | touches with confluence | share of sample |")
    A("|---:|---:|---:|")
    for c in disc.get("coverage", []):
        A(f"| ≤{c['distance']:g} pts | {c['n_with']} | {_pct(c['share'])} |")
    A("")
    worst = max((c["share"] or 0.0) for c in disc.get("coverage", [{"share": 0.0}]))
    if worst > 0.7:
        A(
            f"> **The cohort contains {_pct(worst)} of the sample at its widest "
            "threshold.** A filter that keeps most of the population is not "
            "separating anything, and a null result through it is a statement "
            "about level density rather than about gamma. Reduce "
            "`gex_ladder_depth`, or read only the tightest threshold row.\n"
        )
    agree = disc.get("agreement") or []
    if agree:
        A(
            "**How often do several metrics agree by chance?** The product's copy "
            "says 'when four metrics agree on one strike, that's the level'. That "
            "is only selective if the base rate is low.\n"
        )
        A("| threshold | >=2 metrics | >=3 metrics | >=4 metrics |")
        A("|---:|---:|---:|---:|")
        for a in agree:
            A(
                f"| <={a['distance']:g} pts | {_pct(a.get('kinds_2_share'))} | "
                f"{_pct(a.get('kinds_3_share'))} | {_pct(a.get('kinds_4_share'))} |"
            )
        A("")

    dg = disc.get("depth_by_group") or {}
    if dg:
        A("| group | n | median depth | mean depth |")
        A("|---|---:|---:|---:|")
        for label, v in dg.items():
            A(
                f"| {label} | {v['n']} | {_num(v.get('median_depth_r'))}R | "
                f"{_num(v.get('mean_depth_r'))}R |"
            )
        A("")
        a = (dg.get("no confluence") or {}).get("median_depth_r")
        b = (dg.get("with confluence") or {}).get("median_depth_r")
        if a is not None and b is not None and a > b * 1.25:
            A(
                "> **Confounded.** The no-confluence group sits systematically "
                "deeper. Ingestion streams strikes only within 3% of spot, so a "
                "far extension is somewhere a gamma level CANNOT exist — the two "
                "cohorts then differ by distance-from-spot as much as by gamma. "
                "Compare within a depth band before reading the confluence "
                "effect.\n"
            )

    A("## Q9 — Is the effect stable across the confluence threshold?\n")
    A("| distance | n with | reversal (with) | n without | reversal (without) | gap |")
    A("|---:|---:|---:|---:|---:|---:|")
    for g in summary["distance_grid"]:
        gap = "—" if g["gap"] is None else f"{g['gap'] * 100:+.1f} pts"
        A(
            f"| ≤{g['distance']:g} pts | {g['n_with']} | {_pct(g['rate_with'])} | "
            f"{g['n_without']} | {_pct(g['rate_without'])} | {gap} |"
        )
    A("")
    A(
        "A real effect should not appear at one distance and vanish at the next. "
        "A gap that flips sign across adjacent buckets is noise being read as "
        "structure.\n"
    )

    A("## Q8 — Out of sample\n")
    A(
        "Sessions are cut chronologically; no shuffling. EVERY cohort is shown, "
        "because the point of the split is to catch the cohort that looked best "
        "in discovery and does not repeat — and that cohort cannot be named in "
        "advance.\n"
    )
    oos = summary["out_of_sample"]
    header = "| cohort |"
    rule = "|---|"
    for name in ("discovery", "validation", "test"):
        header += f" {name} ({oos.get(name, {}).get('sessions', 0)}d) |"
        rule += "---:|"
    A(header)
    A(rule)
    for c in summary["cohorts"]:
        row = f"| {c['label']} |"
        for name in ("discovery", "validation", "test"):
            st = (oos.get(name, {}).get("cohorts", {}) or {}).get(c["key"]) or {}
            nres = st.get("n_resolved", 0)
            ex = st.get("excess_vs_column")
            ex_txt = "" if ex is None or c["key"] == "all" else f", {ex * 100:+.1f}"
            row += f" {_pct(st.get('reversal_rate'))} (n={nres}{ex_txt}) |" if nres else " — |"
        A(row)
    A("")
    A(
        "A cohort whose rate moves by more than its in-sample interval across "
        "these columns was fitted to the discovery period, whatever its p-value "
        "there. Note the BASELINE itself drifts across the columns, so read each "
        "cohort against the `All OR extension touches` row of the SAME column, "
        "never against the pooled figure above. The signed figure in each cell "
        "is exactly that: points above or below its own column's baseline.\n"
    )

    h2 = summary.get("h2") or {}
    if h2.get("whole_window"):
        A("## H2 — broken ladders: effect and power\n")
        A(
            "Cohort 11 against the baseline, then net of the mechanical null "
            "(how much lower the null itself runs for broken-ladder touches). "
            "Power is read off the cohort's own session-clustered interval; it "
            "is not a textbook binomial, which would assume independent touches.\n"
        )
        A(
            "| slice | n | cohort sessions | calendar sessions | vs baseline | null shift "
            "| net of null | clustered 95% CI (net) | p | MDE (80%) |"
        )
        A("|---|---:|---:|---:|---:|---:|---:|---|---:|---:|")
        for label, blk in (
            ("whole window", h2.get("whole_window")),
            (f"from {h2.get('fresh_from')}", h2.get("fresh")),
        ):
            if not blk:
                continue

            def f(v: Optional[float]) -> str:
                return "—" if v is None else f"{v * 100:+.1f}"

            band = (
                f"[{blk['adjusted_ci_low'] * 100:+.1f}, {blk['adjusted_ci_high'] * 100:+.1f}]"
                if blk.get("adjusted_ci_low") is not None
                and blk.get("adjusted_ci_high") is not None
                else "—"
            )
            pw = blk.get("power") or {}
            pval = "—" if blk.get("clustered_p") is None else f"{blk['clustered_p']:.3f}"
            mde = pw.get("mde")
            A(
                f"| {label} | {blk.get('n')} | {blk.get('cohort_sessions')} | "
                f"{blk.get('n_calendar_sessions')} | {f(blk.get('clustered_diff'))} | "
                f"{f(blk.get('null_shift'))} | {f(blk.get('adjusted_diff'))} | {band} | "
                f"{pval} | {'—' if mde is None else f'{mde * 100:.1f}'} |"
            )
        A("")
        A("| slice | true effect | power now | sessions for 80% | more sessions | ≈ months |")
        A("|---|---:|---:|---:|---:|---:|")
        for label, blk in (
            ("whole window", h2.get("whole_window")),
            (f"from {h2.get('fresh_from')}", h2.get("fresh")),
        ):
            for sc in ((blk or {}).get("power") or {}).get("scenarios", []):
                name = {"observed": "as measured (net)", "september": "September's -6.0"}.get(
                    sc["name"], sc["name"]
                )
                pw = sc.get("power_now")
                months = sc.get("more_months")
                A(
                    f"| {label} | {name}: {sc['effect'] * 100:+.1f} | "
                    f"{'—' if pw is None else f'{pw:.0%}'} | {sc.get('sessions_needed') or '—'} | "
                    f"{sc.get('more_sessions') if sc.get('more_sessions') is not None else '—'} | "
                    f"{'—' if months is None else f'{months:.0f}'} |"
                )
        A("")
        A(
            "Sessions scale the cohort's interval as 1/sqrt(sessions), which "
            "assumes broken-ladder touches keep arriving at today's rate. The "
            "fresh slice is the only clean test: the hypothesis was named after "
            "looking at the September window.\n"
        )

    hurdle = summary.get("cost_hurdle") or {}
    if hurdle.get("available"):
        A("## Q10 — The NQ cost hurdle at this range length\n")
        A(
            f"Median NQ opening range **{hurdle['median_or_width_pts']:.1f} pts** over "
            f"{hurdle['sessions']} sessions, so one step is {hurdle['step_pts']:.1f} pts. "
            f"A round turn costs {hurdle['cost_pts']:.3f} pts "
            f"(${hurdle['cost_dollars']:.2f}: commission plus one tick of slippage "
            f"per side). Break-even needs the reversal rate to beat its mechanical "
            f"null by **{hurdle['hurdle'] * 100:.2f} pts**.\n"
        )

    A("## Pooling check — do the two option books agree?\n")
    pc = summary.get("pooling_check") or {}
    A(
        "Pooling is what makes this study powered at all: on the measured sample "
        "a single symbol resolves roughly a 12-18 point effect, all six together "
        "about 5. But SPY/SPX/ES are three price axes over ONE S&P chain and "
        "QQQ/NDX/NQ over ONE Nasdaq chain, so there are two samples here, not "
        "six — and `wall_break_odds` found those two books to be different "
        "processes for wall breaks.\n"
    )
    A("| book | sessions | n | effect vs baseline | clustered 95% CI |")
    A("|---|---:|---:|---:|---|")
    for book, v in (pc.get("per_book") or {}).items():
        band = (
            f"[{v['clustered_ci_low'] * 100:+.1f}, {v['clustered_ci_high'] * 100:+.1f}]"
            if v.get("clustered_ci_low") is not None
            else "—"
        )
        eff = "—" if v.get("clustered_diff") is None else f"{v['clustered_diff'] * 100:+.1f} pts"
        A(f"| {book} | {v.get('n_sessions', 0)} | {v.get('n', 0)} | {eff} | {band} |")
    A("")
    verdict = pc.get("verdict")
    A(
        {
            "consistent": "**Intervals overlap — the pooled figure above is admissible.** "
            "Overlap at this sample size is weak evidence of agreement, "
            "not proof of it.",
            "books_disagree": "**The books DISAGREE — do not quote the pooled figure.** "
            "An average of two different processes describes neither. "
            "Report per book.",
            "single_book": "Only one book is present, so there is nothing to pool and "
            "nothing to check. The result does not carry to the other index.",
            "undetermined": "Not enough resolved events in one or both books to compare. "
            "Treat the pooled figure as unverified.",
        }.get(verdict, f"Verdict: {verdict}")
    )
    A("")

    A("## Per symbol\n")
    A("| symbol | n | sessions | reversal | censored |")
    A("|---|---:|---:|---:|---:|")
    for sym, s in summary["by_symbol"].items():
        thin = " ⚠︎thin" if s.get("thin") else ""
        A(
            f"| {sym}{thin} | {s['n_resolved']} | {s['n_sessions']} | "
            f"{_pct(s.get('reversal_rate'))} | {_pct(s.get('censored_rate'))} |"
        )
    A("")
    A(
        "Per-symbol rows are the replication test. An effect present in one symbol "
        "and absent in the others is a property of that sample, not of gamma — "
        "which is how every candidate feature in `research/wall_break_odds` died.\n"
    )

    A("## Questions this run does and does not answer\n")
    for i, q in enumerate(_QUESTIONS, start=1):
        A(f"{i}. {q}")
    A("")
    A(
        "Q10 (economic significance after costs) is answered by the trade "
        "simulation, not by this report. A reversal rate above 50% is not an edge "
        "until it is priced with tick size, commission and slippage.\n"
    )
    return "\n".join(out)


def write_csv(rows: Sequence[Mapping[str, Any]], path: str | Path) -> int:
    """Flat CSV of the event rows, for spreadsheet work.

    Nested columns (the stored nearby-level list) are JSON-encoded rather than
    exploded, so the CSV stays one row per event and the audit trail survives.
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out.write_text("", encoding="utf-8")
        return 0
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    k: (json.dumps(v, default=str) if isinstance(v, (list, dict)) else v)
                    for k, v in row.items()
                }
            )
    return len(rows)
