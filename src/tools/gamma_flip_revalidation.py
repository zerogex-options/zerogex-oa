"""Re-validate the relative gamma-flip thresholds after the flip redefinition.

Commit 7106711 changed ``_calculate_gamma_flip_point`` from the per-strike
adjacent net-GEX sign change to the cumulative zero-gamma level
(SpotGamma/SqueezeMetrics convention).  Every flip consumer in the
codebase reads the flip *relatively* (``flip_distance`` = signed
``(price - flip) / price``), so no absolute threshold is miscalibrated —
but the redefinition moves where the flip sits, which shifts the
empirical distribution of that relative distance, and two firing-rate-
sensitive gates ride on it:

  * ``gamma_anchor.flip_distance_subscore >= _FLIP_DISTANCE_MIN`` (0.6)
    — the near-flip gate shared by the ``gamma_flip_bounce`` /
    ``gamma_flip_break`` playbook patterns.  That subscore *is* the
    ``FlipDistanceComponent`` score: ``clamp(1 - |fd| / sat)`` with a
    vol-adaptive ``sat`` bounded by ``_FLIP_MIN_PCT`` (0.5%) /
    ``_FLIP_MAX_PCT`` (5%), fallback ``_FLIP_FALLBACK_PCT`` (2%).
  * ``portfolio_engine``'s ``abs(flip_distance) >= 0.006`` "flip not
    near" band (``src/signals/portfolio_engine.py``).

``gex_summary.flip_distance`` already persists the exact relative
quantity per (underlying, timestamp): rows written before the
2026-05-15 deploy carry the OLD per-strike definition, rows after carry
the NEW cumulative one.  So a genuine before/after needs no
recomputation — split the persisted column at the deploy boundary and
compare the |flip_distance| distribution and the resulting gate
firing-rates.  The deploy boundary is the SAME cutoff the normalizer
refresh uses (``NORMALIZER_DEPLOY_CUTOFF``), so the two follow-ups stay
consistent.

This tool is READ-ONLY (SELECT only — safe to run any time, including
mid-session) and changes NOTHING.  It produces the evidence; any
threshold change is a separate, human-confirmed decision.

Complementary: per-pattern *hit-rates* for the flip patterns come from
the existing harness — ``python -m src.signals.playbook.backtest
--no-write --days <N>`` (look at gamma_flip_bounce / gamma_flip_break).

Usage:
    python -m src.tools.gamma_flip_revalidation
    python -m src.tools.gamma_flip_revalidation --underlying SPY --window-days 30
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

import numpy as np

from src.database.connection import db_connection

# Single source of truth for the deploy boundary (same env var / parser
# the normalizer refresh uses, so "pre vs post deploy" means the same
# instant in both follow-ups).
from src.tools.normalizer_cache_refresh import (
    DEPLOY_CUTOFF_ENV,
    _resolve_deploy_cutoff,
)

# Track the LIVE thresholds rather than hardcoding: if an operator
# overrides them via env, this analysis reflects the same values the
# signal path uses.
from src.signals.components.flip_distance import (
    _FLIP_FALLBACK_PCT,
    _FLIP_MAX_PCT,
    _FLIP_MIN_PCT,
)
from src.signals.playbook.patterns.gamma_flip_bounce import (
    _FLIP_DISTANCE_MIN as _FLIP_MIN_BOUNCE,
)
from src.signals.playbook.patterns.gamma_flip_break import (
    _FLIP_DISTANCE_MIN as _FLIP_MIN_BREAK,
)

logger = logging.getLogger(__name__)

# Below this an era's distribution is too thin to compare (mirrors the
# normalizer tool's reliability bar).
MIN_ERA_SAMPLES = 30

# portfolio_engine.py "flip not near" band (abs(flip_distance) >= 0.006).
# Not a module constant over there (inline literal), so it is mirrored
# here and kept env-overridable for the analysis.
DEFAULT_FLIP_NOT_NEAR_PCT = float(os.getenv("GAMMA_FLIP_NOT_NEAR_PCT", "0.006"))

# Optional later boundary: the flip calc kept changing after the deploy
# (7106711 -> 1731efc spot-shift -> 62c70df DTE-weighted ...), so the raw
# post-cutoff window can mix flip definitions.  Set this to when the flip
# calc last STABILIZED in production (e.g. the 62c70df prod-deploy
# instant) to restrict the POST era to a single, settled definition; the
# rows between the deploy cutoff and this instant are reported as an
# excluded "transitional (mixed flip defs)" bucket.  Blank = single split
# at the deploy cutoff (original behavior).
STABLE_SINCE_ENV = "GAMMA_FLIP_STABLE_SINCE"

# Verdict heuristics — thresholds for FLAGGING a human review, never for
# auto-applying a change.  A gate firing-rate moving by more than this
# (in absolute percentage points), or the median |flip_distance| more
# than doubling/halving, is "material".
_MATERIAL_FIRE_DELTA = 0.10
_MATERIAL_P50_RATIO = 2.0


def _subscore(fd: float, sat: float) -> float:
    """The ``FlipDistanceComponent`` core: ``clamp(1 - |fd| / sat)``.

    Pinned to the live component by test (fallback-saturation path), so
    if the component's formula ever changes the test fails loudly.
    """
    if sat <= 0:
        return 0.0
    return max(-1.0, min(1.0, 1.0 - (abs(float(fd)) / sat)))


@dataclass(frozen=True)
class EraStats:
    era: str
    n: int
    p05: float
    p25: float
    p50: float
    p75: float
    p95: float
    pct_not_near: float  # share with |flip_distance| >= not_near band
    # Share with subscore >= flip-min gate, at the low / fallback / high
    # saturation — brackets the vol-adaptive firing rate.
    fire_at_min_sat: float
    fire_at_fallback_sat: float
    fire_at_max_sat: float


def _summarize_era(
    era: str,
    abs_fd: np.ndarray,
    *,
    flip_min: float,
    not_near_pct: float,
) -> Optional[EraStats]:
    if abs_fd.size < MIN_ERA_SAMPLES:
        return None

    def _fire(sat: float) -> float:
        scores = np.clip(1.0 - abs_fd / sat, -1.0, 1.0) if sat > 0 else np.full_like(abs_fd, 0.0)
        return float(np.mean(scores >= flip_min))

    return EraStats(
        era=era,
        n=int(abs_fd.size),
        p05=float(np.percentile(abs_fd, 5)),
        p25=float(np.percentile(abs_fd, 25)),
        p50=float(np.percentile(abs_fd, 50)),
        p75=float(np.percentile(abs_fd, 75)),
        p95=float(np.percentile(abs_fd, 95)),
        pct_not_near=float(np.mean(abs_fd >= not_near_pct)),
        fire_at_min_sat=_fire(_FLIP_MIN_PCT),
        fire_at_fallback_sat=_fire(_FLIP_FALLBACK_PCT),
        fire_at_max_sat=_fire(_FLIP_MAX_PCT),
    )


def _fetch_flip_distances(cur, underlying: str, window_days: int) -> list[tuple[datetime, float]]:
    """READ-ONLY: persisted relative flip distance per timestamp."""
    cur.execute(
        """
        SELECT timestamp, flip_distance
        FROM gex_summary
        WHERE underlying = %s
          AND timestamp >= NOW() - (%s || ' days')::interval
          AND flip_distance IS NOT NULL
        ORDER BY timestamp
        """,
        (underlying, str(window_days)),
    )
    out: list[tuple[datetime, float]] = []
    for ts, fd in cur.fetchall():
        if fd is None:
            continue
        try:
            out.append((ts, float(fd)))
        except (TypeError, ValueError):
            continue
    return out


def _parse_optional_instant(raw: Optional[str]) -> Optional[datetime]:
    """Parse an optional ISO date/datetime (ET when offset-less), or None
    when blank.  Reuses the deploy-cutoff parser (same ET/fail-closed
    rules) — a non-blank value never falls through to its default."""
    if raw is None or not raw.strip():
        return None
    return _resolve_deploy_cutoff(raw)


def analyze(
    conn,
    underlying: str,
    *,
    window_days: int,
    deploy_cutoff: datetime,
    flip_min: float,
    not_near_pct: float,
    stable_since: Optional[datetime] = None,
) -> tuple[Optional[EraStats], Optional[EraStats], int, int]:
    """Partition persisted flip_distance and summarize each era.

    ``pre`` is always rows before the deploy cutoff (old per-strike
    flip).  When ``stable_since`` is set the POST era is restricted to
    rows at/after it (a single settled flip definition) and rows in
    ``[deploy_cutoff, stable_since)`` are an excluded transitional
    bucket; otherwise POST is everything at/after the deploy cutoff.
    Returns ``(pre, post, total_rows, transitional_n)``."""
    with conn.cursor() as cur:
        rows = _fetch_flip_distances(cur, underlying, window_days)

    post_floor = stable_since if stable_since is not None else deploy_cutoff
    pre = np.array([abs(fd) for ts, fd in rows if ts < deploy_cutoff], dtype=float)
    post = np.array([abs(fd) for ts, fd in rows if ts >= post_floor], dtype=float)
    transitional_n = (
        sum(1 for ts, _ in rows if deploy_cutoff <= ts < stable_since)
        if stable_since is not None
        else 0
    )
    post_label = (
        "post-stable (settled flip def, >= stable_since)"
        if stable_since is not None
        else "post-deploy (cumulative zero-gamma)"
    )
    pre_stats = _summarize_era(
        "pre-deploy (old per-strike flip)",
        pre,
        flip_min=flip_min,
        not_near_pct=not_near_pct,
    )
    post_stats = _summarize_era(
        post_label,
        post,
        flip_min=flip_min,
        not_near_pct=not_near_pct,
    )
    return pre_stats, post_stats, len(rows), transitional_n


def _verdict(pre: Optional[EraStats], post: Optional[EraStats]) -> str:
    if post is None:
        return (
            "INSUFFICIENT POST-DEPLOY DATA — re-run once more sessions have "
            f"accrued (need >= {MIN_ERA_SAMPLES} post-cutoff rows)."
        )
    if pre is None:
        return (
            "NO PRE-DEPLOY BASELINE in window — cannot diff. Judge the "
            "post-deploy absolute distribution against the gates directly; "
            "widen --window-days to capture pre-deploy rows for a true diff."
        )
    fire_delta = abs(post.fire_at_fallback_sat - pre.fire_at_fallback_sat)
    p50_ratio = post.p50 / pre.p50 if pre.p50 > 0 else (float("inf") if post.p50 > 0 else 1.0)
    material = fire_delta >= _MATERIAL_FIRE_DELTA or not (
        1.0 / _MATERIAL_P50_RATIO <= p50_ratio <= _MATERIAL_P50_RATIO
    )
    if material:
        return (
            "MATERIAL SHIFT — the flip redefinition moved the relative-"
            "distance distribution enough that the near-flip gate "
            f"(_FLIP_DISTANCE_MIN) fires {pre.fire_at_fallback_sat:.0%} -> "
            f"{post.fire_at_fallback_sat:.0%} and median |flip_distance| "
            f"moved x{p50_ratio:.2f}. RECOMMEND reviewing _FLIP_DISTANCE_MIN "
            "/ the flip-distance saturation band / the 0.006 portfolio band "
            "with the owner before any change. Do NOT auto-adjust."
        )
    return (
        "NO MATERIAL SHIFT — relative gates remain well-calibrated under "
        f"the new flip definition (gate firing {pre.fire_at_fallback_sat:.0%} "
        f"-> {post.fire_at_fallback_sat:.0%}, median |flip_distance| "
        f"x{p50_ratio:.2f}). No threshold change recommended."
    )


def _fmt_era(s: Optional[EraStats]) -> str:
    if s is None:
        return "  (insufficient samples)"
    return (
        f"  n={s.n}\n"
        f"  |flip_distance| pctiles: p05={s.p05:.4f} p25={s.p25:.4f} "
        f"p50={s.p50:.4f} p75={s.p75:.4f} p95={s.p95:.4f}\n"
        f"  share |fd| >= not-near band: {s.pct_not_near:.1%}\n"
        f"  near-flip gate fires (subscore >= flip-min): "
        f"minσ={s.fire_at_min_sat:.1%}  fallbackσ={s.fire_at_fallback_sat:.1%}"
        f"  maxσ={s.fire_at_max_sat:.1%}"
    )


def _format_report(
    *,
    underlying: str,
    window_days: int,
    deploy_cutoff: datetime,
    flip_min: float,
    not_near_pct: float,
    pre: Optional[EraStats],
    post: Optional[EraStats],
    total_rows: int,
    stable_since: Optional[datetime] = None,
    transitional_n: int = 0,
) -> str:
    if stable_since is not None:
        post_header = f"POST-stable (settled flip def, >= {stable_since.isoformat()}):"
        scope_line = (
            f"post era restricted to >= stable_since "
            f"({STABLE_SINCE_ENV}): {stable_since.isoformat()}  "
            f"[excluded {transitional_n} transitional rows in "
            f"[{deploy_cutoff.isoformat()}, stable_since) — mixed flip defs]"
        )
    else:
        post_header = "POST-deploy (cumulative zero-gamma flip):"
        scope_line = (
            "post era = all rows >= deploy cutoff (may mix flip defs if the "
            f"calc changed again; set {STABLE_SINCE_ENV} for a clean read)"
        )
    return "\n".join(
        [
            "=== Gamma-flip threshold re-validation ===",
            f"underlying={underlying}  window_days={window_days}  " f"rows={total_rows}",
            f"deploy boundary ({DEPLOY_CUTOFF_ENV}): {deploy_cutoff.isoformat()}",
            scope_line,
            f"near-flip gate _FLIP_DISTANCE_MIN: bounce={_FLIP_MIN_BOUNCE:.2f} "
            f"break={_FLIP_MIN_BREAK:.2f} (analysis uses {flip_min:.2f})",
            f"saturation band: min={_FLIP_MIN_PCT} fallback={_FLIP_FALLBACK_PCT} "
            f"max={_FLIP_MAX_PCT};  not-near band={not_near_pct}",
            "",
            "PRE-deploy (old per-strike flip):",
            _fmt_era(pre),
            "",
            post_header,
            _fmt_era(post),
            "",
            "VERDICT: " + _verdict(pre, post),
        ]
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Re-validate relative gamma-flip thresholds (read-only)."
    )
    parser.add_argument("--underlying", default=os.getenv("GAMMA_FLIP_SYMBOL", "SPY"))
    parser.add_argument(
        "--window-days",
        type=int,
        default=int(os.getenv("GAMMA_FLIP_WINDOW_DAYS", "30")),
        help="Look-back spanning pre- and post-deploy rows (default 30).",
    )
    parser.add_argument(
        "--flip-not-near-pct",
        type=float,
        default=DEFAULT_FLIP_NOT_NEAR_PCT,
        help="portfolio_engine 'flip not near' band (default 0.006).",
    )
    parser.add_argument(
        "--stable-since",
        default=os.getenv(STABLE_SINCE_ENV),
        help=(
            "Optional ISO date/datetime (ET if offset-less): restrict the "
            "POST era to a single settled flip definition (e.g. the "
            "62c70df prod-deploy instant). Blank = single split at the "
            "deploy cutoff."
        ),
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if args.window_days <= 0:
        parser.error("--window-days must be positive")

    deploy_cutoff = _resolve_deploy_cutoff(os.getenv(DEPLOY_CUTOFF_ENV))
    try:
        stable_since = _parse_optional_instant(args.stable_since)
    except ValueError as exc:
        parser.error(str(exc))
    if stable_since is not None and stable_since <= deploy_cutoff:
        parser.error(
            f"--stable-since ({stable_since.isoformat()}) must be after the "
            f"deploy cutoff ({deploy_cutoff.isoformat()}); it marks when the "
            "flip calc settled, which is necessarily after the deploy."
        )
    flip_min = max(_FLIP_MIN_BOUNCE, _FLIP_MIN_BREAK)  # the stricter gate
    logger.info(
        "Re-validating %s flip thresholds (window=%dd, boundary=%s, "
        "stable_since=%s, flip_min=%.2f) — READ-ONLY",
        args.underlying,
        args.window_days,
        deploy_cutoff.isoformat(),
        stable_since.isoformat() if stable_since else "none",
        flip_min,
    )

    with db_connection() as conn:
        pre, post, total, transitional_n = analyze(
            conn,
            args.underlying.upper(),
            window_days=args.window_days,
            deploy_cutoff=deploy_cutoff,
            flip_min=flip_min,
            not_near_pct=args.flip_not_near_pct,
            stable_since=stable_since,
        )
        # Defensive: this tool never writes, but make the read-only
        # contract explicit even if a future edit adds a statement.
        conn.rollback()

    print(
        _format_report(
            underlying=args.underlying.upper(),
            window_days=args.window_days,
            deploy_cutoff=deploy_cutoff,
            flip_min=flip_min,
            not_near_pct=args.flip_not_near_pct,
            pre=pre,
            post=post,
            total_rows=total,
            stable_since=stable_since,
            transitional_n=transitional_n,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
