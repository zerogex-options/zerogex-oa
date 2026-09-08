"""The feature vector, measured strictly at the moment of the touch.

Every value here is computed from bars and frames timestamped **at or before**
``TouchEvent.touched_at``.  A feature that peeks one minute past the touch
produces a model that looks brilliant in backtest and is worthless live, and
the failure is invisible in every summary statistic.  The selftest pins this by
poisoning all post-touch data and asserting the vector does not move.

Two features need more care than the rest, because the obvious implementation
of each is a look-ahead:

**``consecutive_extensions_broken``.**  The tempting definition is "how many
inner rungs had a *continuation* outcome" — but a rung touched at 10:00 may not
resolve until 11:00, so reading its outcome at a 10:30 touch imports
information from the future.  It is therefore derived from the PRICE PATH
instead: an inner rung counts as respected only if price had already retraced
``respect_retrace_frac`` of a step back toward the anchor **before this touch**.
That is answerable from bars the touch had already seen, and it means the same
thing.

**Session high / low / realised range "so far".**  Computed on ``[session
open, touch bar]`` inclusive — the touch bar is part of what price had already
done, unlike the outcome window, which starts at the next bar.

VWAP follows production: cumulative ``sum(close * volume) / sum(volume)`` over
the session (``src/signals/unified_signal_engine.py:300-330``), with a cash
index borrowing its ETF's volume profile via
:func:`src.symbols.resolve_volume_proxy` — SPX→SPY, NDX→QQQ.  A symbol with no
usable volume reports VWAP as unavailable rather than fabricating one.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from research.msi_regime_excursion.excursion import ET, Bar, BarSeries
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.events import TouchEvent
from research.or_gamma_confluence.levels import Confluence, GammaSnapshot
from research.or_gamma_confluence.ranges import SIDE_UP, ExtensionLadder, OpeningRange

__all__ = [
    "TREND_FILTERS",
    "SessionContext",
    "wma",
    "hma_series",
    "ema_series",
    "atr_series",
    "build_features",
]

#: Selectable trend reads.  All are recorded on every event; ``trend_filter``
#: only chooses which one a cohort splits on, because whether a trend filter
#: helps is a research question rather than a setting.
TREND_FILTERS: tuple[str, ...] = ("none", "ema_slope", "hma", "vwap_slope", "trade_bias")

_EPS = 1e-12


def wma(values: Sequence[float], period: int) -> list[Optional[float]]:
    """Linearly weighted moving average, newest bar weighted heaviest."""
    n = len(values)
    out: list[Optional[float]] = [None] * n
    if period <= 0:
        return out
    denom = period * (period + 1) / 2.0
    for i in range(period - 1, n):
        acc = 0.0
        for j in range(period):
            acc += values[i - period + 1 + j] * (j + 1)
        out[i] = acc / denom
    return out


def hma_series(values: Sequence[float], period: int) -> list[Optional[float]]:
    """Hull moving average — ``WMA(2*WMA(n/2) - WMA(n), sqrt(n))``.

    The trader's chart shows a fast-reacting yellow trend line; HMA is the
    common implementation of that shape.  Included as a CANDIDATE filter, not
    as a claim about what he actually uses — his implementation is
    proprietary and the study must not depend on it.
    """
    n = len(values)
    if period <= 1 or n == 0:
        return [None] * n
    half = max(1, period // 2)
    root = max(1, int(math.sqrt(period)))
    w_half = wma(values, half)
    w_full = wma(values, period)
    raw: list[float] = []
    valid_from = n
    for i in range(n):
        if w_half[i] is None or w_full[i] is None:
            raw.append(0.0)
        else:
            if i < valid_from:
                valid_from = i
            raw.append(2.0 * w_half[i] - w_full[i])
    smoothed = wma(raw, root)
    return [
        v if (v is not None and i >= valid_from + root - 1) else None
        for i, v in enumerate(smoothed)
    ]


def ema_series(values: Sequence[float], period: int) -> list[Optional[float]]:
    """Exponential moving average, seeded on the first ``period`` bars."""
    n = len(values)
    out: list[Optional[float]] = [None] * n
    if period <= 0 or n < period:
        return out
    alpha = 2.0 / (period + 1.0)
    prev = sum(values[:period]) / period
    out[period - 1] = prev
    for i in range(period, n):
        prev = alpha * values[i] + (1.0 - alpha) * prev
        out[i] = prev
    return out


def atr_series(bars: Sequence[Bar], period: int) -> list[Optional[float]]:
    """Average true range, simple mean of the trailing ``period`` true ranges.

    Backward-looking by construction: the value at bar ``i`` uses bars
    ``i-period+1..i`` and nothing after.
    """
    n = len(bars)
    out: list[Optional[float]] = [None] * n
    if period <= 0 or n == 0:
        return out
    trs: list[float] = []
    for i, bar in enumerate(bars):
        if i == 0:
            trs.append(bar.high - bar.low)
        else:
            pc = bars[i - 1].close
            trs.append(max(bar.high - bar.low, abs(bar.high - pc), abs(pc - bar.low)))
        if i >= period - 1:
            out[i] = sum(trs[i - period + 1 : i + 1]) / period
    return out


@dataclass
class SessionContext:
    """Per-session series, every one of them backward-safe at each index.

    Built once per session and indexed by timestamp, so a feature lookup is a
    binary search rather than a rescan.  Everything stored as a per-bar list
    aligned to ``bars``: the value at index ``i`` uses bars ``0..i`` only.
    """

    symbol: str
    bars: list[Bar]
    series: BarSeries
    #: Total volume per bar (``up_volume + down_volume``), proxied for a cash
    #: index.  Empty when the instrument has no usable volume.
    volumes: list[float] = field(default_factory=list)
    volume_proxy: Optional[str] = None

    stamps: list[datetime] = field(default_factory=list)
    vwap: list[Optional[float]] = field(default_factory=list)
    run_high: list[float] = field(default_factory=list)
    run_low: list[float] = field(default_factory=list)
    atr: list[Optional[float]] = field(default_factory=list)
    ema: list[Optional[float]] = field(default_factory=list)
    hma: list[Optional[float]] = field(default_factory=list)
    #: ``trade_bias_scores`` rows, ascending — read as a step function.
    bias_rows: list[Mapping[str, Any]] = field(default_factory=list)
    _bias_stamps: list[datetime] = field(default_factory=list)

    @classmethod
    def build(
        cls,
        symbol: str,
        bars: Sequence[Bar],
        cfg: ResearchConfig,
        *,
        volumes: Optional[Mapping[datetime, float]] = None,
        volume_proxy: Optional[str] = None,
        bias_rows: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> "SessionContext":
        ordered = sorted(bars, key=lambda b: b.ts)
        closes = [b.close for b in ordered]
        vols = [float((volumes or {}).get(b.ts, 0.0)) for b in ordered]

        vwap: list[Optional[float]] = []
        cum_pv = cum_v = 0.0
        for bar, vol in zip(ordered, vols):
            cum_pv += bar.close * vol
            cum_v += vol
            vwap.append(cum_pv / cum_v if cum_v > _EPS else None)

        run_high: list[float] = []
        run_low: list[float] = []
        hi = lo = None
        for bar in ordered:
            hi = bar.high if hi is None else max(hi, bar.high)
            lo = bar.low if lo is None else min(lo, bar.low)
            run_high.append(hi)
            run_low.append(lo)

        rows = list(bias_rows or [])
        return cls(
            symbol=symbol,
            bars=ordered,
            series=BarSeries(ordered),
            volumes=vols,
            volume_proxy=volume_proxy,
            stamps=[b.ts for b in ordered],
            vwap=vwap,
            run_high=run_high,
            run_low=run_low,
            atr=atr_series(ordered, cfg.atr_period),
            ema=ema_series(closes, cfg.ema_period),
            hma=hma_series(closes, cfg.hma_period),
            bias_rows=rows,
            _bias_stamps=[r["timestamp"] for r in rows],
        )

    def index_at(self, ts: datetime) -> Optional[int]:
        i = bisect.bisect_right(self.stamps, ts) - 1
        return i if i >= 0 else None

    def bias_at(self, ts: datetime) -> Optional[Mapping[str, Any]]:
        """The ``trade_bias_scores`` row in force at ``ts`` — never a later one."""
        i = bisect.bisect_right(self._bias_stamps, ts) - 1
        return self.bias_rows[i] if i >= 0 else None


def _slope_direction(
    values: Sequence[Optional[float]], idx: int, lookback: int, flat_bp: float
) -> Optional[str]:
    """``up`` / ``down`` / ``flat`` from a series' change over ``lookback`` bars."""
    j = idx - lookback
    if idx < 0 or j < 0 or idx >= len(values):
        return None
    now, then = values[idx], values[j]
    if now is None or then is None or then <= 0:
        return None
    move_bp = 10_000.0 * (now - then) / then
    if abs(move_bp) < flat_bp:
        return "flat"
    return "up" if move_bp > 0 else "down"


def _trend_reads(
    ctx: SessionContext, idx: int, ts: datetime, cfg: ResearchConfig
) -> dict[str, Any]:
    """Every candidate trend read at one bar.  All are recorded; none is truth."""
    lb = max(1, cfg.trend_lookback_minutes)
    closes = [b.close for b in ctx.bars]
    out: dict[str, Any] = {
        "trend_ema_slope": _slope_direction(ctx.ema, idx, lb, cfg.trend_flat_bp),
        "trend_hma": _slope_direction(ctx.hma, idx, lb, cfg.trend_flat_bp),
        "trend_vwap_slope": _slope_direction(ctx.vwap, idx, lb, cfg.trend_flat_bp),
        "trend_price_slope": _slope_direction(closes, idx, lb, cfg.trend_flat_bp),
    }
    bias = ctx.bias_at(ts)
    if bias is not None:
        direction = str(bias.get("direction") or "").lower()
        out["trend_trade_bias"] = {"long": "up", "short": "down"}.get(direction, "flat")
        out["trade_bias_code"] = bias.get("bias_code")
        out["trade_bias_state"] = bias.get("market_state")
        out["trade_bias_confidence"] = (
            float(bias["confidence"]) if bias.get("confidence") is not None else None
        )
    else:
        out["trend_trade_bias"] = None
        out["trade_bias_code"] = None
        out["trade_bias_state"] = None
        out["trade_bias_confidence"] = None

    selected = {
        "none": None,
        "ema_slope": out["trend_ema_slope"],
        "hma": out["trend_hma"],
        "vwap_slope": out["trend_vwap_slope"],
        "trade_bias": out["trend_trade_bias"],
    }.get(cfg.trend_filter)
    out["trend_selected"] = selected
    out["trend_filter"] = cfg.trend_filter
    return out


def _prior_rung_respect(
    event: TouchEvent,
    prior: Sequence[TouchEvent],
    ctx: SessionContext,
    orange: OpeningRange,
    cfg: ResearchConfig,
) -> dict[str, Any]:
    """Backward-looking read of how the inner rungs behaved before this touch.

    "Respected" means price retraced ``respect_retrace_frac`` of one step back
    toward the anchor AFTER touching that rung and BEFORE the current touch.
    Deliberately not "that rung's outcome was a reversal" — an outcome can
    resolve after the current touch, and using it would be a look-ahead.
    """
    step_pts = cfg.extension_step * orange.width
    retrace = cfg.respect_retrace_frac * step_pts
    same_side = [
        e
        for e in prior
        if e.side == event.side
        and abs(e.rung_k) < abs(event.rung_k)
        and e.touched_at <= event.touched_at
    ]
    same_side.sort(key=lambda e: abs(e.rung_k))

    hi_idx = ctx.index_at(event.touched_at)
    respected: dict[int, bool] = {}
    for e in same_side:
        start = ctx.index_at(e.touched_at)
        if start is None or hi_idx is None or start >= hi_idx:
            respected[e.rung_index] = False
            continue
        # Strictly AFTER that rung's own touch bar. Including it would read the
        # bar's far extreme — which is where price came FROM on its way to the
        # level — as a retrace away from it, and every rung reached by a large
        # bar would score as respected.
        window = ctx.bars[start + 1 : hi_idx + 1]
        if event.side == SIDE_UP:
            got = any((e.level_price - b.low) >= retrace for b in window)
        else:
            got = any((b.high - e.level_price) >= retrace for b in window)
        respected[e.rung_index] = bool(got)

    # Walk INWARD from the current rung: how many consecutive rungs directly
    # beneath this one went unrespected?
    consecutive = 0
    step_sign = 1 if event.rung_index > 0 else -1
    probe = event.rung_index - step_sign
    while probe in respected:
        if respected[probe]:
            break
        consecutive += 1
        probe -= step_sign

    n = len(respected)
    n_respected = sum(1 for v in respected.values() if v)
    return {
        "prior_extensions_touched": n,
        "prior_extensions_respected": n_respected,
        "prior_extension_respect_score": (n_respected / n) if n else None,
        "consecutive_extensions_broken": consecutive,
        "lowest_prior_rung_respected": (respected.get(event.rung_index - step_sign) if n else None),
    }


def build_features(
    event: TouchEvent,
    ctx: SessionContext,
    orange: OpeningRange,
    ladder: ExtensionLadder,
    cfg: ResearchConfig,
    *,
    snapshot: Optional[GammaSnapshot] = None,
    confluence: Optional[Confluence] = None,
    prior: Sequence[TouchEvent] = (),
) -> dict[str, Any]:
    """One event's feature vector.  Nothing here reads past ``touched_at``."""
    idx = ctx.index_at(event.touched_at)
    et = event.touched_at.astimezone(ET)
    r = orange.width

    out: dict[str, Any] = {
        # ── Opening range geometry ──
        **orange.to_dict(),
        "extension_number": abs(event.rung_index),
        "extension_k": event.rung_k,
        "extension_depth_r": abs(event.rung_k),
        "extension_price": event.level_price,
        "extension_mode": cfg.extension_mode,
        "direction_from_open": ("above" if event.level_price >= orange.open_price else "below"),
        "distance_from_open_pts": event.level_price - orange.open_price,
        "distance_from_open_r": ((event.level_price - orange.open_price) / r if r > _EPS else None),
        # ── Clock ──
        "time_of_day_et": et.strftime("%H:%M:%S"),
        "minutes_since_open": (
            et
            - et.replace(
                hour=cfg.session_start.hour,
                minute=cfg.session_start.minute,
                second=0,
                microsecond=0,
            )
        ).total_seconds()
        / 60.0,
        "minutes_since_or_complete": event.minutes_since_or,
        "minutes_to_close": (
            et.replace(
                hour=cfg.session_end.hour, minute=cfg.session_end.minute, second=0, microsecond=0
            )
            - et
        ).total_seconds()
        / 60.0,
    }

    # ── Path so far (inclusive of the touch bar) ──
    if idx is not None:
        session_high = ctx.run_high[idx]
        session_low = ctx.run_low[idx]
        out.update(
            {
                "session_high_so_far": session_high,
                "session_low_so_far": session_low,
                "realized_range_so_far": session_high - session_low,
                "realized_range_r": (session_high - session_low) / r if r > _EPS else None,
                "atr": ctx.atr[idx],
                "atr_r": (ctx.atr[idx] / r) if (ctx.atr[idx] and r > _EPS) else None,
            }
        )
        vwap = ctx.vwap[idx]
        out["vwap"] = vwap
        out["vwap_available"] = vwap is not None
        out["vwap_proxy_symbol"] = ctx.volume_proxy
        if vwap:
            out["distance_from_vwap_pts"] = event.level_price - vwap
            out["distance_from_vwap_bp"] = 10_000.0 * (event.level_price - vwap) / vwap
            out["price_vs_vwap"] = "above" if event.spot_at_touch >= vwap else "below"
        else:
            out["distance_from_vwap_pts"] = None
            out["distance_from_vwap_bp"] = None
            out["price_vs_vwap"] = None
        out.update(_trend_reads(ctx, idx, event.touched_at, cfg))
    else:  # pragma: no cover - a touch always has a bar
        out["vwap_available"] = False

    # ── Prior ladder behaviour ──
    out.update(_prior_rung_respect(event, prior, ctx, orange, cfg))

    # ── Gamma ──
    out["gamma_available"] = snapshot is not None
    if snapshot is not None:
        out.update(snapshot.to_audit())
        out["gamma_lead_seconds"] = (event.touched_at - snapshot.available_at).total_seconds()
        out["net_gex_total"] = snapshot.total_net_gex
        out["net_gex_at_spot"] = snapshot.net_gex_at_spot
        out["gamma_regime_sign"] = snapshot.regime_sign()
        out["local_gex"] = snapshot.local_gex
        out["convexity_risk"] = snapshot.convexity_risk
        out["call_wall_strength"] = snapshot.call_wall_strength
        out["put_wall_strength"] = snapshot.put_wall_strength
        out["pin_score"] = snapshot.pin_score
        out["pin_confidence"] = snapshot.pin_confidence
        flip = snapshot.gamma_flip()
        out["gamma_flip"] = flip
        out["flip_distance_frame"] = snapshot.flip_distance
        if flip:
            out["distance_to_flip_pts"] = event.level_price - flip
            out["distance_to_flip_r"] = (event.level_price - flip) / r if r > _EPS else None
            out["spot_above_flip"] = event.spot_at_touch >= flip
        else:
            out["distance_to_flip_pts"] = None
            out["distance_to_flip_r"] = None
            out["spot_above_flip"] = None
    else:
        for key in (
            "gamma_lead_seconds",
            "net_gex_total",
            "net_gex_at_spot",
            "gamma_regime_sign",
            "local_gex",
            "convexity_risk",
            "call_wall_strength",
            "put_wall_strength",
            "pin_score",
            "pin_confidence",
            "gamma_flip",
            "flip_distance_frame",
            "distance_to_flip_pts",
            "distance_to_flip_r",
            "spot_above_flip",
        ):
            out[key] = None

    # ── Confluence (distances raw; thresholds applied in cohorts) ──
    if confluence is not None:
        out.update(confluence.to_dict(cfg.confluence_buckets_pts))
        near = confluence.nearest
        if near is not None and r > _EPS:
            out["nearest_gamma_distance_r"] = abs(near[1]) / r
        else:
            out["nearest_gamma_distance_r"] = None
        if near is not None and event.level_price > 0:
            out["nearest_gamma_distance_bp"] = 10_000.0 * abs(near[1]) / event.level_price
        else:
            out["nearest_gamma_distance_bp"] = None
        atr = out.get("atr")
        out["nearest_gamma_distance_atr"] = (
            abs(near[1]) / atr if (near is not None and atr) else None
        )
    else:
        out["nearest_gamma_level_type"] = None
        out["nearest_gamma_distance"] = None
        out["nearest_gamma_distance_r"] = None
        out["nearest_gamma_distance_bp"] = None
        out["nearest_gamma_distance_atr"] = None
    return out
