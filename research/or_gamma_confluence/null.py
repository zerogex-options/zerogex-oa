"""The mechanical null, measured rather than assumed.

The September run found that the headline reversal rate was the event
definition measuring itself: a touch fires on the bar's extreme but the outcome
is scanned from its close, so price starts the prev-vs-next race displaced
toward prev.  Gambler's ruin puts a driftless walk at

    P(reversal) = 0.5 + d / (2 * step)

and :func:`gamblers_ruin_null` is that formula, per event.

**The formula assumes the race always finishes.**  It is the probability of
hitting prev before next with unlimited time.  The study only counts touches
that resolve before the bell, and conditioning on resolution tilts a driftless
walk toward the NEARER rung, which is prev.  A 60-minute range makes that the
live question: the range and its steps are wider, the day is an hour shorter,
and far more touches end unresolved.

Checked in simulation before trusting it either way: a driftless walk with an
opening-hour volatility bump, 28,800 sessions at a 60-minute range and 0.5R
steps, pushed through the identical event rule.  A sixth of touches ended
unresolved, and the resolved reversal rate still matched the formula to
+0.01 points (session-clustered 95% interval -0.39 to +0.46).  The tilt can
only scale the offset's own contribution, ``d / (2 * step)``, and at 60
minutes that is about one point to begin with.  So the formula is expected to
hold.

**It is also measured, because real bars are not a simulation.**  For every
real session, the bars AFTER the opening range are replayed with each bar's
direction flipped by a fair coin: same minute-by-minute size, same bar shapes,
same intraday volatility profile, no direction persistence in either sign.  The
opening range, and so the ladder, is the real one.  The identical event,
outcome and prior-rung code then runs on each replay.  What comes out is the
reversal rate this event rule produces on this session's own volatility with no
mean reversion and no momentum, with the bell, the censoring and the
close-offset all included.

Two things come from it that the formula cannot give.  It confirms or refutes
the formula on the real tape -- fat tails, clustered volatility, real bar
shapes -- at each range length.  And it gives every PRICE-only cohort a null of
its own: all touches, depth bands, named rungs, and the broken / respected
ladder cohorts.  A cohort selects touches by where and when they happen, and
the touches that reach -200% at 14:30 are not a random draw of all touches.
The surrogates carry no gamma, so a confluence cohort's null is the comparison
with the baseline itself, as before.
"""

from __future__ import annotations

import random
from typing import Any, Optional, Sequence

from research.msi_regime_excursion.excursion import Bar, BarSeries
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.events import extract_touch_events
from research.or_gamma_confluence.features import (
    SessionContext,
    prior_rung_respect,
    touch_offset_r,
)
from research.or_gamma_confluence.ranges import ExtensionLadder, OpeningRange

__all__ = [
    "gamblers_ruin_null",
    "sign_flip_bars",
    "surrogate_rows",
    "NULL_ROW_KEYS",
]

#: The fields every surrogate row carries.  Named exactly as on a real event
#: row, so the same cohort predicates select from both.
NULL_ROW_KEYS: tuple[str, ...] = (
    "symbol",
    "session",
    "replica",
    "rung_k",
    "extension_k",
    "side",
    "outcome",
    "next_exists",
    "touch_offset_r",
    "consecutive_extensions_broken",
    "prior_extensions_touched",
    "minutes_since_or",
    "or_width",
)


def gamblers_ruin_null(offset_r: Optional[float], step: float) -> Optional[float]:
    """``0.5 + d / (2 * step)``, clipped to ``[0, 1]``.

    ``offset_r`` is ``touch_offset_r``; ``step`` is ``extension_step``, both in
    units of R.  An offset beyond a full step means the close already sat past
    a neighboring rung, where the race is decided before it starts.
    """
    if offset_r is None or step <= 0:
        return None
    return min(1.0, max(0.0, 0.5 + float(offset_r) / (2.0 * step)))


def sign_flip_bars(bars: Sequence[Bar], start_index: int, rng: random.Random) -> list[Bar]:
    """``bars[:start_index]`` unchanged; every later bar's moves sign-flipped.

    Each bar from ``start_index`` on is rebuilt from the previous REBUILT
    close: its gap from the prior close, and its high, low and close relative
    to its open, are all multiplied by one fair coin.  A flipped bar is the
    mirror image of the original, so its range, body and wick lengths are
    unchanged and only its direction is random.  Magnitudes keep their real
    order, so the session's volatility clustering survives; direction
    persistence does not.
    """
    out = list(bars[:start_index])
    if start_index >= len(bars):
        return out
    real_prev = bars[start_index - 1].close if start_index > 0 else bars[0].open
    new_prev = real_prev
    for bar in bars[start_index:]:
        sign = 1.0 if rng.random() < 0.5 else -1.0
        o = new_prev + sign * (bar.open - real_prev)
        up, down = bar.high - bar.open, bar.low - bar.open
        if sign > 0:
            hi, lo = o + up, o + down
        else:
            hi, lo = o - down, o - up
        c = o + sign * (bar.close - bar.open)
        out.append(Bar(ts=bar.ts, open=o, high=hi, low=lo, close=c))
        real_prev, new_prev = bar.close, c
    return out


def _seed(cfg: ResearchConfig, symbol: str, session: Any, replica: int) -> str:
    # ``random.Random`` seeds a str through SHA-512, so this is stable across
    # processes, unlike ``hash()``.
    return f"{cfg.null_seed}|{symbol}|{session}|{replica}"


def surrogate_rows(
    symbol: str,
    bars: Sequence[Bar],
    orange: OpeningRange,
    ladder: ExtensionLadder,
    cfg: ResearchConfig,
    *,
    tick: Optional[float] = None,
    replicas: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Surrogate touch events for one session, ``cfg.null_replicas`` replays.

    ``bars`` are the session's bars AFTER the phantom repair, ascending, and
    ``orange`` / ``ladder`` are the real frozen range and ladder.  Only bars
    at or after ``orange.end`` are flipped, so the range a trader saw is the
    range every replay is measured against.
    """
    n = cfg.null_replicas if replicas is None else replicas
    if n <= 0 or not bars:
        return []
    ordered = sorted(bars, key=lambda b: b.ts)
    start = next((i for i, b in enumerate(ordered) if b.ts >= orange.end), len(ordered))
    if start >= len(ordered):
        return []
    session = orange.session.isoformat()
    r = orange.width
    rows: list[dict[str, Any]] = []
    for replica in range(n):
        rng = random.Random(_seed(cfg, symbol, session, replica))
        synth = sign_flip_bars(ordered, start, rng)
        events = extract_touch_events(symbol, orange, ladder, synth, cfg, tick=tick)
        if not events:
            continue
        # Only bars and stamps are read by prior_rung_respect; the indicator
        # series a full SessionContext builds are not needed here.
        ctx = SessionContext(
            symbol=symbol, bars=synth, series=BarSeries(synth), stamps=[b.ts for b in synth]
        )
        for i, ev in enumerate(events):
            prior = prior_rung_respect(ev, events[:i], ctx, orange, cfg)
            rows.append(
                {
                    "symbol": symbol,
                    "session": session,
                    "replica": replica,
                    "rung_k": ev.rung_k,
                    "extension_k": ev.rung_k,
                    "side": ev.side,
                    "outcome": ev.outcome,
                    "next_exists": ev.next_exists,
                    "touch_offset_r": touch_offset_r(ev, r),
                    "consecutive_extensions_broken": prior["consecutive_extensions_broken"],
                    "prior_extensions_touched": prior["prior_extensions_touched"],
                    "minutes_since_or": ev.minutes_since_or,
                    "or_width": r,
                }
            )
    return rows
