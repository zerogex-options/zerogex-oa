"""Playbook backtest harness — PR-14.

Walks persisted ``signal_action_cards`` rows over a backtest window, computes
forward-return outcomes against ``underlying_quotes``, and aggregates
per-pattern hit rate / MFE / MAE statistics into ``playbook_pattern_stats``.

Read-only with respect to live behavior: patterns keep their hard-coded
``pattern_base`` priors.  A follow-up PR can wire the empirical numbers
into live confidence; this PR ships the measurement infrastructure so
those numbers exist to be reviewed first.

CLI:

    python -m src.signals.playbook.backtest --underlying SPY --days 60

Methodology:
- For each Card in the window, fetch underlying prices from
  ``underlying_quotes`` over [card.timestamp, card.timestamp +
  max_hold_minutes].
- Compute MFE (max favorable) and MAE (max adverse) excursions in the
  Card's signed direction.
- Entry-trigger enforcement: for a touch/break entry the trade is not
  considered filled until the underlying actually reaches
  ``entry.ref_price``; MFE/MAE and target/stop are measured only from the
  fill bar onward. Cards whose trigger never fills are ``no_fill`` and are
  excluded from resolved stats (counting never-filled Cards as filled
  inflated the hit rate).
- Outcome:
    * ``target_hit`` — favorable price reached the target before adverse
      reached the stop.
    * ``stop_hit`` — adverse touched stop first.
    * ``time_exit`` — neither resolved within max_hold.
    * ``no_data`` — too few quotes to decide.
    * ``no_fill`` — entry trigger never reached inside the hold window.
    * ``unresolved`` — neither target nor stop is a price level
      (premium_pct / signal_event short-premium structures); not
      resolvable from the underlying series, excluded from stats.
- Cost: an optional round-trip cost/slippage haircut
  (``BACKTEST_ROUND_TRIP_COST_PCT``, default 0.0) shifts the target/stop
  and nets MFE/MAE so ``proposed_base`` can reflect net-of-cost edge. The
  gross underlying-touch hit rate materially overstates realized 0DTE
  edge; enable a cost before wiring proposed_base into live sizing.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

import pytz

logger = logging.getLogger(__name__)


_OUTCOME_LABELS = (
    "target_hit",
    "stop_hit",
    "time_exit",
    "no_data",
    # The Card's entry trigger never filled inside the hold window. Scoring
    # such Cards as if filled (the old behavior) inflated hit rate, because
    # the forward window was selected *because the pattern fired*.
    "no_fill",
    # Neither target nor stop is a price level (e.g. iron-condor
    # premium_pct / signal_event exits). These cannot be resolved from the
    # underlying series. The old code labeled them ``time_exit``, which
    # counted them as resolved-but-not-a-loss and hid short-premium tail
    # risk. They are now excluded from resolved stats entirely.
    "unresolved",
)

# Entry triggers that fill immediately at the Card timestamp (no separate
# touch/break confirmation). Anything else (at_touch / on_break / at_limit /
# at_level …) requires the underlying to reach entry.ref_price before the
# trade is considered filled.
_IMMEDIATE_TRIGGERS = {"at_market", "at_close", "market", "immediate", "now", ""}

# Optional round-trip cost/slippage haircut as a fraction of entry price,
# applied to both the target/stop levels and the MFE/MAE excursions so the
# empirical base reflects net-of-cost edge rather than a frictionless
# underlying touch. Default 0.0 keeps measurements identical to the
# pre-existing gross numbers; set BACKTEST_ROUND_TRIP_COST_PCT (e.g. 0.0015)
# to enable. 0DTE option round-trips frequently cost 5-15% of *premium*,
# so the gross underlying-touch hit rate overstates realized edge — operators
# should enable this before wiring proposed_base into live sizing.
try:
    _ROUND_TRIP_COST_PCT = max(0.0, float(os.getenv("BACKTEST_ROUND_TRIP_COST_PCT", "0")))
except (TypeError, ValueError):
    _ROUND_TRIP_COST_PCT = 0.0
_DEFAULT_DAYS = 60
# Smoothing prior: empirical_base = (target_hits + alpha) / (resolved + alpha + beta).
# Defaults pull untested patterns toward 0.50 so a single lucky/unlucky
# resolved trade doesn't generate an outsized "empirical" base.
_PRIOR_ALPHA = 5.0
_PRIOR_BETA = 5.0


@dataclass
class CardRow:
    """A row pulled from ``signal_action_cards``, normalized for the backtest."""

    underlying: str
    timestamp: datetime
    pattern: str
    action: str
    tier: str
    direction: str
    confidence: float
    payload: dict


@dataclass
class CardOutcome:
    """Result of applying forward-return analysis to one Card."""

    card: CardRow
    outcome: str  # one of _OUTCOME_LABELS
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    target_hit_at: Optional[datetime] = None
    stop_hit_at: Optional[datetime] = None
    expired_at: Optional[datetime] = None
    note: str = ""


@dataclass
class PatternStats:
    pattern: str
    underlying: str
    window_start: date
    window_end: date
    n_emitted: int = 0
    n_resolved: int = 0
    n_target_hit: int = 0
    n_stop_hit: int = 0
    n_time_exit: int = 0
    sum_confidence: float = 0.0
    mfe_total: float = 0.0
    mae_total: float = 0.0

    @property
    def hit_rate(self) -> Optional[float]:
        if self.n_resolved == 0:
            return None
        return self.n_target_hit / self.n_resolved

    @property
    def avg_confidence(self) -> Optional[float]:
        if self.n_emitted == 0:
            return None
        return self.sum_confidence / self.n_emitted

    @property
    def avg_mfe_pct(self) -> Optional[float]:
        if self.n_resolved == 0:
            return None
        return self.mfe_total / self.n_resolved

    @property
    def avg_mae_pct(self) -> Optional[float]:
        if self.n_resolved == 0:
            return None
        return self.mae_total / self.n_resolved

    @property
    def proposed_base(self) -> Optional[float]:
        """Beta-smoothed empirical base, clamped to [0.40, 0.85].

        Mirrors the spec range for ``pattern_base`` in §5 of the catalog.
        Returns None when there's nothing to estimate from.
        """
        if self.n_resolved == 0:
            return None
        wins = self.n_target_hit + _PRIOR_ALPHA
        total = self.n_resolved + _PRIOR_ALPHA + _PRIOR_BETA
        raw = wins / total
        return max(0.40, min(0.85, raw))


# ---------------------------------------------------------------------------
# Outcome computation
# ---------------------------------------------------------------------------


def _signed_excursion(direction: str, entry: float, price: float) -> float:
    """Signed excursion in fraction of entry — positive = favorable."""
    if entry <= 0:
        return 0.0
    delta = (price - entry) / entry
    if direction == "bearish":
        delta = -delta
    return delta


def _normalize_quote(q: tuple) -> tuple:
    """Accept either ``(ts, close)`` or ``(ts, open, high, low, close)``.

    A 2-tuple is treated as a degenerate bar (O=H=L=C) so callers/tests
    that only have closes get exactly the old close-only semantics; a
    5-tuple uses the true intrabar range.
    """
    if len(q) == 5:
        ts, o, h, low, c = q
        return ts, float(o), float(h), float(low), float(c)
    ts, c = q[0], float(q[1])
    return ts, c, c, c, c


def _hit_target(direction: str, target: Optional[float], high: float, low: float) -> bool:
    """True if the bar's range reached the (directional) target."""
    if target is None:
        return False
    if direction == "bullish":
        return high >= target
    return low <= target


def _hit_stop(direction: str, stop: Optional[float], high: float, low: float) -> bool:
    """True if the bar's range reached the (directional) stop."""
    if stop is None:
        return False
    if direction == "bullish":
        return low <= stop
    return high >= stop


def _level_or_none(level: Optional[dict]) -> Optional[float]:
    if not isinstance(level, dict):
        return None
    if level.get("kind") != "level":
        return None
    val = level.get("ref_price")
    if not isinstance(val, (int, float)):
        return None
    return float(val)


def compute_outcome(
    card: CardRow,
    quotes: Iterable[tuple],
) -> CardOutcome:
    """Decide the outcome of ``card`` from the trailing price series.

    Each quote is ``(ts, open, high, low, close)`` (preferred) or the
    legacy ``(ts, close)``.  Resolution uses the bar's intrabar range:
    a bar that traded *through* the stop or target counts as a touch
    even if it closed back inside (close-only resolution silently
    under-counted both stops and targets and understated MAE).  When a
    single bar's range spans BOTH the target and the stop the intrabar
    sequence is unknown, so it resolves conservatively to ``stop_hit``.

    ``quotes`` must be ordered oldest → newest and span the Card's hold
    window (``card.timestamp`` to ``card.timestamp + max_hold_minutes``).
    Quotes outside that window are tolerated and ignored.
    """
    payload = card.payload or {}
    entry_payload = payload.get("entry") or {}
    target_payload = payload.get("target") or {}
    stop_payload = payload.get("stop") or {}
    max_hold = int(payload.get("max_hold_minutes") or 0)

    entry = entry_payload.get("ref_price")
    if not isinstance(entry, (int, float)) or entry <= 0 or max_hold <= 0:
        return CardOutcome(
            card=card,
            outcome="no_data",
            note="missing entry / max_hold_minutes",
        )

    direction = card.direction or payload.get("direction") or ""
    if direction not in ("bullish", "bearish"):
        return CardOutcome(
            card=card,
            outcome="no_data",
            note=f"non-directional Card ({direction!r}); not price-resolvable",
        )

    target_price = _level_or_none(target_payload)
    stop_price = _level_or_none(stop_payload)

    # Neither exit is a price level (premium_pct / signal_event short-premium
    # structures). These are not resolvable from the underlying series. The
    # old code fell through to ``time_exit``, which counted them as
    # resolved-and-not-a-loss and made iron-condor/fear-fade tail risk
    # invisible. Classify as ``unresolved`` so they are excluded from
    # n_resolved / hit_rate / proposed_base instead of flattering the stats.
    if target_price is None and stop_price is None:
        return CardOutcome(
            card=card,
            outcome="unresolved",
            note="non-level target and stop; not price-resolvable",
        )

    trigger = str(entry_payload.get("trigger") or "").strip().lower()
    fills_immediately = trigger in _IMMEDIATE_TRIGGERS
    cost = _ROUND_TRIP_COST_PCT
    entry_f = float(entry)

    deadline = card.timestamp + timedelta(minutes=max_hold)
    mfe_pct = 0.0
    mae_pct = 0.0
    target_hit_at: Optional[datetime] = None
    stop_hit_at: Optional[datetime] = None
    n_quotes = 0
    filled = fills_immediately

    for raw in quotes:
        ts, o, high, low, c = _normalize_quote(raw)
        if ts < card.timestamp:
            continue
        if ts > deadline:
            break
        n_quotes += 1

        # Entry-trigger enforcement: for a touch/break entry the trade is
        # not live until the underlying actually reaches entry.ref_price.
        # MFE/MAE and target/stop are measured ONLY from the fill bar
        # onward — scoring the favorable excursion of a fill that never
        # happened was a systematic upward bias in hit rate.
        if not filled:
            if low <= entry_f <= high:
                filled = True
            else:
                continue

        # Intrabar extremes in the Card's signed direction: the most
        # favorable price is the high (bullish) / low (bearish); the
        # most adverse is the opposite extreme.
        if direction == "bullish":
            favorable, adverse = high, low
        else:
            favorable, adverse = low, high
        fav_exc = _signed_excursion(direction, entry_f, favorable)
        adv_exc = _signed_excursion(direction, entry_f, adverse)
        if fav_exc > mfe_pct:
            mfe_pct = fav_exc
        if adv_exc < mae_pct:
            mae_pct = adv_exc

        # Optional round-trip cost: move the target further away and the
        # stop closer (both by entry*cost) so resolution reflects
        # net-of-cost edge. Fully inert at the default cost=0.0 — the
        # original frictionless levels are used unchanged.
        if cost and target_price is not None:
            eff_target = (
                target_price + entry_f * cost
                if direction == "bullish"
                else target_price - entry_f * cost
            )
        else:
            eff_target = target_price
        if cost and stop_price is not None:
            eff_stop = (
                stop_price + entry_f * cost
                if direction == "bullish"
                else stop_price - entry_f * cost
            )
        else:
            eff_stop = stop_price

        hit_target = _hit_target(direction, eff_target, high, low)
        hit_stop = _hit_stop(direction, eff_stop, high, low)

        # Same-bar both-touch: intrabar order is unknowable from OHLC,
        # so resolve conservatively to the stop (never inflate edge).
        if hit_target and hit_stop:
            stop_hit_at = ts
            break
        if hit_target:
            target_hit_at = ts
            break
        if hit_stop:
            stop_hit_at = ts
            break

    if n_quotes == 0:
        return CardOutcome(
            card=card,
            outcome="no_data",
            note="no underlying quotes inside hold window",
        )

    if not filled:
        return CardOutcome(
            card=card,
            outcome="no_fill",
            note=f"entry trigger {trigger!r} never reached entry={entry_f}",
        )

    # Net the reported excursions by the round-trip cost (inert at cost=0).
    net_mfe = mfe_pct - cost
    net_mae = mae_pct - cost

    if target_hit_at is not None and (stop_hit_at is None or target_hit_at <= stop_hit_at):
        return CardOutcome(
            card=card,
            outcome="target_hit",
            mfe_pct=round(net_mfe, 6),
            mae_pct=round(net_mae, 6),
            target_hit_at=target_hit_at,
        )
    if stop_hit_at is not None:
        return CardOutcome(
            card=card,
            outcome="stop_hit",
            mfe_pct=round(net_mfe, 6),
            mae_pct=round(net_mae, 6),
            stop_hit_at=stop_hit_at,
        )

    return CardOutcome(
        card=card,
        outcome="time_exit",
        mfe_pct=round(net_mfe, 6),
        mae_pct=round(net_mae, 6),
        expired_at=deadline,
        note="neither level reached",
    )


# ---------------------------------------------------------------------------
# DB I/O (psycopg2-style sync)
# ---------------------------------------------------------------------------


def fetch_action_cards(conn, underlying: str, start: datetime, end: datetime) -> list[CardRow]:
    """Read non-STAND_DOWN Cards from ``signal_action_cards`` in [start, end]."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT underlying, timestamp, pattern, action, tier, direction, confidence, payload
        FROM signal_action_cards
        WHERE underlying = %s
          AND timestamp BETWEEN %s AND %s
          AND action <> 'STAND_DOWN'
        ORDER BY timestamp ASC
        """,
        (underlying, start, end),
    )
    rows = cur.fetchall()
    out: list[CardRow] = []
    for r in rows:
        payload = r[7]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        out.append(
            CardRow(
                underlying=r[0],
                timestamp=r[1],
                pattern=r[2],
                action=r[3],
                tier=r[4],
                direction=r[5],
                confidence=float(r[6] or 0.0),
                payload=payload or {},
            )
        )
    return out


def fetch_quotes(
    conn, underlying: str, start: datetime, end: datetime
) -> list[tuple[datetime, float, float, float, float]]:
    """Trailing 1-min underlying OHLC bars inside [start, end].

    Returns ``(ts, open, high, low, close)`` so outcome resolution can
    see the intrabar range, not just the close.  O/H/L fall back to
    close when NULL (older rows); rows with a NULL close are dropped.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, open, high, low, close
        FROM underlying_quotes
        WHERE symbol = %s
          AND timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
        """,
        (underlying, start, end),
    )
    out: list[tuple[datetime, float, float, float, float]] = []
    for r in cur.fetchall():
        if r[4] is None:
            continue
        c = float(r[4])
        out.append(
            (
                r[0],
                float(r[1]) if r[1] is not None else c,
                float(r[2]) if r[2] is not None else c,
                float(r[3]) if r[3] is not None else c,
                c,
            )
        )
    return out


def upsert_pattern_stats(conn, stats: Iterable[PatternStats]) -> None:
    cur = conn.cursor()
    for s in stats:
        cur.execute(
            """
            INSERT INTO playbook_pattern_stats
                (pattern, underlying, window_start, window_end,
                 n_emitted, n_resolved, n_target_hit, n_stop_hit, n_time_exit,
                 hit_rate, avg_confidence, avg_mfe_pct, avg_mae_pct,
                 proposed_base, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (pattern, underlying, window_start, window_end) DO UPDATE SET
                n_emitted     = EXCLUDED.n_emitted,
                n_resolved    = EXCLUDED.n_resolved,
                n_target_hit  = EXCLUDED.n_target_hit,
                n_stop_hit    = EXCLUDED.n_stop_hit,
                n_time_exit   = EXCLUDED.n_time_exit,
                hit_rate      = EXCLUDED.hit_rate,
                avg_confidence= EXCLUDED.avg_confidence,
                avg_mfe_pct   = EXCLUDED.avg_mfe_pct,
                avg_mae_pct   = EXCLUDED.avg_mae_pct,
                proposed_base = EXCLUDED.proposed_base,
                computed_at   = NOW()
            """,
            (
                s.pattern,
                s.underlying,
                s.window_start,
                s.window_end,
                s.n_emitted,
                s.n_resolved,
                s.n_target_hit,
                s.n_stop_hit,
                s.n_time_exit,
                s.hit_rate,
                s.avg_confidence,
                s.avg_mfe_pct,
                s.avg_mae_pct,
                s.proposed_base,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def aggregate(
    outcomes: Iterable[CardOutcome],
    *,
    underlying: str,
    window_start: date,
    window_end: date,
) -> list[PatternStats]:
    by_pattern: dict[str, PatternStats] = {}
    for oc in outcomes:
        ps = by_pattern.setdefault(
            oc.card.pattern,
            PatternStats(
                pattern=oc.card.pattern,
                underlying=underlying,
                window_start=window_start,
                window_end=window_end,
            ),
        )
        ps.n_emitted += 1
        ps.sum_confidence += oc.card.confidence
        if oc.outcome == "target_hit":
            ps.n_target_hit += 1
            ps.n_resolved += 1
            ps.mfe_total += oc.mfe_pct
            ps.mae_total += oc.mae_pct
        elif oc.outcome == "stop_hit":
            ps.n_stop_hit += 1
            ps.n_resolved += 1
            ps.mfe_total += oc.mfe_pct
            ps.mae_total += oc.mae_pct
        elif oc.outcome == "time_exit":
            ps.n_time_exit += 1
            ps.n_resolved += 1
            ps.mfe_total += oc.mfe_pct
            ps.mae_total += oc.mae_pct
        # no_data / no_fill / unresolved are counted in n_emitted but NOT
        # n_resolved, so hit_rate and proposed_base are computed only over
        # genuinely filled, price-resolvable trades.
    return list(by_pattern.values())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run(
    *,
    underlying: str,
    days: int,
    conn=None,
    write: bool = True,
) -> list[PatternStats]:
    """End-to-end: fetch cards + quotes, compute outcomes, aggregate, persist.

    When ``conn`` is None we open a fresh ``db_connection``.  When
    ``write`` is False the stats are returned but not persisted — useful
    for tests and dry-runs.
    """
    et = pytz.timezone("America/New_York")
    end_dt = datetime.now(pytz.UTC)
    start_dt = end_dt - timedelta(days=days)
    window_start = start_dt.astimezone(et).date()
    window_end = end_dt.astimezone(et).date()

    if conn is None:
        from src.database import db_connection

        with db_connection() as local_conn:
            return _run_with_conn(
                conn=local_conn,
                underlying=underlying,
                start_dt=start_dt,
                end_dt=end_dt,
                window_start=window_start,
                window_end=window_end,
                write=write,
            )
    return _run_with_conn(
        conn=conn,
        underlying=underlying,
        start_dt=start_dt,
        end_dt=end_dt,
        window_start=window_start,
        window_end=window_end,
        write=write,
    )


def _run_with_conn(
    *,
    conn,
    underlying: str,
    start_dt: datetime,
    end_dt: datetime,
    window_start: date,
    window_end: date,
    write: bool,
) -> list[PatternStats]:
    cards = fetch_action_cards(conn, underlying, start_dt, end_dt)
    if not cards:
        logger.info(
            "Backtest: no cards found for %s in [%s, %s]",
            underlying,
            window_start,
            window_end,
        )
        return []

    # One bulk quote pull covering the whole window (worst-case end =
    # last card timestamp + max max_hold across patterns).  Minutes-aligned
    # quotes mean even a multi-day hold window is small in row terms.
    quote_window_end = max(
        end_dt,
        max(
            c.timestamp + timedelta(minutes=int(c.payload.get("max_hold_minutes") or 0))
            for c in cards
        ),
    )
    quotes = fetch_quotes(conn, underlying, start_dt, quote_window_end)

    outcomes: list[CardOutcome] = []
    for card in cards:
        max_hold = int(card.payload.get("max_hold_minutes") or 0)
        if max_hold <= 0:
            outcomes.append(CardOutcome(card=card, outcome="no_data", note="no max_hold"))
            continue
        deadline = card.timestamp + timedelta(minutes=max_hold)
        relevant = [q for q in quotes if card.timestamp <= q[0] <= deadline]
        outcomes.append(compute_outcome(card, relevant))

    stats = aggregate(
        outcomes,
        underlying=underlying,
        window_start=window_start,
        window_end=window_end,
    )
    if write and stats:
        upsert_pattern_stats(conn, stats)
    return stats


def _format_table(stats: list[PatternStats]) -> str:
    """Plain-text summary for CLI output."""
    if not stats:
        return "No stats produced (no Cards in window?)."
    rows = ["pattern,n_emitted,n_resolved,hit_rate,avg_conf,avg_mfe,avg_mae,proposed_base"]
    for s in sorted(stats, key=lambda x: (x.pattern,)):
        rows.append(
            f"{s.pattern},{s.n_emitted},{s.n_resolved},"
            f"{(s.hit_rate or 0):.3f},{(s.avg_confidence or 0):.3f},"
            f"{(s.avg_mfe_pct or 0):.4f},{(s.avg_mae_pct or 0):.4f},"
            f"{(s.proposed_base or 0):.3f}"
        )
    return "\n".join(rows)


def main(argv: Optional[list[str]] = None) -> int:
    import os

    parser = argparse.ArgumentParser(description="Playbook pattern backtest harness")
    parser.add_argument("--underlying", default=os.getenv("BACKTEST_UNDERLYING", "SPY"))
    parser.add_argument(
        "--days",
        type=int,
        default=int(os.getenv("BACKTEST_DAYS", str(_DEFAULT_DAYS))),
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Compute and print stats but skip the playbook_pattern_stats UPSERT.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    stats = run(underlying=args.underlying, days=args.days, write=not args.no_write)
    print(_format_table(stats))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
