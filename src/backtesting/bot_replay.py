"""Replay a catalog strategy's BOT rule over history into Action Cards.

The third card source, alongside persisted playbook Cards
(``fetch_action_cards``) and the custom-strategy compiler
(``src/backtesting/strategy.py``). It exists so that **every** strategy in
``src/strategies`` is backtestable from one place, not just the ones a
playbook pattern happens to implement — 12 of the catalog's 35 strategies are
bot-only, and before this they could be screened solely by the offline
``make tradeworkz-backtest`` CLI and never appeared in Backtesting or Pattern
Insights at all.

Why cards and not a parallel simulator
--------------------------------------
``src/tradeworkz/backtest.py`` already replays a bot end to end, but it owns
its own fills, exits and P&L accounting, so its numbers are not comparable
with a pattern backtest. This module takes the bot's **entry rule only** and
emits :class:`CardRow` objects, which ``engine.run_backtest`` then prices
through the exact same forward walk, sizing, concurrency cap and fill model as
every other card. One equity curve, one cooldown, one set of assumptions — so
a bot strategy and a pattern strategy in the same run are genuinely
comparable. Exits come from the signal's own target/stop levels, which is how
the engine resolves a pattern card too.

Faithfulness to live behavior
-----------------------------
* **Snapshots** are rebuilt with ``build_snapshot(as_of=t)``, which bounds
  every read to ``timestamp <= t`` — the bot sees only rows that existed then.
* **The clock** is injected via ``bots.base.set_backtest_clock``, so a bot's
  time-of-day gates evaluate on replay time rather than wall-clock now.
* **The bias veto** and the **RTH no-new-opens window** are applied exactly as
  the live engine applies them, so a signal the engine would have refused is
  refused here.
* **ML is cold** (empty state) and **sizing is the engine's**, not the bot's
  Kelly sleeve — this measures the entry rule, not the capital policy.

Known limitation, disclosed: regime percentile bands
(``gex_historical_stats``) are a nightly 30-day aggregate with no per-instant
history, so a symbol-relative "strong/weak" split uses today's bands rather
than the bands as they stood at ``t``. It shifts slowly and never flips the
sign of a regime, but it can nudge a boundary entry. Everything else is
strictly as-of.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from src.backtesting.models import BacktestSpec
from src.signals.playbook.backtest import CardRow
from src.strategies import StrategyEntry, find
from src.tradeworkz import config as tw_config
from src.tradeworkz.bots import base as bot_base
from src.tradeworkz.context import build_snapshot
from src.tradeworkz.registry import get_bot_class, known_specs

logger = logging.getLogger(__name__)

#: Minutes between evaluated instants. Matches the live engine's effective
#: per-symbol cadence closely enough that entry frequency is comparable, while
#: keeping a 60-day single-symbol replay to a few thousand snapshots.
DEFAULT_INTERVAL_MIN = 5

_RTH_OPEN = time(9, 30)
_RTH_CLOSE = time(16, 0)

#: Replay clock, read by the injected ``set_backtest_clock`` callable. Module
#: level (not a closure over a loop variable) so the bot's ``_utcnow`` sees
#: each step as it advances.
_CLOCK: Dict[str, datetime] = {"now": datetime(1970, 1, 1, tzinfo=timezone.utc)}


def _et(dt: datetime) -> datetime:
    from src.market_calendar import ET

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ET)


def _is_trading_day(d) -> bool:
    """Weekday that is not a configured NYSE holiday.

    The noon probe must be LOCALIZED to ET, not naive: ``is_market_hours``
    reads a naive datetime as UTC, and noon UTC is 08:00 ET — before the open
    — so a naive probe reports every day as closed and the replay produces no
    timesteps at all.
    """
    from src.market_calendar import ET, is_market_hours

    return is_market_hours(ET.localize(datetime.combine(d, time(12, 0))))


def rth_timesteps(start: datetime, end: datetime, interval_min: int) -> List[datetime]:
    """Evaluated instants: every ``interval_min`` inside RTH on trading days.

    Kept local rather than imported from ``src.tradeworkz.backtest`` so the
    backtesting service does not depend on the research CLI module (which pulls
    in argparse-era reporting helpers it has no use for).
    """
    if interval_min <= 0:
        interval_min = DEFAULT_INTERVAL_MIN
    out: List[datetime] = []
    cursor = start
    step = timedelta(minutes=interval_min)
    while cursor <= end:
        local = _et(cursor)
        if _RTH_OPEN <= local.time() < _RTH_CLOSE and _is_trading_day(local.date()):
            out.append(cursor)
        cursor += step
    return out


def _parse_hhmm(raw: str, fallback: time) -> time:
    try:
        hh, mm = str(raw).split(":")
        return time(int(hh), int(mm))
    except (ValueError, AttributeError):
        return fallback


def _opens_allowed(now_utc: datetime, no_new_opens_after: time) -> bool:
    """Mirror the live engine's late-session open cutoff."""
    return _et(now_utc).time() < no_new_opens_after


def _leg_payload(leg: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a TradeWorkz leg dict to the engine's card-leg shape.

    TradeWorkz uses ``side`` long/short and ``option_type`` call/put; the
    engine's ``_select_legs`` reads ``side`` BUY/SELL and ``right`` C/P.
    """
    side = "SELL" if str(leg.get("side") or "long").lower() == "short" else "BUY"
    right = "P" if str(leg.get("option_type") or "call").lower().startswith("p") else "C"
    return {
        "expiry": leg.get("expiration"),
        "strike": leg.get("strike"),
        "right": right,
        "side": side,
    }


def _level(price: Optional[float], name: str) -> Dict[str, Any]:
    """A level-kind target/stop the engine can resolve on the price series.

    A bot that returns no target (or no stop) yields a non-level entry, which
    the engine then resolves from the run's premium overlay — the same
    fallback a pattern card without levels takes.
    """
    if price is None:
        return {"ref_price": None, "kind": "premium_pct"}
    return {"ref_price": float(price), "kind": "level", "level_name": name}


def _signal_to_card(
    entry: StrategyEntry,
    signal: Any,
    snap: Any,
    *,
    underlying: str,
    at: datetime,
    tier: str,
    max_hold: int,
) -> CardRow:
    """One accepted bot signal as a CardRow keyed on the CATALOG id.

    Keying on the catalog id (not the legacy ``tw_bots.id``) is what makes the
    per-strategy rollups in the run summary, the trade blotter and Pattern
    Insights line up with the same strategy measured through its pattern
    binding.
    """
    extra = getattr(snap, "extra", None) or {}
    hold = signal.time_stop_at
    if hold is not None:
        minutes = int(max(1, (hold - at).total_seconds() // 60))
    else:
        minutes = max_hold
    payload: Dict[str, Any] = {
        "direction": signal.direction,
        "entry": {"ref_price": float(signal.entry_price or snap.spot), "trigger": "at_market"},
        "max_hold_minutes": minutes,
        "legs": [_leg_payload(asdict(leg)) for leg in signal.legs],
        "target": _level(signal.target_price, "bot_target"),
        "stop": _level(signal.stop_price, "bot_stop"),
        "context": {
            "net_gex": getattr(snap, "net_gex", None),
            "regime": extra.get("msi_regime"),
            "msi": extra.get("msi"),
        },
        # Provenance, so a trade row is traceable back to the rule that made it.
        "source": "bot_replay",
        "bot_id": entry.bot_id,
        "strategy_type": signal.strategy_type,
        "rationale": signal.rationale,
    }
    return CardRow(
        underlying=underlying,
        timestamp=at,
        pattern=entry.id,
        action=str(signal.strategy_type or "BOT").upper(),
        tier=tier,
        direction=signal.direction,
        confidence=float(signal.conviction or 0.0),
        payload=payload,
    )


def bot_backed(strategy_ids: Iterable[str]) -> List[StrategyEntry]:
    """The subset of ``strategy_ids`` a bot implements (pattern-only dropped)."""
    out: List[StrategyEntry] = []
    for sid in strategy_ids:
        entry = find(sid)
        if entry is not None and entry.bot_class is not None:
            out.append(entry)
    return out


def generate_bot_cards(
    conn,
    spec: BacktestSpec,
    entries: Sequence[StrategyEntry],
    *,
    max_hold: int,
    interval_min: int = DEFAULT_INTERVAL_MIN,
) -> List[CardRow]:
    """Replay each entry's bot rule over the spec window into CardRows.

    Snapshots are built once per instant and shared across every bot in
    ``entries``, so screening ten strategies over one window costs about the
    same wall clock as screening one.

    Returns cards pre-cooldown, ascending by timestamp; ``run_backtest``
    applies the per-strategy cooldown that collapses a continuous signal
    stream into discrete entries, exactly as it does for playbook cards.
    """
    if not entries:
        return []
    specs = known_specs()
    runners = []
    for entry in entries:
        bot_spec = specs.get(entry.id) or (entry.bot_id and specs.get(entry.bot_id))
        if bot_spec is None:  # pragma: no cover - guarded by bot_backed()
            logger.warning("bot_replay: no BotSpec for %s, skipping", entry.id)
            continue
        runners.append((entry, get_bot_class(bot_spec.strategy_class)(bot_spec, ml_state=None)))
    if not runners:
        return []

    start = datetime.combine(spec.start_date, _RTH_OPEN).replace(tzinfo=timezone.utc)
    end = datetime.combine(spec.end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
    steps = rth_timesteps(start, end, interval_min)
    no_new_opens_after = _parse_hhmm(tw_config.RTH_NO_NEW_OPENS_AFTER_ET, time(15, 55))
    underlying = spec.underlying

    cards: List[CardRow] = []
    vetoed = 0
    snapshots = 0
    bot_base.set_backtest_clock(lambda: _CLOCK["now"])
    try:
        for t in steps:
            _CLOCK["now"] = t
            if not _opens_allowed(t, no_new_opens_after):
                continue
            snap = build_snapshot(conn, underlying, as_of=t)
            if snap is None:
                continue
            snapshots += 1
            for entry, bot in runners:
                try:
                    signal = bot.open_criteria(snap)
                except Exception:  # one bad instant must not kill the replay
                    logger.warning(
                        "bot_replay: %s raised at %s", entry.id, t.isoformat(), exc_info=True
                    )
                    continue
                if signal is None:
                    continue
                if bot._bias_veto(snap, signal.direction):
                    vetoed += 1
                    continue
                cards.append(
                    _signal_to_card(
                        entry,
                        signal,
                        snap,
                        underlying=underlying,
                        at=t,
                        tier=entry.tier,
                        max_hold=max_hold,
                    )
                )
    finally:
        bot_base.set_backtest_clock(None)
        _CLOCK["now"] = datetime(1970, 1, 1, tzinfo=timezone.utc)

    logger.info(
        "bot_replay: %d strategies x %d steps (%d priced snapshots) -> %d signals (%d bias-vetoed)",
        len(runners),
        len(steps),
        snapshots,
        len(cards),
        vetoed,
    )
    cards.sort(key=lambda c: c.timestamp)
    return cards
