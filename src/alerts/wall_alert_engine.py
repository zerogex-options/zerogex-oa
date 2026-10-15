"""Wall-alert service: watch the tape against the walls and alert members.

Run as ``python -m src.alerts.wall_alert_engine`` (systemd unit
``zerogex-oa-wall-alerts``). Every ``WALL_ALERTS_POLL_SECONDS`` during the
regular session it reads, per symbol, the newest 1-minute bars (including the
minute still forming) and the published walls, steps one
:class:`~src.alerts.wall_detector.WallAlertDetector` per stream (each
sensitivity profile at its own touch band and at each touch tolerance a member
may choose, :func:`~src.alerts.wall_detector.stream_profiles`), and records
each event with its deliveries in one transaction.

A separate process, not a hook in the analytics engine or the API, on
purpose: a bug or a slow query here must never cost the product its GEX
numbers or its endpoints. It reads two tables those services already write and
writes only its own.

Operational guarantees:

* **One instance.** A Postgres advisory lock is held for the life of the
  process; a second copy (a manual run beside the service) waits idle rather
  than double-alerting. The events' unique key backs this up.
* **Restart-safe.** Detector state is saved per (symbol, stream) whenever it
  changes, so a restart resumes open tests and cooldowns instead of losing
  them or re-alerting. With no saved state it warms up from the last
  ``WALL_ALERTS_WARMUP_MINUTES`` of tape with delivery suppressed.
* **No stale alerts.** A symbol whose tape or walls stopped updating is
  skipped, and an event found on a bar older than
  ``WALL_ALERTS_MAX_EVENT_AGE_SECONDS`` (catch-up after a gap) updates state
  but is never delivered.

``--replay SYMBOL --date YYYY-MM-DD`` runs the same detectors over a past
session offline and prints what each profile would have alerted. It writes
nothing; it is how the profiles are tuned and how an operator checks the noise
level before trusting a change. ``--touch 0.10`` replays at a member's touch
tolerance instead of each profile's own band, and ``--explain`` prints every
decision behind the alerts: when each wall armed, every test and why its hit
did or did not alert, near misses outside the band, and each outcome. It is
the answer to "why didn't that fire?".
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.alerts import config as alerts_config
from src.alerts.wall_detector import (
    ET,
    PROFILES,
    SensitivityProfile,
    StreamKey,
    TraceEntry,
    WallAlertDetector,
    WallEvent,
    session_date_for,
    stream_profiles,
)
from src.alerts.wall_payload import strength_tier
from src.alerts.wall_store import (
    WallsRow,
    load_bars,
    load_odds,
    load_states,
    load_walls,
    prune,
    record_and_fan_out,
    save_states,
    session_bounds,
    walls_at,
)
from src.utils import get_logger

logger = get_logger(__name__)

#: Advisory-lock key: hashtext('zerogex-wall-alerts') would do, but a literal
#: keeps the lock identifiable in pg_locks without a function call.
ADVISORY_LOCK_KEY = 7_303_250_017

#: A minute bar counts as complete this many seconds after its minute ends, so
#: the final upsert of the minute has landed before its close is trusted.
BAR_SETTLE_SECONDS = 5.0

_FAILURE_BACKOFF_MAX_SECONDS = 60.0
_SHUTDOWN_POLL_SECONDS = 2.0
_ODDS_TTL_SECONDS = 600.0


@dataclass
class TickSummary:
    symbols: int = 0
    skipped: Dict[str, str] = field(default_factory=dict)
    events: int = 0
    delivered: int = 0
    suppressed_late: int = 0


class WallAlertService:
    """Owns the detectors and drives them from the database."""

    def __init__(self, symbols: Optional[Tuple[str, ...]] = None) -> None:
        self.symbols = tuple(symbols or alerts_config.symbols())
        self.session: Optional[date] = None
        self.streams: Dict[StreamKey, SensitivityProfile] = stream_profiles(
            alerts_config.TOUCH_OPTIONS_BP
        )
        self.detectors: Dict[str, Dict[StreamKey, WallAlertDetector]] = {}
        self._saved: Dict[Tuple[str, StreamKey], str] = {}
        self._restored: Dict[str, bool] = {}
        self._skip_reason: Dict[str, str] = {}
        self._symbol_errors: Dict[str, int] = {}
        self._pctile_cache: Dict[Tuple[str, str, int, int], Optional[float]] = {}
        self._odds_cache: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]]]] = {}
        self._last_prune: Optional[date] = None
        self._lock_conn: Any = None
        self.running = False

    # ------------------------------------------------------------ lifecycle

    def _start_session(self, session: date) -> None:
        self.session = session
        self.streams = stream_profiles(alerts_config.TOUCH_OPTIONS_BP)
        self.detectors = {
            sym: {key: WallAlertDetector(sym, prof) for key, prof in self.streams.items()}
            for sym in self.symbols
        }
        self._saved = {}
        self._restored = {sym: False for sym in self.symbols}
        self._skip_reason = {}
        self._symbol_errors = {}
        self._pctile_cache = {}
        logger.info(
            "Wall alerts: session %s, watching %s under %d streams (%s, touch tolerances %s bp)",
            session.isoformat(),
            ",".join(self.symbols),
            len(self.streams),
            ",".join(PROFILES),
            ",".join(str(bp) for bp in alerts_config.TOUCH_OPTIONS_BP),
        )

    def _restore(self, conn: Any, symbol: str, session: date) -> bool:
        """Load today's saved state for ``symbol``; True if every stream restored."""
        saved = load_states(conn, symbol, session)
        restored = 0
        for key, det in self.detectors[symbol].items():
            state = saved.get(key)
            if state and det.restore(state):
                self._saved[(symbol, key)] = json.dumps(state, sort_keys=True)
                restored += 1
        if restored == len(self.detectors[symbol]):
            logger.info("Wall alerts: restored %s state from %s", symbol, session.isoformat())
            return True
        # Partial restores would leave the streams stepping different tapes;
        # start them all fresh from the warm-up window instead.
        for key, prof in self.streams.items():
            self.detectors[symbol][key] = WallAlertDetector(symbol, prof)
            self._saved.pop((symbol, key), None)
        return False

    def acquire_lock(self) -> bool:
        """Take (or confirm) the single-instance advisory lock."""
        from src.database.connection import close_db_connection, get_db_connection

        if self._lock_conn is not None and not getattr(self._lock_conn, "closed", 1):
            return True
        if self._lock_conn is not None:
            try:
                close_db_connection(self._lock_conn)
            except Exception:  # noqa: BLE001
                pass
            self._lock_conn = None
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got = bool(cur.fetchone()[0])
        if not got:
            conn.autocommit = False
            close_db_connection(conn)
            return False
        self._lock_conn = conn
        return True

    def release_lock(self) -> None:
        from src.database.connection import close_db_connection

        conn = self._lock_conn
        self._lock_conn = None
        if conn is None or getattr(conn, "closed", 1):
            return
        try:
            cur = conn.cursor()
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
            conn.autocommit = False
        except Exception:  # noqa: BLE001
            logger.debug("advisory unlock failed", exc_info=True)
        finally:
            close_db_connection(conn)

    # ----------------------------------------------------------------- tick

    def tick(self, now: Optional[datetime] = None) -> TickSummary:
        """One pass over every symbol. Returns what happened, for logging.

        Symbols are independent: one failing (bad data, a lock timeout) is
        logged and retried next tick without starving the others. Only when
        every symbol fails — the database itself is down — does the error
        propagate, so the loop backs off.
        """
        from src.database.connection import db_connection
        from src.market_calendar import is_trading_session

        now = now or datetime.now(tz=timezone.utc)
        summary = TickSummary()
        session = session_date_for(now)
        if not is_trading_session(session):
            return summary
        open_ts, close_ts = session_bounds(session)
        # Keep stepping briefly after the bell so the final minute completes.
        if now < open_ts or now >= close_ts + timedelta(minutes=2):
            return summary
        if session != self.session:
            self._start_session(session)

        failures: List[BaseException] = []
        with db_connection() as conn:
            for symbol in self.symbols:
                try:
                    self._tick_symbol(conn, symbol, session, open_ts, close_ts, now, summary)
                    conn.commit()
                    self._symbol_errors.pop(symbol, None)
                    summary.symbols += 1
                except Exception as exc:  # noqa: BLE001 — isolate symbols
                    conn.rollback()
                    failures.append(exc)
                    count = self._symbol_errors.get(symbol, 0) + 1
                    self._symbol_errors[symbol] = count
                    if count == 1:
                        logger.error("Wall alerts: %s tick failed: %s", symbol, exc, exc_info=True)
                    elif count % 60 == 0:
                        logger.warning(
                            "Wall alerts: %s still failing (#%d): %s", symbol, count, exc
                        )
            if failures and len(failures) == len(self.symbols):
                raise failures[-1]
            self._maybe_prune(conn, session, now, close_ts)
        return summary

    def _tick_symbol(
        self,
        conn: Any,
        symbol: str,
        session: date,
        open_ts: datetime,
        close_ts: datetime,
        now: datetime,
        summary: TickSummary,
    ) -> None:
        dets = self.detectors[symbol]
        if not self._restored.get(symbol):
            self._restore(conn, symbol, session)
            self._restored[symbol] = True

        done = [d.last_bar_ts for d in dets.values() if d.last_bar_ts is not None]
        if done:
            since = min(done) + timedelta(minutes=1)
        else:
            since = max(open_ts, now - timedelta(minutes=alerts_config.WARMUP_MINUTES))
            since = since.replace(second=0, microsecond=0)
        bars = load_bars(conn, symbol, since, min(now, close_ts))
        if not bars:
            # Normal for the first seconds of a minute, before its first print.
            # Only a gap longer than the staleness bound is a stalled feed.
            last = max(done) if done else None
            if (
                last is not None
                and (now - last).total_seconds() - 60.0 > alerts_config.MAX_BAR_AGE_SECONDS
            ):
                self._skip(symbol, "tape stale", summary)
            return

        walls_rows = load_walls(conn, symbol, since - timedelta(minutes=1), now)
        latest = walls_rows[-1] if walls_rows else None
        walls_age = (
            (now - latest.walls.ts).total_seconds()
            if latest is not None and latest.walls.ts is not None
            else None
        )
        if latest is None or walls_age is None or walls_age > alerts_config.MAX_WALLS_AGE_SECONDS:
            # Without current walls nothing can be judged. The bars stay
            # unapplied and are stepped (delivery suppressed by age) once the
            # walls are fresh again, so no test is lost across the gap.
            self._skip(symbol, "walls stale", summary)
            return
        self._resume(symbol)

        # Detector state and the events it emits commit together: if recording
        # an event fails, the detectors are rewound to where this tick began
        # and the next tick re-emits it (the event key makes the replay of any
        # event that DID commit a no-op).
        checkpoint = {name: det.to_state() for name, det in dets.items()}
        try:
            for tape in bars:
                bar = tape.bar
                complete = (now - bar.ts).total_seconds() >= 60.0 + BAR_SETTLE_SECONDS
                row = walls_at(walls_rows, bar.ts) or walls_rows[0]
                for det in dets.values():
                    for event in det.observe(bar, row.walls, complete=complete):
                        self._handle_event(conn, event, latest, now, summary)
            self._save(conn, symbol, session)
        except Exception:
            for name, det in list(dets.items()):
                if not det.restore(checkpoint[name]):
                    # A detector that had seen nothing yet has no session to
                    # restore; rewinding it means starting it fresh.
                    dets[name] = WallAlertDetector(symbol, self.streams[name])
            # A failed save left _saved claiming a state the database does not
            # have; forget it so the next tick writes again.
            for name in dets:
                self._saved.pop((symbol, name), None)
            raise

    def _skip(self, symbol: str, reason: str, summary: TickSummary) -> None:
        summary.skipped[symbol] = reason
        if self._skip_reason.get(symbol) != reason:
            logger.warning("Wall alerts: skipping %s — %s", symbol, reason)
        self._skip_reason[symbol] = reason

    def _resume(self, symbol: str) -> None:
        reason = self._skip_reason.pop(symbol, None)
        if reason is not None:
            logger.info("Wall alerts: %s resumed (was: %s)", symbol, reason)

    def _handle_event(
        self, conn: Any, event: WallEvent, latest: WallsRow, now: datetime, summary: TickSummary
    ) -> None:
        bar_end = event.bar_ts + timedelta(minutes=1)
        # A hit on the minute still forming is seconds old even though its
        # minute started up to 60s ago; measure age from the minute's end,
        # floored at zero for that case.
        age = max((now - bar_end).total_seconds(), 0.0)
        if age > alerts_config.MAX_EVENT_AGE_SECONDS:
            summary.suppressed_late += 1
            logger.info(
                "Wall alerts: not delivering late %s %s %s %.2f (%ds old)",
                event.symbol,
                event.side,
                event.kind,
                event.wall,
                int(age),
            )
            return
        pctile = self._pctile(conn, event)
        odds = self._odds(conn, event.symbol, event.side)
        result = record_and_fan_out(
            conn,
            event,
            strength_pctile=pctile,
            net_gex_at_spot=latest.net_gex_at_spot,
            current_wall=latest.walls.wall(event.side),
            odds=odds,
            now=now,
        )
        conn.commit()
        if result.duplicate:
            return
        summary.events += 1
        summary.delivered += result.deliveries
        logger.info(
            "Wall alert %s %s %s wall %.2f (%s %dbp, test %d) price %.2f strength %s"
            " → %d rule(s), %d delivery row(s)%s",
            event.symbol,
            event.side,
            event.kind,
            event.wall,
            event.profile,
            event.touch_bp,
            event.ordinal,
            event.price,
            "n/a" if pctile is None else f"p{pctile:.0f}",
            result.rules_matched,
            result.deliveries,
            (
                f", {result.filtered_by_strength} below strength floor"
                if result.filtered_by_strength
                else ""
            ),
        )

    def _pctile(self, conn: Any, event: WallEvent) -> Optional[float]:
        from src.tradeworkz.gex_stats import fetch_wall_strength_pctile, tod_bucket_for

        if event.strength is None:
            return None
        ts = event.walls_ts or event.hit_ts
        key = (event.symbol, event.side, int(event.strength), tod_bucket_for(ts))
        if key not in self._pctile_cache:
            self._pctile_cache[key] = fetch_wall_strength_pctile(
                conn, event.symbol, event.side, event.strength, ts
            )
        return self._pctile_cache[key]

    def _odds(self, conn: Any, symbol: str, side: str) -> List[Dict[str, Any]]:
        key = (symbol, side)
        cached = self._odds_cache.get(key)
        mono = time.monotonic()
        if cached is not None and mono - cached[0] < _ODDS_TTL_SECONDS:
            return cached[1]
        rows = load_odds(conn, symbol, side)
        self._odds_cache[key] = (mono, rows)
        return rows

    def _save(self, conn: Any, symbol: str, session: date) -> None:
        changed: Dict[StreamKey, Dict[str, Any]] = {}
        for name, det in self.detectors[symbol].items():
            state = det.to_state()
            blob = json.dumps(state, sort_keys=True)
            if self._saved.get((symbol, name)) != blob:
                changed[name] = state
                self._saved[(symbol, name)] = blob
        if changed:
            save_states(conn, symbol, session, changed)

    def _maybe_prune(self, conn: Any, session: date, now: datetime, close_ts: datetime) -> None:
        if self._last_prune == session or now < close_ts:
            return
        self._last_prune = session
        try:
            removed = prune(conn, alerts_config.RETENTION_DAYS)
            conn.commit()
            if removed:
                logger.info("Wall alerts: pruned %d event(s) past retention", removed)
        except Exception:  # noqa: BLE001 — housekeeping must not fail a tick
            conn.rollback()
            logger.warning("Wall alerts: prune failed", exc_info=True)

    # ----------------------------------------------------------------- loop

    def _signal_handler(self, signum, frame):  # noqa: ARG002 — signal API
        logger.info("Wall alerts received signal %s — stopping", signum)
        self.running = False

    def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + max(0.0, seconds)
        while self.running:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(_SHUTDOWN_POLL_SECONDS, remaining))

    def run(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.running = True
        logger.info(
            "Starting wall-alert service (symbols=%s poll=%ss enabled=%s)",
            ",".join(self.symbols),
            alerts_config.POLL_SECONDS,
            alerts_config.ENABLED,
        )
        failures = 0
        lock_warned = False
        while self.running:
            if not alerts_config.ENABLED:
                self._sleep(60.0)
                continue
            started = time.monotonic()
            try:
                if not self.acquire_lock():
                    if not lock_warned:
                        logger.warning("Wall alerts: another instance holds the lock; standing by")
                        lock_warned = True
                    self._sleep(30.0)
                    continue
                if lock_warned:
                    logger.info("Wall alerts: lock acquired; this instance is active")
                    lock_warned = False
                summary = self.tick()
                if summary.events or summary.suppressed_late:
                    logger.debug("Wall alerts tick: %s", summary)
                if failures:
                    logger.info("Wall alerts recovered after %d failed tick(s)", failures)
                failures = 0
            except Exception as exc:  # noqa: BLE001 — one tick must not kill the loop
                failures += 1
                if failures == 1:
                    logger.error("Wall alerts tick failed: %s", exc, exc_info=True)
                else:
                    logger.warning("Wall alerts tick still failing (#%d): %s", failures, exc)
                # A failure may have been the lock connection dying; drop it so
                # the next tick re-acquires on a fresh connection.
                self.release_lock()
            elapsed = time.monotonic() - started
            if failures:
                base = min(
                    alerts_config.POLL_SECONDS * (2 ** (failures - 1)), _FAILURE_BACKOFF_MAX_SECONDS
                )
                pause = base + random.uniform(0, base * 0.1)
            else:
                pause = max(0.5, alerts_config.POLL_SECONDS - elapsed)
            self._sleep(pause)
        self.release_lock()
        logger.info("Wall-alert service stopped")


# ---------------------------------------------------------------------------
# Offline replay
# ---------------------------------------------------------------------------


@dataclass
class ReplayStream:
    """What one stream did over a replayed session."""

    profile: str
    touch_bp: int
    events: List[WallEvent]
    trace: List[TraceEntry] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.profile} · touch {self.touch_bp / 100:.2f}%"


def replay(
    symbol: str,
    session: date,
    profiles: Optional[List[str]] = None,
    *,
    touch_bps: Optional[List[int]] = None,
    publish_lag_minutes: float = 0.0,
    explain: bool = False,
) -> List[ReplayStream]:
    """Run the detectors over one stored session. Read-only.

    Each profile runs at its own touch band, or at each of ``touch_bps`` when
    given. With ``explain`` every stream also records why it did what it did.

    Each bar is judged against the walls row stamped at or before it. Live,
    that row can still be the previous minute's for the first seconds of a
    minute (the analytics cycle has not reached it yet); walls change at most
    every few minutes, so the difference is rare, and ``publish_lag_minutes``
    exists to measure it when it matters.
    """
    from src.database.connection import db_connection

    names = profiles or list(PROFILES)
    open_ts, close_ts = session_bounds(session)
    with db_connection() as conn:
        bars = load_bars(conn, symbol, open_ts, close_ts)
        walls_rows = load_walls(conn, symbol, open_ts - timedelta(minutes=30), close_ts)
    out: List[ReplayStream] = []
    for name in names:
        base = PROFILES[name]
        variants = [base] if not touch_bps else [base.with_touch_bp(bp) for bp in touch_bps]
        for prof in variants:
            det = WallAlertDetector(symbol, prof)
            if explain:
                det.trace = []
            events: List[WallEvent] = []
            for tape in bars:
                row = walls_at(walls_rows, tape.bar.ts - timedelta(minutes=publish_lag_minutes))
                if row is None:
                    continue
                events.extend(det.observe(tape.bar, row.walls, complete=True))
            det.flush_trace()
            trace = sorted(det.trace or [], key=lambda t: t.ts)
            out.append(ReplayStream(prof.name, prof.touch_bp, events, trace))
    return out


def _hhmm(ts: datetime) -> str:
    return ts.astimezone(ET).strftime("%H:%M")


def _print_replay(
    symbol: str,
    session: date,
    results: List[ReplayStream],
    as_json: bool,
    explain: bool = False,
) -> None:
    if as_json:
        out: Dict[str, Any] = {}
        for stream in results:
            entry: Dict[str, Any] = {
                "profile": stream.profile,
                "touch_bp": stream.touch_bp,
                "alerts": [
                    {
                        "time": _hhmm(e.bar_ts),
                        "side": e.side,
                        "kind": e.kind,
                        "wall": e.wall,
                        "price": e.price,
                        "test": e.ordinal,
                        "alerted_hit": e.alerted_hit,
                    }
                    for e in stream.events
                ],
            }
            if explain:
                entry["explain"] = [
                    {"time": _hhmm(t.ts), "side": t.side, "wall": t.wall, "text": t.text}
                    for t in stream.trace
                ]
            out[stream.label] = entry
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return
    print(f"Wall-alert replay — {symbol} {session.isoformat()}")
    for stream in results:
        events = stream.events
        counts = {k: sum(1 for e in events if e.kind == k) for k in ("hit", "break", "hold")}
        print(
            f"\n[{stream.label}] {len(events)} alert(s): "
            f"{counts['hit']} hit · {counts['break']} broke · {counts['hold']} held"
        )
        if explain:
            if not stream.trace:
                print("  (no walls published for this session)")
            for t in stream.trace:
                print(f"  {_hhmm(t.ts)}  {t.side:4}  {t.wall:>10.2f}  {t.text}")
            continue
        for e in events:
            extra = ""
            if e.kind != "hit":
                extra = f"  ({e.minutes_since_hit:.0f} min after test opened)"
            print(
                f"  {_hhmm(e.bar_ts)}  {e.side:4}  {e.kind:5}  "
                f"wall {e.wall:>10.2f}  price {e.price:>10.2f}  test #{e.ordinal}{extra}"
            )


def _parse_touch(raw: Optional[List[str]]) -> Optional[List[int]]:
    """``--touch 0.10`` (percent of the wall, repeatable or comma-separated) → bp."""
    if not raw:
        return None
    out: List[int] = []
    for chunk in raw:
        for part in chunk.split(","):
            part = part.strip().rstrip("%")
            if not part:
                continue
            bp = int(round(float(part) * 100))
            if not 1 <= bp <= 100:
                raise argparse.ArgumentTypeError(
                    f"--touch {part}: expected a percent between 0.01 and 1.00"
                )
            if bp not in out:
                out.append(bp)
    return out or None


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="ZeroGEX wall-alert service")
    parser.add_argument("--once", action="store_true", help="Run one tick and exit.")
    parser.add_argument("--replay", metavar="SYMBOL", help="Replay a stored session offline.")
    parser.add_argument("--date", help="Session date for --replay (YYYY-MM-DD, default today).")
    parser.add_argument("--profile", action="append", choices=sorted(PROFILES))
    parser.add_argument(
        "--touch",
        action="append",
        metavar="PCT",
        help="--replay at this touch tolerance (percent of the wall, e.g. 0.10) instead of "
        "each profile's own band. Repeatable or comma-separated.",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="--replay: print every decision (arming, tests and why each hit did or did not "
        "alert, near misses, outcomes).",
    )
    parser.add_argument("--json", action="store_true", help="--replay output as JSON.")
    args = parser.parse_args(argv)

    if args.replay:
        session = date.fromisoformat(args.date) if args.date else datetime.now(ET).date()
        symbol = args.replay.upper()
        try:
            touch_bps = _parse_touch(args.touch)
        except (argparse.ArgumentTypeError, ValueError) as exc:
            parser.error(str(exc))
        results = replay(symbol, session, args.profile, touch_bps=touch_bps, explain=args.explain)
        _print_replay(symbol, session, results, args.json, args.explain)
        return 0

    service = WallAlertService()
    if args.once:
        if not service.acquire_lock():
            print("Another wall-alert instance holds the lock; not running.", file=sys.stderr)
            return 1
        try:
            summary = service.tick()
        finally:
            service.release_lock()
        print(summary)
        return 0
    service.run()
    return 0


__all__ = ["WallAlertService", "replay", "main", "strength_tier"]


if __name__ == "__main__":
    sys.exit(main())
