"""Alert when a configured underlying stops writing bars during its session.

This is the detector the 2026-08-17 QQQ outage did not have. One ingestion
worker (a child process of ``zerogex-oa-ingestion``, one per symbol) died at
the Monday session close and was never restarted, so QQQ wrote nothing from
Monday 20:00 ET until an operator noticed by eye and restarted the unit at
Tuesday 10:03 ET -- roughly six hours into a session it should have been
streaming. Nothing caught it: the parent process was still alive, so systemd
reported the unit ``active``, ``Restart=always`` never triggered, ``OnFailure``
never dispatched, and the per-minute liveness watchdog -- which only runs
``systemctl is-active`` -- stayed green the whole time.

Every one of those checks asks "is the process up". None asks "is the data
arriving". This one does, per symbol, which is the only question whose answer
would have caught a single dead worker among healthy siblings.

Where freshness is asserted
---------------------------
Only inside the regular cash session (09:30-16:00 ET), where a 1-minute bar
feed really is dense and a multi-minute hole is a genuine fault.

Extended hours are reported ``not_checked`` and never alert, because the
vendor's own extended-hours delivery is discontinuous -- not because a worker
is down. Two measurements from 2026-08-18 alone:

* QQQ served **no bars at all from 04:00 to 07:35 ET** (a 3h35m hole), and
  ``probe_bar_availability`` confirmed the same 07:35 first bar under every
  session template and both query modes -- a boundary in the vendor's store,
  not in our request;
* SPY *and* QQQ both stopped at 16:14 ET, the same minute, ~14 minutes past
  the cash close, while their workers were alive and supervised.

A fixed threshold cannot separate that from a dead worker, so applying one
across 04:00-20:00 produces a false page every quiet stretch -- which is how
an alert gets muted and stops being a detector at all. What it would buy is
small: a worker that dies pre-market is still dead at 09:30 and gets caught
then. Set ``--extended-max-stale-minutes`` (or
``INGEST_FRESHNESS_EXTENDED_MAX_STALE_MINUTES``) to opt a deployment back in
if its feed is denser than ours.

Staleness is anchored at ``max(last_bar, regular_open)`` so the first run
after the open measures from the open rather than from the pre-market's last
sparse print -- otherwise every session would start with a spurious gap.
Symbols outside their delivery window entirely are reported ``not_expected``:
cash indices (SPX, NDX) print an underlying level only 09:30-16:00 ET even
under a 24-hour template, which ``feed_session_window`` already encodes.

Paging once per episode
-----------------------
The unit runs every 10 minutes and pages via ``OnFailure=``, so a nonzero
exit on every run means one email per 10 minutes for as long as the condition
lasts -- and the condition this detector exists to catch (a dead worker)
lasts until a human acts. The original QQQ outage would have sent ~36.

With ``--state-file`` the tool pages on the edge into stale and then at most
once per ``--renotify-minutes``, the same edge-triggered contract
``zerogex-liveness-watch.sh`` already uses for the same reason. Between
pages the exit code is 0 while the ERROR lines still name every stale symbol
on every run, so ``journalctl -u zerogex-oa-ingestion-freshness`` stays the
truthful live view even though ``systemctl --failed`` has gone quiet.
Without ``--state-file`` (a human running ``make`` by hand) the exit code is
level-triggered as before and no state is touched, so an ad-hoc run neither
consumes the alert budget nor suppresses the next page.

Usage:
    python -m src.tools.ingestion_freshness_healthcheck
    python -m src.tools.ingestion_freshness_healthcheck --json
    python -m src.tools.ingestion_freshness_healthcheck --max-stale-minutes 10

Exit codes:
    0 -- every checked symbol is fresh (or none is being checked), or a
         known failure is inside its re-notify quiet period.
    1 -- at least one symbol is stale and due to page.
    2 -- database connection or query error, and due to page.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import pytz

from src.market_calendar import feed_session_window, underlying_feed_expected
from src.symbols import get_canonical_symbol, parse_underlyings

logger = logging.getLogger(__name__)

ET = pytz.timezone("US/Eastern")

# The regular cash session, and the only window in which a 1-minute bar feed
# is dense enough for "no bar for N minutes" to mean anything. See the module
# docstring for the extended-hours measurements this is drawn from.
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)

# A 1-minute bar feed is legitimately silent up to ~60s between bars, and even
# in the cash session a thin minute can skip. 15 minutes is comfortably above
# that noise floor and still an order of magnitude tighter than the ~6 hours
# the QQQ outage ran.
DEFAULT_MAX_STALE_MINUTES = 15

# How long a stale symbol stays paged-for before it pages again. Long enough
# that a sustained outage does not flood an inbox, short enough that one
# ignored email is not the only one anybody ever gets.
DEFAULT_RENOTIFY_MINUTES = 60

# Alert key used for the "could not reach the database" failure, so an RDS
# blip is deduped on the same schedule as a stale symbol rather than paging
# every 10 minutes on its own path.
DB_ALERT_KEY = "database"


@dataclass(frozen=True)
class SymbolFreshness:
    symbol: str
    # "fresh" | "stale" | "not_expected" (outside the feed's delivery window
    # entirely) | "not_checked" (inside it, but extended hours, where the
    # vendor's delivery is too discontinuous to hold to a deadline)
    status: str
    last_bar: Optional[str]
    stale_minutes: Optional[float]


def configured_symbols() -> List[str]:
    """Canonical DB symbols for the underlyings the ingestion unit runs.

    Mirrors ``main_engine.main``: ``INGEST_UNDERLYINGS`` (falling back to
    ``INGEST_UNDERLYING``) resolved through ``SYMBOL_ALIASES``, then reversed
    to the canonical symbol the engine actually writes rows under -- the
    engine queries TradeStation for ``$SPXW.X`` but stores ``SPX``.
    """
    raw = os.getenv("INGEST_UNDERLYINGS") or os.getenv("INGEST_UNDERLYING") or "SPY"
    return [get_canonical_symbol(symbol) for symbol in parse_underlyings(raw)]


def evaluate(
    symbol: str,
    last_bar: Optional[datetime],
    now_et: datetime,
    session_template: str,
    max_stale: timedelta,
    extended_max_stale: Optional[timedelta] = None,
) -> SymbolFreshness:
    """Classify one symbol's feed. Pure -- no DB, no clock.

    ``max_stale`` applies inside the regular cash session.
    ``extended_max_stale`` applies in the pre/after-hours parts of the feed's
    delivery window; ``None`` (the default) means those hours are not checked
    at all -- see the module docstring for why a fixed threshold there pages
    on the vendor's own gaps rather than on a fault.
    """
    if not underlying_feed_expected(now_et, session_template, symbol):
        return SymbolFreshness(symbol, "not_expected", _iso(last_bar), None)

    feed_open, _feed_close = feed_session_window(session_template, symbol)
    in_regular_session = REGULAR_OPEN <= now_et.time() <= REGULAR_CLOSE

    if in_regular_session:
        # Anchor at the regular open, not the feed's 04:00 open: that is where
        # bar density actually changes, so it is the point from which a gap
        # becomes evidence. Without it the first checks after 09:30 measure
        # against whatever sparse pre-market print came last.
        threshold = max_stale
        window_open = max(feed_open, REGULAR_OPEN)
    else:
        if extended_max_stale is None:
            return SymbolFreshness(symbol, "not_checked", _iso(last_bar), None)
        threshold = extended_max_stale
        window_open = feed_open

    anchor = ET.localize(datetime.combine(now_et.date(), window_open))
    if last_bar is not None:
        last_bar_et = last_bar.astimezone(ET)
        if last_bar_et > anchor:
            anchor = last_bar_et

    stale = now_et - anchor
    stale_minutes = round(stale.total_seconds() / 60.0, 1)
    status = "stale" if stale > threshold else "fresh"
    return SymbolFreshness(symbol, status, _iso(last_bar), stale_minutes)


def reconcile_alerts(
    state: Dict[str, dict],
    active_keys: List[str],
    now_et: datetime,
    renotify: timedelta,
) -> Tuple[Dict[str, dict], List[str], List[str]]:
    """Decide which of ``active_keys`` should page. Pure -- no I/O, no clock.

    Returns ``(next_state, to_alert, recovered)``. A key pages on the edge
    into a bad state and then at most once per ``renotify``; the rest of the
    time it is carried in the state untouched, so ``since`` keeps recording
    when the episode actually began. Keys present in ``state`` but no longer
    active have recovered and are dropped.
    """
    stamp = now_et.isoformat()
    next_state: Dict[str, dict] = {}
    to_alert: List[str] = []

    for key in active_keys:
        previous = state.get(key)
        if previous is None:
            next_state[key] = {"since": stamp, "last_alert": stamp}
            to_alert.append(key)
            continue

        last_alert = _parse_iso(previous.get("last_alert"))
        if last_alert is None or now_et - last_alert >= renotify:
            next_state[key] = {"since": previous.get("since", stamp), "last_alert": stamp}
            to_alert.append(key)
        else:
            next_state[key] = previous

    active = set(active_keys)
    recovered = [key for key in state if key not in active]
    return next_state, to_alert, recovered


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse a stored ET timestamp, tolerating a hand-edited/truncated file."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else ET.localize(parsed)


def _load_state(path: str) -> Dict[str, dict]:
    """Read the alert state. A missing/corrupt file means "nothing is known",
    which pages on the next stale run -- fail toward alerting, never away."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        logger.warning("Freshness alert state at %s unreadable (%s) — starting fresh", path, exc)
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {key: value for key, value in loaded.items() if isinstance(value, dict)}


def _save_state(path: str, state: Dict[str, dict]) -> None:
    """Persist the alert state atomically. Best-effort: a state file we cannot
    write degrades to level-triggered paging, which is noisy but never silent."""
    directory = os.path.dirname(path) or "."
    try:
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".freshness-state-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2, sort_keys=True)
            os.replace(tmp, path)
        except Exception:
            os.unlink(tmp)
            raise
    except OSError as exc:
        logger.warning("Could not write freshness alert state to %s: %s", path, exc)


def _apply_dedupe(
    state_path: Optional[str],
    active_keys: List[str],
    now_et: datetime,
    renotify: timedelta,
) -> bool:
    """Return True when this run should exit nonzero (i.e. page).

    Without a state file the answer is simply "is anything wrong now", which
    is what a human running the check by hand wants.
    """
    if not state_path:
        return bool(active_keys)

    state = _load_state(state_path)
    next_state, to_alert, recovered = reconcile_alerts(state, active_keys, now_et, renotify)
    _save_state(state_path, next_state)

    for key in recovered:
        logger.info("%s: recovered — no longer stale", key)

    suppressed = [key for key in active_keys if key not in to_alert]
    for key in suppressed:
        since = next_state.get(key, {}).get("since", "?")
        logger.error(
            "%s: still failing (since %s) — already paged, so this run does not "
            "alert again. Next page in at most %.0f min; the ERROR lines above "
            "are the live state on every run.",
            key,
            since,
            renotify.total_seconds() / 60.0,
        )

    return bool(to_alert)


def main(argv: Optional[List[str]] = None) -> int:
    from dotenv import load_dotenv

    # Before the parser is built: the argument defaults below read the
    # environment as they are constructed, so loading .env afterwards left
    # every INGEST_FRESHNESS_* value in it invisible to a hand run (the
    # systemd unit only worked because EnvironmentFile= puts them in the real
    # environment). Explicit CLI arguments still win over both.
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Alert when a configured underlying stops writing bars mid-session."
    )
    parser.add_argument(
        "--max-stale-minutes",
        type=float,
        default=float(os.getenv("INGEST_FRESHNESS_MAX_STALE_MINUTES", DEFAULT_MAX_STALE_MINUTES)),
        help=f"Regular-session staleness threshold (default: {DEFAULT_MAX_STALE_MINUTES})",
    )
    parser.add_argument(
        "--extended-max-stale-minutes",
        type=float,
        default=float(os.getenv("INGEST_FRESHNESS_EXTENDED_MAX_STALE_MINUTES", 0) or 0),
        help=(
            "Extended-hours staleness threshold in minutes. 0 (the default) "
            "does not check extended hours at all — the vendor's own delivery "
            "there is discontinuous, so a threshold pages on its gaps."
        ),
    )
    parser.add_argument(
        "--state-file",
        default=os.getenv("INGEST_FRESHNESS_STATE_FILE") or None,
        help=(
            "Path for edge-triggered alert state. Given, a stale symbol pages "
            "on its first run and then at most once per --renotify-minutes. "
            "Omitted, every stale run exits nonzero and no state is touched."
        ),
    )
    parser.add_argument(
        "--renotify-minutes",
        type=float,
        default=float(os.getenv("INGEST_FRESHNESS_RENOTIFY_MINUTES", DEFAULT_RENOTIFY_MINUTES)),
        help=f"Re-page interval while stale (default: {DEFAULT_RENOTIFY_MINUTES})",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated canonical symbols to check (default: INGEST_UNDERLYINGS)",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of log lines")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.symbols:
        symbols = [
            get_canonical_symbol(s.strip().upper()) for s in args.symbols.split(",") if s.strip()
        ]
    else:
        symbols = configured_symbols()

    if not symbols:
        logger.error("No underlyings configured (INGEST_UNDERLYINGS / INGEST_UNDERLYING)")
        return 2

    session_template = os.getenv("SESSION_TEMPLATE", "Default")
    max_stale = timedelta(minutes=args.max_stale_minutes)
    extended_max_stale = (
        timedelta(minutes=args.extended_max_stale_minutes)
        if args.extended_max_stale_minutes > 0
        else None
    )
    renotify = timedelta(minutes=args.renotify_minutes)
    now_et = datetime.now(ET)

    try:
        last_bars = _fetch_last_bars(symbols)
    except Exception as exc:  # pragma: no cover - needs a live DB to exercise
        logger.error("Freshness check could not query underlying_quotes: %s", exc, exc_info=True)
        # Deduped like a stale symbol: a database outage lasts as long as it
        # lasts, and paging every 10 minutes for it helps nobody. Inside the
        # quiet period this exits 0 with the ERROR above still in the journal,
        # the same trade the stale-symbol path makes.
        return 2 if _apply_dedupe(args.state_file, [DB_ALERT_KEY], now_et, renotify) else 0

    results = [
        evaluate(
            symbol,
            last_bars.get(symbol),
            now_et,
            session_template,
            max_stale,
            extended_max_stale,
        )
        for symbol in symbols
    ]
    stale = [r for r in results if r.status == "stale"]

    if args.json:
        print(
            json.dumps(
                {
                    "checked_at": now_et.isoformat(),
                    "session_template": session_template,
                    "max_stale_minutes": args.max_stale_minutes,
                    "extended_max_stale_minutes": args.extended_max_stale_minutes or None,
                    "symbols": [asdict(r) for r in results],
                },
                indent=2,
            )
        )
    else:
        for r in results:
            if r.status == "not_expected":
                logger.info("%s: outside its delivery window — not checked", r.symbol)
            elif r.status == "not_checked":
                logger.info(
                    "%s: extended hours — freshness not asserted (the feed's own "
                    "delivery is discontinuous here; set "
                    "INGEST_FRESHNESS_EXTENDED_MAX_STALE_MINUTES to check anyway)",
                    r.symbol,
                )
            elif r.status == "fresh":
                logger.info("%s: fresh (%.1f min since last bar)", r.symbol, r.stale_minutes or 0.0)
            else:
                logger.error(
                    "%s: STALE — no bar for %.1f min (threshold %.1f). Last bar: %s. "
                    "The per-symbol ingestion worker for %s is likely dead or stalled; "
                    "check `systemctl status zerogex-oa-ingestion` and the child PIDs.",
                    r.symbol,
                    r.stale_minutes or 0.0,
                    args.max_stale_minutes,
                    r.last_bar or "none in the last 7 days",
                    r.symbol,
                )

    if stale:
        logger.error(
            "Ingestion freshness check FAILED for %d of %d symbol(s): %s",
            len(stale),
            len(results),
            ", ".join(r.symbol for r in stale),
        )

    return 1 if _apply_dedupe(args.state_file, [r.symbol for r in stale], now_et, renotify) else 0


def _fetch_last_bars(symbols: List[str]) -> dict:
    """Return ``{symbol: last_timestamp_or_None}`` for the given symbols."""
    from src.database.connection import db_connection

    out = {symbol: None for symbol in symbols}
    if not symbols:
        return out
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT symbol, MAX(timestamp)
                FROM underlying_quotes
                WHERE symbol = ANY(%s)
                  AND timestamp > NOW() - INTERVAL '7 days'
                GROUP BY symbol
                """,
                (symbols,),
            )
            for symbol, last_ts in cursor.fetchall():
                out[symbol] = last_ts
    return out


if __name__ == "__main__":
    sys.exit(main())
