#!/usr/bin/env python3
"""Tail the real-time quote NOTIFY bus (`zgx_quote_updates`) in arrival order.

This is the "source of truth" view of the streaming quote path: ingestion
publishes every underlying tick via ``pg_notify('zgx_quote_updates', <json>)``
inside the same transaction as the ``underlying_quotes`` upsert (see
``src/ingestion/main_engine.py::_publish_quote_notify``), and Postgres
delivers it to every LISTEN-ing connection on COMMIT. That is the exact
stream the API's ``QuoteBroadcaster`` fans out to browsers — so watching it
here shows the *commit order* the frontend should ultimately render in.

Use it to validate the candlestick out-of-order fix end to end:

    * This script shows the DB/commit order (no ``server_ts`` — that stamp is
      added later, per-frame, by the API worker's fan-out in
      ``quote_broadcaster.py::_fanout``).
    * Chrome DevTools -> Network -> WS -> Messages shows what the browser
      actually receives, WITH ``server_ts``. Compare the two: any frame whose
      ``server_ts`` steps backwards relative to the previous one is a reordered
      frame — exactly what ``applyLiveQuote``'s guard now drops instead of
      letting it rewind the tip candle.

Connection uses the same credentials the app does (``src.config`` +
``get_db_password``), so it honours DB_HOST/DB_PORT/DB_NAME/DB_USER, the
``DB_PASSWORD_PROVIDER`` (pgpass / env / secretsmanager), and DB_SSLMODE.

Run from the repo root:

    python bin/watch_quote_notify.py                 # all symbols
    python bin/watch_quote_notify.py --symbol SPY     # one symbol
    python bin/watch_quote_notify.py --raw            # also dump raw JSON

Notes:
    * LISTEN receives only ticks that commit AFTER this connects — it does not
      replay history. Start it, then watch the tape roll in.
    * Every LISTEN-ing connection (each API worker, plus this script) receives
      every NOTIFY independently; running this does not steal ticks from the
      API.
    * Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Allow ``import src.*`` when invoked as ``python bin/watch_quote_notify.py``
# from anywhere — mirror the repo-root-on-path assumption the app modules make.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    import asyncpg
except ImportError:  # pragma: no cover - dependency is declared in pyproject
    print(
        "asyncpg is required (it is a project dependency). Run inside the "
        "project venv, e.g. `uv run bin/watch_quote_notify.py`.",
        file=sys.stderr,
    )
    raise

NOTIFY_CHANNEL = "zgx_quote_updates"

# ANSI colours — disabled automatically when stdout is not a TTY (piped to a
# file or a pager) so the log stays clean.
_TTY = sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _TTY else text


def _resolve_connect_kwargs() -> Dict[str, Any]:
    """Build asyncpg connect kwargs the same way the API pool does.

    Falls back to raw env defaults if the project modules can't be imported
    (e.g. run outside the venv), so the script degrades gracefully.
    """
    try:
        from src.config import DB_HOST, DB_NAME, DB_PORT, DB_USER
        from src.database.password_providers import get_db_password

        password = get_db_password()  # may be None for the .pgpass provider
        host, port, database, user = DB_HOST, DB_PORT, DB_NAME, DB_USER
    except Exception as exc:  # pragma: no cover - defensive fallback
        print(_c("33", f"[warn] falling back to raw DB_* env ({exc})"), file=sys.stderr)
        host = os.getenv("DB_HOST", "localhost")
        port = int(os.getenv("DB_PORT", "5432"))
        database = os.getenv("DB_NAME", "zerogex")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD") or None

    kwargs: Dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }
    ssl_mode = os.getenv("DB_SSLMODE", "").strip().lower()
    if ssl_mode in {"require", "verify-ca", "verify-full"}:
        kwargs["ssl"] = True
    return kwargs


def _fmt_num(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


class Tape:
    """Formats notifications and tracks per-symbol continuity for anomalies."""

    def __init__(self, symbol_filter: Optional[str], raw: bool) -> None:
        self.symbol_filter = symbol_filter.upper() if symbol_filter else None
        self.raw = raw
        self.count = 0
        self._last_close: Dict[str, float] = {}
        self._last_bucket: Dict[str, str] = {}
        self._last_arrival: Dict[str, float] = {}

    def on_notify(
        self,
        _conn: "asyncpg.Connection",
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        recv = datetime.now(timezone.utc)
        try:
            data: Dict[str, Any] = json.loads(payload)
        except (ValueError, TypeError):
            print(_c("31", f"[malformed payload, {len(payload)}B] {payload[:120]!r}"))
            return

        symbol = str(data.get("symbol", "?")).upper()
        if self.symbol_filter and symbol != self.symbol_filter:
            return

        self.count += 1
        bucket = str(data.get("timestamp"))
        close = data.get("close")

        # Δclose vs the last tick we saw for this symbol — watch the price walk.
        delta = ""
        if close is not None:
            try:
                fc = float(close)
                prev = self._last_close.get(symbol)
                if prev is not None:
                    d = fc - prev
                    sign = "+" if d >= 0 else ""
                    delta = _c("32" if d >= 0 else "31", f"Δ{sign}{d:.2f}")
                self._last_close[symbol] = fc
            except (TypeError, ValueError):
                pass

        # Inter-arrival gap (ms) for this symbol — cadence at a glance.
        gap = ""
        mono = recv.timestamp()
        prev_arr = self._last_arrival.get(symbol)
        if prev_arr is not None:
            gap = f"+{(mono - prev_arr) * 1000:.0f}ms"
        self._last_arrival[symbol] = mono

        # Source-order anomaly: the bucket timestamp regressed for this symbol.
        # Intra-minute ticks share a (minute-floored) bucket, so this only
        # fires on a genuine out-of-order COMMIT — rare, and worth seeing.
        flag = ""
        prev_bucket = self._last_bucket.get(symbol)
        if prev_bucket is not None and bucket < prev_bucket:
            flag = _c("1;31", "  <-- BUCKET WENT BACKWARDS (out-of-order commit)")
        if prev_bucket is None or bucket >= prev_bucket:
            self._last_bucket[symbol] = bucket

        stamp = recv.strftime("%H:%M:%S.") + f"{recv.microsecond // 1000:03d}"
        line = (
            f"{_c('90', f'#{self.count:05d}')} {_c('90', stamp)} "
            f"{_c('36', symbol):<6} bucket={bucket} "
            f"O={_fmt_num(data.get('open'))} H={_fmt_num(data.get('high'))} "
            f"L={_fmt_num(data.get('low'))} C={_c('1', _fmt_num(close))} "
            f"upV={data.get('up_volume')} dnV={data.get('down_volume')} "
            f"{delta} {_c('90', gap)}{flag}"
        )
        print(line)
        if self.raw:
            print(_c("90", f"        raw: {payload}"))


async def run(symbol_filter: Optional[str], raw: bool) -> None:
    kwargs = _resolve_connect_kwargs()
    where = f"{kwargs['user']}@{kwargs['host']}:{kwargs['port']}/{kwargs['database']}"
    print(_c("1", f"Connecting to {where} ..."), file=sys.stderr)
    conn = await asyncpg.connect(**kwargs)
    tape = Tape(symbol_filter, raw)
    await conn.add_listener(NOTIFY_CHANNEL, tape.on_notify)

    scope = f" (symbol={tape.symbol_filter})" if tape.symbol_filter else ""
    print(
        _c("1;32", f"LISTEN {NOTIFY_CHANNEL}{scope} — waiting for ticks. Ctrl+C to stop."),
        file=sys.stderr,
    )
    print(
        _c("90", "Columns: #seq  recv-time  SYMBOL  bucket(minute)  OHLC  volumes  "
                 "Δclose  inter-arrival.  (server_ts lives on the WS wire, not here.)"),
        file=sys.stderr,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    try:
        await stop.wait()
    finally:
        try:
            await conn.remove_listener(NOTIFY_CHANNEL, tape.on_notify)
        finally:
            await conn.close()
        print(_c("1", f"\nStopped. {tape.count} notifications observed."), file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--symbol", help="Only print this symbol (e.g. SPY).")
    parser.add_argument("--raw", action="store_true", help="Also print the raw JSON payload.")
    args = parser.parse_args()
    try:
        asyncio.run(run(args.symbol, args.raw))
    except KeyboardInterrupt:  # pragma: no cover
        pass


if __name__ == "__main__":
    main()
