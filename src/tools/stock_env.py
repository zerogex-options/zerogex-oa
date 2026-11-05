"""Add a stock to the .env lists it needs, the way NVDA was added.

A stock goes into three lists every time (INGEST_UNDERLYINGS,
ANALYTICS_UNDERLYINGS, MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS) and into the
strike-band map INGEST_STRIKE_PCT_RANGE_BY_SYMBOL. The optional lists
(WALL_ALERTS_SYMBOLS, REGIME_SESSION_SYMBOLS, DAILY_ATM_IV_SYMBOLS,
WS_ALLOWED_SYMBOLS) get it only where the template stock, NVDA, already is, so
each new stock is set up exactly as NVDA was. Lists a stock must stay out of
(the signal and forecast models, the bulletin tweets, TradeWorkz) are never
touched; if the template is found in one, it says so.

Why a tool for a one-line edit: the strike-band map is JSON, and a value that
does not parse is ignored with one log line (src/config.py
_parse_symbol_float_map), which would put NVDA, not only the new stock, back on
the 3% band. And a slip in INGEST_UNDERLYINGS can break a symbol customers are
already reading. This only appends, shows the change first, and backs the file
up before it writes.

Usage::

    python -m src.tools.stock_env --symbol MSFT              # show the change
    python -m src.tools.stock_env --symbol MSFT --apply      # back up, then write

Exit codes: 0 changed or nothing to change, 1 refused (nothing written).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

#: Every stock goes in these.
REQUIRED_LISTS = (
    "INGEST_UNDERLYINGS",
    "ANALYTICS_UNDERLYINGS",
    "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS",
)
#: A stock goes in these only where the template stock already is.
MIRRORED_LISTS = (
    "WALL_ALERTS_SYMBOLS",
    "REGIME_SESSION_SYMBOLS",
    "DAILY_ATM_IV_SYMBOLS",
    "WS_ALLOWED_SYMBOLS",
)
#: Models and feeds a stock must stay out of (docs/runbooks/stock_onboarding.md).
FORBIDDEN_LISTS = (
    "SIGNALS_UNDERLYINGS",
    "FORECAST_SYMBOLS",
    "CONE_SYMBOLS",
    "BULLETIN_TWEET_SYMBOLS",
    "TRADEWORKZ_UNIVERSE",
    "INGEST_FUTURES_INDEXES",
    "ETF_SYMBOLS",
)
BAND_KEY = "INGEST_STRIKE_PCT_RANGE_BY_SYMBOL"
#: Read as a make variable by the Makefile, so space-separated (see .env.example).
SPACE_SEPARATED = ("DAILY_ATM_IV_SYMBOLS",)
#: Which running service reads each key at start, so a change to it needs that restart.
RESTART_FOR = {
    "INGEST_UNDERLYINGS": "zerogex-oa-ingestion",
    BAND_KEY: "zerogex-oa-ingestion",
    "ANALYTICS_UNDERLYINGS": "zerogex-oa-analytics",
    "WS_ALLOWED_SYMBOLS": "zerogex-oa-api",
    "WALL_ALERTS_SYMBOLS": "zerogex-oa-wall-alerts",
}

LINE_RE = re.compile(
    r"^(?P<lead>\s*(?:export\s+)?)(?P<key>[A-Z][A-Z0-9_]*)=(?P<value>.*?)(?P<nl>\r?\n?)$"
)
SYMBOL_RE = re.compile(r"^[A-Z]{1,5}$")


class Refused(Exception):
    """The change cannot be made safely; nothing is written."""


@dataclass
class Plan:
    lines: List[str]
    changed: Dict[str, Tuple[str, str]] = field(default_factory=dict)  # key -> (old, new) line
    notes: List[str] = field(default_factory=list)

    @property
    def restarts(self) -> List[str]:
        units = {RESTART_FOR[k] for k in self.changed if k in RESTART_FOR}
        order = [
            "zerogex-oa-ingestion",
            "zerogex-oa-analytics",
            "zerogex-oa-api",
            "zerogex-oa-wall-alerts",
        ]
        return [u for u in order if u in units]


def _split_quotes(value: str) -> Tuple[str, str, str]:
    """(open quote, inner value, close quote)."""
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[0], value[1:-1], value[0]
    return "", value, ""


def _tokens(inner: str) -> List[str]:
    return [t for t in re.split(r"[,\s]+", inner.strip()) if t]


def append_token(key: str, value: str, symbol: str) -> str:
    """``value`` with ``symbol`` appended in the list's own separator."""
    if "#" in value:
        raise Refused(f"{key} has a comment on its line; edit that line by hand")
    open_q, inner, close_q = _split_quotes(value.strip())
    stripped = inner.strip()
    if "," in stripped:
        sep = ","
    elif " " in stripped:
        sep = " "
    else:
        sep = " " if key in SPACE_SEPARATED else ","
    joined = f"{stripped}{sep}{symbol}" if stripped else symbol
    return f"{open_q}{joined}{close_q}"


def add_band(value: str, symbol: str, band: float) -> str:
    """The strike-band JSON map with ``symbol: band`` added, unquoted as the
    Makefile needs it (it reads .env as a makefile, where quotes stay literal)."""
    if "#" in value:
        raise Refused(f"{BAND_KEY} has a comment on its line; edit that line by hand")
    _, inner, _ = _split_quotes(value.strip())
    try:
        parsed = json.loads(inner) if inner.strip() else {}
    except ValueError:
        raise Refused(
            f"{BAND_KEY} is not valid JSON today ({inner!r}), so every stock is on the 3% "
            "band already. Fix that line by hand first."
        )
    if not isinstance(parsed, dict):
        raise Refused(f"{BAND_KEY} is not a JSON object ({inner!r})")
    parsed[symbol] = int(band) if float(band).is_integer() else band
    out = json.dumps(parsed, separators=(", ", ": "))
    assert json.loads(out) == parsed  # what we write must parse to what we meant
    return out


def plan_addition(
    lines: List[str], symbol: str, band: Optional[float] = None, template: str = "NVDA"
) -> Plan:
    """What adding ``symbol`` changes, line by line. Raises Refused."""
    symbol = symbol.strip().upper()
    template = template.strip().upper()
    if not SYMBOL_RE.match(symbol):
        raise Refused(f"{symbol!r} is not a stock ticker")

    plan = Plan(lines=list(lines))
    seen: Dict[str, int] = {}
    for i, line in enumerate(lines):
        m = LINE_RE.match(line)
        if not m or line.lstrip().startswith("#"):
            continue
        key = m["key"]
        if key in seen:
            raise Refused(f"{key} is set twice in .env (lines {seen[key] + 1} and {i + 1})")
        seen[key] = i

    def tokens_of(key: str) -> List[str]:
        """The list's symbols. A comment is not a symbol: "# MSFT next" must
        not read as MSFT being listed already."""
        if key not in seen:
            return []
        m = LINE_RE.match(lines[seen[key]])
        assert m is not None
        return _tokens(_split_quotes(m["value"].split("#", 1)[0].strip())[1])

    for key in FORBIDDEN_LISTS:
        if symbol in tokens_of(key):
            raise Refused(
                f"{symbol} is already in {key}, which no stock belongs in; fix that first"
            )
        if template in tokens_of(key):
            plan.notes.append(f"{template} is in {key}, which no stock belongs in; left alone")

    template_band: Optional[float] = None
    if BAND_KEY in seen:
        m = LINE_RE.match(lines[seen[BAND_KEY]])
        assert m is not None
        try:
            current = json.loads(_split_quotes(m["value"].strip())[1] or "{}")
            if isinstance(current, dict) and template in current:
                template_band = float(current[template])
        except ValueError:
            pass  # add_band refuses with the reason

    def change(key: str, new_value: str) -> None:
        i = seen[key]
        m = LINE_RE.match(lines[i])
        assert m is not None
        new_line = f"{m['lead']}{key}={new_value}{m['nl']}"
        plan.lines[i] = new_line
        plan.changed[key] = (lines[i], new_line)

    for key in REQUIRED_LISTS:
        if key not in seen:
            raise Refused(
                f"{key} is not in .env; this deployment is not set up the way the runbook expects"
            )
        if symbol in tokens_of(key):
            continue
        m = LINE_RE.match(lines[seen[key]])
        assert m is not None
        change(key, append_token(key, m["value"], symbol))

    for key in MIRRORED_LISTS:
        if key in seen and template in tokens_of(key) and symbol not in tokens_of(key):
            m = LINE_RE.match(lines[seen[key]])
            assert m is not None
            change(key, append_token(key, m["value"], symbol))
        elif key in seen and template not in tokens_of(key):
            plan.notes.append(f"{key} does not list {template}, so {symbol} is left out of it too")

    use_band = band if band is not None else (template_band if template_band is not None else 8.0)
    if not 0.5 <= use_band <= 50.0:
        raise Refused(f"a {use_band}% band is outside the 0.5-50 the parser accepts")
    if BAND_KEY in seen:
        m = LINE_RE.match(lines[seen[BAND_KEY]])
        assert m is not None
        current_value = m["value"]
        new_value = add_band(current_value, symbol, use_band)
        if json.loads(new_value) != json.loads(_split_quotes(current_value.strip())[1] or "{}"):
            change(BAND_KEY, new_value)
    else:
        new_line = f"{BAND_KEY}={add_band('', symbol, use_band)}\n"
        if plan.lines and not plan.lines[-1].endswith("\n"):
            plan.lines[-1] += "\n"
        plan.lines.append(new_line)
        plan.changed[BAND_KEY] = ("", new_line)
    return plan


def describe(plan: Plan) -> str:
    """Only the changed lines, old then new. Never a neighbouring line: .env
    holds passwords and keys, and this is printed to a terminal."""
    if not plan.changed:
        return "Nothing to change: it is already in every list it needs."
    out = []
    for key, (old, new) in plan.changed.items():
        out.append(f"- {old.rstrip()}" if old else f"- (no {key} line)")
        out.append(f"+ {new.rstrip()}")
    return "\n".join(out)


def backup_path(env: Path, symbol: str, now: datetime, home: Path) -> Path:
    base = home / f".env.pre-{symbol.lower()}-{now:%Y-%m-%d}"
    return base if not base.exists() else home / f"{base.name}-{now:%H%M%S}"


def write(env: Path, plan: Plan, symbol: str, now: datetime, home: Path) -> Path:
    """Back up ``env``, then replace it in one rename, keeping its permissions."""
    backup = backup_path(env, symbol, now, home)
    shutil.copy2(env, backup)
    mode = env.stat().st_mode
    fd, tmp = tempfile.mkstemp(dir=env.parent, prefix=".env.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write("".join(plan.lines))
        os.chmod(tmp, mode)
        os.replace(tmp, env)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return backup


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--symbol", required=True, help="the stock, e.g. MSFT")
    parser.add_argument(
        "--band", type=float, default=None, help="strike band in percent (default: NVDA's)"
    )
    parser.add_argument("--template", default="NVDA", help="the stock to mirror (default NVDA)")
    parser.add_argument("--env", default=".env", help="the .env file (default ./.env)")
    parser.add_argument("--apply", action="store_true", help="back up .env, then write the change")
    args = parser.parse_args(argv)

    from src.symbols import asset_class

    symbol = args.symbol.strip().upper()
    env = Path(args.env)
    try:
        if asset_class(symbol) != "STOCK":
            raise Refused(
                f"{symbol} is not a single stock here ({asset_class(symbol)}); this is for stocks"
            )
        if not env.is_file():
            raise Refused(f"{env} not found; run this from the repo root")
        lines = env.read_text().splitlines(keepends=True)
        plan = plan_addition(lines, symbol, args.band, args.template)
    except Refused as e:
        print(f"Refused, nothing written: {e}")
        return 1

    print(describe(plan))
    for note in plan.notes:
        print(f"  note: {note}")
    if not plan.changed:
        return 0
    if not args.apply:
        print("\nNothing written. Run again with CONFIRM=yes to back up .env and write this.")
        return 0
    backup = write(env, plan, symbol, datetime.now(), Path.home())
    print(f"\nWritten. The old file is {backup}")
    print("Restart for it to take effect: " + ", ".join(plan.restarts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
