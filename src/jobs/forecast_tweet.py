"""Gamma Forecast auto-tweet — Phase 3b companion to forecast_writer/receipt.

Two modes, both back-ended by the same script + two systemd timers:

  * ``--mode morning`` fires 07:10 ET, ~10 minutes after the morning writer
    lands the ``daily_forecast`` row. Tweets today's projected range /
    pin strike / regime + the ``/forecast/{symbol}/{date}`` permalink.
  * ``--mode receipt`` fires 16:10 ET, ~5 minutes after the receipt writer
    grades the morning commitment. Tweets the verdict overlay ("range
    held/broken", "pin hit/missed", "regime correct/wrong") + the same
    permalink (which now renders in receipt state).

Both modes share the ``scorecard_tweet`` design rules:
  * Never throw. Every failure path logs + exits 0 so the timer keeps
    running tomorrow.
  * Dry-run by default. Live posting requires BOTH the ``--post`` flag
    AND ``X_BOT_BEARER_TOKEN`` in env — defense in depth so a
    half-configured rollout can't accidentally tweet.
  * Skip silently on non-trading days.
  * Skip silently when the referenced ``daily_forecast`` row doesn't
    exist yet (morning mode after 07:10 without a writer fire, or
    receipt mode against a day that had no morning row).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.forecast_tweet")
ET = ZoneInfo("America/New_York")

DEFAULT_SITE_URL = "https://zerogex.io"
TWEET_MAX_LEN = 280

# Idempotency marker directory — one file per (mode, symbol, date) recording a
# successful live post.  Guards against a duplicate tweet when the job re-runs
# for a day it already posted (systemd Persistent= catch-up, a manual re-run,
# or downtime recovery).  Mirrors the bulletin-tweets state dir convention;
# override with FORECAST_TWEET_STATE_DIR.  Best-effort: if the dir can't be
# written the guard fails OPEN (posts proceed) — better a rare duplicate than a
# silently-never-posted forecast.
DEFAULT_TWEET_STATE_DIR = "/var/lib/zerogex-oa/forecast-tweets"


def _tweet_state_dir() -> Path:
    override = os.environ.get("FORECAST_TWEET_STATE_DIR", "").strip()
    return Path(override) if override else Path(DEFAULT_TWEET_STATE_DIR)


def _posted_marker_path(mode: str, symbol: str, day: date) -> Path:
    return _tweet_state_dir() / f"{mode}-{symbol.upper()}-{day.isoformat()}.json"


def _already_posted(mode: str, symbol: str, day: date) -> Optional[dict[str, Any]]:
    """Return the recorded post marker for (mode, symbol, day), or None.

    Read-only — never creates the state dir.  Any read error is treated as
    'not posted' so a corrupt/unreadable marker never blocks a post."""
    path = _posted_marker_path(mode, symbol, day)
    try:
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — fail open
        return None


def _record_posted(mode: str, symbol: str, day: date, tweet_id: Optional[str]) -> None:
    """Persist a marker after a successful live post.  Best-effort: a write
    failure (unwritable dir, race) logs at DEBUG and is otherwise ignored."""
    path = _posted_marker_path(mode, symbol, day)
    payload = {
        "mode": mode,
        "symbol": symbol.upper(),
        "date": day.isoformat(),
        "tweet_id": tweet_id,
        "posted_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 — best-effort; don't fail the run
        logger.debug("forecast_tweet: could not record post marker %s (%s)", path, exc)


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


def _humanize_regime(value: str | None) -> str:
    if not value:
        return "Unknown"
    return value.replace("_", " ").title()


def _fmt_price(v: float | None) -> str:
    if v is None:
        return "—"
    return f"${v:.2f}"


def _fmt_verdict(v: bool | None) -> str:
    if v is True:
        return "✓"
    if v is False:
        return "✗"
    return "—"


# ---------------------------------------------------------------------------
# Tweet builders
# ---------------------------------------------------------------------------


def build_morning_tweet(row: dict[str, Any], site_url: str) -> str:
    """Commit tweet — posted after the morning writer lands the row."""
    sym = row["symbol"]
    day = row["date"]
    if isinstance(day, str):
        day_iso = day
    else:
        day_iso = day.isoformat()
    low = _fmt_price(row.get("projected_low"))
    high = _fmt_price(row.get("projected_high"))
    pin = _fmt_price(row.get("pin_strike"))
    regime = _humanize_regime(row.get("regime"))
    permalink = f"{site_url.rstrip('/')}/forecast/{sym}/{day_iso}"

    # Asymmetric ± % display makes the confidence readable at a glance —
    # a ±0.35% band and a −0.60%/+0.90% band are very different claims
    # even when the dollar bounds look similar.
    pct_line = ""
    spot = row.get("open_spot")
    pl = row.get("projected_low")
    ph = row.get("projected_high")
    try:
        if spot is not None and float(spot) > 0 and pl is not None and ph is not None:
            spot_f = float(spot)
            down_pct = (spot_f - float(pl)) / spot_f * 100.0
            up_pct = (float(ph) - spot_f) / spot_f * 100.0
            pct_line = f" (−{down_pct:.2f}%/+{up_pct:.2f}%)"
    except (TypeError, ValueError):
        pass

    body = (
        f"{sym} · {day_iso} morning forecast\n"
        f"Range: {low} – {high}{pct_line}\n"
        f"Pin: {pin} · Regime: {regime}"
    )
    text = f"{body}\n{permalink}"
    if len(text) <= TWEET_MAX_LEN:
        return text
    # Fallback if a pathological site_url overflows the tweet limit.
    trimmed = body[: max(0, len(body) - (len(text) - TWEET_MAX_LEN) - 1)].rstrip()
    return f"{trimmed}\n{permalink}"


def build_receipt_tweet(row: dict[str, Any], site_url: str) -> str:
    """Receipt tweet — posted after the 16:05 grade lands."""
    sym = row["symbol"]
    day = row["date"]
    if isinstance(day, str):
        day_iso = day
    else:
        day_iso = day.isoformat()
    range_v = row.get("range_respected")
    pin_v = row.get("pin_hit")
    regime_v = row.get("regime_correct")
    actual_close = _fmt_price(row.get("actual_close"))
    permalink = f"{site_url.rstrip('/')}/forecast/{sym}/{day_iso}"

    range_txt = "held" if range_v is True else "broken" if range_v is False else "—"
    pin_txt = "hit" if pin_v is True else "missed" if pin_v is False else "—"
    regime_txt = (
        "correct" if regime_v is True else "wrong" if regime_v is False else "n/a"
    )

    body = (
        f"{sym} · {day_iso} receipt\n"
        f"Range {_fmt_verdict(range_v)} {range_txt} · "
        f"Pin {_fmt_verdict(pin_v)} {pin_txt} · Regime {_fmt_verdict(regime_v)} {regime_txt}\n"
        f"Close: {actual_close}"
    )
    text = f"{body}\n{permalink}"
    if len(text) <= TWEET_MAX_LEN:
        return text
    trimmed = body[: max(0, len(body) - (len(text) - TWEET_MAX_LEN) - 1)].rstrip()
    return f"{trimmed}\n{permalink}"


# ---------------------------------------------------------------------------
# X API client — mirrors scorecard_tweet.post_tweet_via_x_api
# ---------------------------------------------------------------------------


def post_tweet_via_x_api(text: str, bearer_token: str, timeout_seconds: int = 15) -> dict[str, Any]:
    req = Request(
        "https://api.x.com/2/tweets",
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "User-Agent": "zerogex-forecast-tweet/1.0",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body) if body else {}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def _tweet_one(
    db: DatabaseManager, symbol: str, day: date, args: argparse.Namespace,
) -> None:
    """Post (or dry-run) one symbol's forecast/receipt tweet. Never raises —
    a symbol-level failure logs and returns so the outer loop keeps going."""
    symbol = symbol.upper()
    try:
        row = await db.get_daily_forecast(symbol, day)
    except Exception as exc:
        logger.warning(
            "forecast_tweet[%s]: get_daily_forecast(%s) failed (%s) — skipping",
            args.mode, symbol, exc,
        )
        return

    if row is None:
        logger.info(
            "forecast_tweet[%s]: no daily_forecast row for %s %s — skipping",
            args.mode, symbol, day.isoformat(),
        )
        return

    if args.mode == "morning":
        tweet_text = build_morning_tweet(row, args.site_url)
    elif args.mode == "receipt":
        if row.get("receipt_ts") is None:
            logger.info(
                "forecast_tweet[receipt]: %s %s morning row exists but receipt "
                "not written yet — skipping",
                symbol, day.isoformat(),
            )
            return
        tweet_text = build_receipt_tweet(row, args.site_url)
    else:
        logger.warning("forecast_tweet: unknown mode %r — skipping %s", args.mode, symbol)
        return

    bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()

    if not args.post or not bearer:
        reason = "no --post flag" if not args.post else "X_BOT_BEARER_TOKEN unset"
        logger.info(
            "forecast_tweet[%s]: DRY RUN %s (%s)\n----\n%s\n----",
            args.mode, symbol, reason, tweet_text,
        )
        return

    # Idempotency: don't re-post a (mode, symbol, day) we've already tweeted.
    # This is the guard against a systemd Persistent= catch-up / manual re-run
    # firing a duplicate live tweet.  --force overrides for an intentional repost.
    if not getattr(args, "force", False):
        prior = _already_posted(args.mode, symbol, day)
        if prior is not None:
            logger.info(
                "forecast_tweet[%s]: %s %s already posted (id=%s at %s) — skipping "
                "(use --force to repost)",
                args.mode, symbol, day.isoformat(),
                prior.get("tweet_id"), prior.get("posted_at"),
            )
            return

    try:
        resp = post_tweet_via_x_api(tweet_text, bearer)
    except (HTTPError, URLError) as exc:
        logger.warning(
            "forecast_tweet[%s]: X API call failed for %s (%s) — skipping",
            args.mode, symbol, exc,
        )
        return
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "forecast_tweet[%s]: unexpected X API error for %s (%s) — skipping",
            args.mode, symbol, exc,
        )
        return

    tweet_id = (resp.get("data") or {}).get("id")
    _record_posted(args.mode, symbol, day, tweet_id)
    logger.info(
        "forecast_tweet[%s]: posted tweet id=%s for %s %s",
        args.mode, tweet_id, symbol, day.isoformat(),
    )


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info(
            "forecast_tweet[%s]: skipping %s — not a trading day",
            args.mode, day.isoformat(),
        )
        return 0

    # Backward-compat: --symbol (singular) overrides the multi-symbol default.
    # Otherwise fan out over every entry in --symbols / FORECAST_SYMBOLS so
    # one systemd fire tweets the whole roster in a fixed order.
    symbols = (
        [args.symbol.upper()]
        if args.symbol
        else [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    )
    if not symbols:
        logger.warning("forecast_tweet: no symbols resolved — exiting 0")
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:
        logger.warning(
            "forecast_tweet[%s]: DB connect failed (%s) — exiting 0", args.mode, exc,
        )
        return 0

    try:
        for symbol in symbols:
            await _tweet_one(db, symbol, day, args)
        return 0
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["morning", "receipt"],
        required=True,
        help="Which tweet to build (morning commit or 4 PM receipt).",
    )
    parser.add_argument(
        "--symbols",
        default=os.environ.get("FORECAST_SYMBOLS", "SPY"),
        help="Comma-separated symbols to tweet for (default: $FORECAST_SYMBOLS or SPY).",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Single symbol override — takes precedence over --symbols. Useful for backfill.",
    )
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Default: today ET.")
    parser.add_argument(
        "--post",
        action="store_true",
        help="Actually post to X. Without this flag the job dry-runs even when X_BOT_BEARER_TOKEN is set.",
    )
    parser.add_argument(
        "--site-url",
        default=os.environ.get("ZEROGEX_SITE_URL", DEFAULT_SITE_URL),
        help="Permalink host (default https://zerogex.io or $ZEROGEX_SITE_URL).",
    )
    parser.add_argument(
        "--allow-non-trading-day",
        action="store_true",
        help="Override the weekend/holiday skip — useful for backfill / testing.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Repost even if a (mode, symbol, date) tweet was already posted "
        "(bypasses the idempotency guard).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
