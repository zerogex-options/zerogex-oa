"""Gamma Forecast auto-tweet — Phase 3b companion to forecast_writer/receipt.

Two modes, both back-ended by the same script + two systemd timers:

  * ``--mode morning`` fires 07:10 ET, ~10 minutes after the morning writer
    lands the ``daily_forecast`` row. Tweets today's projected range /
    pin strike / regime + the ``/forecast/{date}`` permalink.
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
from datetime import date, datetime
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
    permalink = f"{site_url.rstrip('/')}/forecast/{day_iso}"

    body = (
        f"{sym} · {day_iso} morning forecast\n"
        f"Range: {low} – {high}\n"
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
    permalink = f"{site_url.rstrip('/')}/forecast/{day_iso}"

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


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info(
            "forecast_tweet[%s]: skipping %s — not a trading day",
            args.mode, day.isoformat(),
        )
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
        try:
            row = await db.get_daily_forecast(args.symbol.upper(), day)
        except Exception as exc:
            logger.warning(
                "forecast_tweet[%s]: get_daily_forecast failed (%s) — exiting 0",
                args.mode, exc,
            )
            return 0

        if row is None:
            logger.info(
                "forecast_tweet[%s]: no daily_forecast row for %s %s — skipping",
                args.mode, args.symbol.upper(), day.isoformat(),
            )
            return 0

        if args.mode == "morning":
            tweet_text = build_morning_tweet(row, args.site_url)
        elif args.mode == "receipt":
            if row.get("receipt_ts") is None:
                logger.info(
                    "forecast_tweet[receipt]: %s %s morning row exists but receipt "
                    "not written yet — skipping",
                    args.symbol.upper(), day.isoformat(),
                )
                return 0
            tweet_text = build_receipt_tweet(row, args.site_url)
        else:
            logger.warning("forecast_tweet: unknown mode %r — exiting 0", args.mode)
            return 0

        bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()

        if not args.post or not bearer:
            reason = "no --post flag" if not args.post else "X_BOT_BEARER_TOKEN unset"
            logger.info(
                "forecast_tweet[%s]: DRY RUN (%s)\n----\n%s\n----",
                args.mode, reason, tweet_text,
            )
            return 0

        try:
            resp = post_tweet_via_x_api(tweet_text, bearer)
        except (HTTPError, URLError) as exc:
            logger.warning(
                "forecast_tweet[%s]: X API call failed (%s) — exiting 0",
                args.mode, exc,
            )
            return 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "forecast_tweet[%s]: unexpected X API error (%s) — exiting 0",
                args.mode, exc,
            )
            return 0

        tweet_id = (resp.get("data") or {}).get("id")
        logger.info(
            "forecast_tweet[%s]: posted tweet id=%s for %s %s",
            args.mode, tweet_id, args.symbol.upper(), day.isoformat(),
        )
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
        "--symbol",
        default=os.environ.get("FORECAST_SYMBOLS", "SPY").split(",")[0].strip(),
        help="Underlying (default: first FORECAST_SYMBOLS entry, else SPY).",
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
