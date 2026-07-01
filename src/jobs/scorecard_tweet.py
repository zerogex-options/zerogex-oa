"""Yesterday's Scorecard auto-tweet job.

Runs once per weekday at 16:15 America/New_York (15 minutes after the
cash session closes). Computes the day's scorecard by re-using the same
``get_daily_scorecard`` query that powers the public API + permalink page,
then either posts the tweet to X (when ``X_BOT_BEARER_TOKEN`` is set) or
emits dry-run output to stdout/journalctl.

Design rules:

* **Never throws.** The job is best-effort: any failure (DB unavailable,
  X API timeout, malformed payload) logs a WARNING and exits 0 so the
  systemd timer keeps running tomorrow.
* **No tweet on non-trading days.** Consults market_calendar.NYSE_HOLIDAYS
  to skip weekends and configured holidays. The cron timer fires Mon-Fri;
  this check catches the holidays inside that window.
* **No tweet on empty days.** If the engine emitted nothing and no signal
  flips happened, skip — better silence than a useless "0 0 0" post that
  makes the brand look dead.
* **Dry-run first.** Until ``X_BOT_BEARER_TOKEN`` is set in the service
  EnvironmentFile, every run logs the tweet copy and the OG image URL it
  would have shared. Operators can flip the env var to go live.

Run manually:
    python -m src.jobs.scorecard_tweet                  # today, dry-run
    python -m src.jobs.scorecard_tweet --date 2026-06-29
    python -m src.jobs.scorecard_tweet --symbol SPY --post
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.api.routers.scorecard import (
    ALL_SIGNAL_NAMES,
    _et_day_to_utc_bounds,
    _label_regime,
)
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.scorecard_tweet")
ET = ZoneInfo("America/New_York")

DEFAULT_SITE_URL = "https://zerogex.io"
TWEET_MAX_LEN = 280


# ---------------------------------------------------------------------------
# Date / market-calendar helpers
# ---------------------------------------------------------------------------


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    """Mon–Fri excluding configured NYSE holidays. Half-days count as
    trading days — the close still produces a real scorecard."""
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


# ---------------------------------------------------------------------------
# Scorecard fetch
# ---------------------------------------------------------------------------


async def _fetch_scorecard(
    db: DatabaseManager, day: date, symbol: str, horizon_minutes: int = 60,
) -> dict[str, Any]:
    start_utc, end_utc = _et_day_to_utc_bounds(day)
    payload = await db.get_daily_scorecard(
        symbol=symbol,
        start_utc=start_utc,
        end_utc=end_utc,
        signal_names=list(ALL_SIGNAL_NAMES),
        horizon_minutes=horizon_minutes,
    )
    cards = payload["cards"]
    cards["first_card_permalink"] = (
        f"/cards/{cards['first_card_id']}" if cards.get("first_card_id") else None
    )
    regime = payload.get("regime")
    if regime is not None:
        regime["label"] = _label_regime(regime)
    return payload


# ---------------------------------------------------------------------------
# Tweet copy
# ---------------------------------------------------------------------------


def _humanize_signal_name(name: str) -> str:
    return name.replace("_", " ").title()


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    pct = value * 100
    sign = "+" if pct >= 0 else "−"
    return f"{sign}{abs(pct):.2f}%"


def build_tweet_copy(payload: dict[str, Any], day: date, symbol: str, site_url: str) -> str:
    """Assemble the canonical tweet body.

    Mirrors the wording produced by the /api/scorecard/daily endpoint so
    the tweet and the permalink page never contradict each other. Always
    ends with the scorecard's own permalink URL so the tweet links to its
    receipt for verification.
    """
    cards = payload["cards"]
    best = payload["signals"]["best"]
    worst = payload["signals"]["worst"]
    regime = payload.get("regime") or {}
    regime_label = regime.get("label") or "unknown"
    permalink = f"{site_url.rstrip('/')}/scorecard/{day.isoformat()}"

    parts: list[str] = [f"{symbol} · {day.isoformat()}"]
    if cards["total"]:
        parts.append(f"{cards['total']} Playbook call{'s' if cards['total'] != 1 else ''}")
    if best:
        parts.append(
            f"Best: {_humanize_signal_name(best['name'])} {_fmt_pct(best['avg_directional_return'])}"
        )
    if worst and (not best or worst["name"] != best["name"]):
        parts.append(
            f"Worst: {_humanize_signal_name(worst['name'])} {_fmt_pct(worst['avg_directional_return'])}"
        )
    if regime_label != "unknown":
        parts.append(f"Regime: {regime_label}")

    if len(parts) == 1:
        body = f"{symbol} · {day.isoformat()} — quiet tape. No Playbook calls, no signal flips."
    else:
        body = " — ".join([parts[0], ". ".join(parts[1:]) + "."])

    # Tweet body + newline + URL. Trim body if the whole thing breaks the
    # X 280-char ceiling (extremely rare; a safety net only).
    text = f"{body}\n{permalink}"
    if len(text) <= TWEET_MAX_LEN:
        return text
    overflow = len(text) - TWEET_MAX_LEN
    trimmed = body[: max(0, len(body) - overflow - 1)].rstrip(" .,;:—-") + "…"
    return f"{trimmed}\n{permalink}"


# ---------------------------------------------------------------------------
# X API client
# ---------------------------------------------------------------------------


def post_tweet_via_x_api(text: str, bearer_token: str, timeout_seconds: int = 15) -> dict[str, Any]:
    """POST to https://api.x.com/2/tweets. Returns the parsed JSON response.

    Uses urllib so the job inherits no new third-party dependency. The
    X v2 API is JSON-over-HTTPS with a Bearer token; the request body is
    ``{"text": "..."}``. Network errors propagate so the caller can log
    a single warning with context.
    """
    req = Request(
        "https://api.x.com/2/tweets",
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "User-Agent": "zerogex-scorecard-tweet/1.0",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body) if body else {}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        help="Trading day to summarize (YYYY-MM-DD). Default: today in America/New_York.",
    )
    parser.add_argument("--symbol", default="SPY", help="Underlying (default SPY).")
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


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info("Skipping %s — not a trading day (weekend or NYSE holiday).", day.isoformat())
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:
        logger.warning("scorecard_tweet: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        try:
            payload = await _fetch_scorecard(db, day, args.symbol.upper())
        except Exception as exc:
            logger.warning("scorecard_tweet: get_daily_scorecard failed (%s) — exiting 0", exc)
            return 0

        is_empty = payload["cards"]["total"] == 0 and not payload["signals"]["events"]
        if is_empty:
            logger.info(
                "scorecard_tweet: empty scorecard for %s %s — skipping tweet",
                args.symbol.upper(), day.isoformat(),
            )
            return 0

        tweet_text = build_tweet_copy(payload, day, args.symbol.upper(), args.site_url)
        bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()

        if not args.post or not bearer:
            reason = "no --post flag" if not args.post else "X_BOT_BEARER_TOKEN unset"
            logger.info(
                "scorecard_tweet: DRY RUN (%s)\n----\n%s\n----", reason, tweet_text,
            )
            return 0

        try:
            resp = post_tweet_via_x_api(tweet_text, bearer)
        except (HTTPError, URLError) as exc:
            logger.warning("scorecard_tweet: X API call failed (%s) — exiting 0", exc)
            return 0
        except Exception as exc:  # noqa: BLE001 — never raise from a cron job
            logger.warning(
                "scorecard_tweet: unexpected X API error (%s) — exiting 0", exc,
            )
            return 0

        tweet_id = (resp.get("data") or {}).get("id")
        logger.info(
            "scorecard_tweet: posted tweet id=%s for %s %s",
            tweet_id, args.symbol.upper(), day.isoformat(),
        )
        return 0
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
