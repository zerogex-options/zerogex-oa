"""Beehiiv newsletter publisher — daily (free) and weekly (paid) cadences.

Fires from two systemd timers:

  * ``--cadence daily``   08:30 ET Mon-Fri — publishes the free
    "GEX Morning Card" for the current trading day, populated from the
    forecast + scorecard artifacts the X-bulletin job also reads.
  * ``--cadence weekly``  07:00 ET Sun     — publishes the paid
    "Sunday Playbook Review" — signal accuracy for the trailing week,
    a pattern deep dive, next week's landscape summary.

Design rules — inherited from the tweet-job pattern:

* **Never throws.** Every failure logs a WARNING and exits 0 so the
  systemd timer keeps running tomorrow. Idempotent re-runs are safe.
* **Dry-run by default.** Live publishing requires BOTH the ``--post``
  flag AND the ``BEEHIIV_API_KEY`` / ``BEEHIIV_PUBLICATION_ID`` env
  vars to be set (defense in depth so a half-configured rollout can't
  accidentally publish). Missing either → dry-run.
* **Idempotency via ``newsletter_publications``.** Before firing the
  Beehiiv API call, check the DB for a row with the same
  (cadence, publication_date) and status ``published``; if present,
  skip. This is what protects us against the systemd
  ``Persistent=true`` catchup path — a re-fire post-outage can't
  produce a duplicate post.
* **Failed publishes leave a receipt.** A ``failed`` row with the
  Beehiiv error message goes to the DB alongside the intended
  ``pending`` transition, so the operator can grep for what didn't
  send.
* **Skip silently on non-trading days.** Half-days count as trading
  days for the daily cadence; the weekly cadence explicitly runs on
  Sundays only (via ``--cadence weekly``).

Run manually:
    python -m src.jobs.publish_beehiiv --cadence daily             # dry-run, today
    python -m src.jobs.publish_beehiiv --cadence weekly            # dry-run, this week
    python -m src.jobs.publish_beehiiv --cadence daily --dry-run   # explicit dry-run
    python -m src.jobs.publish_beehiiv --cadence daily --post      # live
    python -m src.jobs.publish_beehiiv --cadence daily --date 2026-07-03 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.market_calendar import NYSE_HOLIDAYS
from src.services.beehiiv_client import BeehiivClient, BeehiivError

logger = logging.getLogger("zerogex.publish_beehiiv")
ET = ZoneInfo("America/New_York")

DEFAULT_SYMBOLS = ("SPX", "SPY", "QQQ")
DEFAULT_UPGRADE_URL = "https://zerogex.io/newsletter#upgrade"
CADENCE_DAILY = "daily"
CADENCE_WEEKLY = "weekly"
AUDIENCE_FREE = "free"
AUDIENCE_PREMIUM = "premium"
TEMPLATE_DIR = Path(__file__).parent / "templates" / "beehiiv"


# ---------------------------------------------------------------------------
# Date / calendar helpers
# ---------------------------------------------------------------------------


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    """Mon-Fri excluding configured NYSE holidays. Matches the pattern
    used by :mod:`src.jobs.bulletin_tweet` and :mod:`src.jobs.scorecard_tweet`."""
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


def _prev_trading_day(day: date) -> date:
    """The nearest earlier trading day (skips weekends + NYSE_HOLIDAYS)."""
    d = day - timedelta(days=1)
    while not _is_trading_day(d):
        d -= timedelta(days=1)
    return d


def _monday_of_week(day: date) -> date:
    """The Monday of the ISO week containing ``day``. Used to label the
    weekly review — ``Week of {monday}`` in the subject line."""
    return day - timedelta(days=day.weekday())


# ---------------------------------------------------------------------------
# Number formatting — matches bulletinHelpers.ts conventions.
# ---------------------------------------------------------------------------


def _fmt_price(v: Any) -> str:
    """Render a numeric price, with ``—`` for missing / non-numeric."""
    if v is None:
        return "—"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    if n != n:  # NaN
        return "—"
    if abs(n) >= 1000:
        return f"{n:,.0f}"
    return f"{n:.2f}"


def _fmt_pct(v: Any) -> str:
    if v is None:
        return "—"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    return f"{n * 100:.1f}%"


# ---------------------------------------------------------------------------
# Data model — one row per symbol on the daily card.
# ---------------------------------------------------------------------------


@dataclass
class DailyLevelRow:
    symbol: str
    gamma_flip: str
    call_wall: str
    put_wall: str


@dataclass
class DailyPatternHint:
    """The one playbook pattern surfaced in the free daily email.

    Sourced from ``daily_forecast.flagship_setup`` — the Playbook Action
    Card the forecast writer selected as most likely to fire today.
    ``name`` and ``rationale`` are the fields we render; the underlying
    setup blob has more but a free-tier email deliberately shows only
    the derived headline."""

    name: str
    symbol: str
    rationale: str


@dataclass
class DailyPayload:
    headline_date: str
    recap: Optional[str]
    levels: list[DailyLevelRow]
    pattern: Optional[DailyPatternHint]
    upgrade_url: str


@dataclass
class WeeklyAccuracyRow:
    pattern: str
    underlying: str
    n_resolved: int
    hit_rate: str


@dataclass
class WeeklyDeepDive:
    pattern: str
    setup: str
    entry: str
    exit: str
    pnl: Optional[str] = None


@dataclass
class WeeklyPayload:
    week_start_label: str
    accuracy_rows: list[WeeklyAccuracyRow]
    deep_dive: Optional[WeeklyDeepDive]
    landscape: Optional[str]
    postmortem: Optional[str]


# ---------------------------------------------------------------------------
# Daily payload — pulls from forecast + scorecard artifacts.
# ---------------------------------------------------------------------------


def _row_gamma_flip(forecast_row: dict[str, Any] | None) -> str:
    return _fmt_price(forecast_row.get("gamma_flip") if forecast_row else None)


def _row_call_wall(forecast_row: dict[str, Any] | None) -> str:
    return _fmt_price(forecast_row.get("call_wall") if forecast_row else None)


def _row_put_wall(forecast_row: dict[str, Any] | None) -> str:
    return _fmt_price(forecast_row.get("put_wall") if forecast_row else None)


def _fetch_daily_symbol_forecasts(
    db: DatabaseManager, day: date, symbols: tuple[str, ...],
) -> "asyncio.Task[list[tuple[str, Optional[dict[str, Any]]]]]":
    """Return a coroutine that fetches one forecast row per symbol.

    Kept as a coroutine so the caller can gather() it inside its own
    event loop; each per-symbol fetch runs in sequence (there are only
    3 symbols, and the DB pool is small — parallelism buys us nothing)."""

    async def _run() -> list[tuple[str, Optional[dict[str, Any]]]]:
        out: list[tuple[str, Optional[dict[str, Any]]]] = []
        for sym in symbols:
            try:
                row = await db.get_daily_forecast(sym, day)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "publish_beehiiv: get_daily_forecast(%s, %s) failed (%s)",
                    sym, day.isoformat(), exc,
                )
                row = None
            out.append((sym, row))
        return out

    return asyncio.ensure_future(_run())


async def _build_yesterday_recap(
    db: DatabaseManager, day: date, symbol: str,
) -> Optional[str]:
    """One-paragraph recap of yesterday's session for the daily card.

    Sourced from the same ``get_daily_scorecard`` payload the X-bulletin
    scorecard_tweet job uses — total playbook calls, best signal, and
    the closing regime. Yields None when yesterday was not a trading
    day (Monday after a weekend, first trading day after a holiday) or
    the query failed / returned nothing."""
    prior = _prev_trading_day(day)
    try:
        from src.api.routers.scorecard import (
            ALL_SIGNAL_NAMES, _et_day_to_utc_bounds, _label_regime,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv: could not import scorecard helpers (%s)", exc,
        )
        return None

    start_utc, end_utc = _et_day_to_utc_bounds(prior)
    try:
        payload = await db.get_daily_scorecard(
            symbol=symbol,
            start_utc=start_utc,
            end_utc=end_utc,
            signal_names=list(ALL_SIGNAL_NAMES),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv: get_daily_scorecard(%s, %s) failed (%s)",
            symbol, prior.isoformat(), exc,
        )
        return None

    if not payload:
        return None

    cards = payload.get("cards") or {}
    best = (payload.get("signals") or {}).get("best")
    regime = payload.get("regime")
    regime_label = _label_regime(regime) if regime else "unknown"

    total = int(cards.get("total") or 0)
    if total == 0 and not best:
        return (
            f"${symbol} had a quiet {prior.strftime('%A')} — no playbook calls "
            f"fired, closing regime {regime_label}."
        )

    parts: list[str] = []
    if total:
        parts.append(f"{total} Playbook call{'s' if total != 1 else ''}")
    if best and best.get("name"):
        parts.append(
            f"best signal {best['name'].replace('_', ' ')} "
            f"({_fmt_pct(best.get('avg_directional_return'))})"
        )
    if regime_label and regime_label != "unknown":
        parts.append(f"closing regime {regime_label}")
    return f"${symbol}, {prior.strftime('%A')}: " + ", ".join(parts) + "."


def _extract_pattern_hint(
    forecast_rows: list[tuple[str, Optional[dict[str, Any]]]],
) -> Optional[DailyPatternHint]:
    """Pick one flagship_setup off today's forecasts to feature.

    Preference: whichever symbol has a populated ``flagship_setup`` first.
    All three symbols may skip if none has a setup — the section elides."""
    for sym, row in forecast_rows:
        if not row:
            continue
        setup = row.get("flagship_setup")
        if not setup:
            continue
        # flagship_setup is a JSONB blob whose shape mirrors the Action
        # Card. We render just the human-readable summary.
        name = str(setup.get("action") or setup.get("name") or "Playbook setup")
        rationale = str(
            setup.get("summary")
            or setup.get("rationale")
            or "Setup identified in the morning forecast — check the "
               "on-site card for full context."
        )
        return DailyPatternHint(
            name=name.replace("_", " ").title(),
            symbol=sym,
            rationale=rationale,
        )
    return None


async def build_daily_payload(
    db: DatabaseManager,
    day: date,
    lead_symbol: str,
    symbols: tuple[str, ...],
    upgrade_url: str,
) -> DailyPayload:
    """Assemble every field the daily template renders.

    Never raises — a subquery that fails degrades that section. The
    caller can still get a partial email out (which is better than
    nothing when we're going out at 08:30 ET before market open)."""
    forecast_rows_task = _fetch_daily_symbol_forecasts(db, day, symbols)
    recap_task = asyncio.ensure_future(_build_yesterday_recap(db, day, lead_symbol))
    forecast_rows = await forecast_rows_task
    recap = await recap_task

    levels: list[DailyLevelRow] = []
    for sym, row in forecast_rows:
        levels.append(DailyLevelRow(
            symbol=sym,
            gamma_flip=_row_gamma_flip(row),
            call_wall=_row_call_wall(row),
            put_wall=_row_put_wall(row),
        ))

    pattern = _extract_pattern_hint(forecast_rows)

    return DailyPayload(
        headline_date=day.strftime("%b %-d, %Y"),
        recap=recap,
        levels=levels,
        pattern=pattern,
        upgrade_url=upgrade_url,
    )


# ---------------------------------------------------------------------------
# Weekly payload — pulls from playbook_pattern_stats + backtest_runs.
# ---------------------------------------------------------------------------


def _fetch_weekly_accuracy_rows(underlying: str, top_n: int = 6) -> list[WeeklyAccuracyRow]:
    """Read the top-N most-resolved patterns from playbook_pattern_stats.

    Uses the sync ``get_pattern_insights`` helper the /api/backtesting
    router already ships — same code path, same numbers the site shows
    on its leaderboard. Returns an empty list on failure (section elides
    in the rendered template)."""
    try:
        from src.backtesting.queries import get_pattern_insights
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv: could not import pattern insights helper (%s)", exc,
        )
        return []

    try:
        rows = get_pattern_insights(underlying=underlying)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv: get_pattern_insights(%s) failed (%s)", underlying, exc,
        )
        return []

    # Filter to rows with at least a handful of resolved samples so the
    # hit-rate isn't statistically meaningless.
    filtered = [r for r in rows if int(r.get("n_resolved") or 0) >= 5]
    filtered.sort(
        key=lambda r: (int(r.get("n_resolved") or 0), float(r.get("hit_rate") or 0.0)),
        reverse=True,
    )
    return [
        WeeklyAccuracyRow(
            pattern=str(r.get("pattern") or "?"),
            underlying=str(r.get("underlying") or underlying),
            n_resolved=int(r.get("n_resolved") or 0),
            hit_rate=_fmt_pct(r.get("hit_rate")),
        )
        for r in filtered[:top_n]
    ]


def _pick_deep_dive(rows: list[WeeklyAccuracyRow]) -> Optional[WeeklyDeepDive]:
    """The weekly deep dive spotlights whichever pattern had the highest
    hit rate on the smallest-still-meaningful sample. We do NOT go find
    a fresh backtest run to write the deep-dive prose — that's an
    editor's job. This function stubs a description grounded in the
    numbers so the email lands even if no operator hand-edit happened."""
    if not rows:
        return None
    winner = rows[0]
    return WeeklyDeepDive(
        pattern=winner.pattern,
        setup=(
            f"{winner.pattern} triggered {winner.n_resolved} times on ${winner.underlying} "
            "over the past week."
        ),
        entry=(
            "Entry conditions from the site's Playbook page — see the pattern's "
            "detail card for the exact criteria."
        ),
        exit=(
            "Exits track the same rules the Playbook publishes: profit-target "
            "hit, stop hit, or time-based unwind at close."
        ),
        pnl=(
            f"Hit rate on resolved trades: {winner.hit_rate}."
        ),
    )


def _fetch_weekly_landscape() -> Optional[str]:
    """Very short prose paragraph on next week's macro-adjacent
    landscape. Derived from calendar flags only (OPEX, VIX expiration,
    holiday-shortened week). Deliberately conservative — we don't render
    macro speculation, just facts about the calendar the reader can
    verify."""
    from src.jobs.forecast_calendar import (
        days_to_next_opex, is_monthly_opex_friday, is_vix_expiration_day,
    )
    today = _today_et()
    # Compute for the coming Wednesday — a rough "middle of next week" anchor.
    anchor = today + timedelta(days=(2 - today.weekday()) % 7)
    facts: list[str] = []
    days = days_to_next_opex(anchor)
    if days is not None:
        if days == 0:
            facts.append("this week contains a monthly OPEX")
        elif days <= 5:
            facts.append(f"monthly OPEX is {days} session(s) out")
    if is_monthly_opex_friday(anchor + timedelta(days=2)):
        facts.append("Friday is a monthly OPEX Friday")
    if is_vix_expiration_day(anchor):
        facts.append("Wednesday carries a VIX expiration")
    if not facts:
        return None
    return "Calendar notes for next week: " + "; ".join(facts) + "."


def _fetch_weekly_postmortem() -> Optional[str]:
    """One-paragraph post-mortem of the most recent backtest run.

    Uses ``list_runs`` (anonymous) to grab the newest completed run and
    format its summary into a sentence. Elides quietly when no runs
    exist or the fetch fails."""
    try:
        from src.backtesting.queries import list_runs
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv: could not import list_runs helper (%s)", exc,
        )
        return None
    try:
        runs = list_runs(end_user=None, limit=1)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv: list_runs failed (%s)", exc,
        )
        return None
    if not runs:
        return None
    run = runs[0]
    summary = run.get("summary")
    if isinstance(summary, dict):
        net = summary.get("net_pnl") or summary.get("total_pnl")
        trades = summary.get("n_trades") or summary.get("total_trades")
        pf = summary.get("profit_factor")
        parts = []
        if trades is not None:
            parts.append(f"{trades} trades")
        if net is not None:
            parts.append(f"net {net}")
        if pf is not None:
            parts.append(f"PF {pf}")
        if parts:
            return (
                f"Last week's parameter sweep run #{run.get('run_id')} on "
                f"${run.get('underlying')}: " + ", ".join(str(p) for p in parts) + "."
            )
    return (
        f"Last week's backtest run #{run.get('run_id')} on ${run.get('underlying')} "
        f"completed with status {run.get('status')}."
    )


async def build_weekly_payload(
    db: DatabaseManager,
    day: date,
    lead_symbol: str,
) -> WeeklyPayload:
    """Assemble every field the weekly template renders.

    Some helpers are sync (they use the psycopg2 pool); we hop through
    ``asyncio.to_thread`` to keep the event loop responsive."""
    week_start = _monday_of_week(day)
    accuracy_rows = await asyncio.to_thread(_fetch_weekly_accuracy_rows, lead_symbol)
    deep_dive = _pick_deep_dive(accuracy_rows)
    landscape = _fetch_weekly_landscape()
    postmortem = await asyncio.to_thread(_fetch_weekly_postmortem)

    return WeeklyPayload(
        week_start_label=week_start.strftime("%b %-d, %Y"),
        accuracy_rows=accuracy_rows,
        deep_dive=deep_dive,
        landscape=landscape,
        postmortem=postmortem,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _render_template(template_name: str, context: dict[str, Any]) -> str:
    """Render a Jinja2 template from src/jobs/templates/beehiiv/.

    Deliberately imports jinja2 lazily so the module can be imported in
    a test process without jinja2 installed — the tests that don't touch
    rendering can still run. HTML escaping is ON (autoescape=True) so a
    malicious flagship_setup blob can't inject arbitrary markup."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "j2"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(template_name)
    return template.render(**context)


def render_daily_html(payload: DailyPayload) -> str:
    return _render_template("daily_card.html.j2", payload.__dict__)


def render_weekly_html(payload: WeeklyPayload) -> str:
    return _render_template("weekly_review.html.j2", payload.__dict__)


# ---------------------------------------------------------------------------
# Beehiiv publish
# ---------------------------------------------------------------------------


def _subject_line(cadence: str, day: date, payload: Any) -> str:
    if cadence == CADENCE_DAILY:
        return f"GEX Morning Card — {day.strftime('%b %-d')}"
    monday = _monday_of_week(day)
    return f"Sunday Playbook Review — Week of {monday.strftime('%b %-d')}"


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _post_payload(
    *,
    cadence: str,
    subject: str,
    body_html: str,
    audience: str,
    scheduled_at_iso: str,
    confirm: bool,
) -> dict[str, Any]:
    """The JSON body sent to Beehiiv's create-post endpoint.

    ``status`` is ``confirmed`` when the publication auto-sends on
    schedule (most Beehiiv setups) and ``draft`` when the caller wants
    to fire a follow-up confirm PATCH — we pass whichever the operator
    configured via ``--confirm``. See prompt/Beehiiv API primer."""
    return {
        "title": subject,
        "subtitle": (
            "Derived dealer-gamma analytics for U.S. index options. "
            "No raw exchange quotes redistributed."
        ),
        "body_html": body_html,
        "subject_line": subject,
        "audience": audience,
        "status": "draft" if confirm else "confirmed",
        "scheduled_at": scheduled_at_iso,
        "content_tags": [
            "zerogex",
            "options",
            "gamma",
            "market-analytics",
        ],
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


@dataclass
class RunConfig:
    cadence: str
    day: date
    lead_symbol: str
    symbols: tuple[str, ...]
    upgrade_url: str
    dry_run: bool
    post: bool
    confirm: bool
    allow_non_trading_day: bool


async def _run(cfg: RunConfig) -> int:
    if cfg.cadence == CADENCE_DAILY:
        if not _is_trading_day(cfg.day) and not cfg.allow_non_trading_day:
            logger.info(
                "publish_beehiiv[daily]: %s is not a trading day — exit 0",
                cfg.day.isoformat(),
            )
            return 0
    elif cfg.cadence == CADENCE_WEEKLY:
        # Sunday only — a fire on any other day is a mis-scheduled cron.
        if cfg.day.weekday() != 6 and not cfg.allow_non_trading_day:
            logger.info(
                "publish_beehiiv[weekly]: %s is not a Sunday — exit 0",
                cfg.day.isoformat(),
            )
            return 0
    else:
        logger.warning("publish_beehiiv: unknown cadence %r — exit 0", cfg.cadence)
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "publish_beehiiv[%s]: DB connect failed (%s) — exit 0", cfg.cadence, exc,
        )
        return 0

    try:
        # Idempotency check — a row with status 'published' means today's
        # cadence already went out and we should not re-fire.
        existing = await db.get_newsletter_publication(cfg.cadence, cfg.day)
        if existing and existing.get("status") == "published" and not cfg.dry_run:
            logger.info(
                "publish_beehiiv[%s]: already published for %s (post_id=%s) — skip",
                cfg.cadence, cfg.day.isoformat(), existing.get("beehiiv_post_id"),
            )
            return 0

        if cfg.cadence == CADENCE_DAILY:
            payload = await build_daily_payload(
                db, cfg.day, cfg.lead_symbol, cfg.symbols, cfg.upgrade_url,
            )
            body_html = render_daily_html(payload)
            audience = AUDIENCE_FREE
        else:
            weekly_payload = await build_weekly_payload(db, cfg.day, cfg.lead_symbol)
            body_html = render_weekly_html(weekly_payload)
            audience = AUDIENCE_PREMIUM

        subject = _subject_line(cfg.cadence, cfg.day, payload if cfg.cadence == CADENCE_DAILY else weekly_payload)
        body_sha = _sha256_hex(body_html)

        api_key = os.environ.get("BEEHIIV_API_KEY", "").strip()
        publication_id = os.environ.get("BEEHIIV_PUBLICATION_ID", "").strip()

        if cfg.dry_run or not cfg.post or not api_key or not publication_id:
            reason = (
                "--dry-run" if cfg.dry_run
                else "no --post flag" if not cfg.post
                else "BEEHIIV_API_KEY unset" if not api_key
                else "BEEHIIV_PUBLICATION_ID unset"
            )
            logger.info(
                "publish_beehiiv[%s]: DRY RUN (%s) subject=%r sha=%s length=%d",
                cfg.cadence, reason, subject, body_sha[:12], len(body_html),
            )
            logger.info("publish_beehiiv[%s]: HTML preview:\n%s", cfg.cadence, body_html)
            # Leave a 'pending' receipt so a subsequent live run
            # transitions in place rather than colliding on the unique
            # constraint.
            await db.upsert_newsletter_publication(
                cadence=cfg.cadence,
                audience=audience,
                publication_date=cfg.day,
                subject_line=subject,
                body_html_sha256=body_sha,
                status="pending",
            )
            return 0

        client = BeehiivClient(api_key=api_key, publication_id=publication_id)
        scheduled_at = (datetime.now(tz=ET) + timedelta(minutes=2)).isoformat()
        try:
            resp = client.create_post(_post_payload(
                cadence=cfg.cadence,
                subject=subject,
                body_html=body_html,
                audience=audience,
                scheduled_at_iso=scheduled_at,
                confirm=cfg.confirm,
            ))
        except BeehiivError as exc:
            logger.warning(
                "publish_beehiiv[%s]: Beehiiv create_post failed (status=%s request_id=%s) — %s",
                cfg.cadence, exc.status, exc.request_id, exc,
            )
            await db.upsert_newsletter_publication(
                cadence=cfg.cadence,
                audience=audience,
                publication_date=cfg.day,
                subject_line=subject,
                body_html_sha256=body_sha,
                status="failed",
                error_message=f"HTTP {exc.status}: {(exc.body or str(exc))[:1000]}",
            )
            return 0

        data = resp.data.get("data") or resp.data
        post_id = str(data.get("id") or "") or None
        beehiiv_url = str(data.get("web_url") or data.get("url") or "") or None

        if cfg.confirm and post_id:
            try:
                client.confirm_post(post_id)
            except BeehiivError as exc:
                logger.warning(
                    "publish_beehiiv[%s]: confirm_post failed for post_id=%s — %s",
                    cfg.cadence, post_id, exc,
                )
                await db.upsert_newsletter_publication(
                    cadence=cfg.cadence,
                    audience=audience,
                    publication_date=cfg.day,
                    subject_line=subject,
                    body_html_sha256=body_sha,
                    status="failed",
                    beehiiv_post_id=post_id,
                    beehiiv_url=beehiiv_url,
                    error_message=f"confirm HTTP {exc.status}: {(exc.body or str(exc))[:1000]}",
                )
                return 0

        await db.upsert_newsletter_publication(
            cadence=cfg.cadence,
            audience=audience,
            publication_date=cfg.day,
            subject_line=subject,
            body_html_sha256=body_sha,
            status="published",
            beehiiv_post_id=post_id,
            beehiiv_url=beehiiv_url,
        )
        logger.info(
            "publish_beehiiv[%s]: published for %s post_id=%s url=%s",
            cfg.cadence, cfg.day.isoformat(), post_id, beehiiv_url,
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
        "--cadence",
        required=True,
        choices=(CADENCE_DAILY, CADENCE_WEEKLY),
        help="Which newsletter cadence to publish.",
    )
    parser.add_argument(
        "--date",
        help="Publication date in America/New_York (YYYY-MM-DD). Defaults to today ET.",
    )
    parser.add_argument(
        "--lead-symbol",
        default=os.environ.get("PUBLISH_BEEHIIV_LEAD_SYMBOL", "SPX"),
        help="Primary symbol used for the yesterday recap and pattern picks (default SPX).",
    )
    parser.add_argument(
        "--symbols",
        default=os.environ.get("PUBLISH_BEEHIIV_SYMBOLS", ",".join(DEFAULT_SYMBOLS)),
        help="Comma-separated symbols to render in the daily level table (default SPX,SPY,QQQ).",
    )
    parser.add_argument(
        "--upgrade-url",
        default=os.environ.get(
            "PUBLISH_BEEHIIV_UPGRADE_URL",
            DEFAULT_UPGRADE_URL,
        ),
        help="Paid checkout URL surfaced in the daily upsell CTA.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render + log HTML but do NOT call Beehiiv. Sets a status='pending' row.",
    )
    parser.add_argument(
        "--post",
        action="store_true",
        help="Actually publish. Requires BEEHIIV_API_KEY + BEEHIIV_PUBLICATION_ID env vars.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Publications that need a manual confirm step: post as draft, then PATCH confirmed.",
    )
    parser.add_argument(
        "--allow-non-trading-day",
        action="store_true",
        help="Bypass the weekday / Sunday guard — useful for backfill and manual dry-runs.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)
    day = date.fromisoformat(args.date) if args.date else _today_et()
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip()) or DEFAULT_SYMBOLS
    cfg = RunConfig(
        cadence=args.cadence,
        day=day,
        lead_symbol=args.lead_symbol.upper(),
        symbols=symbols,
        upgrade_url=args.upgrade_url,
        dry_run=args.dry_run,
        post=args.post,
        confirm=args.confirm,
        allow_non_trading_day=args.allow_non_trading_day,
    )
    return asyncio.run(_run(cfg))


if __name__ == "__main__":
    sys.exit(main())
