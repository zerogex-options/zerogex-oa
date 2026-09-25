"""Intraday cone auto-tweet — the receipt Quant Data structurally cannot post.

One mode, ``--mode receipt``, fires after the close once the day's cones are
graded.  It publishes the running record first and the day's number inside it:

    Intraday bands, committed live and graded on the tape.

    Today: 84 committed, 61 held - said 74%, got 73%.
    Record (12 sessions): 3,940 committed, 3,196 held - said 82%, got 81%.

    Held = price never left the band.
    https://zerogex.io/forecast/cone

Why the record leads and the day is nested inside it
----------------------------------------------------
A single session is a coin flip on how it reads.  The vol anchor is a trailing
median, so it cannot see a breakout coming, and on a regime-break day the cone
misses by twenty points and no amount of copy makes that look good.  Leading
with the cumulative number is not softening that: every session is in the
cumulative, the bad ones included, so there is nothing selected and nothing
withheld.  It just puts one session in the frame it belongs in - a data point
inside a graded record rather than a verdict on its own.

The aggregate is across every symbol, never a chosen subset.  Picking the
symbols that happen to look calibrated is the exact behaviour this whole
feature exists to not do.  The per-symbol breakdown - including any symbol
that is NOT beating its baseline - lives on the linked page, which is one
click away and publishes it whether it flatters us or not.

The publication gate
--------------------
This job refuses to post a record that has not earned the word "record".
Before any live post it scores the cumulative sample against the honest
strawman - always predict the base rate - and stands down if the cone does not
beat it, or if the sample is too thin to say.  A calibrated-sounding number
with no skill behind it is worse than silence, because it spends credibility
that the graded receipt is supposed to be building.

The gate is checked in code rather than left to an operator's judgement so
that a timer running unattended cannot talk its way past it.

Design rules shared with ``forecast_tweet`` / ``scorecard_tweet``:
  * Never throw.  Every failure path logs and exits 0 so the timer keeps
    running tomorrow.
  * Dry-run by default.  Live posting requires BOTH ``--post`` AND
    ``X_BOT_BEARER_TOKEN`` - defence in depth, so a half-configured rollout
    cannot accidentally tweet.
  * Skip silently on non-trading days and on days with nothing graded.

``--preview-sessions N`` renders what this job WOULD have tweeted on each of
the last N sessions, so the copy can be read against real history before a
single post goes out.  The cumulative in a preview row is computed as of that
session's close and no later: a preview that quoted today's totals on a date
three weeks ago would be reporting a record that did not exist yet, which is
the same lookahead that made the first cone backfill worthless.
"""

from __future__ import annotations

import argparse
import asyncio
import html
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.jobs.intraday_cone_model import (
    MIN_BRIER_SKILL,
    base_rate_brier,
    beats_base_rate,
    brier_skill,
    calibration_error,
)
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.cone_tweet")
ET = ZoneInfo("America/New_York")

DEFAULT_SITE_URL = "https://zerogex.io"
DEFAULT_SYMBOLS = "SPY,QQQ,SPX,NDX"
TWEET_MAX_LEN = 280

#: Minimum cumulative graded claims before the record may be published at all.
#: Deliberately far above the page's MIN_GRADED_FOR_RATES (40): the page is a
#: reader who came looking and can see the sample size next to the number, a
#: tweet is a claim pushed at people who cannot.  At ~80 claims a session this
#: is roughly three sessions of data, and the session floor below makes sure
#: they are three DIFFERENT days rather than one busy one.
MIN_CLAIMS_FOR_PUBLICATION = 200

#: Minimum distinct graded sessions behind the record.  Claims inside one
#: session are not independent - one quiet afternoon can produce 80 holds that
#: say nothing about the next day - so the sample has to span days, not rows.
MIN_SESSIONS_FOR_PUBLICATION = 5

#: The skill floor and the verdict live in intraday_cone_model alongside
#: the other scoring helpers, so the published /reliability endpoint and
#: this job can never disagree about whether a symbol earned its record.
#: Re-exported here because the gate reads as a property of the tweet.
_MIN_SKILL = MIN_BRIER_SKILL

#: Idempotency markers - one file per session date recording a successful live
#: post.  Mirrors forecast_tweet's convention (and fails OPEN the same way, on
#: the same reasoning: a rare duplicate beats a silently-never-posted receipt).
DEFAULT_TWEET_STATE_DIR = "/var/lib/zerogex-oa/cone-tweets"


def _tweet_state_dir() -> Path:
    override = os.environ.get("CONE_TWEET_STATE_DIR", "").strip()
    return Path(override) if override else Path(DEFAULT_TWEET_STATE_DIR)


def _posted_marker_path(day: date) -> Path:
    return _tweet_state_dir() / f"receipt-{day.isoformat()}.json"


def _already_posted(day: date) -> Optional[dict[str, Any]]:
    path = _posted_marker_path(day)
    try:
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - fail open
        return None


def _record_posted(day: date, tweet_id: Optional[str]) -> None:
    path = _posted_marker_path(day)
    payload = {
        "mode": "receipt",
        "date": day.isoformat(),
        "tweet_id": tweet_id,
        "posted_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001 - best effort
        # WARNING for the same reason forecast_tweet's is: a marker that did
        # not persist means the guard against a duplicate post is silently off.
        logger.warning(
            "cone_tweet: could not record post marker %s (%s) — duplicate "
            "protection is OFF for this post", path, exc,
        )


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in NYSE_HOLIDAYS


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def summarize(claims: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Score one set of graded claims.

    ``beats_baseline`` compares the cone's Brier against always predicting the
    realized base rate.  That strawman is fitted in-sample and so is the most
    generous version of itself - beating it means the cone's PER-CLAIM
    confidence carried information, not just that ~80% of bands hold.

    The verdict is a skill MARGIN, not a bare inequality, and both sides of the
    comparison are computed unrounded.  Rounding one side and not the other
    manufactured a win out of float noise in an earlier revision of this
    function: ``0.15999999999999998 < 0.16`` is True, and the two symbols where
    that mattered are exactly the two sitting on the line in live data.
    """
    pairs = [
        (float(c["hold_prob"]), bool(c["held"]))
        for c in claims
        if c.get("hold_prob") is not None and c.get("held") is not None
    ]
    n = len(pairs)
    if n == 0:
        return {
            "n": 0, "held_n": 0, "sessions": 0, "hold_rate": None,
            "mean_predicted": None, "brier": None, "baseline_brier": None,
            "brier_skill": None, "beats_baseline": None,
            "calibration_error": None,
        }
    held_n = sum(1 for _, h in pairs if h)
    brier = sum((p - (1.0 if h else 0.0)) ** 2 for p, h in pairs) / n
    return {
        "n": n,
        "held_n": held_n,
        "sessions": len({c["session_date"] for c in claims if c.get("session_date")}),
        "hold_rate": held_n / n,
        "mean_predicted": sum(p for p, _ in pairs) / n,
        "brier": round(brier, 6),
        # All three via the shared helpers, so every number here matches what
        # /reliability publishes for the same claims.
        "baseline_brier": base_rate_brier(pairs),
        "brier_skill": brier_skill(pairs),
        "beats_baseline": beats_base_rate(pairs),
        "calibration_error": calibration_error(pairs),
    }


def _symbol_shortfall(row: dict[str, Any]) -> Optional[str]:
    """Why this symbol has not earned its place in the record, or None."""
    n = row.get("n") or 0
    if n < MIN_CLAIMS_FOR_PUBLICATION:
        return f"{row['symbol']} ({n:,} claims)"
    if (row.get("sessions") or 0) < MIN_SESSIONS_FOR_PUBLICATION:
        return f"{row['symbol']} ({row['sessions']} sessions)"
    if row.get("beats_baseline") is None:
        return f"{row['symbol']} (no baseline to beat)"
    if not row["beats_baseline"]:
        return f"{row['symbol']} (skill {row['brier_skill']:+.4f})"
    return None


def publication_gate(
    cume: dict[str, Any], per_symbol: Sequence[dict[str, Any]]
) -> tuple[bool, str]:
    """May this record be published?  Returns ``(ok, reason)``.

    Checked before every live post.  The reason is logged either way, so a
    stood-down day leaves the same trail as a posted one.

    EVERY symbol has to clear the bar, not just the pooled aggregate.  The
    aggregate alone is a weaker test than it looks: over the 20 sessions to
    2026-09-24 it scores +2.9% skill while SPX (+0.02%) and NDX (-0.04%) are
    dead heats with their own base rate, carried by SPY (+7.9%) and QQQ
    (+3.6%).  The tweet's number covers all four, so a reader is entitled to
    assume all four earned it.  Pooling until the claim comes out true is the
    same move as choosing the symbols that look good, run in reverse.

    ``per_symbol`` is required rather than optional on purpose: an optional
    argument would leave a lenient path that a caller could take by forgetting
    it, and the lenient path is the one that publishes something unearned.

    A symbol with too little data blocks rather than being skipped.  Skipping
    would let a symbol sit inside the tweeted aggregate while being excluded
    from the test the aggregate is supposed to pass — which is precisely the
    hole this gate exists to close.  "We do not know yet" is a reason not to
    publish, not a reason to look away.
    """
    n = cume.get("n") or 0
    sessions = cume.get("sessions") or 0
    if n < MIN_CLAIMS_FOR_PUBLICATION:
        return False, (
            f"sample too thin: {n} graded claims < {MIN_CLAIMS_FOR_PUBLICATION}"
        )
    if sessions < MIN_SESSIONS_FOR_PUBLICATION:
        return False, (
            f"too few sessions: {sessions} < {MIN_SESSIONS_FOR_PUBLICATION} "
            "(claims inside one session are not independent)"
        )
    if cume.get("beats_baseline") is None:
        return False, (
            "no baseline to beat: every claim resolved the same way, so the "
            "strawman is already perfect and skill is undefined"
        )
    if not cume["beats_baseline"]:
        return False, (
            f"Brier skill {cume['brier_skill']:+.4f} is below the "
            f"{MIN_BRIER_SKILL:.2f} floor (cone {cume['brier']:.4f} vs "
            f"base-rate {cume['baseline_brier']:.4f}) - too close to the "
            "strawman to call it a record, so it stays unpublished"
        )
    shortfalls = [s for s in map(_symbol_shortfall, per_symbol) if s]
    if shortfalls:
        return False, (
            "the aggregate clears the bar but these symbols do not: "
            + ", ".join(shortfalls)
            + f" — the record covers all {len(per_symbol)}, so one that has "
            "not earned its place blocks the whole claim"
        )

    cleared = ", ".join(
        f"{r['symbol']} {r['brier_skill']:+.4f}" for r in per_symbol
    )
    return True, (
        f"Brier skill {cume['brier_skill']:+.4f} over the base rate "
        f"(cone {cume['brier']:.4f} vs {cume['baseline_brier']:.4f}) "
        f"across {n:,} claims / {sessions} sessions; every symbol clears "
        f"[{cleared}]"
    )


# ---------------------------------------------------------------------------
# Tweet builder
# ---------------------------------------------------------------------------


def _pct(value: Optional[float]) -> str:
    return "—" if value is None else f"{value * 100:.0f}%"


def build_receipt_tweet(
    day: dict[str, Any],
    cume: dict[str, Any],
    site_url: str = DEFAULT_SITE_URL,
) -> str:
    """Cumulative-led receipt, with the day nested inside it.

    Trims from the bottom up when the copy overflows: the definition line goes
    first, then the day line.  The record line and the link never go - they are
    the claim and the place to check it.
    """
    permalink = f"{site_url.rstrip('/')}/forecast/cone"
    lead = "Intraday bands, committed live and graded on the tape."

    # None means "this line does not exist"; "" means "blank line here".  The
    # distinction matters: an earlier revision used "" for both and the
    # assembler's truthiness filter silently ate every paragraph break.
    day_line: Optional[str] = None
    if (day.get("n") or 0) > 0:
        day_line = (
            f"Today: {day['n']:,} committed, {day['held_n']:,} held — "
            f"said {_pct(day['mean_predicted'])}, got {_pct(day['hold_rate'])}."
        )

    sessions = cume.get("sessions") or 0
    session_word = "session" if sessions == 1 else "sessions"
    record_line = (
        f"Record ({sessions} {session_word}): {cume['n']:,} committed, "
        f"{cume['held_n']:,} held — said {_pct(cume['mean_predicted'])}, "
        f"got {_pct(cume['hold_rate'])}."
    )
    definition = "Held = price never left the band."

    def _assemble(parts: Iterable[Optional[str]]) -> str:
        body = "\n".join(p for p in parts if p is not None)
        return f"{body}\n{permalink}"

    for parts in (
        [lead, "", day_line, record_line, "", definition, ""],
        [lead, "", day_line, record_line, ""],
        [lead, "", record_line, ""],
        [record_line, ""],
    ):
        text = _assemble(parts)
        if len(text) <= TWEET_MAX_LEN:
            return text
    # Unreachable with a sane site_url; keep the link rather than the prose.
    return _assemble([record_line[: TWEET_MAX_LEN - len(permalink) - 2], ""])


# ---------------------------------------------------------------------------
# X API client - mirrors forecast_tweet.post_tweet_via_x_api
# ---------------------------------------------------------------------------


def post_tweet_via_x_api(
    text: str, bearer_token: str, timeout_seconds: int = 15
) -> dict[str, Any]:
    req = Request(
        "https://api.x.com/2/tweets",
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "User-Agent": "zerogex-cone-tweet/1.0",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body) if body else {}


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


async def fetch_claims(
    db: DatabaseManager, symbols: Sequence[str], since: date
) -> list[dict[str, Any]]:
    """Every graded claim for ``symbols`` since ``since``, symbol attached.

    One query per symbol because that is the shape the reliability endpoint
    already uses; the volume is a few thousand rows once a day.
    """
    out: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            rows = await db.get_graded_cone_history(symbol, since)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "cone_tweet: get_graded_cone_history(%s) failed (%s) — skipping",
                symbol, exc,
            )
            continue
        for row in rows:
            row["symbol"] = symbol
            out.append(row)
    return out


def _split(
    claims: Sequence[dict[str, Any]], day: date
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(today's claims, cumulative-through-today's claims)``.

    The cumulative is bounded at ``day`` rather than taking everything in the
    fetch window, so a preview of an old session reports the record as it stood
    that evening and not as it stands now.
    """
    today = [c for c in claims if c.get("session_date") == day]
    cume = [
        c for c in claims
        if c.get("session_date") is not None and c["session_date"] <= day
    ]
    return today, cume


def per_symbol_breakdown(claims: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Diagnostic only — never tweeted, always shown to the operator.

    The aggregate can read calibrated while one symbol is badly biased and
    another cancels it out.  The tweet links to the page that publishes this;
    the preview prints it so nobody decides to go live without having seen it.
    """
    rows = []
    for symbol in sorted({c["symbol"] for c in claims if c.get("symbol")}):
        stats = summarize([c for c in claims if c.get("symbol") == symbol])
        stats["symbol"] = symbol
        rows.append(stats)
    return rows


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt_stats(label: str, s: dict[str, Any]) -> str:
    if not s.get("n"):
        return f"  {label:<10} (nothing graded)"
    gap = (s["hold_rate"] - s["mean_predicted"]) * 100
    skill = s.get("brier_skill")
    verdict = (
        "beats baseline" if s["beats_baseline"]
        else "NO SKILL vs baseline" if s["beats_baseline"] is False
        else "baseline n/a"
    )
    return (
        f"  {label:<10} n={s['n']:>5,}  held={s['hold_rate'] * 100:>5.1f}%  "
        f"said={s['mean_predicted'] * 100:>5.1f}%  gap={gap:>+5.1f}pts  "
        f"brier={s['brier']:.4f} vs {s['baseline_brier']:.4f}  "
        f"skill={'  n/a' if skill is None else format(skill, '+.4f')}  {verdict}"
    )


def render_preview(
    day: date,
    day_stats: dict[str, Any],
    cume: dict[str, Any],
    tweet_text: str,
    gate_ok: bool,
    gate_reason: str,
    breakdown: Sequence[dict[str, Any]],
) -> str:
    banner = "WOULD POST" if gate_ok else "WOULD STAND DOWN"
    lines = [
        "=" * 72,
        f"{day.isoformat()}   [{banner}]   {gate_reason}",
        "=" * 72,
        tweet_text,
        f"--- {len(tweet_text)} chars ---",
        "",
        "  not tweeted, shown so the aggregate can't hide a biased symbol:",
        _fmt_stats("TODAY", day_stats),
        _fmt_stats("RECORD", cume),
    ]
    lines.extend(_fmt_stats(r["symbol"], r) for r in breakdown)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Draft email - the trial mode that runs before anything is ever posted
# ---------------------------------------------------------------------------
#
# The point of this mode is to watch the job for a couple of weeks and judge
# the copy against real sessions before a single tweet goes out.  So the email
# is sent on EVERY trading day, whether the gate passed or stood down, and the
# stand-down days are the more interesting ones: they are the days that show
# how far SPX and NDX still are from clearing.  An email that only arrived on
# the good days would answer the wrong question.
#
# Emailing a draft to yourself is not publishing, so the publication gate does
# not govern it.  The gate governs --post.  The two are deliberately separate:
# the trial can run for as long as it needs to without ever risking a post.

#: Falls back to the bulletin job's recipient so this works with no new config
#: at all - RESEND_API_KEY and RESEND_FROM_EMAIL are already set for that job.
def _email_recipient() -> str:
    for var in ("CONE_TWEET_EMAIL_TO", "BULLETIN_TWEET_EMAIL_TO"):
        value = os.environ.get(var, "").strip()
        if value:
            return value
    return ""


def build_draft_email(
    day: date,
    day_stats: dict[str, Any],
    cume: dict[str, Any],
    breakdown: Sequence[dict[str, Any]],
    tweet_text: str,
    gate_ok: bool,
    gate_reason: str,
) -> tuple[str, str, str]:
    """``(subject, html, text)`` for one day's draft.

    The subject carries the verdict and, when it stood down, the symbols
    responsible - so a fortnight of these can be judged from the inbox list
    without opening any of them.
    """
    if gate_ok:
        verdict = "would POST"
    else:
        short = [r["symbol"] for r in breakdown if _symbol_shortfall(r)]
        verdict = f"would STAND DOWN ({', '.join(short)})" if short else "would STAND DOWN"
    subject = f"Cone draft {day.isoformat()} - {verdict}"

    rows = "".join(
        "<tr>"
        f"<td style=\"padding:4px 10px 4px 0\"><b>{html.escape(r['symbol'])}</b></td>"
        f"<td style=\"padding:4px 10px 4px 0;text-align:right\">{r['n']:,}</td>"
        f"<td style=\"padding:4px 10px 4px 0;text-align:right\">"
        f"{(r['hold_rate'] or 0) * 100:.1f}%</td>"
        f"<td style=\"padding:4px 10px 4px 0;text-align:right\">"
        f"{(r['mean_predicted'] or 0) * 100:.1f}%</td>"
        f"<td style=\"padding:4px 10px 4px 0;text-align:right\">"
        f"{'n/a' if r.get('brier_skill') is None else format(r['brier_skill'], '+.4f')}</td>"
        f"<td style=\"padding:4px 0;color:{'#137333' if r['beats_baseline'] else '#b3261e'}\">"
        f"{'clears' if r['beats_baseline'] else 'no skill'}</td>"
        "</tr>"
        for r in breakdown
    )

    body_html = (
        f"<h2 style=\"margin:0 0 4px\">Cone draft &middot; {day.isoformat()}</h2>"
        f"<p style=\"margin:0 0 16px;color:{'#137333' if gate_ok else '#b3261e'};"
        f"font-weight:600\">{html.escape(verdict)}</p>"
        f"<p style=\"margin:0 0 6px;color:#666;font-size:13px\">Nothing was posted. "
        f"This is the copy the job would publish, sent for review only.</p>"
        f"<pre style=\"background:#f6f6f6;border:1px solid #ddd;border-radius:8px;"
        f"padding:14px;white-space:pre-wrap;font-size:14px;line-height:1.5\">"
        f"{html.escape(tweet_text)}</pre>"
        f"<p style=\"margin:0 0 20px;color:#666;font-size:13px\">"
        f"{len(tweet_text)} of {TWEET_MAX_LEN} characters.</p>"
        f"<p style=\"margin:0 0 6px\"><b>Gate</b><br>"
        f"<span style=\"color:#444;font-size:14px\">{html.escape(gate_reason)}</span></p>"
        f"<table style=\"border-collapse:collapse;font-size:14px;margin:16px 0 0\">"
        f"<tr style=\"color:#666;font-size:12px;text-align:left\">"
        f"<th style=\"padding:0 10px 4px 0\">Symbol</th>"
        f"<th style=\"padding:0 10px 4px 0;text-align:right\">Claims</th>"
        f"<th style=\"padding:0 10px 4px 0;text-align:right\">Held</th>"
        f"<th style=\"padding:0 10px 4px 0;text-align:right\">Said</th>"
        f"<th style=\"padding:0 10px 4px 0;text-align:right\">Skill</th>"
        f"<th style=\"padding:0 0 4px\">Verdict</th></tr>{rows}</table>"
    )

    lines = [
        f"Cone draft - {day.isoformat()}",
        verdict,
        "",
        "Nothing was posted. This is the copy the job would publish.",
        "",
        "-" * 60,
        tweet_text,
        "-" * 60,
        f"{len(tweet_text)} of {TWEET_MAX_LEN} characters.",
        "",
        f"Gate: {gate_reason}",
        "",
        _fmt_stats("TODAY", day_stats),
        _fmt_stats("RECORD", cume),
    ]
    lines.extend(_fmt_stats(r["symbol"], r) for r in breakdown)
    return subject, body_html, "\n".join(lines)


def send_email_via_resend(subject: str, body_html: str, body_text: str) -> bool:
    """Best-effort send.  Returns False and logs on any missing config or
    error - a failed email must never fail the job or block tomorrow's run."""
    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    from_email = os.environ.get("RESEND_FROM_EMAIL", "").strip()
    to_email = _email_recipient()
    if not (api_key and from_email and to_email):
        logger.warning(
            "cone_tweet: draft email skipped - need RESEND_API_KEY, "
            "RESEND_FROM_EMAIL and CONE_TWEET_EMAIL_TO (or "
            "BULLETIN_TWEET_EMAIL_TO) in the environment",
        )
        return False

    req = Request(
        "https://api.resend.com/emails",
        data=json.dumps(
            {
                "from": from_email,
                "to": [to_email],
                "subject": subject,
                "html": body_html,
                "text": body_text,
            }
        ).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Resend sits behind Cloudflare, which 403s the default
            # "Python-urllib/x.y" User-Agent as a bot.  Same UA treatment the
            # bulletin job needs for the same reason.
            "User-Agent": "zerogex-cone-tweet/1.0 (+https://zerogex.io)",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            resp.read()
    except (HTTPError, URLError) as exc:
        logger.warning("cone_tweet: draft email failed (%s)", exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("cone_tweet: draft email error (%s)", exc)
        return False
    logger.info("cone_tweet: draft email sent to %s (%s)", to_email, subject)
    return True


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def _run_preview(
    db: DatabaseManager, args: argparse.Namespace, symbols: Sequence[str]
) -> int:
    """Render what this job would have tweeted on each of the last N sessions."""
    lookback = max(args.preview_sessions * 4, 60)
    claims = await fetch_claims(db, symbols, _today_et() - timedelta(days=lookback))
    if not claims:
        logger.warning("cone_tweet: no graded claims found — nothing to preview")
        return 0

    sessions = sorted({c["session_date"] for c in claims if c.get("session_date")})
    for day in sessions[-args.preview_sessions:]:
        today_claims, cume_claims = _split(claims, day)
        day_stats = summarize(today_claims)
        cume_stats = summarize(cume_claims)
        breakdown = per_symbol_breakdown(cume_claims)
        gate_ok, gate_reason = publication_gate(cume_stats, breakdown)
        text = build_receipt_tweet(day_stats, cume_stats, args.site_url)
        print(
            render_preview(
                day, day_stats, cume_stats, text, gate_ok, gate_reason,
                breakdown,
            )
        )
        print()
    return 0


async def _run_live(
    db: DatabaseManager, args: argparse.Namespace, symbols: Sequence[str], day: date
) -> int:
    claims = await fetch_claims(db, symbols, day - timedelta(days=args.window * 2))
    today_claims, cume_claims = _split(claims, day)

    if not today_claims:
        logger.info(
            "cone_tweet: nothing graded for %s yet — skipping", day.isoformat()
        )
        return 0

    day_stats = summarize(today_claims)
    cume_stats = summarize(cume_claims)
    breakdown = per_symbol_breakdown(cume_claims)
    gate_ok, gate_reason = publication_gate(cume_stats, breakdown)
    tweet_text = build_receipt_tweet(day_stats, cume_stats, args.site_url)

    for row in breakdown:
        logger.info("cone_tweet: %s", _fmt_stats(row["symbol"], row).strip())

    # Before the gate, deliberately.  The trial is watching for the days the
    # gate refuses as much as the days it clears, and a draft in your own inbox
    # is not a publication.
    if args.email:
        send_email_via_resend(
            *build_draft_email(
                day, day_stats, cume_stats, breakdown,
                tweet_text, gate_ok, gate_reason,
            )
        )

    if not gate_ok:
        logger.warning(
            "cone_tweet: STANDING DOWN for %s — %s\n----\n%s\n----",
            day.isoformat(), gate_reason, tweet_text,
        )
        return 0

    bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()
    if not args.post or not bearer:
        reason = "no --post flag" if not args.post else "X_BOT_BEARER_TOKEN unset"
        logger.info(
            "cone_tweet: DRY RUN (%s) — gate passed: %s\n----\n%s\n----",
            reason, gate_reason, tweet_text,
        )
        return 0

    if not args.force:
        prior = _already_posted(day)
        if prior is not None:
            logger.info(
                "cone_tweet: %s already posted (id=%s at %s) — skipping "
                "(use --force to repost)",
                day.isoformat(), prior.get("tweet_id"), prior.get("posted_at"),
            )
            return 0

    try:
        resp = post_tweet_via_x_api(tweet_text, bearer)
    except (HTTPError, URLError) as exc:
        logger.warning("cone_tweet: X API call failed (%s) — skipping", exc)
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.warning("cone_tweet: unexpected X API error (%s) — skipping", exc)
        return 0

    tweet_id = (resp.get("data") or {}).get("id")
    _record_posted(day, tweet_id)
    logger.info("cone_tweet: posted tweet id=%s for %s", tweet_id, day.isoformat())
    return 0


async def _run(args: argparse.Namespace) -> int:
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        logger.warning("cone_tweet: no symbols resolved — exiting 0")
        return 0

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("cone_tweet: DB connect failed (%s) — exiting 0", exc)
        return 0

    try:
        if args.preview_sessions:
            return await _run_preview(db, args, symbols)

        day = date.fromisoformat(args.date) if args.date else _today_et()
        if not _is_trading_day(day) and not args.allow_non_trading_day:
            logger.info(
                "cone_tweet: skipping %s — not a trading day", day.isoformat()
            )
            return 0
        return await _run_live(db, args, symbols, day)
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=["receipt"], default="receipt",
        help="Which tweet to build. Only the after-close receipt exists today.",
    )
    parser.add_argument(
        "--symbols", default=os.environ.get("CONE_SYMBOLS", DEFAULT_SYMBOLS),
        help=f"Comma-separated symbols to aggregate (default: {DEFAULT_SYMBOLS}). "
             "The record is across ALL of them — never a chosen subset.",
    )
    parser.add_argument("--date", help="Target session (YYYY-MM-DD). Default: today ET.")
    parser.add_argument(
        "--window", type=int, default=60,
        help="Trailing sessions included in the cumulative record (default 60).",
    )
    parser.add_argument(
        "--preview-sessions", type=int, default=0, metavar="N",
        help="Render what this job WOULD have tweeted on each of the last N "
             "sessions, with the record computed as of that session's close. "
             "Never posts, never touches the DB for writes.",
    )
    parser.add_argument(
        "--email", action="store_true",
        help="Email the day's draft to CONE_TWEET_EMAIL_TO (or "
             "BULLETIN_TWEET_EMAIL_TO) for review. Sends on EVERY trading day, "
             "whether the gate cleared or stood down, because the refused days "
             "are what a trial is for. Independent of --post: this never "
             "publishes anything.",
    )
    parser.add_argument(
        "--post", action="store_true",
        help="Actually post to X. Without this flag the job dry-runs even when "
             "X_BOT_BEARER_TOKEN is set.",
    )
    parser.add_argument(
        "--site-url", default=os.environ.get("ZEROGEX_SITE_URL", DEFAULT_SITE_URL),
        help=f"Permalink host (default {DEFAULT_SITE_URL} or $ZEROGEX_SITE_URL).",
    )
    parser.add_argument(
        "--allow-non-trading-day", action="store_true",
        help="Override the weekend/holiday skip — useful for backfill / testing.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Repost even if this session was already tweeted.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return asyncio.run(_run(_parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
