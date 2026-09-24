"""Live-Bulletin auto-tweet — three daily reads posted to the ZeroGEX X account.

Fires three times per trading day, backed by the same script + three
systemd timers:

  * ``--mode premarket`` at 09:15 ET — the Morning Read, 15 min before the
    cash open.
  * ``--mode midday`` at 12:30 ET — the Midday Read.
  * ``--mode close`` at 16:05 ET — the Post-Market Read, 5 min after the
    cash bell.

Every post features ONE symbol: ``$BULLETIN_TWEET_LEAD_SYMBOL`` (SPY by
default), falling back to the symbol with the cleanest setup only when the
lead has no data (see :func:`select_featured_symbol`).

Every post is:

  * **The live bulletin, attached.**  The job screenshots the exact
    ``GammaReportCard`` the paid /live-bulletin page renders (the frontend
    Playwright helper ``scripts/render-bulletin-png.mjs`` against
    ``/live-bulletin/snapshot/<sym>``) at the moment it fires, and attaches
    that PNG to the post.
  * **Quoting the card's own numbers.**  The helper also returns the levels
    the card drew, and the post's key-levels list and prose are built from
    those, so the post and the picture can't disagree.  On the close read
    those are the next session's map (the day's 0DTE rolled off at the
    bell), and the list is labeled as such; what the levels did during the
    session comes from :mod:`src.jobs.level_history`.
  * **Written from the latest CNBC headlines** (:mod:`src.jobs.cnbc_news`,
    only items from the last ``$BULLETIN_TWEET_NEWS_MAX_AGE_HOURS``) and the
    featured symbol's price action, by Claude (:mod:`src.jobs.bulletin_llm`),
    in plain American English.  No site link or hashtags in the main post;
    the ``https://zerogex.io`` link goes out as a threaded reply.
  * **Reviewed before it goes anywhere.**  Python checks the header, the
    levels list, the characters and the lengths; a second Claude call
    fact-checks every claim against the headlines, the numbers and the card
    image.  Problems the writer can fix go back to it once.

**If anything goes wrong, nothing is posted.**  A missing image, a failed
render, no fresh headlines, a writer or review failure, a problem the
review still finds, a missed deadline, or an X API error all hold the post:
the job writes the draft and the reasons to the artifact dir and the
/admin/x-post review page, emails the operator (Resend), and exits 1 so
systemd marks the run failed.  There is no text-only or template fallback.

Other rules:

* **Dry-run by default.**  Live posting requires the ``--post`` flag (or
  ``--stage`` with ``BULLETIN_TWEET_AUTOPILOT=1``) plus the four OAuth1
  credentials (``X_BOT_API_KEY``, ``X_BOT_API_SECRET``,
  ``X_BOT_ACCESS_TOKEN``, ``X_BOT_ACCESS_TOKEN_SECRET``), which sign both
  the image upload and the post.
* **Skip silently on non-trading days.**  Half-days count as trading days.
* **Artifacts on disk.**  The post, reply, PNG and a manifest (with the
  state and any problems) land under
  ``$BULLETIN_TWEET_ARTIFACT_DIR/<mode>/<date>/``.  Defaults to
  ``/var/lib/zerogex-oa/bulletin-tweets`` and falls back to
  ``$XDG_STATE_HOME`` / ``$HOME/.local/state/zerogex-oa/…`` when the
  primary path isn't writable (dev laptops).

Run manually:
    python -m src.jobs.bulletin_tweet --mode midday            # today, dry-run
    python -m src.jobs.bulletin_tweet --mode close --date 2026-07-03
    python -m src.jobs.bulletin_tweet --mode premarket --post  # live
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import html
import json
import logging
import os
import re
import struct
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
from src.jobs import level_history as lh
from src.jobs.index_projection import implied_index_spot
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.bulletin_tweet")
ET = ZoneInfo("America/New_York")

DEFAULT_SITE_URL = "https://zerogex.io"
# SPY only by default (the operator's spec).  Still configurable to a wider
# set via $BULLETIN_TWEET_SYMBOLS — the admin review page reads this list to
# populate its per-symbol regenerate dropdown.
DEFAULT_SYMBOLS = ("SPY",)
# Fallback symbol to feature when the "cleanest setup" selector can't pick one
# (e.g. no symbol has both a live spot and a level).  Normally the featured
# symbol is chosen at runtime by :func:`select_featured_symbol`.
DEFAULT_LEAD_SYMBOL = "SPY"
# The site link no longer rides in the main post (link-in-body suppresses X
# reach).  Instead it's posted as a threaded reply under every bulletin tweet.
# This is the fixed prefix; the URL is appended from ``--site-url``.
DEFAULT_REPLY_PREFIX = "Free delayed SPY / SPX / QQQ gamma levels:"
# When choosing the featured symbol, proximity to the gamma flip counts more
# than proximity to a wall: the flip is the regime boundary — where dealer
# hedging (and therefore the tape's whole character) flips sign — so a symbol
# straddling its flip is a stronger story than one merely pinned to a wall.
# The flip's distance is scaled by this factor (< 1 = "counts as closer"), so a
# symbol wins on its flip when the flip is within ~1/weight of a rival's wall
# distance.  0.6 ⇒ the flip is worth ~1.67× a wall.  Tune toward 0 to lean
# harder into flip/regime-transition stories, toward 1 to weight all three
# levels equally (pure nearest-to-any-level).
FLIP_PROXIMITY_WEIGHT = 0.6
LONG_TWEET_MAX_LEN = 25_000  # X Premium long-form ceiling; classic 280 is the
# floor the fallback body targets when the caller
# doesn't have Premium enabled on the bot handle.
# The threaded link reply is a standard post, so it has to fit X's classic cap
# (a link counts as 23 characters however long it is).
REPLY_MAX_LEN = 280
X_LINK_LEN = 23
# The latest ET time each fire may still post.  The timers are Persistent, so
# a run delayed by downtime still fires later; a Morning Read that says
# "heading into the open" must not go out at noon.
POST_DEADLINE_ET = {"premarket": time(9, 30), "midday": time(14, 0), "close": time(18, 0)}
# Only headlines published within this many hours count as the latest news.
DEFAULT_NEWS_MAX_AGE_HOURS = 24.0

# Modes ---------------------------------------------------------------------
MODES = ("premarket", "midday", "close")

# The header label each fire opens with — the operator-approved "…Read — $SPY"
# format.  Also the "timing" label the admin review page switches on
# (09:15 ET → Morning, 12:30 ET → Midday, 16:05 ET → Post-Market).
MODE_READ_LABEL = {
    "premarket": "Morning Read",
    "midday": "Midday Read",
    "close": "Post-Market Read",
}


def _mode_read_label(mode: str) -> str:
    return MODE_READ_LABEL.get(mode, "Market Read")


@dataclass
class ModeCopy:
    """Static per-mode copy — headline label + the section preamble.

    Keeping the copy in code (not in a JSON blob or CMS) means the
    tweet body renders even if the DB is only partially available.
    The auto-lead sentence is deliberately generic; the operator can
    swap it out by editing the class if a specific day warrants a
    hand-written lead (e.g. FOMC or CPI print)."""

    label: str
    lead_variants: list[str]


MODE_COPY: dict[str, ModeCopy] = {
    "premarket": ModeCopy(
        label="pre-market update",
        lead_variants=[
            "Opening the tape with the dealer gamma map locked in.",
            "Fifteen minutes to the open — here is where dealers are positioned.",
            "Pre-cash read: the gamma structure heading into the bell.",
        ],
    ),
    "midday": ModeCopy(
        label="midday update",
        lead_variants=[
            "Halfway through the session — checking in on the dealer gamma map.",
            "Mid-session read on where the walls have held (and where they haven't).",
            "Noon-hour snapshot of the gamma structure carrying the tape.",
        ],
    ),
    "close": ModeCopy(
        label="post-market update",
        lead_variants=[
            "Closing read on where the tape parked into the bell.",
            "The bell rang — here is the dealer gamma map on the close.",
            "End-of-day read on the levels that mattered.",
        ],
    ),
}


# ---------------------------------------------------------------------------
# Date / market-calendar helpers
# ---------------------------------------------------------------------------


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def resolve_current_mode(now: datetime | None = None) -> str:
    """The "timing" the /admin review page shows, by wall-clock ET.

    The page switches format at each fire time and holds it until the next:
      * 09:15 → 12:30  → ``premarket`` (Morning Read)
      * 12:30 → 16:05  → ``midday``    (Midday Read)
      * 16:05 → 09:15  → ``close``     (Post-Market Read; incl. overnight)
    Before the first fire of the day it stays on the prior session's close."""
    now = now or datetime.now(tz=ET)
    hm = (now.hour, now.minute)
    if (9, 15) <= hm < (12, 30):
        return "premarket"
    if (12, 30) <= hm < (16, 5):
        return "midday"
    return "close"


def _is_trading_day(day: date) -> bool:
    """Mon–Fri excluding configured NYSE holidays. Matches the pattern
    already used by :mod:`src.jobs.forecast_tweet` and :mod:`src.jobs.scorecard_tweet`."""
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


def _hash_seed(*nums: float | None) -> int:
    """FNV-ish deterministic hash of a handful of floats → int.

    Mirrors ``bulletinHelpers.hashSeed`` on the web side so the tweet
    body's lead sentence rotates identically to what the operator
    sees when they preview the same card in the UI."""
    h = 2166136261
    for n in nums:
        v = 0 if n is None else int(round(n))
        h = ((h ^ (v & 0xFFFF)) * 16777619) & 0xFFFFFFFF
        h = ((h ^ ((v >> 16) & 0xFFFF)) * 16777619) & 0xFFFFFFFF
    return h


def _pick(seq: list[str], seed: int) -> str:
    return seq[seed % len(seq)] if seq else ""


# ---------------------------------------------------------------------------
# Number formatting — mirrors bulletinHelpers.ts so the tweet reads the
# same as the web bulletin card.
# ---------------------------------------------------------------------------


def _fmt_price(v: float | None) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:.2f}"


def _fmt_price_spot(v: float | None) -> str:
    """SPX / indices are quoted whole; ETFs (SPY / QQQ) are quoted to
    the cent. `bulletinHelpers.fmtPrice` picks by magnitude — same
    rule here so the tweet body and the UI match."""
    return _fmt_price(v)


def _fmt_net_gex(v: float | None) -> str:
    if v is None:
        return "—"
    abs_v = abs(v)
    # A plain hyphen, not the typographic minus the card draws: the post has
    # to read as typed.
    sign = "+" if v >= 0 else "-"
    if abs_v >= 1e9:
        return f"{sign}${abs_v / 1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v / 1e6:.1f}M"
    if abs_v >= 1e3:
        return f"{sign}${abs_v / 1e3:.0f}K"
    return f"{sign}${abs_v:.0f}"


def _fmt_dollars(v: float) -> str:
    """Compact UNSIGNED dollar magnitude: $1.20B / $340M / $12K / $500."""
    a = abs(v)
    if a >= 1e9:
        return f"${a / 1e9:.2f}B"
    if a >= 1e6:
        return f"${a / 1e6:.0f}M"
    if a >= 1e3:
        return f"${a / 1e3:.0f}K"
    return f"${a:.0f}"


# Only surface the Charm-into-Close headline when it clears this floor, so a
# quiet session doesn't render a trivial "$0" forced-flow claim.
_CHARM_HEADLINE_MIN_USD = 1_000_000.0


def _fmt_level(v: float | None) -> str:
    """Format a level for the post, with the same digits the card shows.

    The Live Bulletin card prints prices under 1,000 to the cent ("745.00",
    "747.29") and index-scale prices as whole numbers with separators
    ("7,483").  The post rounds the same way (half up on the exact value, as
    the browser does) and drops a whole number's ".00", which is how a
    person writes a strike: "745", "747.29", "7,483"."""
    if v is None:
        return "—"
    exact = Decimal(v)
    if abs(v) >= 1000:
        return f"{exact.quantize(Decimal(1), rounding=ROUND_HALF_UP):,}"
    cents = exact.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if cents == cents.to_integral_value():
        return f"{int(cents)}"
    return f"{cents}"


def _derive_regime(
    net_gex: float | None, spot: float | None, gamma_flip: float | None
) -> str | None:
    """A plain-language dealer-gamma regime label for the LLM prompt.

    Primary signal is the sign of Net GEX (at spot); when that's missing we
    fall back to spot vs the gamma flip (below the flip ≈ short gamma).
    Returns None when neither is available."""
    if net_gex is not None:
        if net_gex < 0:
            return "negative"
        if net_gex > 0:
            return "positive"
        return "neutral"
    if spot is not None and gamma_flip is not None:
        return "negative" if spot < gamma_flip else "positive"
    return None


def _derive_momentum_label(b: "SymbolBulletin") -> str | None:
    """A cheap, honest momentum cue derived from the session path.

    Combines the day's direction (spot vs prior close) with where spot sits
    in the session range (pressing highs / lows / mid-range).  Deliberately
    qualitative — the LLM gets the raw numbers too and does the nuance; this
    is just a nudge so it doesn't have to infer direction from scratch."""
    if b.spot is None:
        return None
    direction = ""
    if b.prior_close not in (None, 0):
        chg = b.spot - b.prior_close  # type: ignore[operator]
        if chg > 0:
            direction = "up on the day"
        elif chg < 0:
            direction = "down on the day"
        else:
            direction = "flat on the day"
    position = ""
    if b.session_high is not None and b.session_low is not None and b.session_high > b.session_low:
        rng = b.session_high - b.session_low
        pos = (b.spot - b.session_low) / rng
        if pos >= 0.66:
            position = "pressing session highs"
        elif pos <= 0.34:
            position = "pressing session lows"
        else:
            position = "mid-range"
    parts = [p for p in (direction, position) if p]
    return ", ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Bulletin data model — one per symbol
# ---------------------------------------------------------------------------


@dataclass
class SymbolBulletin:
    """The subset of GEX summary fields we render in the tweet.

    Sourced from ``DatabaseManager.get_latest_gex_summary`` so the
    numbers exactly match the Live Bulletin card in the web UI. All
    fields are Optional — the tweet renders each with a ``—`` fallback
    when a value is missing, and the whole symbol block is elided if
    none of the level fields resolved."""

    symbol: str
    spot: float | None = None
    gamma_flip: float | None = None
    call_wall: float | None = None
    put_wall: float | None = None
    max_pain: float | None = None
    net_gex: float | None = None
    # Price action fed to the LLM so the narrative can describe the day's path
    # ("dumped through the 740 put wall, ripped back, stalled short of 745").
    # All best-effort — populated in :func:`_fetch_bulletins` from the quote /
    # session-close queries, left None on any miss (the LLM tolerates nulls).
    prior_close: float | None = None
    session_open: float | None = None
    session_high: float | None = None
    session_low: float | None = None
    momentum_label: str | None = None
    regime: str | None = None  # "negative" / "positive" / "neutral"
    # Charm-into-Close headline (Phase 4): dollars of stock dealers must trade by
    # the 4pm bell from time decay ALONE if spot holds. Positive = forced buying.
    charm_close_flow: float | None = None
    # True when ``spot`` is a futures-implied projection (a cash index
    # outside the cash session) rather than a live cash print; ``future_symbol``
    # is the future it was projected from (e.g. "@ES") for the indicator.
    spot_is_projected: bool = False
    future_symbol: str | None = None
    # How the walls / flip MOVED through the session, and what price did to
    # each one while it was in force (see :mod:`src.jobs.level_history`).
    # Populated on the midday and close fires only — the pre-market read has
    # no session path yet.  None everywhere else.
    level_history: "lh.LevelHistory | None" = None
    # The card's own "as of" label once its numbers have replaced the ones
    # from the database (see :func:`_apply_card_levels`); None until then.
    card_as_of: str | None = None

    def has_any_level(self) -> bool:
        return any(
            v is not None
            for v in (self.gamma_flip, self.call_wall, self.put_wall, self.max_pain, self.net_gex)
        )


def _to_float(v: Any) -> float | None:
    """Coerce ``Decimal``/``int``/``str`` → float, or None if unreadable.

    ``get_latest_gex_summary`` returns ``asyncpg`` rows whose numeric
    columns come back as ``Decimal``; every downstream formatter here
    expects plain float, so we normalize at the boundary."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # asyncpg preserves NaN when the DB stored one; treat it as missing
    # to avoid rendering "$nan" in the tweet body.
    if f != f:  # noqa: PLR0124 — NaN check
        return None
    return f


def _pick_net_gex(row: dict[str, Any]) -> float | None:
    """Net GEX for the tweet — prefer ``net_gex_at_spot``.

    ``get_latest_gex_summary`` exposes two different Net GEX quantities:
    ``net_gex`` (the chain-wide total, re-derived as call+put GEX summed
    across every strike) and ``net_gex_at_spot`` (the dealer gamma read
    off the spot-shift profile at the current price).  The analytics
    engine documents ``net_gex_at_spot`` as *the regime-correct headline
    figure* and warns the two "use different bases and can legitimately
    differ in magnitude (and occasionally sign)".

    The main dashboard and the gamma-exposure page already headline
    ``net_gex_at_spot``, so the tweet prefers it too — otherwise the
    post's Net GEX could show a different number (or the opposite sign)
    than the rest of the site for the same snapshot, and could even
    contradict the tweet's own above/below-the-flip framing.  Falls back
    to the chain-wide ``net_gex`` only when the at-spot value is absent."""
    at_spot = _to_float(row.get("net_gex_at_spot"))
    if at_spot is not None:
        return at_spot
    return _to_float(row.get("net_gex"))


def _shape_bulletin(row: dict[str, Any] | None, symbol: str) -> SymbolBulletin:
    if not row:
        return SymbolBulletin(symbol=symbol)
    return SymbolBulletin(
        symbol=symbol,
        spot=_to_float(row.get("spot_price")),
        gamma_flip=_to_float(row.get("gamma_flip")),
        call_wall=_to_float(row.get("call_wall")),
        put_wall=_to_float(row.get("put_wall")),
        max_pain=_to_float(row.get("max_pain")),
        net_gex=_pick_net_gex(row),
    )


# ---------------------------------------------------------------------------
# Tweet body builder — the multi-paragraph format the operator specified.
# ---------------------------------------------------------------------------


@dataclass
class TweetBody:
    """The full text of a bulletin tweet plus a shortened fallback.

    ``text`` is the long-form (Premium) version — the featured symbol's
    intro, its numeric map, and the interpretation.  ``fallback`` is a
    280-char single-tweet compression the caller can post instead when
    the bot handle isn't Premium-enabled (or when the API rejects the
    long text with a 403).  Neither carries a link or hashtags.

    ``reply_text`` is the threaded link comment posted *after* the main
    tweet (``Free delayed SPY / SPX / QQQ gamma levels: …``) — kept off
    the main post so an in-body link doesn't throttle its reach.
    ``featured_symbol`` is the single symbol the post centers on."""

    text: str
    fallback: str
    lead_symbol: str
    symbols_present: list[str] = field(default_factory=list)
    reply_text: str = ""
    featured_symbol: str = ""
    # The writer's raw fragments, so a review can hand them back for a rewrite.
    llm_post: Any = None
    # Why there's no post, when the writer couldn't produce one (``text`` is
    # then empty).
    problems: list[str] = field(default_factory=list)


def _future_label(future_symbol: str | None) -> str:
    """UI ticker for a continuous future — "@ES" -> "ES"."""
    return (future_symbol or "").lstrip("@").upper() or "futures"


def _symbol_block(b: SymbolBulletin, include_prefix: bool = True) -> str | None:
    """One symbol's level block. Returns None when nothing resolved so
    the caller can silently drop it from the tweet.

    ``include_prefix`` controls the spot line: ``True`` renders
    ``SPY spot: ~744.51`` (used when a symbol is named inline), ``False``
    renders just ``Spot: ~744.51`` (used for the featured symbol's
    ``Current map:`` block, where the header already names the symbol)."""
    if not b.has_any_level() and b.spot is None:
        return None
    if include_prefix:
        spot_line = f"{b.symbol} spot: ~{_fmt_price_spot(b.spot)}"
    else:
        spot_line = f"Spot: ~{_fmt_price_spot(b.spot)}"
    if b.spot_is_projected:
        # Make it unmistakable this is a projection, not a live cash print.
        spot_line += f" (implied from {_future_label(b.future_symbol)} futures, cash closed)"
    lines = [spot_line]
    if b.gamma_flip is not None:
        lines.append(f"Gamma Flip: {_fmt_price(b.gamma_flip)}")
    if b.call_wall is not None:
        lines.append(f"Call Wall: {_fmt_price(b.call_wall)}")
    if b.put_wall is not None:
        lines.append(f"Put Wall: {_fmt_price(b.put_wall)}")
    if b.max_pain is not None:
        lines.append(f"Max Pain: {_fmt_price(b.max_pain)}")
    if b.net_gex is not None:
        lines.append(f"Net GEX: {_fmt_net_gex(b.net_gex)}")
    # Charm-into-Close (Phase 4): a forecast with a deadline. Time decay alone,
    # no move required, forces this much dealer stock trading by the bell.
    if b.charm_close_flow is not None and abs(b.charm_close_flow) >= _CHARM_HEADLINE_MIN_USD:
        side = "buy" if b.charm_close_flow > 0 else "sell"
        lines.append(
            f"Charm into close: time decay alone forces dealers to {side} "
            f"{_fmt_dollars(b.charm_close_flow)} by 4pm ET if {b.symbol} holds here"
        )
    return "\n".join(lines)


def select_featured_symbol(
    bulletins: list[SymbolBulletin],
    fallback: str = DEFAULT_LEAD_SYMBOL,
) -> SymbolBulletin | None:
    """Pick the single symbol with the "cleanest setup" to feature.

    Cleanest = spot pressed closest (in % terms) to a decision level —
    the gamma flip, the put wall or the call wall.  That's the clearest
    line to narrate: "price drove into the 740 put wall and ripped off
    it" only reads that cleanly when spot is actually sitting on 740.

    Proximity to the **gamma flip** is weighted more heavily than
    proximity to a wall (see :data:`FLIP_PROXIMITY_WEIGHT`): the flip is
    the regime boundary — where dealer hedging flips sign and the tape
    changes character — so a symbol straddling its flip is a stronger
    story than one merely pinned to a wall, and wins when the two are
    close.

    Only symbols with a live/implied spot AND at least one of those
    three levels are eligible.  Ties break toward the more complete
    data, then toward the input order, so a dry-run and the live post
    always feature the same symbol for a given fire.

    When nothing is eligible (every symbol missing spot or all three
    levels) we fall back to the configured ``fallback`` symbol if it has
    any renderable data, then to the first symbol that does — never
    None unless the whole trio is empty (the caller skips before then).
    """
    scored: list[tuple[float, int, int, SymbolBulletin]] = []
    for idx, b in enumerate(bulletins):
        if b.spot is None or b.spot <= 0:
            continue
        # Distance to each decision level as a fraction of spot; the gamma
        # flip's distance is scaled down so being near it counts for more.
        weighted: list[float] = []
        if b.gamma_flip is not None:
            weighted.append(abs(b.spot - b.gamma_flip) / b.spot * FLIP_PROXIMITY_WEIGHT)
        if b.put_wall is not None:
            weighted.append(abs(b.spot - b.put_wall) / b.spot)
        if b.call_wall is not None:
            weighted.append(abs(b.spot - b.call_wall) / b.spot)
        if not weighted:
            continue
        proximity = min(weighted)
        completeness = sum(
            1
            for v in (b.gamma_flip, b.call_wall, b.put_wall, b.max_pain, b.net_gex)
            if v is not None
        )
        # (proximity asc, completeness desc, input-order asc) — the index
        # keeps the sort total-ordered so it never compares SymbolBulletin.
        scored.append((proximity, -completeness, idx, b))

    if scored:
        scored.sort(key=lambda t: (t[0], t[1], t[2]))
        return scored[0][3]

    by_symbol = {b.symbol: b for b in bulletins}
    lead = by_symbol.get(fallback.upper())
    if lead is not None and (lead.has_any_level() or lead.spot is not None):
        return lead
    for b in bulletins:
        if b.has_any_level() or b.spot is not None:
            return b
    return None


_URL_RE = re.compile(r"https?://\S+")


def _compose_reply(llm_reply: str | None, site_url: str, override: str | None = None) -> str:
    """The threaded reply posted under the main tweet.

    Precedence:
      1. ``override`` (``$BULLETIN_TWEET_REPLY_TEXT``) — wins verbatim.
      2. The LLM-authored reply (plays off the post) + the ZeroGEX link.
         The model is told NOT to include a URL, but we strip any it added
         and append the canonical ``site_url`` — on the same line when the
         reply ends on a call-to-action colon, else as its own paragraph.
      3. The fixed prefix + link (no-LLM fallback)."""
    url = site_url.rstrip("/")
    if override and override.strip():
        return override.strip()
    if llm_reply and llm_reply.strip():
        body = _URL_RE.sub("", llm_reply).strip()
        # Tidy a trailing stub the URL removal may have left (" ." / ":" etc.
        # stay; a dangling "()" or double space gets squeezed).
        body = re.sub(r"[ \t]{2,}", " ", body).strip()
        if body.endswith(":"):
            return f"{body} {url}"
        return f"{body}\n\n{url}"
    return f"{DEFAULT_REPLY_PREFIX} {url}"


def build_tweet_body(
    mode: str,
    day: date,
    bulletins: list[SymbolBulletin],
    site_url: str = DEFAULT_SITE_URL,
    lead_symbol: str = DEFAULT_LEAD_SYMBOL,
    reply_text: str | None = None,
    headlines: list | None = None,
    force_featured: str | None = None,
    revise_from: Any = None,
    feedback: list[str] | None = None,
) -> TweetBody:
    """Assemble the "…Read - $SYM" post + threaded reply for one fire.

    Layout (the operator-approved voice):

        <Morning|Midday|Post-Market> Read - $<FEATURED>

        <two to four short paragraphs: news, price action, regime>

        Key levels:            (the close read: "Levels for tomorrow:")
        • <put wall> put wall
        • <call wall> call wall
        • <flip> gamma flip

        Bottom line: <takeaway>

    The prose comes from Claude (:func:`_try_llm_post`), fed the day's
    headlines + the featured symbol's price action + its gamma structure.
    Python owns the header, every price in the levels list, and the reply's
    link.  ``revise_from`` + ``feedback`` hand an earlier draft back with a
    review's findings.  When the writer can't produce a post, ``text`` is
    empty and ``problems`` says why: there is no template fallback, because a
    post without the news is not one the operator wants published."""
    if mode not in MODE_READ_LABEL:
        raise ValueError(f"Unknown mode: {mode!r}")
    read_label = _mode_read_label(mode)

    present = [b for b in bulletins if b.has_any_level() or b.spot is not None]
    symbols_present = [b.symbol for b in present]

    # ``force_featured`` (the scheduled job passes the lead symbol) pins the
    # featured symbol so the auto-post is always that symbol — e.g. SPY —
    # rather than the "cleanest setup" among a multi-symbol dropdown list.
    # Falls through to the cleanest-setup selector when the forced symbol has
    # no data this fire.
    featured = None
    if force_featured:
        ff = force_featured.upper()
        featured = next((b for b in present if b.symbol.upper() == ff), None)
    if featured is None:
        featured = select_featured_symbol(bulletins, fallback=lead_symbol)
    if featured is None:
        featured_symbol = lead_symbol.upper()
        return TweetBody(
            text="",
            fallback="",
            lead_symbol=featured_symbol,
            symbols_present=symbols_present,
            featured_symbol=featured_symbol,
            problems=["No symbol had any data to write about."],
        )

    featured_symbol = featured.symbol
    # Only the featured symbol is handed to the model — the posts are
    # single-symbol, so this keeps the prose clean and the no-invented-price
    # guard scoped to that symbol.
    errors: list[str] = []
    post = _try_llm_post(
        mode,
        day,
        [featured],
        featured_symbol,
        headlines,
        revise_from=revise_from,
        feedback=feedback,
        errors=errors,
    )
    if post is None:
        reason = "; ".join(errors) or "no reason was given"
        return TweetBody(
            text="",
            fallback=_build_fallback_tweet(featured, read_label),
            lead_symbol=featured_symbol,
            symbols_present=symbols_present,
            featured_symbol=featured_symbol,
            problems=[f"The AI writer didn't produce a post ({reason})."],
        )
    return TweetBody(
        text=_compose_new_post(post, featured, mode, day),
        fallback=_build_fallback_tweet(featured, read_label),
        lead_symbol=featured_symbol,
        symbols_present=symbols_present,
        reply_text=_compose_reply(post.reply, site_url, reply_text),
        featured_symbol=featured_symbol,
        llm_post=post,
    )


def _hget(h: Any, name: str, default: Any = None) -> Any:
    """Read a headline field from either a dict or a NewsItem-like object."""
    if isinstance(h, dict):
        return h.get(name, default)
    return getattr(h, name, default)


def _news_max_age_hours() -> float:
    raw = os.environ.get("BULLETIN_TWEET_NEWS_MAX_AGE_HOURS", "").strip()
    try:
        value = float(raw) if raw else DEFAULT_NEWS_MAX_AGE_HOURS
    except ValueError:
        return DEFAULT_NEWS_MAX_AGE_HOURS
    return value if value > 0 else DEFAULT_NEWS_MAX_AGE_HOURS


def _fetch_fresh_headlines() -> tuple[list, str | None]:
    """The latest CNBC headlines for the writer, and a problem when there are none.

    Only items published within ``$BULLETIN_TWEET_NEWS_MAX_AGE_HOURS``
    (default 24) count: the post has to be about today's news.  Never
    raises."""
    max_age = _news_max_age_hours()
    try:
        from src.jobs import cnbc_news  # noqa: WPS433 — optional

        items = cnbc_news.fetch_headlines(max_age_hours=max_age)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: headline fetch failed (%s)", exc)
        return [], f"The CNBC headlines couldn't be fetched ({exc})."
    if not items:
        return [], (
            f"No CNBC headlines from the last {max_age:g} hours came back "
            "(the feeds were down, stale, or turned off with "
            "BULLETIN_TWEET_NEWS_ENABLED=0)."
        )
    return items, None


def _llm_symbol_inputs(present: list[SymbolBulletin]) -> list:
    """The writer's (and the reviewer's) view of each symbol."""
    from src.jobs import bulletin_llm  # noqa: WPS433 — optional

    return [
        bulletin_llm.SymbolInput(
            symbol=b.symbol,
            spot=b.spot,
            prior_close=b.prior_close,
            session_open=b.session_open,
            session_high=b.session_high,
            session_low=b.session_low,
            gamma_flip=b.gamma_flip,
            call_wall=b.call_wall,
            put_wall=b.put_wall,
            max_pain=b.max_pain,
            net_gex=b.net_gex,
            regime=b.regime,
            momentum_label=b.momentum_label,
            spot_is_projected=b.spot_is_projected,
            future_symbol=b.future_symbol,
            # How the levels moved through the session and what price did to
            # each print — so the prose narrates the real path instead of
            # inferring one from the closing snapshot.  The post-bell reset is
            # shown by direction only: the post prints its value on its own
            # line, and a number the model is handed is a number it will put
            # next to "call wall".
            level_history=(
                b.level_history.to_prompt_dict(include_post_close_values=False)
                if b.level_history
                else None
            ),
            historical_level_values=(b.level_history.quoted_values() if b.level_history else []),
            level_paths=(b.level_history.session_values() if b.level_history else {}),
        )
        for b in present
    ]


def _llm_headlines(headlines: list | None) -> list:
    """The scraped CNBC items (NewsItem objects or dicts) as prompt Headlines."""
    from src.jobs import bulletin_llm  # noqa: WPS433 — optional

    heads = []
    for h in headlines or []:
        title = _hget(h, "title", "")
        if not title:
            continue
        heads.append(
            bulletin_llm.Headline(
                title=title,
                summary=_hget(h, "summary", "") or "",
                source=_hget(h, "source", "CNBC") or "CNBC",
                published=_hget(h, "published"),
            )
        )
    return heads


def _try_llm_post(
    mode: str,
    day: date,
    present: list[SymbolBulletin],
    featured_symbol: str,
    headlines: list | None = None,
    revise_from: Any = None,
    feedback: list[str] | None = None,
    errors: list[str] | None = None,
):
    """Attempt LLM post+reply generation. Returns an ``LlmPost`` or None.

    ``featured_symbol`` is the symbol the post centers on; ``headlines``
    are the scraped CNBC items.  On failure the reason is appended to
    ``errors``.  Never raises."""
    try:
        from src.jobs import bulletin_llm  # noqa: WPS433 — optional

        return bulletin_llm.generate_post(
            mode=mode,
            day=day,
            symbols=_llm_symbol_inputs(present),
            headlines=_llm_headlines(headlines),
            featured_symbol=featured_symbol,
            revise_from=revise_from,
            feedback=feedback,
            errors=errors,
        )
    except Exception as exc:  # noqa: BLE001 — never let the LLM path throw
        logger.warning("bulletin_tweet: LLM post generation failed (%s)", exc)
        if errors is not None:
            errors.append(f"the writer crashed ({exc})")
        return None


def _append_para(blocks: list[str], value: str) -> None:
    """Append ``value`` as its own blank-line-separated paragraph (skip empty)."""
    stripped = (value or "").strip()
    if not stripped:
        return
    if blocks and blocks[-1] != "":
        blocks.append("")
    blocks.append(stripped)


# The key-levels list, in the order the operator reads it.
KEY_LEVELS = (("put_wall", "put wall"), ("call_wall", "call wall"), ("gamma_flip", "gamma flip"))


def _next_trading_day(day: date) -> date | None:
    cursor = day + timedelta(days=1)
    for _ in range(10):
        if _is_trading_day(cursor):
            return cursor
        cursor += timedelta(days=1)
    return None


def _levels_heading(mode: str, day: date) -> str:
    """The key-levels list's heading.

    The close read quotes what the live card shows at 16:05, which is the
    next session's map (the day's 0DTE rolled off at the bell), so it says so
    rather than passing tomorrow's levels off as today's."""
    if mode != "close":
        return "Key levels:"
    nxt = _next_trading_day(day)
    if nxt is None or nxt == day + timedelta(days=1):
        return "Levels for tomorrow:"
    return f"Levels for {nxt.strftime('%A')}:"


def _key_level_line(value: float, label: str) -> str:
    return f"• {_fmt_level(value)} {label}"


def _key_levels_block(featured: SymbolBulletin) -> str:
    """The ``• <price> <level>`` lines, written the way a person types them.

    Python owns every price here, and they are the live card's numbers (see
    :func:`_apply_card_levels`).  No expiration scope and no annotations: the
    prose says what happened at a level, the list just says where it is."""
    lines = [
        _key_level_line(value, label)
        for key, label in KEY_LEVELS
        if (value := getattr(featured, key)) is not None
    ]
    return "\n".join(lines)


def _append_key_levels(blocks: list[str], featured: SymbolBulletin, mode: str, day: date) -> None:
    levels = _key_levels_block(featured)
    if levels:
        _append_para(blocks, f"{_levels_heading(mode, day)}\n{levels}")


def _post_header(mode: str, symbol: str) -> str:
    """The first line, with a plain hyphen: "Midday Read - $SPY"."""
    return f"{_mode_read_label(mode)} - ${symbol}"


_BOTTOM_LINE_LABEL_RE = re.compile(r"^\s*bottom\s+line\s*[:\-]\s*", re.IGNORECASE)


def _compose_new_post(post, featured: SymbolBulletin, mode: str, day: date) -> str:
    """Assemble the post body around an LLM-generated ``LlmPost``.

        <Read label> - $<SYM>

        {opening}

        Key levels:
        {• … list}

        Bottom line: {bottom_line}

    Empty sections are elided.  No link / hashtags — the link rides in the
    threaded reply."""
    blocks: list[str] = [_post_header(mode, featured.symbol)]
    _append_para(blocks, post.opening)
    _append_key_levels(blocks, featured, mode, day)
    bottom = _BOTTOM_LINE_LABEL_RE.sub("", post.bottom_line or "").strip()
    if bottom:
        _append_para(blocks, f"Bottom line: {bottom}")
    return "\n".join(blocks).strip()


def _build_fallback_tweet(featured: SymbolBulletin, label: str) -> str:
    """A ≤280-char version of the featured symbol's levels, for ``--short``.

    Only the card's numbers, written out plainly, with no link (the link goes
    out as the threaded reply).  Trimmed if it somehow overflows (rare; one
    symbol fits easily)."""
    parts: list[str] = []
    if featured.spot is not None:
        spot_txt = f"spot {_fmt_level(featured.spot)}"
        if featured.spot_is_projected:
            spot_txt += f" (implied from {_future_label(featured.future_symbol)} futures)"
        parts.append(spot_txt)
    for key, name in (
        ("gamma_flip", "gamma flip"),
        ("call_wall", "call wall"),
        ("put_wall", "put wall"),
    ):
        value = getattr(featured, key)
        if value is not None:
            parts.append(f"{name} {_fmt_level(value)}")
    if featured.net_gex is not None:
        parts.append(f"net GEX {_fmt_net_gex(featured.net_gex)}")
    head = f"{label} - ${featured.symbol}"
    text = f"{head}: {', '.join(parts)}" if parts else head
    if len(text) <= 280:
        return text
    return text[:279].rstrip(" ,.") + "…"


# ---------------------------------------------------------------------------
# Review gate — nothing goes out until the finished post passes
# ---------------------------------------------------------------------------

# Characters that give a post away as generated: nobody types these by hand.
_BANNED_CHARACTERS = {
    "—": "an em dash",
    "–": "an en dash",
    "→": "an arrow",
    "…": "an ellipsis character",
    "−": "a typographic minus sign",
}
# Common British spellings and their American forms.  Lowercase matches only,
# so a proper noun ("Ministry of Defence") doesn't trip it; the fact-check
# reads for spelling too.
_BRITISH_SPELLINGS = {
    "colour": "color",
    "colours": "colors",
    "favour": "favor",
    "favoured": "favored",
    "favourable": "favorable",
    "behaviour": "behavior",
    "neighbour": "neighbor",
    "rumour": "rumor",
    "rumours": "rumors",
    "centre": "center",
    "centred": "centered",
    "defence": "defense",
    "offence": "offense",
    "licence": "license",
    "analyse": "analyze",
    "analysed": "analyzed",
    "analysing": "analyzing",
    "realise": "realize",
    "realised": "realized",
    "recognise": "recognize",
    "recognised": "recognized",
    "emphasise": "emphasize",
    "stabilise": "stabilize",
    "stabilised": "stabilized",
    "normalise": "normalize",
    "summarise": "summarize",
    "prioritise": "prioritize",
    "whilst": "while",
    "amongst": "among",
    "programme": "program",
    "grey": "gray",
    "sceptical": "skeptical",
    "manoeuvre": "maneuver",
}
_BRITISH_RE = re.compile(
    r"\b(" + "|".join(sorted(_BRITISH_SPELLINGS, key=len, reverse=True)) + r")\b"
)
_EMOJI_RE = re.compile("[\U0001f000-\U0001faff☀-➿️]")
_HASHTAG_RE = re.compile(r"(?<![\w&/])#[A-Za-z]\w*")
_MARKDOWN_RE = re.compile(r"\*\*|__|^\s{0,3}#{1,6}\s", re.MULTILINE)
_SITE_MENTION_RE = re.compile(r"https?://\S+|\bzerogex\.io\b", re.IGNORECASE)


def _x_length(text: str) -> int:
    """A post's length as X counts it: every link is 23 characters."""
    return len(_URL_RE.sub("x" * X_LINK_LEN, text))


def _text_problems(
    tweet: TweetBody,
    featured: SymbolBulletin | None,
    mode: str,
) -> list[str]:
    """Checks on the finished post and reply that the writer can fix."""
    text = tweet.text or ""
    if not text.strip() or featured is None:
        return []
    problems: list[str] = []
    first_line = text.splitlines()[0]
    want = _post_header(mode, featured.symbol)
    if first_line != want:
        problems.append(f'The first line is "{first_line}" but should be "{want}".')
    for key, label in KEY_LEVELS:
        value = getattr(featured, key)
        if value is not None and _key_level_line(value, label) not in text:
            problems.append(f"The key levels list is missing the {label} at {_fmt_level(value)}.")
    reply = tweet.reply_text or ""
    for where, body in (("post", text), ("reply", reply)):
        for char, name in _BANNED_CHARACTERS.items():
            if char in body:
                problems.append(
                    f"The {where} uses {name} ({char}); use a comma, a period or a plain hyphen."
                )
        if _EMOJI_RE.search(body):
            problems.append(f"The {where} contains an emoji.")
        if _HASHTAG_RE.search(body):
            problems.append(f"The {where} contains a hashtag.")
        if _MARKDOWN_RE.search(body):
            problems.append(f"The {where} contains markdown formatting.")
        for word in sorted(set(_BRITISH_RE.findall(body))):
            problems.append(
                f'The {where} uses the British spelling "{word}" '
                f'(American: "{_BRITISH_SPELLINGS[word]}").'
            )
    if _SITE_MENTION_RE.search(text):
        problems.append("The post contains a link; the link belongs in the threaded reply only.")
    if len(text) > LONG_TWEET_MAX_LEN:
        problems.append(
            f"The post is {len(text):,} characters, over X's {LONG_TWEET_MAX_LEN:,} limit."
        )
    if not reply.strip():
        problems.append("There's no threaded reply.")
    else:
        if not _URL_RE.search(reply):
            problems.append("The threaded reply has no link.")
        if _x_length(reply) > REPLY_MAX_LEN:
            problems.append(
                f"The reply is {_x_length(reply)} characters as X counts them, "
                f"over the {REPLY_MAX_LEN} limit."
            )
    return problems


def _data_problems(
    featured: SymbolBulletin | None,
    card: "CardRender | None",
    news_problem: str | None,
    image_required: bool = True,
) -> list[str]:
    """What's missing from the inputs.  The writer can't fix these."""
    if featured is None:
        return ["No symbol had any data to post about."]
    problems: list[str] = []
    if image_required:
        if card is None:
            problems.append("The live bulletin image wasn't rendered.")
        elif card.error:
            problems.append(card.error)
    for key, label in KEY_LEVELS:
        if getattr(featured, key) is None:
            problems.append(
                f"The live bulletin shows no {label} for {featured.symbol}, "
                "so the key levels would be incomplete."
            )
    if featured.spot is None:
        problems.append(f"There's no {featured.symbol} price to write from.")
    if news_problem:
        problems.append(news_problem)
    return problems


@dataclass
class ReviewResult:
    """What the review found.  ``fixable`` goes back to the writer once;
    ``blocking`` (image problems, a review that couldn't run) can't be fixed
    by rewriting."""

    fixable: list[str] = field(default_factory=list)
    blocking: list[str] = field(default_factory=list)

    @property
    def problems(self) -> list[str]:
        return self.fixable + self.blocking


def _review(
    mode: str,
    day: date,
    tweet: TweetBody,
    featured: SymbolBulletin,
    headlines: list | None,
    card_png: bytes | None,
) -> ReviewResult:
    """The deterministic checks plus the independent fact-check."""
    result = ReviewResult(fixable=_text_problems(tweet, featured, mode))
    try:
        from src.jobs import bulletin_llm  # noqa: WPS433 — optional

        review = bulletin_llm.review_post(
            mode=mode,
            day=day,
            post_text=tweet.text,
            reply_text=tweet.reply_text,
            symbol=_llm_symbol_inputs([featured])[0],
            headlines=_llm_headlines(headlines),
            card_png=card_png,
        )
    except Exception as exc:  # noqa: BLE001 — a crash here holds the post
        logger.warning("bulletin_tweet: review crashed (%s)", exc)
        result.blocking.append(f"The fact-check crashed ({exc}).")
        return result
    if not review.ran:
        result.blocking.append(f"The fact-check couldn't run ({review.error}).")
    result.fixable += review.problems
    result.blocking += [f"Image: {p}" for p in review.image_problems]
    return result


def _write_and_review(
    mode: str,
    day: date,
    bulletins: list[SymbolBulletin],
    featured: SymbolBulletin,
    headlines: list | None,
    card_png: bytes | None,
    site_url: str,
) -> tuple[TweetBody, list[str]]:
    """Write the post, review it, and give the writer one chance to fix
    what the review found.  Returns the final post and what's still wrong."""
    reply_override = os.environ.get("BULLETIN_TWEET_REPLY_TEXT", "").strip() or None

    def _write(revise_from: Any = None, feedback: list[str] | None = None) -> TweetBody:
        return build_tweet_body(
            mode=mode,
            day=day,
            bulletins=bulletins,
            site_url=site_url,
            lead_symbol=featured.symbol,
            reply_text=reply_override,
            headlines=headlines,
            force_featured=featured.symbol,
            revise_from=revise_from,
            feedback=feedback,
        )

    tweet = _write()
    if not tweet.text:
        return tweet, list(tweet.problems)
    review = _review(mode, day, tweet, featured, headlines, card_png)
    if review.fixable:
        logger.warning(
            "bulletin_tweet[%s]: review found problems, asking for a rewrite: %s",
            mode,
            "; ".join(review.fixable),
        )
        revised = _write(revise_from=tweet.llm_post, feedback=review.fixable)
        if not revised.text:
            return tweet, review.problems + revised.problems
        tweet = revised
        review = _review(mode, day, tweet, featured, headlines, card_png)
    return tweet, review.problems


def _deadline_problem(mode: str, day: date, now: datetime | None = None) -> str | None:
    """Why it's too late to post this fire, or None."""
    now = now or datetime.now(tz=ET)
    if day != now.date():
        return f"The post is for {day.isoformat()}, not today, so it wasn't sent."
    deadline = POST_DEADLINE_ET.get(mode)
    if deadline is not None and now.time() >= deadline:
        return (
            f"It was {now.strftime('%-I:%M %p')} ET, past the {deadline.strftime('%-I:%M %p')} "
            f"cutoff for the {_mode_read_label(mode)}, so it wasn't sent."
        )
    return None


# ---------------------------------------------------------------------------
# Artifact directory — persist dry-run bodies + media for inspection
# ---------------------------------------------------------------------------


# Production default artifact root (preference #3). A module constant so tests
# can point it at an unwritable/temp location and deterministically exercise
# the XDG/HOME fallback, rather than depending on whatever state the host's
# real /var/lib path happens to be in.
PRIMARY_ARTIFACT_ROOT = Path("/var/lib/zerogex-oa/bulletin-tweets")


def resolve_artifact_dir(explicit: str | None, mode: str, day: date) -> Path:
    """Pick the directory dry-run + media renderings are dropped into.

    Preference order:
      1. explicit ``--artifact-dir`` CLI value
      2. ``$BULLETIN_TWEET_ARTIFACT_DIR`` env
      3. :data:`PRIMARY_ARTIFACT_ROOT` (``/var/lib/zerogex-oa/bulletin-tweets``,
         the production default)
      4. ``$XDG_STATE_HOME/zerogex-oa/bulletin-tweets``
      5. ``$HOME/.local/state/zerogex-oa/bulletin-tweets`` (dev)

    First writable path wins. The chosen root gets ``/<mode>/<date>/``
    appended so a day's three fires each land in their own folder and
    successive dry-runs don't smear over each other."""
    for root in _artifact_root_candidates(explicit):
        target = root / mode / day.isoformat()
        try:
            target.mkdir(parents=True, exist_ok=True)
            probe = target / ".writable"
            probe.touch()
            probe.unlink()
            return target
        except OSError:
            continue

    # Every path failed. Fall back to a fresh tempdir so we still
    # produce inspectable output; the caller logs where it landed.
    import tempfile

    fallback = Path(tempfile.mkdtemp(prefix="zerogex-bulletin-"))
    return fallback


def _artifact_root_candidates(explicit: str | None = None) -> list[Path]:
    """The ordered writable-root ladder shared by the per-fire artifact dir
    and the ``latest/`` review-page store."""
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    env = os.environ.get("BULLETIN_TWEET_ARTIFACT_DIR", "").strip()
    if env:
        candidates.append(Path(env))
    candidates.append(PRIMARY_ARTIFACT_ROOT)
    xdg = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg:
        candidates.append(Path(xdg) / "zerogex-oa" / "bulletin-tweets")
    home = os.environ.get("HOME", "").strip()
    if home:
        candidates.append(Path(home) / ".local" / "state" / "zerogex-oa" / "bulletin-tweets")
    return candidates


def resolve_latest_dir() -> Path | None:
    """The stable ``<root>/latest`` directory the admin review page reads from.

    Unlike :func:`resolve_artifact_dir` this is NOT per-date — each fire
    overwrites ``<SYMBOL>-<mode>.json`` so the page always shows the last
    auto-generated post for a given (timing, symbol).  Returns the first
    writable candidate, or None if none is writable (caller degrades)."""
    for root in _artifact_root_candidates():
        target = root / "latest"
        try:
            target.mkdir(parents=True, exist_ok=True)
            probe = target / ".writable"
            probe.touch()
            probe.unlink()
            return target
        except OSError:
            continue
    return None


def _latest_record_path(latest_dir: Path, symbol: str, mode: str) -> Path:
    return latest_dir / f"{symbol.upper()}-{mode}.json"


def build_latest_record(
    mode: str,
    day: date,
    tweet: TweetBody,
    featured: SymbolBulletin | None,
    headlines: list | None = None,
    media: "MediaArtifacts | None" = None,
    generated_at: str | None = None,
    status: str | None = None,
    problems: list[str] | None = None,
    tweet_url: str | None = None,
) -> dict[str, Any]:
    """The JSON payload the review page renders (post text + reply + meta).

    ``status`` says what happened to the post: ``posted``, ``ready`` (passed
    review, waiting for the operator), ``blocked`` (held back; ``problems``
    says why), ``post_failed``, ``dry_run`` or ``regenerated``."""
    heads = []
    for h in (headlines or [])[:8]:
        heads.append(
            {
                "title": _hget(h, "title", ""),
                "summary": _hget(h, "summary", ""),
                "source": _hget(h, "source", "CNBC"),
                "link": _hget(h, "link"),
                "published": _hget(h, "published"),
            }
        )
    levels = None
    if featured is not None:
        levels = {
            "spot": featured.spot,
            "prior_close": featured.prior_close,
            "gamma_flip": featured.gamma_flip,
            "call_wall": featured.call_wall,
            "put_wall": featured.put_wall,
            "max_pain": featured.max_pain,
            "net_gex": featured.net_gex,
            "regime": featured.regime,
            # The live card's "as of" label when the levels came from it.
            "card_as_of": featured.card_as_of,
            # The session's level path (walls that migrated, what price did
            # to each print, the post-bell roll-off).  Null on pre-market
            # fires and on days with too thin a path to read.  Lets the
            # review page show WHY the post says what it says about a level.
            "level_history": (
                featured.level_history.to_prompt_dict() if featured.level_history else None
            ),
        }
    return {
        "mode": mode,
        "timing_label": _mode_read_label(mode),
        "symbol": tweet.featured_symbol or tweet.lead_symbol,
        "date": day.isoformat(),
        "generated_at": generated_at,
        "post_text": tweet.text,
        "reply_text": tweet.reply_text,
        "fallback_text": tweet.fallback,
        "headlines": heads,
        "levels": levels,
        "media": {
            "png": str(media.png_path) if media and media.png_path else None,
            "clip": str(media.clip_path) if media and media.clip_path else None,
        },
        "status": status,
        "problems": list(problems or []),
        "tweet_url": tweet_url,
    }


def write_latest_record(record: dict[str, Any]) -> Path | None:
    """Persist one ``latest`` record for the review page. Best-effort."""
    latest_dir = resolve_latest_dir()
    if latest_dir is None:
        logger.warning("bulletin_tweet: no writable latest dir — skipping review record")
        return None
    symbol = str(record.get("symbol") or DEFAULT_LEAD_SYMBOL)
    mode = str(record.get("mode") or "midday")
    path = _latest_record_path(latest_dir, symbol, mode)
    try:
        path.write_text(json.dumps(record, indent=2, default=str) + "\n", encoding="utf-8")
        return path
    except OSError as exc:
        logger.warning("bulletin_tweet: failed to write latest record (%s)", exc)
        return None


def _latest_read_dirs() -> list[Path]:
    """Candidate ``latest/`` dirs to READ records from — no writability needed.

    The scheduled tweet job and the API run under different systemd units:
    the job can write ``/var/lib/zerogex-oa`` while the API is sandboxed
    (ProtectSystem=strict, ProtectHome=read-only) and can only WRITE
    ``/home/ubuntu/zerogex-oa``.  ``resolve_latest_dir`` write-probes, so it
    returns None for the API even though ``/var/lib`` is perfectly READABLE.
    Reads therefore search the candidate roots directly by existence, so the
    API finds whatever the job wrote regardless of its own write access."""
    return [root / "latest" for root in _artifact_root_candidates()]


def read_latest_record(symbol: str, mode: str) -> dict[str, Any] | None:
    """Read the last-generated record for (symbol, mode), or None if absent.

    Searches every candidate ``latest/`` dir (read-only is fine) and returns
    the first existing record in candidate order."""
    fname = f"{symbol.upper()}-{mode}.json"
    for latest_dir in _latest_read_dirs():
        path = latest_dir / fname
        try:
            if not path.exists():
                continue
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("bulletin_tweet: failed to read latest record %s (%s)", path, exc)
            # Keep looking in the remaining candidate roots.
    return None


def latest_image_path(record: dict[str, Any] | None) -> Path | None:
    """The PNG a latest record attached, if it's still on disk.

    Only a .png inside one of the artifact roots is served, so a hand-edited
    record can't point the review page at any other file on the box."""
    png = ((record or {}).get("media") or {}).get("png")
    if not png:
        return None
    try:
        path = Path(str(png)).resolve()
    except (OSError, RuntimeError):
        return None
    if path.suffix.lower() != ".png" or not path.is_file():
        return None
    for root in _artifact_root_candidates():
        try:
            path.relative_to(root.resolve())
            return path
        except (ValueError, OSError, RuntimeError):
            continue
    return None


def read_latest_record_any(mode: str) -> dict[str, Any] | None:
    """The most-recently-generated record for ``mode`` across all symbols.

    Lets the review page pre-fill from the last scheduled run even when that
    run featured a symbol other than the page's default (e.g. a multi-symbol
    ``BULLETIN_TWEET_SYMBOLS`` where the 'cleanest setup' wasn't SPY).  Reads
    from the first candidate root that has any matching record and picks the
    newest by ``generated_at``.  Returns None when nothing exists."""
    for latest_dir in _latest_read_dirs():
        try:
            if not latest_dir.is_dir():
                continue
            matches = sorted(latest_dir.glob(f"*-{mode}.json"))
        except OSError:
            continue
        best: dict[str, Any] | None = None
        best_key = ""
        for path in matches:
            try:
                rec = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            key = str(rec.get("generated_at") or "")
            if best is None or key > best_key:
                best, best_key = rec, key
        if best is not None:
            return best
    return None


def configured_symbols() -> tuple[list[str], str]:
    """The ticker list + default for the review page's regenerate dropdown.

    Sourced from ``$BULLETIN_TWEET_SYMBOLS`` (falls back to
    :data:`DEFAULT_SYMBOLS` = SPY).  The default is
    ``$BULLETIN_TWEET_LEAD_SYMBOL`` when it's in the list, else the first
    symbol."""
    raw = os.environ.get("BULLETIN_TWEET_SYMBOLS", "").strip()
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()] or list(DEFAULT_SYMBOLS)
    lead = os.environ.get("BULLETIN_TWEET_LEAD_SYMBOL", DEFAULT_LEAD_SYMBOL).strip().upper()
    default = lead if lead in symbols else symbols[0]
    return symbols, default


async def generate_and_store(
    db: "DatabaseManager",
    mode: str,
    symbol: str,
    day: date | None = None,
    site_url: str | None = None,
) -> dict[str, Any]:
    """Generate a post+reply for (symbol, mode), review it, persist it, and return the record.

    Backs the /admin review page's Regenerate button.  Reuses an already-
    connected ``db`` (the API's shared manager) and never posts.  The API
    can't run the screenshot, so the levels come from the same latest
    summary the live card reads, the fact-check runs without the image, and
    the page points the operator at the Live Bulletin page to export one."""
    if mode not in MODE_READ_LABEL:
        raise ValueError(f"Unknown mode: {mode!r}")
    day = day or _today_et()
    symbol = symbol.upper()
    site = site_url or os.environ.get("ZEROGEX_SITE_URL", "").strip() or DEFAULT_SITE_URL

    bulletins = await _fetch_bulletins(db, [symbol], day, mode)
    featured = _choose_featured(bulletins, symbol)
    headlines, news_problem = _fetch_fresh_headlines()
    problems = _data_problems(featured, None, news_problem, image_required=False)
    if featured is None:
        tweet = TweetBody(text="", fallback="", lead_symbol=symbol, featured_symbol=symbol)
    else:
        tweet, draft_problems = _write_and_review(
            mode, day, bulletins, featured, headlines, None, site
        )
        problems += draft_problems
    record = build_latest_record(
        mode=mode,
        day=day,
        tweet=tweet,
        featured=featured,
        headlines=headlines,
        generated_at=datetime.now(tz=ET).isoformat(),
        status="regenerated",
        problems=problems,
    )
    write_latest_record(record)
    return record


def _choose_featured(bulletins: list[SymbolBulletin], lead_symbol: str) -> SymbolBulletin | None:
    """The symbol this fire is about: the lead symbol when it has data, else
    the cleanest setup (see :func:`select_featured_symbol`)."""
    lead = lead_symbol.upper()
    for b in bulletins:
        if b.symbol.upper() == lead and (b.has_any_level() or b.spot is not None):
            return b
    return select_featured_symbol(bulletins, fallback=lead)


# ---------------------------------------------------------------------------
# Media rendering — bulletin PNG + replay clip
# ---------------------------------------------------------------------------


@dataclass
class MediaArtifacts:
    """Paths to the rendered media, or None when a render failed.

    ``clip_path`` is kept for the manifest's shape; the replay clip is no
    longer attached (X takes up to four images or one video per post, not a
    mix, and the post's picture is the live bulletin)."""

    png_path: Path | None = None
    clip_path: Path | None = None


@dataclass
class CardRender:
    """One screenshot of the live bulletin card and the numbers it drew."""

    png_path: Path | None = None
    levels: dict[str, Any] | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.png_path is not None and self.levels is not None


def _locate_frontend_helper(
    filename: str,
    explicit: str | None,
    env_var: str,
) -> Path | None:
    """Resolve one of the Playwright helper scripts on disk.

    Both render helpers live in the sibling ``zerogex-web`` repo at
    ``frontend/scripts/<filename>.mjs``. This walks the same candidate
    ladder for each: explicit override → ``$env_var`` → ``$ZEROGEX_WEB_DIR``
    → the dev checkout layout ``../zerogex-web/frontend/scripts/…``.
    Returns None (the caller degrades) when no candidate exists — the
    tweet job never depends on Playwright being installed."""
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    env_helper = os.environ.get(env_var, "").strip()
    if env_helper:
        candidates.append(Path(env_helper))
    web_dir = os.environ.get("ZEROGEX_WEB_DIR", "").strip()
    if web_dir:
        candidates.append(Path(web_dir) / "scripts" / filename)
    candidates.append(
        Path(__file__).resolve().parents[2].parent
        / "zerogex-web"
        / "frontend"
        / "scripts"
        / filename
    )
    return next((p for p in candidates if p.exists()), None)


def _run_frontend_helper(
    helper: Path,
    cmd_args: list[str],
    timeout_seconds: int,
    label: str,
) -> tuple[int | None, str]:
    """Run ``node <helper> <args>``: (exit code, stderr tail), or (None, why)
    when it couldn't run at all.

    Systemd runs with a stripped PATH — nvm-installed node isn't
    reachable via bare ``node``.  Operators can either symlink node
    into /usr/local/bin OR set ``BULLETIN_TWEET_NODE_BINARY`` in .env
    to the full path (e.g.
    ``/home/ubuntu/.nvm/versions/node/v22.22.2/bin/node``)."""
    node_bin = os.environ.get("BULLETIN_TWEET_NODE_BINARY", "").strip() or "node"
    cmd = [node_bin, str(helper), *cmd_args]
    try:
        proc = subprocess.run(  # noqa: S603 — args are constructed in-process
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.warning("bulletin_tweet: %s helper timed out after %ds", label, timeout_seconds)
        return None, f"it took longer than {timeout_seconds} seconds"
    except FileNotFoundError:
        return None, (
            f"{node_bin!r} wasn't found (set BULLETIN_TWEET_NODE_BINARY, or run "
            "make bulletin-tweet-bootstrap)"
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: %s helper couldn't start (%s)", label, exc)
        return None, f"it couldn't start ({exc})"
    tail = (proc.stderr or "").strip()[-500:]
    if proc.returncode != 0:
        logger.warning(
            "bulletin_tweet: %s helper exited %d, stderr: %s", label, proc.returncode, tail
        )
    return proc.returncode, tail


# What each of render-bulletin-png.mjs's exit codes means, for the notice.
_CARD_EXIT_REASONS = {
    2: (
        "playwright-core isn't installed in zerogex-web/frontend (deploy the website, "
        "then run make bulletin-tweet-bootstrap in zerogex-oa)"
    ),
    3: (
        "the snapshot page had no bulletin card (check BULLETIN_SNAPSHOT_TOKEN is the "
        "same in both .env files)"
    ),
    4: "the screenshot came out empty",
    5: "the card never finished loading (its data or logo didn't load)",
    6: "Chromium couldn't start (run make bulletin-tweet-bootstrap in zerogex-oa)",
}
# A rendered card is ~1280 px wide (640 CSS px at 2x); much smaller isn't one.
_MIN_CARD_PX = (600, 400)


def _png_dimensions(path: Path) -> tuple[int, int] | None:
    """(width, height) from a PNG's header, or None when it isn't a PNG."""
    try:
        with path.open("rb") as fh:
            head = fh.read(24)
    except OSError:
        return None
    if len(head) < 24 or head[:8] != b"\x89PNG\r\n\x1a\n" or head[12:16] != b"IHDR":
        return None
    return struct.unpack(">II", head[16:24])


def render_bulletin_card(
    symbol: str,
    mode: str,
    site_url: str,
    out_path: Path,
    helper_path: str | None = None,
    timeout_seconds: int = 120,
) -> CardRender:
    """Screenshot the live bulletin card and read back the numbers it drew.

    The helper (``frontend/scripts/render-bulletin-png.mjs``) visits
    ``/live-bulletin/snapshot/{symbol}``, waits for the card's ready signal,
    captures the ``[data-bulletin-card]`` element as PNG, and writes the
    card's own levels next to it.  This is the SAME ``<GammaReportCard>``
    the paid /live-bulletin page renders, at the moment the job fires.

    The snapshot page is token-gated by ``BULLETIN_SNAPSHOT_TOKEN`` on the
    frontend side; we pass the same value from env here so a stranger can't
    hit the public route and scrape gamma data.

    Every failure comes back as ``error``, in words the operator's notice
    can use; the caller holds the post."""
    helper = _locate_frontend_helper(
        "render-bulletin-png.mjs",
        helper_path,
        "BULLETIN_TWEET_PNG_HELPER",
    )
    if helper is None:
        return CardRender(
            error=(
                "The live bulletin screenshot script (zerogex-web/frontend/scripts/"
                "render-bulletin-png.mjs) wasn't found on this server; set "
                "ZEROGEX_WEB_DIR in the zerogex-oa .env."
            )
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path = out_path.with_suffix(".json")
    # A failed run must not leave an older picture behind to be attached.
    for stale in (out_path, meta_path):
        stale.unlink(missing_ok=True)

    cmd_args = [
        "--symbol",
        symbol.upper(),
        "--mode",
        mode,
        "--site-url",
        site_url,
        "--out",
        str(out_path),
        "--meta-out",
        str(meta_path),
    ]
    token = os.environ.get("BULLETIN_SNAPSHOT_TOKEN", "").strip()
    if token:
        cmd_args.extend(["--token", token])

    rc, detail = _run_frontend_helper(helper, cmd_args, timeout_seconds, "bulletin-png")
    if rc is None:
        return CardRender(error=f"The live bulletin screenshot failed: {detail}.")
    if rc != 0:
        reason = _CARD_EXIT_REASONS.get(rc, f"the screenshot script failed (exit {rc})")
        last = detail.splitlines()[-1] if detail else ""
        return CardRender(error=f"The live bulletin screenshot failed: {reason}. {last}".strip())
    dims = _png_dimensions(out_path)
    if dims is None or dims[0] < _MIN_CARD_PX[0] or dims[1] < _MIN_CARD_PX[1]:
        return CardRender(
            error=f"The live bulletin screenshot isn't a usable image ({dims or 'not a PNG'})."
        )
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        meta = None
    levels = meta.get("levels") if isinstance(meta, dict) else None
    if not isinstance(levels, dict) or not meta.get("ready"):
        return CardRender(
            png_path=out_path,
            error=(
                "The screenshot script didn't report the card's levels, so the post "
                "couldn't be matched to the picture (deploy the latest zerogex-web)."
            ),
        )
    card_symbol = str(levels.get("symbol") or "").upper()
    if card_symbol != symbol.upper():
        return CardRender(
            png_path=out_path,
            error=(
                f"The live bulletin card came back for {card_symbol or 'no symbol'}, "
                f"not {symbol.upper()}."
            ),
        )
    return CardRender(png_path=out_path, levels=levels)


_CARD_LEVEL_FIELDS = ("spot", "gamma_flip", "call_wall", "put_wall", "max_pain", "net_gex")
_CARD_REGIMES = ("positive", "negative", "neutral", "unresolved")


def _apply_card_levels(bulletin: SymbolBulletin, levels: dict[str, Any]) -> None:
    """Make the post quote exactly what the attached card shows.

    The database query ran a few seconds before the screenshot and the
    analytics engine republishes every minute, so the card's own numbers
    replace ours: spot, the flip, both walls, max pain, net GEX (at spot)
    and the regime the card's badge shows.  A level the card doesn't show
    stays empty here too, and the post is held for it."""
    for name in _CARD_LEVEL_FIELDS:
        setattr(bulletin, name, _to_float(levels.get(name)))
    bulletin.spot_is_projected = bool(levels.get("spot_is_projected"))
    if bulletin.spot_is_projected and levels.get("spot_source"):
        bulletin.future_symbol = str(levels["spot_source"])
    regime = levels.get("regime")
    if regime in _CARD_REGIMES:
        bulletin.regime = regime
    bulletin.card_as_of = str(levels.get("as_of") or "") or None
    bulletin.momentum_label = _derive_momentum_label(bulletin)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _reference_close(closes: dict[str, Any], mode: str) -> Any:
    """The session close the featured symbol's spot is measured against.

    ``get_session_closes`` returns TWO closes and folds today's close into
    ``current_session_close`` only once the cash session has ended (>= 16:00
    ET):

      * ``current_session_close`` — the most recent COMPLETED session close.
      * ``prior_session_close``   — the session close immediately before it.

    Which one is "the prior close" the read gaps from depends on the fire,
    because the featured ``spot`` refers to a different session in each:

      * premarket / midday — ``spot`` is today's pre-market / live price and
        today's session has NOT closed, so ``current_session_close`` is
        yesterday's close: exactly the reference the gap is measured from.
      * close — fired at 16:05 once today's session HAS closed, so ``spot``
        is today's fresh close and ``current_session_close`` is today; the
        reference is the session *before* it, ``prior_session_close``.

    Reading ``prior_session_close`` for every mode (the old behaviour) quoted
    the close from TWO sessions ago on the pre-market and midday reads — e.g.
    a Wednesday Morning Read gapping from Monday's close instead of
    Tuesday's."""
    key = "prior_session_close" if mode == "close" else "current_session_close"
    return closes.get(key)


async def _attach_price_action(
    db: DatabaseManager, bulletin: SymbolBulletin, day: date, mode: str
) -> None:
    """Best-effort: hang the featured symbol's price action off the bulletin.

    Pulls the previous close (position vs prior close) and the day's
    intraday session range so the LLM can narrate the path, then derives a
    plain-language regime + momentum cue.  Every query is wrapped — a miss
    just leaves that field None, and the LLM writes with what resolved.

    ``mode`` picks which of the two session closes is the "prior close" the
    spot is measured against — see :func:`_reference_close`."""
    sym = bulletin.symbol
    try:
        closes = await db.get_session_closes(sym)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: get_session_closes(%s) failed (%s)", sym, exc)
        closes = None
    if closes:
        bulletin.prior_close = _to_float(_reference_close(closes, mode))

    try:
        ohlc = await db.get_intraday_ohlc(sym, day)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: get_intraday_ohlc(%s) failed (%s)", sym, exc)
        ohlc = None
    if ohlc:
        bulletin.session_open = _to_float(ohlc.get("session_open"))
        bulletin.session_high = _to_float(ohlc.get("session_high"))
        bulletin.session_low = _to_float(ohlc.get("session_low"))

    bulletin.regime = _derive_regime(bulletin.net_gex, bulletin.spot, bulletin.gamma_flip)
    bulletin.momentum_label = _derive_momentum_label(bulletin)


# The fires that have a session path worth reading.  The 09:15 pre-market
# read is deliberately excluded: nothing has traded yet, so there is no
# migration to describe and no tape to test a level against.
LEVEL_HISTORY_MODES = ("midday", "close")


async def _attach_level_history(
    db: DatabaseManager, bulletin: SymbolBulletin, day: date, mode: str
) -> None:
    """Best-effort: hang the day's level path off the bulletin.

    The walls migrate: a put wall that walked 777 → 776 → 775, breaking the
    first two and holding the third, is three separate stories, and the
    latest snapshot alone tells none of them.  The writer narrates the
    session from this path, and the checks hold a level claim to it.

    The bulletin's own levels are NOT changed.  They stay what the live card
    shows, which on the 16:05 close fire is the post-bell chain (the day's
    0DTE has rolled off, so the walls are the next session's); the post labels
    them that way and tells the session's story from this path instead.

    Every failure just leaves ``level_history`` None."""
    if mode not in LEVEL_HISTORY_MODES:
        return
    sym = bulletin.symbol
    try:
        level_rows = await db.get_intraday_level_series(sym, day)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: get_intraday_level_series(%s) failed (%s)", sym, exc)
        return
    if not level_rows:
        return
    try:
        price_rows = await db.get_underlying_candles_for_session(sym, day)
    except Exception as exc:  # noqa: BLE001
        # Without bars we can still report the migration, just not the
        # tested/held/broke outcomes.
        logger.warning(
            "bulletin_tweet: get_underlying_candles_for_session(%s) failed (%s) — "
            "level outcomes will be unknown",
            sym,
            exc,
        )
        price_rows = []

    try:
        history = lh.build_level_history(sym, day, mode, level_rows, price_rows)
    except Exception as exc:  # noqa: BLE001 — never let this break the tweet
        logger.warning("bulletin_tweet: level-history build failed for %s (%s)", sym, exc)
        return
    if history is None:
        return
    bulletin.level_history = history


async def _fetch_bulletins(
    db: DatabaseManager,
    symbols: list[str],
    day: date,
    mode: str,
) -> list[SymbolBulletin]:
    """Fetch the latest GEX summary + price action for every requested symbol.

    Each call is wrapped so a single symbol's DB miss doesn't take
    the whole tweet down — we just render a placeholder block for the
    missing symbol (or elide it entirely if it has no fields at all).

    ``mode`` is passed down to the price-action attach so the "prior close"
    the read gaps from is the right session for the fire (see
    :func:`_reference_close`)."""
    out: list[SymbolBulletin] = []
    for sym in symbols:
        sym = sym.upper()
        try:
            row = await db.get_latest_gex_summary(sym)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bulletin_tweet: get_latest_gex_summary(%s) failed (%s) — eliding symbol block",
                sym,
                exc,
            )
            row = None
        # Charm-into-Close headline is sourced from the persisted forced-flow
        # snapshot; a miss just elides that one line (best-effort, never fatal).
        try:
            ff_row = await db.get_latest_forced_flow(sym)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bulletin_tweet: get_latest_forced_flow(%s) failed (%s) — eliding charm line",
                sym,
                exc,
            )
            ff_row = None
        bulletin = _shape_bulletin(row, sym)
        if ff_row is not None:
            bulletin.charm_close_flow = _to_float(ff_row.get("close_charm_flow"))
        # SPX (cash index) has no live overnight print — outside the cash
        # session its spot_price is a frozen prior 16:00 close.  Project the
        # implied level from @ES so a pre-market bulletin shows where the
        # index is trading now, and FLAG it as projected.  The walls / gamma
        # flip / max pain / net GEX stay the last cash-session structure —
        # they're strike-space quantities with no futures equivalent (mixed
        # card: projected spot + last-session structure).  Returns None (keep
        # the frozen cash spot) in-session, on weekends, or on an empty feed —
        # so SPY / QQQ and mid-session fires are untouched.
        try:
            proj = await implied_index_spot(db, sym)
        except Exception as exc:  # noqa: BLE001 — never let projection break the tweet
            logger.warning("bulletin_tweet: index projection failed (%s): %s", sym, exc)
            proj = None
        if proj is not None:
            bulletin.spot = proj.implied_price
            bulletin.spot_is_projected = True
            bulletin.future_symbol = proj.future_symbol
            logger.info(
                "bulletin_tweet: %s spot projected from %s — implied $%.2f "
                "(cash close $%.2f, overnight %+.2f pts)",
                sym,
                proj.future_symbol,
                proj.implied_price,
                proj.cash_ref_close,
                proj.gap_points,
            )
        # The day's level path (midday and close fires).
        await _attach_level_history(db, bulletin, day, mode)
        # Price action (prior close, session range, regime, momentum) — the
        # inputs the LLM narrates the day's path from.  Best-effort.
        await _attach_price_action(db, bulletin, day, mode)
        out.append(bulletin)
    return out


def _write_manifest_and_text(
    artifact_dir: Path,
    tweet: TweetBody,
    media: MediaArtifacts,
    mode: str,
    day: date,
    bulletins: list[SymbolBulletin],
    state: str = "dry_run",
    posted_id: str | None = None,
    reply_id: str | None = None,
    problems: list[str] | None = None,
) -> None:
    """Persist a JSON manifest + the raw tweet text next to the media.

    The ``state`` is one of:

      * dry_run     — no --post flag, just showing what would go out
      * pending     — --stage was used and the review passed; waiting for
                      approval to post
      * blocked     — held back; ``problems`` says why
      * post_failed — the review passed but X rejected the post
      * posted      — the tweet was successfully sent to X

    Operators need to be able to open one directory and see everything
    that would have gone out — the main text, the threaded link reply,
    PNG, clip and a small JSON with the level fields sourced from the DB
    (so a wrong number in the tweet can be traced back to the underlying
    summary row).  The ``state`` field lets the approve command
    distinguish drafts that are eligible to POST from ones already
    sent."""
    body_path = artifact_dir / "tweet_text.md"
    body_path.write_text(tweet.text + "\n", encoding="utf-8")
    fallback_path = artifact_dir / "tweet_text_fallback.md"
    fallback_path.write_text(tweet.fallback + "\n", encoding="utf-8")
    reply_path = artifact_dir / "tweet_reply.md"
    reply_path.write_text(tweet.reply_text + "\n", encoding="utf-8")

    manifest = {
        "mode": mode,
        "date": day.isoformat(),
        "state": state,
        "posted_id": posted_id,
        "reply_id": reply_id,
        "lead_symbol": tweet.lead_symbol,
        "featured_symbol": tweet.featured_symbol,
        "symbols_present": tweet.symbols_present,
        "reply_text": tweet.reply_text,
        "text_len": len(tweet.text),
        "fallback_len": len(tweet.fallback),
        "problems": list(problems or []),
        "media": {
            "png": str(media.png_path) if media.png_path else None,
            "clip": str(media.clip_path) if media.clip_path else None,
        },
        "bulletins": [
            {
                "symbol": b.symbol,
                "spot": b.spot,
                "gamma_flip": b.gamma_flip,
                "call_wall": b.call_wall,
                "put_wall": b.put_wall,
                "max_pain": b.max_pain,
                "net_gex": b.net_gex,
                # The live card's label when the fields above came from it.
                "card_as_of": b.card_as_of,
                # Traceability: the session path behind anything the post
                # says about a level.
                "level_history": (b.level_history.to_prompt_dict() if b.level_history else None),
            }
            for b in bulletins
        ],
    }
    (artifact_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def _will_post(args: argparse.Namespace) -> bool:
    """Whether this run posts to X once the review passes.

    Autopilot: BULLETIN_TWEET_AUTOPILOT=1 in .env silently upgrades
    --stage to --post at runtime, so switching to full autopost is a
    one-line env-var flip — no systemd surgery required.  Explicit
    --post on the CLI always wins regardless."""
    autopilot = os.environ.get("BULLETIN_TWEET_AUTOPILOT", "").strip() in ("1", "true", "yes")
    return bool(args.post) or (bool(args.stage) and autopilot)


async def _run(args: argparse.Namespace) -> int:
    """One fire.  Returns 0 when it posted, staged, dry-ran cleanly or skipped
    a non-trading day; 1 when the post was held back or X rejected it."""
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info(
            "bulletin_tweet[%s]: skipping %s — not a trading day (weekend or NYSE holiday).",
            args.mode,
            day.isoformat(),
        )
        return 0

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        logger.warning("bulletin_tweet: no symbols resolved — exiting 0")
        return 0
    lead = args.lead_symbol.upper()

    effective_post = _will_post(args)
    effective_stage = bool(args.stage) and not effective_post
    fire = _Fire(
        mode=args.mode,
        day=day,
        artifact_dir=resolve_artifact_dir(args.artifact_dir, args.mode, day),
        notify=effective_post or effective_stage,
        posting=effective_post,
    )

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet[%s]: DB connect failed (%s)", args.mode, exc)
        return fire.hold([f"Couldn't connect to the database ({exc})."], symbol=lead)

    try:
        bulletins = await _fetch_bulletins(db, symbols, day, args.mode)
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass

    featured = _choose_featured(bulletins, lead)
    if featured is None:
        return fire.hold(
            ["Every symbol's GEX summary was missing, so there was nothing to post."],
            symbol=lead,
            bulletins=bulletins,
        )

    # The live bulletin, as it stands now: the picture that gets attached,
    # and the numbers the post quotes (see _apply_card_levels).
    media = MediaArtifacts()
    if args.no_media:
        card = CardRender(error="Rendering the live bulletin image was skipped (--no-media).")
    else:
        card = render_bulletin_card(
            featured.symbol,
            args.mode,
            args.site_url,
            fire.artifact_dir / f"bulletin-{featured.symbol.lower()}.png",
        )
        if card.ok:
            media.png_path = card.png_path
            _apply_card_levels(featured, card.levels)
    fire.card_png = card.png_path

    headlines, news_problem = _fetch_fresh_headlines()
    problems = _data_problems(featured, card, news_problem)
    card_png = media.png_path.read_bytes() if media.png_path else None
    tweet, draft_problems = _write_and_review(
        args.mode, day, bulletins, featured, headlines, card_png, args.site_url
    )
    problems += draft_problems
    if effective_post and not problems:
        late = _deadline_problem(args.mode, day)
        if late:
            problems.append(late)

    fire.tweet, fire.media, fire.bulletins = tweet, media, bulletins
    fire.featured, fire.headlines = featured, headlines
    if problems:
        return fire.hold(problems, symbol=featured.symbol)

    if effective_stage:
        fire.record("pending", status="ready")
        _log_approval_required(args.mode, fire.artifact_dir, tweet)
        _call_notify_hook(args.mode, fire.artifact_dir, tweet, media)
        _send_xpost_ready_email(args.mode, png_path=media.png_path)
        return 0

    if not effective_post:
        fire.record("dry_run", status="dry_run")
        logger.info(
            "bulletin_tweet[%s]: DRY RUN (passed review; no --post flag) — artifacts at %s\n"
            "----\n%s\n----",
            args.mode,
            fire.artifact_dir,
            tweet.text,
        )
        return 0

    result = post_bulletin(tweet=tweet, media=media, long=args.long, mode_label=args.mode)
    if not result.ok:
        return fire.hold([result.error or "X rejected the post."], state="post_failed")
    fire.record(
        "posted",
        status="posted",
        posted_id=result.tweet_id,
        reply_id=result.reply_id,
        tweet_url=result.tweet_url,
        problems=[result.reply_error] if result.reply_error else None,
    )
    _send_xpost_sent_email(args.mode, featured.symbol, result)
    return 0


@dataclass
class _Fire:
    """What one run has produced so far, so any exit path can record it,
    hold it, and tell the operator.  Filled in as the run goes."""

    mode: str
    day: date
    artifact_dir: Path
    notify: bool  # a scheduled fire (--stage / --post), not a manual preview
    posting: bool
    tweet: TweetBody | None = None
    media: MediaArtifacts = field(default_factory=MediaArtifacts)
    bulletins: list[SymbolBulletin] = field(default_factory=list)
    featured: SymbolBulletin | None = None
    headlines: list = field(default_factory=list)
    card_png: Path | None = None

    def record(
        self,
        state: str,
        status: str,
        problems: list[str] | None = None,
        posted_id: str | None = None,
        reply_id: str | None = None,
        tweet_url: str | None = None,
        symbol: str | None = None,
    ) -> None:
        """Write the manifest + the review page's latest record."""
        sym = (symbol or (self.featured.symbol if self.featured else "") or "").upper()
        tweet = self.tweet or TweetBody(text="", fallback="", lead_symbol=sym, featured_symbol=sym)
        try:
            _write_manifest_and_text(
                self.artifact_dir,
                tweet,
                self.media,
                self.mode,
                self.day,
                self.bulletins,
                state=state,
                posted_id=posted_id,
                reply_id=reply_id,
                problems=problems,
            )
        except OSError as exc:
            logger.warning("bulletin_tweet: failed to write the manifest (%s)", exc)
        write_latest_record(
            build_latest_record(
                mode=self.mode,
                day=self.day,
                tweet=tweet,
                featured=self.featured,
                headlines=self.headlines,
                media=self.media,
                generated_at=datetime.now(tz=ET).isoformat(),
                status=status,
                problems=problems,
                tweet_url=tweet_url,
            )
        )

    def hold(
        self,
        problems: list[str],
        symbol: str | None = None,
        bulletins: list[SymbolBulletin] | None = None,
        state: str = "blocked",
    ) -> int:
        """Don't post: record why, tell the operator, and fail the run."""
        if bulletins is not None:
            self.bulletins = bulletins
        sym = (symbol or (self.featured.symbol if self.featured else "") or "").upper()
        self.record(state, status=state, problems=problems, symbol=sym)
        logger.error(
            "bulletin_tweet[%s]: NOT POSTED — %d problem(s):\n%s",
            self.mode,
            len(problems),
            "\n".join(f"  - {p}" for p in problems),
        )
        if self.notify:
            _send_xpost_held_email(
                self.mode,
                sym,
                problems,
                self.tweet,
                self.card_png,
                posting=self.posting,
            )
        return 1


@dataclass
class PostResult:
    """What happened when the post went to X."""

    ok: bool
    tweet_id: str | None = None
    reply_id: str | None = None
    error: str | None = None
    reply_error: str | None = None

    @property
    def tweet_url(self) -> str | None:
        return f"https://x.com/i/web/status/{self.tweet_id}" if self.tweet_id else None


def post_bulletin(
    tweet: TweetBody,
    media: MediaArtifacts,
    long: bool = True,
    mode_label: str = "",
) -> PostResult:
    """Upload the live bulletin image, post the text with it, then thread
    the link reply under it.

    Shared between the direct-post path (``bulletin_tweet --post``) and
    the approve-a-staged-draft path (``bulletin_approve``).  Nothing goes
    out without the image: a missing picture or a failed upload stops here,
    and there is no text-only retry and no swap to the short body.  Every
    call is signed with the four OAuth1 keys (see
    :mod:`src.jobs.x_media_client`).  A failed reply doesn't undo the main
    post; it's reported in ``reply_error``."""
    from src.jobs import x_media_client  # local import — optional dep path

    try:
        creds = x_media_client.load_credentials_from_env()
    except x_media_client.MissingCredentialsError as exc:
        return PostResult(ok=False, error=f"{exc}. Nothing was posted.")
    if media.png_path is None or not media.png_path.exists():
        return PostResult(
            ok=False, error="There's no live bulletin image to attach, so nothing was posted."
        )
    text = tweet.text if long else tweet.fallback
    if not text.strip():
        return PostResult(ok=False, error="The post text is empty, so nothing was posted.")
    if len(text) > LONG_TWEET_MAX_LEN:
        return PostResult(
            ok=False,
            error=f"The post is {len(text):,} characters, over X's {LONG_TWEET_MAX_LEN:,} limit.",
        )

    try:
        media_id = x_media_client.upload_image(media.png_path, creds)
    except x_media_client.XApiError as exc:
        logger.warning("bulletin_tweet[%s]: image upload failed (%s)", mode_label, exc)
        return PostResult(
            ok=False, error=f"X rejected the image upload, so nothing was posted ({exc})."
        )
    try:
        tweet_id = x_media_client.post_tweet(text, creds, media_ids=[media_id])
    except x_media_client.XApiError as exc:
        logger.warning("bulletin_tweet[%s]: X rejected the post (%s)", mode_label, exc)
        return PostResult(ok=False, error=f"X rejected the post ({exc}).")
    logger.info("bulletin_tweet[%s]: posted tweet id=%s with the image", mode_label, tweet_id)

    result = PostResult(ok=True, tweet_id=tweet_id)
    if tweet.reply_text:
        try:
            result.reply_id = x_media_client.post_tweet(tweet.reply_text, creds, reply_to=tweet_id)
            logger.info(
                "bulletin_tweet[%s]: posted link reply id=%s under %s",
                mode_label,
                result.reply_id,
                tweet_id,
            )
        except x_media_client.XApiError as exc:
            logger.warning("bulletin_tweet[%s]: link reply failed (%s)", mode_label, exc)
            result.reply_error = f"The post went out, but the threaded link reply failed ({exc})."
    return result


def _log_approval_required(mode: str, artifact_dir: Path, tweet: TweetBody) -> None:
    logger.info(
        "\n"
        "================================================================\n"
        "bulletin_tweet[%s]: STAGED — APPROVAL REQUIRED\n"
        "================================================================\n"
        "Artifacts:    %s\n"
        "Text length:  %d chars (fallback %d)\n"
        "Approve with: bin/bulletin-approve.sh %s\n"
        "Discard with: bin/bulletin-approve.sh %s --discard\n"
        "Autopilot:    set BULLETIN_TWEET_AUTOPILOT=1 in .env\n"
        "================================================================\n"
        "----\n%s\n----",
        mode,
        artifact_dir,
        len(tweet.text),
        len(tweet.fallback),
        mode,
        mode,
        tweet.text,
    )


# Friendly timing word for the email subject — matches the operator's
# "market open / midday / market close" phrasing.
_TIMING_WORD = {"premarket": "Market Open", "midday": "Midday", "close": "Market Close"}


def _xpost_admin_url() -> str:
    """The /admin/x-post review-page URL used in the X-Post-Ready email."""
    explicit = os.environ.get("BULLETIN_TWEET_ADMIN_URL", "").strip()
    if explicit:
        return explicit
    site = os.environ.get("ZEROGEX_SITE_URL", "").strip().rstrip("/") or DEFAULT_SITE_URL
    return f"{site}/admin/x-post"


def _emails_enabled() -> bool:
    return os.environ.get("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _png_attachments(png_path: Path | None) -> list[dict[str, str]]:
    """The bulletin image as a Resend attachment, when there is one."""
    if png_path is None:
        return []
    try:
        data = png_path.read_bytes()
    except OSError:
        return []
    return [{"filename": png_path.name, "content": base64.b64encode(data).decode("ascii")}]


def _send_operator_email(
    subject: str,
    html_body: str,
    text_body: str,
    attachments: list[dict[str, str]] | None = None,
) -> bool:
    """Email the operator through Resend.  Best-effort: False (logged) on any
    missing config or send error, never raises.

    Reuses the same RESEND_API_KEY / RESEND_FROM_EMAIL / BULLETIN_TWEET_EMAIL_TO
    the frontend already uses."""
    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    from_email = os.environ.get("RESEND_FROM_EMAIL", "").strip()
    to_email = os.environ.get("BULLETIN_TWEET_EMAIL_TO", "").strip()
    if not (api_key and from_email and to_email):
        logger.info(
            "bulletin_tweet: email %r skipped — RESEND_API_KEY / RESEND_FROM_EMAIL / "
            "BULLETIN_TWEET_EMAIL_TO not all set",
            subject,
        )
        return False
    payload: dict[str, Any] = {
        "from": from_email,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
        "text": text_body,
    }
    if attachments:
        payload["attachments"] = attachments
    req = Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Resend sits behind Cloudflare, which 403s the default
            # "Python-urllib/x.y" User-Agent as a bot.  Send a real product UA
            # so the API call isn't blocked (curl-based hooks worked for the
            # same reason — curl's UA isn't on the bad-bot list).
            "User-Agent": "zerogex-bulletin/1.0 (+https://zerogex.io)",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=15) as resp:
            resp.read()
    except (HTTPError, URLError) as exc:
        logger.warning("bulletin_tweet: email %r failed (%s)", subject, exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: email %r error (%s)", subject, exc)
        return False
    logger.info("bulletin_tweet: email %r sent to %s", subject, to_email)
    return True


def _review_link_html(url: str, label: str) -> str:
    return (
        f'<p><a href="{url}" style="display:inline-block;padding:10px 18px;'
        f"background:#111;color:#fff;border-radius:8px;text-decoration:none;"
        f'font-weight:600">{label} &rarr;</a></p>'
        f'<p style="color:#666;font-size:13px">Or open: <a href="{url}">{url}</a>'
        f"<br>You'll need to be signed in to your Admin account to view it.</p>"
    )


def _send_xpost_ready_email(mode: str, png_path: Path | None = None) -> bool:
    """Email a "<Timing> X-Post Ready" notice with a link to /admin/x-post.

    Sent when a staged (not autopilot) post passed review.  The live
    bulletin image rides along as an attachment, ready to add to the post.
    Disable with BULLETIN_TWEET_ADMIN_EMAIL_ENABLED=0.

    This REPLACES the old "approval needed" email — remove the deprecated
    BULLETIN_TWEET_NOTIFY_HOOK from .env so the old one stops (a warning is
    logged if both are active)."""
    if not _emails_enabled():
        return False
    timing = _TIMING_WORD.get(mode, mode)
    url = _xpost_admin_url()
    image_note = " The live bulletin image is attached; add it to the post." if png_path else ""
    html_body = (
        f"<h2>{timing} X-Post Ready</h2>"
        f"<p>The {timing.lower()} X-post has been generated and passed review.{image_note}</p>"
        + _review_link_html(url, "Review &amp; copy the post")
    )
    text_body = (
        f"{timing} X-Post Ready\n\nPassed review.{image_note}\n\n"
        f"Review & copy the post: {url}\n(Admin sign-in required.)"
    )
    sent = _send_operator_email(
        f"{timing} X-Post Ready", html_body, text_body, _png_attachments(png_path)
    )
    # Nudge if the deprecated approval hook is ALSO set — that's the old
    # "approval needed" email; they'll get both until it's removed.
    if sent and os.environ.get("BULLETIN_TWEET_NOTIFY_HOOK", "").strip():
        logger.warning(
            "bulletin_tweet: BULLETIN_TWEET_NOTIFY_HOOK is set alongside the "
            "built-in X-Post Ready email — remove it from .env to stop the old "
            "'approval needed' email.",
        )
    return sent


def _send_xpost_sent_email(mode: str, symbol: str, result: PostResult) -> bool:
    """Email a "<Timing> X-Post Sent" notice with the link to the post.

    Disable with BULLETIN_TWEET_ADMIN_EMAIL_ENABLED=0; a failed link reply is
    reported here either way."""
    if not _emails_enabled() and not result.reply_error:
        return False
    timing = _TIMING_WORD.get(mode, mode)
    link = result.tweet_url or ""
    reply_html = (
        f"<p><strong>{html.escape(result.reply_error)}</strong></p>" if result.reply_error else ""
    )
    reply_text = f"\n\n{result.reply_error}" if result.reply_error else ""
    html_body = (
        f"<h2>{timing} X-Post Sent</h2>"
        f"<p>The ${html.escape(symbol)} {timing.lower()} post passed review and went out "
        f"with the live bulletin image attached.</p>"
        f'<p><a href="{link}">{link}</a></p>{reply_html}'
    )
    text_body = (
        f"{timing} X-Post Sent\n\n${symbol}, with the live bulletin image: {link}{reply_text}"
    )
    subject = f"{timing} X-Post Sent" + (" (link reply failed)" if result.reply_error else "")
    return _send_operator_email(subject, html_body, text_body)


def _send_xpost_held_email(
    mode: str,
    symbol: str,
    problems: list[str],
    tweet: TweetBody | None,
    png_path: Path | None,
    posting: bool,
) -> bool:
    """Email the operator that the post was held back, with every reason, the
    draft, and the image when one rendered.

    Always attempted when Resend is configured, whatever
    BULLETIN_TWEET_ADMIN_EMAIL_ENABLED says: this is the notice the operator
    can't do without."""
    timing = _TIMING_WORD.get(mode, mode)
    url = _xpost_admin_url()
    where = "posted to X" if posting else "sent for review"
    items_html = "".join(f"<li>{html.escape(p)}</li>" for p in problems)
    items_text = "\n".join(f"- {p}" for p in problems)
    draft_html = ""
    draft_text = ""
    if tweet is not None and tweet.text.strip():
        draft = f"{tweet.text}\n\n--- reply ---\n{tweet.reply_text}"
        draft_html = (
            "<p>The draft, as it stood:</p>"
            f'<pre style="white-space:pre-wrap;font-family:inherit;background:#f5f5f5;'
            f'padding:12px;border-radius:8px">{html.escape(draft)}</pre>'
        )
        draft_text = f"\n\nThe draft, as it stood:\n\n{draft}"
    image_note = " The live bulletin image that rendered is attached." if png_path else ""
    html_body = (
        f"<h2>{timing} X-Post NOT {'sent' if posting else 'ready'}</h2>"
        f"<p>Nothing was {where}. Here's what went wrong:</p><ul>{items_html}</ul>"
        f"<p>{image_note.strip()}</p>{draft_html}" + _review_link_html(url, "Open the review page")
    )
    text_body = (
        f"{timing} X-Post NOT {'sent' if posting else 'ready'}\n\n"
        f"Nothing was {where}. Here's what went wrong:\n{items_text}\n{image_note.strip()}"
        f"{draft_text}\n\nReview page: {url}\n(Admin sign-in required.)"
    )
    subject = f"{timing} X-Post NOT {'sent' if posting else 'ready'}: ${symbol}"
    return _send_operator_email(subject, html_body, text_body, _png_attachments(png_path))


def _call_notify_hook(
    mode: str,
    artifact_dir: Path,
    tweet: TweetBody,
    media: MediaArtifacts,
) -> None:
    """Invoke $BULLETIN_TWEET_NOTIFY_HOOK if configured — noop otherwise.

    The hook is called with args ``<mode> <artifact_dir>`` and receives
    these extra env vars so an email/Slack/ntfy script has everything
    it needs without re-parsing the manifest:

      * BULLETIN_TWEET_MODE
      * BULLETIN_TWEET_ARTIFACT_DIR
      * BULLETIN_TWEET_TEXT_LEN
      * BULLETIN_TWEET_HAS_PNG
      * BULLETIN_TWEET_HAS_CLIP
      * BULLETIN_TWEET_LEAD_SYMBOL

    Any error from the hook logs a warning and returns — never fails
    the staging job."""
    hook = os.environ.get("BULLETIN_TWEET_NOTIFY_HOOK", "").strip()
    if not hook:
        logger.info(
            "bulletin_tweet: no BULLETIN_TWEET_NOTIFY_HOOK configured — "
            "skipping notification (operator polls artifact dir)",
        )
        return
    hook_path = Path(hook)
    if not hook_path.exists():
        logger.warning(
            "bulletin_tweet: notify hook %s does not exist — skipping notification",
            hook_path,
        )
        return

    env = os.environ.copy()
    env["BULLETIN_TWEET_MODE"] = mode
    env["BULLETIN_TWEET_ARTIFACT_DIR"] = str(artifact_dir)
    env["BULLETIN_TWEET_TEXT_LEN"] = str(len(tweet.text))
    env["BULLETIN_TWEET_HAS_PNG"] = "1" if media.png_path else "0"
    env["BULLETIN_TWEET_HAS_CLIP"] = "1" if media.clip_path else "0"
    env["BULLETIN_TWEET_LEAD_SYMBOL"] = tweet.lead_symbol
    logger.info("bulletin_tweet: calling notify hook %s", hook_path)
    try:
        proc = subprocess.run(  # noqa: S603
            [str(hook_path), mode, str(artifact_dir)],
            env=env,
            timeout=30,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: notify hook failed (%s) — %s", hook_path, exc)
        return
    if proc.returncode != 0:
        logger.warning(
            "bulletin_tweet: notify hook exited %d (stdout=%r stderr=%r)",
            proc.returncode,
            proc.stdout[:500],
            proc.stderr[:500],
        )
    else:
        logger.info(
            "bulletin_tweet: notify hook exited 0 — stdout: %s",
            proc.stdout.strip()[:500] or "<empty>",
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=MODES,
        required=True,
        help="Which of the three daily fires this invocation is.",
    )
    parser.add_argument(
        "--symbols",
        default=os.environ.get("BULLETIN_TWEET_SYMBOLS", ",".join(DEFAULT_SYMBOLS)),
        help=("Comma-separated symbols to fetch (default: $BULLETIN_TWEET_SYMBOLS or SPY)."),
    )
    parser.add_argument(
        "--lead-symbol",
        default=os.environ.get("BULLETIN_TWEET_LEAD_SYMBOL", DEFAULT_LEAD_SYMBOL),
        help=(
            "The symbol the post is about; its Live Bulletin card is attached. "
            "Default: $BULLETIN_TWEET_LEAD_SYMBOL or SPY."
        ),
    )
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Default: today ET.")
    parser.add_argument(
        "--post",
        action="store_true",
        help=(
            "Actually post to X (once the review passes). Without this flag the "
            "job dry-runs even when the X keys are set — safe by default."
        ),
    )
    parser.add_argument(
        "--stage",
        action="store_true",
        help=(
            "Stage the draft for human approval instead of posting.  Writes "
            "the full artifact set (text + PNG + manifest with state=pending, "
            "or state=blocked when the review fails) and emails the operator.  "
            "Operator approves with ``bin/bulletin-approve.sh <mode>``.  "
            "Set BULLETIN_TWEET_AUTOPILOT=1 in .env to upgrade --stage to "
            "--post at runtime — the one-line switch to full autopilot."
        ),
    )
    parser.add_argument(
        "--long",
        action="store_true",
        default=True,
        help=(
            "Post the full multi-paragraph body (requires X Premium on the bot "
            "handle). Default on.  If X rejects it, nothing is posted."
        ),
    )
    parser.add_argument(
        "--short",
        dest="long",
        action="store_false",
        help="Force the 280-char single-tweet body regardless of Premium.",
    )
    parser.add_argument(
        "--no-media",
        action="store_true",
        help=(
            "Skip the live bulletin screenshot.  Useful for a fast preview of the "
            "copy; a run without the image can never post."
        ),
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help=(
            "Override the directory dry-run artifacts (text + PNG + manifest) are "
            "written to. Default: $BULLETIN_TWEET_ARTIFACT_DIR or "
            "/var/lib/zerogex-oa/bulletin-tweets."
        ),
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
    try:
        return asyncio.run(_run(args))
    except Exception as exc:  # noqa: BLE001 — a crash still has to reach the operator
        logger.exception("bulletin_tweet[%s]: crashed", args.mode)
        if args.stage or args.post:
            _send_xpost_held_email(
                args.mode,
                args.lead_symbol.upper(),
                [
                    f"The job crashed before it finished ({exc!r}). "
                    "The details are in the journal: make bulletin-tweet-status."
                ],
                None,
                None,
                posting=_will_post(args),
            )
        return 1


if __name__ == "__main__":
    sys.exit(main())
