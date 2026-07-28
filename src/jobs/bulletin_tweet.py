"""Live-Bulletin auto-tweet — three daily reads posted to @zerogex_io.

Fires three times per trading day, backed by the same script + three
systemd timers:

  * ``--mode premarket`` at 09:15 ET — pre-market update posted 15 min
    before the cash open, framing the day's Gamma Flip / Call Wall /
    Put Wall structure across SPY, SPX and QQQ.
  * ``--mode midday`` at 12:30 ET — mid-session update, same shape,
    posted against the noon-hour snapshot.
  * ``--mode close`` at 16:05 ET — closing update posted 5 min after
    the cash bell.

Every post FEATURES ONE symbol — the one with the cleanest setup at
the moment the job fires (see :func:`select_featured_symbol`: the
symbol whose spot is pressed closest to a decision level, with the
gamma flip — the regime boundary — weighted above the walls, since a
symbol straddling its flip is a stronger story than one merely pinned).
SPY / SPX / QQQ are all still fetched so
the copy can cross-reference the other two, but the headline, the
numeric map and the attached card all center on the featured symbol.

Every post includes:

  * A multi-paragraph, X-native read-out of the featured symbol's
    levels sourced live from the same ``get_latest_gex_summary``
    powering the Live Bulletin admin page — so the tweet and the
    on-site card can never contradict each other.  The main post
    carries NO site link and NO hashtags (the cashtag is in the
    header); the ``https://zerogex.io`` link is posted separately as a
    threaded reply so it doesn't suppress the main post's reach.
  * A PNG attachment: a screenshot of the exact ``GammaReportCard``
    component the paid /live-bulletin page renders, captured by the
    frontend Playwright helper ``scripts/render-bulletin-png.mjs``
    against the public snapshot route ``/live-bulletin/snapshot/<sym>``.
    Optional — if Playwright isn't installed on the host the tweet
    still goes out text-only.
  * A short video/GIF clip attachment: the day's Replay scrubber
    animated across the session frames, rendered by the frontend
    ``scripts/render-replay-clip.mjs`` Playwright helper. Also optional
    on the same graceful-degradation path.

Design rules — inherited wholesale from
:mod:`src.jobs.forecast_tweet` and :mod:`src.jobs.scorecard_tweet`:

* **Never throws.** Every failure logs a WARNING and exits 0 so the
  systemd timer keeps running tomorrow.
* **Dry-run by default.** Live posting requires BOTH the ``--post``
  flag AND ``X_BOT_BEARER_TOKEN`` (v2 tweet) plus the four OAuth1
  credentials (``X_BOT_API_KEY``, ``X_BOT_API_SECRET``,
  ``X_BOT_ACCESS_TOKEN``, ``X_BOT_ACCESS_TOKEN_SECRET``) needed for
  the v1.1 media/upload endpoint. Missing any of them silently
  degrades to dry-run.
* **Skip silently on non-trading days.** Half-days count as trading
  days — the close still produces a legitimate read.
* **Skip silently when the required data is missing.** A single
  symbol's GEX row not being present doesn't kill the whole tweet;
  the level block for that symbol is elided and the post goes out
  with what did resolve. If NO symbols resolve, skip the post
  entirely rather than emit a broken carcass.
* **Dry-run writes to disk.** The rendered tweet body, PNG and clip
  are persisted under
  ``$BULLETIN_TWEET_ARTIFACT_DIR/<mode>/<date>/`` so the operator
  can inspect exactly what would have gone out before flipping
  ``--post`` on. Defaults to
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
import json
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from src.api.database import DatabaseManager
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
    sign = "+" if v >= 0 else "−"
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
    """Format a strike/level for the ``Key levels:`` block.

    Matches the operator's examples: whole-dollar walls print with no
    decimals ("735", "740"), a computed gamma flip keeps two ("747.29"),
    and index-scale values (>= 1000) get thousands separators."""
    if v is None:
        return "—"
    whole = abs(v - round(v)) < 0.005
    if abs(v) >= 1000:
        return f"{v:,.0f}" if whole else f"{v:,.2f}"
    return f"{int(round(v))}" if whole else f"{v:.2f}"


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
) -> TweetBody:
    """Assemble the "…Read — $SYM" post + threaded reply for one fire.

    Layout (operator-approved voice):

        <Morning|Midday|Post-Market> Read — $<FEATURED>

        <opening — hook + news + price action + dealer-gamma regime>

        Key levels:
        • <put wall>  → Put Wall (<note>)
        • <call wall> → Call Wall (<note>)
        • <flip>      → Gamma Flip (<note>)

        Bottom line: <takeaway>

    The prose comes from Claude (:func:`_try_llm_post`), fed the day's
    headlines + the featured symbol's price action + its gamma structure.
    Python owns every price in the ``Key levels:`` block and the reply's
    link — the model never invents a level.  No site link / hashtags ride
    in the main post; the ZeroGEX link goes out in the threaded reply.  On
    any LLM failure we fall back to a deterministic post in the same shape."""
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
        # Defensive floor — the runner skips empty days before we get here.
        featured_symbol = lead_symbol.upper()
        header = f"{read_label} — ${featured_symbol}"
        return TweetBody(
            text=header,
            fallback=header,
            lead_symbol=featured_symbol,
            symbols_present=symbols_present,
            reply_text=_compose_reply(None, site_url, reply_text),
            featured_symbol=featured_symbol,
        )

    featured_symbol = featured.symbol

    # LLM-generated post + reply if ANTHROPIC_API_KEY is set. Any failure
    # (no key, API down, malformed reply, invented prices) returns None and
    # we fall back to the deterministic post below — never fail the tweet
    # just because the LLM path had a bad day.  Only the featured symbol is
    # handed to the model — the posts are single-symbol, so this keeps the
    # prose clean and the no-invented-price guard scoped to that symbol.
    post = _try_llm_post(mode, day, [featured], featured_symbol, headlines)
    if post is not None:
        return TweetBody(
            text=_compose_new_post(post, featured, mode),
            fallback=_build_fallback_tweet(featured, read_label),
            lead_symbol=featured_symbol,
            symbols_present=symbols_present,
            reply_text=_compose_reply(post.reply, site_url, reply_text),
            featured_symbol=featured_symbol,
        )

    return TweetBody(
        text=_build_static_post(mode, featured),
        fallback=_build_fallback_tweet(featured, read_label),
        lead_symbol=featured_symbol,
        symbols_present=symbols_present,
        reply_text=_compose_reply(None, site_url, reply_text),
        featured_symbol=featured_symbol,
    )


def _hget(h: Any, name: str, default: Any = None) -> Any:
    """Read a headline field from either a dict or a NewsItem-like object."""
    if isinstance(h, dict):
        return h.get(name, default)
    return getattr(h, name, default)


def _fetch_headlines_safe() -> list:
    """Scrape CNBC headlines for the LLM — never raises, ``[]`` on any issue."""
    try:
        from src.jobs import cnbc_news  # noqa: WPS433 — optional
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: cnbc_news import failed (%s)", exc)
        return []
    try:
        return cnbc_news.fetch_headlines()
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: headline scrape failed (%s)", exc)
        return []


def _try_llm_post(
    mode: str,
    day: date,
    present: list[SymbolBulletin],
    featured_symbol: str,
    headlines: list | None = None,
):
    """Attempt LLM post+reply generation. Returns an ``LlmPost`` or None.

    Imports the LLM helper lazily so a broken import cannot take down the
    static-template path.  All ``present`` symbols are passed so the model
    can cross-reference; ``featured_symbol`` is the one the post centers on.
    ``headlines`` are the scraped CNBC items (NewsItem objects or dicts)."""
    try:
        from src.jobs import bulletin_llm  # noqa: WPS433 — optional
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: bulletin_llm import failed (%s)", exc)
        return None

    inputs = [
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
        )
        for b in present
    ]
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
    try:
        return bulletin_llm.generate_post(
            mode=mode,
            day=day,
            symbols=inputs,
            headlines=heads,
            featured_symbol=featured_symbol,
        )
    except Exception as exc:  # noqa: BLE001 — never let the LLM path throw
        logger.warning("bulletin_tweet: LLM post generation failed (%s)", exc)
        return None


def _valid_read_label(label: str | None, mode: str) -> str:
    """Accept the model's header only if it's one of the three read labels."""
    want = {v.lower(): v for v in MODE_READ_LABEL.values()}
    return want.get((label or "").strip().lower(), _mode_read_label(mode))


def _append_para(blocks: list[str], value: str) -> None:
    """Append ``value`` as its own blank-line-separated paragraph (skip empty)."""
    stripped = (value or "").strip()
    if not stripped:
        return
    if blocks and blocks[-1] != "":
        blocks.append("")
    blocks.append(stripped)


def _key_levels_block(featured: SymbolBulletin, level_notes: dict[str, str] | None) -> str:
    """The deterministic ``• <price> → <label>`` block — Python owns the prices.

    Order matches the operator's examples: Put Wall, Call Wall, Gamma Flip.
    Only levels present in the DB row render; an optional short LLM note is
    shown in parentheses after the base label."""
    notes = level_notes or {}
    specs = (
        ("put_wall", featured.put_wall, "Put Wall"),
        ("call_wall", featured.call_wall, "Call Wall"),
        ("gamma_flip", featured.gamma_flip, "Gamma Flip"),
    )
    lines: list[str] = []
    for key, value, base in specs:
        if value is None:
            continue
        note = (notes.get(key) or "").strip()
        label = f"{base} ({note})" if note else base
        lines.append(f"• {_fmt_level(value)} → {label}")
    return "\n".join(lines)


def _compose_new_post(post, featured: SymbolBulletin, mode: str) -> str:
    """Assemble the post body around an LLM-generated ``LlmPost``.

        <Read label> — $<SYM>

        {opening}

        Key levels:
        {• … block}

        Bottom line: {bottom_line}

    Empty sections are elided.  No link / hashtags — the link rides in the
    threaded reply."""
    header_label = _valid_read_label(post.header_label, mode)
    blocks: list[str] = [f"{header_label} — ${featured.symbol}"]
    _append_para(blocks, post.opening)
    levels = _key_levels_block(featured, post.level_notes)
    if levels:
        _append_para(blocks, "Key levels:\n" + levels)
    if post.bottom_line.strip():
        _append_para(blocks, f"Bottom line: {post.bottom_line.strip()}")
    return "\n".join(blocks).strip()


def _static_hook(featured: SymbolBulletin, mode: str) -> str:
    """A short, factual opener for the no-LLM fallback — no invented color."""
    parts: list[str] = []
    if featured.spot is not None:
        if featured.gamma_flip is not None:
            rel = "below" if featured.spot < featured.gamma_flip else "above"
            parts.append(
                f"{featured.symbol} is trading ~{_fmt_price_spot(featured.spot)}, "
                f"{rel} the {_fmt_level(featured.gamma_flip)} gamma flip."
            )
        else:
            parts.append(f"{featured.symbol} is trading ~{_fmt_price_spot(featured.spot)}.")
    if featured.net_gex is not None:
        regime = featured.regime or _derive_regime(
            featured.net_gex, featured.spot, featured.gamma_flip
        )
        if regime == "negative":
            parts.append(
                f"Dealers are net {_fmt_net_gex(featured.net_gex)} — short gamma, so "
                f"hedging tends to amplify moves rather than dampen them."
            )
        elif regime == "positive":
            parts.append(
                f"Dealers are net {_fmt_net_gex(featured.net_gex)} — long gamma, so "
                f"hedging tends to dampen moves."
            )
        else:
            parts.append(f"Dealers are net {_fmt_net_gex(featured.net_gex)}.")
    return " ".join(parts) if parts else f"{featured.symbol} {_mode_read_label(mode).lower()}."


def _static_bottom_line(featured: SymbolBulletin) -> str:
    if featured.gamma_flip is not None and featured.spot is not None:
        if featured.spot < featured.gamma_flip:
            return (
                f"Structure stays defensive until {featured.symbol} can reclaim the "
                f"{_fmt_level(featured.gamma_flip)} gamma flip."
            )
        return (
            f"{featured.symbol} is holding above the {_fmt_level(featured.gamma_flip)} "
            f"gamma flip — constructive as long as that holds."
        )
    return "Watch the levels above for where dealer hedging shifts."


def _build_static_post(mode: str, featured: SymbolBulletin) -> str:
    """Deterministic new-voice post for when the LLM is unavailable.

    Same shape as the LLM path (header, hook, Key levels, Bottom line) but
    built only from the DB numbers — no scraped news, no narrated path."""
    blocks: list[str] = [f"{_mode_read_label(mode)} — ${featured.symbol}"]
    _append_para(blocks, _static_hook(featured, mode))
    levels = _key_levels_block(featured, {})
    if levels:
        _append_para(blocks, "Key levels:\n" + levels)
    _append_para(blocks, f"Bottom line: {_static_bottom_line(featured)}")
    return "\n".join(blocks).strip()


def _build_fallback_tweet(featured: SymbolBulletin, label: str) -> str:
    """A ≤280-char compression of the featured symbol's read.

    Used when the bot handle isn't X-Premium enabled: we still want to
    post *something* rather than silently swallowing the fire.  Carries
    the featured symbol's spot + gamma flip + walls + Net GEX and NO
    link (the link goes out as the threaded reply).  Trimmed with an
    ellipsis if it somehow overflows (rare — one symbol fits easily)."""
    parts = [f"${featured.symbol} {label}"]
    if featured.spot is not None:
        spot_txt = f"~{_fmt_price(featured.spot)}"
        if featured.spot_is_projected:
            spot_txt += f" (impl {_future_label(featured.future_symbol)})"
        parts.append(spot_txt)
    if featured.gamma_flip is not None:
        parts.append(f"Flip {_fmt_price(featured.gamma_flip)}")
    if featured.call_wall is not None and featured.put_wall is not None:
        parts.append(f"CW {_fmt_price(featured.call_wall)} / PW {_fmt_price(featured.put_wall)}")
    if featured.net_gex is not None:
        parts.append(f"Net GEX {_fmt_net_gex(featured.net_gex)}")
    text = " · ".join(parts)
    if len(text) <= 280:
        return text
    return text[:279].rstrip(" ·,.") + "…"


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
) -> dict[str, Any]:
    """The JSON payload the review page renders (post text + reply + meta)."""
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
    """Generate a post+reply for (symbol, mode), persist it, and return the record.

    Backs the /admin review page's Regenerate button.  Reuses an already-
    connected ``db`` (the API's shared manager) — it never connects/posts and
    skips media rendering so the page responds fast; the scheduled job remains
    the one that renders the PNG/clip for the eventual X post."""
    if mode not in MODE_READ_LABEL:
        raise ValueError(f"Unknown mode: {mode!r}")
    day = day or _today_et()
    symbol = symbol.upper()
    site = site_url or os.environ.get("ZEROGEX_SITE_URL", "").strip() or DEFAULT_SITE_URL

    bulletins = await _fetch_bulletins(db, [symbol], day)
    headlines = _fetch_headlines_safe()
    tweet = build_tweet_body(
        mode=mode,
        day=day,
        bulletins=bulletins,
        site_url=site,
        lead_symbol=symbol,
        reply_text=os.environ.get("BULLETIN_TWEET_REPLY_TEXT", "").strip() or None,
        headlines=headlines,
        force_featured=symbol,
    )
    featured = next(
        (b for b in bulletins if b.symbol == tweet.featured_symbol),
        next((b for b in bulletins if b.symbol == symbol), None),
    )
    record = build_latest_record(
        mode=mode,
        day=day,
        tweet=tweet,
        featured=featured,
        headlines=headlines,
        generated_at=datetime.now(tz=ET).isoformat(),
    )
    write_latest_record(record)
    return record


# ---------------------------------------------------------------------------
# Media rendering — bulletin PNG + replay clip
# ---------------------------------------------------------------------------


@dataclass
class MediaArtifacts:
    """Paths to the rendered media, or None when a render failed."""

    png_path: Path | None = None
    clip_path: Path | None = None


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
    out_path: Path,
    timeout_seconds: int,
    label: str,
) -> Path | None:
    """Run a node <helper> subprocess and return ``out_path`` on success.

    Shared by both media helpers so their failure-logging and non-zero-
    exit handling stays consistent. Any non-zero exit or empty output
    file returns None and logs a warning — the caller degrades to a
    text-only (or PNG-only) tweet.

    Systemd runs with a stripped PATH — nvm-installed node isn't
    reachable via bare ``node``.  Operators can either symlink node
    into /usr/local/bin OR set ``BULLETIN_TWEET_NODE_BINARY`` in .env
    to the full path (e.g.
    ``/home/ubuntu/.nvm/versions/node/v22.22.2/bin/node``)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
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
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.warning("bulletin_tweet: %s helper failed (%s) — %s", label, cmd, exc)
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "bulletin_tweet: %s helper unexpected error (%s) — %s",
            label,
            cmd,
            exc,
        )
        return None

    if proc.returncode != 0 or not out_path.exists() or out_path.stat().st_size == 0:
        logger.warning(
            "bulletin_tweet: %s helper exited %d, stderr: %s",
            label,
            proc.returncode,
            proc.stderr[:500],
        )
        return None
    return out_path


def render_bulletin_png(
    symbol: str,
    day: date,
    mode: str,
    site_url: str,
    out_path: Path,
    helper_path: str | None = None,
    timeout_seconds: int = 90,
) -> Path | None:
    """Screenshot the Live Bulletin card via the frontend's Playwright helper.

    The helper (``frontend/scripts/render-bulletin-png.mjs``) visits
    ``/live-bulletin/snapshot/{symbol}``, waits for the card's ready
    signal, and captures the ``[data-bulletin-card]`` element as PNG.
    This screenshots the SAME ``<GammaReportCard>`` component the paid
    /live-bulletin page renders — no parallel implementation, no drift.

    Requires Playwright installed on the host running this cron. When
    it isn't, the helper exits with code 2 and we log + return None so
    the tweet still goes out text-only. The v1 dry-run + text-only
    posting paths were validated against the ripped-out ``next/og``
    fallback; the render is graceful-degradation-tested.

    The snapshot page is token-gated by ``BULLETIN_SNAPSHOT_TOKEN`` on
    the frontend side; we pass the same value from env here so a
    stranger can't hit the public route and scrape gamma data."""
    helper = _locate_frontend_helper(
        "render-bulletin-png.mjs",
        helper_path,
        "BULLETIN_TWEET_PNG_HELPER",
    )
    if helper is None:
        logger.info(
            "bulletin_tweet: bulletin-png helper not found — skipping PNG attachment",
        )
        return None

    token = os.environ.get("BULLETIN_SNAPSHOT_TOKEN", "").strip()
    cmd_args = [
        "--symbol",
        symbol.upper(),
        "--mode",
        mode,
        "--date",
        day.isoformat(),
        "--site-url",
        site_url,
        "--out",
        str(out_path),
    ]
    if token:
        cmd_args.extend(["--token", token])

    return _run_frontend_helper(
        helper,
        cmd_args,
        out_path,
        timeout_seconds,
        label="bulletin-png",
    )


def render_replay_clip(
    symbol: str,
    day: date,
    site_url: str,
    out_path: Path,
    helper_path: str | None = None,
    timeout_seconds: int = 180,
) -> Path | None:
    """Invoke the frontend Playwright helper to record the replay clip.

    The helper lives in the ``zerogex-web`` repo at
    ``frontend/scripts/render-replay-clip.mjs`` and is called as a
    subprocess with ``--symbol``, ``--date``, ``--site-url`` and
    ``--out`` args. Playwright is not a hard dependency of this job;
    when the helper is missing or Playwright/Chromium isn't
    installed on the host, the subprocess exits non-zero and we log
    a warning and return None so the tweet still goes out with the
    PNG only."""
    helper = _locate_frontend_helper(
        "render-replay-clip.mjs",
        helper_path,
        "BULLETIN_TWEET_REPLAY_HELPER",
    )
    if helper is None:
        logger.info(
            "bulletin_tweet: replay-clip helper not found — skipping video attachment",
        )
        return None

    cmd_args = [
        "--symbol",
        symbol.upper(),
        "--date",
        day.isoformat(),
        "--site-url",
        site_url,
        "--out",
        str(out_path),
    ]
    return _run_frontend_helper(
        helper,
        cmd_args,
        out_path,
        timeout_seconds,
        label="replay-clip",
    )


# ---------------------------------------------------------------------------
# X API client — v2 tweet (bearer) + v1.1 media upload (OAuth1) for images
# ---------------------------------------------------------------------------


def post_tweet_via_x_api(
    text: str,
    bearer_token: str,
    media_ids: list[str] | None = None,
    reply_to: str | None = None,
    timeout_seconds: int = 15,
) -> dict[str, Any]:
    """POST to https://api.x.com/2/tweets with optional media IDs.

    Mirrors :func:`src.jobs.forecast_tweet.post_tweet_via_x_api` but
    optionally attaches ``media`` when ``media_ids`` is given — the
    v2 endpoint takes uploaded v1.1 media by ID — and threads the tweet
    under ``reply_to`` (the parent tweet id) when given, which is how the
    link comment is posted as a reply to the main bulletin.

    Uses urllib so we inherit no new third-party dependency."""
    payload: dict[str, Any] = {"text": text}
    if media_ids:
        payload["media"] = {"media_ids": media_ids}
    if reply_to:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to}
    req = Request(
        "https://api.x.com/2/tweets",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
            "User-Agent": "zerogex-bulletin-tweet/1.0",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body) if body else {}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def _attach_price_action(db: DatabaseManager, bulletin: SymbolBulletin, day: date) -> None:
    """Best-effort: hang the featured symbol's price action off the bulletin.

    Pulls the previous close (position vs prior close) and the day's
    intraday session range so the LLM can narrate the path, then derives a
    plain-language regime + momentum cue.  Every query is wrapped — a miss
    just leaves that field None, and the LLM writes with what resolved."""
    sym = bulletin.symbol
    try:
        closes = await db.get_session_closes(sym)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: get_session_closes(%s) failed (%s)", sym, exc)
        closes = None
    if closes:
        bulletin.prior_close = _to_float(closes.get("prior_session_close"))

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


async def _fetch_bulletins(
    db: DatabaseManager,
    symbols: list[str],
    day: date,
) -> list[SymbolBulletin]:
    """Fetch the latest GEX summary + price action for every requested symbol.

    Each call is wrapped so a single symbol's DB miss doesn't take
    the whole tweet down — we just render a placeholder block for the
    missing symbol (or elide it entirely if it has no fields at all)."""
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
        # Price action (prior close, session range, regime, momentum) — the
        # inputs the LLM narrates the day's path from.  Best-effort.
        await _attach_price_action(db, bulletin, day)
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
) -> None:
    """Persist a JSON manifest + the raw tweet text next to the media.

    Written at three points in the fire lifecycle:

      * dry_run — no --post flag, just showing what would go out
      * pending — --stage was used, waiting for approval to post
      * posted  — the tweet was successfully sent to X

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
            }
            for b in bulletins
        ],
    }
    (artifact_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def _upload_media_files(
    media: MediaArtifacts,
) -> list[str]:
    """Upload the PNG (and video, when present) and return media_ids.

    The v1.1 media/upload endpoint requires OAuth1 signing — the
    signing helpers live in :mod:`src.jobs.x_media_client` to keep
    this module readable. Returns an empty list when either the
    credentials are missing or an upload failed, so the tweet still
    gets posted text-only."""
    from src.jobs import x_media_client  # local import — optional dep path

    try:
        creds = x_media_client.load_credentials_from_env()
    except x_media_client.MissingCredentialsError as exc:
        logger.info(
            "bulletin_tweet: media upload skipped — %s. Text-only post.",
            exc,
        )
        return []

    media_ids: list[str] = []
    if media.png_path is not None:
        try:
            mid = x_media_client.upload_media(media.png_path, creds, mime_type="image/png")
            if mid:
                media_ids.append(mid)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bulletin_tweet: PNG upload failed (%s) — dropping attachment",
                exc,
            )

    if media.clip_path is not None:
        # Category has to be video_tweet (chunked) for MP4; the helper
        # dispatches on file extension.
        try:
            mid = x_media_client.upload_media(media.clip_path, creds)
            if mid:
                media_ids.append(mid)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "bulletin_tweet: clip upload failed (%s) — dropping attachment",
                exc,
            )
    return media_ids


async def _run(args: argparse.Namespace) -> int:
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

    db = DatabaseManager()
    try:
        await db.connect()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "bulletin_tweet[%s]: DB connect failed (%s) — exiting 0",
            args.mode,
            exc,
        )
        return 0

    try:
        bulletins = await _fetch_bulletins(db, symbols, day)
    finally:
        try:
            await db.disconnect()
        except Exception:  # noqa: BLE001
            pass

    if not any(b.has_any_level() or b.spot is not None for b in bulletins):
        logger.info(
            "bulletin_tweet[%s]: every symbol's GEX summary was missing — skipping",
            args.mode,
        )
        return 0

    # Scrape the day's CNBC headlines to feed the LLM (best-effort, never fatal).
    headlines = _fetch_headlines_safe()

    tweet = build_tweet_body(
        mode=args.mode,
        day=day,
        bulletins=bulletins,
        site_url=args.site_url,
        lead_symbol=args.lead_symbol.upper(),
        reply_text=os.environ.get("BULLETIN_TWEET_REPLY_TEXT", "").strip() or None,
        headlines=headlines,
        # The scheduled auto-post is always the lead symbol (default SPY), not
        # the "cleanest setup" among a multi-symbol dropdown list.
        force_featured=args.lead_symbol.upper(),
    )

    artifact_dir = resolve_artifact_dir(args.artifact_dir, args.mode, day)

    # Media render — always attempted even in dry-run so the operator
    # can inspect the PNG/clip before flipping --post on. Failures
    # degrade gracefully to text-only.
    media = MediaArtifacts()
    if not args.no_media:
        png_out = artifact_dir / f"bulletin-{tweet.lead_symbol.lower()}.png"
        media.png_path = render_bulletin_png(
            tweet.lead_symbol,
            day,
            args.mode,
            args.site_url,
            png_out,
        )
        # Clip lands on the same lead symbol so the visual pairing
        # (card + video) is coherent.
        clip_out = artifact_dir / f"replay-{tweet.lead_symbol.lower()}.mp4"
        media.clip_path = render_replay_clip(
            tweet.lead_symbol,
            day,
            args.site_url,
            clip_out,
        )

    _write_manifest_and_text(
        artifact_dir,
        tweet,
        media,
        args.mode,
        day,
        bulletins,
        state="dry_run",
    )

    # Persist the "latest" record the /admin review page reads (best-effort,
    # never fatal).  Overwrites the previous fire for this (timing, symbol) so
    # the page always shows the last auto-generated post.
    _featured = next(
        (b for b in bulletins if b.symbol == tweet.featured_symbol),
        next((b for b in bulletins if b.symbol == tweet.lead_symbol), None),
    )
    write_latest_record(
        build_latest_record(
            mode=args.mode,
            day=day,
            tweet=tweet,
            featured=_featured,
            headlines=headlines,
            media=media,
            generated_at=datetime.now(tz=ET).isoformat(),
        )
    )

    bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()

    # Autopilot: BULLETIN_TWEET_AUTOPILOT=1 in .env silently upgrades
    # --stage to --post at runtime, so switching to full autopost is a
    # one-line env-var flip — no systemd surgery required.  Explicit
    # --post on the CLI always wins regardless.
    autopilot = os.environ.get("BULLETIN_TWEET_AUTOPILOT", "").strip() in ("1", "true", "yes")
    effective_post = bool(args.post) or (bool(args.stage) and autopilot)
    effective_stage = bool(args.stage) and not effective_post

    # A real scheduled fire (--stage) or an autopilot post emails the operator a
    # "<Timing> X-Post Ready" notice linking to the /admin/x-post review page.
    # Manual dry-run previews don't email.  Best-effort — never fatal.
    if effective_stage or effective_post:
        _send_xpost_ready_email(args.mode)

    if effective_stage:
        _write_manifest_and_text(
            artifact_dir,
            tweet,
            media,
            args.mode,
            day,
            bulletins,
            state="pending",
        )
        _log_approval_required(args.mode, artifact_dir, tweet)
        _call_notify_hook(args.mode, artifact_dir, tweet, media)
        return 0

    if not effective_post or not bearer:
        reason = "no --post flag" if not effective_post else "X_BOT_BEARER_TOKEN unset"
        logger.info(
            "bulletin_tweet[%s]: DRY RUN (%s) — artifacts at %s\n----\n%s\n----",
            args.mode,
            reason,
            artifact_dir,
            tweet.text,
        )
        return 0

    post_result = post_bulletin(
        tweet=tweet,
        media=media,
        bearer=bearer,
        long=args.long,
        mode_label=args.mode,
    )
    if post_result:
        _write_manifest_and_text(
            artifact_dir,
            tweet,
            media,
            args.mode,
            day,
            bulletins,
            state="posted",
            posted_id=post_result.get("id"),
            reply_id=post_result.get("reply_id"),
        )
    return 0


def post_bulletin(
    tweet: TweetBody,
    media: MediaArtifacts,
    bearer: str,
    long: bool,
    mode_label: str,
) -> dict[str, Any] | None:
    """Upload media, POST to X, return {"id": tweet_id} on success.

    Shared between the direct-post path (``bulletin_tweet --post``) and
    the approve-a-staged-draft path (``bulletin_approve``) so both use
    identical upload + fallback + retry logic.  Returns None on failure
    so the caller can update the manifest state accordingly.

    After the main tweet lands, posts ``tweet.reply_text`` (the
    ``Free delayed … zerogex.io`` link comment) as a threaded reply.  A
    failed reply is logged but never fails the call — the main post is
    already out, and the link is a nice-to-have, not load-bearing."""
    media_ids = _upload_media_files(media)

    text_to_post = tweet.text if long else tweet.fallback
    if len(text_to_post) > LONG_TWEET_MAX_LEN:
        logger.warning(
            "bulletin_tweet[%s]: text length %d > %d — falling back to short body",
            mode_label,
            len(text_to_post),
            LONG_TWEET_MAX_LEN,
        )
        text_to_post = tweet.fallback

    resp: dict[str, Any] | None = None
    try:
        resp = post_tweet_via_x_api(text_to_post, bearer, media_ids=media_ids or None)
    except (HTTPError, URLError) as exc:
        logger.warning("bulletin_tweet[%s]: X API call failed (%s)", mode_label, exc)
        # If a long-form post failed with 403, try again with the fallback
        # (classic 280-char body) in case the bot handle isn't Premium.
        if long and text_to_post != tweet.fallback:
            logger.info("bulletin_tweet[%s]: retrying with short fallback body", mode_label)
            try:
                resp = post_tweet_via_x_api(tweet.fallback, bearer, media_ids=media_ids or None)
            except Exception as exc2:  # noqa: BLE001
                logger.warning(
                    "bulletin_tweet[%s]: fallback retry also failed (%s)",
                    mode_label,
                    exc2,
                )
                return None
        else:
            return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet[%s]: unexpected X API error (%s)", mode_label, exc)
        return None

    if resp is None:
        return None

    tweet_id = (resp.get("data") or {}).get("id")
    logger.info(
        "bulletin_tweet[%s]: posted tweet id=%s (media=%d)",
        mode_label,
        tweet_id,
        len(media_ids),
    )

    result: dict[str, Any] = {"id": tweet_id, "response": resp}
    reply_id = _post_link_reply(tweet, tweet_id, bearer, mode_label)
    if reply_id is not None:
        result["reply_id"] = reply_id
    return result


def _post_link_reply(
    tweet: TweetBody,
    parent_id: str | None,
    bearer: str,
    mode_label: str,
) -> str | None:
    """Post the link comment as a threaded reply to ``parent_id``.

    Best-effort: any failure (or a missing parent id / empty reply text)
    logs and returns None without disturbing the already-posted main
    tweet."""
    if not parent_id or not tweet.reply_text:
        return None
    try:
        reply_resp = post_tweet_via_x_api(
            tweet.reply_text,
            bearer,
            reply_to=parent_id,
        )
    except Exception as exc:  # noqa: BLE001 — reply is non-load-bearing
        logger.warning(
            "bulletin_tweet[%s]: link reply failed (%s) — main tweet still posted",
            mode_label,
            exc,
        )
        return None
    reply_id = (reply_resp.get("data") or {}).get("id")
    logger.info(
        "bulletin_tweet[%s]: posted link reply id=%s under %s",
        mode_label,
        reply_id,
        parent_id,
    )
    return reply_id


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


def _send_xpost_ready_email(mode: str) -> bool:
    """Email a "<Timing> X-Post Ready" notice with a link to /admin/x-post.

    Built into the job (not a shell hook) so the scheduled fire notifies the
    operator with no extra wiring — reuses the same RESEND_API_KEY /
    RESEND_FROM_EMAIL / BULLETIN_TWEET_EMAIL_TO the frontend already uses.
    Disable with BULLETIN_TWEET_ADMIN_EMAIL_ENABLED=0.  Best-effort: returns
    False (and logs) on any missing config or send error — never fatal.

    This REPLACES the old "approval needed" email — remove the deprecated
    BULLETIN_TWEET_NOTIFY_HOOK from .env so the old one stops (a warning is
    logged if both are active)."""
    if os.environ.get("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return False
    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    from_email = os.environ.get("RESEND_FROM_EMAIL", "").strip()
    to_email = os.environ.get("BULLETIN_TWEET_EMAIL_TO", "").strip()
    if not (api_key and from_email and to_email):
        logger.info(
            "bulletin_tweet: X-Post-Ready email skipped — RESEND_API_KEY / "
            "RESEND_FROM_EMAIL / BULLETIN_TWEET_EMAIL_TO not all set",
        )
        return False

    timing = _TIMING_WORD.get(mode, mode)
    url = _xpost_admin_url()
    html = (
        f"<h2>{timing} X-Post Ready</h2>"
        f"<p>The {timing.lower()} X-post has been generated.</p>"
        f'<p><a href="{url}" style="display:inline-block;padding:10px 18px;'
        f"background:#111;color:#fff;border-radius:8px;text-decoration:none;"
        f'font-weight:600">Review &amp; copy the post &rarr;</a></p>'
        f'<p style="color:#666;font-size:13px">Or open: <a href="{url}">{url}</a>'
        f"<br>You'll need to be signed in to your Admin account to view it.</p>"
    )
    text = f"{timing} X-Post Ready\n\nReview & copy the post: {url}\n" f"(Admin sign-in required.)"
    body = json.dumps(
        {
            "from": from_email,
            "to": [to_email],
            "subject": f"{timing} X-Post Ready",
            "html": html,
            "text": text,
        }
    ).encode("utf-8")
    req = Request(
        "https://api.resend.com/emails",
        data=body,
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
        logger.warning("bulletin_tweet: X-Post-Ready email failed (%s)", exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: X-Post-Ready email error (%s)", exc)
        return False

    logger.info("bulletin_tweet: X-Post-Ready email sent to %s (%s)", to_email, timing)
    # Nudge if the deprecated approval hook is ALSO set — that's the old
    # "approval needed" email; they'll get both until it's removed.
    if os.environ.get("BULLETIN_TWEET_NOTIFY_HOOK", "").strip():
        logger.warning(
            "bulletin_tweet: BULLETIN_TWEET_NOTIFY_HOOK is set alongside the "
            "built-in X-Post-Ready email — remove it from .env to stop the old "
            "'approval needed' email.",
        )
    return True


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
        help=(
            "Comma-separated symbols to render (default: $BULLETIN_TWEET_SYMBOLS or "
            "SPY,SPX,QQQ)."
        ),
    )
    parser.add_argument(
        "--lead-symbol",
        default=os.environ.get("BULLETIN_TWEET_LEAD_SYMBOL", DEFAULT_LEAD_SYMBOL),
        help=(
            "Symbol whose Live Bulletin card is attached as the PNG (and whose "
            "replay drives the video). Default: $BULLETIN_TWEET_LEAD_SYMBOL or SPX."
        ),
    )
    parser.add_argument("--date", help="Target date (YYYY-MM-DD). Default: today ET.")
    parser.add_argument(
        "--post",
        action="store_true",
        help=(
            "Actually post to X. Without this flag the job dry-runs even when "
            "X_BOT_BEARER_TOKEN is set — safe by default."
        ),
    )
    parser.add_argument(
        "--stage",
        action="store_true",
        help=(
            "Stage the draft for human approval instead of posting.  Writes "
            "the full artifact set (text + PNG + clip + manifest with "
            "state=pending) and calls $BULLETIN_TWEET_NOTIFY_HOOK if set.  "
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
            "Post the full multi-paragraph body (requires X-Premium on the bot "
            "handle). Default on; the job falls back to a 280-char summary if the "
            "long-form post is rejected."
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
            "Skip PNG + video rendering. Useful for a fast text-only manual "
            "invocation while debugging tweet copy."
        ),
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help=(
            "Override the directory dry-run artifacts (text + PNG + clip) are "
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
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
