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

Every post includes:

  * A multi-paragraph read-out of $SPY / $SPX / $QQQ levels sourced
    live from the same ``get_latest_gex_summary`` powering the Live
    Bulletin admin page — so the tweet and the on-site card can never
    contradict each other.
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
from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.bulletin_tweet")
ET = ZoneInfo("America/New_York")

DEFAULT_SITE_URL = "https://zerogex.io"
DEFAULT_SYMBOLS = ("SPY", "SPX", "QQQ")
DEFAULT_LEAD_SYMBOL = "SPX"
LONG_TWEET_MAX_LEN = 25_000  # X Premium long-form ceiling; classic 280 is the
                             # floor the fallback body targets when the caller
                             # doesn't have Premium enabled on the bot handle.

# Modes ---------------------------------------------------------------------
MODES = ("premarket", "midday", "close")


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
        net_gex=_to_float(row.get("net_gex")),
    )


# ---------------------------------------------------------------------------
# Tweet body builder — the multi-paragraph format the operator specified.
# ---------------------------------------------------------------------------


@dataclass
class TweetBody:
    """The full text of a bulletin tweet plus a shortened fallback.

    ``text`` is the long-form (Premium) version — includes the intro,
    a level block per symbol, and the site tagline. ``fallback`` is a
    280-char single-tweet compression the caller can post instead when
    the bot handle isn't Premium-enabled (or when the API rejects the
    long text with a 403). Both share the same permalink so
    click-through analytics survive the truncation path."""

    text: str
    fallback: str
    lead_symbol: str
    symbols_present: list[str] = field(default_factory=list)


def _symbol_block(b: SymbolBulletin) -> str | None:
    """One symbol's level block. Returns None when nothing resolved so
    the caller can silently drop it from the tweet."""
    if not b.has_any_level() and b.spot is None:
        return None
    lines = [f"{b.symbol} spot: ~{_fmt_price_spot(b.spot)}"]
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
    return "\n".join(lines)


def build_tweet_body(
    mode: str,
    day: date,
    bulletins: list[SymbolBulletin],
    site_url: str = DEFAULT_SITE_URL,
    lead_symbol: str = DEFAULT_LEAD_SYMBOL,
) -> TweetBody:
    """Assemble the full tweet body for one mode.

    Layout mirrors the operator's spec:

        $SPY / $SPX / $QQQ <label>:

        <mode lead sentence>

        Current ZeroGEX read:

        <per-symbol level block>
        <blank line>
        <per-symbol level block>
        ...

        <site tagline>

        $SPY $SPX $QQQ <hashtags>

    Symbols with no resolved GEX data are dropped from the level
    section; if none resolve at all the caller should skip the post."""
    copy = MODE_COPY.get(mode)
    if copy is None:
        raise ValueError(f"Unknown mode: {mode!r}")

    present = [b for b in bulletins if b.has_any_level() or b.spot is not None]
    symbols_present = [b.symbol for b in present]
    header_symbols = " / ".join(f"${b.symbol}" for b in bulletins)

    numeric_block = _numeric_read_block(present)

    # LLM-generated narrative if ANTHROPIC_API_KEY is set. Any failure
    # (no key, API down, malformed reply, invented prices) returns None
    # and we fall back to the static template below — never fail the
    # tweet just because the LLM path had a bad day.
    section = _try_llm_section(mode, day, present)
    if section is not None:
        text = _compose_with_llm_section(
            section, header_symbols, numeric_block, site_url,
        )
        fallback = _build_fallback_tweet(
            mode, present, header_symbols, site_url, section.header_label or copy.label,
        )
        return TweetBody(
            text=text,
            fallback=fallback,
            lead_symbol=lead_symbol,
            symbols_present=symbols_present,
        )

    # Static template fallback — rotates the lead sentence off a hash
    # of (date, mode) so it varies day-to-day but stays deterministic
    # within a given fire, so a dry-run and the live post match.
    day_seed = int(day.strftime("%Y%m%d"))
    seed = _hash_seed(day_seed, hash(mode) & 0xFFFFFFFF)
    lead = _pick(copy.lead_variants, seed)

    blocks: list[str] = [
        f"{header_symbols} {copy.label}:",
        "",
        lead,
        "",
        "Current ZeroGEX read:",
        "",
        numeric_block,
        "",
        site_url.rstrip("/").replace("https://", "").replace("http://", ""),
        "",
        "$SPY $SPX $QQQ #Gamma #GEX #OptionsTrading #0DTE",
    ]
    text = "\n".join(blocks).strip()

    fallback = _build_fallback_tweet(mode, present, header_symbols, site_url, copy.label)
    return TweetBody(
        text=text,
        fallback=fallback,
        lead_symbol=lead_symbol,
        symbols_present=symbols_present,
    )


def _numeric_read_block(present: list[SymbolBulletin]) -> str:
    """The deterministic ``Current ZeroGEX read:`` numeric block.

    Extracted so both the LLM composition path and the static template
    can share the same source of truth for the numbers — no chance the
    two paths drift on formatting."""
    parts: list[str] = []
    for i, b in enumerate(present):
        block = _symbol_block(b)
        if block:
            if i > 0:
                parts.append("")
            parts.append(block)
    return "\n".join(parts)


def _try_llm_section(
    mode: str, day: date, present: list[SymbolBulletin],
):
    """Attempt LLM narrative generation. Returns None on any failure.

    Imports the LLM helper lazily so a broken import (missing env
    variable dependency, network outage during module import) cannot
    take down the static-template path."""
    try:
        from src.jobs import bulletin_llm  # noqa: WPS433 — optional
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: bulletin_llm import failed (%s)", exc)
        return None

    inputs = [
        bulletin_llm.SymbolInput(
            symbol=b.symbol,
            spot=b.spot,
            gamma_flip=b.gamma_flip,
            call_wall=b.call_wall,
            put_wall=b.put_wall,
            max_pain=b.max_pain,
            net_gex=b.net_gex,
        )
        for b in present
    ]
    try:
        return bulletin_llm.generate_narrative(mode=mode, day=day, symbols=inputs)
    except Exception as exc:  # noqa: BLE001 — never let the LLM path throw
        logger.warning("bulletin_tweet: LLM narrative generation failed (%s)", exc)
        return None


def _compose_with_llm_section(
    section,
    header_symbols: str,
    numeric_block: str,
    site_url: str,
) -> str:
    """Assemble the tweet body around an LLM-generated ``LlmSection``.

    Layout matches the operator's spec:

        {header_symbols} {header_label}:
        <blank line>
        {opening}
        <blank line>
        Current ZeroGEX read:
        <blank line>
        {numeric_block}
        <blank line>
        {clean_read}
        <blank line>
        {closing}
        <blank line>
        {signoff (optional)}
        <blank line>
        zerogex.io
        <blank line>
        $SPY $SPX $QQQ #Gamma #GEX #OptionsTrading #0DTE

    Any section coming back empty is elided so the composed body
    never carries a lonely trailing blank paragraph."""
    def _sep(existing: list[str], value: str) -> None:
        stripped = value.strip()
        if not stripped:
            return
        if existing and existing[-1] != "":
            existing.append("")
        existing.append(stripped)

    header_label = section.header_label.strip() or "update"
    blocks: list[str] = [f"{header_symbols} {header_label}:"]
    _sep(blocks, section.opening)
    _sep(blocks, "Current ZeroGEX read:")
    _sep(blocks, numeric_block)
    _sep(blocks, section.clean_read)
    _sep(blocks, section.closing)
    _sep(blocks, section.signoff)
    _sep(blocks, site_url.rstrip("/").replace("https://", "").replace("http://", ""))
    _sep(blocks, "$SPY $SPX $QQQ #Gamma #GEX #OptionsTrading #0DTE")
    return "\n".join(blocks).strip()


def _build_fallback_tweet(
    mode: str,
    present: list[SymbolBulletin],
    header_symbols: str,
    site_url: str,
    label: str,
) -> str:
    """A ≤280-char compression of the long-form body.

    Used when the bot handle isn't X-Premium enabled: we still want to
    post *something* rather than silently swallowing the fire. Includes
    the lead symbol's spot + gamma flip + walls and a permalink to the
    on-site bulletin. Trimmed with an ellipsis if it still overflows
    (extremely rare — the lead symbol's block fits comfortably)."""
    if not present:
        return f"{header_symbols} {label} — data pending. {site_url.rstrip('/')}/live-bulletin"
    lead = present[0]
    parts = [f"{header_symbols} {label}"]
    if lead.spot is not None:
        parts.append(f"{lead.symbol} ~{_fmt_price(lead.spot)}")
    if lead.gamma_flip is not None:
        parts.append(f"Flip {_fmt_price(lead.gamma_flip)}")
    if lead.call_wall is not None and lead.put_wall is not None:
        parts.append(
            f"CW {_fmt_price(lead.call_wall)} / PW {_fmt_price(lead.put_wall)}"
        )
    if lead.net_gex is not None:
        parts.append(f"Net GEX {_fmt_net_gex(lead.net_gex)}")
    body = " · ".join(parts)
    permalink = f"{site_url.rstrip('/')}/live-bulletin"
    text = f"{body}\n{permalink}"
    if len(text) <= 280:
        return text
    overflow = len(text) - 280
    trimmed = body[: max(0, len(body) - overflow - 2)].rstrip(" ·,.") + "…"
    return f"{trimmed}\n{permalink}"


# ---------------------------------------------------------------------------
# Artifact directory — persist dry-run bodies + media for inspection
# ---------------------------------------------------------------------------


def resolve_artifact_dir(explicit: str | None, mode: str, day: date) -> Path:
    """Pick the directory dry-run + media renderings are dropped into.

    Preference order:
      1. explicit ``--artifact-dir`` CLI value
      2. ``$BULLETIN_TWEET_ARTIFACT_DIR`` env
      3. ``/var/lib/zerogex-oa/bulletin-tweets`` (production default)
      4. ``$XDG_STATE_HOME/zerogex-oa/bulletin-tweets``
      5. ``$HOME/.local/state/zerogex-oa/bulletin-tweets`` (dev)

    First writable path wins. The chosen root gets ``/<mode>/<date>/``
    appended so a day's three fires each land in their own folder and
    successive dry-runs don't smear over each other."""
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    env = os.environ.get("BULLETIN_TWEET_ARTIFACT_DIR", "").strip()
    if env:
        candidates.append(Path(env))
    candidates.append(Path("/var/lib/zerogex-oa/bulletin-tweets"))
    xdg = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg:
        candidates.append(Path(xdg) / "zerogex-oa" / "bulletin-tweets")
    home = os.environ.get("HOME", "").strip()
    if home:
        candidates.append(Path(home) / ".local" / "state" / "zerogex-oa" / "bulletin-tweets")

    for root in candidates:
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
            "bulletin_tweet: %s helper unexpected error (%s) — %s", label, cmd, exc,
        )
        return None

    if proc.returncode != 0 or not out_path.exists() or out_path.stat().st_size == 0:
        logger.warning(
            "bulletin_tweet: %s helper exited %d, stderr: %s",
            label, proc.returncode, proc.stderr[:500],
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
        "--symbol", symbol.upper(),
        "--mode", mode,
        "--date", day.isoformat(),
        "--site-url", site_url,
        "--out", str(out_path),
    ]
    if token:
        cmd_args.extend(["--token", token])

    return _run_frontend_helper(
        helper, cmd_args, out_path, timeout_seconds, label="bulletin-png",
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
        "--symbol", symbol.upper(),
        "--date", day.isoformat(),
        "--site-url", site_url,
        "--out", str(out_path),
    ]
    return _run_frontend_helper(
        helper, cmd_args, out_path, timeout_seconds, label="replay-clip",
    )


# ---------------------------------------------------------------------------
# X API client — v2 tweet (bearer) + v1.1 media upload (OAuth1) for images
# ---------------------------------------------------------------------------


def post_tweet_via_x_api(
    text: str,
    bearer_token: str,
    media_ids: list[str] | None = None,
    timeout_seconds: int = 15,
) -> dict[str, Any]:
    """POST to https://api.x.com/2/tweets with optional media IDs.

    Mirrors :func:`src.jobs.forecast_tweet.post_tweet_via_x_api` but
    optionally attaches ``media`` when ``media_ids`` is given — the
    v2 endpoint takes uploaded v1.1 media by ID.

    Uses urllib so we inherit no new third-party dependency."""
    payload: dict[str, Any] = {"text": text}
    if media_ids:
        payload["media"] = {"media_ids": media_ids}
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


async def _fetch_bulletins(
    db: DatabaseManager, symbols: list[str],
) -> list[SymbolBulletin]:
    """Fetch the latest GEX summary for every requested symbol.

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
                sym, exc,
            )
            row = None
        out.append(_shape_bulletin(row, sym))
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
) -> None:
    """Persist a JSON manifest + the raw tweet text next to the media.

    Written at three points in the fire lifecycle:

      * dry_run — no --post flag, just showing what would go out
      * pending — --stage was used, waiting for approval to post
      * posted  — the tweet was successfully sent to X

    Operators need to be able to open one directory and see everything
    that would have gone out — text, PNG, clip and a small JSON with
    the level fields sourced from the DB (so a wrong number in the
    tweet can be traced back to the underlying summary row).  The
    ``state`` field lets the approve command distinguish drafts that
    are eligible to POST from ones already sent."""
    body_path = artifact_dir / "tweet_text.md"
    body_path.write_text(tweet.text + "\n", encoding="utf-8")
    fallback_path = artifact_dir / "tweet_text_fallback.md"
    fallback_path.write_text(tweet.fallback + "\n", encoding="utf-8")

    manifest = {
        "mode": mode,
        "date": day.isoformat(),
        "state": state,
        "posted_id": posted_id,
        "lead_symbol": tweet.lead_symbol,
        "symbols_present": tweet.symbols_present,
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
        json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8",
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
            "bulletin_tweet: media upload skipped — %s. Text-only post.", exc,
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
                "bulletin_tweet: PNG upload failed (%s) — dropping attachment", exc,
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
                "bulletin_tweet: clip upload failed (%s) — dropping attachment", exc,
            )
    return media_ids


async def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    if not _is_trading_day(day) and not args.allow_non_trading_day:
        logger.info(
            "bulletin_tweet[%s]: skipping %s — not a trading day (weekend or NYSE holiday).",
            args.mode, day.isoformat(),
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
            "bulletin_tweet[%s]: DB connect failed (%s) — exiting 0", args.mode, exc,
        )
        return 0

    try:
        bulletins = await _fetch_bulletins(db, symbols)
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

    tweet = build_tweet_body(
        mode=args.mode,
        day=day,
        bulletins=bulletins,
        site_url=args.site_url,
        lead_symbol=args.lead_symbol.upper(),
    )

    artifact_dir = resolve_artifact_dir(args.artifact_dir, args.mode, day)

    # Media render — always attempted even in dry-run so the operator
    # can inspect the PNG/clip before flipping --post on. Failures
    # degrade gracefully to text-only.
    media = MediaArtifacts()
    if not args.no_media:
        png_out = artifact_dir / f"bulletin-{tweet.lead_symbol.lower()}.png"
        media.png_path = render_bulletin_png(
            tweet.lead_symbol, day, args.mode, args.site_url, png_out,
        )
        # Clip lands on the same lead symbol so the visual pairing
        # (card + video) is coherent.
        clip_out = artifact_dir / f"replay-{tweet.lead_symbol.lower()}.mp4"
        media.clip_path = render_replay_clip(
            tweet.lead_symbol, day, args.site_url, clip_out,
        )

    _write_manifest_and_text(
        artifact_dir, tweet, media, args.mode, day, bulletins, state="dry_run",
    )

    bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()

    # Autopilot: BULLETIN_TWEET_AUTOPILOT=1 in .env silently upgrades
    # --stage to --post at runtime, so switching to full autopost is a
    # one-line env-var flip — no systemd surgery required.  Explicit
    # --post on the CLI always wins regardless.
    autopilot = os.environ.get("BULLETIN_TWEET_AUTOPILOT", "").strip() in ("1", "true", "yes")
    effective_post = bool(args.post) or (bool(args.stage) and autopilot)
    effective_stage = bool(args.stage) and not effective_post

    if effective_stage:
        _write_manifest_and_text(
            artifact_dir, tweet, media, args.mode, day, bulletins, state="pending",
        )
        _log_approval_required(args.mode, artifact_dir, tweet)
        _call_notify_hook(args.mode, artifact_dir, tweet, media)
        return 0

    if not effective_post or not bearer:
        reason = "no --post flag" if not effective_post else "X_BOT_BEARER_TOKEN unset"
        logger.info(
            "bulletin_tweet[%s]: DRY RUN (%s) — artifacts at %s\n----\n%s\n----",
            args.mode, reason, artifact_dir, tweet.text,
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
            artifact_dir, tweet, media, args.mode, day, bulletins,
            state="posted", posted_id=post_result.get("id"),
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
    so the caller can update the manifest state accordingly."""
    media_ids = _upload_media_files(media)

    text_to_post = tweet.text if long else tweet.fallback
    if len(text_to_post) > LONG_TWEET_MAX_LEN:
        logger.warning(
            "bulletin_tweet[%s]: text length %d > %d — falling back to short body",
            mode_label, len(text_to_post), LONG_TWEET_MAX_LEN,
        )
        text_to_post = tweet.fallback

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
                    "bulletin_tweet[%s]: fallback retry also failed (%s)", mode_label, exc2,
                )
        else:
            return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet[%s]: unexpected X API error (%s)", mode_label, exc)
        return None

    tweet_id = (resp.get("data") or {}).get("id")
    logger.info(
        "bulletin_tweet[%s]: posted tweet id=%s (media=%d)",
        mode_label, tweet_id, len(media_ids),
    )
    return {"id": tweet_id, "response": resp}


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
        mode, artifact_dir, len(tweet.text), len(tweet.fallback), mode, mode, tweet.text,
    )


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
            proc.returncode, proc.stdout[:500], proc.stderr[:500],
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
