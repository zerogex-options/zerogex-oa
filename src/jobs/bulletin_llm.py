"""LLM-generated narrative for bulletin tweets.

The static template at :mod:`src.jobs.bulletin_tweet` fills in
``SPY / SPX / QQQ update:`` and a rotating one-line lead sentence, then
concatenates the numeric read.  Serviceable, but every fire reads the
same shape — great for reliability, not great for engagement.

This module optionally hands the day's structured snapshot to Claude
and asks it to write the narrative sections (opening, clean-read
interpretation, closing takeaway, optional signoff).  The caller
composes those with the deterministic numeric block, so:

  * The LLM controls voice, framing, and flow.
  * Python controls every price the tweet quotes — the model NEVER
    invents a level, and the numeric block is untouched.

Contract:
  * Enabled when ``ANTHROPIC_API_KEY`` is set.  Missing key → returns
    None → caller falls back to the static template.
  * Any API error / malformed response also returns None — never
    raises.  A dud LLM run must never take the tweet down.
  * Model default is ``claude-sonnet-5``; override with
    ``BULLETIN_TWEET_LLM_MODEL`` for A/B tests.

Uses stdlib ``urllib`` so we inherit no new third-party dependency —
same discipline as the X API clients.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.bulletin_llm")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-sonnet-5"
# 2500 gives comfortable headroom over the typical output size (~1200
# tokens for a nuanced multi-paragraph response with JSON scaffolding).
# The static template fallback kicks in if the model still overflows,
# but at 2500 that's a rare case rather than the default.
DEFAULT_MAX_TOKENS = 2500
DEFAULT_TIMEOUT_SECONDS = 45


SYSTEM_PROMPT = """\
You write short trader commentary for the ZeroGEX X (Twitter) account.

VOICE:
* Conversational trader voice — not marketing, not academic
* Short paragraphs, one idea per paragraph, blank line between
* Occasional structural labels ("The clean read:", "The important part:",
  "The setup:") to break long posts up
* Confident but not hyped. No exclamations, no "🚀", no all-caps

STRUCTURE:
Reply with a single JSON object and nothing else.  The object has:
{
  "header_label":  short suffix that closes the header line —
                   e.g. "update", "read", "recap", "midday update",
                   "post-market update".  For a mode="premarket" fire
                   prefer "pre-market update" / "pre-market read";
                   for mode="midday" prefer "midday update"; for
                   mode="close" prefer "post-market update" /
                   "post-market recap".
  "opening":       Narrative that frames what happened / what to watch.
                   2-6 short paragraphs.  Use "\\n\\n" between paragraphs.
                   You MAY include structured setup lines like
                   "SPY box: 745-750" — but only using prices from the
                   input's level fields.
  "clean_read":    Narrative interpreting the current numeric read.
                   2-4 short paragraphs.  You may lead with a label
                   like "The clean read:" or "The important part:".
                   Reference specific levels the input provided.
  "closing":       Takeaway.  1-3 short paragraphs.  May include a
                   plain-text bulleted list of scenarios (use "* " or
                   dashes; not markdown syntax that will render literally
                   on X).
  "signoff":       Optional final line (event flavor, e.g.
                   "Happy 250th, America. 🇺🇸").  Empty string when there
                   is nothing thematic to note.
}

STRICT RULES:
* Every dollar figure or strike price you write MUST appear verbatim
  in the input's ``levels`` block.  Never invent numbers.
* When quoting Net GEX, use the ``net_gex_display`` value ("+$7.74B",
  "−$125.0M") — NEVER the raw ``net_gex`` float.  Writing "SPY net GEX
  of 7,740,721,297.75" is wrong; write "SPY net GEX at +$7.74B".
* Do not include the numeric block "Current ZeroGEX read: ..." — that
  gets interpolated by the caller.
* Do not include hashtags, "zerogex.io", or the ticker cash-tag line —
  those are appended separately.
* Do not use markdown (**, __, ##).  Emojis are OK but sparingly and
  only if thematically appropriate.
* Do NOT give trading recommendations ("buy X", "sell Y", "target Z").
  Describe positioning and mechanics.
* If the input's ``context`` flags an event (holiday eve, FOMC, CPI,
  half-day), work it into the framing naturally.
* Match the mode: premarket = look-ahead framing; midday = mid-session
  positioning; close = look-back plus overnight-setup framing.
"""


@dataclass
class SymbolInput:
    """One symbol's structured snapshot for the LLM prompt."""

    symbol: str
    spot: float | None = None
    prior_close: float | None = None
    session_open: float | None = None
    session_high: float | None = None
    session_low: float | None = None
    gamma_flip: float | None = None
    call_wall: float | None = None
    put_wall: float | None = None
    max_pain: float | None = None
    net_gex: float | None = None
    regime: str | None = None  # "positive", "negative", "neutral", "unresolved"

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "spot": self.spot,
            "prior_close": self.prior_close,
            "session_open": self.session_open,
            "session_high": self.session_high,
            "session_low": self.session_low,
            "gamma_flip": self.gamma_flip,
            "call_wall": self.call_wall,
            "put_wall": self.put_wall,
            "max_pain": self.max_pain,
            # Present net_gex both as raw float (for the model to reason
            # about magnitude/sign) AND pre-formatted in the short scale
            # the model MUST use verbatim if it quotes the number.
            # Without this, the model sometimes writes "SPY's net GEX of
            # 7,740,721,297.75" instead of "+$7.74B" — factually right
            # but visually ugly.
            "net_gex": self.net_gex,
            "net_gex_display": _short_scale_gex(self.net_gex),
            "regime": self.regime,
            "change_pct": (
                None
                if self.spot is None or self.prior_close in (None, 0)
                else (self.spot - self.prior_close) / self.prior_close * 100
            ),
        }


def _short_scale_gex(v: float | None) -> str | None:
    """Mirror :func:`src.jobs.bulletin_tweet._fmt_net_gex` — the short-scale
    form ("+$7.74B", "−$125.0M") the tweet's numeric block uses.

    Duplicated here so bulletin_llm has no import dependency on
    bulletin_tweet (which imports the LLM module lazily via
    _try_llm_section).  Keeps them decoupled."""
    if v is None:
        return None
    abs_v = abs(v)
    sign = "+" if v >= 0 else "−"
    if abs_v >= 1e9:
        return f"{sign}${abs_v / 1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v / 1e6:.1f}M"
    if abs_v >= 1e3:
        return f"{sign}${abs_v / 1e3:.0f}K"
    return f"{sign}${abs_v:.0f}"


@dataclass
class DayContext:
    """Non-per-symbol context the LLM should weave into the framing."""

    mode: str
    date: date
    is_holiday_eve: bool = False
    market_closed_tomorrow: bool = False
    next_trading_day: date | None = None
    half_day: bool = False
    event_labels: list[str] = field(default_factory=list)

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "date": self.date.isoformat(),
            "day_of_week": self.date.strftime("%A"),
            "is_holiday_eve": self.is_holiday_eve,
            "market_closed_tomorrow": self.market_closed_tomorrow,
            "next_trading_day": (
                self.next_trading_day.isoformat() if self.next_trading_day else None
            ),
            "half_day": self.half_day,
            "event_labels": list(self.event_labels),
        }


@dataclass
class LlmSection:
    """Narrative fragments the caller composes with the numeric block."""

    header_label: str
    opening: str
    clean_read: str
    closing: str
    signoff: str = ""


# ---------------------------------------------------------------------------
# Day-context computation
# ---------------------------------------------------------------------------


def _is_trading_day(day: date) -> bool:
    if day.weekday() >= 5:
        return False
    if day in NYSE_HOLIDAYS:
        return False
    return True


def next_trading_day(day: date, max_lookahead: int = 10) -> date | None:
    """First trading day strictly after ``day``.

    Used to seed the LLM's "back Tuesday" / "with the market closed
    tomorrow" framing.  Caps at 10 days lookahead as a runaway guard
    (a real market gap that long doesn't exist)."""
    cursor = day + timedelta(days=1)
    for _ in range(max_lookahead):
        if _is_trading_day(cursor):
            return cursor
        cursor += timedelta(days=1)
    return None


def build_day_context(mode: str, day: date) -> DayContext:
    """Compute the non-per-symbol context flags for one fire."""
    next_td = next_trading_day(day)
    market_closed_tomorrow = (
        next_td is not None and next_td != day + timedelta(days=1)
    )
    # "Holiday eve" = today's close is followed by a market-closed
    # non-weekend day (excludes normal Friday closes).  Signal the
    # LLM to note the pause in framing.
    tomorrow = day + timedelta(days=1)
    is_holiday_eve = tomorrow.weekday() < 5 and tomorrow in NYSE_HOLIDAYS
    return DayContext(
        mode=mode,
        date=day,
        is_holiday_eve=is_holiday_eve,
        market_closed_tomorrow=market_closed_tomorrow,
        next_trading_day=next_td,
    )


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------


def _build_user_message(
    symbols: list[SymbolInput], day_context: DayContext,
) -> str:
    """Assemble the JSON payload the model sees as the user message.

    Format is JSON-in-plain-text so the model can quote figures from
    it verbatim without ambiguity, and can compare fields across
    symbols easily."""
    payload = {
        "context": day_context.to_prompt_dict(),
        "levels": [s.to_prompt_dict() for s in symbols],
        "instructions": (
            "Write the narrative sections in the JSON shape described in the "
            "system prompt.  Every price you mention must appear in ``levels``."
        ),
    }
    return json.dumps(payload, indent=2, default=str)


def _extract_json_block(text: str) -> str | None:
    """Find the JSON object block in the model output.

    Uses first-``{`` to last-``}`` rather than brace counting because
    the model's string values can contain literal braces (e.g. in a
    bulleted item like ``* SPY {750}``) that throw off a naive depth
    counter.  Since we ask for a JSON object as the entire response,
    the outermost braces bracket the whole payload."""
    # Strip common markdown code-fence wrappers first — some models
    # add ``` ```json ... ``` `` even when told not to.
    stripped = text.strip()
    for fence in ("```json", "```JSON", "```"):
        if stripped.startswith(fence):
            stripped = stripped[len(fence):].lstrip()
            break
    if stripped.endswith("```"):
        stripped = stripped[:-3].rstrip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    return stripped[start:end + 1]


def _call_claude(
    system: str,
    user_message: str,
    api_key: str,
    model: str,
    max_tokens: int,
    timeout_seconds: int,
) -> dict[str, Any] | None:
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_message}],
    }
    req = Request(
        ANTHROPIC_API_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
            "user-agent": "zerogex-bulletin-llm/1.0",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError) as exc:
        logger.warning("bulletin_llm: Claude API call failed (%s)", exc)
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_llm: unexpected Claude API error (%s)", exc)
        return None

    try:
        return json.loads(raw) if raw else None
    except json.JSONDecodeError as exc:
        logger.warning("bulletin_llm: Claude response was not JSON (%s)", exc)
        return None


def _extract_text_from_response(payload: dict[str, Any]) -> str | None:
    """Pull the assistant's text out of the messages API response.

    Shape: ``{"content": [{"type": "text", "text": "..."}, ...]}``.
    We concatenate every text block just in case the model split the
    reply across parts."""
    content = payload.get("content")
    if not isinstance(content, list):
        return None
    chunks: list[str] = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str):
                chunks.append(text)
    return "\n".join(chunks) if chunks else None


def _parse_section(body_json: str) -> LlmSection | None:
    """Parse the model's JSON body into an ``LlmSection``.

    Guards against missing fields, wrong types, and pathologically long
    strings (a runaway model could otherwise blow past X's tweet
    ceiling).  Returns None on any structural failure so the caller
    falls back to the template."""
    try:
        # strict=False accepts unescaped control chars (literal \n, \r,
        # \t) inside string values.  Claude sometimes emits multi-para
        # ``opening`` fields with literal newlines rather than ``\\n``
        # escape sequences; that renders fine but breaks strict JSON.
        # Tolerating it here saves an otherwise-good response.
        obj = json.loads(body_json, strict=False)
    except json.JSONDecodeError as exc:
        logger.warning("bulletin_llm: model JSON did not parse (%s)", exc)
        return None
    if not isinstance(obj, dict):
        logger.warning("bulletin_llm: model returned non-object payload")
        return None

    def _str_field(name: str, required: bool = True) -> str | None:
        value = obj.get(name)
        if value is None:
            return None if required else ""
        if not isinstance(value, str):
            logger.warning(
                "bulletin_llm: model field %r was %s, expected string",
                name, type(value).__name__,
            )
            return None
        # Cap at 5000 chars per section — well under X's long-form
        # ceiling.  A runaway response gets truncated rather than
        # crashing the composer.
        return value.strip()[:5000]

    header_label = _str_field("header_label") or ""
    opening = _str_field("opening") or ""
    clean_read = _str_field("clean_read") or ""
    closing = _str_field("closing") or ""
    signoff = _str_field("signoff", required=False) or ""

    # The three narrative fields must all be present and non-empty for
    # the section to be usable — an empty clean_read would leave the
    # numeric block dangling with no interpretation.
    if not header_label or not opening or not clean_read:
        logger.warning(
            "bulletin_llm: model output missing required narrative sections "
            "(header=%r, opening_len=%d, clean_read_len=%d)",
            header_label, len(opening), len(clean_read),
        )
        return None

    return LlmSection(
        header_label=header_label,
        opening=opening,
        clean_read=clean_read,
        closing=closing,
        signoff=signoff,
    )


def _validate_no_invented_prices(
    section: LlmSection, symbols: list[SymbolInput],
) -> bool:
    """Best-effort check that the model didn't quote a price we didn't provide.

    Scans every 4+ digit number in the narrative and confirms it appears
    (as an integer or a rounded form) in the input's ``levels`` block.
    Not a strict guarantee — a determined model could still smuggle in
    a wrong value — but catches the obvious "3-decimal fabricated
    strike" case and lets us fall back to the template on any hit."""
    import re

    known_values: set[int] = set()
    for s in symbols:
        for v in (
            s.spot, s.prior_close, s.session_open, s.session_high, s.session_low,
            s.gamma_flip, s.call_wall, s.put_wall, s.max_pain,
        ):
            if v is None:
                continue
            # Integer form + rounded to nearest 5 covers ETF strike
            # spacing without a false-positive storm on decimal spot
            # prices.
            for candidate in (int(round(v)), int(round(v / 5) * 5)):
                if candidate > 0:
                    known_values.add(candidate)
        # Net GEX in millions/billions gets separately mentioned as
        # strings — skip numeric matching for it.

    combined = "\n".join([section.opening, section.clean_read, section.closing])
    # Collapse comma-thousands so "7,483" tokenizes as a single 4-digit
    # number, not as "7" + "483".  We keep decimals intact so a spot
    # like "744.51" is captured whole.
    normalized = re.sub(r"(?<=\d),(?=\d{3}\b)", "", combined)
    # Match 3-6 digit integer parts (skip 1-2 digits — those are
    # everywhere: "2 short paragraphs", "6.8G disk", etc.).  Decimal
    # tail is optional.
    hits = re.findall(r"(?<!\d)(\d{3,6})(?:\.\d+)?(?!\d)", normalized)
    invented: list[str] = []
    for h in hits:
        n = int(h)
        # Tolerate ±2 to handle the model quoting "7,483" for spot 7482.71.
        if not any(abs(n - k) <= 2 for k in known_values):
            invented.append(h)

    if invented:
        logger.warning(
            "bulletin_llm: model mentioned prices not in input levels: %s — "
            "falling back to template",
            sorted(set(invented))[:8],
        )
        return False
    return True


def generate_narrative(
    mode: str,
    day: date,
    symbols: list[SymbolInput],
    day_context: DayContext | None = None,
    api_key: str | None = None,
    model: str | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> LlmSection | None:
    """Call Claude and return an ``LlmSection`` — or None on any failure.

    The caller invokes this AFTER assembling the deterministic bulletin
    data.  A None return is expected and normal (no API key configured,
    API outage, malformed response) and instructs the caller to fall
    back to the static template."""
    if not symbols:
        return None
    key = (api_key or os.environ.get("ANTHROPIC_API_KEY", "")).strip()
    if not key:
        logger.debug("bulletin_llm: ANTHROPIC_API_KEY unset — skipping LLM")
        return None

    ctx = day_context or build_day_context(mode, day)
    model_id = (
        model or os.environ.get("BULLETIN_TWEET_LLM_MODEL", "").strip() or DEFAULT_MODEL
    )
    env_max_tokens = os.environ.get("BULLETIN_TWEET_LLM_MAX_TOKENS", "").strip()
    if env_max_tokens.isdigit():
        max_tokens = int(env_max_tokens)

    user_msg = _build_user_message(symbols, ctx)
    resp = _call_claude(
        SYSTEM_PROMPT, user_msg, key, model_id, max_tokens, timeout_seconds,
    )
    if resp is None:
        return None

    text = _extract_text_from_response(resp)
    if not text:
        logger.warning("bulletin_llm: Claude response had no text content")
        return None

    stop_reason = resp.get("stop_reason")
    json_block = _extract_json_block(text)
    if not json_block:
        # Most common cause: max_tokens capped the response mid-JSON.
        # Surface that explicitly so the operator knows to bump the
        # ceiling rather than think it's a prompt problem.
        if stop_reason == "max_tokens":
            logger.warning(
                "bulletin_llm: model output truncated at max_tokens=%d — "
                "increase BULLETIN_TWEET_LLM_MAX_TOKENS if this keeps happening. "
                "Falling back to static template.",
                max_tokens,
            )
        else:
            logger.warning(
                "bulletin_llm: could not find a JSON object in model output "
                "(stop_reason=%s) — first 200 chars: %r",
                stop_reason, text[:200],
            )
        return None

    section = _parse_section(json_block)
    if section is None:
        return None

    if not _validate_no_invented_prices(section, symbols):
        return None

    return section
