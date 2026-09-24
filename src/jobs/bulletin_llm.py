"""LLM-written narrative for bulletin tweets, plus the review that gates them.

The bulletin auto-tweet fires three times a trading day (pre-market,
midday, close).  Each fire hands the day's structured snapshot (the
Live Bulletin card's levels, the featured symbol's price action vs the
previous close, and the latest market headlines from CNBC) to Claude and
asks it to write a natural market read in the founder's voice, plus a
threaded reply that plays off it and links back to ZeroGEX.

Division of labor:

  * The LLM controls voice, framing and flow (the prose that ties news,
    price action and the dealer-gamma regime together, the bottom-line
    takeaway, and the reply).
  * Python controls the header line and every price in the key-levels
    list, and checks every number the prose ties to a level: it must be
    that level's value (its standing value or a print from its own session
    path), and any other in-band number must be a price from the input.  A
    draft that fails is handed back to the model once with the specifics.
  * A second, independent Claude call (:func:`review_post`) fact-checks the
    finished post against the headlines, the numbers and the attached card
    image before anything is sent.  The caller holds the post when it
    finds a problem the writer couldn't fix.

Contract:
  * Enabled when ``ANTHROPIC_API_KEY`` is set.  Without it nothing can be
    written or reviewed, and the caller holds the post.
  * API errors and malformed responses never raise; they come back as None
    (writer) or a failed :class:`Review`, with the reason appended to the
    caller's ``errors`` list so the operator's notice can say what happened.
  * Model default is ``claude-opus-5`` for both calls; override with
    ``BULLETIN_TWEET_LLM_MODEL`` / ``BULLETIN_TWEET_REVIEW_MODEL``.

Uses stdlib ``urllib`` so we inherit no new third-party dependency (same
discipline as the X API clients).
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.bulletin_llm")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-opus-5"
DEFAULT_REVIEW_MODEL = "claude-opus-5"
# The model thinks before it answers, and those tokens count against the cap,
# so this is sized for thinking plus the JSON.  A cut-off reply holds the post.
DEFAULT_MAX_TOKENS = 16000
DEFAULT_TIMEOUT_SECONDS = 180
# Claude Opus 5's safety classifiers can decline a request.  With this beta the
# API re-runs a declined request on Anthropic's recommended fallback model
# inside the same call, instead of handing back a refusal.  Only sent for
# DEFAULT_MODEL: another model named in the env may not accept the parameter.
FALLBACK_BETA = "server-side-fallback-2026-07-01"
# How many times a draft that misstates a level is handed back to the model
# with the specifics before we give up.  The usual failure is one sentence in
# an otherwise good post; "the call wall was 765, never 780" lets the model
# fix that sentence instead of the whole post being thrown away.
MAX_CORRECTION_ROUNDS = 1
# A busy or unreachable API gets one more try after a short pause before the
# post is held for it.
API_ATTEMPTS = 2
API_RETRY_PAUSE_SECONDS = 10
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504, 529})

# The header label for each mode.  Python writes the header line itself.
MODE_HEADER_LABEL = {
    "premarket": "Morning Read",
    "midday": "Midday Read",
    "close": "Post-Market Read",
}


SYSTEM_PROMPT = """\
You write the market posts the ZeroGEX X account publishes three times each
trading day.  The founder posts them as their own, so every one has to read
like the founder typed it: a sharp trader explaining what the tape is doing,
in plain American English.  Never like a bot, a newsletter or a data readout.

Each post is about ONE symbol, the one named in "featured_symbol".

WHAT YOU ARE GIVEN
1. "headlines": the latest market headlines from CNBC, newest first, with the
   time each was published.
2. "levels": the featured symbol's numbers.  spot, put_wall, call_wall,
   gamma_flip, max_pain and net_gex are exactly what the Live Bulletin image
   attached to the post shows, and "regime" is the regime that image shows
   ("positive": spot above the gamma flip, dealers long gamma; "negative":
   below it, dealers short gamma; "neutral": sitting right on the flip).
   Price action: prior_close, change_vs_prior_close_pct, session_open,
   session_high, session_low and momentum.  On the midday and close posts,
   "level_history" says how the walls and the flip moved during the session
   and what price did at each one.
3. "context": the mode (premarket, midday or close), the date and calendar
   flags (a holiday tomorrow, a half day).

WHAT TO WRITE
* "opening": two to four short paragraphs.  Tie the news to what price has
  done and to where it sits against the dealer gamma levels.  Name the one or
  two levels that matter, not all of them.  Explain the regime in plain
  words: above the gamma flip dealers are long gamma and their hedging tends
  to damp moves; below it they are short gamma and their hedging tends to add
  to them.
* "bottom_line": one to three sentences with the takeaway.  An opinion is
  fine; a trade call is not.  Don't start it with "Bottom line", that label
  is added for you.
* "reply": ONE sentence posted as a threaded reply.  Add one more specific
  point (a nuance, an implication, or the tell to watch next) about today's
  setup.  Don't restate the bottom line, don't end with a colon or a call to
  action, and don't include a link or hashtags; the zerogex.io link is added
  after it for you.
The caller adds the header line and the list of key levels.  Don't write
either, and don't list the levels in your prose.

MATCH THE MODE
* premarket (the Morning Read, 9:15 AM ET): look ahead to the open.  Where
  the symbol sits against the levels going in, and what the overnight and
  morning news sets up.
* midday (the Midday Read, 12:30 PM ET): what has happened so far this
  session, what held and what didn't.
* close (the Post-Market Read, 4:05 PM ET): look back at the session, then
  ahead.  On this post the top-level put_wall, call_wall and gamma_flip are
  the map for the NEXT session: after the 4:00 PM bell the day's 0DTE
  options expired, the chain re-priced, and the attached image shows the new
  map.  What the levels were during the session is in "level_history".  Keep
  the two apart, and never say today's tape reacted to a level that is only
  in the new map.

FACTS: ONLY WHAT YOU ARE GIVEN
* Every piece of news comes from the headlines (a title or a summary).  Add
  no details, figures, names, quotes or causes that aren't in them, and
  nothing from your own memory of events.
* Don't say a headline caused a move, or that a move came before or after a
  headline, unless the inputs show the timing.  Say what the news is and what
  price did.
* Price comes from the numbers given: up or down on the day from
  change_vs_prior_close_pct, the range from session_open, session_high and
  session_low, and what happened at a level only from level_history's
  outcomes ("broke", "held", "untested").  Without level_history, don't say a
  level held, broke or was tested.
* Every price or level you write must appear in "levels": a level's value,
  or a value from its own path in level_history.  Write a level the way the
  input gives it; a 778 call wall is never "the 780 call wall".  If you quote
  net gamma, use "net_gex_display".
* Use the most relevant of the headlines.  If none of them is about markets,
  the economy, rates or companies, leave the news out rather than stretch.

HOW IT SHOULD SOUND
* American English spelling and usage (color, favor, center, analyze).
* Short, plain sentences.  Contractions are fine.  Specific beats clever.
* Punctuation a person types: periods, commas, colons, parentheses and plain
  hyphens.  No em dashes or en dashes (use a comma or a period), no arrows,
  no ellipsis character.  Negative numbers take a plain hyphen.
* No emojis, hashtags, all-caps words, exclamation points, markdown or bullet
  lists.
* No stock phrases ("here's the thing", "let's dive in", "buckle up", "it's
  worth noting", "all eyes on", "the stage is set", "at the end of the day",
  "remains to be seen"), no formula openers such as a two-line "X changed. Y
  didn't." hook, and no rhetorical questions.
* No hype, no marketing and no trade recommendations (no buy, sell, target,
  long or short calls).  Describe positioning and mechanics.

The voice, as an example (never reuse its wording):
  SPY got hit early and went straight into the put wall, and buyers showed up
  right there.  The bounce ran out of steam under the call wall, so it's been
  a chop between the two ever since.

  We're still below the gamma flip, which keeps dealers short gamma.  That's
  why the swings have been quick in both directions.

Reply with the JSON object only.
"""

# Structured output: the API guarantees a reply that parses to this shape.
POST_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "opening": {"type": "string"},
        "bottom_line": {"type": "string"},
        "reply": {"type": "string"},
    },
    "required": ["opening", "bottom_line", "reply"],
    "additionalProperties": False,
}


@dataclass
class Headline:
    """One market headline for the LLM prompt."""

    title: str
    summary: str = ""
    source: str = "CNBC"
    published: str | None = None

    def to_prompt_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"title": self.title, "source": self.source}
        if self.summary:
            d["summary"] = self.summary
        if self.published:
            d["published"] = self.published
        return d


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
    # The regime the attached card shows (spot vs the flip): "positive",
    # "negative", "neutral" (on the flip) or "unresolved".
    regime: str | None = None
    momentum_label: str | None = None  # e.g. "down on the day, pressing session lows"
    vwap: float | None = None
    vwap_position: str | None = None  # e.g. "Above VWAP", "Below VWAP"
    # True when ``spot`` is a futures-implied projection (cash index outside
    # the cash session), with the future it came from (e.g. "@ES").  The model
    # must frame such a spot as implied/overnight, never as a live cash print.
    spot_is_projected: bool = False
    future_symbol: str | None = None
    # How the walls / gamma flip MOVED through the session, and what price did
    # to each print while it was in force — see
    # :func:`src.jobs.level_history.LevelHistory.to_prompt_dict`.  None on the
    # pre-market fire (no session path yet).  ``historical_level_values`` is
    # the flat list of every value in that path, so the invented-price guard
    # accepts a wall print that has since been superseded.  ``level_paths`` is
    # the same values kept per level ("call_wall" / "put_wall" / "gamma_flip"),
    # so a sentence naming a level can be held to that level's own values.
    level_history: dict[str, Any] | None = None
    historical_level_values: list[float] = field(default_factory=list)
    level_paths: dict[str, list[float]] = field(default_factory=dict)

    def change_pct(self) -> float | None:
        if self.spot is None or self.prior_close in (None, 0):
            return None
        return (self.spot - self.prior_close) / self.prior_close * 100

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "spot": self.spot,
            "spot_is_projected": self.spot_is_projected,
            "spot_future_symbol": (self.future_symbol or "").lstrip("@").upper() or None,
            "prior_close": self.prior_close,
            "change_vs_prior_close_pct": self.change_pct(),
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
            "net_gex": self.net_gex,
            "net_gex_display": _short_scale_gex(self.net_gex),
            "regime": self.regime,
            "momentum": self.momentum_label,
            "vwap": self.vwap,
            "vwap_position": self.vwap_position,
            # Present only on the midday / close fires: how the levels moved
            # during the session and what price did at each.  The level fields
            # above are what the attached card shows now (on the close read,
            # the map for the next session).
            "level_history": self.level_history,
        }


def _short_scale_gex(v: float | None) -> str | None:
    """Mirror :func:`src.jobs.bulletin_tweet._fmt_net_gex` — the short-scale
    form ("+$7.74B", "-$125.0M") the post uses.  A plain hyphen, not the
    typographic minus the card draws: the post has to read as typed.

    Duplicated here so bulletin_llm has no import dependency on
    bulletin_tweet (which imports the LLM module lazily).  Keeps them
    decoupled."""
    if v is None:
        return None
    abs_v = abs(v)
    sign = "+" if v >= 0 else "-"
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
            "read_label": MODE_HEADER_LABEL.get(self.mode, "Market Read"),
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
class LlmPost:
    """The narrative fragments the caller assembles into post + reply."""

    opening: str
    bottom_line: str
    reply: str

    def to_json(self) -> str:
        return json.dumps(
            {"opening": self.opening, "bottom_line": self.bottom_line, "reply": self.reply}
        )


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
    tomorrow" framing.  Caps at 10 days lookahead as a runaway guard."""
    cursor = day + timedelta(days=1)
    for _ in range(max_lookahead):
        if _is_trading_day(cursor):
            return cursor
        cursor += timedelta(days=1)
    return None


def build_day_context(mode: str, day: date) -> DayContext:
    """Compute the non-per-symbol context flags for one fire."""
    next_td = next_trading_day(day)
    market_closed_tomorrow = next_td is not None and next_td != day + timedelta(days=1)
    # "Holiday eve" = today's close is followed by a market-closed
    # non-weekend day (excludes normal Friday closes).
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
    symbols: list[SymbolInput],
    day_context: DayContext,
    headlines: list[Headline] | None = None,
    featured_symbol: str | None = None,
) -> str:
    """Assemble the JSON payload the model sees as the user message.

    Format is JSON-in-plain-text so the model can quote figures from
    it verbatim without ambiguity.  ``featured_symbol`` marks which one
    to write the post about; each level dict carries a ``featured`` flag."""
    featured = (featured_symbol or "").upper() or None
    levels = []
    for s in symbols:
        d = s.to_prompt_dict()
        d["featured"] = featured is not None and s.symbol.upper() == featured
        levels.append(d)
    payload = {
        "context": day_context.to_prompt_dict(),
        "featured_symbol": featured,
        "headlines": [h.to_prompt_dict() for h in (headlines or [])],
        "levels": levels,
        "instructions": (
            "Write the post about the symbol marked featured=true, tying the "
            "headlines, its price action and its dealer-gamma regime together "
            "in the voice described in the system prompt, then the threaded "
            "reply.  Every price you mention, in the post or the reply, must "
            "appear in levels."
        ),
    }
    return json.dumps(payload, indent=2, default=str)


def _extract_json_block(text: str) -> str | None:
    """Find the JSON object block in the model output.

    Uses first-``{`` to last-``}`` rather than brace counting because
    the model's string values can contain literal braces that throw off
    a naive depth counter.  Since we ask for a JSON object as the entire
    response, the outermost braces bracket the whole payload."""
    stripped = text.strip()
    for fence in ("```json", "```JSON", "```"):
        if stripped.startswith(fence):
            stripped = stripped[len(fence) :].lstrip()
            break
    if stripped.endswith("```"):
        stripped = stripped[:-3].rstrip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    return stripped[start : end + 1]


def _call_claude(
    system: str,
    messages: list[dict[str, Any]],
    api_key: str,
    model: str,
    max_tokens: int,
    timeout_seconds: int,
    schema: dict[str, Any] | None = None,
    errors: list[str] | None = None,
) -> dict[str, Any] | None:
    """POST one Messages API request; the parsed response, or None.

    ``schema`` switches on structured output, so the reply is guaranteed to
    parse to that shape.  Failures log and, when ``errors`` is given, append
    a plain-English reason the operator's notice can quote."""
    body: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
        "user-agent": "zerogex-bulletin-llm/1.0",
    }
    if schema is not None:
        body["output_config"] = {"format": {"type": "json_schema", "schema": schema}}
    if model == DEFAULT_MODEL:
        body["fallbacks"] = "default"
        headers["anthropic-beta"] = FALLBACK_BETA
    raw = ""
    retries_left = API_ATTEMPTS - 1
    while True:
        req = Request(
            ANTHROPIC_API_URL,
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(req, timeout=timeout_seconds) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            break
        except HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", errors="replace")[:300]
            except Exception:  # noqa: BLE001
                pass
            logger.warning("bulletin_llm: Claude API call failed (%s) %s", exc, detail)
            if exc.code == 400 and "fallbacks" in body and "fallback" in detail.lower():
                # The refusal fallback is a beta option; if this account is
                # refused it, the post shouldn't be held for it.
                logger.warning("bulletin_llm: retrying without the refusal fallback")
                body.pop("fallbacks")
                headers.pop("anthropic-beta", None)
                continue
            if exc.code in _RETRYABLE_STATUS and retries_left > 0:
                retries_left -= 1
                time.sleep(API_RETRY_PAUSE_SECONDS)
                continue
            _note(errors, f"the Claude API returned HTTP {exc.code} {detail}".strip())
            return None
        except (URLError, TimeoutError, OSError) as exc:
            logger.warning("bulletin_llm: Claude API call failed (%s)", exc)
            if retries_left > 0:
                retries_left -= 1
                time.sleep(API_RETRY_PAUSE_SECONDS)
                continue
            _note(errors, f"the Claude API could not be reached ({exc})")
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning("bulletin_llm: unexpected Claude API error (%s)", exc)
            _note(errors, f"unexpected Claude API error ({exc})")
            return None

    try:
        return json.loads(raw) if raw else None
    except json.JSONDecodeError as exc:
        logger.warning("bulletin_llm: Claude response was not JSON (%s)", exc)
        _note(errors, "the Claude API returned something that wasn't JSON")
        return None


def _note(errors: list[str] | None, message: str) -> None:
    if errors is not None:
        errors.append(message)


def _extract_text_from_response(payload: dict[str, Any]) -> str | None:
    """Pull the assistant's text out of the messages API response."""
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


def _parse_post(body_json: str) -> LlmPost | None:
    """Parse the model's JSON body into an ``LlmPost``.

    Guards against missing fields, wrong types, and pathologically long
    strings.  Returns None on any structural failure."""
    try:
        # strict=False accepts unescaped control chars inside string values;
        # Claude sometimes emits multi-para fields with literal newlines.
        obj = json.loads(body_json, strict=False)
    except json.JSONDecodeError as exc:
        logger.warning("bulletin_llm: model JSON did not parse (%s)", exc)
        return None
    if not isinstance(obj, dict):
        logger.warning("bulletin_llm: model returned non-object payload")
        return None

    def _str_field(name: str) -> str:
        value = obj.get(name)
        if not isinstance(value, str):
            if value is not None:
                logger.warning(
                    "bulletin_llm: model field %r was %s, expected string",
                    name,
                    type(value).__name__,
                )
            return ""
        # Cap at 5000 chars per section — well under X's long-form ceiling.
        return value.strip()[:5000]

    opening = _str_field("opening")
    bottom_line = _str_field("bottom_line")
    reply = _str_field("reply")

    # opening + reply are the load-bearing fields; without them there's no
    # post worth composing.
    if not opening or not reply:
        logger.warning(
            "bulletin_llm: model output missing required sections "
            "(opening_len=%d, reply_len=%d)",
            len(opening),
            len(reply),
        )
        return None

    return LlmPost(opening=opening, bottom_line=bottom_line, reply=reply)


def _post_text(post: LlmPost) -> str:
    """Everything the model wrote (post + reply), comma-thousands
    collapsed so "7,483" reads as one number."""
    combined = "\n".join([post.opening, post.bottom_line, post.reply])
    return re.sub(r"(?<=\d),(?=\d{3}\b)", "", combined)


def _input_prices(s: SymbolInput) -> list[float]:
    """Every price the model was given for ``s``."""
    values = (
        s.spot,
        s.prior_close,
        s.session_open,
        s.session_high,
        s.session_low,
        s.gamma_flip,
        s.call_wall,
        s.put_wall,
        s.max_pain,
        s.vwap,
        *s.historical_level_values,
    )
    return [v for v in values if v is not None]


def _price_band(s: SymbolInput) -> tuple[float, float] | None:
    """The range where a number would read as a price for ``s``.

    Anchored on spot when we have it, else the center of the provided levels.
    ±15% is wide enough to cover the walls / flip / session range and tight
    enough to exclude typical macro news figures."""
    if s.spot:
        anchor = s.spot
    else:
        values = _input_prices(s)
        if not values:
            return None
        anchor = sum(values) / len(values)
    return anchor * 0.85, anchor * 1.15


def _in_band(n: float, s: SymbolInput) -> bool:
    band = _price_band(s)
    return band is not None and band[0] <= n <= band[1]


def _invented_prices(post: LlmPost, symbols: list[SymbolInput]) -> list[str]:
    """In-band numbers in the narrative that match no price in the input.

    Scans the narrative (post + reply + notes) for numbers that fall inside the
    symbol's plausible PRICE BAND — i.e. numbers that could be mistaken for a
    (fabricated) level — and confirms each appears in the input's ``levels``
    block.  Numbers OUTSIDE the band are left alone: with the post now
    single-symbol, the "known" set is just SPY's handful of ~744 levels, and a
    real market post naturally cites news figures ("the S&P 500", "the Dow rose
    300", a year, dollar amounts) that are nowhere near spot.  The old check
    flagged those as invented prices and threw the whole (good) post away,
    which is exactly what forced the bland static fallback.  Anchoring the
    check on the price band keeps the anti-hallucination guard for actual level
    claims while letting the story breathe.

    Values the level path carried EARLIER in the session count as known: once
    the model has been told the put wall walked 777 → 776 → 775, "it lost 777
    and 776 before defending 775" is the accurate read, and rejecting it would
    force the post back to the closing-snapshot version this tracking exists
    to replace.

    This check is deliberately loose (±2, and the nearest multiple of 5 of any
    input price counts as known) so a plain price reference like "SPY pushed
    toward 780" survives.  It says nothing about WHICH level a number is — that
    is :func:`_misstated_levels`' job."""
    raw_values: list[float] = []
    known_values: set[int] = set()
    for s in symbols:
        for v in _input_prices(s):
            raw_values.append(v)
            for candidate in (int(round(v)), int(round(v / 5) * 5)):
                if candidate > 0:
                    known_values.add(candidate)

    if not raw_values:
        # No prices were provided → nothing to validate against; don't reject
        # (the model has no levels to quote anyway).
        return []

    # Match 3-6 digit integer parts (skip 1-2 digits — those are everywhere).
    hits = re.findall(r"(?<!\d)(\d{3,6})(?:\.\d+)?(?!\d)", _post_text(post))
    invented: list[str] = []
    for h in hits:
        n = int(h)
        if not any(_in_band(n, s) for s in symbols):
            # Outside the symbol's price band → a news/other figure, not a
            # level claim.  Leave it alone.
            continue
        # Tolerate ±2 to handle the model quoting "7,483" for spot 7482.71.
        if not any(abs(n - k) <= 2 for k in known_values) and h not in invented:
            invented.append(h)
    return invented


def _validate_no_invented_prices(
    post: LlmPost,
    symbols: list[SymbolInput],
) -> bool:
    """True when every in-band number in the narrative is an input price.

    See :func:`_invented_prices`."""
    return not _invented_prices(post, symbols)


# ---------------------------------------------------------------------------
# Level claims — "the 780 call wall" has to BE the call wall
# ---------------------------------------------------------------------------

LEVEL_NAMES = {
    "call_wall": "call wall",
    "put_wall": "put wall",
    "gamma_flip": "gamma flip",
    "max_pain": "max pain",
}

# A price as the model writes it: "780", "$780", "761.30".  Never part of a
# larger token, so "780s" (a range, not a level), "2026" inside a date, or
# the "5" of "0.5%" don't read as one.
_NUM = r"(?<![\w.$])\$?(?P<num>\d+(?:\.\d+)?)(?![\w])"

# "<number> <level>": "the 780 call wall", "the 780-strike call wall",
# "780 (the call wall)".  Bare "flip" / "wall" count here, since "the 747.29
# flip" and "the 740 wall" are the voice.
_NUM_THEN_LEVEL = re.compile(
    _NUM
    + r"(?:[\s-]+(?:strike|level))?(?:[\s-]+|\s*\(\s*(?:the\s+)?)"
    + r"(?P<label>call[\s-]+wall|put[\s-]+wall|gamma[\s-]+flip|zero[\s-]+gamma"
    + r"|max(?:imum)?[\s-]+pain|flip|wall)\b",
    re.IGNORECASE,
)

# "<level> <connectives> <number>": "call wall at 780", "the put wall sat at
# 755", "gamma flip (758.40)", "the flip, which sits at 758.4".  Only words
# that keep the number ATTACHED to the level may sit between the two, so "the
# call wall held and SPY ran to 780" is not read as a claim.  Bare "flip" and
# "wall" need a "the" in front: "SPY could flip 760" is a verb, not a level.
_CONNECTIVES = (
    "at|of|near|around|is|was|sits|sat|sitting|stands|stood|now|still|up|down|"
    "moved|moves|shifted|shifts|rolled|rolls|reset|resets|migrated|walked|stepped|"
    "to|from|holding|held|pinned|parked|back|just|right|firmly|squarely|remains|"
    "remained|stays|stayed|lives|overhead|higher|lower|broke|cracked|failed|gave|"
    "way|which|point|level|line|the|a|its"
)
_LEVEL_THEN_NUM = re.compile(
    r"\b(?P<label>call[\s-]+wall|put[\s-]+wall|gamma[\s-]+flip|zero[\s-]+gamma"
    r"|max(?:imum)?[\s-]+pain|(?<=\bthe\s)flip|(?<=\bthe\s)wall)\b"
    + rf"(?:[\s,:(=—–-]+(?:{_CONNECTIVES})\b)*[\s,:(=—–-]+"
    + _NUM,
    re.IGNORECASE,
)


def _label_key(label: str) -> str:
    """Matched level wording → ``LEVEL_NAMES`` key, or "wall" for either."""
    words = re.sub(r"[\s-]+", " ", label.lower())
    if words.startswith("call"):
        return "call_wall"
    if words.startswith("put"):
        return "put_wall"
    if "flip" in words or "gamma" in words:
        return "gamma_flip"
    if "pain" in words:
        return "max_pain"
    return "wall"


def _level_claims(text: str) -> list[tuple[float, str, str]]:
    """Every (number, level key, matched text) pairing the prose makes."""
    claims: list[tuple[float, str, str]] = []
    claimed: set[int] = set()
    for pattern in (_NUM_THEN_LEVEL, _LEVEL_THEN_NUM):
        for m in pattern.finditer(text):
            # A number is claimed once, and the level named right AFTER it
            # wins: in "the put wall gave way to the 758 flip", 758 is the
            # flip's, not the put wall's.
            if m.start("num") in claimed:
                continue
            claimed.add(m.start("num"))
            claims.append((float(m.group("num")), _label_key(m.group("label")), m.group(0).strip()))
    return claims


def _level_values(s: SymbolInput, key: str) -> list[float]:
    """Every value ``key`` held for ``s`` in the window the read describes —
    its path through the session, then the standing value."""
    if key == "wall":
        return _level_values(s, "call_wall") + _level_values(s, "put_wall")
    standing = getattr(s, key)
    out: list[float] = []
    for v in (*s.level_paths.get(key, ()), standing):
        if v is not None and v not in out:
            out.append(v)
    return out


def _flip_tolerance(values: list[float]) -> float:
    """The flip is a computed price, so "the 758 flip" for 758.40 is fine."""
    return max(1.0, 0.001 * max(abs(v) for v in values))


def _claim_holds(n: float, key: str, values: list[float]) -> bool:
    if not values:
        return False
    if key == "gamma_flip":
        # The flip drifts rather than steps: anything inside its session band
        # is a value it actually held.
        tol = _flip_tolerance(values)
        return min(values) - tol <= n <= max(values) + tol
    # Walls and max pain are strikes, and are quoted as strikes.
    return any(abs(n - v) <= 0.5 for v in values)


def _fmt_value(v: float) -> str:
    """Print a level as the post would: "765", "758.4", "7,650"."""
    return f"{v:,.2f}".rstrip("0").rstrip(".")


def _describe_level(s: SymbolInput, key: str) -> str:
    """What the input says ``key`` was, for the correction note."""
    if key == "wall":
        parts = [_describe_level(s, k) for k in ("call_wall", "put_wall")]
        return " and ".join(parts)
    name = LEVEL_NAMES[key]
    values = _level_values(s, key)
    if not values:
        return f"the input has no {name}"
    if key == "gamma_flip" and len(values) > 1:
        # Its values are the ends of a drift band, not a sequence of prints.
        return f"the {name} ranged {_fmt_value(min(values))}–{_fmt_value(max(values))}"
    if len(values) == 1:
        return f"the {name} was {_fmt_value(values[0])}"
    return f"the {name} was {' then '.join(_fmt_value(v) for v in values)}"


def _misstated_levels(post: LlmPost, symbols: list[SymbolInput]) -> list[tuple[float, str]]:
    """(number, problem) for each sentence that names a level with a number
    that level never had.

    This is the check the number-presence guard can't make.  "Stalled under
    the 780 call wall" passes that guard whenever 780 is near ANY input price
    — the session high, a round number off spot, a post-bell reset — while
    the Key-levels block right below it says the call wall was 765.  Here the
    number next to "call wall" has to be the call wall: its standing value or
    a print from its own session path."""
    problems: list[tuple[float, str]] = []
    for n, key, snippet in _level_claims(_post_text(post)):
        in_band = [s for s in symbols if _in_band(n, s)]
        if not in_band:
            # A points move or a news figure ("the call wall is 15 points
            # up"), not a price for the level.
            continue
        if any(_claim_holds(n, key, _level_values(s, key)) for s in in_band):
            continue
        actual = _describe_level(in_band[0], key)
        if _level_values(in_band[0], key):
            actual += f", never {_fmt_value(n)}"
        problems.append((n, f'"{snippet}": {actual}'))
    return problems


def _post_problems(post: LlmPost, symbols: list[SymbolInput]) -> list[str]:
    """Everything wrong with a draft's numbers, most specific first.  A
    number already reported against the level it was named for isn't
    reported a second time as merely unknown."""
    misstated = _misstated_levels(post, symbols)
    named = {int(n) for n, _ in misstated}
    return [problem for _, problem in misstated] + [
        f"{h} is not a price anywhere in the input"
        for h in _invented_prices(post, symbols)
        if int(h) not in named
    ]


def _correction_message(problems: list[str]) -> str:
    """The follow-up turn that hands a draft's problems back to the model."""
    listed = "\n".join(f"* {p}" for p in problems)
    return (
        "Your draft doesn't match the input levels:\n"
        f"{listed}\n\n"
        "Rewrite the JSON object with those fixed and everything else kept.  A "
        "number you tie to a level must be that level's value in the input; if a "
        "sentence only works with a different number, drop the number or the "
        "sentence.  Reply with the JSON object only."
    )


def _revision_message(problems: list[str]) -> str:
    """The follow-up turn that hands the final review's findings back."""
    listed = "\n".join(f"* {p}" for p in problems)
    return (
        "A final review of your draft, as it will be posted (with the header "
        "line and the key levels list added), found these problems:\n"
        f"{listed}\n\n"
        "Rewrite the JSON object with every one of them fixed and everything "
        "else kept.  If a sentence can't be supported by the inputs, drop it."
    )


def generate_post(
    mode: str,
    day: date,
    symbols: list[SymbolInput],
    day_context: DayContext | None = None,
    headlines: list[Headline] | None = None,
    api_key: str | None = None,
    model: str | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    featured_symbol: str | None = None,
    revise_from: LlmPost | None = None,
    feedback: list[str] | None = None,
    errors: list[str] | None = None,
) -> LlmPost | None:
    """Call Claude and return an ``LlmPost``, or None on any failure.

    ``featured_symbol`` is the single symbol the post centers on.  With
    ``revise_from`` and ``feedback``, the model gets its earlier draft back
    with the review's findings and rewrites it.  None means no usable post
    (no API key, API outage, malformed response, levels still misstated);
    when ``errors`` is given, the reason is appended to it."""
    if not symbols:
        _note(errors, "there was no symbol data to write about")
        return None
    key = (api_key or os.environ.get("ANTHROPIC_API_KEY", "")).strip()
    if not key:
        logger.warning("bulletin_llm: ANTHROPIC_API_KEY unset — can't write the post")
        _note(errors, "ANTHROPIC_API_KEY is not set, so the post could not be written")
        return None

    ctx = day_context or build_day_context(mode, day)
    model_id = model or os.environ.get("BULLETIN_TWEET_LLM_MODEL", "").strip() or DEFAULT_MODEL
    env_max_tokens = os.environ.get("BULLETIN_TWEET_LLM_MAX_TOKENS", "").strip()
    if env_max_tokens.isdigit():
        max_tokens = int(env_max_tokens)

    user_msg = _build_user_message(
        symbols,
        ctx,
        headlines=headlines,
        featured_symbol=featured_symbol,
    )
    messages: list[dict[str, Any]] = [{"role": "user", "content": user_msg}]
    if revise_from is not None and feedback:
        messages += [
            {"role": "assistant", "content": revise_from.to_json()},
            {"role": "user", "content": _revision_message(feedback)},
        ]
    for correction_round in range(MAX_CORRECTION_ROUNDS + 1):
        resp = _call_claude(
            SYSTEM_PROMPT,
            messages,
            key,
            model_id,
            max_tokens,
            timeout_seconds,
            POST_SCHEMA,
            errors,
        )
        if resp is None:
            return None
        if resp.get("stop_reason") == "refusal":
            logger.warning("bulletin_llm: the model declined to write the post")
            _note(errors, "the writing model declined the request")
            return None

        text = _extract_text_from_response(resp)
        if not text:
            logger.warning("bulletin_llm: Claude response had no text content")
            _note(errors, "the writing model returned no text")
            return None

        post = _post_from_text(text, resp.get("stop_reason"), max_tokens)
        if post is None:
            _note(errors, "the writing model's reply could not be read as a post")
            return None

        problems = _post_problems(post, symbols)
        if not problems:
            return post
        if correction_round == MAX_CORRECTION_ROUNDS:
            logger.warning(
                "bulletin_llm: draft still misstates the levels after %d correction "
                "round(s): %s",
                MAX_CORRECTION_ROUNDS,
                "; ".join(problems),
            )
            _note(errors, "the draft kept misstating levels: " + "; ".join(problems))
            return None
        logger.warning(
            "bulletin_llm: draft misstates the levels — sending it back to the model: %s",
            "; ".join(problems),
        )
        messages = messages + [
            {"role": "assistant", "content": text},
            {"role": "user", "content": _correction_message(problems)},
        ]
    return None


def _post_from_text(text: str, stop_reason: Any, max_tokens: int) -> LlmPost | None:
    """The ``LlmPost`` in one model reply, or None (logged) when there isn't one."""
    json_block = _extract_json_block(text)
    if not json_block:
        if stop_reason == "max_tokens":
            logger.warning(
                "bulletin_llm: model output truncated at max_tokens=%d — "
                "increase BULLETIN_TWEET_LLM_MAX_TOKENS if this keeps happening.",
                max_tokens,
            )
        else:
            logger.warning(
                "bulletin_llm: could not find a JSON object in model output "
                "(stop_reason=%s) — first 200 chars: %r",
                stop_reason,
                text[:200],
            )
        return None
    return _parse_post(json_block)


# ---------------------------------------------------------------------------
# The review: an independent fact-check of the finished post
# ---------------------------------------------------------------------------

REVIEW_SYSTEM_PROMPT = """\
You are the last check before a market post goes out on the ZeroGEX X
account.  The founder posts these as their own, so anything wrong in one is
the founder's mistake in public.  You get the post and the threaded reply
exactly as they will be published, the inputs they were written from, and
usually the Live Bulletin image attached to the post.

List a problem for each of these you find:
1. News that isn't in the headlines: an event, figure, name, quote, cause or
   timing that no headline's title or summary supports.  General market
   mechanics (how dealer hedging works) are not news and need no source.
2. Price action the numbers contradict: up or down on the day must match
   change_vs_prior_close_pct; highs, lows and the range must match
   session_open, session_high and session_low; "held", "broke", "tested" or
   "never tested" at a level must match that level's outcome in
   level_history, and without level_history no such claim is supported.
3. A level with the wrong number: a number named as the put wall, call wall,
   gamma flip or max pain must be that level's value in "levels" or a value
   from its own level_history path.  A flip rounded to fewer decimals is
   fine; a wall rounded to a different strike is not.
4. A regime claim the data contradicts: above the gamma flip is positive
   gamma (dealers long gamma), below it is negative gamma (dealers short
   gamma), and "regime" is what the attached image shows.
5. On the close post, the top-level levels are the next session's map (the
   day's 0DTE options expired at the bell and the chain re-priced).  A
   sentence that has today's tape reacting to a level that appears only
   there, and not in level_history, is a problem.
6. Headlines ignored: when the headlines include real market, economic, rate
   or company news, the post should use at least one of them.
7. Writing that gives it away as generated or careless: British spellings;
   em dashes or en dashes; emojis, hashtags, markdown, all-caps words or
   exclamation points; stock phrases or a formula hook; anything that reads
   like a bot or a press release instead of a trader typing; trade
   recommendations; hype.

Don't flag correct statements you would phrase differently, wording
preferences, the header line, or the key levels list (those are checked
separately), or numbers that match the inputs.

The image ("image_problems"): it should be a fully rendered ZeroGEX Live
Bulletin card for the featured symbol.  Flag a blank, cut-off or error page,
a card for a different symbol, or a card whose main levels show a dash
instead of a number.  The exact numbers were read from the page itself and
are in "levels", so don't re-read small digits from the picture.  The change
shown next to the card's price is measured from the card's own reference
close, which after the 4:00 PM bell is today's close; judge the day's move by
change_vs_prior_close_pct only.

Each problem is one short sentence that quotes the words at fault and says
what is wrong.  Empty lists mean the post can go out.
"""

REVIEW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "problems": {"type": "array", "items": {"type": "string"}},
        "image_problems": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["problems", "image_problems"],
    "additionalProperties": False,
}


@dataclass
class Review:
    """The fact-check's verdict.  ``ran`` is False when no verdict came back
    (no key, API trouble, an unreadable reply); ``error`` then says why."""

    ran: bool
    problems: list[str] = field(default_factory=list)
    image_problems: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def approved(self) -> bool:
        return self.ran and not self.problems and not self.image_problems


def review_post(
    mode: str,
    day: date,
    post_text: str,
    reply_text: str,
    symbol: SymbolInput,
    headlines: list[Headline] | None = None,
    card_png: bytes | None = None,
    day_context: DayContext | None = None,
    api_key: str | None = None,
    model: str | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> Review:
    """Fact-check the finished post against its inputs and the card image.

    A separate call from the writer, so it reads the post the way a reader
    will rather than the way it was meant.  Never raises."""
    key = (api_key or os.environ.get("ANTHROPIC_API_KEY", "")).strip()
    if not key:
        return Review(ran=False, error="ANTHROPIC_API_KEY is not set")
    model_id = (
        model or os.environ.get("BULLETIN_TWEET_REVIEW_MODEL", "").strip() or DEFAULT_REVIEW_MODEL
    )
    ctx = day_context or build_day_context(mode, day)
    payload = {
        "context": ctx.to_prompt_dict(),
        "featured_symbol": symbol.symbol.upper(),
        "headlines": [h.to_prompt_dict() for h in (headlines or [])],
        "levels": symbol.to_prompt_dict(),
        "post": post_text,
        "reply": reply_text,
    }
    content: list[dict[str, Any]] = []
    if card_png:
        content.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": base64.b64encode(card_png).decode("ascii"),
                },
            }
        )
    content.append({"type": "text", "text": json.dumps(payload, indent=2, default=str)})

    errors: list[str] = []
    resp = _call_claude(
        REVIEW_SYSTEM_PROMPT,
        [{"role": "user", "content": content}],
        key,
        model_id,
        max_tokens,
        timeout_seconds,
        REVIEW_SCHEMA,
        errors,
    )
    if resp is None:
        return Review(ran=False, error="; ".join(errors) or "the review call failed")
    if resp.get("stop_reason") == "refusal":
        return Review(ran=False, error="the review model declined the request")
    if resp.get("stop_reason") == "max_tokens":
        return Review(ran=False, error="the review was cut off before it finished")
    text = _extract_text_from_response(resp)
    block = _extract_json_block(text or "")
    try:
        verdict = json.loads(block, strict=False) if block else None
    except json.JSONDecodeError:
        verdict = None
    if not isinstance(verdict, dict):
        return Review(ran=False, error="the review's reply could not be read")

    def _strings(name: str) -> list[str]:
        items = verdict.get(name)
        if not isinstance(items, list):
            return []
        return [str(i).strip() for i in items if str(i).strip()]

    return Review(
        ran=True,
        problems=_strings("problems"),
        image_problems=_strings("image_problems"),
    )
