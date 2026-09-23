"""LLM-generated narrative for bulletin tweets — the "human wrote it" voice.

The bulletin auto-tweet fires three times a trading day (pre-market,
midday, close).  Each fire hands the day's structured snapshot — the
Live-Bulletin gamma structure, the featured symbol's price action vs the
previous close, and the day's top market headlines scraped from CNBC — to
Claude and asks it to write a natural, human-sounding market read plus a
threaded reply that plays off it and links back to ZeroGEX.

Division of labour — the same discipline the old template used, kept:

  * The LLM controls voice, framing and flow (the opening hook, the prose
    that weaves news + price action + dealer-gamma regime, the bottom-line
    takeaway, and the reply).
  * Python controls every price the post QUOTES in its ``Key levels:``
    block — the model NEVER invents a level.  The model may *reference* a
    level in prose ("dumped through the 740 put wall"), but every draft is
    checked: a number named as a level must be that level's value (its
    standing value or a print from its own session path), and any other
    in-band number must be a price from the input.  A draft that fails is
    handed back to the model once with the specifics; if it still fails, the
    caller falls back to a deterministic template.

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
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.market_calendar import NYSE_HOLIDAYS

logger = logging.getLogger("zerogex.bulletin_llm")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-sonnet-5"
# 3000 gives comfortable headroom over the typical output size for the
# post + reply + JSON scaffolding.  The static template fallback kicks in
# if the model still overflows, but at 3000 that's a rare case.
DEFAULT_MAX_TOKENS = 3000
DEFAULT_TIMEOUT_SECONDS = 45
# How many times a draft that misstates a level is handed back to the model
# with the specifics before we give up and post the static template.  The
# usual failure is one sentence in an otherwise good post; "the call wall was
# 765, never 780" lets the model fix that sentence instead of the whole post
# being thrown away.
MAX_CORRECTION_ROUNDS = 1

# The three canonical level keys the model may annotate.  Python owns the
# actual prices and base labels; the model only supplies an optional short
# contextual note per level ("primary support", "successfully defended").
LEVEL_KEYS = ("put_wall", "call_wall", "gamma_flip")

# Per-mode header label the model should use verbatim.  Matches the
# operator's approved examples ("Morning Read — $SPY", etc.).
MODE_HEADER_LABEL = {
    "premarket": "Morning Read",
    "midday": "Midday Read",
    "close": "Post-Market Read",
}


SYSTEM_PROMPT = """\
You write market commentary for the ZeroGEX X (Twitter) account.  Your job is
to sound like a sharp human trader wrote it — natural, confident, plain-spoken
— NOT like an automated bot or a data readout.

Each post features ONE symbol — the one marked "featured": true in the input
(also named in "featured_symbol").  Write about THAT symbol.

You are given three things to weave together:
  1. HEADLINES — the day's top market news scraped from CNBC (in "headlines").
  2. THE LIVE BULLETIN — the featured symbol's dealer-gamma structure: gamma
     flip, call wall, put wall, max pain, and net gamma (in "levels").
  3. PRICE ACTION — where the symbol is trading now vs the previous close, the
     session's path (open / high / low), and momentum (in "levels").

STUDY THIS VOICE — it is exactly the shape we want:

  ---
  The headlines changed.

  The regime didn't.

  SPY dumped straight through the 740 put wall this morning before news of
  potential renewed U.S.–Iran talks sent oil lower and sparked a sharp
  reversal.

  Price ripped back through 740, but the rally stalled well short of the 745
  call wall and has since rolled back over.

  That's classic negative gamma: fast moves, sharp reversals, and little
  follow-through.
  ---

Notice:
* A two-line contrasting HOOK to open ("The headlines changed." / "The regime
  didn't.").  One idea per short line, blank line between.  Vary it every time.
* Then prose that ties the NEWS to the PRICE ACTION to the GAMMA REGIME.  Say
  what price DID at a level ("dumped straight through the 740 put wall",
  "stalled well short of the 745 call wall", "buyers defended 735 into the
  bell").
* Explain the regime plainly: whether spot is above/below the gamma flip,
  whether net gamma is positive or negative, and what that means for the tape
  ("negative gamma = dealer hedging amplifies moves rather than dampens them:
  fast moves, sharp reversals, little follow-through").
* No hype, no marketing, no exclamation points, no all-caps words, no emojis,
  no hashtags, no markdown (**, __, ##).

MATCH THE MODE:
* premarket ("Morning Read") — look AHEAD into the open.  Frame where the
  symbol sits vs the flip/walls going in, and what the overnight news sets up.
* midday ("Midday Read") — mid-session.  What has held, what hasn't, what the
  morning's path says about the regime.
* close ("Post-Market Read") — look BACK at the session's battle around the
  levels, then the standing structure into tomorrow.

THE LEVELS MOVE — READ "level_history" BEFORE YOU DESCRIBE ANY OF THEM:
On the midday and close fires each symbol carries a "level_history" object
describing what the walls and the gamma flip actually did during the session.
The top-level "put_wall" / "call_wall" / "gamma_flip" figures are the
structure as of the LAST IN-SESSION frame — the end of the story, not the
whole of it.  When "level_history" is present it is the ONLY acceptable
source for a claim about what happened at a level.

* Each wall carries a "path": the ordered list of values it sat at, each with
  the window it was in force and an "outcome" — "broke" (price traded
  decisively through it), "held" (price came to it and turned), "untested"
  (price never got near it), "unknown" (no tape to judge by).  Narrate the
  path when "changed_during_session" is true: a put wall that walked
  777 → 776 → 775, losing the first two and defending the third, is a far
  better story than the closing number alone, and it is what actually
  happened.
* NEVER say a level was untested, defended, held or broken unless the
  matching "outcome" says so.  A level that only became the wall at 14:00 was
  not in play at the open — do not narrate it as if it were.
* An "after_the_bell_reset" ("higher" / "lower") means the chain re-priced
  that level once the day's 0DTE expiries rolled off after 16:00 ET — that is
  tomorrow's structure, never a level the tape traded against today.  You may
  mention the reset as a forward-looking note ("the roll-off resets the put
  wall well lower into tomorrow").  You are not given the new value: a
  separate line stating it is appended for you, so never write a number for
  it.
* The gamma flip is a drifting computed price, not a strike.  Use
  "spot_crossings", "spot_side_at_open" and "spot_side_at_close" for whether
  the tape ever changed regime — zero crossings means it never did, however
  close it came.
* Any value appearing in a wall's "path" is a real level from today and may
  be quoted in prose even though it is no longer the standing wall.

REPLY:
Also write a threaded reply that is exactly ONE sentence — a sharp, specific
add-on that builds on THIS post, not a generic lesson or a restatement of it.
Make it particular to today's setup (the level that mattered, the news, the
regime) so it reads fresh every time and never falls back on stock phrases.
Think "one more incisive beat" — a nuance, an implication, or the tell to
watch next — the kind of line that makes the reader smarter.  Examples of the
SHAPE (do not reuse the wording):
* "The tell wasn't 740 breaking — it was how fast it reclaimed once the
  headline hit; that's short gamma doing the work in both directions."
* "Flat on the close, violent underneath — that gap between the print and the
  path is the whole story when dealers are this short."
Do NOT restate the bottom line.  Do NOT end with a call-to-action or a colon.
Do NOT include a URL or any hashtags — the zerogex.io link is appended for you.

STRICT RULES:
* Every dollar figure or strike price you write — in the post OR the reply, for
  ANY symbol — MUST appear verbatim in the input's "levels" block.  Never
  invent a number.  If you are unsure of a number, describe it without quoting
  a figure.
* A number you put next to a named level — "the 745 call wall", "call wall at
  745", "the 740 put wall", "the 747.29 flip", "max pain at 744" — must be
  THAT level's value in the input: its top-level figure, or a value from that
  level's own "path" in "level_history".  Never round a level to a nearby
  round number (a 778 call wall is not "the 780 call wall"), never give one
  level another level's number, and never call a strike a wall because price
  stalled there or because it is a round number.
* When quoting net gamma, use the "net_gex_display" value ("+$7.74B",
  "−$125.0M") — NEVER the raw "net_gex" float.
* Do NOT restate the levels as a bulleted list in your prose — the caller adds
  a clean "Key levels:" block after your opening.  Weave only the few levels
  that matter into the sentences.
* Do NOT give trading recommendations ("buy X", "sell Y", "target Z", "long
  here").  Describe positioning and mechanics, not what the reader should do.
* If a symbol has "spot_is_projected": true (a cash index outside the cash
  session), its "spot" is IMPLIED from the futures ("spot_future_symbol"), not
  a live cash quote — frame it that way, never as a live print.
* If the input's "context" flags an event (holiday eve, FOMC, CPI, half-day),
  work it into the framing naturally.

OUTPUT — reply with a single JSON object and NOTHING else:
{
  "header_label": one of "Morning Read", "Midday Read", "Post-Market Read"
                  (match the mode; use header_label_hint from the input),
  "opening":      the hook + the prose body, everything from the top down to
                  just before the "Key levels:" block.  Use "\\n\\n" between
                  short paragraphs/lines.
  "level_notes":  an object with any of the keys "put_wall", "call_wall",
                  "gamma_flip" mapping to a SHORT contextual note (2-5 words,
                  no numbers) — e.g. {"put_wall": "successfully defended",
                  "call_wall": "first resistance"}.  Omit a key or use "" when
                  there's nothing to add.  These annotate the Key levels block.
                  When the input carries "level_history" the caller writes
                  those notes itself from the session path and ignores yours,
                  so spend the detail on the prose instead.
  "bottom_line":  the takeaway — 1-3 sentences.  Opinionated but no trade
                  calls.  Do NOT include the words "Bottom line:" — that label
                  is added for you.
  "reply":        the ONE-sentence sharp add-on described above.  No URL, no
                  hashtags, no call-to-action, no colon ending.
}
"""


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
    regime: str | None = None  # "positive", "negative", "neutral", "unresolved"
    momentum_label: str | None = None  # e.g. "Rising", "Collapsing", "Stable"
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
            # Present only on the midday / close fires.  The four level fields
            # above are the structure as of the LAST IN-SESSION frame; this is
            # the path they took to get there.
            "level_history": self.level_history,
        }


def _short_scale_gex(v: float | None) -> str | None:
    """Mirror :func:`src.jobs.bulletin_tweet._fmt_net_gex` — the short-scale
    form ("+$7.74B", "−$125.0M") the tweet's numeric block uses.

    Duplicated here so bulletin_llm has no import dependency on
    bulletin_tweet (which imports the LLM module lazily).  Keeps them
    decoupled."""
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
            "header_label_hint": MODE_HEADER_LABEL.get(self.mode, "Market Read"),
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
    """The composed narrative fragments the caller assembles into post + reply."""

    header_label: str
    opening: str
    bottom_line: str
    reply: str
    level_notes: dict[str, str] = field(default_factory=dict)


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
            "Feature the symbol marked featured=true (featured_symbol); write "
            "the post about it, weaving the headlines, its price action, and its "
            "dealer-gamma regime together in the voice described in the system "
            "prompt.  Then write the threaded reply.  Every price you mention — "
            "in the post or the reply, for any symbol — must appear in ``levels``. "
            "Reply with the JSON object described in the system prompt."
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
    messages: list[dict[str, str]],
    api_key: str,
    model: str,
    max_tokens: int,
    timeout_seconds: int,
) -> dict[str, Any] | None:
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
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
    strings.  Returns None on any structural failure so the caller falls
    back to the template."""
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

    def _str_field(name: str, required: bool = True) -> str | None:
        value = obj.get(name)
        if value is None:
            return None if required else ""
        if not isinstance(value, str):
            logger.warning(
                "bulletin_llm: model field %r was %s, expected string",
                name,
                type(value).__name__,
            )
            return None
        # Cap at 5000 chars per section — well under X's long-form ceiling.
        return value.strip()[:5000]

    header_label = _str_field("header_label") or ""
    opening = _str_field("opening") or ""
    bottom_line = _str_field("bottom_line") or ""
    reply = _str_field("reply") or ""

    # level_notes is optional; tolerate absence / wrong type.
    notes_raw = obj.get("level_notes")
    level_notes: dict[str, str] = {}
    if isinstance(notes_raw, dict):
        for key in LEVEL_KEYS:
            v = notes_raw.get(key)
            if isinstance(v, str) and v.strip():
                # 60-char cap keeps a runaway note from bloating the block.
                level_notes[key] = v.strip()[:60]

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

    return LlmPost(
        header_label=header_label,
        opening=opening,
        bottom_line=bottom_line,
        reply=reply,
        level_notes=level_notes,
    )


def _post_text(post: LlmPost) -> str:
    """Everything the model wrote (post + reply + notes), comma-thousands
    collapsed so "7,483" reads as one number."""
    combined = "\n".join(
        [
            post.opening,
            post.bottom_line,
            post.reply,
            " ".join(post.level_notes.values()),
        ]
    )
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

    Anchored on spot when we have it, else the centre of the provided levels.
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
) -> LlmPost | None:
    """Call Claude and return an ``LlmPost`` — or None on any failure.

    The caller invokes this AFTER assembling the deterministic bulletin
    data + the scraped headlines.  ``featured_symbol`` is the single symbol
    the post centers on.  A None return is expected and normal (no API key,
    API outage, malformed response) and instructs the caller to fall back to
    the static template."""
    if not symbols:
        return None
    key = (api_key or os.environ.get("ANTHROPIC_API_KEY", "")).strip()
    if not key:
        logger.debug("bulletin_llm: ANTHROPIC_API_KEY unset — skipping LLM")
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
    messages: list[dict[str, str]] = [{"role": "user", "content": user_msg}]
    for correction_round in range(MAX_CORRECTION_ROUNDS + 1):
        resp = _call_claude(
            SYSTEM_PROMPT,
            messages,
            key,
            model_id,
            max_tokens,
            timeout_seconds,
        )
        if resp is None:
            return None

        text = _extract_text_from_response(resp)
        if not text:
            logger.warning("bulletin_llm: Claude response had no text content")
            return None

        post = _post_from_text(text, resp.get("stop_reason"), max_tokens)
        if post is None:
            return None

        problems = _post_problems(post, symbols)
        if not problems:
            return post
        if correction_round == MAX_CORRECTION_ROUNDS:
            logger.warning(
                "bulletin_llm: draft still misstates the levels after %d correction "
                "round(s) — falling back to template: %s",
                MAX_CORRECTION_ROUNDS,
                "; ".join(problems),
            )
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
                "increase BULLETIN_TWEET_LLM_MAX_TOKENS if this keeps happening. "
                "Falling back to static template.",
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
