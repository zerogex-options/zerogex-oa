"""Intraday level-path tracking for the bulletin write-ups.

The gamma structure is NOT static through a session.  Dealer positioning
re-prices minute by minute, so the put wall / call wall can migrate several
strikes over a day and the gamma flip drifts continuously.  The bulletin
tweet used to quote only the LATEST ``gex_summary`` row, which produced two
distinct classes of wrong statement in the midday and post-market reads:

  * **Migration erased.**  A put wall that sat at 777, broke, re-formed at
    776, broke again and finally held 775 was reported as a single
    never-moved level — and the LLM, seeing only the last value and a
    session low above it, annotated it "never tested".
  * **Post-close contamination.**  The 16:05 fire reads a summary row
    written AFTER the bell, once the day's 0DTE contracts have rolled off
    the chain.  That resets the walls to the next session's structure — a
    put wall of 765 that was never in play during the session it is
    supposedly describing.

This module turns the day's per-minute ``gex_summary`` path plus the
underlying's 1-min bars into an honest account of what each level actually
did:

  * :class:`WallTrack` — the ordered chain of distinct wall values, each
    with the window it was in force and what price did to it in that window
    (``broke`` / ``held`` / ``untested``).
  * :class:`FlipTrack` — the gamma flip's drift band plus how many times
    spot crossed it and which side it started and ended on.
  * ``session_close_levels`` — the structure in force at the last in-session
    frame (<= 16:00 ET).  This is what the close read must quote.
  * ``post_close_levels`` — the latest post-bell frame, kept separately so
    the post can say "after the bell the 0DTE rolled off and the put wall
    reset to X" instead of silently passing it off as the day's level.

Everything here is pure computation over rows the caller already fetched —
no DB, no network, no exceptions raised at the caller.  A missing or empty
input just yields ``None`` and the tweet renders exactly as it did before.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger("zerogex.level_history")

ET = ZoneInfo("America/New_York")

SESSION_START = time(9, 30)
SESSION_END = time(16, 0)

# The two discrete strike-space levels.  Both migrate in whole-strike jumps,
# so they get the "chain of distinct values" treatment.  The gamma flip is a
# computed continuous price and is tracked as a drift band instead.
WALL_KEYS = ("put_wall", "call_wall")
FLIP_KEY = "gamma_flip"

# How close price has to come for a level to count as TESTED, as a fraction
# of the level itself.  5 bps ≈ 39c on a 775 SPY strike — tight enough that a
# genuine approach is required, loose enough that a wick that stops a few
# cents short still reads as a test rather than "never tested".
TOUCH_PCT = 0.0005
# ...and the floor, so a low-priced underlying doesn't get a sub-tick band.
TOUCH_MIN = 0.01

# A wall value has to hold for this long to make the reported chain.  The
# analytics engine republishes every minute and the wall can flicker between
# two adjacent strikes when their gamma is near-tied; without this the chain
# would be a dozen entries of noise.  The standing (final) value is always
# kept regardless of how long it has been in force.
MIN_HOLD_MINUTES = 10

# Longest chain rendered in the tweet's Key-levels note before it elides the
# middle ("777 → … → 776 → 775").
MAX_CHAIN = 4

# Fewer in-session frames than this and we don't claim to know the path —
# a handful of scattered rows can't distinguish "held all day" from "we only
# sampled it twice".
MIN_FRAMES = 5


# ---------------------------------------------------------------------------
# Formatting — mirrors bulletin_tweet._fmt_level so the notes read the same as
# the prices in the Key-levels block.  Duplicated (not imported) to keep this
# module free of any dependency on its caller.
# ---------------------------------------------------------------------------


def _fmt(v: float | None) -> str:
    if v is None:
        return "—"
    whole = abs(v - round(v)) < 0.005
    if abs(v) >= 1000:
        return f"{v:,.0f}" if whole else f"{v:,.2f}"
    return f"{int(round(v))}" if whole else f"{v:.2f}"


def _hhmm(dt: datetime | None) -> str | None:
    return dt.astimezone(ET).strftime("%H:%M") if dt is not None else None


def _to_float(v: Any) -> float | None:
    """Coerce ``Decimal``/``int``/``str`` → float; None on anything unusable."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # noqa: PLR0124 — NaN
        return None
    return f


def _tolerance(level: float) -> float:
    return max(abs(level) * TOUCH_PCT, TOUCH_MIN)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


@dataclass
class PriceBar:
    """One minute of the underlying — only the fields the tests need."""

    ts: datetime
    high: float
    low: float
    close: float


def _normalize_bars(rows: Iterable[dict[str, Any]] | None) -> list[PriceBar]:
    """Coerce ``get_underlying_candles_for_session`` rows into PriceBars.

    Rows missing a usable timestamp or close are dropped; a missing high/low
    (pre-backfill rows carry close only) falls back to the close so the bar
    still contributes to the path."""
    bars: list[PriceBar] = []
    for row in rows or []:
        ts = row.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        close = _to_float(row.get("close"))
        if close is None:
            continue
        high = _to_float(row.get("high"))
        low = _to_float(row.get("low"))
        bars.append(
            PriceBar(
                ts=ts,
                high=high if high is not None else close,
                low=low if low is not None else close,
                close=close,
            )
        )
    bars.sort(key=lambda b: b.ts)
    return bars


def _split_frames(
    rows: Iterable[dict[str, Any]] | None,
    session_date: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Partition level rows into (in-session, post-close) by ET wall clock.

    In-session is ``[09:30, 16:00]`` of ``session_date`` — the window the
    read is describing.  Anything stamped after 16:00 is the next session's
    structure forming as the day's expiries roll off the chain, and is kept
    apart so it can be reported as such rather than quoted as today's."""
    in_session: list[dict[str, Any]] = []
    post_close: list[dict[str, Any]] = []
    for row in rows or []:
        ts = row.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        ts_et = ts.astimezone(ET)
        if ts_et.date() != session_date:
            continue
        if SESSION_START <= ts_et.time() <= SESSION_END:
            in_session.append(row)
        elif ts_et.time() > SESSION_END:
            post_close.append(row)
    in_session.sort(key=lambda r: r["timestamp"])
    post_close.sort(key=lambda r: r["timestamp"])
    return in_session, post_close


# ---------------------------------------------------------------------------
# Wall tracks
# ---------------------------------------------------------------------------


@dataclass
class WallSegment:
    """One contiguous window during which a wall sat at ``value``."""

    value: float
    start: datetime
    end: datetime
    # What price did to this level WHILE IT WAS IN FORCE:
    #   broke    — traded decisively through it
    #   held     — came within the touch band and turned
    #   untested — never got close
    #   unknown  — no bars covering the window
    outcome: str = "unknown"
    # Closest approach in the window, signed as a % of the level (negative =
    # price stayed below it).  None when there were no bars.
    approach_pct: float | None = None

    @property
    def minutes(self) -> float:
        return max((self.end - self.start).total_seconds() / 60.0, 0.0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "from": _hhmm(self.start),
            "to": _hhmm(self.end),
            "minutes_in_force": round(self.minutes),
            "outcome": self.outcome,
        }


@dataclass
class WallTrack:
    """A wall's full intraday path plus its post-bell reset, if any."""

    key: str
    segments: list[WallSegment] = field(default_factory=list)
    post_close_value: float | None = None

    @property
    def values(self) -> list[float]:
        return [s.value for s in self.segments]

    @property
    def session_value(self) -> float | None:
        """The value in force at the last in-session frame."""
        return self.segments[-1].value if self.segments else None

    @property
    def changed(self) -> bool:
        return len(self.segments) > 1

    @property
    def post_close_shift(self) -> bool:
        sv = self.session_value
        if sv is None or self.post_close_value is None:
            return False
        return abs(self.post_close_value - sv) > _tolerance(sv)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "changed_during_session": self.changed,
            "path": [s.to_dict() for s in self.segments],
            "at_session_close": self.session_value,
        }
        if self.post_close_shift:
            d["after_the_bell"] = self.post_close_value
            d["after_the_bell_note"] = (
                "post-bell value only — the day's 0DTE rolled off the chain after "
                "16:00 ET, so this level was NOT in play during the session"
            )
        return d


def _raw_runs(frames: Sequence[dict[str, Any]], key: str) -> list[WallSegment]:
    """Collapse the per-minute path into runs of an unchanged value."""
    runs: list[WallSegment] = []
    for row in frames:
        value = _to_float(row.get(key))
        if value is None:
            continue
        ts = row["timestamp"]
        if runs and abs(runs[-1].value - value) < 1e-6:
            runs[-1].end = ts
        else:
            runs.append(WallSegment(value=value, start=ts, end=ts))
    return runs


def _merge_adjacent(runs: list[WallSegment]) -> list[WallSegment]:
    """Fuse neighbouring runs that carry the same value (post-pruning)."""
    merged: list[WallSegment] = []
    for run in runs:
        if merged and abs(merged[-1].value - run.value) < 1e-6:
            merged[-1].end = run.end
        else:
            merged.append(run)
    return merged


def _prune_flicker(runs: list[WallSegment]) -> list[WallSegment]:
    """Drop sub-:data:`MIN_HOLD_MINUTES` runs, then re-merge.

    A wall bouncing between two near-tied strikes for a minute at a time is
    noise, not a migration — reporting it would bury the two or three moves
    that actually mattered.  The final run is always kept: it is the standing
    level the post quotes, however recently it formed."""
    if len(runs) <= 1:
        return runs
    kept = [r for r in runs[:-1] if r.minutes >= MIN_HOLD_MINUTES]
    kept.append(runs[-1])
    return _merge_adjacent(kept)


def _score_segment(segment: WallSegment, key: str, bars: Sequence[PriceBar]) -> None:
    """Decide what price did to ``segment`` during the window it was in force."""
    window = [b for b in bars if segment.start <= b.ts <= segment.end]
    if not window:
        segment.outcome = "unknown"
        return
    tol = _tolerance(segment.value)
    if key == "put_wall":
        # Support: the approach comes from above, the break goes below.
        extreme = min(b.low for b in window)
        broke = extreme < segment.value - tol
        tested = extreme <= segment.value + tol
    else:
        # Resistance: mirrored.
        extreme = max(b.high for b in window)
        broke = extreme > segment.value + tol
        tested = extreme >= segment.value - tol
    segment.outcome = "broke" if broke else ("held" if tested else "untested")
    segment.approach_pct = (extreme - segment.value) / segment.value * 100


def _build_wall_track(
    key: str,
    in_session: Sequence[dict[str, Any]],
    post_close: Sequence[dict[str, Any]],
    bars: Sequence[PriceBar],
) -> WallTrack | None:
    segments = _prune_flicker(_raw_runs(in_session, key))
    if not segments:
        return None
    for segment in segments:
        _score_segment(segment, key, bars)
    post_value: float | None = None
    for row in reversed(post_close):
        post_value = _to_float(row.get(key))
        if post_value is not None:
            break
    return WallTrack(key=key, segments=segments, post_close_value=post_value)


# ---------------------------------------------------------------------------
# Gamma-flip track
# ---------------------------------------------------------------------------


@dataclass
class FlipTrack:
    """The gamma flip's drift band and spot's relationship to it.

    The flip is a computed price off the spot-shift profile, not a strike, so
    it moves a little every cycle.  A chain of distinct values would be
    meaningless; what matters is how far it travelled and whether the tape
    ever got to the other side of it."""

    first: float
    last: float
    low: float
    high: float
    crossings: int = 0
    side_start: str | None = None  # "above" / "below"
    side_end: str | None = None
    post_close_value: float | None = None

    @property
    def drifted(self) -> bool:
        return (self.high - self.low) > _tolerance(self.last)

    @property
    def post_close_shift(self) -> bool:
        if self.post_close_value is None:
            return False
        return abs(self.post_close_value - self.last) > _tolerance(self.last)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "drifted_during_session": self.drifted,
            "opened_at": self.first,
            "at_session_close": self.last,
            "session_low": self.low,
            "session_high": self.high,
            "spot_side_at_open": self.side_start,
            "spot_side_at_close": self.side_end,
            "spot_crossings": self.crossings,
        }
        if self.post_close_shift:
            d["after_the_bell"] = self.post_close_value
            d["after_the_bell_note"] = (
                "post-bell value only — recomputed after the day's 0DTE rolled off"
            )
        return d


def _flip_at(frames: Sequence[tuple[datetime, float]], ts: datetime) -> float | None:
    """The flip value in force at ``ts`` (the last frame at-or-before it)."""
    value: float | None = None
    for frame_ts, frame_value in frames:
        if frame_ts > ts:
            break
        value = frame_value
    return value


def _build_flip_track(
    in_session: Sequence[dict[str, Any]],
    post_close: Sequence[dict[str, Any]],
    bars: Sequence[PriceBar],
) -> FlipTrack | None:
    path: list[tuple[datetime, float]] = []
    for row in in_session:
        value = _to_float(row.get(FLIP_KEY))
        if value is not None:
            path.append((row["timestamp"], value))
    if not path:
        return None
    values = [v for _, v in path]
    track = FlipTrack(
        first=values[0],
        last=values[-1],
        low=min(values),
        high=max(values),
    )
    for row in reversed(post_close):
        candidate = _to_float(row.get(FLIP_KEY))
        if candidate is not None:
            track.post_close_value = candidate
            break

    # Which side of the flip the tape sat on, minute by minute.  Only a close
    # BEYOND the touch band flips the recorded side, so a price grinding along
    # the flip doesn't register a dozen crossings.
    side: str | None = None
    for bar in bars:
        flip = _flip_at(path, bar.ts)
        if flip is None:
            continue
        tol = _tolerance(flip)
        if bar.close > flip + tol:
            new_side = "above"
        elif bar.close < flip - tol:
            new_side = "below"
        else:
            continue
        if side is None:
            side = new_side
            track.side_start = new_side
        elif new_side != side:
            side = new_side
            track.crossings += 1
    track.side_end = side
    return track


# ---------------------------------------------------------------------------
# The assembled history
# ---------------------------------------------------------------------------

# The summary columns the close read re-anchors to the last in-session frame.
SESSION_CLOSE_KEYS = ("put_wall", "call_wall", "gamma_flip", "max_pain", "net_gex_at_spot")


@dataclass
class LevelHistory:
    """Everything the write-up needs to describe how the levels moved."""

    symbol: str
    session_date: date
    mode: str
    put_wall: WallTrack | None = None
    call_wall: WallTrack | None = None
    gamma_flip: FlipTrack | None = None
    # Structure in force at the last in-session frame — what a close read
    # must quote, as opposed to whatever the post-bell chain now says.
    session_close_levels: dict[str, float] = field(default_factory=dict)
    post_close_levels: dict[str, float] = field(default_factory=dict)
    session_frames: int = 0
    last_session_ts: datetime | None = None
    last_post_close_ts: datetime | None = None

    def track(self, key: str) -> WallTrack | FlipTrack | None:
        if key == "put_wall":
            return self.put_wall
        if key == "call_wall":
            return self.call_wall
        if key == FLIP_KEY:
            return self.gamma_flip
        return None

    @property
    def any_change(self) -> bool:
        return any(
            [
                self.put_wall is not None and self.put_wall.changed,
                self.call_wall is not None and self.call_wall.changed,
                self.gamma_flip is not None and self.gamma_flip.drifted,
            ]
        )

    @property
    def post_close_shifted(self) -> bool:
        """True when the chain re-priced after the bell (0DTE roll-off)."""
        return any(
            [
                self.put_wall is not None and self.put_wall.post_close_shift,
                self.call_wall is not None and self.call_wall.post_close_shift,
                self.gamma_flip is not None and self.gamma_flip.post_close_shift,
            ]
        )

    def quoted_values(self) -> list[float]:
        """Every level value the history legitimately lets the prose cite.

        Feeds the LLM output validator: once the model is told the put wall
        walked 777 → 776 → 775, quoting 777 is a fact, not a hallucination."""
        out: list[float] = []
        for track in (self.put_wall, self.call_wall):
            if track is None:
                continue
            out.extend(track.values)
            if track.post_close_value is not None:
                out.append(track.post_close_value)
        if self.gamma_flip is not None:
            out.extend([self.gamma_flip.first, self.gamma_flip.last])
            if self.gamma_flip.post_close_value is not None:
                out.append(self.gamma_flip.post_close_value)
        return out

    def to_prompt_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "session_frames": self.session_frames,
            "levels_moved_during_session": self.any_change,
            "structure_as_of": _hhmm(self.last_session_ts),
        }
        if self.put_wall is not None:
            d["put_wall"] = self.put_wall.to_dict()
        if self.call_wall is not None:
            d["call_wall"] = self.call_wall.to_dict()
        if self.gamma_flip is not None:
            d["gamma_flip"] = self.gamma_flip.to_dict()
        if self.post_close_shifted:
            d["post_close_roll_off"] = (
                "The chain re-priced after the 16:00 bell as the day's 0DTE expiries "
                "rolled off. Any 'after_the_bell' value is TOMORROW's structure — it "
                "was never in play during the session, so never describe the tape as "
                "having reacted to it."
            )
        return d


def build_level_history(
    symbol: str,
    session_date: date,
    mode: str,
    level_rows: Iterable[dict[str, Any]] | None,
    price_rows: Iterable[dict[str, Any]] | None = None,
) -> LevelHistory | None:
    """Assemble a :class:`LevelHistory` from raw DB rows.

    ``level_rows`` are per-minute ``gex_summary`` rows for the day (see
    ``DatabaseManager.get_intraday_level_series``) — in-session and post-close
    alike; the split is done here.  ``price_rows`` are the underlying's 1-min
    bars for the same session, used to decide whether price actually tested
    each level while it was in force.  Without them every outcome degrades to
    ``unknown`` and the notes describe the migration only.

    Returns None when the day has too few frames to make an honest claim
    about the path — the caller then renders exactly as it did before."""
    in_session, post_close = _split_frames(level_rows, session_date)
    if len(in_session) < MIN_FRAMES:
        logger.debug(
            "level_history: %s %s has %d in-session frames (< %d) — skipping",
            symbol,
            session_date,
            len(in_session),
            MIN_FRAMES,
        )
        return None
    bars = _normalize_bars(price_rows)

    history = LevelHistory(
        symbol=symbol.upper(),
        session_date=session_date,
        mode=mode,
        session_frames=len(in_session),
        last_session_ts=in_session[-1]["timestamp"],
        last_post_close_ts=(post_close[-1]["timestamp"] if post_close else None),
    )
    for key in WALL_KEYS:
        setattr(history, key, _build_wall_track(key, in_session, post_close, bars))
    history.gamma_flip = _build_flip_track(in_session, post_close, bars)

    # The last in-session frame is the canonical "on the close" structure.
    # Walk backwards per column so a single NULL on the 16:00 row falls back
    # to the most recent frame that actually carried the value.
    for key in SESSION_CLOSE_KEYS:
        for row in reversed(in_session):
            value = _to_float(row.get(key))
            if value is not None:
                history.session_close_levels[key] = value
                break
        for row in reversed(post_close):
            value = _to_float(row.get(key))
            if value is not None:
                history.post_close_levels[key] = value
                break
    return history


# ---------------------------------------------------------------------------
# Copy — the Python-owned notes that annotate the tweet's Key-levels block
# ---------------------------------------------------------------------------


def _chain_text(values: Sequence[float]) -> str:
    """ "777 → 776 → 775", eliding the middle when the chain runs long."""
    if len(values) <= MAX_CHAIN:
        parts = [_fmt(v) for v in values]
    else:
        parts = [_fmt(values[0]), "…"] + [_fmt(v) for v in values[-(MAX_CHAIN - 1) :]]
    return " → ".join(parts)


_SINGLE_WALL_NOTE = {
    "broke": "broke",
    "held": "tested and held",
    "untested": "never tested",
}

_FINAL_WALL_NOTE = {
    "broke": "{v} gave way too",
    "held": "{v} tested and held",
    "untested": "{v} never tested",
}


def _wall_note(track: WallTrack) -> str:
    """The factual annotation for a wall in the Key-levels block.

    Unchanged wall → what price did to it ("tested and held").  Migrated wall
    → the chain plus the outcome of the moves ("moved 777 → 776 → 775; both
    broke, 775 tested and held")."""
    segments = track.segments
    if not segments:
        return ""
    final = segments[-1]
    if len(segments) == 1:
        return _SINGLE_WALL_NOTE.get(final.outcome, "")

    earlier = segments[:-1]
    broken = [s for s in earlier if s.outcome == "broke"]
    if broken and len(broken) == len(earlier):
        if len(earlier) == 1:
            broke_clause = "the first broke"
        elif len(earlier) == 2:
            broke_clause = "both broke"
        else:
            broke_clause = f"all {len(earlier)} broke"
    elif broken:
        broke_clause = f"{len(broken)} of {len(earlier)} broke"
    else:
        broke_clause = ""

    final_clause = _FINAL_WALL_NOTE.get(final.outcome, "").format(v=_fmt(final.value))
    tail = ", ".join(c for c in (broke_clause, final_clause) if c)
    chain = f"moved {_chain_text(track.values)}"
    return f"{chain}; {tail}" if tail else chain


def _flip_note(track: FlipTrack) -> str:
    """The factual annotation for the gamma flip."""
    parts: list[str] = []
    if track.drifted:
        parts.append(f"drifted {_fmt(track.low)}–{_fmt(track.high)}")
    if track.side_end is None:
        pass
    elif track.crossings == 0:
        parts.append(f"spot held {track.side_end} all session")
    elif track.crossings == 1:
        parts.append(f"spot crossed once, closed {track.side_end}")
    else:
        parts.append(f"spot crossed {track.crossings}x, closed {track.side_end}")
    return "; ".join(parts)


def note_for(history: LevelHistory | None, key: str) -> str:
    """Python-owned Key-levels note for ``key``, or "" when there's nothing.

    Takes precedence over the LLM's note: the model writes colour, but what
    a level DID is a fact the tape already settled."""
    if history is None:
        return ""
    track = history.track(key)
    if isinstance(track, WallTrack):
        return _wall_note(track)
    if isinstance(track, FlipTrack):
        return _flip_note(track)
    return ""


def post_close_line(history: LevelHistory | None) -> str:
    """The "after the bell" line for the close read, or "" when nothing moved.

    Without this, a 16:05 fire that quotes the session's real put wall is
    silently contradicted by the attached card (which renders the live, now
    post-roll-off chain).  Naming the reset makes the two agree and keeps the
    next session's structure clearly labelled as such."""
    if history is None or not history.post_close_shifted:
        return ""
    moves: list[str] = []
    for key, label in (("put_wall", "Put Wall"), ("call_wall", "Call Wall")):
        track = history.track(key)
        if isinstance(track, WallTrack) and track.post_close_shift:
            moves.append(f"{label} resets to {_fmt(track.post_close_value)}")
    flip = history.gamma_flip
    if isinstance(flip, FlipTrack) and flip.post_close_shift:
        moves.append(f"Gamma Flip resets to {_fmt(flip.post_close_value)}")
    if not moves:
        return ""
    return (
        "After the bell: today's 0DTE rolled off and "
        + ", ".join(moves)
        + ". That's tomorrow's map, not today's tape."
    )
