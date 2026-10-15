"""Wall-alert detection: hit, broke, held — with the chop taken out.

What a member asks for is "tell me when price gets to the call wall".  The
naive implementation — fire whenever price is within a few cents of the wall —
fires dozens of times a session, because a big strike is a magnet and a
barrier at once and price spends long stretches oscillating across it.  Every
one of those crossings is the same decision for a trader.  So this module
models a **test** of a wall, the unit ``src/analytics/wall_breaks.py`` already
uses for the published break odds, not a crossing::

    armed ──price reaches the touch band──▶ test ──N closes past the buffer──▶ BROKE (wall spent)
      ▲                                       ├──closes back off by the hold distance──▶ HELD
      │                                       └──resolution window lapses──────────────▶ (silent)
      └──a close clearly away from the wall

and turns each test into at most two alerts: the **hit** that opened it and
the **outcome** that closed it.

The noise controls, each answering one way an alert stream degrades:

* **Arming (hysteresis).**  A wall can only alert a hit after a minute closed
  at least the *arm distance* away on its approach side (below a call wall,
  above a put wall).  Price chopping inside that distance cannot re-arm it, so
  forty minutes pressed against a wall is one hit, not forty.
* **Cooldown.**  At most one hit alert per wall per ``cooldown_minutes``,
  however cleanly price backs off and returns in between.
* **Volatility-scaled distances.**  The arm, hold and break distances are
  multiples of the platform's own break buffer
  (:func:`src.analytics.walls.wall_break_buffer`: the larger of 0.1% of spot
  and 40% of the typical 30-minute range) — the same yardstick that decides
  when a wall changes sides on the chart.  A fixed cents figure reads the same
  on a quiet morning and a fast afternoon; this does not.
* **Confirmed breaks.**  A break is ``confirm_minutes`` consecutive 1-minute
  closes beyond the wall by the break buffer, and the run resets the moment a
  close falls back inside — the failed-breakout shape ZeroGEX's research
  found "often holds for the first ten or fifteen minutes before unwinding".
  A broken wall is spent for the session on that side.
* **Re-published walls start disarmed.**  When the walls are re-picked onto a
  strike price is already sitting at, that is a relabel, not an arrival; the
  new wall must see price back off before it can alert a hit.
* **Open grace.**  The first minutes of the cash session are an auction, not
  a test; hits there are not alerted (the test still opens, so a break that
  follows is).

Every touch of a published wall opens a test, armed or not; arming only
decides whether the *hit* is alerted.  That is what lets a wall that chopped
all morning still alert the afternoon it finally gives way.  The outcomes then
follow two rules a member can hold in their head:

* a **break** is alerted for every test.  It is a confirmed event that cannot
  chop, and no cooldown or grace period may swallow the one alert that says
  the wall gave way;
* a **hold** is alerted only for a test whose hit was alerted.  "Held" with no
  "hit" before it reads as nonsense, and a quiet period that suppressed the
  hit covers the bounce that follows it too.

Hits are checked on the in-progress minute (its running high/low), so an alert
goes out within seconds of the print; outcomes and arming use completed
closes only, so a single wick can neither confirm a break nor re-arm a wall.

**Touch tolerance.**  How close price must come for a hit to count is the
profile's touch band, unless the member chose their own
(:meth:`SensitivityProfile.with_touch_bp`).  Each tolerance in use is its own
detector stream, so one member's choice never changes what another receives.
A custom tolerance moves where a hit counts and, with it, how far price must
back off to re-arm or to count as a hold (both stay at least twice the band,
or a wide band would re-fire on itself); it never moves the break threshold,
which stays on the profile's own band.

**Explain.**  Setting :attr:`WallAlertDetector.trace` to a list makes the
detector record every decision in plain words (each wall it started watching,
each arming, each test and why its hit did or did not alert, each near miss
outside the band, each outcome).  ``wall_alert_engine --replay --explain``
prints it; live, ``trace`` stays ``None`` and costs nothing.

Everything here is pure: no database, no clock, no configuration.  The same
sequence of bars and walls always produces the same events, which is what lets
``wall_alert_engine --replay`` reproduce a past session exactly and lets the
service persist and restore its state across restarts.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from src.analytics.walls import wall_break_buffer

ET = ZoneInfo("America/New_York")

SIDE_CALL = "call"
SIDE_PUT = "put"
SIDES: Tuple[str, ...] = (SIDE_CALL, SIDE_PUT)

EVENT_HIT = "hit"
EVENT_BREAK = "break"
EVENT_HOLD = "hold"
EVENT_TYPES: Tuple[str, ...] = (EVENT_HIT, EVENT_BREAK, EVENT_HOLD)

#: Bumped whenever the serialized state shape changes; a stored state with a
#: different version is discarded (the service then warms up from the tape).
#: 2: states are per (profile, touch tolerance) stream and carry ``touch_bp``.
STATE_VERSION = 2


@dataclass(frozen=True)
class SensitivityProfile:
    """Every threshold one alert stream depends on, in one auditable place.

    Members pick a profile, not the numbers: the trade-off between alerting
    early and alerting cleanly is one dial, and the numbers behind each stop
    stay coherent with each other (a wider touch band with a shorter arm
    distance would re-fire on its own band).
    """

    name: str
    #: Touch band as a fraction of the wall (5 bp = ``level_history.TOUCH_PCT``).
    touch_pct: float
    #: Floor under the touch band so a low-priced symbol keeps one tick of room.
    touch_min: float
    #: How far (in break buffers) a close must be from the wall to arm it.
    arm_buffers: float
    #: Minimum minutes between two hit alerts on the same wall.
    cooldown_minutes: float
    #: Consecutive closes beyond the break buffer that confirm a break.
    confirm_minutes: int
    #: How far (in break buffers) price must close back off to confirm a hold.
    hold_buffers: float
    #: A test still unresolved after this long lapses without an alert.
    resolution_minutes: float
    #: No hit alerts this soon after the cash open.
    open_grace_minutes: float
    #: The profile's own touch band as a fraction, when a member's tolerance
    #: replaced ``touch_pct``; the break buffer's floor stays on it.
    break_floor_pct: Optional[float] = None

    def touch_band(self, wall: float) -> float:
        return max(abs(float(wall)) * self.touch_pct, self.touch_min)

    @property
    def touch_bp(self) -> int:
        """The touch tolerance in basis points (0.01% each): the stream's key."""
        return int(round(self.touch_pct * 10_000))

    def break_floor(self, wall: float) -> float:
        """Floor under the break buffer: the profile's OWN touch band.

        A member's tolerance changes where a hit counts, not what a break is,
        so a wide tolerance never makes a break harder to confirm.
        """
        pct = self.touch_pct if self.break_floor_pct is None else self.break_floor_pct
        return max(abs(float(wall)) * pct, self.touch_min)

    def with_touch_bp(self, touch_bp: int) -> "SensitivityProfile":
        """This profile with a member-chosen touch tolerance, in basis points."""
        if int(touch_bp) == self.touch_bp:
            return self
        base = self.touch_pct if self.break_floor_pct is None else self.break_floor_pct
        return replace(self, touch_pct=int(touch_bp) / 10_000, break_floor_pct=base)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "touch_pct": self.touch_pct,
            "touch_bp": self.touch_bp,
            "arm_buffers": self.arm_buffers,
            "cooldown_minutes": self.cooldown_minutes,
            "confirm_minutes": self.confirm_minutes,
            "hold_buffers": self.hold_buffers,
            "resolution_minutes": self.resolution_minutes,
            "open_grace_minutes": self.open_grace_minutes,
        }


PROFILES: Dict[str, SensitivityProfile] = {
    # Earliest warning: a wider band (8 bp) so the hit lands as price closes
    # in, quick re-arming, and breaks on three closes. More alerts, more of
    # them near-misses.
    "responsive": SensitivityProfile(
        name="responsive",
        touch_pct=0.0008,
        touch_min=0.01,
        arm_buffers=1.0,
        cooldown_minutes=10.0,
        confirm_minutes=3,
        hold_buffers=1.0,
        resolution_minutes=45.0,
        open_grace_minutes=0.0,
    ),
    # The default, and the published research's own definitions: a 5 bp touch
    # band (level_history.TOUCH_PCT) and five-minute break confirmation.
    "balanced": SensitivityProfile(
        name="balanced",
        touch_pct=0.0005,
        touch_min=0.01,
        arm_buffers=1.5,
        cooldown_minutes=20.0,
        confirm_minutes=5,
        hold_buffers=1.5,
        resolution_minutes=60.0,
        open_grace_minutes=3.0,
    ),
    # Fewest, cleanest alerts: price must really reach the wall (3 bp), back
    # well off before it can alert again, and a break needs ten closes — the
    # confirmation the wall-break odds are measured on
    # (level_history.MIN_HOLD_MINUTES).
    "conservative": SensitivityProfile(
        name="conservative",
        touch_pct=0.0003,
        touch_min=0.01,
        arm_buffers=2.0,
        cooldown_minutes=30.0,
        confirm_minutes=10,
        hold_buffers=2.0,
        resolution_minutes=60.0,
        open_grace_minutes=10.0,
    ),
}

DEFAULT_PROFILE = "balanced"

#: A detector stream: a profile name and the touch tolerance it runs at (bp).
StreamKey = Tuple[str, int]


def stream_profiles(touch_options_bp: Sequence[int]) -> Dict[StreamKey, SensitivityProfile]:
    """Every stream the service runs: each profile at its own touch band, and at
    each tolerance a member may choose instead.

    Keyed by the tolerance actually in effect, so "Balanced, Auto" and
    "Balanced, 0.05%" are one stream, not two that always agree.
    """
    out: Dict[StreamKey, SensitivityProfile] = {}
    for name, profile in PROFILES.items():
        out[(name, profile.touch_bp)] = profile
        for bp in touch_options_bp:
            variant = profile.with_touch_bp(int(bp))
            out.setdefault((name, variant.touch_bp), variant)
    return out


def stream_for(profile: str, touch_bp: Optional[int]) -> StreamKey:
    """The stream a rule listens to: its profile at its tolerance, or the
    profile's own band when the rule leaves the tolerance on Auto (None)."""
    base = PROFILES[profile]
    return (profile, base.touch_bp if touch_bp is None else int(touch_bp))


@dataclass(frozen=True)
class Bar:
    """One minute of the underlying, stamped at the START of the minute."""

    ts: datetime
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class WallsInForce:
    """The walls a member could see for one symbol at a given moment.

    One published ``gex_summary`` row reduced to what detection needs.
    ``break_buffer`` is that row's ``wall_break_buffer`` — the distance past a
    strike price must close before the platform moves the strike to the other
    side; ``None`` on rows written before the column existed.
    """

    ts: Optional[datetime]
    call_wall: Optional[float]
    put_wall: Optional[float]
    call_strength: Optional[float] = None
    put_strength: Optional[float] = None
    break_buffer: Optional[float] = None

    def wall(self, side: str) -> Optional[float]:
        value = self.call_wall if side == SIDE_CALL else self.put_wall
        return None if value is None else float(value)

    def strength(self, side: str) -> Optional[float]:
        value = self.call_strength if side == SIDE_CALL else self.put_strength
        return None if value is None else float(value)


@dataclass(frozen=True)
class WallEvent:
    """One alert-worthy moment in one wall test.

    ``price`` is the print that made the event: the high (call) or low (put)
    that reached the touch band for a hit, the confirming close for an
    outcome.  ``peak`` is the furthest price got toward the break side during
    the test.  ``alerted_hit`` records whether the test's hit was itself
    alerted (it is not during the open grace or a cooldown), so an outcome
    message can say "after the 10:14 hit" only when there was one.
    """

    symbol: str
    profile: str
    side: str
    kind: str
    wall: float
    ordinal: int
    session_date: date
    bar_ts: datetime
    price: float
    last_price: float
    hit_ts: datetime
    hit_price: float
    strength: Optional[float]
    walls_ts: Optional[datetime]
    break_buffer: float
    touch_band: float
    hold_distance: float
    confirm_minutes: int
    minutes_since_hit: float
    peak: float
    alerted_hit: bool
    #: Which genuine approach of this wall today the test was (1 = first), or
    #: ``None`` for a test that opened without one (a wall re-picked onto the
    #: price, or price that never left it). Distinct from ``ordinal``, which
    #: counts every test and keys the event.
    approach_number: Optional[int] = None
    #: The stream's touch tolerance in basis points: the profile's own, or the
    #: one a member chose. With ``profile`` it names the stream.
    touch_bp: int = 0

    @property
    def key(self) -> Tuple[str, str, int, str, float, int, str]:
        """Identity of the event within a session — the store's uniqueness key."""
        return (
            self.symbol,
            self.profile,
            self.touch_bp,
            self.side,
            self.wall,
            self.ordinal,
            self.kind,
        )


@dataclass(frozen=True)
class TraceEntry:
    """One decision the detector made, in words, for ``--explain`` replays."""

    ts: datetime
    side: str
    wall: float
    text: str


@dataclass
class _Test:
    ordinal: int
    hit_ts: datetime
    hit_price: float
    strength: Optional[float]
    walls_ts: Optional[datetime]
    break_buffer: float
    touch_band: float
    approached: bool
    alerted_hit: bool
    peak: float
    run: int = 0
    approach_number: Optional[int] = None


@dataclass
class _WallState:
    side: str
    strike: float
    armed: bool = False
    published: bool = False
    spent: bool = False
    tests: int = 0
    approaches: int = 0
    last_hit_alert_ts: Optional[datetime] = None
    test: Optional[_Test] = None


@dataclass
class _Clock:
    """Per-detector bookkeeping that is not about any one wall."""

    session_date: Optional[date] = None
    last_bar_ts: Optional[datetime] = None
    last_close: Optional[float] = None
    published: Dict[str, Optional[float]] = field(
        default_factory=lambda: {SIDE_CALL: None, SIDE_PUT: None}
    )


def session_date_for(ts: datetime) -> date:
    """ET trading date of a bar timestamp (naive timestamps are UTC)."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ET).date()


def _session_open(session: date) -> datetime:
    return datetime(session.year, session.month, session.day, 9, 30, tzinfo=ET)


def _round_strike(value: float) -> float:
    """Strikes are keys; round away float noise from NUMERIC → float casts."""
    return round(float(value), 4)


def break_buffer_for(walls: WallsInForce, reference_price: float, touch_band: float) -> float:
    """The break buffer to judge a test by.

    The published ``wall_break_buffer`` when the row carries one, else the
    same function's floor (0.1% of price) — never narrower than the touch
    band, or "tested" and "broke" would overlap.
    """
    published = walls.break_buffer
    if published is not None and published > 0:
        buffer = float(published)
    else:
        buffer = wall_break_buffer(reference_price, None)
    return max(buffer, touch_band)


class WallAlertDetector:
    """Wall tests for one symbol under one sensitivity profile.

    Feed bars in time order through :meth:`observe`.  A completed bar is
    applied exactly once (a repeat or older completed bar is ignored, so a
    caller may safely re-feed an overlapping window); the in-progress bar may
    be fed as often as it updates.
    """

    def __init__(self, symbol: str, profile: SensitivityProfile) -> None:
        self.symbol = symbol.upper()
        self.profile = profile
        self._clock = _Clock()
        self._walls: Dict[Tuple[str, float], _WallState] = {}
        #: Set to a list to record every decision (``--explain``); None live.
        self.trace: Optional[List[TraceEntry]] = None
        self._trace_inside: Dict[Tuple[str, float], bool] = {}
        self._trace_near: Dict[Tuple[str, float], Tuple[float, datetime, float, float]] = {}
        self._trace_disarm: Dict[Tuple[str, float], str] = {}

    # ------------------------------------------------------------------ API

    @property
    def session_date(self) -> Optional[date]:
        return self._clock.session_date

    @property
    def last_bar_ts(self) -> Optional[datetime]:
        """The last COMPLETED bar applied, or ``None`` today."""
        return self._clock.last_bar_ts

    def observe(self, bar: Bar, walls: WallsInForce, *, complete: bool) -> List[WallEvent]:
        """Apply one bar under the walls in force for it; return its events."""
        session = session_date_for(bar.ts)
        if session != self._clock.session_date:
            self._reset(session)
        if complete and self._clock.last_bar_ts is not None and bar.ts <= self._clock.last_bar_ts:
            return []

        self._sync_published(walls)
        events = self._check_hits(bar, walls)
        if complete:
            events.extend(self._resolve_tests(bar))
            self._update_arming(bar, walls)
            self._clock.last_bar_ts = bar.ts
            self._clock.last_close = float(bar.close)
        return events

    # ------------------------------------------------------------ internals

    def _reset(self, session: date) -> None:
        self._clock = _Clock(session_date=session)
        self._walls = {}
        self._trace_inside = {}
        self._trace_near = {}
        self._trace_disarm = {}

    def _state(self, side: str, strike: float) -> _WallState:
        key = (side, _round_strike(strike))
        state = self._walls.get(key)
        if state is None:
            state = _WallState(side=side, strike=key[1])
            self._walls[key] = state
        return state

    def _arm_distance(self, wall: float, buffer: float) -> float:
        return max(self.profile.arm_buffers * buffer, 2.0 * self.profile.touch_band(wall))

    def _hold_distance(self, test: _Test, wall: float) -> float:
        return max(self.profile.hold_buffers * test.break_buffer, 2.0 * test.touch_band)

    @staticmethod
    def _approach_distance(side: str, wall: float, price: float) -> float:
        """How far ``price`` sits on the wall's approach side (negative past it)."""
        return wall - price if side == SIDE_CALL else price - wall

    def _sync_published(self, walls: WallsInForce) -> None:
        """Track which strike is each side's wall; disarm walls that appear at price."""
        for side in SIDES:
            strike = walls.wall(side)
            strike = None if strike is None else _round_strike(strike)
            previous = self._clock.published.get(side)
            if strike == previous:
                continue
            if previous is not None:
                self._state(side, previous).published = False
            self._clock.published[side] = strike
            if strike is None:
                continue
            state = self._state(side, strike)
            state.published = True
            # A strike that becomes the wall is armed only if price is already
            # clearly away from it. Re-picking the walls onto a strike price is
            # sitting at is a relabel, not an arrival.
            last_close = self._clock.last_close
            if state.test is None and not state.spent and last_close is not None:
                buffer = break_buffer_for(walls, last_close, self.profile.break_floor(strike))
                arm = self._arm_distance(strike, buffer)
                gap = self._approach_distance(side, strike, last_close)
                state.armed = gap >= arm
            else:
                state.armed = False
            if self.trace is not None:
                self._trace_published(walls, state, previous, last_close)

    def _check_hits(self, bar: Bar, walls: WallsInForce) -> List[WallEvent]:
        events: List[WallEvent] = []
        session_open = _session_open(self._clock.session_date or session_date_for(bar.ts))
        grace_end = session_open + timedelta(minutes=self.profile.open_grace_minutes)
        in_grace = bar.ts < grace_end
        for side in SIDES:
            strike = self._clock.published.get(side)
            if strike is None:
                continue
            state = self._state(side, strike)
            band = self.profile.touch_band(strike)
            extreme = float(bar.high) if side == SIDE_CALL else float(bar.low)
            distance = self._approach_distance(side, strike, extreme)
            if self.trace is not None:
                self._trace_approach(bar, state, extreme, distance, band)
            if state.spent or state.test is not None:
                continue
            if distance > band:
                continue

            approached = state.armed
            cooling = state.last_hit_alert_ts is not None and bar.ts < (
                state.last_hit_alert_ts + timedelta(minutes=self.profile.cooldown_minutes)
            )
            alert = approached and not in_grace and not cooling
            buffer = break_buffer_for(walls, float(bar.close), self.profile.break_floor(strike))
            if self.trace is not None:
                self._trace_test(bar, state, extreme, distance, band, buffer, in_grace, grace_end)
            state.tests += 1
            if approached:
                state.approaches += 1
            state.armed = False
            state.test = _Test(
                ordinal=state.tests,
                hit_ts=bar.ts,
                hit_price=extreme,
                strength=walls.strength(side),
                walls_ts=walls.ts,
                break_buffer=buffer,
                touch_band=band,
                approached=approached,
                alerted_hit=alert,
                peak=extreme,
                approach_number=state.approaches if approached else None,
            )
            if alert:
                state.last_hit_alert_ts = bar.ts
                events.append(self._event(EVENT_HIT, state, bar, price=extreme))
        return events

    def _resolve_tests(self, bar: Bar) -> List[WallEvent]:
        events: List[WallEvent] = []
        close = float(bar.close)
        for state in self._walls.values():
            test = state.test
            if test is None or bar.ts < test.hit_ts:
                continue
            side, wall = state.side, state.strike
            extreme = float(bar.high) if side == SIDE_CALL else float(bar.low)
            if side == SIDE_CALL:
                test.peak = max(test.peak, extreme)
            else:
                test.peak = min(test.peak, extreme)

            past = -self._approach_distance(side, wall, close)
            test.run = test.run + 1 if past > test.break_buffer else 0
            if test.run >= self.profile.confirm_minutes:
                events.append(self._event(EVENT_BREAK, state, bar, price=close))
                if self.trace is not None:
                    self._note(
                        bar.ts,
                        state,
                        f"BROKE: test #{test.ordinal} confirmed by {test.run} closes more "
                        f"than {test.break_buffer:.2f} through the wall (close {close:.2f}) "
                        "-> break alert; the wall stays quiet for the rest of the day",
                    )
                state.test = None
                state.spent = True
                state.armed = False
                continue

            hold = self._hold_distance(test, wall)
            if self._approach_distance(side, wall, close) >= hold:
                if test.alerted_hit:
                    events.append(self._event(EVENT_HOLD, state, bar, price=close))
                if self.trace is not None:
                    self._note(
                        bar.ts,
                        state,
                        f"HELD: test #{test.ordinal} closed {close:.2f}, at least {hold:.2f} back "
                        "off the wall -> "
                        + (
                            "hold alert"
                            if test.alerted_hit
                            else "no alert (its hit was not alerted)"
                        ),
                    )
                state.test = None
                continue

            if bar.ts - test.hit_ts >= timedelta(minutes=self.profile.resolution_minutes):
                if self.trace is not None:
                    self._note(
                        bar.ts,
                        state,
                        f"test #{test.ordinal} lapsed after "
                        f"{self.profile.resolution_minutes:.0f} min with no break or hold",
                    )
                state.test = None
        return events

    def _update_arming(self, bar: Bar, walls: WallsInForce) -> None:
        close = float(bar.close)
        for side in SIDES:
            strike = self._clock.published.get(side)
            if strike is None:
                continue
            state = self._state(side, strike)
            if state.armed or state.spent or state.test is not None:
                continue
            buffer = break_buffer_for(walls, close, self.profile.break_floor(strike))
            arm = self._arm_distance(strike, buffer)
            gap = self._approach_distance(side, strike, close)
            if gap >= arm:
                state.armed = True
                if self.trace is not None:
                    self._trace_disarm.pop((side, strike), None)
                    self._note(
                        bar.ts,
                        state,
                        f"armed: close {close:.2f} is {gap:.2f} {_side_word(side)} the wall "
                        f"(needs {arm:.2f})",
                    )

    def _event(self, kind: str, state: _WallState, bar: Bar, *, price: float) -> WallEvent:
        test = state.test
        assert test is not None
        return WallEvent(
            symbol=self.symbol,
            profile=self.profile.name,
            side=state.side,
            kind=kind,
            wall=state.strike,
            ordinal=test.ordinal,
            session_date=self._clock.session_date or session_date_for(bar.ts),
            bar_ts=bar.ts,
            price=float(price),
            last_price=float(bar.close),
            hit_ts=test.hit_ts,
            hit_price=test.hit_price,
            strength=test.strength,
            walls_ts=test.walls_ts,
            break_buffer=test.break_buffer,
            touch_band=test.touch_band,
            hold_distance=self._hold_distance(test, state.strike),
            confirm_minutes=self.profile.confirm_minutes,
            minutes_since_hit=max((bar.ts - test.hit_ts).total_seconds() / 60.0, 0.0),
            peak=test.peak,
            alerted_hit=test.alerted_hit,
            approach_number=test.approach_number,
            touch_bp=self.profile.touch_bp,
        )

    # -------------------------------------------------------------- explain

    def flush_trace(self) -> None:
        """Record near misses still in progress when the tape ended."""
        if self.trace is None:
            return
        for key in list(self._trace_near):
            self._emit_near(key)

    def _note(self, ts: datetime, state: _WallState, text: str) -> None:
        assert self.trace is not None
        self.trace.append(TraceEntry(ts=ts, side=state.side, wall=state.strike, text=text))

    def _trace_published(
        self,
        walls: WallsInForce,
        state: _WallState,
        previous: Optional[float],
        last_close: Optional[float],
    ) -> None:
        moved = f" (was {previous:.2f})" if previous is not None else ""
        side, strike = state.side, state.strike
        if state.spent:
            status = "it already broke today, so it stays quiet"
        elif state.test is not None:
            status = f"test #{state.test.ordinal} on it is still open"
        elif last_close is None:
            status = "not armed until a completed minute closes clearly away from it"
            self._trace_disarm[(side, strike)] = (
                f"since it became the wall at {_hhmm(walls.ts)}"
                if walls.ts
                else "since it became the wall"
            )
        elif state.armed:
            status = "armed"
        else:
            buffer = break_buffer_for(walls, last_close, self.profile.break_floor(strike))
            arm = self._arm_distance(strike, buffer)
            gap = self._approach_distance(side, strike, last_close)
            status = (
                f"not armed: price is {gap:.2f} from it and a close {arm:.2f} "
                f"{_side_word(side)} arms it"
            )
            self._trace_disarm[(side, strike)] = (
                f"since it became the wall at {_hhmm(self._clock.last_bar_ts)}"
                if self._clock.last_bar_ts
                else "since it became the wall"
            )
        ts = self._clock.last_bar_ts or walls.ts
        if ts is None:
            return
        self.trace.append(  # type: ignore[union-attr]
            TraceEntry(
                ts=ts,
                side=side,
                wall=strike,
                text=f"{side} wall is now {strike:.2f}{moved}; {status}",
            )
        )

    def _trace_approach(
        self, bar: Bar, state: _WallState, extreme: float, distance: float, band: float
    ) -> None:
        """Near misses outside the band, and touches a test or a break blocked."""
        key = (state.side, state.strike)
        inside = distance <= band
        was_inside = self._trace_inside.get(key, False)
        self._trace_inside[key] = inside
        if inside:
            # An approach that reached the band is a test (or a blocked one).
            self._trace_near.pop(key, None)
            if was_inside:
                return
            if state.spent:
                self._note(
                    bar.ts,
                    state,
                    f"{_extreme_word(state.side)} {extreme:.2f} is within the touch band, but "
                    "this wall already broke today and stays quiet",
                )
            elif state.test is not None:
                self._note(
                    bar.ts,
                    state,
                    f"{_extreme_word(state.side)} {extreme:.2f} is back within the touch band "
                    f"while test #{state.test.ordinal} (opened "
                    f"{_hhmm(state.test.hit_ts)}) is still open: no new test until that one "
                    "breaks, holds or lapses",
                )
            return
        if distance <= 3.0 * band:
            near = self._trace_near.get(key)
            if near is None or distance < near[0]:
                self._trace_near[key] = (distance, bar.ts, extreme, band)
            return
        self._emit_near(key)

    def _emit_near(self, key: Tuple[str, float]) -> None:
        near = self._trace_near.pop(key, None)
        state = self._walls.get(key)
        if near is None or state is None:
            return
        distance, ts, extreme, band = near
        self._note(
            ts,
            state,
            f"near miss: {_extreme_word(state.side)} {extreme:.2f} stopped {distance:.2f} "
            f"{_side_word(state.side)} the wall, outside the {band:.2f} touch band "
            f"({self.profile.touch_bp / 100:.2f}%)",
        )

    def _trace_test(
        self,
        bar: Bar,
        state: _WallState,
        extreme: float,
        distance: float,
        band: float,
        buffer: float,
        in_grace: bool,
        grace_end: datetime,
    ) -> None:
        side, strike = state.side, state.strike
        if distance > 0:
            where = f"{distance:.2f} {_side_word(side)} the wall"
        elif distance == 0:
            where = "at the wall"
        else:
            where = f"{-distance:.2f} through the wall"
        reasons: List[str] = []
        if not state.armed:
            arm = self._arm_distance(strike, buffer)
            why = self._trace_disarm.get((side, strike), "since the session began")
            reasons.append(
                f"not armed: no 1-minute close at least {arm:.2f} {_side_word(side)} "
                f"the wall {why}"
            )
        if in_grace:
            reasons.append(f"opening grace until {_hhmm(grace_end)}")
        if state.last_hit_alert_ts is not None and bar.ts < (
            state.last_hit_alert_ts + timedelta(minutes=self.profile.cooldown_minutes)
        ):
            reasons.append(
                f"cooldown: last hit alert on this wall at {_hhmm(state.last_hit_alert_ts)}, "
                f"{self.profile.cooldown_minutes:.0f} min between hits"
            )
        outcome = "hit alert" if not reasons else "no hit alert: " + "; ".join(reasons)
        self._note(
            bar.ts,
            state,
            f"TEST #{state.tests + 1}: {_extreme_word(side)} {extreme:.2f} is {where} "
            f"(touch band {band:.2f}) -> {outcome}",
        )
        self._trace_disarm[(side, strike)] = f"since test #{state.tests + 1} at {_hhmm(bar.ts)}"

    # -------------------------------------------------------- serialization

    def to_state(self) -> Dict[str, Any]:
        """JSON-safe snapshot, restorable with :meth:`restore`."""
        clock = self._clock
        return {
            "version": STATE_VERSION,
            "symbol": self.symbol,
            "profile": self.profile.name,
            "touch_bp": self.profile.touch_bp,
            "session_date": clock.session_date.isoformat() if clock.session_date else None,
            "last_bar_ts": _iso(clock.last_bar_ts),
            "last_close": clock.last_close,
            "published": dict(clock.published),
            "walls": [_wall_state_to_dict(s) for s in self._walls.values()],
        }

    def restore(self, state: Dict[str, Any]) -> bool:
        """Load a :meth:`to_state` snapshot; ``False`` (and no change) if unusable."""
        try:
            if int(state.get("version", 0)) != STATE_VERSION:
                return False
            if state.get("profile") != self.profile.name:
                return False
            if int(state.get("touch_bp", -1)) != self.profile.touch_bp:
                return False
            session = date.fromisoformat(state["session_date"])
            clock = _Clock(
                session_date=session,
                last_bar_ts=_parse_ts(state.get("last_bar_ts")),
                last_close=_opt_float(state.get("last_close")),
                published={
                    side: _opt_float((state.get("published") or {}).get(side)) for side in SIDES
                },
            )
            walls: Dict[Tuple[str, float], _WallState] = {}
            for raw in state.get("walls") or []:
                ws = _wall_state_from_dict(raw)
                walls[(ws.side, ws.strike)] = ws
        except (KeyError, TypeError, ValueError):
            return False
        self._clock = clock
        self._walls = walls
        return True


def _hhmm(ts: datetime) -> str:
    """Wall-clock ET minute of a bar timestamp, for explain lines."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(ET).strftime("%H:%M")


def _side_word(side: str) -> str:
    """Where price sits on a wall's approach side: below a call wall, above a put wall."""
    return "below" if side == SIDE_CALL else "above"


def _extreme_word(side: str) -> str:
    return "high" if side == SIDE_CALL else "low"


def _iso(ts: Optional[datetime]) -> Optional[str]:
    return ts.isoformat() if ts is not None else None


def _parse_ts(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    ts = datetime.fromisoformat(str(raw))
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


def _opt_float(raw: Any) -> Optional[float]:
    return None if raw is None else float(raw)


def _wall_state_to_dict(state: _WallState) -> Dict[str, Any]:
    test = state.test
    return {
        "side": state.side,
        "strike": state.strike,
        "armed": state.armed,
        "published": state.published,
        "spent": state.spent,
        "tests": state.tests,
        "approaches": state.approaches,
        "last_hit_alert_ts": _iso(state.last_hit_alert_ts),
        "test": (
            None
            if test is None
            else {
                "ordinal": test.ordinal,
                "hit_ts": _iso(test.hit_ts),
                "hit_price": test.hit_price,
                "strength": test.strength,
                "walls_ts": _iso(test.walls_ts),
                "break_buffer": test.break_buffer,
                "touch_band": test.touch_band,
                "approached": test.approached,
                "alerted_hit": test.alerted_hit,
                "peak": test.peak,
                "run": test.run,
                "approach_number": test.approach_number,
            }
        ),
    }


def _wall_state_from_dict(raw: Dict[str, Any]) -> _WallState:
    side = str(raw["side"])
    if side not in SIDES:
        raise ValueError(f"unknown side {side!r}")
    test_raw = raw.get("test")
    test = None
    if test_raw:
        hit_ts = _parse_ts(test_raw["hit_ts"])
        if hit_ts is None:
            raise ValueError("test without hit_ts")
        test = _Test(
            ordinal=int(test_raw["ordinal"]),
            hit_ts=hit_ts,
            hit_price=float(test_raw["hit_price"]),
            strength=_opt_float(test_raw.get("strength")),
            walls_ts=_parse_ts(test_raw.get("walls_ts")),
            break_buffer=float(test_raw["break_buffer"]),
            touch_band=float(test_raw["touch_band"]),
            approached=bool(test_raw["approached"]),
            alerted_hit=bool(test_raw["alerted_hit"]),
            peak=float(test_raw["peak"]),
            run=int(test_raw.get("run") or 0),
            approach_number=(
                None
                if test_raw.get("approach_number") is None
                else int(test_raw["approach_number"])
            ),
        )
    return _WallState(
        side=side,
        strike=_round_strike(raw["strike"]),
        armed=bool(raw.get("armed")),
        published=bool(raw.get("published")),
        spent=bool(raw.get("spent")),
        tests=int(raw.get("tests") or 0),
        approaches=int(raw.get("approaches") or 0),
        last_hit_alert_ts=_parse_ts(raw.get("last_hit_alert_ts")),
        test=test,
    )
