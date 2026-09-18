"""Gamma Weather -- one current-state read from the pieces already on screen.

Phase 1 of Barrie's spec. The panel exists to remove mental assembly, not to
add another indicator: every input below is already rendered somewhere on the
Hedging Flow page, and the job here is to say what they amount to together.

This is a MARKET-HEALTH CLASSIFICATION. It is not a directional signal, an
entry or exit, or a recommendation, and the vocabulary is chosen to describe
conditions rather than to forecast outcomes. It also inherits the estimated,
not observed caveat from the hedging flow it reads (see
:mod:`src.analytics.hedging_flow`), and nothing here upgrades that.

--------------------------------------------------------------------------
Precedence, which is the part the spec left open
--------------------------------------------------------------------------
Pressure, lean, stability and cushion give roughly four dozen combinations
mapping onto a handful of states, and without a rule for what wins, "mixed"
silently becomes the most common reading and the panel says nothing all day.
Two decisions fix that:

1. **Cushion is a MODIFIER, not a peer state.** The spec lists "transition
   risk" alongside "stable bid", but they are answers to different questions:
   one is about the condition, the other about how close that condition is to
   ending. As peers they compete and one has to be suppressed. As a state plus
   a qualifier they compose -- "stable bid, cushion thinning" -- which is also
   how a trader would actually say it.

2. **Stability decides the state; lean colors it.** The spec's own examples
   have lean and stability agreeing (supportive + pinning, capping +
   accelerative), so it does not say what happens when they disagree, and they
   disagree often. Stability wins because the question the panel answers is
   whether a condition can persist, and stability is what says whether moves
   get damped or amplified. Lean is directional flavor and goes in the
   sentence. This keeps every combination covered without inventing a dozen
   new state names, and leaves MIXED meaning what the spec says it means:
   the inputs genuinely disagree.

Every threshold lives in one block below so tuning is a single edit and never
a logic change. Nothing here is stored: states are derived on read from the
components, so retuning a threshold reclassifies the whole archive instead of
leaving old sessions labelled by a rule that is no longer live. That is also
what makes an honest base-rate comparison possible later.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, List, Mapping, Optional, Sequence

from src.analytics.flip_cushion import DEFAULT_RATE_BARS
from src.analytics.flip_cushion import build_series as build_cushion_series
from src.analytics.flip_cushion import (
    STATE_CROSSING,
    STATE_NO_FLIP,
    STATE_THIN,
    CushionBar,
)
from src.analytics.hedging_flow import DEFAULT_SMOOTHING_BARS, smooth

# --------------------------------------------------------------------------- #
# Tunables. Everything adjustable lives here; the logic below reads them.
# --------------------------------------------------------------------------- #

#: |pressure| at or below which a bar is not pointing anywhere. Without a floor
#: a few dollars of imbalance would read as a direction on a dead tape.
PRESSURE_FLOOR_USD = 25_000_000.0

#: |stability| at or below which the book is neither firming nor thinning.
#: Flat is grouped WITH pinning for the state, because the question is whether
#: a condition can persist and a book that is not degrading is closer to
#: stable than to fragile. The sentence still says "flat" rather than
#: "pinning", so the reading is never overstated.
STABILITY_FLAT_BAND_USD = 50_000_000.0

#: Pressure persistence ladder, in completed 5-minute bars. One bar is a
#: pulse; two of the last three plus an aligned average is building; three is
#: persistent enough to lean on. A pulse is not nothing -- it is the first
#: evidence -- but it is not yet a condition.
PERSISTENCE_WINDOW_BARS = 3
PERSISTENCE_BUILDING_BARS = 2
PERSISTENCE_PERSISTENT_BARS = 3

#: State-age thresholds, in minutes. Duration is the thing being studied here:
#: not whether gamma calls direction, but whether a condition that exists is
#: healthy enough to persist.
AGE_ESTABLISHED_MIN = 15
AGE_CONFIRMED_MIN = 30
AGE_MATURE_MIN = 60

#: Completed bars a NEW state must repeat before it takes the header.
#:
#: Not cosmetic. Measured over seven sessions, the unconfirmed classifier
#: changed state roughly every 8 minutes: 347 runs with a median life of one
#: bar, and outside a single instance no state survived 30 minutes. Requiring
#: two bars cuts that to 94 runs, moves the median state life to 20-25
#: minutes, and lifts 30-minute survival from 0.3% to about 30%, for a median
#: 5 minutes of lateness. See src/tools/gamma_weather_base_rates.py, which
#: measures both sides of that trade.
#:
#: Two rather than three because it is the number already agreed with Barrie
#: for flip dots, not because seven sessions chose it. Re-run the report as
#: history accumulates before moving it.
CONFIRM_BARS = 2

#: Share of the current cushion that the trailing window must have given up
#: for the narrowing to count as a transition risk rather than drift. A ratio
#: of points to points, so it is scale free across symbols.
TRANSITION_RATE_SHARE = 0.25

# --------------------------------------------------------------------------- #

# Two ladders that answer different questions, so they share no words. They
# used to: both ran DEVELOPING -> ESTABLISHED, which made "established" mean
# either a settled pressure leg or a state old enough to trust, and a reader
# could not tell which from the value alone. Barrie caught it from the live
# panel and picked the split below. The codes are distinct too, not just the
# display names, because the API emits both fields side by side.
PERSISTENCE_PULSE = "PULSE"
PERSISTENCE_BUILDING = "BUILDING"
PERSISTENCE_PERSISTENT = "PERSISTENT"

AGE_NEW = "NEW"
AGE_ESTABLISHED = "ESTABLISHED"
AGE_CONFIRMED = "CONFIRMED"
AGE_MATURE = "MATURE"

PRESSURE_BUYING = "BUYING"
PRESSURE_SELLING = "SELLING"
PRESSURE_MIXED = "MIXED"

STRUCTURE_PINNING = "PINNING"
STRUCTURE_ACCELERATIVE = "ACCELERATIVE"
STRUCTURE_FLAT = "FLAT"

LEAN_SUPPORTIVE = "SUPPORTIVE"
LEAN_CAPPING = "CAPPING"

STATE_STABLE_BID = "STABLE_BID"
STATE_SUPPORTED_DIP = "SUPPORTED_DIP"
STATE_FRAGILE_RALLY = "FRAGILE_RALLY"
STATE_UNSTABLE = "UNSTABLE"
STATE_MIXED = "MIXED"

CUSHION_TRANSITION_RISK = "TRANSITION_RISK"
CUSHION_NARROWING = "NARROWING"
CUSHION_WIDENING = "WIDENING"
CUSHION_STEADY = "STEADY"
CUSHION_NONE = "NONE"

#: Display names, kept apart from the codes so the UI wording can change
#: without touching anything that classifies or is stored in a log.
STATE_LABELS = {
    STATE_STABLE_BID: "Stable bid",
    STATE_SUPPORTED_DIP: "Supported dip",
    STATE_FRAGILE_RALLY: "Fragile rally",
    STATE_UNSTABLE: "Unstable",
    STATE_MIXED: "Mixed",
}

PERSISTENCE_LABELS = {
    PERSISTENCE_PULSE: "Pulse",
    PERSISTENCE_BUILDING: "Building",
    PERSISTENCE_PERSISTENT: "Persistent",
}

AGE_LABELS = {
    AGE_NEW: "New",
    AGE_ESTABLISHED: "Established",
    AGE_CONFIRMED: "Confirmed",
    AGE_MATURE: "Mature",
}


@dataclass(frozen=True)
class WeatherInputs:
    """The per-bar components, all of which are already on the page.

    ``pressure_bar`` and ``pressure_avg`` are this bar's hedging pressure and
    its three-bar average. ``lean`` and ``stability`` are the rolling
    structure scores. ``gamma_trend`` is the since-open stability, which the
    spec calls background health. The cushion fields come from
    :mod:`src.analytics.flip_cushion`.
    """

    pressure_bar: Optional[float] = None
    pressure_avg: Optional[float] = None
    lean: Optional[float] = None
    stability: Optional[float] = None
    gamma_trend: Optional[float] = None
    cushion_state: Optional[str] = None
    cushion_pts: Optional[float] = None
    cushion_rate_pts: Optional[float] = None


@dataclass(frozen=True)
class Weather:
    """The classified read, with the components that produced it.

    The components are returned alongside the state on purpose. A panel that
    shows only a verdict cannot be checked, and this one is meant to be
    auditable against the charts directly below it.
    """

    state: str
    label: str
    pressure: str
    structure: str
    #: Since-open stability, classified the same way as ``structure``. The
    #: spec calls this background health: where structure says what the book
    #: is doing right now, this says where the session has migrated to.
    gamma_trend: str
    lean_side: Optional[str]
    cushion: str
    sentence: str
    #: How settled the pressure direction is: PULSE / BUILDING / PERSISTENT.
    #: A pulse is the first evidence, not yet a condition.
    persistence: str = PERSISTENCE_PULSE
    #: How long this state has held: NEW / ESTABLISHED / CONFIRMED / MATURE.
    #: Bars rather than a stored timestamp, because states are derived on read
    #: and a retuned threshold must re-age history as well as re-label it.
    age_bars: int = 0
    age_minutes: Optional[float] = None
    age: Optional[str] = None
    #: Display wording for the two ladders, carried alongside the codes for
    #: the same reason ``label`` sits beside ``state``: one source of truth.
    #: The panel used to keep its own copy of both maps, which is precisely
    #: what a rename like this one silently breaks.
    persistence_label: str = PERSISTENCE_LABELS[PERSISTENCE_PULSE]
    age_label: Optional[str] = None
    #: The state this bar would read without confirmation, when that differs
    #: from the one holding the header. Carried rather than suppressed: the
    #: whole point of confirming is to stop the header chasing noise, and the
    #: whole point of showing the candidate anyway is that the early read is
    #: information. Barrie's words for the same idea on flip dots -- show it
    #: immediately as unconfirmed, upgrade it when it holds.
    pending_state: Optional[str] = None
    pending_label: Optional[str] = None
    #: Completed bars the candidate has held, 1 .. CONFIRM_BARS - 1.
    pending_bars: int = 0


def classify_pressure(bar: Optional[float], avg: Optional[float]) -> str:
    """Buying, selling, or mixed.

    Mixed when the current bar and the three-bar average point different ways,
    which is the honest reading of a turn in progress: something is happening
    but it has not established yet. Also mixed when either is missing or both
    sit inside the floor.
    """
    if bar is None or avg is None:
        return PRESSURE_MIXED
    if abs(bar) <= PRESSURE_FLOOR_USD and abs(avg) <= PRESSURE_FLOOR_USD:
        return PRESSURE_MIXED
    if bar > 0 and avg > 0:
        return PRESSURE_BUYING
    if bar < 0 and avg < 0:
        return PRESSURE_SELLING
    return PRESSURE_MIXED


def classify_structure(stability: Optional[float]) -> str:
    """Pinning, accelerative, or flat."""
    if stability is None:
        return STRUCTURE_FLAT
    if stability > STABILITY_FLAT_BAND_USD:
        return STRUCTURE_PINNING
    if stability < -STABILITY_FLAT_BAND_USD:
        return STRUCTURE_ACCELERATIVE
    return STRUCTURE_FLAT


def classify_lean(lean: Optional[float]) -> Optional[str]:
    if lean is None:
        return None
    return LEAN_SUPPORTIVE if lean >= 0 else LEAN_CAPPING


def classify_cushion(
    cushion_state: Optional[str],
    cushion_pts: Optional[float],
    rate_pts: Optional[float],
) -> str:
    """The modifier: how close this condition is to ending.

    Transition risk needs BOTH proximity and speed. A thin cushion sitting
    still is a fact about where price is, not a warning, and a fast-narrowing
    cushion with plenty of room left is just movement. The spec's own wording
    is "thin and contracting quickly", and this is that, with "quickly"
    expressed as a share of the room remaining so it means the same thing on
    every symbol.
    """
    if cushion_state is None or cushion_state == STATE_NO_FLIP:
        return CUSHION_NONE
    if rate_pts is None:
        return CUSHION_STEADY

    narrowing = rate_pts < 0
    if narrowing and cushion_state in (STATE_THIN, STATE_CROSSING):
        if cushion_pts and abs(rate_pts) >= TRANSITION_RATE_SHARE * cushion_pts:
            return CUSHION_TRANSITION_RISK
    if narrowing:
        return CUSHION_NARROWING
    if rate_pts > 0:
        return CUSHION_WIDENING
    return CUSHION_STEADY


def classify_persistence(
    recent_bars: Sequence[Optional[float]],
    avg: Optional[float],
    direction: str,
) -> str:
    """How settled the pressure direction is, on Barrie's ladder.

    ``recent_bars`` is chronological with the current bar last. A bar counts as
    aligned when it points the same way as ``direction`` and clears the floor;
    bars inside the floor are not evidence either way rather than evidence
    against, so they simply do not count.

    Mixed pressure has no direction to be persistent about, so it is always a
    pulse. That is not a hedge: it is the honest reading of a turn that has not
    established.
    """
    if direction == PRESSURE_MIXED:
        return PERSISTENCE_PULSE

    want = 1.0 if direction == PRESSURE_BUYING else -1.0
    window = list(recent_bars)[-PERSISTENCE_WINDOW_BARS:]
    aligned = sum(
        1 for v in window if v is not None and abs(v) > PRESSURE_FLOOR_USD and (v > 0) == (want > 0)
    )

    if aligned >= PERSISTENCE_PERSISTENT_BARS:
        return PERSISTENCE_PERSISTENT

    avg_aligned = avg is not None and abs(avg) > PRESSURE_FLOOR_USD and (avg > 0) == (want > 0)
    if aligned >= PERSISTENCE_BUILDING_BARS and avg_aligned:
        return PERSISTENCE_BUILDING

    return PERSISTENCE_PULSE


def classify_age(minutes: Optional[float]) -> Optional[str]:
    """How long the current state has held, as a word.

    Bands are Barrie's. New covers everything below the established line,
    which absorbs the gap between his "under 10 minutes is provisional" and
    "15 minutes is established" -- a state at 12 minutes is not yet
    established, and calling it anything else would overstate it.
    """
    if minutes is None:
        return None
    if minutes >= AGE_MATURE_MIN:
        return AGE_MATURE
    if minutes >= AGE_CONFIRMED_MIN:
        return AGE_CONFIRMED
    if minutes >= AGE_ESTABLISHED_MIN:
        return AGE_ESTABLISHED
    return AGE_NEW


def state_age_bars(states: Sequence[str]) -> int:
    """How many consecutive trailing bars share the newest state.

    Counts backward from the end, so a state that has held all session and one
    that just formed are distinguishable. Returns 0 for an empty series.

    Deliberately counts BARS rather than reading a stored timestamp: states are
    derived on read, so a retuned threshold has to re-age history as well as
    re-label it. An age carried from a stored row would survive a change it
    should not survive.
    """
    if not states:
        return 0
    newest = states[-1]
    count = 0
    for value in reversed(states):
        if value != newest:
            break
        count += 1
    return count


def _state_for(pressure: str, structure: str) -> str:
    """Pressure crossed with structure. Covers every combination.

    Flat structure groups with pinning (see STABILITY_FLAT_BAND_USD); the
    sentence keeps the distinction so nothing is overstated.
    """
    if pressure == PRESSURE_MIXED:
        return STATE_MIXED
    settled = structure in (STRUCTURE_PINNING, STRUCTURE_FLAT)
    if pressure == PRESSURE_BUYING:
        return STATE_STABLE_BID if settled else STATE_FRAGILE_RALLY
    return STATE_SUPPORTED_DIP if settled else STATE_UNSTABLE


def _sentence(
    state: str,
    pressure: str,
    structure: str,
    lean_side: Optional[str],
    cushion: str,
    pending: Optional[str] = None,
) -> str:
    """One plain sentence: what the tape is doing, what the book is doing, and
    how much room is left. Conditions, never outcomes.

    ``pending`` is not decoration. Once the header is confirmed, it can name a
    condition the components no longer support: "Stable bid" over a bar whose
    pressure reads selling. That looks like a bug and is not one, so when a
    different state is waiting on confirmation the sentence says so, and the
    reader gets the reason the components and the header disagree instead of
    being left to wonder.
    """
    push = {
        PRESSURE_BUYING: "Hedging pressure is buying",
        PRESSURE_SELLING: "Hedging pressure is selling",
        PRESSURE_MIXED: "Hedging pressure is mixed",
    }[pressure]

    book = {
        STRUCTURE_PINNING: "gamma near price is building",
        STRUCTURE_ACCELERATIVE: "gamma near price is thinning",
        STRUCTURE_FLAT: "gamma near price is flat",
    }[structure]

    if lean_side == LEAN_SUPPORTIVE:
        book += " and leaning supportive"
    elif lean_side == LEAN_CAPPING:
        book += " and leaning capping"

    room = {
        CUSHION_TRANSITION_RISK: "the flip cushion is thin and closing quickly",
        CUSHION_NARROWING: "the flip cushion is narrowing",
        CUSHION_WIDENING: "the flip cushion is widening",
        CUSHION_STEADY: "the flip cushion is steady",
        CUSHION_NONE: "there is no gamma flip in the profile",
    }[cushion]

    line = f"{STATE_LABELS[state]}. {push}, {book}, and {room}."
    if pending and pending != state:
        line += f" {STATE_LABELS[pending]} is forming, not yet confirmed."
    return line


class _Confirmation:
    """The hold-before-you-change rule, as a state machine over one session.

    Kept here, in one place, because production and the base-rate report have
    to agree on it exactly. A report that measured a lookalike of this rule
    would produce numbers about a panel nobody runs, which is the failure the
    whole tool exists to prevent.

    Strictly causal: each bar is decided from bars at or before it, never from
    what came next. That is what lets a completed session be replayed and give
    the same headline the panel showed live, and what makes the report's
    comparison of confirmed against raw legitimate rather than flattered by
    hindsight.
    """

    def __init__(self, confirm_bars: int) -> None:
        self.confirm_bars = confirm_bars
        self.headline: Optional[str] = None
        self.candidate: Optional[str] = None
        self.streak = 0

    def push(self, raw: str) -> tuple:
        """Feed one bar's raw state; get ``(headline, pending, pending_bars)``.

        The first bar of a session takes the header immediately. There is
        nothing for it to be confirmed against, and withholding a headline for
        the first ten minutes of every session would be a worse read than the
        one bar of noise it avoids.
        """
        if self.headline is None or self.confirm_bars <= 1:
            self.headline = raw
            self.candidate, self.streak = None, 0
            return self.headline, None, 0

        if raw == self.headline:
            self.candidate, self.streak = None, 0
            return self.headline, None, 0

        if raw == self.candidate:
            self.streak += 1
        else:
            self.candidate, self.streak = raw, 1

        if self.streak >= self.confirm_bars:
            self.headline = raw
            self.candidate, self.streak = None, 0
            return self.headline, None, 0

        return self.headline, self.candidate, self.streak


def classify_series(
    inputs: Sequence[WeatherInputs],
    bar_minutes: float = 5.0,
    confirm_bars: int = CONFIRM_BARS,
) -> List[Weather]:
    """Classify a chronological run of bars, as the panel reads them.

    The per-bar :func:`classify` cannot see history, so it reports every bar
    as an unconfirmed pulse of unknown age. This is the form the panel wants:
    one walk of the session that fills confirmation, persistence and age.

    ``state`` is the CONFIRMED headline, not this bar's raw read. A state that
    has just appeared is carried on ``pending_state`` until it holds
    ``confirm_bars`` bars, so the early information is visible without the
    header chasing it. ``confirm_bars`` of 1 disables confirmation entirely,
    which is how the base-rate report measures what confirming is worth.

    Age counts consecutive bars sharing the CONFIRMED state. Measuring it on
    the raw series would reset the clock on every one-bar flicker and make the
    age ladder unreachable, which is exactly what it did before this rule
    existed.
    """
    out: List[Weather] = []
    states: List[str] = []
    pressures: List[Optional[float]] = []
    confirmation = _Confirmation(confirm_bars)

    for row in inputs:
        base = classify(row)
        pressures.append(row.pressure_bar)

        headline, pending, pending_bars = confirmation.push(base.state)
        states.append(headline)

        persistence = classify_persistence(pressures, row.pressure_avg, base.pressure)
        age_bars = state_age_bars(states)
        age_minutes = age_bars * bar_minutes
        age = classify_age(age_minutes)

        out.append(
            replace(
                base,
                state=headline,
                label=STATE_LABELS[headline],
                sentence=_sentence(
                    headline,
                    base.pressure,
                    base.structure,
                    base.lean_side,
                    base.cushion,
                    pending,
                ),
                pending_state=pending,
                pending_label=STATE_LABELS[pending] if pending else None,
                pending_bars=pending_bars,
                persistence=persistence,
                persistence_label=PERSISTENCE_LABELS[persistence],
                age_bars=age_bars,
                age_minutes=age_minutes,
                age=age,
                age_label=AGE_LABELS.get(age) if age else None,
            )
        )

    return out


def classify(inputs: WeatherInputs) -> Weather:
    """The whole read for one bar."""
    pressure = classify_pressure(inputs.pressure_bar, inputs.pressure_avg)
    structure = classify_structure(inputs.stability)
    lean_side = classify_lean(inputs.lean)
    cushion = classify_cushion(inputs.cushion_state, inputs.cushion_pts, inputs.cushion_rate_pts)
    state = _state_for(pressure, structure)

    return Weather(
        state=state,
        label=STATE_LABELS[state],
        pressure=pressure,
        structure=structure,
        gamma_trend=classify_structure(inputs.gamma_trend),
        lean_side=lean_side,
        cushion=cushion,
        sentence=_sentence(state, pressure, structure, lean_side, cushion),
    )


# --------------------------------------------------------------------------- #
# Assembling the inputs from the two stored series.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PairedBar:
    """One bar that both stored series cover, with its classifier inputs.

    The panel and any offline pass over history have to agree bar for bar. If
    they assemble the inputs separately they will eventually disagree -- a
    different smoothing window, a different cushion rate window, a paired-on
    nothing -- and an offline base-rate number would then describe a rule no
    user has ever seen. So this is the single assembler, and the endpoint and
    the tooling both go through it.
    """

    bar_start: datetime
    inputs: WeatherInputs
    cushion: CushionBar
    #: The source regime row, so a caller can echo the raw components it was
    #: classified from without re-reading or re-deriving them.
    regime: Mapping[str, Any]
    pressure_bar: Optional[float]
    pressure_avg: Optional[float]


def _as_float(value: Any) -> Optional[float]:
    return float(value) if value is not None else None


def pair_series(
    regime_chrono: Sequence[Mapping[str, Any]],
    flow_chrono: Sequence[Mapping[str, Any]],
    smoothing_bars: int = DEFAULT_SMOOTHING_BARS,
    rate_bars: int = DEFAULT_RATE_BARS,
) -> List[PairedBar]:
    """Pair the hedging-flow and gamma-structure series on their shared bars.

    Both arguments are CHRONOLOGICAL (oldest first), which is the opposite of
    how both series come back from the database; reversing is the caller's job
    because only the caller knows which way its rows arrived.

    Pairs on ``bar_start`` rather than on position. The two series are written
    by different paths on the same 5-minute grid, and a bar present in one but
    not the other would otherwise shift every later bar by one, putting this
    bar's pressure next to last bar's structure in a sentence that claims to
    describe the same five minutes.

    The flow moving average is computed over the FULL flow series before
    pairing, so a bar's three-bar average is the same number the flow chart
    draws even when the structure series starts later.
    """
    ma = smooth(
        [_as_float(r.get("net_flow_usd")) or 0.0 for r in flow_chrono],
        smoothing_bars,
    )
    flow_by_bar = {
        r["bar_start"]: (_as_float(r.get("net_flow_usd")) or 0.0, m)
        for r, m in zip(flow_chrono, ma)
    }

    cushions = build_cushion_series(
        [
            (r["bar_start"], r.get("spot"), r.get("gamma_flip"), r.get("typical_move_30m"))
            for r in regime_chrono
        ],
        rate_bars=rate_bars,
    )

    out: List[PairedBar] = []
    for row, cushion in zip(regime_chrono, cushions):
        found = flow_by_bar.get(row["bar_start"])
        if found is None:
            continue
        pressure_bar, pressure_avg = found
        out.append(
            PairedBar(
                bar_start=row["bar_start"],
                inputs=WeatherInputs(
                    pressure_bar=pressure_bar,
                    pressure_avg=pressure_avg,
                    lean=_as_float(row.get("rolling_lean")),
                    stability=_as_float(row.get("rolling_stability")),
                    gamma_trend=_as_float(row.get("anchored_stability")),
                    cushion_state=cushion.state,
                    cushion_pts=cushion.cushion_pts,
                    cushion_rate_pts=cushion.rate_pts,
                ),
                cushion=cushion,
                regime=row,
                pressure_bar=pressure_bar,
                pressure_avg=pressure_avg,
            )
        )
    return out
