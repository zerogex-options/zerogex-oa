"""Hedging Flow -- the OBSERVED half of dealer hedge demand.

Every dealer-positioning surface ZeroGEX ships today is built from OPEN
INTEREST: net GEX, the gamma flip, the walls, the pin, and the Forced Flow
scenario engine all read a *book* and ask what it would do under a move.
That is the modeled, predicted half. This module is the other one -- what the
tape did to that book during the session just traded.

The quantity
------------
For every option that traded in a bucket we take the aggressor-classified net
customer position change and convert it to the stock the dealer on the other
side has to trade to stay delta-flat::

    hedge_usd = SUM over contracts of
                (buy_volume - sell_volume) * delta * CONTRACT_MULTIPLIER * spot

``delta`` is signed by the Black-Scholes convention (calls positive, puts
negative -- see :func:`src.greeks_fd.bsm_delta`), which makes the four cases
come out right without a single special case:

===========================  ==========  =========  =======================
Customer action              net contr.  delta      Dealer must
===========================  ==========  =========  =======================
buys calls                   +           +          BUY stock   (+, upward)
sells calls                  -           +          SELL stock  (-, downward)
buys puts                    +           -          SELL stock  (-, downward)
sells puts                   -           -          BUY stock   (+, upward)
===========================  ==========  =========  =======================

Positive therefore means *the hedge buys stock* -- the identical sign
convention as :attr:`src.analytics.forced_flow.ForcedFlow.total_usd`, in the
identical units (USD notional). That is deliberate and it is the whole point:
:func:`src.analytics.forced_flow.combine_flow_sources` was written with an
``observed_signed_flow_usd`` term that has returned 0.0 since the day it was
added, because the observed source did not exist. This module computes it.
Delta is linear, so the two sources sum with no reconciliation.

Level versus change
-------------------
This is NOT DEX. ZeroGEX declines to publish raw options-only delta exposure
as a flow signal (see the ``why-we-dont-publish-dex`` article) on the grounds
that a delta *level* omits the offsetting hedge and does not say how delta
will change next. That objection is about a level. What is computed here is
the CHANGE in portfolio delta caused by new trades -- which the same article
names as one of the things that genuinely does create hedge demand. The
distinction is the reason this metric is publishable and a raw DEX print is
not, and it should survive any rewording of the UI copy.

What this is NOT
----------------
An observation of dealer activity. Every number here inherits the
passive-side-is-a-market-maker assumption: that when a print lands on the
offer, the resting side was a dealer. A print at the ask can equally be a
customer lifting a dealer or a dealer lifting a customer, and the tape cannot
separate them. That assumption is Arm B of the study in
``docs/design/aggressor-inferred-positioning-experiment.md``, it has not yet
been measured against exchange-classified market-maker data, and the
terminology table in that document is binding on this module and everything
downstream of it: this is ESTIMATED HEDGING PRESSURE or AGGRESSOR-INFERRED
HEDGING FLOW. It is never "observed dealer flow", "dealer positioning", or
"actual dealer hedging".

Two further honesties, both surfaced in the payload rather than buried:

* the buy/sell split is EXTRAPOLATED (``flow_contract_facts`` redistributes
  mid-classified volume across buy/sell pro-rata), so a bucket whose volume
  was entirely mid-classified contributes nothing rather than something
  wrong -- :attr:`HedgingFlowBar.classified_ratio` reports that coverage;
* ``delta`` is the contract's delta at the bucket timestamp, not at the
  instant of each print inside it.

Rate, not level, is the signal
------------------------------
The cumulative curve answers "which way has the session leaned", and it
crosses zero rarely -- by the time it does, the day already told you. What a
trader actually watches for ("where does pressure accelerate, when does it
reverse") is the curve's SLOPE: the per-bar hedge demand. So this module
computes both and flips are detected on the rate:

* :func:`sign_flip_events` on the smoothed rate -- the immediate push turning
  over. This is the frequent, tradeable one.
* :func:`zero_cross_events` on the cumulative -- the session's whole lean
  changing hands. Rare, and context rather than trigger.

Raw 5-minute rate is spiky enough that flipping on it alone would fire
constantly, which is exactly why the moving average is not decoration: it is
what makes the flip legible. :func:`smooth` is a trailing SMA over
:data:`DEFAULT_SMOOTHING_BARS` bars, and flips are read off the smoothed
series.

Every function here is pure and unit-tested; the SQL lives in
:mod:`src.hedging_flow_sql` and the I/O shell in the API layer, matching the
split :mod:`src.analytics.regime_shift` already uses.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence

#: Shares per option contract. Imported rather than redefined so this module
#: cannot drift from the Greeks layer's notion of contract size.
from src.greeks_fd import CONTRACT_MULTIPLIER

#: Trailing SMA length, in 5-minute bars, used for the rate line and for flip
#: detection. Three bars = 15 minutes: long enough that a single busy bucket
#: cannot manufacture a flip, short enough that a genuine turn is flagged
#: while it still matters intraday. Callers may override; this is the
#: defensible default rather than a hard rule.
DEFAULT_SMOOTHING_BARS = 3

#: A flip whose swing is below this multiple of the session's own typical
#: swing is reported but marked ``is_significant=False``, so a UI can show
#: every flip or only the ones that carried size.
#:
#: Note what 1.0 actually means here: :func:`session_scale` is a MEDIAN, so
#: roughly half the distribution sits above it by construction. 1.0 is
#: therefore the middle of the day's swings, not the tail, and on a live tape
#: it lets through more than "significant" suggests. It is left at 1.0 because
#: the deadband below is the right tool for chatter and this one should stay a
#: size filter; raise it if you want only the day's largest turns.
DEFAULT_SIGNIFICANCE_RATIO = 1.0

#: Half-width of the flat band around zero, as a multiple of the session's own
#: typical |rate|. Values inside the band are treated as FLAT rather than as a
#: side, so a series hugging zero and nicking across it repeatedly produces no
#: flips at all.
#:
#: This is the fix for the real complaint about flip counts. The significance
#: filter grades crossings that already happened; it cannot help when the
#: problem is that a near-zero line crosses a dozen times. Requiring the rate
#: to actually establish itself on the new side is what removes those, and it
#: stays computable live: the flip is timestamped at the bar the move leaves
#: the band, which is the first moment it is knowable at all.
DEFAULT_FLAT_BAND_RATIO = 0.5

#: Floor for the robust session scale, in USD. Prevents a quiet open (where
#: the running median of |rate| is a handful of dollars) from scoring an
#: equally tiny flip as wildly significant.
_MIN_SESSION_SCALE_USD = 1_000.0


@dataclass(frozen=True)
class HedgingFlowBar:
    """One 5-minute bar of estimated hedging pressure.

    ``*_usd`` fields are USD of stock the delta-flat hedge implies, positive
    for buying. The ``call_*`` / ``put_*`` split is by the OPTION TYPE that
    produced the pressure, not by the direction of the pressure: customers
    selling puts and customers buying calls both push ``net_flow_usd``
    positive, and they land in ``put_flow_usd`` and ``call_flow_usd``
    respectively. That is the split worth showing -- a chart that colours put
    activity bearish by construction would be wrong on exactly the days it
    matters.

    ``classified_ratio`` is the share of the bar's traded volume that carried
    an aggressor classification (bid/ask), in [0, 1]. Mid-classified volume is
    redistributed pro-rata upstream, but a bar with NO classified volume
    contributes zero flow; a low ratio means this bar's reading rests on a
    thin sample and should be read as such.
    """

    bar_start: datetime
    call_flow_usd: float
    put_flow_usd: float
    net_flow_usd: float
    cum_call_usd: float
    cum_put_usd: float
    cum_net_usd: float
    underlying_price: Optional[float]
    contract_count: int
    classified_ratio: float
    is_synthetic: bool


@dataclass(frozen=True)
class FlipEvent:
    """A sign change in estimated hedging pressure.

    ``kind`` is ``"rate"`` (the immediate push turned over -- read off the
    SMOOTHED per-bar series) or ``"cumulative"`` (the session's net lean
    changed hands). ``direction`` is ``"to_buying"`` when the new sign is
    positive.

    ``magnitude_usd`` is the SWING across zero -- ``|value - previous value|``
    -- not the value at the crossing. The distinction is not a detail: a
    series is near zero at the moment it crosses zero, by definition, so the
    level at a flip is structurally tiny and measures nothing. How hard it
    crossed is what separates a genuine turn from a wobble.

    ``session_ratio`` is that swing over the session's typical swing so far,
    computed from bars STRICTLY BEFORE the flip. Causal on purpose: this
    endpoint serves a live session, and a score that needed later bars could
    not be alerted on when it mattered.
    """

    bar_start: datetime
    kind: str
    direction: str
    magnitude_usd: float
    session_ratio: float
    is_significant: bool
    underlying_price: Optional[float]


def smooth(
    values: Sequence[Optional[float]], window: int = DEFAULT_SMOOTHING_BARS
) -> List[Optional[float]]:
    """Trailing simple moving average, ``None`` until the window fills.

    Leading ``None`` rather than a partial average: a 1-bar "3-bar average" is
    the raw value wearing a smoothed label, and flip detection reading it
    would fire on precisely the noise the smoothing exists to remove. A
    ``window`` of 1 or less is the identity (with ``None`` holes preserved).

    ``None`` entries inside the series are treated as gaps -- any window
    containing one yields ``None`` -- so a hole in the source data cannot be
    silently averaged into a number that looks measured.
    """
    if window <= 1:
        return [None if v is None else float(v) for v in values]

    out: List[Optional[float]] = []
    for i in range(len(values)):
        if i + 1 < window:
            out.append(None)
            continue
        chunk = values[i + 1 - window : i + 1]
        if any(v is None for v in chunk):
            out.append(None)
            continue
        out.append(sum(float(v) for v in chunk) / window)  # type: ignore[arg-type]
    return out


def session_scale(values: Sequence[Optional[float]]) -> float:
    """Robust typical magnitude of a series, for normalizing flip size.

    Median of ``|value|`` over the non-null entries, floored at
    :data:`_MIN_SESSION_SCALE_USD`. Median rather than mean because one 0DTE
    print into the close routinely dwarfs an entire quiet morning, and a mean
    would let that single bar redefine "normal" for the whole session. The
    floor stops a dead open, where a typical move is a few dollars, from
    scoring an equally tiny flip as enormous.

    Fed bar-to-bar SWINGS by the flip scanners (see :class:`FlipEvent`), and
    usable on any series a caller wants a robust scale for.
    """
    magnitudes = [abs(float(v)) for v in values if v is not None]
    if not magnitudes:
        return _MIN_SESSION_SCALE_USD
    return max(statistics.median(magnitudes), _MIN_SESSION_SCALE_USD)


def _crossed(prev: float, curr: float) -> bool:
    """True when ``prev`` and ``curr`` sit on opposite sides of zero."""
    return (prev > 0 and curr < 0) or (prev < 0 and curr > 0)


def _scan_flips(
    bars: Sequence[HedgingFlowBar],
    values: Sequence[Optional[float]],
    kind: str,
    significance_ratio: float,
    flat_band_ratio: float = DEFAULT_FLAT_BAND_RATIO,
    always_significant: bool = False,
) -> List[FlipEvent]:
    """Walk a series and emit the direction changes that actually established.

    Three properties this walk has to get right, all three learned from real
    sessions or from tests:

    * a value inside the FLAT BAND is on neither side. The band generalises
      the older "zero is not a side" rule: a rate hovering around zero and
      nicking across it is not changing direction a dozen times, it is flat
      and noisy, and reporting each nick is what made "significant flips only"
      still show a dozen dots on a live tape. ``prev_side`` therefore tracks
      the last value OUTSIDE the band, and a flip is registered at the bar the
      series establishes itself on the far side.
    * swing size is measured from that established side to the current value,
      so it describes the whole traverse rather than one bar of it. Bar-to-bar
      movement is tracked separately, in ``prior_swings``, purely to build the
      scale to score against.
    * every scale is built from bars strictly BEFORE the current one, so a
      score is computable in the moment rather than needing the rest of the
      session. An alert that needed hindsight could not fire when it mattered.
    """
    events: List[FlipEvent] = []
    prev_value: Optional[float] = None  # last value, for typical bar-to-bar swing
    prev_side: Optional[float] = None  # last value outside the band, for which side we are on
    prior_swings: List[float] = []
    prior_magnitudes: List[float] = []

    for bar, value in zip(bars, values):
        if value is None:
            continue

        band = flat_band_ratio * session_scale(prior_magnitudes)
        outside = abs(value) > band

        if outside and prev_side is not None and _crossed(prev_side, value):
            swing = abs(value - prev_side)
            ratio = swing / session_scale(prior_swings)
            events.append(
                FlipEvent(
                    bar_start=bar.bar_start,
                    kind=kind,
                    direction="to_buying" if value > 0 else "to_selling",
                    magnitude_usd=swing,
                    session_ratio=ratio,
                    is_significant=always_significant or ratio >= significance_ratio,
                    underlying_price=bar.underlying_price,
                )
            )

        if prev_value is not None:
            prior_swings.append(abs(value - prev_value))
        prior_magnitudes.append(abs(value))
        prev_value = value
        if outside:
            prev_side = value

    return events


def sign_flip_events(
    bars: Sequence[HedgingFlowBar],
    window: int = DEFAULT_SMOOTHING_BARS,
    significance_ratio: float = DEFAULT_SIGNIFICANCE_RATIO,
    flat_band_ratio: float = DEFAULT_FLAT_BAND_RATIO,
) -> List[FlipEvent]:
    """Flips of the SMOOTHED per-bar rate -- the immediate push turning over.

    The frequent, actionable one: it says the hedging pressure being created
    right now has changed direction, which is an earlier and different
    statement from the session's cumulative lean changing.

    Read off the smoothed series because raw 5-minute rate crosses zero on
    noise alone; scored so a caller can render every flip while drawing only
    the ones that carried size.
    """
    smoothed = smooth([b.net_flow_usd for b in bars], window)
    return _scan_flips(bars, smoothed, "rate", significance_ratio, flat_band_ratio)


def zero_cross_events(
    bars: Sequence[HedgingFlowBar],
    flat_band_ratio: float = DEFAULT_FLAT_BAND_RATIO,
) -> List[FlipEvent]:
    """Zero crossings of the CUMULATIVE curve -- the session's lean changing.

    Rare by construction, and context rather than trigger: by the time a
    running total crosses back through zero, the move that did it has usually
    already happened. Reported separately from :func:`sign_flip_events` so
    the two are never conflated in the UI.

    Read off the raw curve -- a running total is already an integral and
    needs no smoothing -- and always marked significant: rarity means there
    is no noise here to filter, so suppressing one would only hide it.
    """
    cumulative = [b.cum_net_usd for b in bars]
    return _scan_flips(
        bars,
        cumulative,
        "cumulative",
        0.0,
        flat_band_ratio,
        always_significant=True,
    )


def hedge_usd(net_contracts: float, delta: float, spot: float) -> float:
    """Stock the delta-flat hedge implies for one contract line, in USD.

    The primitive the SQL aggregation implements; kept here so the sign
    convention has a single executable definition that tests can pin, and so
    a caller working from Python rows gets the same number as the database.
    """
    return net_contracts * delta * CONTRACT_MULTIPLIER * spot
