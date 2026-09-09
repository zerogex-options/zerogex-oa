"""Gamma Regime Series -- the intraday time series of the Gamma Shift read.

:mod:`src.analytics.regime_shift` answers "how has dealer gamma changed
between these two moments". That is a CARD: one comparison, one headline. This
module turns the same maths into a LINE -- a reading at every 5-minute bar of
the session -- so structure can sit on the same timeline as
:mod:`src.analytics.hedging_flow` and be read against it.

Why the pairing is the point
----------------------------
Hedging Flow says how hard the tape is pushing right now. It cannot say
whether that push lands in a book that will absorb it or amplify it. This
series says exactly that, and the two together are the thing a trader
actually wants: a move away from a level with buying pressure behind it AND a
book turning more explosive is a different trade from the same move into a
book that is firming up.

The two lenses, and why they do not sum
---------------------------------------
Each bar carries two independent readings, deliberately mirroring the two
views of the flow panel:

``anchored``
    versus the session's first bar. "How has structure changed TODAY." The
    counterpart of the flow panel's cumulative curve.

``rolling``
    versus :data:`DEFAULT_ROLLING_BARS` bars back. "How is structure changing
    RIGHT NOW." The counterpart of the flow panel's rate line, and the one to
    read next to a flip.

These are computed independently and **the rolling readings do not sum to the
anchored one.** That is not an approximation to be tidied up later: both
scores are proximity-weighted around the bar's OWN spot
(:func:`src.analytics.regime_shift.proximity_weight`), so the kernel is
re-centred on every bar. A build at 700 weighted against a 705 spot is a
different number than the same build weighted against 690. Summing bar-to-bar
diffs would silently assert a fixed kernel and produce a figure that matches
neither lens. A test pins this.

Expiry awareness comes free: :func:`~src.analytics.regime_shift.shift_rows`
differences only the expirations present on BOTH sides, so an expiry rolling
off mid-session is reported as expired rather than booked as a gamma shed.

Cost, and why this module is pure
---------------------------------
A chain snapshot is ~40-60 strikes x ~10-25 expirations, so a session is
~78 bars x up to ~1500 rows. Computing that per request, per viewer, on a
poll is precisely the shape that took the strike-profile timeseries down (see
``docs/runbooks/strike_profile_timeseries_stampede.md``): a read too slow for
its own guard returns empty, never populates its cache, and the next poll
re-enters the same work.

So the series is not computed on read. This module is pure functions over
snapshots already in hand; the Analytics Engine calls it once per bar and
materialises the result, and the API serves ~78 small rows from that table.
The expensive part happens once, in the background, instead of once per
viewer per poll.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from src.analytics.regime_shift import ShiftScores, shift_rows, weighted_scores

#: Lookback for the rolling lens, in 5-minute bars. Six bars = 30 minutes,
#: matching the shipped Gamma Shift page's shortest ``_INTRADAY_OFFSETS``
#: preset so a reader moving between the card and the line sees the same
#: comparison rather than two differently-scoped numbers.
DEFAULT_ROLLING_BARS = 6


@dataclass(frozen=True)
class ChainSnapshot:
    """The per-strike dealer-gamma chain at one bar, plus that bar's spot.

    ``rows`` are ``gex_by_strike`` rows (or anything carrying ``net_gex`` /
    ``net_gamma``, ``strike`` and ``expiration`` -- the regime_shift row
    accessors tolerate both vocabularies). ``spot`` is the underlying at the
    bar and matters as much as the rows: it centres the proximity kernel, so
    passing a stale spot quietly mis-weights every strike.
    """

    bar_start: datetime
    spot: float
    rows: Sequence[Mapping[str, Any]]


@dataclass(frozen=True)
class RegimeSeriesBar:
    """One bar of the regime series.

    Scores are raw, in dollar-GEX units -- normalisation against a trailing
    session distribution is the API layer's job, exactly as it is for the
    Gamma Shift card, so this module stays free of stored history.

    Positive ``stability`` means more long gamma near spot: dealers hedge
    AGAINST moves, so vol is suppressed and the tape pins. Negative means the
    book has turned accelerant. Positive ``lean`` means the change is
    supportive (building below spot / eroding above); negative means capping.

    ``rolling_*`` is ``None`` for the first :data:`DEFAULT_ROLLING_BARS` bars,
    where there is no lookback to compare against. None rather than zero: a
    zero would draw a flat line through the open that reads as a measured
    "no change".
    """

    bar_start: datetime
    spot: float
    anchored_lean: float
    anchored_stability: float
    anchored_net_shift: float
    anchored_gross_shift: float
    rolling_lean: Optional[float]
    rolling_stability: Optional[float]
    rolling_net_shift: Optional[float]
    rolling_gross_shift: Optional[float]
    sigma_price: float
    near_spot_stock: float
    strike_count: int
    #: Expirations that were present at the comparison point and are gone by
    #: this bar. Reported, never booked as a shed -- an expiry rolling off is
    #: not dealers selling gamma.
    expired_expirations: tuple[date, ...]


def _score(
    base: ChainSnapshot,
    current: ChainSnapshot,
    restrict_expirations: Optional[Iterable[date]],
) -> tuple[ShiftScores, tuple[date, ...], int]:
    """One comparison: diff two snapshots, project onto the two axes.

    Weighted against ``current``'s spot, not the base's. The question is
    always "what does the change mean for where price is NOW"; centring the
    kernel on where spot used to be would score today's structure against
    yesterday's location.
    """
    diff = shift_rows(base.rows, current.rows, restrict_expirations=restrict_expirations)
    scores = weighted_scores(diff.rows, current.spot)
    return scores, diff.expired_expirations, len(diff.rows)


def build_series(
    snapshots: Sequence[ChainSnapshot],
    rolling_bars: int = DEFAULT_ROLLING_BARS,
    restrict_expirations: Optional[Iterable[date]] = None,
) -> List[RegimeSeriesBar]:
    """Compute the anchored and rolling readings for every bar.

    ``snapshots`` must be chronological; the first is the anchor. Returns one
    bar per snapshot, so the caller can align it to the flow series without
    re-deriving a timeline.

    The anchor bar itself is included with zero anchored scores -- it is a
    real bar of the session with a real spot, and dropping it would leave the
    structure line starting one bar later than the flow line it is meant to
    sit beside.
    """
    if not snapshots:
        return []

    anchor = snapshots[0]
    exps = list(restrict_expirations) if restrict_expirations is not None else None

    out: List[RegimeSeriesBar] = []
    for i, snap in enumerate(snapshots):
        anchored, expired, strike_count = _score(anchor, snap, exps)

        if i >= rolling_bars:
            rolling, _, _ = _score(snapshots[i - rolling_bars], snap, exps)
        else:
            rolling = None

        out.append(
            RegimeSeriesBar(
                bar_start=snap.bar_start,
                spot=snap.spot,
                anchored_lean=anchored.lean,
                anchored_stability=anchored.stability,
                anchored_net_shift=anchored.net_shift,
                anchored_gross_shift=anchored.gross_shift,
                rolling_lean=rolling.lean if rolling else None,
                rolling_stability=rolling.stability if rolling else None,
                rolling_net_shift=rolling.net_shift if rolling else None,
                rolling_gross_shift=rolling.gross_shift if rolling else None,
                sigma_price=anchored.sigma_price,
                near_spot_stock=anchored.near_spot_stock,
                strike_count=strike_count,
                expired_expirations=expired,
            )
        )

    return out


def build_latest_bar(
    anchor: ChainSnapshot,
    lookback: Optional[ChainSnapshot],
    current: ChainSnapshot,
    restrict_expirations: Optional[Iterable[date]] = None,
) -> RegimeSeriesBar:
    """The incremental form: one bar, from the three snapshots it needs.

    This is what the Analytics Engine calls each cycle. It exists so the
    steady-state write is O(1) in the session's length -- rebuilding all 78
    bars every minute to store the newest one would reintroduce the very scan
    this design avoids. ``lookback`` is ``None`` early in the session, when
    there is nothing far enough back to roll against.

    Equivalent by construction to the corresponding entry of
    :func:`build_series`; a test asserts the two agree.
    """
    exps = list(restrict_expirations) if restrict_expirations is not None else None
    anchored, expired, strike_count = _score(anchor, current, exps)
    rolling = _score(lookback, current, exps)[0] if lookback is not None else None

    return RegimeSeriesBar(
        bar_start=current.bar_start,
        spot=current.spot,
        anchored_lean=anchored.lean,
        anchored_stability=anchored.stability,
        anchored_net_shift=anchored.net_shift,
        anchored_gross_shift=anchored.gross_shift,
        rolling_lean=rolling.lean if rolling else None,
        rolling_stability=rolling.stability if rolling else None,
        rolling_net_shift=rolling.net_shift if rolling else None,
        rolling_gross_shift=rolling.gross_shift if rolling else None,
        sigma_price=anchored.sigma_price,
        near_spot_stock=anchored.near_spot_stock,
        strike_count=strike_count,
        expired_expirations=expired,
    )
