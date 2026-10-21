"""Call and put centroids -- where the session's option trading is centered.

The call centroid is the average strike of the calls traded, weighted by how
many contracts traded at each strike; the put centroid is the same for puts.
Calls trade mostly at and above the price and puts at and below it, so the two
lines sit on either side of the price and move with it.

Two versions of each, per 5-minute bar:

* **Session** -- every contract traded from the open through the end of the
  bar. Smooth, and slower to move as the day's volume builds.
* **Recent** -- only the contracts traded in the last ``window_minutes``
  through the end of the bar. Follows the price, and noisier.

The input is one row per (bar, side) of plain sums -- contracts traded and
strike times contracts -- so a run of bars combines by adding, with no
re-weighting. Bars are matched on time, not counted, so a bar with no trades
does not stretch the recent window over older bars.

Known artifact, inherited from the facts: when a contract joins the ingested
strike window mid-session, its whole day's volume so far lands in the minute
it joins. The session line absorbs that as it would have anyway; the recent
line can jump for one window.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

BAR_MINUTES = 5
DEFAULT_WINDOW_MINUTES = 15

_SIDES = ("C", "P")


def _centroid(volume: int, strike_volume: float) -> Optional[float]:
    return strike_volume / volume if volume > 0 else None


def build_centroid_series(
    rows: Iterable[Dict[str, Any]],
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
) -> List[Dict[str, Any]]:
    """Session and recent centroids for each bar that traded, oldest first.

    ``rows`` carry ``bar_start``, ``option_type`` (``'C'``/``'P'``),
    ``volume`` and ``strike_volume`` (see ``src/flow_centroids_sql.py``), in
    any order. Each returned bar has ``bar_start``, ``call_centroid``,
    ``put_centroid``, ``call_centroid_recent``, ``put_centroid_recent``,
    ``call_volume`` and ``put_volume`` (session totals). A side with nothing
    traded yet, or nothing in the recent window, reads ``None``.
    """
    if window_minutes < BAR_MINUTES or window_minutes % BAR_MINUTES:
        raise ValueError(f"window_minutes must be a positive multiple of {BAR_MINUTES}")

    by_bar: Dict[datetime, Dict[str, Tuple[int, float]]] = {}
    for row in rows:
        side = row["option_type"]
        if side not in _SIDES:
            continue
        bar = row["bar_start"]
        volume, strike_volume = by_bar.setdefault(bar, {}).get(side, (0, 0.0))
        by_bar[bar][side] = (
            volume + int(row["volume"] or 0),
            strike_volume + float(row["strike_volume"] or 0.0),
        )

    bars = sorted(by_bar)
    span = timedelta(minutes=window_minutes - BAR_MINUTES)
    session = {side: [0, 0.0] for side in _SIDES}
    out: List[Dict[str, Any]] = []
    for i, bar in enumerate(bars):
        recent = {side: [0, 0.0] for side in _SIDES}
        j = i
        while j >= 0 and bars[j] >= bar - span:
            for side, (volume, strike_volume) in by_bar[bars[j]].items():
                recent[side][0] += volume
                recent[side][1] += strike_volume
            j -= 1
        for side, (volume, strike_volume) in by_bar[bar].items():
            session[side][0] += volume
            session[side][1] += strike_volume
        out.append(
            {
                "bar_start": bar,
                "call_centroid": _centroid(*session["C"]),
                "put_centroid": _centroid(*session["P"]),
                "call_centroid_recent": _centroid(*recent["C"]),
                "put_centroid_recent": _centroid(*recent["P"]),
                "call_volume": session["C"][0],
                "put_volume": session["P"][0],
            }
        )
    return out
