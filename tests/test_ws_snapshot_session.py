"""A replayed WS snapshot must be labelled with the session it is replayed IN.

``QuoteBroadcaster`` caches each symbol's last fan-out frame in ``_latest``
and replays it to every new subscriber, so a mounting consumer paints a price
immediately instead of waiting for the next tick. That cache has no expiry: in
a quiet market the same frame is served for as long as the worker lives.

The frame carries a ``session`` label, and it used to be replayed frozen —
whatever the clock said when the tick first arrived. So the socket and
``GET /api/market/quote`` (which always answers from the clock) could disagree
about what session it is, and the frontend takes ``quote.session`` as
authoritative for how to render the whole header.

Observed over the weekend of 2026-09-12: SPY's last tick of the week lands
Friday ~19:59 ET and is labelled ``after-hours``. Every Saturday (re)subscribe
replayed that label, flipping the header into its extended-hours rendering —
the separate after-hours row, plus row 1 re-anchored onto Friday's extended
print — until the next HTTP poll answered ``closed`` and put it back. The
client's stall watchdog forces a reconnect roughly once a minute while no
quote frames flow, so the SPY quote visibly switched back and forth all
weekend.

The prices and timestamp stay frozen — that IS the last print, and reporting
it is the whole point of the hello frame. Only the wall-clock label is
re-derived.
"""

import asyncio
from typing import Optional
from unittest.mock import MagicMock

import pytest

from src.api.quote_broadcaster import QuoteBroadcaster

# Friday's final SPY tick of the week, as ingestion delivered it.
FRIDAY_TICK = {
    "symbol": "SPY",
    "asset_type": "ETF",
    "timestamp": "2026-09-11T19:59:00-04:00",
    "open": 761.20,
    "high": 762.05,
    "low": 760.50,
    "close": 761.65,
    "volume": 4_120,
}


class _Clock:
    """Stands in for main.get_market_session — answers whatever the wall
    clock is set to, and records that it was asked."""

    def __init__(self, session: Optional[str]) -> None:
        self.session = session
        self.calls = 0

    def __call__(self, asset_type, price_is_stable, close_data_available):
        self.calls += 1
        return self.session


def _broadcaster(clock: _Clock) -> QuoteBroadcaster:
    return QuoteBroadcaster(lambda: {}, clock)


def _friday_then_saturday(saturday_session: str = "closed"):
    """Fan out Friday's after-hours tick, then subscribe on Saturday."""
    clock = _Clock("after-hours")
    b = _broadcaster(clock)

    async def run():
        await b._fanout(dict(FRIDAY_TICK))
        clock.session = saturday_session  # the weekend arrives
        return await b.subscribe(MagicMock(), "SPY")

    return b, clock, asyncio.run(run())


# --- the regression --------------------------------------------------------


def test_weekend_replay_is_not_labelled_after_hours():
    """The bug. Friday's ``after-hours`` label, replayed on Saturday, put the
    header's extended-hours rendering back on screen every reconnect."""
    _b, _clock, snap = _friday_then_saturday()
    assert snap["session"] == "closed"


def test_replay_consults_the_session_state_machine():
    """Not merely 'not after-hours' — the label must come from the same
    helper GET /api/market/quote uses, so the two endpoints agree."""
    _b, clock, _snap = _friday_then_saturday()
    assert clock.calls == 2  # once on fan-out, once on replay


def test_replay_serves_the_real_last_print():
    """Re-labelling must not touch the data. Friday's close is the honest
    last SPY print on a Saturday, and the hello frame exists to say so."""
    _b, _clock, snap = _friday_then_saturday()
    assert snap["close"] == pytest.approx(761.65)
    assert snap["timestamp"] == FRIDAY_TICK["timestamp"]
    assert snap["volume"] == 4_120


def test_replay_keeps_the_original_emission_stamp():
    """``server_ts`` is the client's out-of-order guard (core/liveQuoteOrdering
    .ts). Restamping a replay to 'now' would make a days-old snapshot beat
    every live frame; it stays the stamp of the tick it describes."""
    _b, _clock, snap = _friday_then_saturday()
    assert snap["server_ts"] == pytest.approx(_b._latest["SPY"]["server_ts"])


def test_replay_does_not_mutate_the_shared_cache():
    """``_latest`` is one dict shared by every subscriber; the per-replay
    label must not be written back into it."""
    b, _clock, snap = _friday_then_saturday()
    assert snap["session"] == "closed"
    assert b._latest["SPY"]["session"] == "after-hours"


def test_a_subscribe_storm_cannot_manufacture_soft_close_stability():
    """Three identical closes settle the INDEX 16:00:00-16:00:29 soft close.
    Only real ticks may advance that tracker — otherwise a page that
    resubscribes three times would flip SPX to 'closed' early."""
    clock = _Clock("open")
    b = _broadcaster(clock)

    async def run():
        await b._fanout({**FRIDAY_TICK, "symbol": "SPX", "asset_type": "INDEX"})
        for _ in range(5):
            await b.subscribe(MagicMock(), "SPX")

    asyncio.run(run())
    assert b._soft_close["SPX"] == [761.65]


# --- the fan-out path is unchanged ----------------------------------------


def test_fanout_still_labels_from_the_clock():
    clock = _Clock("open")
    b = _broadcaster(clock)
    asyncio.run(b._fanout(dict(FRIDAY_TICK)))
    assert b._latest["SPY"]["session"] == "open"


def test_fanout_soft_close_stability_still_settles_an_index():
    """Three identical closes in a row is what ``price_is_stable`` means."""
    seen = []

    class _Recorder(_Clock):
        def __call__(self, asset_type, price_is_stable, close_data_available):
            seen.append(price_is_stable)
            return super().__call__(asset_type, price_is_stable, close_data_available)

    b = _broadcaster(_Recorder("open"))

    async def run():
        for _ in range(3):
            await b._fanout({**FRIDAY_TICK, "symbol": "SPX", "asset_type": "INDEX"})

    asyncio.run(run())
    assert seen == [False, False, True]


def test_no_snapshot_yet_returns_none():
    """A symbol the broadcaster has never seen has nothing to replay, and
    must not be answered with a re-labelled empty frame."""
    clock = _Clock("closed")
    b = _broadcaster(clock)
    assert asyncio.run(b.subscribe(MagicMock(), "QQQ")) is None
    assert clock.calls == 0
