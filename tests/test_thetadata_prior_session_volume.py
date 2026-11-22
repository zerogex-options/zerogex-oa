"""Regression: yesterday's option volume is not booked again as today's flow.

ThetaData's ohlc snapshot serves the most recent DAILY bar, so a contract that
has not traded today comes back with the last day's volume; fetch_chain_state
reports 0 for it on purpose. The option poller kept volume only when positive,
so it threw that 0 away: yesterday's cumulative volume stayed in the stream
overnight (workers run 24x5, and the midnight rebuild carries it), and at 09:30
the new session's accumulator booked all of it as one bucket of today's flow.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.ingestion.providers import thetadata
from src.ingestion.providers.thetadata import ThetaDataProvider, build_occ_symbol

ET = timezone(timedelta(hours=-5))
NOW = datetime.now(ET)
EXP = (NOW + timedelta(days=10)).date()
SYM = build_occ_symbol("SPY", EXP, 670.0, "C")


class _FakeClient:
    def __init__(self):
        self.ohlc_ts = NOW
        self.volume = 5000

    def option_snapshot_quote(self, **kw):
        return [{"strike": 670.0, "right": "CALL", "bid": 2.00, "ask": 2.04, "timestamp": NOW}]

    def option_snapshot_ohlc(self, **kw):
        return [
            {
                "strike": 670.0,
                "right": "CALL",
                "close": 2.02,
                "volume": self.volume,
                "timestamp": self.ohlc_ts,
            }
        ]

    def option_snapshot_open_interest(self, **kw):
        return [{"strike": 670.0, "right": "CALL", "open_interest": 12000}]


def _next_day(monkeypatch):
    """Move the poller's clock to the next New York day (the bar stays put)."""
    tomorrow = NOW.date() + timedelta(days=1)
    monkeypatch.setattr(thetadata, "_today_et", lambda: tomorrow)


def test_a_prior_session_zero_replaces_yesterdays_volume(monkeypatch):
    client = _FakeClient()
    prov = ThetaDataProvider(client, max_concurrency=1)
    stream = prov.stream_option_quotes([SYM])
    stream._poll(include_open_interest=True)
    assert stream.snapshot()[SYM].volume == 5000

    # After midnight the daily bar is a prior session's: the provider says 0.
    client.ohlc_ts = NOW - timedelta(days=1)
    _next_day(monkeypatch)
    stream._poll(include_open_interest=False)
    assert stream.snapshot()[SYM].volume == 0, "yesterday's 5,000 must not survive"
    # Open interest is still sticky.
    assert stream.snapshot()[SYM].open_interest == 12000


def test_a_rebuilt_stream_does_not_resurrect_yesterdays_volume(monkeypatch):
    client = _FakeClient()
    prov = ThetaDataProvider(client, max_concurrency=1)
    first = prov.stream_option_quotes([SYM])
    first._poll(include_open_interest=True)
    carried = first.sticky_state()  # carries 5,000 (and the OI)

    client.ohlc_ts = NOW - timedelta(days=1)
    _next_day(monkeypatch)
    rebuilt = prov.stream_option_quotes([SYM])
    rebuilt.carry_sticky_state(carried)
    rebuilt._poll(include_open_interest=True)
    assert rebuilt.drain()[SYM].volume == 0


def test_todays_volume_keeps_moving_up():
    client = _FakeClient()
    prov = ThetaDataProvider(client, max_concurrency=1)
    stream = prov.stream_option_quotes([SYM])
    stream._poll(include_open_interest=False)
    client.volume = 5200
    stream._poll(include_open_interest=False)
    assert stream.snapshot()[SYM].volume == 5200


def test_a_zero_within_the_same_day_still_cannot_erase_the_running_total():
    # The incumbent feed's most repeated bug: a transient zero from a slow
    # endpoint. Same day: the 5,000 stays (tests/test_thetadata_provider.py
    # pins the same rule).
    client = _FakeClient()
    prov = ThetaDataProvider(client, max_concurrency=1)
    stream = prov.stream_option_quotes([SYM])
    stream._poll(include_open_interest=False)
    stream._state[SYM]  # noqa: B018 - seeded
    client.volume = 0
    stream._poll(include_open_interest=False)
    assert stream.snapshot()[SYM].volume == 5000
