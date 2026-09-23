"""The scorecard's forward return must stay inside one regular session.

Regression cover for the bug that made `eod_pressure` unreadable on the
public scorecard: the forward-price lateral join took the first quote at or
after ``event + horizon`` with no upper bound, so a signal firing inside
``horizon`` of the close was graded against an after-hours print — and on a
Friday, against the following Monday's open.

`eod_pressure` was affected on every flip it can ever register, because its
time ramp is zero until 90 minutes before the close. That number also feeds
``best``/``worst`` in ``tweet_text``, which the 4:15 PM ET job posts verbatim.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.api.queries.signals import SignalsQueriesMixin, _session_closes_since

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _et_midnight_utc(day: date) -> datetime:
    """ET local midnight for ``day``, in UTC — what the router passes in."""
    return datetime.combine(day, time(0, 0), tzinfo=ET).astimezone(UTC)


def _close_et(day: date) -> datetime:
    return SignalsQueriesMixin._session_close_utc(_et_midnight_utc(day)).astimezone(ET)


def test_session_close_is_four_pm_et_on_a_regular_day():
    assert _close_et(date(2026, 9, 11)).timetz().replace(tzinfo=None) == time(16, 0)


def test_session_close_tracks_the_et_calendar_date_not_utc():
    # ET midnight on 2026-09-11 is 04:00 UTC the same day, but the ET date is
    # what identifies the trading session. A naive UTC .date() would work here
    # and break for any day whose ET midnight lands on the prior UTC date.
    closed = SignalsQueriesMixin._session_close_utc(_et_midnight_utc(date(2026, 9, 11)))
    assert closed.astimezone(ET).date() == date(2026, 9, 11)


def test_session_close_survives_both_dst_transitions():
    # EST (-05:00) and EDT (-04:00) must each give a 16:00 *local* close.
    for day in (date(2026, 1, 14), date(2026, 7, 15)):
        assert _close_et(day).hour == 16, day


def test_session_close_returns_an_aware_utc_datetime():
    closed = SignalsQueriesMixin._session_close_utc(_et_midnight_utc(date(2026, 9, 11)))
    assert closed.tzinfo is not None
    assert closed.utcoffset() == timedelta(0)


def test_session_close_honors_an_early_close(monkeypatch):
    half = date(2026, 11, 27)
    import src.api.freshness as freshness

    monkeypatch.setattr(
        freshness,
        "_calendar",
        lambda: (set(), {half}, time(16, 0), time(13, 0), time(20, 0), time(17, 0)),
    )
    assert _close_et(half).timetz().replace(tzinfo=None) == time(13, 0)
    assert _close_et(date(2026, 11, 30)).timetz().replace(tzinfo=None) == time(16, 0)


def test_session_close_falls_back_to_four_pm_when_the_calendar_breaks(monkeypatch):
    import src.api.freshness as freshness

    def _boom(*_a, **_k):
        raise RuntimeError("calendar unavailable")

    monkeypatch.setattr(freshness, "session_close_for", _boom)
    # Degrades to the regular close rather than failing the scorecard.
    assert _close_et(date(2026, 9, 11)).timetz().replace(tzinfo=None) == time(16, 0)


def test_every_forward_quote_join_is_bounded_by_a_session_close():
    """Both forward-return queries must bound q1; guard against a revert.

    Three queries compute a forward return this way — ``get_daily_scorecard``
    (the public scorecard, one session), ``get_signal_component_events`` (each
    signal's Event Timeline, two sessions) and ``get_signal_trailing_record``
    (the cross-session record). The first two shipped with the same unbounded
    join. The count is asserted so a fourth cannot be added without reading
    this; the per-join bound is the actual guard.
    """
    source = open("src/api/queries/signals.py", encoding="utf-8").read()
    joins = re.findall(
        r"LEFT JOIN LATERAL \((?:(?!LEFT JOIN LATERAL).)*?\) q1 ON TRUE",
        source,
        re.S,
    )
    assert len(joins) == 3, f"expected 3 forward-price joins, found {len(joins)}"
    for body in joins:
        assert "uq.timestamp >= scs.timestamp + INTERVAL" in body
        assert "uq.timestamp <=" in body, (
            "q1 must be bounded by a session close, or late-firing signals are "
            "graded against after-hours prints"
        )


def test_session_closes_since_covers_weekdays_and_skips_weekends():
    cutoff = datetime.combine(date(2026, 9, 10), time(9, 30), tzinfo=ET)
    now = datetime.combine(date(2026, 9, 14), time(12, 0), tzinfo=ET)
    closes = [c.astimezone(ET) for c in _session_closes_since(cutoff, now)]
    assert [c.date() for c in closes] == [
        date(2026, 9, 10),
        date(2026, 9, 11),
        date(2026, 9, 14),
    ], "Sat/Sun must not produce a session close"
    assert {c.hour for c in closes} == {16}


def test_a_friday_flip_cannot_reach_mondays_open():
    """The bug in one assertion: Friday 15:30 + 60m must not resolve.

    The bound for a 15:30 ET Friday event is that Friday's 16:00 close, so a
    quote at Monday's open cannot satisfy `uq.timestamp <= bound`.
    """
    cutoff = datetime.combine(date(2026, 9, 10), time(9, 30), tzinfo=ET)
    now = datetime.combine(date(2026, 9, 14), time(12, 0), tzinfo=ET)
    closes = _session_closes_since(cutoff, now)

    event = datetime.combine(date(2026, 9, 11), time(15, 30), tzinfo=ET)
    bound = min((c for c in closes if c >= event), default=None)
    assert bound is not None
    assert bound.astimezone(ET) == datetime.combine(
        date(2026, 9, 11), time(16, 0), tzinfo=ET
    )

    monday_open = datetime.combine(date(2026, 9, 14), time(9, 30), tzinfo=ET)
    assert not (monday_open <= bound), "Monday's open must be out of bounds"
    # And the horizon itself already overshoots the close, so nothing scores.
    assert event + timedelta(minutes=60) > bound
