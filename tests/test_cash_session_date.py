"""Tests for the cash-session-date helpers in src.validation.

These pin the boundary semantics that Item 8 of the volume-tracking
review (docs/architecture/volume-tracking-review.md) is built on.
The helpers themselves are scaffolding -- no production code consumes
them yet -- but the tests must lock in the boundary, DST, and
naive-input behavior so a future migration of the FlowAccumulator
session key and the flow_contract_facts LAG-CASE branches can be
performed safely.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytz

from src.validation import cash_session_date, cash_session_start_utc

_ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# cash_session_date
# ---------------------------------------------------------------------------


def test_cash_session_date_rth_returns_calendar_date():
    """During RTH (>= 09:30 ET), the cash-session date is the calendar
    date.  Sample several intra-RTH points to be sure."""
    for hour, minute in [(9, 30), (10, 15), (12, 0), (15, 59), (16, 0), (19, 0), (23, 59)]:
        ts = _ET.localize(datetime(2025, 4, 15, hour, minute))
        assert cash_session_date(ts) == date(
            2025, 4, 15
        ), f"{hour:02d}:{minute:02d} ET on 2025-04-15 should be session 2025-04-15"


def test_cash_session_date_pre_cash_returns_prior_calendar_date():
    """Strictly before 09:30 ET on day D, the cash-session date is D-1
    (those timestamps belong to the prior session's extended-hours
    tail, before the vendor-side cumulative reset)."""
    for hour, minute in [(0, 0), (4, 0), (8, 0), (9, 0), (9, 29)]:
        ts = _ET.localize(datetime(2025, 4, 15, hour, minute))
        assert cash_session_date(ts) == date(
            2025, 4, 14
        ), f"{hour:02d}:{minute:02d} ET on 2025-04-15 should belong to session 2025-04-14"


def test_cash_session_date_boundary_09_30_exact_belongs_to_today():
    """09:30:00.000000 ET on day D is the FIRST instant of session D
    (>= comparison, not >).  This is the moment of the vendor reset."""
    ts = _ET.localize(datetime(2025, 4, 15, 9, 30, 0))
    assert cash_session_date(ts) == date(2025, 4, 15)


def test_cash_session_date_just_before_boundary_belongs_to_prior():
    """09:29:59.999999 ET is the LAST instant of the prior session.
    Pins the strict-less-than half of the boundary."""
    # 09:29 ET resolves to "prior session" via the minutes_since_midnight
    # check.  09:29:59.999... isn't representable exactly in pytz
    # localization (microsecond is the practical floor), so 09:29:59 is
    # the sharpest assertion we can cleanly write -- still strictly
    # before 09:30 and must map to the prior session.
    ts = _ET.localize(datetime(2025, 4, 15, 9, 29, 59, 999999))
    assert cash_session_date(ts) == date(2025, 4, 14)


def test_cash_session_date_naive_datetime_treated_as_utc():
    """A naive datetime is interpreted as UTC, then converted to ET.
    13:30 UTC == 09:30 EDT (during DST) == 08:30 EST (outside DST).
    Pins both regimes."""
    # 2025-04-15 (Tue, EDT, UTC-4): 13:30 UTC = 09:30 EDT -> session today.
    naive_dst = datetime(2025, 4, 15, 13, 30, 0)
    assert cash_session_date(naive_dst) == date(2025, 4, 15)
    # 2025-12-16 (Tue, EST, UTC-5): 13:30 UTC = 08:30 EST -> session
    # yesterday (Mon Dec 15).  Picking a Tuesday so the prior calendar
    # day is a trading day -- the weekend roll-back wouldn't otherwise
    # change the result.
    naive_no_dst = datetime(2025, 12, 16, 13, 30, 0)
    assert cash_session_date(naive_no_dst) == date(2025, 12, 15)


def test_cash_session_date_utc_input_converts_correctly():
    """Explicit UTC input is honored.  09:30 EDT == 13:30 UTC."""
    ts = datetime(2025, 4, 15, 13, 30, 0, tzinfo=timezone.utc)
    assert cash_session_date(ts) == date(2025, 4, 15)
    ts = datetime(2025, 4, 15, 13, 29, 0, tzinfo=timezone.utc)
    assert cash_session_date(ts) == date(2025, 4, 14)


def test_cash_session_date_handles_spring_forward_dst():
    """On the spring-forward DST transition the ET wall clock jumps from
    02:00 EST to 03:00 EDT.  09:30 ET resolves correctly across the
    transition (pytz handles the offset).

    The transition day itself (2025-03-09) is a Sunday, so any timestamp
    on that day rolls back to the prior Friday (2025-03-07) under the
    weekend-aware cash session date.  Use the Monday after to exercise
    DST correctness without the weekend roll-back masking it.
    """
    # 2025-03-10 is the Monday after DST start (EDT now in effect).
    # 09:30 EDT = 13:30 UTC.
    ts = datetime(2025, 3, 10, 13, 30, 0, tzinfo=timezone.utc)
    assert cash_session_date(ts) == date(2025, 3, 10)
    # 09:29 EDT on the same Monday = 13:29 UTC -> prior session.  The
    # naive prior-day arithmetic gives Sunday 2025-03-09 and the
    # weekend roll-back maps that to Friday 2025-03-07.
    ts = datetime(2025, 3, 10, 13, 29, 0, tzinfo=timezone.utc)
    assert cash_session_date(ts) == date(2025, 3, 7)


def test_cash_session_date_handles_fall_back_dst():
    """On the fall-back DST transition the ET wall clock jumps from
    02:00 EDT back to 01:00 EST.  09:30 ET resolves correctly across
    the transition.

    2025-11-02 is the Sunday on which DST ends.  Like the spring case,
    a Sunday timestamp rolls back to the prior Friday under the
    weekend-aware cash session date, so we exercise DST correctness on
    the Monday after.
    """
    # 2025-11-03 is the Monday after DST end.  09:30 EST = 14:30 UTC.
    ts = datetime(2025, 11, 3, 14, 30, 0, tzinfo=timezone.utc)
    assert cash_session_date(ts) == date(2025, 11, 3)
    # 09:29 EST on the same Monday = 14:29 UTC -> prior session.  The
    # weekend roll-back maps Sunday 2025-11-02 -> Friday 2025-10-31.
    ts = datetime(2025, 11, 3, 14, 29, 0, tzinfo=timezone.utc)
    assert cash_session_date(ts) == date(2025, 10, 31)


def test_cash_session_date_weekend_input_rolls_back_to_prior_friday():
    """Weekend timestamps must map to the prior Friday's cash session.

    The cash session that is "open" on a Sat/Sun timestamp began at the
    most recent Friday 09:30 ET cash open and runs until the next
    Monday 09:30 ET cash open.  Without rolling weekends back to that
    Friday, downstream consumers (the ingest accumulator's session
    key, the LAG-CASE in flow_contract_facts) hydrate a fresh
    accumulator at a non-trading-day "session" with no anchor rows
    and attribute Friday's residual cumulative as phantom flow at
    Mon 00:00 ET.  This is the bug behind the 2026-06-01 phantom
    midnight rows.  Pin the weekend roll-back so future callers can
    rely on it.
    """
    # Saturday 14:00 ET (post-09:30) -> prior Friday.
    sat_pm = _ET.localize(datetime(2025, 4, 19, 14, 0, 0))  # Sat
    assert cash_session_date(sat_pm) == date(2025, 4, 18)
    # Sunday 03:00 ET (pre-09:30) -> Saturday before the shift, Friday after.
    sun_early = _ET.localize(datetime(2025, 4, 20, 3, 0, 0))
    assert cash_session_date(sun_early) == date(2025, 4, 18)
    # Sunday 16:00 ET (post-09:30 but on a non-trading day) -> Friday.
    sun_pm = _ET.localize(datetime(2025, 4, 20, 16, 0, 0))
    assert cash_session_date(sun_pm) == date(2025, 4, 18)


def test_cash_session_date_monday_pre_open_rolls_back_to_prior_friday():
    """The flagship regression: Mon 00:00 ET -> prior Friday.

    Mon 00:00 ET is the timestamp that produced the 2026-06-01 phantom
    rows.  Before the weekend-aware roll-back it returned Sunday
    (via the plain ``timestamp - 9h30m`` arithmetic, which lands on
    Sun 14:30 ET); now it must return Friday.
    """
    # Replicates the production case directly.
    mon_midnight = _ET.localize(datetime(2026, 6, 1, 0, 0, 0))
    assert cash_session_date(mon_midnight) == date(2026, 5, 29)  # Fri
    # And anything in the Mon pre-open band stays on Friday.
    mon_pre_open = _ET.localize(datetime(2026, 6, 1, 9, 29, 59))
    assert cash_session_date(mon_pre_open) == date(2026, 5, 29)
    # Once the cash open hits, the date rolls to Monday.
    mon_open = _ET.localize(datetime(2026, 6, 1, 9, 30, 0))
    assert cash_session_date(mon_open) == date(2026, 6, 1)


def test_cash_session_date_month_boundary_pre_cash_returns_prior_month():
    """Pre-cash on the 1st of a month rolls back to the last day of the
    previous month.  Verifies the date arithmetic doesn't blow up on
    month/year transitions."""
    ts = _ET.localize(datetime(2025, 5, 1, 6, 0, 0))
    assert cash_session_date(ts) == date(2025, 4, 30)
    ts = _ET.localize(datetime(2025, 1, 1, 6, 0, 0))
    assert cash_session_date(ts) == date(2024, 12, 31)


# ---------------------------------------------------------------------------
# cash_session_start_utc
# ---------------------------------------------------------------------------


def test_cash_session_start_utc_returns_utc_aware():
    """The companion helper returns a tz-aware UTC datetime."""
    start = cash_session_start_utc(date(2025, 4, 15))
    assert start.tzinfo is not None
    assert start.utcoffset() == timedelta(0)


def test_cash_session_start_utc_edt_offset():
    """In EDT (UTC-4), 09:30 ET == 13:30 UTC."""
    start = cash_session_start_utc(date(2025, 4, 15))  # EDT
    assert start == datetime(2025, 4, 15, 13, 30, 0, tzinfo=timezone.utc)


def test_cash_session_start_utc_est_offset():
    """In EST (UTC-5), 09:30 ET == 14:30 UTC."""
    start = cash_session_start_utc(date(2025, 12, 15))  # EST
    assert start == datetime(2025, 12, 15, 14, 30, 0, tzinfo=timezone.utc)


def test_cash_session_start_utc_is_inverse_of_cash_session_date_during_rth():
    """For any RTH timestamp ts, cash_session_start_utc(cash_session_date(ts))
    must be <= ts and the gap must be <= 6h45m (full RTH session length).
    Smoke check that the two helpers are mutually consistent.

    Inputs must be on actual trading days; a weekend timestamp rolls
    back to the prior Friday, breaking the 6h45m gap bound.
    """
    # Pick weekdays in both DST regimes.  Tue Apr 15 2025 (EDT) and
    # Mon Nov 3 2025 (EST) -- avoids weekends.
    for d, h, m in [(date(2025, 4, 15), 10, 30), (date(2025, 11, 3), 12, 0)]:
        ts = _ET.localize(datetime(d.year, d.month, d.day, h, m))
        session_date = cash_session_date(ts)
        start = cash_session_start_utc(session_date)
        assert start <= ts.astimezone(timezone.utc)
        assert (ts.astimezone(timezone.utc) - start) <= timedelta(hours=6, minutes=45)


def test_cash_session_start_utc_invariant_under_dst_transition_day():
    """09:30 ET on the spring-forward day is 13:30 UTC (EDT).
    09:30 ET on the day before is 14:30 UTC (EST, UTC-5).  Verify the
    helper picks the right offset on each side of the transition."""
    # 2025-03-09 is the DST-start day in the US (EDT effective at 02:00).
    # Day before (2025-03-08) is still EST.
    before = cash_session_start_utc(date(2025, 3, 8))
    after = cash_session_start_utc(date(2025, 3, 9))
    assert before == datetime(2025, 3, 8, 14, 30, 0, tzinfo=timezone.utc)  # EST
    assert after == datetime(2025, 3, 9, 13, 30, 0, tzinfo=timezone.utc)  # EDT


# ---------------------------------------------------------------------------
# Future-consumer contract: cash_session_date is partition-stable
# ---------------------------------------------------------------------------


def test_cash_session_date_is_constant_across_a_single_cash_session():
    """Every timestamp from 09:30 ET on day D through 09:29:59 ET on day
    D+1 must return the same session_date == D.  This is the property
    that LAG-CASE consumers will rely on once they switch from
    calendar-day to cash-session partitioning."""
    d = date(2025, 4, 15)
    open_et = _ET.localize(datetime(d.year, d.month, d.day, 9, 30))
    # Sample 8 evenly-spaced points across the next 24 hours.
    for hours in [0, 3, 6, 12, 18, 23]:
        ts = open_et + timedelta(hours=hours)
        assert cash_session_date(ts) == d, f"ts={ts.isoformat()} should belong to session {d}"
    # And one just before the next day's 09:30 boundary.
    next_open_minus_one = (open_et + timedelta(days=1)) - timedelta(seconds=1)
    assert cash_session_date(next_open_minus_one) == d
    # While the next day's open belongs to the new session.
    next_open = open_et + timedelta(days=1)
    assert cash_session_date(next_open) == d + timedelta(days=1)
