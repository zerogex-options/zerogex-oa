"""Tests for the pre-market short-circuit guards in the standalone flow
cycle's two downstream refreshes.

When the flow refresh was a stage of ``run_calculation``, the cycle-skip
optimisations there gated it off pre-market — so neither
``_refresh_flow_caches`` nor ``_refresh_flow_series_snapshot`` ever ran
with a degenerate (pre-09:30 ET) wall-clock. After the architectural
fix that extracted them into ``_run_flow_cycle``, both run on every
loop iteration including pre-market, where:

* ``_refresh_flow_series_snapshot`` would clamp ``session_end`` up to
  ``session_start`` and then enter the cold-start branch with a
  0-duration window — running the heavy 8-CTE upsert SQL for 0 rows
  and logging a misleading "cold-start or gap detected" line every
  cycle.
* ``_refresh_flow_caches`` would compute pre-session bucket targets
  that the ``WHERE bucket_start >= session_open`` clause filters out,
  again running the multi-CTE INSERT for 0 rows.

Both functions now short-circuit before issuing the heavy SQL.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _engine() -> AnalyticsEngine:
    eng = AnalyticsEngine(underlying="SPY")
    eng._analytics_flow_cache_refresh_enabled = True
    return eng


def _mock_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, conn


def _mock_cursor():
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.rowcount = 0
    return cursor


def _frozen_datetime(at_utc: datetime):
    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return at_utc.astimezone(tz) if tz is not None else at_utc.replace(tzinfo=None)

    return _FrozenDateTime


def test_snapshot_refresh_short_circuits_premarket():
    """At 09:25 ET on Tuesday, wall-clock is 5 minutes before the
    session opens. session_end (09:25 ET) < session_start (09:30 ET) —
    the function must return without issuing any SQL or logging the
    "cold-start" line."""
    engine = _engine()
    # Anchor timestamp's ET date is today (Tue) — typical pre-market
    # for SPY/QQQ once NULL-Greek option_chains rows start arriving.
    anchor = datetime(2026, 6, 16, 13, 19, tzinfo=timezone.utc)  # 09:19 ET Tue
    pre_open_wallclock = datetime(2026, 6, 16, 13, 25, tzinfo=timezone.utc)  # 09:25 ET Tue

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with (
        patch.object(main_engine, "db_connection", return_value=cm),
        patch.object(main_engine, "datetime", _frozen_datetime(pre_open_wallclock)),
    ):
        engine._refresh_flow_series_snapshot(anchor)

    cursor.execute.assert_not_called()  # no probe, no upsert


def test_snapshot_refresh_runs_at_session_open_boundary():
    """Boundary check: 09:30:00 ET exactly. session_end == session_start
    (equal, NOT less than) — the function must proceed normally so the
    first bar of the session lands without a 5-min delay."""
    engine = _engine()
    open_ts = datetime(2026, 6, 16, 13, 30, tzinfo=timezone.utc)  # 09:30 ET Tue

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with (
        patch.object(main_engine, "db_connection", return_value=cm),
        patch.object(main_engine, "datetime", _frozen_datetime(open_ts)),
    ):
        engine._refresh_flow_series_snapshot(open_ts)

    # At least the prev_bar probe should have run — the function did
    # not short-circuit on the boundary case.
    assert cursor.execute.called, (
        "session_end == session_start (09:30 ET sharp) is the first-bar "
        "boundary, not pre-market; the function must NOT short-circuit"
    )


def test_snapshot_refresh_runs_during_rth():
    """Sanity: mid-session, the function proceeds and issues SQL."""
    engine = _engine()
    anchor = datetime(2026, 6, 16, 17, 30, tzinfo=timezone.utc)  # 13:30 ET Tue
    mid_rth = datetime(2026, 6, 16, 17, 30, tzinfo=timezone.utc)

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with (
        patch.object(main_engine, "db_connection", return_value=cm),
        patch.object(main_engine, "datetime", _frozen_datetime(mid_rth)),
    ):
        engine._refresh_flow_series_snapshot(anchor)

    assert cursor.execute.called


def test_snapshot_refresh_runs_post_close_with_yesterday_anchor():
    """Overnight: anchor is yesterday's last data; wall-clock is today
    pre-open. session_start = yesterday 09:30 ET, session_end caps at
    yesterday 16:15 ET. session_end (Mon 16:15) > session_start
    (Mon 09:30) — function must proceed so any tail-bar gaps from a
    crashed previous instance can be backfilled overnight."""
    engine = _engine()
    yesterday_anchor = datetime(2026, 6, 15, 22, 0, tzinfo=timezone.utc)  # Mon 18:00 ET
    pre_open_today = datetime(2026, 6, 16, 13, 25, tzinfo=timezone.utc)  # Tue 09:25 ET

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with (
        patch.object(main_engine, "db_connection", return_value=cm),
        patch.object(main_engine, "datetime", _frozen_datetime(pre_open_today)),
    ):
        engine._refresh_flow_series_snapshot(yesterday_anchor)

    assert cursor.execute.called, (
        "overnight cycles must still run so Monday's missing tail bars " "get backfilled on restart"
    )


def test_flow_caches_refresh_short_circuits_premarket():
    """At 09:25 ET Tuesday with a Tuesday-dated anchor, both bucket
    targets (08:15 and 08:20 ET) sit before session_open (09:30 ET),
    so the WHERE clause would filter them out. The function must
    return before issuing the multi-CTE INSERT."""
    engine = _engine()
    pre_open = datetime(2026, 6, 16, 13, 25, tzinfo=timezone.utc)  # 09:25 ET Tue

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_caches(pre_open, underlying_price=500.0)

    cursor.execute.assert_not_called()


def test_flow_caches_refresh_runs_at_session_open_boundary():
    """09:30 ET sharp: curr_bucket_start == session_open. Not strictly
    less than, so the short-circuit must NOT fire."""
    engine = _engine()
    open_ts = datetime(2026, 6, 16, 13, 30, tzinfo=timezone.utc)  # 09:30 ET Tue

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_caches(open_ts, underlying_price=500.0)

    assert cursor.execute.called, (
        "the 09:30 ET boundary is when the first bucket can take rows; "
        "the function must NOT short-circuit"
    )


def test_flow_caches_refresh_runs_during_rth():
    """Mid-session: cur_bucket > session_open, function dispatches."""
    engine = _engine()
    mid_rth = datetime(2026, 6, 16, 17, 30, tzinfo=timezone.utc)  # 13:30 ET Tue

    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_caches(mid_rth, underlying_price=500.0)

    assert cursor.execute.called


def test_flow_caches_refresh_short_circuit_respects_throttle_advance():
    """Pre-market returns must NOT update the in-memory throttle state,
    otherwise the first valid post-open cycle could be silenced by the
    just-incremented throttle clock."""
    engine = _engine()
    pre_open = datetime(2026, 6, 16, 13, 25, tzinfo=timezone.utc)
    cursor = _mock_cursor()
    cm, _ = _mock_conn(cursor)

    last_ts_before = engine._last_flow_cache_ts
    last_mono_before = engine._last_flow_cache_refresh_mono

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_flow_caches(pre_open, underlying_price=500.0)

    assert engine._last_flow_cache_ts == last_ts_before
    assert engine._last_flow_cache_refresh_mono == last_mono_before
