"""The nightly wall-break-stats job: what it measures and what it refuses to.
The figure this job writes goes on a levels page next to a price, so the two
properties that matter most are that a thin sample is flagged rather than
rendered, and that the curve it stores is the same curve the study reports.
"""

from __future__ import annotations
from datetime import date, datetime, time, timedelta
from src.analytics.wall_breaks import ET, EventConfig, PriceBar, WallFrame
from src.tools import wall_break_stats_refresh as job

SESSION = date(2026, 6, 1)
WALL = 500.0
OPEN = datetime.combine(SESSION, time(9, 30), tzinfo=ET)


class _FakeCursor:
    """Serves the two SELECTs measure_symbol issues, in order."""

    def __init__(self, sessions, frames, bars):
        self._sessions, self._frames, self._bars = sessions, frames, bars
        self._rows: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if "DISTINCT" in sql:
            self._rows = [(s,) for s in self._sessions]
        elif "gex_summary" in sql:
            self._rows = [(f.ts, f.call_wall, f.put_wall) for f in self._frames]
        elif "underlying_quotes" in sql:
            self._rows = [(b.ts, b.high, b.low, b.close) for b in self._bars]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, sessions, frames, bars):
        self._args = (sessions, frames, bars)

    def cursor(self):
        return _FakeCursor(*self._args)


def _sustained_break_session():
    """One session where the wall is tested and then decisively broken."""
    closes = [WALL * 1.002 if m >= 30 else WALL * 0.998 for m in range(180)]
    bars = [
        PriceBar(
            ts=OPEN + timedelta(minutes=m),
            high=max(c, WALL * 0.9999),
            low=min(c, WALL * 0.9999),
            close=c,
        )
        for m, c in enumerate(closes)
    ]
    frames = [
        WallFrame(ts=OPEN + timedelta(minutes=m), call_wall=WALL, put_wall=WALL * 0.98)
        for m in range(180)
    ]
    return frames, bars


def test_measures_a_curve_with_one_row_per_horizon_and_side():
    frames, bars = _sustained_break_session()
    conn = _FakeConn([SESSION], frames, bars)
    result = job.measure_symbol(conn, "SPX", window=60)
    assert result.skipped is None
    horizons = {r["horizon_minutes"] for r in result.rows}
    assert horizons == set(job.HORIZONS)
    assert {r["side"] for r in result.rows} <= set(job.SIDES)
    # A single session cannot support a published figure.
    assert all(r["reportable"] is False for r in result.rows)


def test_a_thin_sample_is_flagged_rather_than_rendered():
    """A break probability from a handful of tests must not reach a levels
    page looking like the ones computed from hundreds."""
    frames, bars = _sustained_break_session()
    result = job.measure_symbol(_FakeConn([SESSION], frames, bars), "SPX", window=60)
    assert result.rows
    thin = result.rows[0]
    assert thin["n_tests"] < job.MIN_TESTS_TO_PUBLISH
    assert thin["reportable"] is False


def test_no_sessions_is_reported_not_invented():
    result = job.measure_symbol(_FakeConn([], [], []), "SPX", window=60)
    assert result.skipped == "no_sessions"
    assert result.rows == []


def test_horizons_are_ordered_and_bracket_a_zero_dte_hold():
    assert list(job.HORIZONS) == sorted(job.HORIZONS)
    assert (
        job.HORIZONS[0] >= EventConfig().confirm_minutes
    ), "a horizon shorter than the confirmation window can never show a break"


def test_upsert_is_a_no_op_on_empty_rows():
    class _Conn:
        def cursor(self):  # pragma: no cover - must not be reached
            raise AssertionError("no cursor should be opened for zero rows")

    assert job.upsert(_Conn(), []) == 0


def test_break_probability_is_monotone_across_stored_horizons():
    """Longer watch, more chances to break — a stored curve that falls with
    the horizon would mean the estimator or the join is wrong."""
    frames, bars = _sustained_break_session()
    result = job.measure_symbol(_FakeConn([SESSION], frames, bars), "SPX", window=60)
    pooled = sorted(
        (r for r in result.rows if r["side"] == "both" and r["break_prob"] is not None),
        key=lambda r: r["horizon_minutes"],
    )
    probs = [r["break_prob"] for r in pooled]
    assert probs == sorted(probs)
