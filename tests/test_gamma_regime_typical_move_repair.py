"""Unit tests for the causal typical_move_30m repair.

Nothing here can reach a database, so what is pinned is the part that would be
wrong in a way no amount of running it would reveal: that the pass computes the
SAME median the live engine does, and that it does not quietly cost twice what
it should.
"""

from datetime import date

from src.analytics.main_engine import AnalyticsEngine
from src.tools import gamma_regime_typical_move_repair as repair


def test_lookback_is_read_off_the_engine_not_copied():
    """A repair computing a different median than the live writer would be a
    second definition of 'typical move' wearing the same column name."""
    assert repair.LOOKBACK_DAYS is AnalyticsEngine.GAMMA_MOVE_LOOKBACK_DAYS
    assert repair.MIN_MINUTES is AnalyticsEngine.GAMMA_MOVE_MIN_MINUTES


def test_the_median_is_evaluated_once_per_bar():
    # The obvious way to write this UPDATE puts the median in both the SET and
    # the WHERE, which doubles the only expensive part of the pass.
    assert repair._UPDATE_SQL.count("percentile_cont") == 1
    assert repair._PREVIEW_SQL.count("percentile_cont") == 1


def test_lookback_uses_absolute_hours_not_calendar_days():
    """Postgres day arithmetic on a timestamptz is calendar arithmetic; the
    engine's timedelta is absolute. They disagree by an hour across a DST edge."""
    for sql in (repair._UPDATE_SQL, repair._PREVIEW_SQL):
        assert "INTERVAL '24 hours'" in sql
        assert "INTERVAL '1 day'" not in sql


def test_no_placeholder_sits_inside_a_quoted_literal():
    # '%(lookback)s days' renders correctly for an int and breaks for anything
    # else, which is the kind of bug that ships.
    for sql in (repair._UPDATE_SQL, repair._PREVIEW_SQL):
        assert "%(lookback)s days" not in sql
        assert "'%(" not in sql


def test_only_the_one_column_is_written():
    assert repair._UPDATE_SQL.count("SET ") == 1
    assert "SET typical_move_30m = s.val" in repair._UPDATE_SQL
    # The guard is what makes a second run a no-op on already-correct bars.
    assert "IS DISTINCT FROM" in repair._UPDATE_SQL


def test_preview_writes_nothing():
    upper = repair._PREVIEW_SQL.upper()
    for verb in ("UPDATE", "INSERT", "DELETE"):
        assert verb not in upper


def test_the_window_matches_the_engines_buckets():
    # Same 30-minute bucketing expression and the same minimum-minutes guard
    # the engine applies, or the two medians are computed over different
    # populations and the column means two things.
    for sql in (repair._UPDATE_SQL, repair._PREVIEW_SQL):
        assert "FLOOR(EXTRACT(MINUTE FROM uq.timestamp)::int / 30)" in sql
        assert "INTERVAL '30 minutes'" in sql
        assert "w.mins >= %(min_minutes)s" in sql
        assert "MAX(uq.high) - MIN(uq.low)" in sql


def test_inverted_range_is_rejected_before_touching_the_database(monkeypatch):
    called = []
    monkeypatch.setattr(repair, "repair_symbol", lambda *a, **k: called.append(a) or {})
    assert repair.main(["--symbols", "SPY", "--start", "2026-09-10", "--end", "2026-09-01"]) == 2
    assert called == []


def test_one_symbol_failing_does_not_abort_the_others(monkeypatch):
    seen = []

    def fake(symbol, start, end, dry_run=False):
        seen.append(symbol)
        if symbol == "SPY":
            raise RuntimeError("statement timeout")
        return {"days": 1, "changed": 4}

    monkeypatch.setattr(repair, "repair_symbol", fake)
    assert repair.main(["--symbols", "SPY,QQQ"]) == 0
    assert seen == ["SPY", "QQQ"]


def test_explicit_range_is_passed_through(monkeypatch):
    seen = {}

    def fake(symbol, start, end, dry_run=False):
        seen.update(symbol=symbol, start=start, end=end, dry=dry_run)
        return {"days": 0, "changed": 0}

    monkeypatch.setattr(repair, "repair_symbol", fake)
    assert (
        repair.main(
            ["--symbols", "SPY", "--start", "2026-07-29", "--end", "2026-09-08", "--dry-run"]
        )
        == 0
    )
    assert seen["start"] == date(2026, 7, 29)
    assert seen["end"] == date(2026, 9, 8)
    assert seen["dry"] is True
