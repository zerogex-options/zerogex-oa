"""Unit tests for the gamma_regime_5min backfill.

The tool's whole job is to point the engine's own writer at a past session, so
what can go wrong is not arithmetic. It is aiming at the wrong day, walking
sessions the source data cannot reach, or racing the live engine for today.
Those are what is pinned here; the bar math belongs to the engine and is
tested with it.
"""

from datetime import date, timedelta


from src.analytics.main_engine import ET as ENGINE_ET
from src.tools.gamma_regime_5min_backfill import (
    SESSION_BARS,
    _session_noon,
    main,
    trading_days,
)


def test_session_noon_lands_on_its_own_et_day():
    """The engine takes the ET calendar day off the timestamp it is handed.

    An hour that resolved to the previous or next ET day would silently
    backfill the wrong session, and every bar written would still look
    perfectly well-formed.
    """
    day = date(2026, 7, 29)
    while day <= date(2026, 12, 31):
        resolved = _session_noon(day).astimezone(ENGINE_ET).date()
        assert resolved == day, f"{day} resolved to {resolved}"
        day += timedelta(days=1)


def test_session_noon_survives_both_dst_transitions():
    # Spring forward (2026-03-08) and fall back (2026-11-01). Noon is chosen
    # precisely so neither can land on a skipped or doubled wall-clock hour.
    for day in (date(2026, 3, 8), date(2026, 11, 1)):
        ts = _session_noon(day)
        assert ts.astimezone(ENGINE_ET).date() == day
        assert ts.utcoffset() is not None

    # And the offsets really do differ either side of the boundary, so the
    # test is exercising the transition rather than a fixed-offset zone.
    assert (
        _session_noon(date(2026, 7, 1)).utcoffset() != _session_noon(date(2026, 12, 1)).utcoffset()
    )


def test_trading_days_drops_weekends_and_keeps_holidays():
    # 2026-07-29 is a Wednesday; the window spans one weekend.
    days = trading_days(date(2026, 7, 29), date(2026, 8, 4))
    assert days == [
        date(2026, 7, 29),
        date(2026, 7, 30),
        date(2026, 7, 31),
        date(2026, 8, 3),
        date(2026, 8, 4),
    ]
    assert all(d.weekday() < 5 for d in days)

    # Labor Day is still in the list. The engine returns without writing when
    # the session open has no chain, which is what makes carrying a market
    # calendar here unnecessary rather than merely inconvenient.
    assert date(2026, 9, 7) in trading_days(date(2026, 9, 4), date(2026, 9, 8))


def test_trading_days_is_inclusive_and_empty_when_inverted():
    assert trading_days(date(2026, 8, 3), date(2026, 8, 3)) == [date(2026, 8, 3)]
    assert trading_days(date(2026, 8, 5), date(2026, 8, 3)) == []


def test_session_bars_matches_the_engine_grid():
    # 09:30 to 16:15 ET inclusive on the 5-minute grid, which is the window
    # the engine walks. A wrong constant here would mark every complete
    # session incomplete and rebuild the entire window on every run.
    assert SESSION_BARS == int(timedelta(hours=6, minutes=45) / timedelta(minutes=5)) + 1


def test_inverted_window_is_rejected_before_touching_the_database(monkeypatch):
    called = []
    monkeypatch.setattr(
        "src.tools.gamma_regime_5min_backfill.backfill_symbol",
        lambda *a, **k: called.append(a) or {},
    )
    assert main(["--symbols", "SPY", "--start", "2026-09-10", "--end", "2026-09-01"]) == 2
    assert called == []


def test_default_window_stops_before_today(monkeypatch):
    """Today belongs to the live engine.

    Its newest bar is still filling and the engine rewrites it every cycle; a
    backfill reaching into it would rewrite that bar underneath the page.
    """
    seen = {}

    def fake(symbol, start, end, dry_run=False, sleep=0.0):
        seen["start"], seen["end"] = start, end
        return {"symbol": symbol, "days": 0, "written": 0, "already": 0, "empty": 0}

    monkeypatch.setattr("src.tools.gamma_regime_5min_backfill.backfill_symbol", fake)
    assert main(["--symbols", "SPY", "--days", "30"]) == 0
    assert seen["end"] < date.today()
    assert seen["start"] == seen["end"] - timedelta(days=30)


def test_explicit_end_is_honored(monkeypatch):
    seen = {}

    def fake(symbol, start, end, dry_run=False, sleep=0.0):
        seen["start"], seen["end"], seen["dry"] = start, end, dry_run
        return {"symbol": symbol, "days": 0, "written": 0, "already": 0, "empty": 0}

    monkeypatch.setattr("src.tools.gamma_regime_5min_backfill.backfill_symbol", fake)
    assert (
        main(["--symbols", "SPY", "--start", "2026-08-01", "--end", "2026-08-31", "--dry-run"]) == 0
    )
    assert (seen["start"], seen["end"]) == (date(2026, 8, 1), date(2026, 8, 31))
    assert seen["dry"] is True


def test_one_symbol_failing_does_not_abort_the_others(monkeypatch):
    seen = []

    def fake(symbol, start, end, dry_run=False, sleep=0.0):
        seen.append(symbol)
        if symbol == "SPY":
            raise RuntimeError("slow query")
        return {"symbol": symbol, "days": 1, "written": 82, "already": 0, "empty": 0}

    monkeypatch.setattr("src.tools.gamma_regime_5min_backfill.backfill_symbol", fake)
    assert main(["--symbols", "SPY,QQQ", "--start", "2026-08-01", "--end", "2026-08-02"]) == 0
    assert seen == ["SPY", "QQQ"]
