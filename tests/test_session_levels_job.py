"""Tests for the session-levels capture cron (pre-market + previous-session
high/low) — weekend/index skips, the underlying_quotes fallback path, the
TradeStation union path, dry-run, and the API-layer captured/live merge."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.session_levels") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import session_levels  # noqa: WPS433

    return session_levels


def _live_levels(
    pm_high=None,
    pm_low=None,
    prev_high=None,
    prev_low=None,
    prev_date=None,
):
    return {
        "premarket_high": pm_high,
        "premarket_low": pm_low,
        "prev_session_date": prev_date,
        "prev_session_high": prev_high,
        "prev_session_low": prev_low,
    }


def _fake_db(live=None):
    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.compute_live_session_levels = AsyncMock(
        return_value=live if live is not None else _live_levels()
    )
    db.ensure_symbol_row = AsyncMock(return_value=None)
    db.upsert_session_levels = AsyncMock(return_value=None)
    return db


class _FakeTsClient:
    """Records get_bars calls and returns canned per-session-template bars."""

    def __init__(self, premarket_bars=None, rth_bars=None):
        self.premarket_bars = premarket_bars if premarket_bars is not None else []
        self.rth_bars = rth_bars if rth_bars is not None else []
        self.calls = []

    def get_bars(self, symbol, interval, unit, **kwargs):
        self.calls.append((symbol, interval, unit, kwargs))
        # The pre-market fetch is the USEQ24Hour one; the previous-session
        # fetch uses Default. (USEQPre is deliberately NOT used — it serves
        # from 06:00 ET, missing the first two hours of the pre-market.)
        if kwargs.get("sessiontemplate") == "USEQ24Hour":
            return {"Bars": self.premarket_bars}
        return {"Bars": self.rth_bars}

    def close_all_streams(self):
        pass


@pytest.mark.asyncio
async def test_skips_weekend(monkeypatch):
    mod = _reload_module()
    monkeypatch.setattr(
        mod,
        "DatabaseManager",
        lambda: pytest.fail("DatabaseManager must not be constructed on a weekend"),
    )
    args = mod._parse_args(["--date", "2026-06-27"])  # Saturday
    rc = await mod._run(args)
    assert rc == 0


@pytest.mark.asyncio
async def test_skips_cash_index(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbols", "SPX", "--no-tradestation"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.upsert_session_levels.assert_not_called()
    assert any("cash index" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_captures_from_underlying_quotes_fallback(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db(
        live=_live_levels(
            pm_high=Decimal("625.40"),
            pm_low=Decimal("622.15"),
            prev_high=Decimal("626.28"),
            prev_low=Decimal("621.40"),
            prev_date=date(2026, 6, 26),
        )
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbols", "SPY", "--no-tradestation"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.upsert_session_levels.assert_called_once()
    kwargs = fake.upsert_session_levels.call_args.kwargs
    assert kwargs["symbol"] == "SPY"
    assert kwargs["trading_date"] == date(2026, 6, 29)
    assert kwargs["premarket_high"] == 625.40
    assert kwargs["premarket_low"] == 622.15
    assert kwargs["premarket_source"] == "underlying_quotes"
    assert kwargs["prev_session_date"] == date(2026, 6, 26)
    assert kwargs["prev_session_high"] == 626.28
    assert kwargs["prev_session_low"] == 621.40
    assert kwargs["prev_session_source"] == "underlying_quotes"


@pytest.mark.asyncio
async def test_dry_run_never_writes(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db(live=_live_levels(pm_high=Decimal("625.40"), pm_low=Decimal("622.15")))
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(
        ["--date", "2026-06-29", "--symbols", "SPY", "--no-tradestation", "--dry-run"]
    )
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.upsert_session_levels.assert_not_called()
    fake.ensure_symbol_row.assert_not_called()
    assert any("DRY RUN" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_no_data_writes_nothing(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db(live=_live_levels())
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbols", "SPY", "--no-tradestation"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.upsert_session_levels.assert_not_called()
    assert any("nothing to write" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_tradestation_union_and_prev_session_precedence(monkeypatch):
    """TS pre-market bars union with the DB aggregate (max high / min low);
    the TS previous-session capture wins over the DB fallback and targets
    the calendar previous trading day (Friday for a Monday run)."""
    mod = _reload_module()
    fake = _fake_db(
        live=_live_levels(
            pm_high=Decimal("625.40"),
            pm_low=Decimal("622.15"),
            prev_high=Decimal("626.28"),
            prev_low=Decimal("621.40"),
            prev_date=date(2026, 6, 26),
        )
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)

    # 2026-06-29 is a Monday; 04:04 ET = 08:04Z (EDT). One pre-market bar
    # extends the DB high; the RTH bars carry a wider Friday session plus a
    # 16:00 auction bar whose open sets the session low.
    client = _FakeTsClient(
        premarket_bars=[
            {
                "TimeStamp": "2026-06-29T08:04:00Z",
                "Open": "624.0",
                "High": "626.10",
                "Low": "623.00",
                "Close": "624.5",
            },
            # Out-of-window bar (09:30 ET = RTH open) must be ignored.
            {
                "TimeStamp": "2026-06-29T13:30:00Z",
                "Open": "630.0",
                "High": "699.00",
                "Low": "1.00",
                "Close": "630.0",
            },
        ],
        rth_bars=[
            {
                "TimeStamp": "2026-06-26T13:30:00Z",
                "Open": "624.0",
                "High": "627.90",
                "Low": "620.80",
                "Close": "625.0",
            },
            # 16:00 ET auction bar: only its open participates.
            {
                "TimeStamp": "2026-06-26T20:00:00Z",
                "Open": "620.10",
                "High": "640.00",
                "Low": "600.00",
                "Close": "626.0",
            },
        ],
    )
    monkeypatch.setattr(mod, "_build_ts_client", lambda: client)

    args = mod._parse_args(["--date", "2026-06-29", "--symbols", "SPY"])
    rc = await mod._run(args)
    assert rc == 0

    kwargs = fake.upsert_session_levels.call_args.kwargs
    # Pre-market: TS high 626.10 beats DB 625.40; DB low 622.15 survives.
    assert kwargs["premarket_high"] == 626.10
    assert kwargs["premarket_low"] == 622.15
    assert kwargs["premarket_source"] == "tradestation+underlying_quotes"
    # Previous session: TS wins — high from the RTH bar, low from the
    # auction print (16:00 bar's open), NOT its AH-contaminated high/low.
    assert kwargs["prev_session_date"] == date(2026, 6, 26)
    assert kwargs["prev_session_high"] == 627.90
    assert kwargs["prev_session_low"] == 620.10
    assert kwargs["prev_session_source"] == "tradestation"

    # Both fetches used the expected session templates. The pre-market leg
    # must NOT use USEQPre: that template serves from 06:00 ET, so it drops
    # 04:00-06:00 from the window this job measures.
    templates = [c[3].get("sessiontemplate") for c in client.calls]
    assert templates == ["USEQ24Hour", "Default"]


def test_prev_trading_day_skips_weekend_and_holiday(monkeypatch):
    mod = _reload_module()
    # Monday → Friday.
    assert mod._prev_trading_day(date(2026, 6, 29)) == date(2026, 6, 26)
    # With Friday marked as a holiday, Monday → Thursday.
    monkeypatch.setattr(mod, "NYSE_HOLIDAYS", {date(2026, 6, 26)})
    assert mod._prev_trading_day(date(2026, 6, 29)) == date(2026, 6, 25)


def test_bars_window_extremes_filters_by_date_and_window():
    mod = _reload_module()
    bars = [
        # In-window (04:04 ET).
        {"TimeStamp": "2026-06-29T08:04:00Z", "High": "625.0", "Low": "623.0"},
        # Wrong date.
        {"TimeStamp": "2026-06-26T08:04:00Z", "High": "999.0", "Low": "1.0"},
        # At the window end (09:30 ET) — excluded, RTH bar.
        {"TimeStamp": "2026-06-29T13:30:00Z", "High": "999.0", "Low": "1.0"},
        # Malformed values are skipped.
        {"TimeStamp": "2026-06-29T08:05:00Z", "High": "n/a", "Low": None},
        {"TimeStamp": None, "High": "999.0", "Low": "1.0"},
    ]
    high, low = mod._bars_window_extremes(
        bars, date(2026, 6, 29), mod.PREMARKET_START, mod.PREMARKET_END
    )
    assert high == 625.0
    assert low == 623.0


def test_default_symbols_falls_back_to_ingest_universe(monkeypatch):
    mod = _reload_module()
    monkeypatch.delenv("SESSION_LEVELS_SYMBOLS", raising=False)
    monkeypatch.setenv("INGEST_UNDERLYINGS", "SPY,SPX")
    assert mod._default_symbols() == "SPY,SPX"
    monkeypatch.setenv("SESSION_LEVELS_SYMBOLS", "QQQ")
    assert mod._default_symbols() == "QQQ"
    monkeypatch.delenv("SESSION_LEVELS_SYMBOLS", raising=False)
    monkeypatch.delenv("INGEST_UNDERLYINGS", raising=False)
    monkeypatch.setenv("INGEST_UNDERLYING", "IWM")
    assert mod._default_symbols() == "IWM"


# ---------------------------------------------------------------------------
# API-layer merge (DatabaseManager._merge_session_levels)
# ---------------------------------------------------------------------------


def _manager_cls():
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api.database import DatabaseManager  # noqa: WPS433

    return DatabaseManager


def _captured_row(
    trading_date,
    pm_high=Decimal("625.40"),
    pm_low=Decimal("622.15"),
    prev_high=Decimal("626.28"),
    prev_low=Decimal("621.40"),
    prev_date=date(2026, 6, 26),
):
    return {
        "trading_date": trading_date,
        "premarket_high": pm_high,
        "premarket_low": pm_low,
        "prev_session_date": prev_date,
        "prev_session_high": prev_high,
        "prev_session_low": prev_low,
        "updated_at": datetime(2026, 6, 29, 13, 55, tzinfo=timezone.utc),
    }


def test_merge_prefers_todays_captured_row_and_unions_live_premarket():
    cls = _manager_cls()
    today = date(2026, 6, 29)
    live = {
        "premarket_high": Decimal("626.00"),
        "premarket_low": Decimal("623.00"),
        "prev_session_date": date(2026, 6, 26),
        "prev_session_high": Decimal("999.00"),  # must NOT leak into result
        "prev_session_low": Decimal("1.00"),
    }
    out = cls._merge_session_levels("SPY", today, _captured_row(today), live)
    assert out["premarket_high"] == 626.00  # live high extends the captured one
    assert out["premarket_low"] == 622.15  # captured low survives
    assert out["prev_session_high"] == 626.28  # prev-session stays captured
    assert out["prev_session_low"] == 621.40
    assert out["source"] == "captured+live"
    assert out["trading_date"] == today


def test_merge_falls_back_to_live_when_row_is_stale():
    cls = _manager_cls()
    today = date(2026, 6, 29)
    live = {
        "premarket_high": Decimal("626.00"),
        "premarket_low": Decimal("623.00"),
        "prev_session_date": date(2026, 6, 26),
        "prev_session_high": Decimal("627.00"),
        "prev_session_low": Decimal("620.00"),
    }
    stale = _captured_row(date(2026, 6, 26))
    out = cls._merge_session_levels("SPY", today, stale, live)
    assert out["source"] == "live"
    assert out["trading_date"] == today
    assert out["premarket_high"] == 626.00
    assert out["prev_session_high"] == 627.00


def test_merge_serves_stale_captured_row_when_no_live_data():
    cls = _manager_cls()
    today = date(2026, 6, 29)  # e.g. weekend/holiday read: no live aggregate
    stale = _captured_row(date(2026, 6, 26))
    out = cls._merge_session_levels("SPY", today, stale, None)
    assert out["source"] == "captured"
    assert out["trading_date"] == date(2026, 6, 26)
    assert out["premarket_high"] == 625.40


def test_merge_returns_none_when_nothing_available():
    cls = _manager_cls()
    assert cls._merge_session_levels("SPY", date(2026, 6, 29), None, None) is None
    empty_live = {
        "premarket_high": None,
        "premarket_low": None,
        "prev_session_date": None,
        "prev_session_high": None,
        "prev_session_low": None,
    }
    assert cls._merge_session_levels("SPY", date(2026, 6, 29), None, empty_live) is None
