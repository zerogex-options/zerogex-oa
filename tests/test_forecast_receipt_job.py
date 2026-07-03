"""Tests for the 4:05 PM ET forecast-receipt cron — graceful no-op when
the morning row is missing, OHLC fetch and verdict propagation when it
exists, and idempotent retries against the immutability trigger."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.forecast_receipt") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import forecast_receipt  # noqa: WPS433

    return forecast_receipt


def _session_closes_today(close=599.40):
    return {
        "symbol": "SPY",
        "current_session_close": Decimal(str(close)),
        "current_session_close_ts": datetime(2026, 6, 29, 20, 0, tzinfo=timezone.utc),
        "prior_session_close": Decimal("600.00"),
        "prior_session_close_ts": datetime(2026, 6, 26, 20, 0, tzinfo=timezone.utc),
    }


def _bars(*ohlc):
    """ohlc is a list of (open, high, low, close) tuples."""
    out = []
    for i, (o, h, low, c) in enumerate(ohlc):
        out.append({
            "timestamp": datetime(2026, 6, 29, 13 + i, 30, tzinfo=timezone.utc),
            "open": Decimal(str(o)),
            "high": Decimal(str(h)),
            "low": Decimal(str(low)),
            "close": Decimal(str(c)),
        })
    return out


def _fake_db(*, closes=None, bars=None, update_result=None):
    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.get_session_closes = AsyncMock(
        return_value=closes if closes is not None else _session_closes_today()
    )
    db.get_underlying_bars_for_session = AsyncMock(
        return_value=bars if bars is not None else _bars((600, 605, 595, 599))
    )
    db.update_daily_forecast_receipt = AsyncMock(return_value=update_result)
    return db


@pytest.mark.asyncio
async def test_receipt_skips_weekend(monkeypatch, caplog):
    mod = _reload_module()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: pytest.fail(
        "DatabaseManager must not be constructed on a weekend"
    ))
    args = mod._parse_args(["--date", "2026-06-27"])  # Saturday
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0


@pytest.mark.asyncio
async def test_receipt_writes_verdict_columns(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db(update_result={
        "range_respected": True, "pin_hit": True, "regime_correct": True,
    })
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.update_daily_forecast_receipt.assert_called_once()
    call = fake.update_daily_forecast_receipt.call_args
    assert call.kwargs["symbol"] == "SPY"
    assert call.kwargs["actual_low"] == 595.0
    assert call.kwargs["actual_high"] == 605.0
    assert call.kwargs["actual_close"] == 599.40
    assert any("wrote receipt for SPY" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_receipt_dry_run_logs_only(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--dry-run"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.update_daily_forecast_receipt.assert_not_called()
    assert any("DRY RUN" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_receipt_skips_when_session_close_mismatches_date(monkeypatch, caplog):
    """If the session-close endpoint reports a different date than the
    one we're grading (e.g. the cron fired before today's bar landed),
    the receipt must skip rather than write a stale row."""
    mod = _reload_module()
    fake = _fake_db(closes={
        "symbol": "SPY",
        "current_session_close": Decimal("600.0"),
        "current_session_close_ts": datetime(2026, 6, 26, 20, 0, tzinfo=timezone.utc),
        "prior_session_close": Decimal("599.0"),
        "prior_session_close_ts": datetime(2026, 6, 25, 20, 0, tzinfo=timezone.utc),
    })
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.update_daily_forecast_receipt.assert_not_called()


@pytest.mark.asyncio
async def test_receipt_skips_when_no_morning_row(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db(update_result=None)  # update returns None when no morning row
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.update_daily_forecast_receipt.assert_called_once()
    assert any("no morning row" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_receipt_falls_back_when_bars_missing(monkeypatch):
    """If intraday bars are missing, the receipt uses the close as a
    degenerate low/high so the writer still records something."""
    mod = _reload_module()
    fake = _fake_db(bars=[], update_result={"range_respected": False, "pin_hit": True})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29"])
    rc = await mod._run(args)
    assert rc == 0
    call = fake.update_daily_forecast_receipt.call_args
    assert call.kwargs["actual_low"] == call.kwargs["actual_high"] == call.kwargs["actual_close"]


@pytest.mark.asyncio
async def test_receipt_swallows_db_failure(monkeypatch, caplog):
    mod = _reload_module()

    class _BrokenDB:
        async def connect(self):
            raise RuntimeError("db down")
        async def disconnect(self):
            return None
    monkeypatch.setattr(mod, "DatabaseManager", lambda: _BrokenDB())
    args = mod._parse_args(["--date", "2026-06-29"])
    with caplog.at_level("WARNING"):
        rc = await mod._run(args)
    assert rc == 0
