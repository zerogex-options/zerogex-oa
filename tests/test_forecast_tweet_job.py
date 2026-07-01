"""Tests for src.jobs.forecast_tweet — the 07:10 morning + 16:10 receipt
tweet crons. Contract: never raises, dry-runs by default, skips silently
when the underlying daily_forecast row is missing or incomplete."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.forecast_tweet") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import forecast_tweet  # noqa: WPS433

    return forecast_tweet


def _morning_row(**overrides):
    base = {
        "symbol": "SPY",
        "date": date(2026, 7, 1),
        "open_ts": datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc),
        "open_spot": 746.77,
        "call_wall": 747.00,
        "put_wall": 745.00,
        "gamma_flip": 746.5,
        "open_msi": 12.73,
        "regime": "long_gamma",
        "projected_low": 744.82,
        "projected_high": 748.76,
        "projected_close": 744.00,
        "pin_strike": 744.00,
        "flagship_setup": None,
        "range_model": "heuristic_v1",
        "content_hash": "a" * 64,
        "receipt_ts": None,
        "actual_low": None,
        "actual_high": None,
        "actual_close": None,
        "range_respected": None,
        "pin_hit": None,
        "regime_correct": None,
    }
    base.update(overrides)
    return base


def _receipt_row(**overrides):
    row = _morning_row(
        receipt_ts=datetime(2026, 7, 1, 20, 5, tzinfo=timezone.utc),
        actual_low=745.10,
        actual_high=748.20,
        actual_close=746.90,
        range_respected=True,
        pin_hit=False,
        regime_correct=True,
    )
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Tweet-copy builders
# ---------------------------------------------------------------------------


def test_morning_tweet_shape():
    mod = _reload_module()
    text = mod.build_morning_tweet(_morning_row(), "https://zerogex.io")
    assert text.startswith("SPY · 2026-07-01 morning forecast")
    assert "Range: $744.82 – $748.76" in text
    assert "Pin: $744.00" in text
    assert "Regime: Long Gamma" in text
    assert text.endswith("https://zerogex.io/forecast/2026-07-01")
    assert len(text) <= mod.TWEET_MAX_LEN


def test_receipt_tweet_uses_verdict_pills():
    mod = _reload_module()
    text = mod.build_receipt_tweet(_receipt_row(), "https://zerogex.io")
    assert text.startswith("SPY · 2026-07-01 receipt")
    # Held / missed / correct verdicts must be individually visible.
    assert "Range ✓ held" in text
    assert "Pin ✗ missed" in text
    assert "Regime ✓ correct" in text
    assert "Close: $746.90" in text
    assert text.endswith("https://zerogex.io/forecast/2026-07-01")


def test_receipt_tweet_handles_transition_regime_gracefully():
    """regime_correct is NULL when the morning regime was 'transition' —
    the tweet should surface a neutral 'n/a' rather than pretend it graded."""
    mod = _reload_module()
    row = _receipt_row(regime="transition", regime_correct=None)
    text = mod.build_receipt_tweet(row, "https://zerogex.io")
    assert "Regime — n/a" in text
    assert "Regime ✗" not in text


def test_morning_tweet_truncates_to_280():
    mod = _reload_module()
    long_url = "https://" + "z" * 200 + ".io"
    text = mod.build_morning_tweet(_morning_row(), long_url)
    assert len(text) <= mod.TWEET_MAX_LEN
    # Permalink must survive intact — it's the whole point.
    assert text.endswith(f"{long_url}/forecast/2026-07-01")


# ---------------------------------------------------------------------------
# Runner behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skips_weekend_without_touching_db(monkeypatch, caplog):
    mod = _reload_module()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: pytest.fail(
        "DatabaseManager must not be constructed on a weekend"
    ))
    args = mod._parse_args(["--mode", "morning", "--date", "2026-07-04"])  # Saturday
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("not a trading day" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_morning_dry_run_logs_tweet(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None
        async def disconnect(self):
            return None
        async def get_daily_forecast(self, symbol, forecast_date):
            return _morning_row()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.delenv("X_BOT_BEARER_TOKEN", raising=False)
    args = mod._parse_args(["--mode", "morning", "--date", "2026-07-01"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("DRY RUN" in r.message for r in caplog.records)
    assert any("Range: $744.82" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_receipt_skips_when_receipt_not_yet_written(monkeypatch, caplog):
    """Race guard: if the 16:10 tweet fires before the 16:05 receipt
    writer somehow landed, we must not tweet an incomplete row."""
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None
        async def disconnect(self):
            return None
        async def get_daily_forecast(self, symbol, forecast_date):
            # Morning row exists but receipt_ts is NULL.
            return _morning_row()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "fake-token")
    posted = {"called": False}
    monkeypatch.setattr(
        mod, "post_tweet_via_x_api", lambda *a, **kw: posted.__setitem__("called", True),
    )
    args = mod._parse_args(["--mode", "receipt", "--date", "2026-07-01", "--post"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert posted["called"] is False
    assert any("receipt not written yet" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_skips_when_no_forecast_row(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None
        async def disconnect(self):
            return None
        async def get_daily_forecast(self, symbol, forecast_date):
            return None  # writer never fired

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    args = mod._parse_args(["--mode", "morning", "--date", "2026-07-01"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("no daily_forecast row" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_requires_post_flag_even_with_token(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None
        async def disconnect(self):
            return None
        async def get_daily_forecast(self, symbol, forecast_date):
            return _morning_row()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "fake-token")
    posted = {"called": False}
    monkeypatch.setattr(
        mod, "post_tweet_via_x_api", lambda *a, **kw: posted.__setitem__("called", True),
    )
    args = mod._parse_args(["--mode", "morning", "--date", "2026-07-01"])  # no --post
    rc = await mod._run(args)
    assert rc == 0
    assert posted["called"] is False


@pytest.mark.asyncio
async def test_swallows_db_failure(monkeypatch, caplog):
    mod = _reload_module()

    class _BrokenDB:
        async def connect(self):
            raise RuntimeError("pool down")
        async def disconnect(self):
            return None

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _BrokenDB())
    args = mod._parse_args(["--mode", "morning", "--date", "2026-07-01"])
    with caplog.at_level("WARNING"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("DB connect failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_swallows_x_api_failure(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None
        async def disconnect(self):
            return None
        async def get_daily_forecast(self, symbol, forecast_date):
            return _receipt_row()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "fake-token")

    def _explode(*a, **kw):
        from urllib.error import HTTPError
        raise HTTPError("https://api.x.com/2/tweets", 429, "Rate limited", {}, None)

    monkeypatch.setattr(mod, "post_tweet_via_x_api", _explode)
    args = mod._parse_args(["--mode", "receipt", "--date", "2026-07-01", "--post"])
    with caplog.at_level("WARNING"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("X API call failed" in r.message for r in caplog.records)
