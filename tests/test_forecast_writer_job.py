"""Tests for the morning forecast writer cron — content_hash determinism,
re-run idempotency, weekend skip, and dry-run safety."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.forecast_writer") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import forecast_writer  # noqa: WPS433

    return forecast_writer


def _gex_row(**overrides):
    base = {
        "spot_price": Decimal("600.00"),
        "call_wall": Decimal("606.00"),
        "put_wall": Decimal("594.00"),
        "gamma_flip": Decimal("600.50"),
        "max_pain": Decimal("599.00"),
    }
    base.update(overrides)
    return base


def _score_row(**overrides):
    base = {"composite_score": -0.32, "normalized_score": -32.0}
    base.update(overrides)
    return base


def _fake_db(*, gex=None, quote=None, score=None, cards=None, full_card=None,
             insert_result=None):
    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.get_latest_gex_summary = AsyncMock(return_value=gex if gex is not None else _gex_row())
    db.get_latest_quote = AsyncMock(
        return_value=quote if quote is not None else {"last": Decimal("600.0")}
    )
    db.get_latest_signal_score = AsyncMock(
        return_value=score if score is not None else _score_row()
    )
    db.get_action_cards_chronological = AsyncMock(return_value=cards or [])
    db.get_action_card_by_id = AsyncMock(return_value=full_card)
    db.insert_daily_forecast_morning = AsyncMock(return_value=insert_result)
    return db


@pytest.mark.asyncio
async def test_writer_skips_weekend(monkeypatch, caplog):
    mod = _reload_module()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: pytest.fail(
        "DatabaseManager must not be constructed on a weekend"
    ))
    args = mod._parse_args(["--date", "2026-06-27"])  # Saturday
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("not a trading day" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_writer_dry_run_logs_payload(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY", "--dry-run"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.insert_daily_forecast_morning.assert_not_called()
    assert any("DRY RUN" in r.message and "SPY" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_writer_commits_row_on_first_call(monkeypatch, caplog):
    mod = _reload_module()

    def _capture_insert(payload):
        return {**payload, "created_at": datetime(2026, 6, 29, 11, tzinfo=timezone.utc)}

    fake = _fake_db()
    fake.insert_daily_forecast_morning = AsyncMock(side_effect=lambda payload: _capture_insert(payload))
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    fake.insert_daily_forecast_morning.assert_called_once()
    payload = fake.insert_daily_forecast_morning.call_args.args[0]
    assert payload["symbol"] == "SPY"
    assert payload["regime"] == "short_gamma"
    assert payload["range_model"] == "heuristic_v1"
    assert len(payload["content_hash"]) == 64
    assert any("committed SPY" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_writer_logs_already_committed_when_hash_differs(monkeypatch, caplog):
    """If a row already exists for the day with a different content_hash
    (e.g. an inputs change between runs), the writer must log it as
    'already committed' rather than overwriting — the public commitment
    is immutable."""
    mod = _reload_module()
    fake = _fake_db()
    # Simulate the existing-row return path: insert_daily_forecast_morning
    # returns the pre-existing row with a different hash.
    fake.insert_daily_forecast_morning = AsyncMock(return_value={
        "symbol": "SPY",
        "date": date(2026, 6, 29),
        "content_hash": "b" * 64,
    })
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("already committed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_writer_skips_when_spot_missing(monkeypatch, caplog):
    mod = _reload_module()
    fake = _fake_db(quote={"last": None}, gex={"spot_price": None})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY"])
    with caplog.at_level("WARNING"):
        rc = await mod._run(args)
    assert rc == 0
    fake.insert_daily_forecast_morning.assert_not_called()


@pytest.mark.asyncio
async def test_writer_handles_multiple_symbols(monkeypatch):
    mod = _reload_module()
    fake = _fake_db()
    fake.insert_daily_forecast_morning = AsyncMock(side_effect=lambda payload: payload)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY,QQQ"])
    rc = await mod._run(args)
    assert rc == 0
    assert fake.insert_daily_forecast_morning.call_count == 2
    symbols = [
        c.args[0]["symbol"] for c in fake.insert_daily_forecast_morning.call_args_list
    ]
    assert symbols == ["SPY", "QQQ"]


@pytest.mark.asyncio
async def test_writer_swallows_db_connect_failure(monkeypatch, caplog):
    mod = _reload_module()

    class _BrokenDB:
        async def connect(self):
            raise RuntimeError("pool exhausted")
        async def disconnect(self):
            return None
    monkeypatch.setattr(mod, "DatabaseManager", lambda: _BrokenDB())
    args = mod._parse_args(["--date", "2026-06-29"])
    with caplog.at_level("WARNING"):
        rc = await mod._run(args)
    assert rc == 0  # Never raises — systemd should not see a failure.


def test_content_hash_excludes_open_ts():
    """The hash must be stable across re-runs of the same day, so it
    must NOT incorporate the wall-clock time of the write."""
    mod = _reload_module()
    payload_a = {
        "symbol": "SPY", "date": "2026-06-29",
        "open_spot": 600.0, "regime": "short_gamma",
        "projected_low": 593.4, "projected_high": 606.6,
        "range_model": "heuristic_v1",
    }
    payload_b = dict(payload_a)
    h_a = mod._content_hash(payload_a)
    h_b = mod._content_hash(payload_b)
    assert h_a == h_b
    # Same data, different value for an unrelated key → different hash.
    payload_b["projected_low"] = 593.5
    assert mod._content_hash(payload_b) != h_a
