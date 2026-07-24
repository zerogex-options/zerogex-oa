"""Tests for src.api.routers.admin_xpost — the /admin/x-post review endpoints.

Exercises the endpoint handlers directly (the require_admin/API-key gate is
covered by the shared security tests); here we assert the symbols/latest/
regenerate logic, timing resolution, and validation."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.api.routers import admin_xpost as ax
from src.jobs import bulletin_tweet as bt


@pytest.mark.asyncio
async def test_symbols_endpoint_reads_env(monkeypatch):
    monkeypatch.setenv("BULLETIN_TWEET_SYMBOLS", "SPY,QQQ,SPX")
    monkeypatch.setenv("BULLETIN_TWEET_LEAD_SYMBOL", "QQQ")
    out = await ax.symbols()
    assert out["symbols"] == ["SPY", "QQQ", "SPX"]
    assert out["default"] == "QQQ"


@pytest.mark.asyncio
async def test_symbols_endpoint_defaults_to_spy(monkeypatch):
    monkeypatch.delenv("BULLETIN_TWEET_SYMBOLS", raising=False)
    monkeypatch.delenv("BULLETIN_TWEET_LEAD_SYMBOL", raising=False)
    out = await ax.symbols()
    assert out["symbols"] == ["SPY"]
    assert out["default"] == "SPY"


@pytest.mark.asyncio
async def test_latest_resolves_timing_and_reads_record(monkeypatch):
    monkeypatch.setattr(bt, "resolve_current_mode", lambda: "midday")
    monkeypatch.setattr(
        bt,
        "read_latest_record",
        lambda sym, mode: {"symbol": sym, "mode": mode, "post_text": "hi"},
    )
    out = await ax.latest(symbol=None, mode=None)
    assert out["mode"] == "midday"
    assert out["timing_label"] == "Midday Read"
    assert out["symbol"] == "SPY"
    assert out["record"]["post_text"] == "hi"


@pytest.mark.asyncio
async def test_latest_null_record_when_absent(monkeypatch):
    monkeypatch.setattr(bt, "read_latest_record", lambda sym, mode: None)
    out = await ax.latest(symbol="SPY", mode="close")
    assert out["record"] is None
    assert out["timing_label"] == "Post-Market Read"


@pytest.mark.asyncio
async def test_latest_rejects_unknown_mode():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as ex:
        await ax.latest(symbol="SPY", mode="brunch")
    assert ex.value.status_code == 400


@pytest.mark.asyncio
async def test_regenerate_rejects_unconfigured_symbol(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setenv("BULLETIN_TWEET_SYMBOLS", "SPY")
    body = ax.RegenerateRequest(symbol="TSLA", mode="midday")
    with pytest.raises(HTTPException) as ex:
        await ax.regenerate(body, db=object())
    assert ex.value.status_code == 400


@pytest.mark.asyncio
async def test_regenerate_calls_generate_and_store(monkeypatch):
    monkeypatch.setenv("BULLETIN_TWEET_SYMBOLS", "SPY")
    fake = AsyncMock(return_value={"post_text": "fresh", "symbol": "SPY", "mode": "close"})
    monkeypatch.setattr(bt, "generate_and_store", fake)

    body = ax.RegenerateRequest(symbol="spy", mode="close")
    sentinel_db = object()
    out = await ax.regenerate(body, db=sentinel_db)

    fake.assert_awaited_once()
    args = fake.await_args.args
    assert args[0] is sentinel_db and args[1] == "close" and args[2] == "SPY"
    assert out["record"]["post_text"] == "fresh"
    assert out["timing_label"] == "Post-Market Read"


@pytest.mark.asyncio
async def test_regenerate_surfaces_generation_failure_as_502(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setenv("BULLETIN_TWEET_SYMBOLS", "SPY")
    monkeypatch.setattr(
        bt,
        "generate_and_store",
        AsyncMock(side_effect=RuntimeError("llm down")),
    )
    with pytest.raises(HTTPException) as ex:
        await ax.regenerate(ax.RegenerateRequest(symbol="SPY", mode="midday"), db=object())
    assert ex.value.status_code == 502
