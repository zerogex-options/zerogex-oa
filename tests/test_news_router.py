"""Tests for src.api.routers.news — the public CNBC headlines endpoint.

Exercises the handler directly (the api_key_auth gate is covered by the
shared security tests). Contract: returns the same headlines the bulletin
tweet uses, never raises on a scrape failure, and honors ``limit``."""

from __future__ import annotations

import pytest

from src.api.routers import news
from src.jobs.cnbc_news import NewsItem


def _items() -> list[NewsItem]:
    return [
        NewsItem(
            title="Stocks slide as oil spikes on Middle East tension",
            summary="The S&P 500 fell as crude jumped.",
            source="CNBC",
            link="https://www.cnbc.com/a.html",
            published="2026-08-15T13:30:00+00:00",
            published_ts=1_000_002.0,
        ),
        NewsItem(
            title="Fed official signals patience on rate cuts",
            summary="Comments from a policymaker moved yields.",
            source="CNBC",
            link="https://www.cnbc.com/b.html",
            published="2026-08-15T13:00:00+00:00",
            published_ts=1_000_001.0,
        ),
    ]


@pytest.mark.asyncio
async def test_headlines_returns_cnbc_items(monkeypatch):
    captured = {}

    def fake_fetch(max_items=None):
        captured["max_items"] = max_items
        return _items()

    monkeypatch.setattr(news.cnbc_news, "fetch_headlines", fake_fetch)

    out = await news.headlines(limit=None)

    assert out["source"] == "cnbc"
    assert out["count"] == 2
    assert "generated_at" in out and out["generated_at"]
    # limit=None must pass through so the bulletin's configured cap applies.
    assert captured["max_items"] is None
    first = out["headlines"][0]
    assert first["title"].startswith("Stocks slide")
    assert first["summary"] == "The S&P 500 fell as crude jumped."
    assert first["source"] == "CNBC"
    assert first["link"] == "https://www.cnbc.com/a.html"
    assert first["published"] == "2026-08-15T13:30:00+00:00"
    # published_ts is an internal sort key, not part of the wire contract.
    assert "published_ts" not in first


@pytest.mark.asyncio
async def test_headlines_passes_limit_through(monkeypatch):
    captured = {}

    def fake_fetch(max_items=None):
        captured["max_items"] = max_items
        return _items()[:1]

    monkeypatch.setattr(news.cnbc_news, "fetch_headlines", fake_fetch)

    out = await news.headlines(limit=1)
    assert captured["max_items"] == 1
    assert out["count"] == 1


@pytest.mark.asyncio
async def test_headlines_never_raises_on_scrape_error(monkeypatch):
    def boom(max_items=None):
        raise RuntimeError("CNBC edge returned 503")

    monkeypatch.setattr(news.cnbc_news, "fetch_headlines", boom)

    out = await news.headlines(limit=None)
    assert out["count"] == 0
    assert out["headlines"] == []
    assert out["source"] == "cnbc"


@pytest.mark.asyncio
async def test_headlines_empty_when_news_disabled(monkeypatch):
    monkeypatch.setattr(news.cnbc_news, "fetch_headlines", lambda max_items=None: [])
    out = await news.headlines(limit=None)
    assert out["count"] == 0
    assert out["headlines"] == []
