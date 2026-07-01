"""Tests for src.jobs.scorecard_tweet — the 4:15 PM ET auto-tweet job.

The job runs from systemd in production. These tests pin down the contract
that matters operationally: (a) tweet copy is deterministic and never
ends with stray punctuation, (b) the job never raises on DB / X API
failure, (c) non-trading days and empty-day scorecards skip silently,
(d) ``--post`` is required *in addition to* ``X_BOT_BEARER_TOKEN`` so a
misconfigured timer can't accidentally start posting before operators
have verified dry-run output.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest


def _reload_module():
    """Force a clean import so DatabaseManager mocks installed by other
    tests in the suite don't leak in."""
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.scorecard_tweet") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import scorecard_tweet  # noqa: WPS433

    return scorecard_tweet


def _payload(**overrides):
    base = {
        "symbol": "SPY",
        "horizon_minutes": 60,
        "cards": {
            "total": 12,
            "by_action": [{"action": "SELL_CALL_SPREAD", "count": 4}],
            "first_card_id": 4221,
            "first_card_permalink": "/cards/4221",
        },
        "signals": {
            "events": [
                {"name": "squeeze_setup", "flips": 3, "scored": 3, "wins": 2, "losses": 1, "avg_directional_return": 0.0074},
                {"name": "vanna_charm_flow", "flips": 3, "scored": 3, "wins": 1, "losses": 2, "avg_directional_return": -0.0031},
            ],
            "best": {"name": "squeeze_setup", "flips": 3, "scored": 3, "wins": 2, "losses": 1, "avg_directional_return": 0.0074},
            "worst": {"name": "vanna_charm_flow", "flips": 3, "scored": 3, "wins": 1, "losses": 2, "avg_directional_return": -0.0031},
        },
        "regime": {"label": "short gamma", "composite_score": -0.28, "direction": "bearish"},
    }
    base.update(overrides)
    return base


def test_build_tweet_copy_full_day():
    mod = _reload_module()
    text = mod.build_tweet_copy(_payload(), date(2026, 6, 29), "SPY", "https://zerogex.io")
    assert text.startswith("SPY · 2026-06-29 —")
    assert "12 Playbook calls" in text
    assert "Best: Squeeze Setup +0.74%" in text
    assert "Worst: Vanna Charm Flow −0.31%" in text
    assert "Regime: short gamma" in text
    assert text.endswith("https://zerogex.io/scorecard/2026-06-29")
    assert len(text) <= mod.TWEET_MAX_LEN


def test_build_tweet_copy_truncates_to_280():
    """Pathologically long site URLs must still keep the tweet ≤ 280 chars."""
    mod = _reload_module()
    long_url = "https://" + "z" * 200 + ".io"
    text = mod.build_tweet_copy(_payload(), date(2026, 6, 29), "SPY", long_url)
    assert len(text) <= mod.TWEET_MAX_LEN
    # Permalink must still be present in full — the receipt is the whole point.
    assert text.endswith(f"{long_url}/scorecard/2026-06-29")


def test_build_tweet_copy_quiet_tape():
    mod = _reload_module()
    payload = _payload(
        cards={"total": 0, "by_action": [], "first_card_id": None, "first_card_permalink": None},
        signals={"events": [], "best": None, "worst": None},
        regime=None,
    )
    text = mod.build_tweet_copy(payload, date(2026, 6, 29), "SPY", "https://zerogex.io")
    assert "quiet tape" in text
    # No trailing whitespace or punctuation artifacts.
    body, sep, url = text.rpartition("\n")
    assert sep == "\n"
    assert url == "https://zerogex.io/scorecard/2026-06-29"
    assert body == body.strip()
    assert not body.endswith((" —", " -", ",", ";"))


def test_is_trading_day_excludes_weekends(monkeypatch):
    mod = _reload_module()
    # 2026-06-27 is a Saturday, 2026-06-29 is a Monday.
    assert mod._is_trading_day(date(2026, 6, 27)) is False
    assert mod._is_trading_day(date(2026, 6, 28)) is False
    assert mod._is_trading_day(date(2026, 6, 29)) is True


def test_is_trading_day_excludes_configured_holidays(monkeypatch):
    """Patch the module's resolved holiday set since it's evaluated at
    import time from $NYSE_HOLIDAYS — re-importing here would reset the
    set to whatever the test env has configured."""
    mod = _reload_module()
    monkeypatch.setattr(mod, "NYSE_HOLIDAYS", {date(2026, 7, 3)})
    assert mod._is_trading_day(date(2026, 7, 3)) is False  # holiday
    assert mod._is_trading_day(date(2026, 7, 2)) is True   # Thursday


@pytest.mark.asyncio
async def test_run_dry_run_logs_tweet_text(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):  # noqa: D401
            return None

        async def disconnect(self):
            return None

        async def get_daily_scorecard(self, **kwargs):
            return _payload()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.delenv("X_BOT_BEARER_TOKEN", raising=False)
    args = mod._parse_args(["--date", "2026-06-29", "--symbol", "SPY"])
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    # Dry-run must surface the tweet text in logs so journalctl shows
    # exactly what would have been posted.
    assert any("DRY RUN" in record.message for record in caplog.records)
    assert any("Squeeze Setup +0.74%" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_run_skips_on_weekend_silently(monkeypatch, caplog):
    mod = _reload_module()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: pytest.fail(
        "DatabaseManager must NOT be constructed on a non-trading day"
    ))
    args = mod._parse_args(["--date", "2026-06-27"])  # Saturday
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    assert any("not a trading day" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_run_skips_empty_scorecard(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None

        async def disconnect(self):
            return None

        async def get_daily_scorecard(self, **kwargs):
            return _payload(
                cards={"total": 0, "by_action": [], "first_card_id": None, "first_card_permalink": None},
                signals={"events": [], "best": None, "worst": None},
            )

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    args = mod._parse_args(["--date", "2026-06-29", "--post"])
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "fake-token")
    posted = {"called": False}
    monkeypatch.setattr(mod, "post_tweet_via_x_api", lambda *a, **kw: posted.__setitem__("called", True))
    with caplog.at_level("INFO"):
        rc = await mod._run(args)
    assert rc == 0
    # Empty days never post — better silence than a "0 calls" tweet.
    assert posted["called"] is False
    assert any("empty scorecard" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_run_requires_post_flag_even_with_token(monkeypatch, caplog):
    """A misconfigured timer that picks up $X_BOT_BEARER_TOKEN must not
    start posting without the explicit --post flag. Defense in depth."""
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None

        async def disconnect(self):
            return None

        async def get_daily_scorecard(self, **kwargs):
            return _payload()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "fake-token-do-not-actually-post")
    posted = {"called": False}
    monkeypatch.setattr(mod, "post_tweet_via_x_api", lambda *a, **kw: posted.__setitem__("called", True))
    # No --post flag.
    args = mod._parse_args(["--date", "2026-06-29"])
    rc = await mod._run(args)
    assert rc == 0
    assert posted["called"] is False


@pytest.mark.asyncio
async def test_run_swallows_db_failure(monkeypatch, caplog):
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
    assert rc == 0
    assert any("DB connect failed" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_run_swallows_x_api_failure(monkeypatch, caplog):
    mod = _reload_module()

    class _FakeDB:
        async def connect(self):
            return None

        async def disconnect(self):
            return None

        async def get_daily_scorecard(self, **kwargs):
            return _payload()

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _FakeDB())
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "fake-token")

    def _explode(text, bearer):
        from urllib.error import HTTPError
        raise HTTPError("https://api.x.com/2/tweets", 429, "Rate limited", {}, None)

    monkeypatch.setattr(mod, "post_tweet_via_x_api", _explode)
    args = mod._parse_args(["--date", "2026-06-29", "--post"])
    with caplog.at_level("WARNING"):
        rc = await mod._run(args)
    assert rc == 0  # Never raises — systemd should not see a failure.
    assert any("X API call failed" in record.message for record in caplog.records)
