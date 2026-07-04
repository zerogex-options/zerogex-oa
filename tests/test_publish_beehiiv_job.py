"""Tests for src.jobs.publish_beehiiv — the daily + weekly Beehiiv publisher.

Pin down the contract that matters operationally:

* Rendered HTML contains the expected substrings so a template regression
  doesn't ship a broken email.
* --dry-run never touches Beehiiv (mocked BeehiivClient never called).
* --post without BEEHIIV_API_KEY silently degrades to dry-run.
* An idempotent second run for an already-published (cadence, date) is
  a no-op (no additional Beehiiv call).
* Weekly runs are Sunday-gated; daily runs are trading-day-gated.
* A Beehiiv API failure writes a 'failed' row to the DB but exits 0.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.publish_beehiiv"):
            sys.modules.pop(mod, None)
    from src.jobs import publish_beehiiv  # noqa: WPS433

    return publish_beehiiv


class _FakeDB:
    """Fake DatabaseManager — records what got upserted so the test can
    assert on the status transitions the job wrote."""

    def __init__(
        self,
        *,
        forecast_by_symbol: dict[str, dict[str, Any]] | None = None,
        scorecard_payload: dict[str, Any] | None = None,
        pre_existing_publication: dict[str, Any] | None = None,
    ):
        self.forecast_by_symbol = forecast_by_symbol or {}
        self.scorecard_payload = scorecard_payload
        self.pre_existing_publication = pre_existing_publication
        self.upserts: list[dict[str, Any]] = []

    async def connect(self):
        return None

    async def disconnect(self):
        return None

    async def get_daily_forecast(self, symbol: str, day: date):
        return self.forecast_by_symbol.get(symbol.upper())

    async def get_daily_scorecard(self, **kwargs):
        return self.scorecard_payload

    async def get_newsletter_publication(self, cadence: str, publication_date: date):
        return self.pre_existing_publication

    async def upsert_newsletter_publication(self, **kwargs):
        row = dict(kwargs)
        self.upserts.append(row)
        return row


def _forecast_row(symbol: str, gamma_flip: float, call_wall: float, put_wall: float,
                  flagship: dict[str, Any] | None = None):
    return {
        "symbol": symbol,
        "gamma_flip": gamma_flip,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "regime": "long_gamma",
        "projected_low": call_wall * 0.99,
        "projected_high": call_wall * 1.01,
        "flagship_setup": flagship,
    }


def _scorecard_payload():
    return {
        "cards": {"total": 4, "by_action": [{"action": "BUY_CALL_SPREAD", "count": 2}]},
        "signals": {
            "best": {"name": "squeeze_setup", "avg_directional_return": 0.0035},
            "worst": None,
            "events": [],
        },
        "regime": {"label": "long gamma", "composite_score": 0.4, "direction": "bullish"},
    }


# ----------------------------------------------------------------------
# Rendering
# ----------------------------------------------------------------------


def test_render_daily_html_contains_symbol_levels():
    mod = _reload_module()
    payload = mod.DailyPayload(
        headline_date="Jul 3, 2026",
        recap="Yesterday was quiet.",
        levels=[
            mod.DailyLevelRow(symbol="SPX", gamma_flip="5,820", call_wall="5,850", put_wall="5,780"),
            mod.DailyLevelRow(symbol="SPY", gamma_flip="581.00", call_wall="585.50", put_wall="577.00"),
        ],
        pattern=mod.DailyPatternHint(
            name="Squeeze Setup",
            symbol="SPX",
            rationale="Watch for a break above the call wall on volume.",
        ),
        upgrade_url="https://zerogex.io/newsletter#upgrade",
    )
    html = mod.render_daily_html(payload)
    assert "GEX Morning Card" in html
    assert "Jul 3, 2026" in html
    assert "SPX" in html
    assert "5,820" in html
    assert "5,850" in html
    assert "Yesterday was quiet" in html
    assert "Squeeze Setup" in html
    # The license-safety footer must be on every email.
    assert "derived analytics" in html.lower()
    assert "do not\n    redistribute raw exchange quotes" in html or "do not redistribute raw exchange quotes" in html


def test_render_daily_html_elides_missing_sections():
    mod = _reload_module()
    payload = mod.DailyPayload(
        headline_date="Jul 3, 2026",
        recap=None,
        levels=[],
        pattern=None,
        upgrade_url="https://zerogex.io/newsletter",
    )
    html = mod.render_daily_html(payload)
    assert "Yesterday's tape" not in html
    assert "Today's key levels" not in html
    assert "One pattern to watch" not in html
    # The upsell CTA + license footer remain even in the pathological case.
    assert "Upgrade to weekly" in html
    assert "derived analytics" in html.lower()


def test_render_weekly_html_contains_accuracy_and_deep_dive():
    mod = _reload_module()
    payload = mod.WeeklyPayload(
        week_start_label="Jun 30, 2026",
        accuracy_rows=[
            mod.WeeklyAccuracyRow(pattern="Squeeze Setup", underlying="SPX", n_resolved=12, hit_rate="72.5%"),
        ],
        deep_dive=mod.WeeklyDeepDive(
            pattern="Squeeze Setup",
            setup="Setup context",
            entry="Entry context",
            exit="Exit context",
            pnl="Hit rate 72.5%.",
        ),
        landscape="Calendar notes: OPEX Friday.",
        postmortem="Backtest run #42 on $SPX completed with 20 trades.",
    )
    html = mod.render_weekly_html(payload)
    assert "Sunday Playbook Review" in html
    assert "Week of Jun 30, 2026" in html
    assert "Squeeze Setup" in html
    assert "Hit rate 72.5%" in html
    assert "OPEX Friday" in html


def test_subject_line_daily():
    mod = _reload_module()
    subj = mod._subject_line(mod.CADENCE_DAILY, date(2026, 7, 3), None)
    assert subj.startswith("GEX Morning Card")
    assert "Jul 3" in subj


def test_subject_line_weekly():
    mod = _reload_module()
    # 2026-07-05 is a Sunday; the Monday of that week is 2026-06-29.
    subj = mod._subject_line(mod.CADENCE_WEEKLY, date(2026, 7, 5), None)
    assert subj.startswith("Sunday Playbook Review")
    assert "Jun 29" in subj


# ----------------------------------------------------------------------
# Skip semantics
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daily_skips_on_weekend(monkeypatch, caplog):
    mod = _reload_module()
    db = _FakeDB()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    cfg = mod.RunConfig(
        cadence=mod.CADENCE_DAILY,
        day=date(2026, 7, 4),  # Saturday
        lead_symbol="SPX", symbols=("SPX",), upgrade_url="https://x",
        dry_run=False, post=False, confirm=False, allow_non_trading_day=False,
    )
    with caplog.at_level("INFO"):
        rc = await mod._run(cfg)
    assert rc == 0
    assert any("not a trading day" in r.message for r in caplog.records)
    assert db.upserts == []


@pytest.mark.asyncio
async def test_weekly_skips_on_non_sunday(monkeypatch, caplog):
    mod = _reload_module()
    db = _FakeDB()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    cfg = mod.RunConfig(
        cadence=mod.CADENCE_WEEKLY,
        day=date(2026, 7, 6),  # Monday
        lead_symbol="SPX", symbols=("SPX",), upgrade_url="https://x",
        dry_run=False, post=False, confirm=False, allow_non_trading_day=False,
    )
    with caplog.at_level("INFO"):
        rc = await mod._run(cfg)
    assert rc == 0
    assert any("not a Sunday" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_daily_dry_run_writes_pending_and_does_not_call_beehiiv(monkeypatch, caplog):
    mod = _reload_module()
    db = _FakeDB(
        forecast_by_symbol={
            "SPX": _forecast_row("SPX", 5820.0, 5850.0, 5780.0),
            "SPY": _forecast_row("SPY", 581.00, 585.50, 577.00),
        },
        scorecard_payload=_scorecard_payload(),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    # Guard: BeehiivClient must never be instantiated on a dry-run.
    monkeypatch.setattr(mod, "BeehiivClient", lambda **kw: pytest.fail("BeehiivClient should not be constructed on dry-run"))
    monkeypatch.setenv("BEEHIIV_API_KEY", "irrelevant-on-dry-run")
    monkeypatch.setenv("BEEHIIV_PUBLICATION_ID", "pub_123")
    cfg = mod.RunConfig(
        cadence=mod.CADENCE_DAILY,
        day=date(2026, 7, 3),  # Friday
        lead_symbol="SPX", symbols=("SPX", "SPY"), upgrade_url="https://x",
        dry_run=True, post=False, confirm=False, allow_non_trading_day=False,
    )
    with caplog.at_level("INFO"):
        rc = await mod._run(cfg)
    assert rc == 0
    assert any("DRY RUN" in r.message for r in caplog.records)
    assert len(db.upserts) == 1
    upsert = db.upserts[0]
    assert upsert["cadence"] == "daily"
    assert upsert["status"] == "pending"
    assert upsert["audience"] == "free"
    assert upsert["publication_date"] == date(2026, 7, 3)
    assert upsert["subject_line"].startswith("GEX Morning Card")


@pytest.mark.asyncio
async def test_post_without_env_degrades_to_dry_run(monkeypatch, caplog):
    mod = _reload_module()
    db = _FakeDB(
        forecast_by_symbol={"SPX": _forecast_row("SPX", 5820, 5850, 5780)},
        scorecard_payload=_scorecard_payload(),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    monkeypatch.setattr(mod, "BeehiivClient", lambda **kw: pytest.fail("no env, no post"))
    monkeypatch.delenv("BEEHIIV_API_KEY", raising=False)
    monkeypatch.delenv("BEEHIIV_PUBLICATION_ID", raising=False)
    cfg = mod.RunConfig(
        cadence=mod.CADENCE_DAILY,
        day=date(2026, 7, 3), lead_symbol="SPX", symbols=("SPX",),
        upgrade_url="https://x",
        dry_run=False, post=True, confirm=False, allow_non_trading_day=False,
    )
    rc = await mod._run(cfg)
    assert rc == 0
    assert len(db.upserts) == 1
    assert db.upserts[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_already_published_skips(monkeypatch, caplog):
    mod = _reload_module()
    db = _FakeDB(
        pre_existing_publication={"cadence": "daily", "status": "published", "beehiiv_post_id": "post_x"},
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    monkeypatch.setenv("BEEHIIV_API_KEY", "k")
    monkeypatch.setenv("BEEHIIV_PUBLICATION_ID", "pub")
    monkeypatch.setattr(mod, "BeehiivClient", lambda **kw: pytest.fail("must not re-publish"))
    cfg = mod.RunConfig(
        cadence=mod.CADENCE_DAILY,
        day=date(2026, 7, 3), lead_symbol="SPX", symbols=("SPX",),
        upgrade_url="https://x",
        dry_run=False, post=True, confirm=False, allow_non_trading_day=False,
    )
    with caplog.at_level("INFO"):
        rc = await mod._run(cfg)
    assert rc == 0
    assert db.upserts == []
    assert any("already published" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_live_publish_success_writes_published_row(monkeypatch):
    mod = _reload_module()
    db = _FakeDB(
        forecast_by_symbol={"SPX": _forecast_row("SPX", 5820, 5850, 5780)},
        scorecard_payload=_scorecard_payload(),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    monkeypatch.setenv("BEEHIIV_API_KEY", "k")
    monkeypatch.setenv("BEEHIIV_PUBLICATION_ID", "pub")

    class _StubClient:
        def __init__(self, **kw):
            self.kw = kw

        def create_post(self, payload):
            from src.services.beehiiv_client import BeehiivResponse
            return BeehiivResponse(
                status=201,
                data={"data": {"id": "post_abc", "web_url": "https://x.beehiiv.com/p/abc"}},
                request_id="req_1",
            )

        def confirm_post(self, post_id):
            raise AssertionError("confirm not requested")

    monkeypatch.setattr(mod, "BeehiivClient", _StubClient)

    cfg = mod.RunConfig(
        cadence=mod.CADENCE_DAILY,
        day=date(2026, 7, 3),
        lead_symbol="SPX", symbols=("SPX",), upgrade_url="https://x",
        dry_run=False, post=True, confirm=False, allow_non_trading_day=False,
    )
    rc = await mod._run(cfg)
    assert rc == 0
    assert len(db.upserts) == 1
    up = db.upserts[0]
    assert up["status"] == "published"
    assert up["beehiiv_post_id"] == "post_abc"
    assert up["beehiiv_url"] == "https://x.beehiiv.com/p/abc"


@pytest.mark.asyncio
async def test_live_publish_failure_writes_failed_row_and_exits_zero(monkeypatch, caplog):
    mod = _reload_module()
    from src.services.beehiiv_client import BeehiivError

    db = _FakeDB(
        forecast_by_symbol={"SPX": _forecast_row("SPX", 5820, 5850, 5780)},
        scorecard_payload=_scorecard_payload(),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    monkeypatch.setenv("BEEHIIV_API_KEY", "k")
    monkeypatch.setenv("BEEHIIV_PUBLICATION_ID", "pub")

    class _StubClient:
        def __init__(self, **kw): pass

        def create_post(self, payload):
            raise BeehiivError(
                "Beehiiv POST failed with HTTP 500",
                status=500, body='{"err":"upstream"}', request_id="req_bad",
            )

    monkeypatch.setattr(mod, "BeehiivClient", _StubClient)

    cfg = mod.RunConfig(
        cadence=mod.CADENCE_DAILY,
        day=date(2026, 7, 3),
        lead_symbol="SPX", symbols=("SPX",), upgrade_url="https://x",
        dry_run=False, post=True, confirm=False, allow_non_trading_day=False,
    )
    with caplog.at_level("WARNING"):
        rc = await mod._run(cfg)
    # Never raises — cron must exit 0 so tomorrow's timer keeps firing.
    assert rc == 0
    assert len(db.upserts) == 1
    up = db.upserts[0]
    assert up["status"] == "failed"
    assert "HTTP 500" in (up.get("error_message") or "")
    assert any("Beehiiv create_post failed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_weekly_dry_run_pulls_accuracy_rows(monkeypatch, caplog):
    mod = _reload_module()
    db = _FakeDB()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)
    monkeypatch.setattr(
        mod, "_fetch_weekly_accuracy_rows",
        lambda underlying, top_n=6: [
            mod.WeeklyAccuracyRow(pattern="Squeeze Setup", underlying=underlying, n_resolved=8, hit_rate="62.5%"),
        ],
    )
    monkeypatch.setattr(mod, "_fetch_weekly_landscape", lambda: "Calendar notes for next week: monthly OPEX Friday.")
    monkeypatch.setattr(mod, "_fetch_weekly_postmortem", lambda: "Last week's backtest run #7 on $SPX completed.")
    cfg = mod.RunConfig(
        cadence=mod.CADENCE_WEEKLY,
        day=date(2026, 7, 5),  # Sunday
        lead_symbol="SPX", symbols=("SPX",), upgrade_url="https://x",
        dry_run=True, post=False, confirm=False, allow_non_trading_day=False,
    )
    with caplog.at_level("INFO"):
        rc = await mod._run(cfg)
    assert rc == 0
    assert any("Squeeze Setup" in r.message for r in caplog.records)
    assert len(db.upserts) == 1
    assert db.upserts[0]["cadence"] == "weekly"
    assert db.upserts[0]["audience"] == "premium"


# ----------------------------------------------------------------------
# Smoke test: fixture-shaped forecast rows survive rendering
# ----------------------------------------------------------------------


def test_smoke_daily_render_from_fixed_rows():
    """Fixed forecast row → rendered HTML has every expected substring.

    This is the "hand-verify staging content shape" backstop referred to
    in the prompt — feeds a well-known row through the same code path
    the cron uses and asserts on the output surface."""
    mod = _reload_module()

    async def _go():
        db = _FakeDB(
            forecast_by_symbol={
                "SPX": _forecast_row(
                    "SPX", 5820.0, 5850.0, 5780.0,
                    flagship={
                        "action": "SQUEEZE_SETUP",
                        "summary": "Watch for a break above the call wall on volume.",
                    },
                ),
                "SPY": _forecast_row("SPY", 581.00, 585.50, 577.00),
                "QQQ": _forecast_row("QQQ", 512.00, 517.00, 508.50),
            },
            scorecard_payload=_scorecard_payload(),
        )
        payload = await mod.build_daily_payload(
            db,
            date(2026, 7, 3),
            "SPX",
            ("SPX", "SPY", "QQQ"),
            "https://zerogex.io/newsletter#upgrade",
        )
        return mod.render_daily_html(payload)

    html = asyncio.run(_go())
    # Every requested symbol renders a row.
    for sym in ("SPX", "SPY", "QQQ"):
        assert sym in html
    assert "5,820" in html  # SPX gamma flip
    assert "585.50" in html  # SPY call wall
    # Playbook pattern surfaces the humanized action name.
    assert "Squeeze Setup" in html
    # License-safety footer.
    assert "raw exchange quotes" in html
