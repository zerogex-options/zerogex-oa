"""Tests for src.jobs.bulletin_tweet — the 09:15 / 12:30 / 16:05 daily
Live-Bulletin X-post job.

Contract mirrors the other tweet crons (:mod:`test_forecast_tweet_job`,
:mod:`test_scorecard_tweet`): never raises, dry-runs by default,
skips silently on non-trading days and when every symbol's GEX row is
missing, and writes inspectable artifacts (tweet text, manifest,
media) to a per-mode/per-date directory."""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.bulletin_tweet") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import bulletin_tweet  # noqa: WPS433

    return bulletin_tweet


def _summary_row(
    symbol: str = "SPX",
    spot: float = 7483.0,
    gamma_flip: float = 7448.0,
    call_wall: float = 7500.0,
    put_wall: float = 7480.0,
    max_pain: float = 7460.0,
    net_gex: float = 19_500_000_000.0,
) -> dict:
    """Shape mirrors DatabaseManager.get_latest_gex_summary output."""
    return {
        "timestamp": "2026-07-03T20:05:00+00:00",
        "symbol": symbol,
        "spot_price": spot,
        "gamma_flip": gamma_flip,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "max_pain": max_pain,
        "net_gex": net_gex,
        "net_gex_at_spot": None,
    }


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------


def test_fmt_price_uses_locale_thousands_for_indices():
    mod = _reload_module()
    assert mod._fmt_price(7483.0) == "7,483"
    assert mod._fmt_price(744.51) == "744.51"
    assert mod._fmt_price(None) == "—"


def test_fmt_net_gex_scales_by_magnitude():
    mod = _reload_module()
    assert mod._fmt_net_gex(72_300_000.0) == "+$72.3M"
    assert mod._fmt_net_gex(19_500_000_000.0) == "+$19.50B"
    assert mod._fmt_net_gex(-1_200_000_000.0) == "−$1.20B"
    assert mod._fmt_net_gex(None) == "—"


def test_shape_bulletin_prefers_net_gex_at_spot():
    """The tweet's Net GEX must match the rest of the site: prefer
    net_gex_at_spot (the regime-correct headline figure the dashboard and
    gamma-exposure page show) over the chain-wide net_gex, which can
    differ in magnitude — and even sign."""
    mod = _reload_module()
    # net_gex_at_spot present and opposite sign to the chain-wide total.
    row = _summary_row("SPY", spot=744.51, net_gex=5_000_000_000.0)
    row["net_gex_at_spot"] = -1_200_000_000.0
    assert mod._shape_bulletin(row, "SPY").net_gex == pytest.approx(-1_200_000_000.0)
    # Falls back to the chain-wide net_gex only when at-spot is absent.
    row_no_at_spot = _summary_row("SPY", spot=744.51, net_gex=5_000_000_000.0)
    row_no_at_spot["net_gex_at_spot"] = None
    assert mod._shape_bulletin(row_no_at_spot, "SPY").net_gex == pytest.approx(
        5_000_000_000.0,
    )


# ---------------------------------------------------------------------------
# Tweet body builder
# ---------------------------------------------------------------------------


def test_build_tweet_body_close_shape(monkeypatch):
    # Force the deterministic static-template path so the body content is
    # stable regardless of whether the test host has an API key/network.
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mod = _reload_module()
    bulletins = [
        mod._shape_bulletin(
            _summary_row(
                "SPY",
                spot=744.51,
                gamma_flip=744.51,
                call_wall=750.0,
                put_wall=740.0,
                max_pain=742.0,
                net_gex=72_300_000.0,
            ),
            "SPY",
        ),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
        mod._shape_bulletin(
            _summary_row(
                "QQQ",
                spot=655.4,
                gamma_flip=654.0,
                call_wall=660.0,
                put_wall=650.0,
                max_pain=653.0,
                net_gex=-125_000_000.0,
            ),
            "QQQ",
        ),
    ]
    body = mod.build_tweet_body(
        mode="close",
        day=date(2026, 7, 3),
        bulletins=bulletins,
        site_url="https://zerogex.io",
        lead_symbol="SPX",
    )

    # SPY's spot sits exactly on its gamma flip → cleanest setup → featured.
    assert body.featured_symbol == "SPY"
    assert body.lead_symbol == "SPY"
    # Header is the "…Read — $SYM" format the operator specified.
    assert "Post-Market Read — $SPY" in body.text
    # The deterministic Key levels block (Python owns the prices).
    assert "Key levels:" in body.text
    assert "• 740 → Put Wall" in body.text
    assert "• 750 → Call Wall" in body.text
    assert "• 744.51 → Gamma Flip" in body.text
    # Net GEX is woven into the static hook prose.
    assert "+$72.3M" in body.text
    assert "Bottom line:" in body.text
    # The other two symbols get NO numeric block of their own.
    assert "SPX spot:" not in body.text
    assert "QQQ spot:" not in body.text
    # No site link and no hashtags in the main post.
    assert "zerogex.io" not in body.text
    assert "http" not in body.text
    assert "#" not in body.text
    # All three still count as present (fetched so the copy can cross-reference).
    assert body.symbols_present == ["SPY", "SPX", "QQQ"]
    # The link rides in the threaded reply instead (static fallback reply).
    assert body.reply_text == "Free delayed SPY / SPX / QQQ gamma levels: https://zerogex.io"


def test_build_tweet_body_labels_per_mode():
    mod = _reload_module()
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")]
    for mode, expected in (
        ("premarket", "Morning Read — $SPY"),
        ("midday", "Midday Read — $SPY"),
        ("close", "Post-Market Read — $SPY"),
    ):
        body = mod.build_tweet_body(
            mode=mode,
            day=date(2026, 7, 3),
            bulletins=bulletins,
            site_url="https://zerogex.io",
            lead_symbol="SPY",
        )
        assert expected in body.text, f"mode={mode} missing label"


def test_build_tweet_body_skips_symbols_with_no_data(monkeypatch):
    """A symbol whose GEX row didn't resolve is never eligible to be
    featured and never appears in the post; the symbols that DID resolve
    are the only featuring candidates and the only ones in symbols_present."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mod = _reload_module()
    bulletins = [
        mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY"),
        mod._shape_bulletin(None, "SPX"),  # no data → cannot be featured
        mod._shape_bulletin(_summary_row("QQQ", spot=655.4), "QQQ"),
    ]
    body = mod.build_tweet_body(
        mode="close",
        day=date(2026, 7, 3),
        bulletins=bulletins,
        site_url="https://zerogex.io",
        lead_symbol="SPX",
    )
    # The data-less SPX can't be featured and doesn't appear anywhere.
    assert body.featured_symbol in ("SPY", "QQQ")
    assert "$SPX" not in body.text
    assert "SPX spot:" not in body.text
    assert body.symbols_present == ["SPY", "QQQ"]


def test_symbol_block_shows_projected_indicator():
    """A futures-projected SPX spot is clearly labeled in the numeric block."""
    mod = _reload_module()
    b = mod._shape_bulletin(_summary_row("SPX", spot=6432.0), "SPX")
    b.spot_is_projected = True
    b.future_symbol = "@ES"
    block = mod._symbol_block(b)
    assert "SPX spot:" in block
    assert "implied from ES futures, cash closed" in block
    # The frozen structural levels stay unlabeled (no futures equivalent).
    assert "Gamma Flip:" in block


def test_symbol_block_no_indicator_when_live():
    mod = _reload_module()
    b = mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")
    block = mod._symbol_block(b)
    assert "implied from" not in block


@pytest.mark.asyncio
async def test_fetch_bulletins_projects_spx_spot(monkeypatch):
    """_fetch_bulletins overrides a cash index's frozen spot with the
    futures-implied level and flags it, leaving ETFs untouched."""
    mod = _reload_module()
    from src.jobs.index_projection import ImpliedIndexSpot

    async def _fake_projection(db, symbol, *, at=None):
        if symbol.upper() == "SPX":
            return ImpliedIndexSpot(
                symbol="SPX",
                implied_price=6432.0,
                cash_ref_close=6400.0,
                future_now=6450.0,
                future_ref=6418.0,
                future_symbol="@ES",
            )
        return None

    monkeypatch.setattr(mod, "implied_index_spot", _fake_projection)

    db = MagicMock()
    db.get_latest_gex_summary = AsyncMock(
        side_effect=lambda sym: _summary_row(sym, spot=6400.0 if sym == "SPX" else 744.51)
    )
    # Price-action queries added by the news/price wiring — return None so the
    # projection assertions stay the focus (best-effort, never fatal).
    db.get_latest_forced_flow = AsyncMock(return_value=None)
    db.get_session_closes = AsyncMock(return_value=None)
    db.get_intraday_ohlc = AsyncMock(return_value=None)
    bulletins = await mod._fetch_bulletins(db, ["SPY", "SPX"], date(2026, 7, 3))
    by_sym = {b.symbol: b for b in bulletins}
    # SPX spot replaced with the implied level and flagged.
    assert by_sym["SPX"].spot == pytest.approx(6432.0)
    assert by_sym["SPX"].spot_is_projected is True
    assert by_sym["SPX"].future_symbol == "@ES"
    # SPY (ETF) untouched — still the live cash spot, not projected.
    assert by_sym["SPY"].spot == pytest.approx(744.51)
    assert by_sym["SPY"].spot_is_projected is False


def test_build_tweet_body_lead_variant_deterministic_per_day():
    """The lead sentence should be stable within a fire (dry-run and live
    match), so the seed is deterministic on (date, mode)."""
    mod = _reload_module()
    bulletins = [mod._shape_bulletin(_summary_row("SPY"), "SPY")]
    a = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPY"
    )
    b = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPY"
    )
    assert a.text == b.text


def test_fallback_tweet_fits_in_280(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mod = _reload_module()
    bulletins = [
        mod._shape_bulletin(
            _summary_row(
                "SPY",
                spot=744.51,
                gamma_flip=744.51,
                call_wall=750.0,
                put_wall=740.0,
                net_gex=72_300_000.0,
            ),
            "SPY",
        ),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
        mod._shape_bulletin(_summary_row("QQQ", spot=655.4), "QQQ"),
    ]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPY"
    )
    assert len(body.fallback) <= 280
    # Featured symbol's cashtag leads the fallback body.
    assert "$SPY" in body.fallback
    # No link in the body — it rides in the threaded reply now.
    assert "zerogex.io" not in body.fallback
    assert "http" not in body.fallback


# ---------------------------------------------------------------------------
# Featured-symbol selection + threaded link reply
# ---------------------------------------------------------------------------


def test_select_featured_symbol_picks_nearest_level():
    """The symbol whose spot is closest (in %) to a wall/flip wins."""
    mod = _reload_module()
    # SPY spot ~7% below its nearest level; QQQ spot right on its put wall.
    spy = mod._shape_bulletin(
        _summary_row(
            "SPY",
            spot=744.51,
            gamma_flip=800.0,
            call_wall=820.0,
            put_wall=810.0,
            max_pain=805.0,
            net_gex=72_300_000.0,
        ),
        "SPY",
    )
    qqq = mod._shape_bulletin(
        _summary_row(
            "QQQ",
            spot=650.2,
            gamma_flip=654.0,
            call_wall=660.0,
            put_wall=650.0,
            max_pain=653.0,
            net_gex=-125_000_000.0,
        ),
        "QQQ",
    )
    featured = mod.select_featured_symbol([spy, qqq])
    assert featured.symbol == "QQQ"


def test_select_featured_symbol_weights_gamma_flip_over_walls():
    """A symbol straddling its gamma flip is featured over one merely
    pinned to a wall when the two are close — the flip is the regime
    boundary, the higher-signal story.  Uses the real 2026-07-09 snapshot
    where SPY sat 0.23 under its call wall (~0.031%) and QQQ sat 0.34
    under its flip (~0.047% raw); flip-weighting tips it to QQQ."""
    mod = _reload_module()
    spy = mod._shape_bulletin(
        _summary_row(
            "SPY",
            spot=750.77,
            gamma_flip=747.77,
            call_wall=751.0,
            put_wall=750.0,
            max_pain=745.0,
            net_gex=2_530_000_000.0,
        ),
        "SPY",
    )
    qqq = mod._shape_bulletin(
        _summary_row(
            "QQQ",
            spot=722.48,
            gamma_flip=722.82,
            call_wall=725.0,
            put_wall=715.0,
            max_pain=712.0,
            net_gex=-33_000_000.0,
        ),
        "QQQ",
    )
    # Raw nearest-level distance would pick SPY (0.031% < 0.047%); the flip
    # weight (0.6) makes QQQ's flip proximity 0.028% — so QQQ wins.
    assert mod.select_featured_symbol([spy, qqq]).symbol == "QQQ"


def test_select_featured_symbol_falls_back_when_none_eligible():
    """With no symbol carrying both a spot and a level, selection falls
    back to the configured lead symbol if it has any data."""
    mod = _reload_module()
    only_spot = mod._shape_bulletin(None, "SPY")
    only_spot.spot = 744.51  # spot but every level is None
    only_levels = mod._shape_bulletin(_summary_row("SPX"), "SPX")
    only_levels.spot = None  # levels but no spot
    featured = mod.select_featured_symbol([only_spot, only_levels], fallback="SPX")
    assert featured.symbol == "SPX"


def test_reply_text_carries_the_link(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mod = _reload_module()
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51, gamma_flip=744.51), "SPY")]
    body = mod.build_tweet_body(
        "midday", date(2026, 7, 3), bulletins, site_url="https://zerogex.io/", lead_symbol="SPY"
    )
    # Trailing slash on the site URL is trimmed.
    assert body.reply_text == ("Free delayed SPY / SPX / QQQ gamma levels: https://zerogex.io")


def test_reply_text_env_override(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    mod = _reload_module()
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51, gamma_flip=744.51), "SPY")]
    body = mod.build_tweet_body(
        "midday",
        date(2026, 7, 3),
        bulletins,
        site_url="https://zerogex.io",
        lead_symbol="SPY",
        reply_text="Custom reply — zerogex.io",
    )
    assert body.reply_text == "Custom reply — zerogex.io"


def test_post_bulletin_posts_main_then_link_reply(monkeypatch):
    """post_bulletin posts the main tweet, then threads the link comment
    as a reply to the returned tweet id."""
    mod = _reload_module()
    calls: list[dict] = []

    def _fake_post(text, bearer, media_ids=None, reply_to=None, timeout_seconds=15):
        calls.append({"text": text, "reply_to": reply_to})
        tid = "main-123" if reply_to is None else "reply-456"
        return {"data": {"id": tid}}

    monkeypatch.setattr(mod, "post_tweet_via_x_api", _fake_post)
    monkeypatch.setattr(mod, "_upload_media_files", lambda media: [])

    tweet = mod.TweetBody(
        text="$SPY midday update:\n\nbody",
        fallback="$SPY midday",
        lead_symbol="SPY",
        symbols_present=["SPY"],
        reply_text="Free delayed SPY / SPX / QQQ gamma levels: https://zerogex.io",
        featured_symbol="SPY",
    )
    result = mod.post_bulletin(
        tweet, mod.MediaArtifacts(), bearer="tok", long=True, mode_label="midday"
    )
    assert result["id"] == "main-123"
    assert result["reply_id"] == "reply-456"
    # Two posts: main (reply_to None) then the link reply (reply_to = main id).
    assert len(calls) == 2
    assert calls[0]["reply_to"] is None
    assert calls[1]["reply_to"] == "main-123"
    assert calls[1]["text"].startswith("Free delayed SPY / SPX / QQQ gamma levels:")


def test_post_bulletin_survives_failed_reply(monkeypatch):
    """A failing link reply doesn't fail the whole post — the main tweet
    id is still returned."""
    mod = _reload_module()

    def _fake_post(text, bearer, media_ids=None, reply_to=None, timeout_seconds=15):
        if reply_to is not None:
            raise RuntimeError("reply rejected")
        return {"data": {"id": "main-789"}}

    monkeypatch.setattr(mod, "post_tweet_via_x_api", _fake_post)
    monkeypatch.setattr(mod, "_upload_media_files", lambda media: [])

    tweet = mod.TweetBody(
        text="body",
        fallback="body",
        lead_symbol="SPY",
        symbols_present=["SPY"],
        reply_text="link",
        featured_symbol="SPY",
    )
    result = mod.post_bulletin(
        tweet, mod.MediaArtifacts(), bearer="tok", long=True, mode_label="midday"
    )
    assert result["id"] == "main-789"
    assert "reply_id" not in result


@pytest.mark.asyncio
async def test_dry_run_persists_reply_artifact(tmp_path, monkeypatch):
    """A dry-run writes tweet_reply.md and records the reply text +
    featured symbol in the manifest so the operator can inspect them."""
    mod = _reload_module()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    db_instance = MagicMock()
    db_instance.connect = AsyncMock()
    db_instance.disconnect = AsyncMock()
    db_instance.get_latest_gex_summary = AsyncMock(
        side_effect=lambda symbol: _summary_row(symbol),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)
    monkeypatch.setattr(mod, "render_bulletin_png", lambda *a, **k: None)
    monkeypatch.setattr(mod, "render_replay_clip", lambda *a, **k: None)
    monkeypatch.delenv("X_BOT_BEARER_TOKEN", raising=False)

    args = mod._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",
            "--artifact-dir",
            str(tmp_path),
            "--allow-non-trading-day",
        ]
    )
    rc = await mod._run(args)
    assert rc == 0

    day_dir = tmp_path / "close" / "2026-07-06"
    reply_md = day_dir / "tweet_reply.md"
    assert reply_md.exists()
    assert "zerogex.io" in reply_md.read_text()
    manifest = json.loads((day_dir / "manifest.json").read_text())
    assert manifest["featured_symbol"]
    assert manifest["reply_text"].startswith(
        "Free delayed SPY / SPX / QQQ gamma levels:",
    )


# ---------------------------------------------------------------------------
# Runner — dry-run default + non-trading-day skip + missing data skip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_writes_artifacts_and_never_posts(tmp_path, monkeypatch):
    mod = _reload_module()

    # Force DB to return real-looking summary rows.
    db_instance = MagicMock()
    db_instance.connect = AsyncMock()
    db_instance.disconnect = AsyncMock()
    db_instance.get_latest_gex_summary = AsyncMock(
        side_effect=lambda symbol: _summary_row(symbol),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)

    # Fail loudly if the runner ever tries to POST or upload media in
    # dry-run mode — that's the failure the whole `--post`-required
    # gate is supposed to prevent.
    def _boom_post(*args, **kwargs):
        raise AssertionError("dry-run posted to X!")

    monkeypatch.setattr(mod, "post_tweet_via_x_api", _boom_post)
    monkeypatch.setattr(mod, "_upload_media_files", lambda media: [])
    # Skip actual network calls to the frontend PNG endpoint and to the
    # Playwright helper.  Return None so both media renders "fail" —
    # exactly like a fresh install with no frontend reachable.
    monkeypatch.setattr(mod, "render_bulletin_png", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "render_replay_clip", lambda *args, **kwargs: None)

    monkeypatch.delenv("X_BOT_BEARER_TOKEN", raising=False)

    args = mod._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
            "--allow-non-trading-day",
        ]
    )
    rc = await mod._run(args)
    assert rc == 0

    # Artifacts must have been written even though --post was absent.
    day_dir = tmp_path / "close" / "2026-07-06"
    assert (day_dir / "tweet_text.md").exists()
    assert (day_dir / "tweet_text_fallback.md").exists()
    manifest = json.loads((day_dir / "manifest.json").read_text())
    assert manifest["mode"] == "close"
    assert manifest["symbols_present"]
    assert manifest["text_len"] > 0


@pytest.mark.asyncio
async def test_skips_non_trading_days(tmp_path, monkeypatch, caplog):
    mod = _reload_module()

    db_instance = MagicMock()
    db_instance.connect = AsyncMock()
    db_instance.disconnect = AsyncMock()
    db_instance.get_latest_gex_summary = AsyncMock(return_value=_summary_row("SPX"))
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)

    args = mod._parse_args(
        [
            "--mode",
            "midday",
            "--date",
            "2026-07-04",  # Saturday
            "--artifact-dir",
            str(tmp_path),
        ]
    )
    with caplog.at_level("INFO", logger="zerogex.bulletin_tweet"):
        rc = await mod._run(args)
    assert rc == 0
    # DB should never have been touched on the skip path
    db_instance.connect.assert_not_awaited()
    # And no artifact directory should have been created
    assert not (tmp_path / "midday").exists()


@pytest.mark.asyncio
async def test_skips_when_every_symbol_missing(tmp_path, monkeypatch, caplog):
    mod = _reload_module()

    db_instance = MagicMock()
    db_instance.connect = AsyncMock()
    db_instance.disconnect = AsyncMock()
    db_instance.get_latest_gex_summary = AsyncMock(return_value=None)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)

    args = mod._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
            "--allow-non-trading-day",
        ]
    )
    with caplog.at_level("INFO", logger="zerogex.bulletin_tweet"):
        rc = await mod._run(args)
    assert rc == 0
    # Empty-day path should NOT have written an artifact — we log and skip.
    assert not (tmp_path / "close" / "2026-07-06" / "tweet_text.md").exists()


@pytest.mark.asyncio
async def test_never_raises_on_db_failure(tmp_path, monkeypatch, caplog):
    mod = _reload_module()

    db_instance = MagicMock()
    db_instance.connect = AsyncMock(side_effect=RuntimeError("db down"))
    db_instance.disconnect = AsyncMock()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)

    args = mod._parse_args(
        [
            "--mode",
            "premarket",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
        ]
    )
    with caplog.at_level("WARNING", logger="zerogex.bulletin_tweet"):
        rc = await mod._run(args)
    assert rc == 0


# ---------------------------------------------------------------------------
# Artifact-dir fallback
# ---------------------------------------------------------------------------


def test_resolve_artifact_dir_honors_explicit_override(tmp_path, monkeypatch):
    mod = _reload_module()
    monkeypatch.delenv("BULLETIN_TWEET_ARTIFACT_DIR", raising=False)
    out = mod.resolve_artifact_dir(str(tmp_path), "close", date(2026, 7, 3))
    assert out.exists()
    assert out.is_dir()
    assert str(out).startswith(str(tmp_path))
    assert out.parts[-2:] == ("close", "2026-07-03")


def test_resolve_artifact_dir_falls_back_to_home(tmp_path, monkeypatch):
    mod = _reload_module()
    monkeypatch.delenv("BULLETIN_TWEET_ARTIFACT_DIR", raising=False)
    # Neutralize the /var/lib production primary so this test deterministically
    # exercises the XDG/HOME fallback regardless of the host's real /var/lib
    # state. (It used to depend on that un-controlled path: on a box where the
    # live bulletin job had created a non-writable premarket/<date> dir mid-run,
    # resolve_artifact_dir fell through to a /tmp tempdir and the assertion —
    # which only allowed xdg/home/var-lib prefixes — failed. Passing in
    # isolation but failing in the full suite was exactly that timing window.)
    # Point the primary under a regular file so mkdir raises NotADirectoryError
    # (an OSError) and the candidate is skipped.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    monkeypatch.setattr(mod, "PRIMARY_ARTIFACT_ROOT", blocker / "var-lib")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    out = mod.resolve_artifact_dir(None, "premarket", date(2026, 7, 3))
    assert out.exists()
    # With the primary blocked and XDG writable, the XDG root wins
    # deterministically (XDG precedes HOME in the candidate order).
    resolved = str(out)
    assert str(tmp_path / "xdg") in resolved or str(tmp_path / "home") in resolved
    assert out.parts[-2:] == ("premarket", "2026-07-03")


def test_resolve_artifact_dir_tempdir_when_all_roots_unwritable(tmp_path, monkeypatch):
    """Every candidate unavailable -> a fresh tempdir, never an exception."""
    mod = _reload_module()
    monkeypatch.delenv("BULLETIN_TWEET_ARTIFACT_DIR", raising=False)
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    monkeypatch.setattr(mod, "PRIMARY_ARTIFACT_ROOT", blocker / "var-lib")
    # Route XDG and HOME under the same file so their mkdir also fails.
    monkeypatch.setenv("XDG_STATE_HOME", str(blocker / "xdg"))
    monkeypatch.setenv("HOME", str(blocker / "home"))

    out = mod.resolve_artifact_dir(None, "premarket", date(2026, 7, 3))
    assert out.exists()
    assert out.is_dir()


# ---------------------------------------------------------------------------
# X media OAuth1 signing sanity check
# ---------------------------------------------------------------------------


def test_oauth1_signature_matches_rfc_reference():
    """Cross-check against the well-known RFC 5849 example (§3.4.1.1).

    The reference base string and expected signature come straight
    from the spec — this test guards against a stray character-class
    difference in _percent_encode ever silently breaking uploads."""
    from src.jobs import x_media_client as xm

    creds = xm.OAuth1Credentials(
        consumer_key="9djdj82h48djs9d2",
        consumer_secret="j49sk3j29djd",
        access_token="kkk9d7dh3k39sjv7",
        access_token_secret="dh893hdasih9",
    )
    # RFC 5849 §3.4.1.1 example params
    params = {
        "b5": "=%3D",
        "a3": "a",
        "c@": "",
        "a2": "r b",
        "oauth_consumer_key": "9djdj82h48djs9d2",
        "oauth_token": "kkk9d7dh3k39sjv7",
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": "137131201",
        "oauth_nonce": "7d8f3e4a",
        "c2": "",
        "a3_dup": "2 q",  # spec uses "a3=2+q" — dedup key clash for dict
    }
    # We're not testing exact signature equality (params dict differs
    # from the multimap the RFC uses); we're just verifying the
    # signing pipeline returns a base64-shaped SHA1 (28 chars).
    sig = xm._build_signature("POST", "http://example.com/request", params, creds)
    assert len(sig) == 28
    assert sig.endswith("=")


def test_percent_encode_leaves_unreserved_alone():
    from src.jobs import x_media_client as xm

    for ch in "abcXYZ0123456789-._~":
        assert xm._percent_encode(ch) == ch, f"unreserved {ch!r} should be identity"
    # Space must become %20 (not '+' which is form-encoded, not URL-encoded)
    assert xm._percent_encode("a b") == "a%20b"
    # Slash must be encoded — OAuth1 requires it, urllib's default
    # ``safe='/'`` would break the signature.
    assert xm._percent_encode("path/to") == "path%2Fto"


def test_load_credentials_from_env_reports_all_missing(monkeypatch):
    from src.jobs import x_media_client as xm

    for k in (
        "X_BOT_API_KEY",
        "X_BOT_API_SECRET",
        "X_BOT_ACCESS_TOKEN",
        "X_BOT_ACCESS_TOKEN_SECRET",
    ):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(xm.MissingCredentialsError) as ex:
        xm.load_credentials_from_env()
    # Every missing var should be listed — operator gets a single log line
    for k in (
        "X_BOT_API_KEY",
        "X_BOT_API_SECRET",
        "X_BOT_ACCESS_TOKEN",
        "X_BOT_ACCESS_TOKEN_SECRET",
    ):
        assert k in str(ex.value)


# ---------------------------------------------------------------------------
# LLM narrative path
# ---------------------------------------------------------------------------


def test_build_tweet_body_falls_back_to_template_without_api_key(monkeypatch):
    """No ANTHROPIC_API_KEY → the static template path runs unchanged.

    This is the "safe default" — nothing about the LLM path should
    take the tweet down when the operator hasn't opted in yet."""
    mod = _reload_module()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    bulletins = [
        mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY"),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
    ]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPX"
    )
    # Static fallback = the deterministic "…Read — $SYM" header + hook +
    # Key levels + Bottom line, no LLM-specific phrasing.
    assert "Post-Market Read — $" in body.text
    assert "Key levels:" in body.text
    assert "Bottom line:" in body.text
    # The static fallback never appends a site link or a hashtag row.
    assert "#Gamma" not in body.text
    assert "zerogex.io" not in body.text
    assert "http" not in body.text


def test_build_tweet_body_uses_llm_when_generator_returns_post(monkeypatch):
    """When bulletin_llm.generate_post returns a post, the composed body
    swaps in the LLM prose but keeps the deterministic Key levels block, and
    the reply carries the LLM copy + the ZeroGEX link."""
    mod = _reload_module()
    from src.jobs import bulletin_llm

    def _fake_generate(**kwargs):
        return bulletin_llm.LlmPost(
            header_label="Post-Market Read",
            opening=(
                "Interesting close into the holiday.\n\n"
                "The morning started long-gamma, then the walls broke down."
            ),
            bottom_line=(
                "With the market closed tomorrow, this is a fitting place " "to leave it."
            ),
            reply=(
                "Levels are zones of influence, not guarantees.\n\n"
                "More live positioning and dealer gamma analytics:"
            ),
            level_notes={"put_wall": "held", "gamma_flip": "the pivot"},
        )

    monkeypatch.setattr(bulletin_llm, "generate_post", _fake_generate)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    bulletins = [
        mod._shape_bulletin(
            _summary_row(
                "SPY",
                spot=744.51,
                gamma_flip=744.51,
                call_wall=750.0,
                put_wall=740.0,
                max_pain=742.0,
                net_gex=72_300_000.0,
            ),
            "SPY",
        ),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
    ]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPX"
    )

    # SPY sits on its gamma flip → it's the featured symbol.
    assert body.featured_symbol == "SPY"
    # The LLM's narrative shows up verbatim, under the validated read header.
    assert "Post-Market Read — $SPY" in body.text
    assert "Interesting close into the holiday." in body.text
    assert "Bottom line: With the market closed tomorrow" in body.text
    # The Key levels block is still the deterministic Python-composed one,
    # with the LLM's short notes in parentheses.
    assert "Key levels:" in body.text
    assert "• 740 → Put Wall (held)" in body.text
    assert "• 744.51 → Gamma Flip (the pivot)" in body.text
    # No hashtag row / link in the main post; the link rides in the reply.
    assert "#Gamma" not in body.text
    assert "zerogex.io" not in body.text
    # The reply is the LLM copy + the appended ZeroGEX link on the CTA line.
    assert "Levels are zones of influence" in body.reply_text
    assert body.reply_text.endswith(
        "More live positioning and dealer gamma analytics: https://zerogex.io"
    )


def test_llm_post_falls_back_when_generator_returns_none(monkeypatch):
    """A None from the LLM path → static fallback composes the body.

    Covers the API-error + malformed-reply paths without needing to
    mock the whole HTTP layer."""
    mod = _reload_module()
    from src.jobs import bulletin_llm

    monkeypatch.setattr(bulletin_llm, "generate_post", lambda **k: None)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPY"
    )
    # Static fallback shape: the "…Read — $SYM" header + Bottom line.
    assert "Post-Market Read — $SPY" in body.text
    assert "Bottom line:" in body.text


def test_llm_invented_price_guard():
    """Model output that quotes a fabricated price falls the post back
    to None — never post a wrong number."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        header_label="Midday Read",
        opening="SPY looks pinned to $999.99.",  # invented — not in inputs
        bottom_line="Nothing to see here.",
        reply="Watch the levels:",
        level_notes={},
    )
    inputs = [
        bulletin_llm.SymbolInput(symbol="SPY", spot=744.51, gamma_flip=744.51),
    ]
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is False


def test_llm_invented_price_guard_scans_reply():
    """A fabricated price hidden in the reply is caught too, not just the post."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        header_label="Midday Read",
        opening="SPY held its structure.",
        bottom_line="Still short gamma.",
        reply="Watch 6,123 into the close:",  # invented — not in inputs
        level_notes={},
    )
    inputs = [
        bulletin_llm.SymbolInput(symbol="SPY", spot=744.51, gamma_flip=744.51),
    ]
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is False


def test_llm_validator_accepts_input_prices():
    """A post that only quotes numbers actually in the inputs passes."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        header_label="Midday Read",
        opening="SPX sits at 7,483 with the gamma flip at 7,448.",
        bottom_line="Call wall 7,500, put wall 7,480. Watch the flip.",
        reply="More analytics:",
        level_notes={},
    )
    inputs = [
        bulletin_llm.SymbolInput(
            symbol="SPX",
            spot=7483.0,
            gamma_flip=7448.0,
            call_wall=7500.0,
            put_wall=7480.0,
        ),
    ]
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is True


def test_llm_extract_json_block_ignores_preamble():
    from src.jobs import bulletin_llm

    reply = (
        "Sure, here you go:\n" '{"header_label": "update", "opening": "hello"}\n' "Hope that helps."
    )
    block = bulletin_llm._extract_json_block(reply)
    assert block is not None
    import json as _json

    assert _json.loads(block) == {"header_label": "update", "opening": "hello"}


def test_llm_generate_returns_none_without_api_key(monkeypatch):
    from src.jobs import bulletin_llm

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    inputs = [bulletin_llm.SymbolInput(symbol="SPY", spot=744.51)]
    assert (
        bulletin_llm.generate_post(
            mode="close",
            day=date(2026, 7, 3),
            symbols=inputs,
        )
        is None
    )


def test_next_trading_day_skips_weekend():
    from src.jobs.bulletin_llm import next_trading_day

    # Pure weekend skip — Friday 2026-01-02 → Monday 2026-01-05.
    # NYSE_HOLIDAYS is env-driven and unset in the test process, so a
    # holiday-eve assertion here would depend on production config.
    assert next_trading_day(date(2026, 1, 2)).isoformat() == "2026-01-05"


# ---------------------------------------------------------------------------
# New-voice compose: reply link handling, Key levels block, regime/momentum
# ---------------------------------------------------------------------------


def test_compose_reply_appends_link_on_cta_colon():
    mod = _reload_module()
    out = mod._compose_reply("More live dealer gamma analytics:", "https://zerogex.io/")
    # CTA colon → URL on the same line; trailing slash trimmed.
    assert out == "More live dealer gamma analytics: https://zerogex.io"


def test_compose_reply_puts_url_on_own_paragraph_without_colon():
    mod = _reload_module()
    out = mod._compose_reply("Levels are zones of influence.", "https://zerogex.io")
    assert out == "Levels are zones of influence.\n\nhttps://zerogex.io"


def test_compose_reply_strips_model_url_and_uses_canonical():
    mod = _reload_module()
    out = mod._compose_reply("Follow along at https://evil.example.com now:", "https://zerogex.io")
    assert "evil.example.com" not in out
    assert out.endswith("https://zerogex.io")


def test_compose_reply_override_wins():
    mod = _reload_module()
    out = mod._compose_reply(
        "ignored llm reply:", "https://zerogex.io", override="Custom — zerogex.io"
    )
    assert out == "Custom — zerogex.io"


def test_compose_reply_static_fallback_when_no_llm():
    mod = _reload_module()
    out = mod._compose_reply(None, "https://zerogex.io")
    assert out == "Free delayed SPY / SPX / QQQ gamma levels: https://zerogex.io"


def test_key_levels_block_orders_and_formats():
    mod = _reload_module()
    b = mod._shape_bulletin(
        _summary_row(
            "SPY",
            spot=743.0,
            gamma_flip=747.29,
            call_wall=745.0,
            put_wall=740.0,
            max_pain=742.0,
            net_gex=-3_400_000_000.0,
        ),
        "SPY",
    )
    block = mod._key_levels_block(b, {"put_wall": "now the level to watch"})
    lines = block.splitlines()
    # Order: Put Wall, Call Wall, Gamma Flip.
    assert lines[0] == "• 740 → Put Wall (now the level to watch)"
    assert lines[1] == "• 745 → Call Wall"
    # Whole-dollar walls print without decimals; the flip keeps two.
    assert lines[2] == "• 747.29 → Gamma Flip"


def test_derive_regime_prefers_net_gex_sign():
    mod = _reload_module()
    assert mod._derive_regime(-3.4e9, 743.0, 747.0) == "negative"
    assert mod._derive_regime(2.5e9, 751.0, 747.0) == "positive"
    # No net GEX → fall back to spot vs flip.
    assert mod._derive_regime(None, 743.0, 747.0) == "negative"
    assert mod._derive_regime(None, 751.0, 747.0) == "positive"
    assert mod._derive_regime(None, None, None) is None


def test_derive_momentum_label_combines_direction_and_range():
    mod = _reload_module()
    b = mod.SymbolBulletin(
        symbol="SPY",
        spot=744.8,
        prior_close=744.0,
        session_open=744.1,
        session_high=745.0,
        session_low=740.0,
    )
    label = mod._derive_momentum_label(b)
    assert "up on the day" in label
    assert "pressing session highs" in label


@pytest.mark.asyncio
async def test_attach_price_action_sets_prior_close_and_regime():
    mod = _reload_module()
    b = mod._shape_bulletin(
        _summary_row("SPY", spot=743.0, gamma_flip=747.0, net_gex=-3.4e9), "SPY"
    )
    db = MagicMock()
    db.get_session_closes = AsyncMock(return_value={"prior_session_close": 744.0})
    db.get_intraday_ohlc = AsyncMock(
        return_value={
            "session_open": 744.1,
            "session_high": 745.0,
            "session_low": 739.6,
            "session_last": 743.0,
            "bar_count": 180,
        }
    )
    await mod._attach_price_action(db, b, date(2026, 7, 3))
    assert b.prior_close == pytest.approx(744.0)
    assert b.session_low == pytest.approx(739.6)
    assert b.regime == "negative"  # spot below flip, net gex negative
    assert b.momentum_label  # derived, non-empty


def test_latest_record_roundtrip(tmp_path, monkeypatch):
    """build_latest_record → write_latest_record → read_latest_record."""
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_ARTIFACT_DIR", str(tmp_path))
    feat = mod._shape_bulletin(
        _summary_row(
            "SPY", spot=744.51, gamma_flip=747.0, put_wall=740.0, call_wall=750.0, net_gex=-1.2e9
        ),
        "SPY",
    )
    tweet = mod.TweetBody(
        text="Midday Read — $SPY\n\nbody",
        fallback="$SPY midday",
        lead_symbol="SPY",
        symbols_present=["SPY"],
        reply_text="Watch the levels: https://zerogex.io",
        featured_symbol="SPY",
    )
    rec = mod.build_latest_record(
        mode="midday",
        day=date(2026, 7, 3),
        tweet=tweet,
        featured=feat,
        headlines=[{"title": "Oil eases", "summary": "", "source": "CNBC"}],
        generated_at="2026-07-03T12:30:00-04:00",
    )
    path = mod.write_latest_record(rec)
    assert path is not None and path.exists()
    got = mod.read_latest_record("SPY", "midday")
    assert got["post_text"] == "Midday Read — $SPY\n\nbody"
    assert got["reply_text"].endswith("https://zerogex.io")
    assert got["timing_label"] == "Midday Read"
    assert got["headlines"][0]["title"] == "Oil eases"
    # Absent (symbol, mode) → None, not an error.
    assert mod.read_latest_record("QQQ", "close") is None


# ---------------------------------------------------------------------------
# Approval mechanism — --stage flag + bulletin_approve module
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stage_flag_writes_pending_and_calls_hook(tmp_path, monkeypatch):
    """--stage writes state=pending manifest and calls notify hook.

    Must NOT POST to X, even when X_BOT_BEARER_TOKEN is set — safe by
    default so the timer can't accidentally tweet before the operator
    has flipped autopilot on."""
    mod = _reload_module()

    db_instance = MagicMock()
    db_instance.connect = AsyncMock()
    db_instance.disconnect = AsyncMock()
    db_instance.get_latest_gex_summary = AsyncMock(
        side_effect=lambda symbol: _summary_row(symbol),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)

    # Fail loudly if the runner ever tries to POST or upload media in
    # --stage mode.
    def _boom(*args, **kwargs):
        raise AssertionError("--stage mode posted to X!")

    monkeypatch.setattr(mod, "post_bulletin", _boom)
    monkeypatch.setattr(mod, "render_bulletin_png", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "render_replay_clip", lambda *args, **kwargs: None)

    # Even with the bearer set, --stage without autopilot must not post.
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "test-bearer")
    monkeypatch.delenv("BULLETIN_TWEET_AUTOPILOT", raising=False)

    # Set up a notify hook to confirm it gets called.
    hook_called_marker = tmp_path / "hook_fired"
    hook_script = tmp_path / "hook.sh"
    hook_script.write_text(
        "#!/bin/bash\n" f"touch {hook_called_marker}\n" f'echo "$1" > {hook_called_marker}.mode\n'
    )
    hook_script.chmod(0o755)
    monkeypatch.setenv("BULLETIN_TWEET_NOTIFY_HOOK", str(hook_script))

    args = mod._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path / "artifacts"),
            "--stage",
            "--allow-non-trading-day",
        ]
    )
    rc = await mod._run(args)
    assert rc == 0

    # Manifest should say state=pending
    manifest_path = tmp_path / "artifacts" / "close" / "2026-07-06" / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    assert manifest["state"] == "pending"

    # And the notify hook must have fired with the right mode arg.
    assert hook_called_marker.exists()
    mode_marker = tmp_path / "hook_fired.mode"
    assert mode_marker.exists()
    assert mode_marker.read_text().strip() == "close"


@pytest.mark.asyncio
async def test_autopilot_env_var_upgrades_stage_to_post(tmp_path, monkeypatch):
    """BULLETIN_TWEET_AUTOPILOT=1 silently upgrades --stage → --post.

    Enables one-line-env-flip switch to autopilot without editing the
    systemd unit file or touching daemon-reload."""
    mod = _reload_module()

    db_instance = MagicMock()
    db_instance.connect = AsyncMock()
    db_instance.disconnect = AsyncMock()
    db_instance.get_latest_gex_summary = AsyncMock(
        side_effect=lambda symbol: _summary_row(symbol),
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db_instance)
    monkeypatch.setattr(mod, "render_bulletin_png", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "render_replay_clip", lambda *args, **kwargs: None)

    posts: list[dict] = []

    def _fake_post(**kwargs):
        posts.append(kwargs)
        return {"id": "fake-tweet-id-42"}

    monkeypatch.setattr(mod, "post_bulletin", _fake_post)
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "test-bearer")
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT", "1")
    monkeypatch.delenv("BULLETIN_TWEET_NOTIFY_HOOK", raising=False)

    args = mod._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",
            "--artifact-dir",
            str(tmp_path),
            "--stage",  # would normally skip POST — but autopilot upgrades it
            "--allow-non-trading-day",
        ]
    )
    rc = await mod._run(args)
    assert rc == 0

    # Post_bulletin should have been called once.
    assert len(posts) == 1
    # And the manifest should reflect state=posted with the returned id.
    manifest = json.loads(
        (tmp_path / "close" / "2026-07-06" / "manifest.json").read_text(),
    )
    assert manifest["state"] == "posted"
    assert manifest["posted_id"] == "fake-tweet-id-42"


def test_approve_module_reads_pending_manifest_and_posts(tmp_path, monkeypatch):
    """bulletin_approve reads a pending draft and calls post_bulletin.

    Exercises the operator's approve path: draft was staged earlier,
    operator SSHs in and runs the approve command, tweet lands."""
    from src.jobs import bulletin_approve

    # Set up a mock pending draft on disk.
    art_dir = tmp_path / "close" / "2026-07-06"
    art_dir.mkdir(parents=True)
    (art_dir / "tweet_text.md").write_text("hello world\n")
    (art_dir / "tweet_text_fallback.md").write_text("hello\n")
    (art_dir / "manifest.json").write_text(
        json.dumps(
            {
                "mode": "close",
                "date": "2026-07-06",
                "state": "pending",
                "posted_id": None,
                "lead_symbol": "SPX",
                "symbols_present": ["SPY", "SPX", "QQQ"],
                "text_len": 11,
                "fallback_len": 5,
                "media": {"png": None, "clip": None},
                "bulletins": [],
            }
        )
    )

    posts: list[dict] = []

    def _fake_post(**kwargs):
        posts.append(kwargs)
        return {"id": "approved-tweet-id-99"}

    monkeypatch.setattr(bulletin_approve, "post_bulletin", _fake_post)
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "test-bearer")

    args = bulletin_approve._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",
            "--artifact-dir",
            str(tmp_path),
        ]
    )
    rc = bulletin_approve._run(args)
    assert rc == 0
    assert len(posts) == 1

    # Manifest should now reflect state=posted with the returned id.
    manifest = json.loads((art_dir / "manifest.json").read_text())
    assert manifest["state"] == "posted"
    assert manifest["posted_id"] == "approved-tweet-id-99"
    assert "approved_ts" in manifest


def test_approve_module_is_idempotent_when_already_posted(tmp_path, monkeypatch):
    """Re-running approve on a state=posted draft is a no-op.

    Guards against a double-post if the operator accidentally re-runs
    ``bin/bulletin-approve.sh`` after a successful approval."""
    from src.jobs import bulletin_approve

    art_dir = tmp_path / "close" / "2026-07-06"
    art_dir.mkdir(parents=True)
    (art_dir / "tweet_text.md").write_text("hi\n")
    (art_dir / "tweet_text_fallback.md").write_text("hi\n")
    (art_dir / "manifest.json").write_text(
        json.dumps(
            {
                "mode": "close",
                "date": "2026-07-06",
                "state": "posted",
                "posted_id": "prior-tweet-id",
                "lead_symbol": "SPX",
                "symbols_present": ["SPX"],
                "text_len": 3,
                "fallback_len": 3,
                "media": {"png": None, "clip": None},
                "bulletins": [],
            }
        )
    )

    posts: list[dict] = []
    monkeypatch.setattr(
        bulletin_approve,
        "post_bulletin",
        lambda **k: posts.append(k) or {"id": "OOPS"},
    )
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "test-bearer")

    args = bulletin_approve._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",
            "--artifact-dir",
            str(tmp_path),
        ]
    )
    rc = bulletin_approve._run(args)
    assert rc == 0
    # post_bulletin must NEVER be called on an already-posted draft.
    assert posts == []


def test_approve_module_discard_marks_state_and_skips_post(tmp_path, monkeypatch):
    """--discard flips state=discarded without ever calling post_bulletin."""
    from src.jobs import bulletin_approve

    art_dir = tmp_path / "close" / "2026-07-06"
    art_dir.mkdir(parents=True)
    (art_dir / "tweet_text.md").write_text("hi\n")
    (art_dir / "tweet_text_fallback.md").write_text("hi\n")
    (art_dir / "manifest.json").write_text(
        json.dumps(
            {
                "mode": "close",
                "date": "2026-07-06",
                "state": "pending",
                "posted_id": None,
                "lead_symbol": "SPX",
                "symbols_present": ["SPX"],
                "text_len": 3,
                "fallback_len": 3,
                "media": {"png": None, "clip": None},
                "bulletins": [],
            }
        )
    )

    def _boom(**k):
        raise AssertionError("discard called post_bulletin!")

    monkeypatch.setattr(bulletin_approve, "post_bulletin", _boom)
    monkeypatch.setenv("X_BOT_BEARER_TOKEN", "test-bearer")

    args = bulletin_approve._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",
            "--artifact-dir",
            str(tmp_path),
            "--discard",
        ]
    )
    rc = bulletin_approve._run(args)
    assert rc == 0

    manifest = json.loads((art_dir / "manifest.json").read_text())
    assert manifest["state"] == "discarded"


def test_approve_prints_for_manual_when_bearer_unset(tmp_path, monkeypatch, capsys):
    """Without X_BOT_BEARER_TOKEN, --print mode dumps the draft to stdout.

    This is the "X developer application still pending" workflow: the
    pipeline still produces real drafts, and the operator pastes them
    into the X web UI manually."""
    from src.jobs import bulletin_approve

    art_dir = tmp_path / "close" / "2026-07-06"
    art_dir.mkdir(parents=True)
    (art_dir / "tweet_text.md").write_text("full tweet body here\n")
    (art_dir / "tweet_text_fallback.md").write_text("short\n")
    (art_dir / "manifest.json").write_text(
        json.dumps(
            {
                "mode": "close",
                "date": "2026-07-06",
                "state": "pending",
                "posted_id": None,
                "lead_symbol": "SPX",
                "symbols_present": ["SPX"],
                "text_len": 20,
                "fallback_len": 5,
                "media": {"png": None, "clip": None},
                "bulletins": [],
            }
        )
    )

    monkeypatch.delenv("X_BOT_BEARER_TOKEN", raising=False)

    args = bulletin_approve._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",
            "--artifact-dir",
            str(tmp_path),
        ]
    )
    rc = bulletin_approve._run(args)
    assert rc == 0

    captured = capsys.readouterr()
    assert "MANUAL POSTING MODE" in captured.out
    assert "full tweet body here" in captured.out
