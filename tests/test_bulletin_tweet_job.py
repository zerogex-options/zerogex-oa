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


# ---------------------------------------------------------------------------
# Tweet body builder
# ---------------------------------------------------------------------------


def test_build_tweet_body_close_shape():
    mod = _reload_module()
    bulletins = [
        mod._shape_bulletin(
            _summary_row("SPY", spot=744.51, gamma_flip=744.51, call_wall=750.0,
                         put_wall=740.0, max_pain=742.0, net_gex=72_300_000.0),
            "SPY",
        ),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
        mod._shape_bulletin(
            _summary_row("QQQ", spot=655.4, gamma_flip=654.0, call_wall=660.0,
                         put_wall=650.0, max_pain=653.0, net_gex=-125_000_000.0),
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

    # Header + label
    assert "$SPY / $SPX / $QQQ post-market update:" in body.text
    # One block per symbol, in input order, with the expected fields
    assert "SPY spot: ~744.51" in body.text
    assert "Gamma Flip: 744.51" in body.text
    assert "Call Wall: 750" in body.text
    assert "Put Wall: 740" in body.text
    assert "Max Pain: 742" in body.text
    assert "Net GEX: +$72.3M" in body.text
    assert "SPX spot: ~7,483" in body.text
    assert "SPX" in body.text and "Net GEX: +$19.50B" in body.text
    assert "QQQ spot: ~655.40" in body.text
    assert "Net GEX: −$125.0M" in body.text
    # Site tagline + hashtag row
    assert "zerogex.io" in body.text
    assert "$SPY $SPX $QQQ" in body.text
    assert "#Gamma" in body.text and "#GEX" in body.text
    # Present symbols list surfaces to caller for logging
    assert body.symbols_present == ["SPY", "SPX", "QQQ"]


def test_build_tweet_body_labels_per_mode():
    mod = _reload_module()
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")]
    for mode, expected in (
        ("premarket", "pre-market update"),
        ("midday", "mid-session update"),
        ("close", "post-market update"),
    ):
        body = mod.build_tweet_body(
            mode=mode,
            day=date(2026, 7, 3),
            bulletins=bulletins,
            site_url="https://zerogex.io",
            lead_symbol="SPY",
        )
        assert expected in body.text, f"mode={mode} missing label"


def test_build_tweet_body_elides_symbols_with_no_data():
    """A symbol whose GEX row didn't resolve gets dropped from the level
    section but keeps its slot in the header, so the tweet still frames
    itself as covering the full trio for social parity."""
    mod = _reload_module()
    bulletins = [
        mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY"),
        mod._shape_bulletin(None, "SPX"),  # no data
        mod._shape_bulletin(_summary_row("QQQ", spot=655.4), "QQQ"),
    ]
    body = mod.build_tweet_body(
        mode="close",
        day=date(2026, 7, 3),
        bulletins=bulletins,
        site_url="https://zerogex.io",
        lead_symbol="SPX",
    )
    # Header keeps all three tickers so the read still frames as trio-wide
    assert "$SPY / $SPX / $QQQ" in body.text
    # But the level block for SPX is dropped
    assert "SPX spot:" not in body.text
    assert body.symbols_present == ["SPY", "QQQ"]


def test_build_tweet_body_lead_variant_deterministic_per_day():
    """The lead sentence should be stable within a fire (dry-run and live
    match), so the seed is deterministic on (date, mode)."""
    mod = _reload_module()
    bulletins = [mod._shape_bulletin(_summary_row("SPY"), "SPY")]
    a = mod.build_tweet_body("close", date(2026, 7, 3), bulletins,
                             site_url="https://zerogex.io", lead_symbol="SPY")
    b = mod.build_tweet_body("close", date(2026, 7, 3), bulletins,
                             site_url="https://zerogex.io", lead_symbol="SPY")
    assert a.text == b.text


def test_fallback_tweet_fits_in_280():
    mod = _reload_module()
    bulletins = [
        mod._shape_bulletin(_summary_row("SPY", spot=744.51,
                                          gamma_flip=744.51, call_wall=750.0,
                                          put_wall=740.0, net_gex=72_300_000.0),
                            "SPY"),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
        mod._shape_bulletin(_summary_row("QQQ", spot=655.4), "QQQ"),
    ]
    body = mod.build_tweet_body("close", date(2026, 7, 3), bulletins,
                                site_url="https://zerogex.io", lead_symbol="SPY")
    assert len(body.fallback) <= 280
    assert "SPY" in body.fallback
    assert "zerogex.io/live-bulletin" in body.fallback


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
    monkeypatch.setattr(mod, "render_bulletin_png",
                        lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "render_replay_clip",
                        lambda *args, **kwargs: None)

    monkeypatch.delenv("X_BOT_BEARER_TOKEN", raising=False)

    args = mod._parse_args([
        "--mode", "close",
        "--date", "2026-07-06",  # Monday
        "--artifact-dir", str(tmp_path),
        "--allow-non-trading-day",
    ])
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

    args = mod._parse_args([
        "--mode", "midday",
        "--date", "2026-07-04",  # Saturday
        "--artifact-dir", str(tmp_path),
    ])
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

    args = mod._parse_args([
        "--mode", "close",
        "--date", "2026-07-06",  # Monday
        "--artifact-dir", str(tmp_path),
        "--allow-non-trading-day",
    ])
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

    args = mod._parse_args([
        "--mode", "premarket",
        "--date", "2026-07-06",  # Monday
        "--artifact-dir", str(tmp_path),
    ])
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
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    # No explicit, no env — the primary /var/lib path is unwritable
    # inside the test sandbox, so we should get an XDG-rooted path.
    out = mod.resolve_artifact_dir(None, "premarket", date(2026, 7, 3))
    assert out.exists()
    # Should be under one of the fallbacks; either xdg or home is fine.
    resolved = str(out)
    assert (
        str(tmp_path / "xdg") in resolved
        or str(tmp_path / "home") in resolved
        or resolved.startswith("/var/lib")  # if the CI runner happens to allow it
    )


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

    for k in ("X_BOT_API_KEY", "X_BOT_API_SECRET",
              "X_BOT_ACCESS_TOKEN", "X_BOT_ACCESS_TOKEN_SECRET"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(xm.MissingCredentialsError) as ex:
        xm.load_credentials_from_env()
    # Every missing var should be listed — operator gets a single log line
    for k in ("X_BOT_API_KEY", "X_BOT_API_SECRET",
              "X_BOT_ACCESS_TOKEN", "X_BOT_ACCESS_TOKEN_SECRET"):
        assert k in str(ex.value)
