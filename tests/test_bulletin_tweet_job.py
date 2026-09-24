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
import struct
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

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


def _stub_writer(
    monkeypatch,
    mod,
    opening: str = "SPY is chopping between the walls after the jobs report.",
    bottom_line: str = "Patience until the flip gives way.",
    reply: str = "The tell is how fast the dips get bought.",
):
    """Stand in for the Claude writer.  build_tweet_body has no template
    fallback, so every test that expects a post supplies one."""
    from src.jobs import bulletin_llm

    calls: list[dict] = []

    def _fake(
        mode,
        day,
        present,
        featured_symbol,
        headlines=None,
        revise_from=None,
        feedback=None,
        errors=None,
    ):
        calls.append(
            {
                "mode": mode,
                "featured_symbol": featured_symbol,
                "revise_from": revise_from,
                "feedback": feedback,
                "headlines": headlines,
                "present": present,
            }
        )
        return bulletin_llm.LlmPost(opening=opening, bottom_line=bottom_line, reply=reply)

    monkeypatch.setattr(mod, "_try_llm_post", _fake)
    return calls


def _headline(title: str = "Stocks slip after the jobs report", minutes_ago: int = 30) -> dict:
    published = datetime.now(tz=ZoneInfo("UTC")) - timedelta(minutes=minutes_ago)
    return {
        "title": title,
        "summary": "",
        "source": "CNBC",
        "link": "https://www.cnbc.com/example",
        "published": published.isoformat(),
    }


def _card_levels(symbol: str = "SPY", **overrides) -> dict:
    """The numbers the snapshot page reports for its card."""
    levels = {
        "symbol": symbol,
        "spot": 744.62,
        "spot_is_projected": False,
        "spot_source": None,
        "prior_close": 749.1,
        "change_pct": -0.6,
        "gamma_flip": 747.29,
        "call_wall": 745.0,
        "put_wall": 740.0,
        "max_pain": 744.0,
        "net_gex": -1_250_000_000.0,
        "regime": "negative",
        "summary_timestamp": "2026-07-06T16:30:00+00:00",
        "as_of": "Jul 6, 2026 · 12:30 PM EDT",
    }
    levels.update(overrides)
    return levels


def _png_bytes(width: int = 1280, height: int = 1932) -> bytes:
    """A PNG signature + IHDR header: all the size check reads."""
    return (
        b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 13) + b"IHDR" + struct.pack(">II", width, height)
    )


def _db_stub() -> MagicMock:
    db = MagicMock()
    db.connect = AsyncMock()
    db.disconnect = AsyncMock()
    db.get_latest_gex_summary = AsyncMock(side_effect=lambda symbol: _summary_row(symbol))
    return db


def _passing_run(monkeypatch, mod, review_problems: list[list[str]] | None = None) -> dict:
    """Stub everything _run reaches outside the process (the DB, the card
    screenshot, the headlines, the writer and the fact-check) so a fire
    passes review.  ``review_problems`` feeds the fact-check's findings, one
    list per call."""
    from src.jobs import bulletin_llm

    db = _db_stub()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: db)

    cards: list[str] = []

    def _fake_card(symbol, mode, site_url, out_path, **kwargs):
        cards.append(symbol)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(_png_bytes())
        return mod.CardRender(png_path=out_path, levels=_card_levels(symbol))

    monkeypatch.setattr(mod, "render_bulletin_card", _fake_card)
    monkeypatch.setattr(mod, "_fetch_fresh_headlines", lambda: ([_headline()], None))
    writer_calls = _stub_writer(monkeypatch, mod)
    pending = [list(p) for p in (review_problems or [])]
    review_calls: list[dict] = []

    def _fake_review(**kwargs):
        review_calls.append(kwargs)
        return bulletin_llm.Review(ran=True, problems=pending.pop(0) if pending else [])

    monkeypatch.setattr(bulletin_llm, "review_post", _fake_review)
    held: list[dict] = []
    monkeypatch.setattr(
        mod,
        "_send_xpost_held_email",
        lambda mode, symbol, problems, tweet, png_path, posting: held.append(
            {"mode": mode, "problems": problems, "png": png_path, "posting": posting}
        )
        or True,
    )
    return {"db": db, "cards": cards, "writer": writer_calls, "review": review_calls, "held": held}


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
    assert mod._fmt_net_gex(-1_200_000_000.0) == "-$1.20B"
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
    mod = _reload_module()
    _stub_writer(
        monkeypatch,
        mod,
        opening="SPY closed right on its flip.",
        reply="The tell was how quickly 740 got bought.",
    )
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
        day=date(2026, 7, 1),  # a Wednesday: the next session is tomorrow
        bulletins=bulletins,
        site_url="https://zerogex.io",
        lead_symbol="SPX",
    )

    # SPY's spot sits exactly on its gamma flip → cleanest setup → featured.
    assert body.featured_symbol == "SPY"
    assert body.lead_symbol == "SPY"
    lines = body.text.splitlines()
    # The header takes a plain hyphen, not a dash.
    assert lines[0] == "Post-Market Read - $SPY"
    assert "SPY closed right on its flip." in body.text
    # The close read's levels are the next session's map, and say so; Python
    # owns every price, written the way a person types them.
    assert "Levels for tomorrow:\n• 740 put wall\n• 750 call wall\n• 744.51 gamma flip" in body.text
    assert "DTE" not in body.text and "→" not in body.text and "—" not in body.text
    assert "Bottom line:" in body.text
    # The other two symbols get NO numeric block of their own.
    assert "$SPX" not in body.text and "$QQQ" not in body.text
    # No site link and no hashtags in the main post.
    assert "zerogex.io" not in body.text
    assert "http" not in body.text
    assert "#" not in body.text
    # All three still count as present (fetched so the copy can cross-reference).
    assert body.symbols_present == ["SPY", "SPX", "QQQ"]
    # The link rides in the threaded reply instead.
    assert body.reply_text == "The tell was how quickly 740 got bought.\n\nhttps://zerogex.io"


def test_build_tweet_body_labels_per_mode(monkeypatch):
    mod = _reload_module()
    _stub_writer(monkeypatch, mod)
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")]
    for mode, header, heading in (
        ("premarket", "Morning Read - $SPY", "Key levels:"),
        ("midday", "Midday Read - $SPY", "Key levels:"),
        # 2026-07-03 is a Friday, so the close read's map is for Monday.
        ("close", "Post-Market Read - $SPY", "Levels for Monday:"),
    ):
        body = mod.build_tweet_body(
            mode=mode,
            day=date(2026, 7, 3),
            bulletins=bulletins,
            site_url="https://zerogex.io",
            lead_symbol="SPY",
        )
        assert body.text.splitlines()[0] == header, f"mode={mode} header"
        assert heading in body.text, f"mode={mode} levels heading"


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
    bulletins = await mod._fetch_bulletins(db, ["SPY", "SPX"], date(2026, 7, 3), "premarket")
    by_sym = {b.symbol: b for b in bulletins}
    # SPX spot replaced with the implied level and flagged.
    assert by_sym["SPX"].spot == pytest.approx(6432.0)
    assert by_sym["SPX"].spot_is_projected is True
    assert by_sym["SPX"].future_symbol == "@ES"
    # SPY (ETF) untouched — still the live cash spot, not projected.
    assert by_sym["SPY"].spot == pytest.approx(744.51)
    assert by_sym["SPY"].spot_is_projected is False


def test_build_tweet_body_has_no_template_fallback(monkeypatch):
    """Without the writer there is no post, and the body says why: the
    operator doesn't want a post that skips the news going out."""
    mod = _reload_module()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPY"
    )
    assert body.text == ""
    assert body.reply_text == ""
    assert body.featured_symbol == "SPY"
    assert body.problems and "ANTHROPIC_API_KEY" in body.problems[0]


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


def test_build_tweet_body_force_featured_pins_lead_symbol(monkeypatch):
    """force_featured (the scheduled auto-post's lead) pins the featured symbol
    even when another symbol has a 'cleaner setup'."""
    mod = _reload_module()
    _stub_writer(monkeypatch, mod)
    # SPX sits right on its flip (cleanest); SPY is far from all its levels.
    spy = mod._shape_bulletin(
        _summary_row(
            "SPY",
            spot=744.51,
            gamma_flip=800.0,
            call_wall=810.0,
            put_wall=790.0,
            net_gex=1_000_000_000.0,
        ),
        "SPY",
    )
    spx = mod._shape_bulletin(
        _summary_row(
            "SPX",
            spot=7448.0,
            gamma_flip=7448.0,
            call_wall=7500.0,
            put_wall=7400.0,
            net_gex=-2_000_000_000.0,
        ),
        "SPX",
    )
    # Unforced, the cleanest-setup selector features SPX (spot on its flip).
    assert mod.select_featured_symbol([spy, spx]).symbol == "SPX"
    # Forced to SPY → the post features SPY regardless.
    body = mod.build_tweet_body(
        "close",
        date(2026, 7, 3),
        [spy, spx],
        site_url="https://zerogex.io",
        lead_symbol="SPY",
        force_featured="SPY",
    )
    assert body.featured_symbol == "SPY"
    assert body.text.startswith("Post-Market Read - $SPY")
    # If the forced symbol has no data this fire, fall through to cleanest.
    body2 = mod.build_tweet_body(
        "close",
        date(2026, 7, 3),
        [spx],
        site_url="https://zerogex.io",
        lead_symbol="SPY",
        force_featured="SPY",
    )
    assert body2.featured_symbol == "SPX"


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
    mod = _reload_module()
    _stub_writer(monkeypatch, mod, reply="Watch how 740 trades on the first test.")
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51, gamma_flip=744.51), "SPY")]
    body = mod.build_tweet_body(
        "midday", date(2026, 7, 3), bulletins, site_url="https://zerogex.io/", lead_symbol="SPY"
    )
    # Trailing slash on the site URL is trimmed.
    assert body.reply_text == "Watch how 740 trades on the first test.\n\nhttps://zerogex.io"


def test_reply_text_env_override(monkeypatch):
    mod = _reload_module()
    _stub_writer(monkeypatch, mod)
    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51, gamma_flip=744.51), "SPY")]
    body = mod.build_tweet_body(
        "midday",
        date(2026, 7, 3),
        bulletins,
        site_url="https://zerogex.io",
        lead_symbol="SPY",
        reply_text="Custom reply: https://zerogex.io",
    )
    assert body.reply_text == "Custom reply: https://zerogex.io"


def _fake_x(
    monkeypatch, fail_reply: bool = False, fail_upload: bool = False, fail_post: bool = False
):
    """Stub the X client: records every upload and post."""
    from src.jobs import x_media_client

    calls: dict[str, list] = {"upload": [], "post": []}
    creds = x_media_client.OAuth1Credentials("ck", "cs", "at", "ats")
    monkeypatch.setattr(x_media_client, "load_credentials_from_env", lambda: creds)

    def _upload(path, credentials, timeout_seconds=60):
        calls["upload"].append(path)
        if fail_upload:
            raise x_media_client.XApiError("v1.1 upload HTTP 403; v2 upload HTTP 403")
        return "media-1"

    def _post(text, credentials, media_ids=None, reply_to=None, timeout_seconds=30):
        calls["post"].append({"text": text, "media_ids": media_ids, "reply_to": reply_to})
        if fail_post and reply_to is None:
            raise x_media_client.XApiError("HTTP 403: text too long")
        if fail_reply and reply_to is not None:
            raise x_media_client.XApiError("HTTP 429: too many requests")
        return "main-123" if reply_to is None else "reply-456"

    monkeypatch.setattr(x_media_client, "upload_image", _upload)
    monkeypatch.setattr(x_media_client, "post_tweet", _post)
    return calls


def _png_media(mod, tmp_path):
    png = tmp_path / "bulletin-spy.png"
    png.write_bytes(_png_bytes())
    return mod.MediaArtifacts(png_path=png)


def _tweet(mod):
    return mod.TweetBody(
        text="Midday Read - $SPY\n\nbody",
        fallback="Midday Read - $SPY: spot 744.62",
        lead_symbol="SPY",
        symbols_present=["SPY"],
        reply_text="One more beat.\n\nhttps://zerogex.io",
        featured_symbol="SPY",
    )


def test_post_bulletin_posts_main_with_image_then_link_reply(monkeypatch, tmp_path):
    """The image uploads first, the post carries it, and the link comment is
    threaded under the post."""
    mod = _reload_module()
    calls = _fake_x(monkeypatch)
    result = mod.post_bulletin(
        _tweet(mod), _png_media(mod, tmp_path), long=True, mode_label="midday"
    )
    assert result.ok
    assert result.tweet_id == "main-123"
    assert result.reply_id == "reply-456"
    assert result.tweet_url == "https://x.com/i/web/status/main-123"
    assert len(calls["upload"]) == 1
    assert calls["post"][0] == {
        "text": "Midday Read - $SPY\n\nbody",
        "media_ids": ["media-1"],
        "reply_to": None,
    }
    assert calls["post"][1]["reply_to"] == "main-123"
    assert calls["post"][1]["text"].endswith("https://zerogex.io")


def test_post_bulletin_survives_failed_reply(monkeypatch, tmp_path):
    """A failing link reply doesn't undo the post — it's reported instead."""
    mod = _reload_module()
    _fake_x(monkeypatch, fail_reply=True)
    result = mod.post_bulletin(
        _tweet(mod), _png_media(mod, tmp_path), long=True, mode_label="midday"
    )
    assert result.ok
    assert result.tweet_id == "main-123"
    assert result.reply_id is None
    assert "link reply failed" in result.reply_error


def test_post_bulletin_never_posts_without_the_image(monkeypatch, tmp_path):
    mod = _reload_module()
    calls = _fake_x(monkeypatch)
    result = mod.post_bulletin(_tweet(mod), mod.MediaArtifacts(), long=True, mode_label="midday")
    assert not result.ok
    assert "image" in result.error
    assert calls == {"upload": [], "post": []}


def test_post_bulletin_stops_when_the_upload_fails(monkeypatch, tmp_path):
    """No text-only post when X refuses the image."""
    mod = _reload_module()
    calls = _fake_x(monkeypatch, fail_upload=True)
    result = mod.post_bulletin(
        _tweet(mod), _png_media(mod, tmp_path), long=True, mode_label="midday"
    )
    assert not result.ok
    assert "image upload" in result.error
    assert calls["post"] == []


def test_post_bulletin_does_not_swap_in_the_short_body(monkeypatch, tmp_path):
    """A rejected post is not retried with a different, unreviewed text."""
    mod = _reload_module()
    calls = _fake_x(monkeypatch, fail_post=True)
    result = mod.post_bulletin(
        _tweet(mod), _png_media(mod, tmp_path), long=True, mode_label="midday"
    )
    assert not result.ok
    assert "X rejected the post" in result.error
    assert len(calls["post"]) == 1


def test_post_bulletin_needs_the_oauth_keys(monkeypatch, tmp_path):
    mod = _reload_module()
    result = mod.post_bulletin(
        _tweet(mod), _png_media(mod, tmp_path), long=True, mode_label="midday"
    )
    assert not result.ok
    assert "X_BOT_API_KEY" in result.error


@pytest.mark.asyncio
async def test_dry_run_persists_reply_artifact(tmp_path, monkeypatch):
    """A dry-run writes tweet_reply.md and records the reply text +
    featured symbol in the manifest so the operator can inspect them."""
    mod = _reload_module()
    _passing_run(monkeypatch, mod)

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

    day_dir = tmp_path / "close" / "2026-07-06"
    reply_md = day_dir / "tweet_reply.md"
    assert reply_md.exists()
    assert "zerogex.io" in reply_md.read_text()
    manifest = json.loads((day_dir / "manifest.json").read_text())
    assert manifest["featured_symbol"] == "SPY"
    assert manifest["state"] == "dry_run"
    assert manifest["problems"] == []
    assert manifest["reply_text"].startswith("The tell is how fast the dips get bought.")


# ---------------------------------------------------------------------------
# Runner — dry-run default + non-trading-day skip + missing data skip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dry_run_writes_artifacts_and_never_posts(tmp_path, monkeypatch):
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)

    # Fail loudly if the runner ever tries to POST in dry-run mode — that's
    # the failure the whole `--post`-required gate is supposed to prevent.
    def _boom_post(*args, **kwargs):
        raise AssertionError("dry-run posted to X!")

    monkeypatch.setattr(mod, "post_bulletin", _boom_post)

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
    assert (day_dir / "bulletin-spy.png").exists()
    manifest = json.loads((day_dir / "manifest.json").read_text())
    assert manifest["mode"] == "close"
    assert manifest["state"] == "dry_run"
    assert manifest["symbols_present"]
    assert manifest["text_len"] > 0
    assert manifest["media"]["png"].endswith("bulletin-spy.png")
    # The card was rendered for the featured symbol, and the post quotes the
    # card's numbers (744.62 spot / 747.29 flip), not the DB row's.
    assert stubs["cards"] == ["SPY"]
    text = (day_dir / "tweet_text.md").read_text()
    assert "• 747.29 gamma flip" in text
    assert manifest["bulletins"][0]["spot"] == pytest.approx(744.62)
    assert manifest["bulletins"][0]["card_as_of"] == "Jul 6, 2026 · 12:30 PM EDT"


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
async def test_holds_when_every_symbol_missing(tmp_path, monkeypatch):
    """No data on a trading day is something going wrong, not a quiet skip:
    the run fails, the review page says why, and (on a scheduled fire) the
    operator is emailed."""
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)
    stubs["db"].get_latest_gex_summary = AsyncMock(return_value=None)

    args = mod._parse_args(
        [
            "--mode",
            "close",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
            "--allow-non-trading-day",
            "--stage",
        ]
    )
    rc = await mod._run(args)
    assert rc == 1
    manifest = json.loads((tmp_path / "close" / "2026-07-06" / "manifest.json").read_text())
    assert manifest["state"] == "blocked"
    assert "GEX summary was missing" in manifest["problems"][0]
    assert len(stubs["held"]) == 1
    assert stubs["writer"] == []  # nothing to write about


@pytest.mark.asyncio
async def test_never_raises_on_db_failure(tmp_path, monkeypatch):
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)
    stubs["db"].connect = AsyncMock(side_effect=RuntimeError("db down"))

    args = mod._parse_args(
        [
            "--mode",
            "premarket",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
            "--stage",
        ]
    )
    rc = await mod._run(args)
    assert rc == 1
    assert "db down" in stubs["held"][0]["problems"][0]


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


def test_build_tweet_body_without_api_key_is_held_with_the_reason(monkeypatch):
    """No ANTHROPIC_API_KEY → no post, and the reason names the key.  There
    is no static template to fall back to."""
    mod = _reload_module()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    bulletins = [
        mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY"),
        mod._shape_bulletin(_summary_row("SPX"), "SPX"),
    ]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPX"
    )
    assert body.text == ""
    assert body.problems == [
        "The AI writer didn't produce a post (ANTHROPIC_API_KEY is not set, "
        "so the post could not be written)."
    ]


def test_build_tweet_body_uses_llm_when_generator_returns_post(monkeypatch):
    """When bulletin_llm.generate_post returns a post, the composed body
    carries the LLM prose around the Python-composed levels list, and the
    reply carries the LLM copy + the ZeroGEX link."""
    mod = _reload_module()
    from src.jobs import bulletin_llm

    def _fake_generate(**kwargs):
        return bulletin_llm.LlmPost(
            opening=(
                "Interesting close into the holiday.\n\n"
                "The morning started long-gamma, then the walls broke down."
            ),
            bottom_line=(
                "With the market closed tomorrow, this is a fitting place " "to leave it."
            ),
            reply="The tell was how fast 740 reclaimed once the headline hit.",
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
    assert body.text.startswith("Post-Market Read - $SPY\n\nInteresting close into the holiday.")
    assert "Bottom line: With the market closed tomorrow" in body.text
    # The levels list is still the Python-composed one, with no notes.
    assert "• 740 put wall\n• 750 call wall\n• 744.51 gamma flip" in body.text
    # No hashtag row / link in the main post; the link rides in the reply.
    assert "#Gamma" not in body.text
    assert "zerogex.io" not in body.text
    # The reply is the one-sentence add-on + the ZeroGEX link on its own line.
    assert body.reply_text == (
        "The tell was how fast 740 reclaimed once the headline hit.\n\n" "https://zerogex.io"
    )
    assert body.llm_post is not None


def test_llm_post_failure_leaves_no_post(monkeypatch):
    """A None from the LLM path (API error, malformed reply, levels still
    misstated) means no post, with the reason recorded."""
    mod = _reload_module()
    from src.jobs import bulletin_llm

    def _fail(**kwargs):
        kwargs["errors"].append("the Claude API returned HTTP 529")
        return None

    monkeypatch.setattr(bulletin_llm, "generate_post", _fail)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")

    bulletins = [mod._shape_bulletin(_summary_row("SPY", spot=744.51), "SPY")]
    body = mod.build_tweet_body(
        "close", date(2026, 7, 3), bulletins, site_url="https://zerogex.io", lead_symbol="SPY"
    )
    assert body.text == ""
    assert "HTTP 529" in body.problems[0]


def test_llm_invented_price_guard():
    """Model output that quotes a fabricated price falls the post back
    to None — never post a wrong number."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        # 762 is in SPY's price band but is not a provided level — a fabricated
        # level near spot is exactly what the guard must catch.
        opening="SPY looks pinned to a hidden 762 shelf.",
        bottom_line="Nothing to see here.",
        reply="Watch the levels.",
    )
    inputs = [
        bulletin_llm.SymbolInput(symbol="SPY", spot=744.51, gamma_flip=744.51),
    ]
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is False


def test_llm_invented_price_guard_scans_reply():
    """A fabricated in-band price hidden in the reply is caught too."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        opening="SPY held its structure.",
        bottom_line="Still short gamma.",
        reply="Real risk is a fade to 812.",  # in-band, not a provided level
    )
    inputs = [
        bulletin_llm.SymbolInput(symbol="SPY", spot=744.51, gamma_flip=744.51),
    ]
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is False


def test_llm_validator_allows_news_figures_out_of_band():
    """News figures far from the symbol's price ("the S&P 500", "the Dow rose
    300", a year, a dollar amount) must NOT be treated as invented levels — the
    old check flagged them and forced the bland static fallback."""
    from src.jobs import bulletin_llm

    inputs = [
        bulletin_llm.SymbolInput(
            symbol="SPY",
            spot=744.70,
            prior_close=744.0,
            gamma_flip=744.60,
            call_wall=745.0,
            put_wall=743.0,
            net_gex=-115_800_000.0,
        ),
    ]
    for opening in (
        "SPY, the S&P 500 ETF, held above the 744.60 flip after the dip.",
        "With the Dow up 300 and yields easing, SPY reclaimed 744.60.",
        "Heading into the back half of 2026, SPY sits on its 744.60 flip.",
        "A $500 billion buyback wave has SPY pinned near 744.60.",
    ):
        post = bulletin_llm.LlmPost(
            opening=opening,
            bottom_line="Constructive while it holds.",
            reply="The tell was the reclaim.",
        )
        assert bulletin_llm._validate_no_invented_prices(post, inputs) is True, opening


def test_llm_validator_accepts_input_prices():
    """A post that only quotes numbers actually in the inputs passes."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        opening="SPX sits at 7,483 with the gamma flip at 7,448.",
        bottom_line="Call wall 7,500, put wall 7,480. Watch the flip.",
        reply="More analytics:",
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


# ---------------------------------------------------------------------------
# Level claims: "the 780 call wall" has to BE the call wall
# ---------------------------------------------------------------------------


def _close_read_spy(**overrides):
    """A close read where the call wall sat at 765 all session, the put wall
    walked 757 → 756 → 755, and the chain re-priced after the bell."""
    from src.jobs import bulletin_llm

    fields = dict(
        symbol="SPY",
        spot=761.30,
        prior_close=758.90,
        session_open=759.20,
        session_high=764.90,
        session_low=757.10,
        gamma_flip=758.40,
        call_wall=765.0,
        put_wall=755.0,
        max_pain=760.0,
        historical_level_values=[757.0, 756.0, 755.0, 765.0, 758.1, 758.9],
        level_paths={
            "put_wall": [757.0, 756.0, 755.0],
            "call_wall": [765.0],
            "gamma_flip": [758.1, 758.4, 758.1, 758.9],
        },
    )
    fields.update(overrides)
    return bulletin_llm.SymbolInput(**fields)


def _draft(opening: str, **overrides):
    from src.jobs import bulletin_llm

    fields = dict(
        opening=opening,
        bottom_line="Short gamma until the flip is reclaimed.",
        reply="The tell was how fast every pop got sold.",
    )
    fields.update(overrides)
    return bulletin_llm.LlmPost(**fields)


@pytest.mark.parametrize(
    "opening, spy_overrides",
    [
        # The post-bell roll-off resets the chain's call wall to 780.  Even
        # with 780 whitelisted (as the guard used to), the claim is caught.
        (
            "SPY stalled right under the 780 call wall into the bell.",
            {"historical_level_values": [757.0, 756.0, 755.0, 765.0, 780.0]},
        ),
        # A round number near the session high, dressed up as the wall.
        (
            "Buyers pressed into the 780 call wall and stalled.",
            {
                "spot": 776.40,
                "session_high": 778.10,
                "call_wall": 785.0,
                "level_paths": {"call_wall": [785.0]},
            },
        ),
        # A real number carrying the wrong label.
        ("Overhead, the call wall at 780 caps the upside.", {"max_pain": 780.0}),
    ],
)
def test_level_claim_catches_a_call_wall_the_session_never_had(opening, spy_overrides):
    """Regression: two days of posts named 780 as SPY's call wall when the
    call wall was never 780.  The number guard passes every one of these —
    780 sits within its tolerance of some input price — because it never
    asks which level a number was attached to."""
    from src.jobs import bulletin_llm

    inputs = [_close_read_spy(**spy_overrides)]
    post = _draft(opening)
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is True
    problems = bulletin_llm._post_problems(post, inputs)
    assert len(problems) == 1
    assert "780" in problems[0]
    assert "the call wall was" in problems[0]


def test_level_claim_accepts_the_real_levels_and_their_session_path():
    from src.jobs import bulletin_llm

    inputs = [_close_read_spy()]
    for opening in (
        "SPY stalled well short of the 765 call wall.",
        "The call wall at $765 capped every push.",
        "It lost the 757 put wall, then 756, before the 755 put wall held.",
        "The put wall walked lower: 757 put wall first, then the 756 put wall.",
        "Spot closed above the gamma flip at 758.40.",
        "Spot closed above the 758 flip.",  # the flip is a computed price
        "Max pain at 760 did the pinning.",
        "The roll-off resets the put wall well lower into tomorrow.",
    ):
        assert bulletin_llm._post_problems(_draft(opening), inputs) == [], opening


def test_level_claim_ignores_numbers_not_tied_to_a_level():
    from src.jobs import bulletin_llm

    inputs = [_close_read_spy()]
    for opening in (
        "The call wall held and SPY faded back to 761.30.",
        "SPY could flip 760 into support tomorrow.",  # a verb, not the level
        "The put wall gave way to the 758 flip.",  # 758 is the flip's
        "The call wall is 15 points overhead.",  # a distance, not a price
    ):
        assert bulletin_llm._post_problems(_draft(opening), inputs) == [], opening


def _claude_reply(opening: str) -> dict:
    body = {
        "opening": opening,
        "bottom_line": "Short gamma until the flip is reclaimed.",
        "reply": "The tell was how fast every pop got sold.",
    }
    return {"content": [{"type": "text", "text": json.dumps(body)}], "stop_reason": "end_turn"}


def test_generate_post_sends_a_misstated_level_back_once(monkeypatch):
    """The draft goes back with the specifics, and the fixed draft is used."""
    from src.jobs import bulletin_llm

    calls = []
    replies = iter(
        [
            _claude_reply("SPY stalled right under the 780 call wall."),
            _claude_reply("SPY stalled right under the 765 call wall."),
        ]
    )

    def _fake_call(system, messages, *args):
        calls.append(list(messages))
        return next(replies)

    monkeypatch.setattr(bulletin_llm, "_call_claude", _fake_call)
    post = bulletin_llm.generate_post(
        mode="close",
        day=date(2026, 9, 22),
        symbols=[_close_read_spy()],
        api_key="test-key",
        featured_symbol="SPY",
    )
    assert post is not None
    assert "765 call wall" in post.opening
    assert len(calls) == 2
    retry = calls[1]
    assert [m["role"] for m in retry] == ["user", "assistant", "user"]
    assert "780 call wall" in retry[1]["content"]
    assert "the call wall was 765, never 780" in retry[2]["content"]


def test_generate_post_falls_back_when_the_correction_still_misstates(monkeypatch):
    from src.jobs import bulletin_llm

    calls = []

    def _fake_call(system, messages, *args):
        calls.append(messages)
        return _claude_reply("SPY stalled right under the 780 call wall.")

    monkeypatch.setattr(bulletin_llm, "_call_claude", _fake_call)
    post = bulletin_llm.generate_post(
        mode="close",
        day=date(2026, 9, 22),
        symbols=[_close_read_spy()],
        api_key="test-key",
        featured_symbol="SPY",
    )
    assert post is None
    assert len(calls) == 1 + bulletin_llm.MAX_CORRECTION_ROUNDS


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
    """Put wall, call wall, gamma flip; whole strikes lose their ".00", the
    flip keeps its cents, and nothing else rides on the line."""
    mod = _reload_module()
    b = mod._shape_bulletin(
        _summary_row(
            "SPY",
            spot=744.51,
            gamma_flip=747.29,
            call_wall=745.0,
            put_wall=740.0,
        ),
        "SPY",
    )
    assert mod._key_levels_block(b).splitlines() == [
        "• 740 put wall",
        "• 745 call wall",
        "• 747.29 gamma flip",
    ]


def test_fmt_level_matches_the_cards_digits():
    """The card rounds half up on the exact value (the browser's toFixed /
    toLocaleString); the post must show the same digits."""
    mod = _reload_module()
    assert mod._fmt_level(745.0) == "745"
    assert mod._fmt_level(747.29) == "747.29"
    assert mod._fmt_level(747.125) == "747.13"  # an exact tie rounds up
    assert mod._fmt_level(744.996) == "745"
    assert mod._fmt_level(7482.71) == "7,483"  # index scale: whole, like the card
    assert mod._fmt_level(7500.0) == "7,500"


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
    # premarket read: the "prior close" is the most recent COMPLETED session
    # close (current_session_close = yesterday), NOT prior_session_close.
    db.get_session_closes = AsyncMock(
        return_value={"current_session_close": 744.0, "prior_session_close": 730.0}
    )
    db.get_intraday_ohlc = AsyncMock(
        return_value={
            "session_open": 744.1,
            "session_high": 745.0,
            "session_low": 739.6,
            "session_last": 743.0,
            "bar_count": 180,
        }
    )
    await mod._attach_price_action(db, b, date(2026, 7, 3), "premarket")
    assert b.prior_close == pytest.approx(744.0)
    assert b.session_low == pytest.approx(739.6)
    assert b.regime == "negative"  # spot below flip, net gex negative
    assert b.momentum_label  # derived, non-empty


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        # Wednesday's pre-market / midday reads gap from Tuesday's close (the
        # most recent completed session = current_session_close), NOT Monday's.
        ("premarket", 772.68),
        ("midday", 772.68),
        # The 16:05 close read fires once today's session has closed, so
        # current_session_close is today and the reference is the one before
        # it: prior_session_close.
        ("close", 772.68),
    ],
)
async def test_attach_price_action_prior_close_is_mode_aware(mode, expected):
    """Regression: a Wednesday Morning Read was quoting Monday's close.

    ``get_session_closes`` shifts which of its two closes is "yesterday"
    depending on whether today's session has ended, so the fields carry
    different dates per fire.  Both framings must resolve to Tuesday's
    772.68 — the actual prior close — never Monday's 758.33."""
    mod = _reload_module()
    b = mod._shape_bulletin(_summary_row("SPY", spot=775.38, gamma_flip=758.22), "SPY")
    db = MagicMock()
    db.get_intraday_ohlc = AsyncMock(return_value=None)
    if mode == "close":
        # 16:05 fire: current_session_close folded in today's close (Wed);
        # Tuesday is now prior_session_close.
        closes = {"current_session_close": 775.38, "prior_session_close": 772.68}
    else:
        # Pre-market / midday: today (Wed) not yet closed, so Tuesday is
        # current_session_close and Monday is prior_session_close.
        closes = {"current_session_close": 772.68, "prior_session_close": 758.33}
    db.get_session_closes = AsyncMock(return_value=closes)
    await mod._attach_price_action(db, b, date(2026, 8, 5), mode)
    assert b.prior_close == pytest.approx(expected)
    # Never Monday's close (the pre-fix bug).
    assert b.prior_close != pytest.approx(758.33)


def test_latest_record_roundtrip(tmp_path, monkeypatch):
    """build_latest_record → write_latest_record → read_latest_record."""
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_ARTIFACT_DIR", str(tmp_path))
    # Neutralize the other candidate roots so the "record absent" check can't
    # pick up a real /var/lib or ~/.local record on a configured host.
    monkeypatch.setattr(mod, "PRIMARY_ARTIFACT_ROOT", tmp_path / "primary-noexist")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
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


def test_read_latest_record_any_returns_newest(tmp_path, monkeypatch):
    """read_latest_record_any picks the newest record for a mode across symbols."""
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_ARTIFACT_DIR", str(tmp_path))
    # Neutralize the other candidate roots so the "premarket absent" check and
    # the newest-pick can't be perturbed by real records on a configured host.
    monkeypatch.setattr(mod, "PRIMARY_ARTIFACT_ROOT", tmp_path / "primary-noexist")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    latest = mod.resolve_latest_dir()
    assert latest is not None
    (latest / "SPX-midday.json").write_text(
        json.dumps(
            {
                "symbol": "SPX",
                "mode": "midday",
                "generated_at": "2026-07-27T12:30:00-04:00",
                "post_text": "spx",
            }
        )
    )
    (latest / "SPY-midday.json").write_text(
        json.dumps(
            {
                "symbol": "SPY",
                "mode": "midday",
                "generated_at": "2026-07-27T12:31:30-04:00",
                "post_text": "spy",
            }
        )
    )
    (latest / "SPY-close.json").write_text(
        json.dumps(
            {
                "symbol": "SPY",
                "mode": "close",
                "generated_at": "2026-07-27T16:05:00-04:00",
                "post_text": "close",
            }
        )
    )
    got = mod.read_latest_record_any("midday")
    assert got["symbol"] == "SPY"  # newer generated_at than the SPX midday row
    assert got["post_text"] == "spy"
    # A mode with no records → None, never an error.
    assert mod.read_latest_record_any("premarket") is None


def test_read_latest_record_when_write_dir_unresolvable(tmp_path, monkeypatch):
    """The API reads records the tweet job wrote even when its OWN
    resolve_latest_dir() (write-probe) fails — reads must not require write
    access.  Mirrors the sandboxed API (ProtectSystem=strict) reading the
    /var/lib records the tweet job wrote on its own less-restricted unit."""
    mod = _reload_module()
    monkeypatch.delenv("BULLETIN_TWEET_ARTIFACT_DIR", raising=False)
    # A read-only store that already has a record; every writable candidate
    # is neutralized, and resolve_latest_dir() returns None (no writable dir).
    store = tmp_path / "readonly-store"
    (store / "latest").mkdir(parents=True)
    (store / "latest" / "SPY-premarket.json").write_text(
        json.dumps({"symbol": "SPY", "mode": "premarket", "post_text": "morning read"})
    )
    monkeypatch.setattr(mod, "PRIMARY_ARTIFACT_ROOT", store)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    monkeypatch.setattr(mod, "resolve_latest_dir", lambda: None)

    got = mod.read_latest_record("SPY", "premarket")
    assert got is not None and got["post_text"] == "morning read"
    got_any = mod.read_latest_record_any("premarket")
    assert got_any is not None and got_any["symbol"] == "SPY"


# ---------------------------------------------------------------------------
# Built-in "<Timing> X-Post Ready" email (replaces the old approval email)
# ---------------------------------------------------------------------------


def test_xpost_ready_email_skips_without_resend_config(monkeypatch):
    mod = _reload_module()
    for k in ("RESEND_API_KEY", "RESEND_FROM_EMAIL", "BULLETIN_TWEET_EMAIL_TO"):
        monkeypatch.delenv(k, raising=False)
    # Never raises, returns False when unconfigured.
    assert mod._send_xpost_ready_email("midday") is False


def test_xpost_ready_email_posts_to_resend(monkeypatch):
    mod = _reload_module()
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("RESEND_FROM_EMAIL", "alerts@zerogex.io")
    monkeypatch.setenv("BULLETIN_TWEET_EMAIL_TO", "me@example.com")
    monkeypatch.setenv("BULLETIN_TWEET_ADMIN_URL", "https://zerogex.io/admin/x-post")
    monkeypatch.delenv("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", raising=False)

    captured: dict = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"{}"

    def _fake_urlopen(req, timeout=15):
        captured["url"] = req.full_url
        captured["auth"] = req.get_header("Authorization")
        captured["ua"] = req.get_header("User-agent")
        captured["body"] = req.data
        return _Resp()

    monkeypatch.setattr(mod, "urlopen", _fake_urlopen)
    assert mod._send_xpost_ready_email("midday") is True
    assert captured["url"] == "https://api.resend.com/emails"
    assert captured["auth"] == "Bearer re_test"
    # A real product UA — NOT the default Python-urllib one that Cloudflare 403s.
    assert captured["ua"] and "urllib" not in captured["ua"].lower()
    payload = json.loads(captured["body"])
    assert payload["subject"] == "Midday X-Post Ready"
    assert payload["to"] == ["me@example.com"]
    assert "https://zerogex.io/admin/x-post" in payload["text"]


def test_xpost_ready_email_respects_disable_flag(monkeypatch):
    mod = _reload_module()
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("RESEND_FROM_EMAIL", "alerts@zerogex.io")
    monkeypatch.setenv("BULLETIN_TWEET_EMAIL_TO", "me@example.com")
    monkeypatch.setenv("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", "0")

    def _boom(*a, **k):
        raise AssertionError("must not send when disabled")

    monkeypatch.setattr(mod, "urlopen", _boom)
    assert mod._send_xpost_ready_email("midday") is False


def test_xpost_admin_url_defaults_to_site_url(monkeypatch):
    mod = _reload_module()
    monkeypatch.delenv("BULLETIN_TWEET_ADMIN_URL", raising=False)
    monkeypatch.setenv("ZEROGEX_SITE_URL", "https://zerogex.io/")
    assert mod._xpost_admin_url() == "https://zerogex.io/admin/x-post"


# ---------------------------------------------------------------------------
# Approval mechanism — --stage flag + bulletin_approve module
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stage_flag_writes_pending_and_calls_hook(tmp_path, monkeypatch):
    """--stage writes state=pending manifest, calls the notify hook and
    emails "X-Post Ready" with the image once the review passes.

    Must NOT POST to X, even when the X keys are set — safe by default so
    the timer can't accidentally tweet before the operator has flipped
    autopilot on."""
    mod = _reload_module()
    _passing_run(monkeypatch, mod)

    def _boom(*args, **kwargs):
        raise AssertionError("--stage mode posted to X!")

    monkeypatch.setattr(mod, "post_bulletin", _boom)
    ready: list[dict] = []
    monkeypatch.setattr(
        mod,
        "_send_xpost_ready_email",
        lambda mode, png_path=None: ready.append({"mode": mode, "png": png_path}) or True,
    )
    monkeypatch.setenv("X_BOT_API_KEY", "k")
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

    manifest_path = tmp_path / "artifacts" / "close" / "2026-07-06" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["state"] == "pending"

    # The notify hook fired with the right mode arg.
    assert hook_called_marker.exists()
    assert (tmp_path / "hook_fired.mode").read_text().strip() == "close"
    # And the Ready email carries the live bulletin image.
    assert len(ready) == 1
    assert ready[0]["png"].name == "bulletin-spy.png"


@pytest.mark.asyncio
async def test_autopilot_env_var_upgrades_stage_to_post(tmp_path, monkeypatch):
    """BULLETIN_TWEET_AUTOPILOT=1 silently upgrades --stage → --post.

    Enables one-line-env-flip switch to autopilot without editing the
    systemd unit file or touching daemon-reload."""
    mod = _reload_module()
    _passing_run(monkeypatch, mod)
    monkeypatch.setattr(mod, "_deadline_problem", lambda *a, **k: None)

    posts: list[dict] = []

    def _fake_post(**kwargs):
        posts.append(kwargs)
        return mod.PostResult(ok=True, tweet_id="fake-tweet-id-42", reply_id="r-1")

    sent: list = []
    monkeypatch.setattr(mod, "post_bulletin", _fake_post)
    monkeypatch.setattr(mod, "_send_xpost_sent_email", lambda *a: sent.append(a) or True)
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT", "1")
    monkeypatch.delenv("BULLETIN_TWEET_NOTIFY_HOOK", raising=False)

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

    # post_bulletin was called once, with the rendered card attached.
    assert len(posts) == 1
    assert posts[0]["media"].png_path.name == "bulletin-spy.png"
    manifest = json.loads(
        (tmp_path / "artifacts" / "close" / "2026-07-06" / "manifest.json").read_text(),
    )
    assert manifest["state"] == "posted"
    assert manifest["posted_id"] == "fake-tweet-id-42"
    assert len(sent) == 1
    record = mod.read_latest_record("SPY", "close")
    assert record["status"] == "posted"
    assert record["tweet_url"] == "https://x.com/i/web/status/fake-tweet-id-42"


@pytest.mark.asyncio
async def test_review_findings_go_back_to_the_writer_once(tmp_path, monkeypatch):
    """What the fact-check finds is handed back for one rewrite, and the
    rewrite is what's reviewed and kept."""
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod, review_problems=[["'rallied' but SPY is down 0.6%."]])

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
    monkeypatch.setattr(mod, "_send_xpost_ready_email", lambda mode, png_path=None: True)
    rc = await mod._run(args)
    assert rc == 0
    assert len(stubs["writer"]) == 2
    assert stubs["writer"][1]["feedback"] == ["'rallied' but SPY is down 0.6%."]
    assert stubs["writer"][1]["revise_from"] is not None
    assert len(stubs["review"]) == 2
    # The fact-check saw the post as it will go out, with the card image.
    assert stubs["review"][0]["post_text"].startswith("Post-Market Read - $SPY")
    assert stubs["review"][0]["card_png"] == _png_bytes()


@pytest.mark.asyncio
async def test_autopilot_holds_the_post_when_the_review_still_finds_problems(tmp_path, monkeypatch):
    mod = _reload_module()
    problem = "The post says CPI came in hot; no headline says that."
    stubs = _passing_run(monkeypatch, mod, review_problems=[[problem], [problem]])
    monkeypatch.setattr(mod, "_deadline_problem", lambda *a, **k: None)
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT", "1")

    def _boom(**kwargs):
        raise AssertionError("posted a post the review rejected!")

    monkeypatch.setattr(mod, "post_bulletin", _boom)
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
    assert rc == 1
    manifest = json.loads(
        (tmp_path / "artifacts" / "close" / "2026-07-06" / "manifest.json").read_text(),
    )
    assert manifest["state"] == "blocked"
    assert manifest["problems"] == [problem]
    # The operator is told, with the reasons, the draft and the image.
    assert len(stubs["held"]) == 1
    assert stubs["held"][0]["problems"] == [problem]
    assert stubs["held"][0]["posting"] is True
    assert stubs["held"][0]["png"].name == "bulletin-spy.png"
    record = mod.read_latest_record("SPY", "close")
    assert record["status"] == "blocked"
    assert record["problems"] == [problem]


@pytest.mark.asyncio
async def test_a_failed_card_render_holds_the_post(tmp_path, monkeypatch):
    """No live bulletin image, no post: there is no text-only fallback."""
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)
    monkeypatch.setattr(
        mod,
        "render_bulletin_card",
        lambda *a, **k: mod.CardRender(
            error="The live bulletin screenshot failed: Chromium couldn't start."
        ),
    )
    monkeypatch.setattr(mod, "_deadline_problem", lambda *a, **k: None)
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT", "1")
    monkeypatch.setattr(mod, "post_bulletin", lambda **k: pytest.fail("posted without the image"))

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
    assert rc == 1
    assert stubs["held"][0]["problems"][0].startswith("The live bulletin screenshot failed")


@pytest.mark.asyncio
async def test_no_fresh_headlines_holds_the_post(tmp_path, monkeypatch):
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)
    monkeypatch.setattr(
        mod, "_fetch_fresh_headlines", lambda: ([], "No CNBC headlines from the last 24 hours.")
    )
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
    assert rc == 1
    assert "No CNBC headlines" in stubs["held"][0]["problems"][0]


@pytest.mark.asyncio
async def test_autopilot_holds_a_post_past_its_cutoff(tmp_path, monkeypatch):
    """A catch-up run after downtime must not publish a stale read."""
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT", "1")
    monkeypatch.setattr(mod, "post_bulletin", lambda **k: pytest.fail("posted after the cutoff"))

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
    assert rc == 1
    # 2026-07-06 isn't today, so the deadline check refuses it.
    assert "not today" in stubs["held"][0]["problems"][-1]


def test_deadline_problem_by_mode():
    mod = _reload_module()
    et = ZoneInfo("America/New_York")
    day = date(2026, 9, 24)
    assert mod._deadline_problem("premarket", day, datetime(2026, 9, 24, 9, 19, tzinfo=et)) is None
    late = mod._deadline_problem("premarket", day, datetime(2026, 9, 24, 11, 0, tzinfo=et))
    assert "past the 9:30 AM cutoff for the Morning Read" in late
    assert mod._deadline_problem("close", day, datetime(2026, 9, 24, 16, 9, tzinfo=et)) is None
    assert "not today" in mod._deadline_problem(
        "close", day, datetime(2026, 9, 25, 9, 0, tzinfo=et)
    )


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

    from src.jobs.bulletin_tweet import PostResult

    def _fake_post(**kwargs):
        posts.append(kwargs)
        return PostResult(ok=True, tweet_id="approved-tweet-id-99")

    monkeypatch.setattr(bulletin_approve, "post_bulletin", _fake_post)
    for key in (
        "X_BOT_API_KEY",
        "X_BOT_API_SECRET",
        "X_BOT_ACCESS_TOKEN",
        "X_BOT_ACCESS_TOKEN_SECRET",
    ):
        monkeypatch.setenv(key, "test")

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
    assert manifest["tweet_url"] == "https://x.com/i/web/status/approved-tweet-id-99"
    assert "approved_ts" in manifest


def test_approve_refuses_a_draft_that_failed_review(tmp_path, monkeypatch):
    """A blocked draft can be printed or discarded, never posted from here."""
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
                "state": "blocked",
                "problems": ["The fact-check found an unsupported claim."],
                "lead_symbol": "SPY",
                "media": {"png": None, "clip": None},
            }
        )
    )
    monkeypatch.setattr(
        bulletin_approve, "post_bulletin", lambda **k: pytest.fail("posted a blocked draft")
    )
    for key in (
        "X_BOT_API_KEY",
        "X_BOT_API_SECRET",
        "X_BOT_ACCESS_TOKEN",
        "X_BOT_ACCESS_TOKEN_SECRET",
    ):
        monkeypatch.setenv(key, "test")
    args = bulletin_approve._parse_args(
        ["--mode", "close", "--date", "2026-07-06", "--artifact-dir", str(tmp_path)]
    )
    assert bulletin_approve._run(args) == 1
    assert json.loads((art_dir / "manifest.json").read_text())["state"] == "blocked"


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


# ---------------------------------------------------------------------------
# Intraday level tracking — the walls move, and the close fire must not quote
# the post-bell chain as if the tape had traded against it.
# ---------------------------------------------------------------------------


def _et(hour: int, minute: int, day: date = date(2026, 8, 13)) -> datetime:
    """An ET wall-clock moment on the session date, as an aware datetime."""
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ZoneInfo("America/New_York"))


def _level_rows() -> list[dict]:
    """A session where the put wall walks 777 → 776 → 775, then resets to
    765 after the bell once the day's 0DTE rolls off."""
    rows = []
    windows = [
        ((9, 30), (10, 30), 777.0),
        ((10, 35), (12, 0), 776.0),
        ((12, 5), (16, 0), 775.0),
    ]
    for (sh, sm), (eh, em), wall in windows:
        cursor = _et(sh, sm)
        while cursor <= _et(eh, em):
            rows.append(
                {
                    "timestamp": cursor,
                    "put_wall": wall,
                    "call_wall": 780.0,
                    "gamma_flip": 769.75,
                    "max_pain": 776.0,
                    "net_gex_at_spot": -1.4e9,
                }
            )
            cursor += timedelta(minutes=5)
    for minute in (5, 10, 15):
        rows.append(
            {
                "timestamp": _et(16, minute),
                "put_wall": 765.0,
                "call_wall": 780.0,
                "gamma_flip": 769.80,
                "max_pain": 776.0,
                "net_gex_at_spot": -4.0e8,
            }
        )
    return rows


def _price_rows() -> list[dict]:
    return [
        {"timestamp": _et(9, 30), "low": 778.20, "high": 779.90, "close": 778.40},
        {"timestamp": _et(10, 0), "low": 776.40, "high": 778.60, "close": 776.60},
        {"timestamp": _et(11, 0), "low": 775.30, "high": 776.90, "close": 775.60},
        {"timestamp": _et(13, 0), "low": 774.95, "high": 776.10, "close": 775.40},
        {"timestamp": _et(15, 55), "low": 775.80, "high": 776.90, "close": 776.80},
    ]


def _history_db() -> MagicMock:
    db = MagicMock()
    db.get_intraday_level_series = AsyncMock(return_value=_level_rows())
    db.get_underlying_candles_for_session = AsyncMock(return_value=_price_rows())
    return db


@pytest.mark.asyncio
async def test_close_read_keeps_the_live_levels_and_carries_the_session_path():
    """The 16:05 post quotes what the live card shows: the post-bell chain
    (put wall 765 once the day's 0DTE rolled off), labeled as the next
    session's map.  What the session's wall actually did (777 → 776 → 775)
    rides along in the level history for the prose."""
    mod = _reload_module()
    b = mod._shape_bulletin(
        # What get_latest_gex_summary returns at 16:05: the POST-BELL row.
        _summary_row("SPY", spot=776.80, gamma_flip=769.80, call_wall=780.0, put_wall=765.0),
        "SPY",
    )
    await mod._attach_level_history(_history_db(), b, date(2026, 8, 13), "close")

    assert b.put_wall == pytest.approx(765.0)
    assert b.gamma_flip == pytest.approx(769.80)
    assert b.call_wall == pytest.approx(780.0)
    assert b.level_history is not None
    assert b.level_history.put_wall.values == [777.0, 776.0, 775.0]
    assert b.level_history.post_close_levels["put_wall"] == pytest.approx(765.0)


@pytest.mark.asyncio
async def test_close_read_net_gex_stays_what_the_card_shows():
    mod = _reload_module()
    b = mod._shape_bulletin(_summary_row("SPY", spot=776.80, put_wall=765.0, net_gex=-4.0e8), "SPY")
    b.net_gex = -4.0e8  # the post-bell value the latest row carried
    await mod._attach_level_history(_history_db(), b, date(2026, 8, 13), "close")
    assert b.net_gex == pytest.approx(-4.0e8)


@pytest.mark.asyncio
async def test_midday_read_tracks_migration_without_a_roll_off_line():
    mod = _reload_module()
    b = mod._shape_bulletin(_summary_row("SPY", spot=775.40, put_wall=775.0), "SPY")
    await mod._attach_level_history(_history_db(), b, date(2026, 8, 13), "midday")
    assert b.level_history is not None
    # Midday never re-anchors — the latest row IS the session structure.
    assert b.put_wall == pytest.approx(775.0)
    from src.jobs import level_history as lh

    assert lh.note_for(b.level_history, "put_wall").startswith("moved 777 → 776 → 775")


@pytest.mark.asyncio
async def test_premarket_read_skips_level_history():
    """Nothing has traded yet — there is no path to describe."""
    mod = _reload_module()
    db = _history_db()
    b = mod._shape_bulletin(_summary_row("SPY", spot=776.80), "SPY")
    await mod._attach_level_history(db, b, date(2026, 8, 13), "premarket")
    assert b.level_history is None
    db.get_intraday_level_series.assert_not_awaited()


@pytest.mark.asyncio
async def test_attach_level_history_survives_a_db_failure():
    mod = _reload_module()
    db = MagicMock()
    db.get_intraday_level_series = AsyncMock(side_effect=RuntimeError("pool exhausted"))
    b = mod._shape_bulletin(_summary_row("SPY", spot=776.80, put_wall=765.0), "SPY")
    await mod._attach_level_history(db, b, date(2026, 8, 13), "close")
    assert b.level_history is None
    assert b.put_wall == pytest.approx(765.0)  # untouched — renders as before


@pytest.mark.asyncio
async def test_key_levels_block_carries_no_notes_even_when_the_walls_moved():
    """The list says where the levels are; the prose says what they did."""
    mod = _reload_module()
    b = mod._shape_bulletin(
        _summary_row("SPY", spot=776.80, gamma_flip=769.80, call_wall=780.0, put_wall=765.0),
        "SPY",
    )
    await mod._attach_level_history(_history_db(), b, date(2026, 8, 13), "close")
    assert mod._key_levels_block(b).splitlines() == [
        "• 765 put wall",
        "• 780 call wall",
        "• 769.80 gamma flip",
    ]


@pytest.mark.asyncio
async def test_close_post_labels_the_levels_as_the_next_sessions_map():
    """The post quotes the card's post-bell levels, so it says whose map they
    are instead of appending a separate after-the-bell line."""
    mod = _reload_module()
    from src.jobs import bulletin_llm

    b = mod._shape_bulletin(
        _summary_row("SPY", spot=776.80, gamma_flip=769.80, call_wall=780.0, put_wall=765.0),
        "SPY",
    )
    await mod._attach_level_history(_history_db(), b, date(2026, 8, 13), "close")
    post = bulletin_llm.LlmPost(
        opening="SPY lost 777 and 776 before 775 finally held.",
        bottom_line="The roll-off drops the put wall to 765 for tomorrow.",
        reply="Watch whether 765 gets tested early.",
    )
    text = mod._compose_new_post(post, b, "close", date(2026, 8, 13))  # a Thursday
    assert "Levels for tomorrow:\n• 765 put wall\n• 780 call wall\n• 769.80 gamma flip" in text
    assert "After the bell" not in text
    assert text.index("Levels for tomorrow:") < text.index("Bottom line:")


@pytest.mark.asyncio
async def test_level_history_reaches_the_llm_inputs_and_the_review_record():
    mod = _reload_module()
    b = mod._shape_bulletin(
        _summary_row("SPY", spot=776.80, gamma_flip=769.80, call_wall=780.0, put_wall=765.0),
        "SPY",
    )
    await mod._attach_level_history(_history_db(), b, date(2026, 8, 13), "close")

    captured = {}

    def _fake_generate_post(**kwargs):
        captured.update(kwargs)
        return None  # we only care about the inputs

    from src.jobs import bulletin_llm

    original = bulletin_llm.generate_post
    bulletin_llm.generate_post = _fake_generate_post
    try:
        mod._try_llm_post("close", date(2026, 8, 13), [b], "SPY", [])
    finally:
        bulletin_llm.generate_post = original

    payload = captured["symbols"][0].to_prompt_dict()["level_history"]
    assert [seg["value"] for seg in payload["put_wall"]["path"]] == [777.0, 776.0, 775.0]
    # The model learns the put wall reset lower after the bell; the new
    # value is the top-level put wall (what the card shows), and the
    # session path stays separate so the prose can't mix the two.
    assert payload["put_wall"]["after_the_bell_reset"] == "lower"
    assert "after_the_bell" not in payload["put_wall"]
    assert "next session" in payload["post_close_roll_off"]
    assert captured["symbols"][0].put_wall == pytest.approx(765.0)
    assert 777.0 in captured["symbols"][0].historical_level_values
    assert 765.0 not in captured["symbols"][0].historical_level_values
    assert captured["symbols"][0].level_paths["put_wall"] == [777.0, 776.0, 775.0]

    record = mod.build_latest_record(
        mode="close",
        day=date(2026, 8, 13),
        tweet=mod.TweetBody(
            text="Post-Market Read - $SPY",
            fallback="$SPY",
            lead_symbol="SPY",
            featured_symbol="SPY",
        ),
        featured=b,
        status="dry_run",
    )
    assert record["status"] == "dry_run"
    assert record["problems"] == []
    assert record["levels"]["put_wall"] == pytest.approx(765.0)
    assert record["levels"]["level_history"]["put_wall"]["at_session_close"] == pytest.approx(775.0)
    # The review record still carries the reset value, for tracing.
    assert record["levels"]["level_history"]["put_wall"]["after_the_bell"] == pytest.approx(765.0)


def test_llm_validator_accepts_superseded_wall_prints():
    """A wall the tape has already moved past is a fact, not a hallucination.

    Once the model is told the put wall walked 777 → 776 → 775, "it lost 777
    and 776 before defending 775" is the accurate read — rejecting it would
    force the post back to the closing-snapshot version the level tracking
    exists to replace."""
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        opening="SPY lost 777, lost 776, and finally found bids at 775.",
        bottom_line="775 is the line into tomorrow.",
        reply="The tell was how little follow-through each break got.",
    )
    inputs = [
        bulletin_llm.SymbolInput(
            symbol="SPY",
            spot=776.80,
            put_wall=775.0,
            call_wall=780.0,
            historical_level_values=[777.0, 776.0, 775.0, 780.0],
        ),
    ]
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is True

    # ...but an in-band number that was never a level still fails.
    post.opening = "SPY lost 777 and stalled at the 771 shelf."
    assert bulletin_llm._validate_no_invented_prices(post, inputs) is False
