"""Tests for the bulletin X-post's review gate and strict posting.

Nothing is posted unless the live bulletin image rendered, the post quotes
that card's numbers, the latest CNBC headlines came back, the writer and an
independent fact-check both ran, and the finished text passes the
deterministic checks.  Anything else holds the post and tells the operator.
"""

from __future__ import annotations

import base64
import json
import struct
from datetime import date
from pathlib import Path
from urllib.error import HTTPError

import pytest


def _png_bytes(width: int = 1280, height: int = 1932) -> bytes:
    return (
        b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 13) + b"IHDR" + struct.pack(">II", width, height)
    )


def _featured(bt, **overrides):
    b = bt.SymbolBulletin(
        symbol="SPY",
        spot=744.62,
        gamma_flip=747.29,
        call_wall=745.0,
        put_wall=740.0,
        max_pain=744.0,
        net_gex=-1.25e9,
    )
    for k, v in overrides.items():
        setattr(b, k, v)
    return b


def _tweet(bt, text: str, reply: str = "One more beat.\n\nhttps://zerogex.io"):
    return bt.TweetBody(
        text=text, fallback="", lead_symbol="SPY", reply_text=reply, featured_symbol="SPY"
    )


GOOD_POST = (
    "Midday Read - $SPY\n\n"
    "SPY slid into the 740 put wall after the jobs report and buyers showed up.\n\n"
    "Key levels:\n• 740 put wall\n• 745 call wall\n• 747.29 gamma flip\n\n"
    "Bottom line: chop between the walls until the flip gives way."
)


# ---------------------------------------------------------------------------
# Deterministic text checks
# ---------------------------------------------------------------------------


def test_a_clean_post_has_no_text_problems():
    from src.jobs import bulletin_tweet as bt

    assert bt._text_problems(_tweet(bt, GOOD_POST), _featured(bt), "midday") == []


@pytest.mark.parametrize(
    "mutate, expected",
    [
        (lambda t: t.replace("Midday Read - $SPY", "Midday Read — $SPY"), "should be"),
        (lambda t: t.replace("after the jobs", "— after the jobs"), "em dash"),
        (lambda t: t.replace("and buyers", "and buyers favour it; buyers"), '"favour"'),
        (lambda t: t + " #SPY", "hashtag"),
        (lambda t: t + " \U0001f680", "emoji"),
        (lambda t: t + " https://zerogex.io", "link"),
        (lambda t: t.replace("• 745 call wall\n", ""), "missing the call wall"),
        (lambda t: t.replace("chop between", "**chop** between"), "markdown"),
    ],
)
def test_text_problems_catch_what_gives_a_post_away(mutate, expected):
    from src.jobs import bulletin_tweet as bt

    problems = bt._text_problems(_tweet(bt, mutate(GOOD_POST)), _featured(bt), "midday")
    assert any(expected in p for p in problems), problems


def test_text_problems_check_the_reply_too():
    from src.jobs import bulletin_tweet as bt

    assert "no link" in " ".join(
        bt._text_problems(_tweet(bt, GOOD_POST, reply="No link here."), _featured(bt), "midday")
    )
    long_reply = "x" * 270 + "\n\nhttps://zerogex.io"  # 270 + 2 + 23 as X counts it
    assert "over the 280 limit" in " ".join(
        bt._text_problems(_tweet(bt, GOOD_POST, reply=long_reply), _featured(bt), "midday")
    )
    ok_reply = "x" * 250 + "\n\nhttps://zerogex.io/some/very/long/path/that/x/shortens"
    assert bt._text_problems(_tweet(bt, GOOD_POST, reply=ok_reply), _featured(bt), "midday") == []


def test_british_spelling_check_skips_proper_nouns():
    from src.jobs import bulletin_tweet as bt

    text = GOOD_POST.replace("buyers showed up", "the Ministry of Defence spoke")
    assert bt._text_problems(_tweet(bt, text), _featured(bt), "midday") == []


def test_data_problems_name_every_missing_piece():
    from src.jobs import bulletin_tweet as bt

    card = bt.CardRender(error="The live bulletin screenshot failed: Chromium couldn't start.")
    problems = bt._data_problems(_featured(bt, gamma_flip=None), card, "No CNBC headlines.")
    assert problems == [
        "The live bulletin screenshot failed: Chromium couldn't start.",
        "The live bulletin shows no gamma flip for SPY, so the key levels would be incomplete.",
        "No CNBC headlines.",
    ]
    assert bt._data_problems(None, None, None) == ["No symbol had any data to post about."]


# ---------------------------------------------------------------------------
# The card: screenshot + its own numbers
# ---------------------------------------------------------------------------


def test_apply_card_levels_makes_the_post_quote_the_card():
    from src.jobs import bulletin_tweet as bt

    b = _featured(bt, spot=744.51, gamma_flip=747.31, regime="positive", prior_close=749.10)
    bt._apply_card_levels(
        b,
        {
            "symbol": "SPY",
            "spot": 744.62,
            "gamma_flip": 747.29,
            "call_wall": 745,
            "put_wall": 740,
            "max_pain": None,
            "net_gex": -1.25e9,
            "regime": "negative",
            "as_of": "Sep 24, 2026 · 12:31 PM EDT",
        },
    )
    assert b.spot == pytest.approx(744.62)
    assert b.gamma_flip == pytest.approx(747.29)
    assert b.max_pain is None  # not on the card → not in the post
    assert b.regime == "negative"
    assert b.card_as_of == "Sep 24, 2026 · 12:31 PM EDT"
    assert b.momentum_label.startswith("down on the day")


def _helper(tmp_path: Path) -> Path:
    helper = tmp_path / "render-bulletin-png.mjs"
    helper.write_text("// stub")
    return helper


def test_render_bulletin_card_reads_back_the_cards_levels(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    seen: dict = {}

    def _fake_helper(helper, cmd_args, timeout_seconds, label):
        seen["args"] = cmd_args
        out = Path(cmd_args[cmd_args.index("--out") + 1])
        meta = Path(cmd_args[cmd_args.index("--meta-out") + 1])
        out.write_bytes(_png_bytes())
        meta.write_text(json.dumps({"ready": True, "levels": {"symbol": "SPY", "call_wall": 745}}))
        return 0, ""

    monkeypatch.setattr(bt, "_run_frontend_helper", _fake_helper)
    monkeypatch.setenv("BULLETIN_SNAPSHOT_TOKEN", "tok")
    card = bt.render_bulletin_card(
        "spy",
        "midday",
        "http://127.0.0.1:3000",
        tmp_path / "bulletin-spy.png",
        helper_path=str(_helper(tmp_path)),
    )
    assert card.ok
    assert card.levels["call_wall"] == 745
    assert "--date" not in seen["args"]  # the card keeps its own "as of" label
    assert seen["args"][seen["args"].index("--token") + 1] == "tok"
    assert seen["args"][seen["args"].index("--site-url") + 1] == "http://127.0.0.1:3000"


@pytest.mark.parametrize(
    "rc, expected",
    [
        (2, "playwright-core isn't installed"),
        (3, "BULLETIN_SNAPSHOT_TOKEN"),
        (5, "never finished loading"),
        (6, "make bulletin-tweet-bootstrap"),
        (7, "pm2 status"),
        (9, "exit 9"),
    ],
)
def test_render_bulletin_card_explains_each_failure(tmp_path, monkeypatch, rc, expected):
    from src.jobs import bulletin_tweet as bt

    monkeypatch.setattr(bt, "_run_frontend_helper", lambda *a, **k: (rc, "boom"))
    card = bt.render_bulletin_card(
        "SPY",
        "midday",
        "https://zerogex.io",
        tmp_path / "b.png",
        helper_path=str(_helper(tmp_path)),
    )
    assert not card.ok
    assert expected in card.error


def test_render_bulletin_card_rejects_a_wrong_or_empty_picture(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    def _tiny(helper, cmd_args, timeout_seconds, label):
        Path(cmd_args[cmd_args.index("--out") + 1]).write_bytes(_png_bytes(300, 200))
        return 0, ""

    monkeypatch.setattr(bt, "_run_frontend_helper", _tiny)
    card = bt.render_bulletin_card(
        "SPY",
        "midday",
        "https://zerogex.io",
        tmp_path / "b.png",
        helper_path=str(_helper(tmp_path)),
    )
    assert not card.ok and "isn't a usable image" in card.error

    def _other_symbol(helper, cmd_args, timeout_seconds, label):
        Path(cmd_args[cmd_args.index("--out") + 1]).write_bytes(_png_bytes())
        Path(cmd_args[cmd_args.index("--meta-out") + 1]).write_text(
            json.dumps({"ready": True, "levels": {"symbol": "QQQ"}})
        )
        return 0, ""

    monkeypatch.setattr(bt, "_run_frontend_helper", _other_symbol)
    card = bt.render_bulletin_card(
        "SPY",
        "midday",
        "https://zerogex.io",
        tmp_path / "b.png",
        helper_path=str(_helper(tmp_path)),
    )
    assert not card.ok and "QQQ" in card.error


def test_render_bulletin_card_never_reuses_an_old_picture(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    out = tmp_path / "b.png"
    out.write_bytes(_png_bytes())  # this morning's card
    monkeypatch.setattr(bt, "_run_frontend_helper", lambda *a, **k: (0, ""))
    card = bt.render_bulletin_card(
        "SPY", "midday", "https://zerogex.io", out, helper_path=str(_helper(tmp_path))
    )
    assert not card.ok
    assert not out.exists()


def test_render_bulletin_card_without_the_helper(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    monkeypatch.setattr(bt, "_locate_frontend_helper", lambda *a, **k: None)
    card = bt.render_bulletin_card("SPY", "midday", "https://zerogex.io", tmp_path / "b.png")
    assert "ZEROGEX_WEB_DIR" in card.error


def test_render_bulletin_card_passes_on_the_scripts_own_reason(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    said = (
        "render-bulletin-png: navigating to http://127.0.0.1:3000/live-bulletin/snapshot/SPY\n"
        "render-bulletin-png: saved what the browser saw to /var/x/bulletin-spy.debug.png\n"
        "render-bulletin-png: the page returned HTTP 502"
    )
    monkeypatch.setattr(bt, "_run_frontend_helper", lambda *a, **k: (7, said))
    card = bt.render_bulletin_card(
        "SPY",
        "midday",
        "http://127.0.0.1:3000",
        tmp_path / "b.png",
        helper_path=str(_helper(tmp_path)),
    )
    assert "pm2 status" in card.error
    assert "The script said: the page returned HTTP 502" in card.error
    assert "/var/x/bulletin-spy.debug.png" in card.error

    # A crash: the message line, not the stack frame after it.
    crash = "render-bulletin-png: TypeError: boom\n    at main (render-bulletin-png.mjs:1:1)"
    monkeypatch.setattr(bt, "_run_frontend_helper", lambda *a, **k: (1, crash))
    card = bt.render_bulletin_card(
        "SPY",
        "midday",
        "http://127.0.0.1:3000",
        tmp_path / "b.png",
        helper_path=str(_helper(tmp_path)),
    )
    assert "TypeError: boom" in card.error
    assert "at main" not in card.error


def test_the_screenshot_loads_the_website_on_this_box(monkeypatch):
    """Not the public domain: Cloudflare can stop a headless browser there."""
    from src.jobs import bulletin_tweet as bt

    monkeypatch.delenv("BULLETIN_TWEET_RENDER_URL", raising=False)
    monkeypatch.delenv("ZEROGEX_SITE_URL", raising=False)
    args = bt._parse_args(["--mode", "midday"])
    assert args.render_url == "http://127.0.0.1:3000"
    assert args.site_url == "https://zerogex.io"  # links in the post stay public

    monkeypatch.setenv("BULLETIN_TWEET_RENDER_URL", "http://10.0.0.5:3000")
    assert bt._parse_args(["--mode", "midday"]).render_url == "http://10.0.0.5:3000"


def test_the_snapshot_token_never_reaches_the_log_or_the_email(tmp_path, monkeypatch, caplog):
    """Playwright quotes the page URL, token and all, in its errors."""
    import logging
    import sys
    from urllib.parse import quote, quote_plus

    from src.jobs import bulletin_tweet as bt

    token = "s3cr3t/+ ~tok"
    fake_node = tmp_path / "node"
    # Stands in for node: prints the token raw and URL-encoded, as Playwright would.
    fake_node.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "from urllib.parse import quote, quote_plus\n"
        "t = sys.argv[sys.argv.index('--token') + 1]\n"
        "parts = ['render-bulletin-png: page.goto: Timeout at http://h/p?token=',\n"
        "         quote_plus(t), ' (', quote(t, safe=''), ') raw=', t, '\\n']\n"
        "sys.stderr.write(''.join(parts))\n"
        "sys.exit(1)\n"
    )
    fake_node.chmod(0o755)
    monkeypatch.setenv("BULLETIN_TWEET_NODE_BINARY", str(fake_node))
    monkeypatch.setenv("BULLETIN_SNAPSHOT_TOKEN", token)

    with caplog.at_level(logging.WARNING, logger="zerogex.bulletin_tweet"):
        card = bt.render_bulletin_card(
            "SPY",
            "midday",
            "http://127.0.0.1:3000",
            tmp_path / "b.png",
            helper_path=str(_helper(tmp_path)),
        )
    assert not card.ok and "page.goto: Timeout" in card.error
    for spelling in (token, quote(token, safe=""), quote_plus(token)):
        assert spelling not in card.error
        assert spelling not in caplog.text
    assert "***" in card.error


# ---------------------------------------------------------------------------
# Headlines
# ---------------------------------------------------------------------------


def test_fresh_headlines_are_asked_for_by_age(monkeypatch):
    from src.jobs import bulletin_tweet as bt
    from src.jobs import cnbc_news

    seen = {}

    def _fake(max_age_hours=None):
        seen["age"] = max_age_hours
        return []

    monkeypatch.setattr(cnbc_news, "fetch_headlines", _fake)
    monkeypatch.setenv("BULLETIN_TWEET_NEWS_MAX_AGE_HOURS", "12")
    items, problem = bt._fetch_fresh_headlines()
    assert items == []
    assert seen["age"] == 12.0
    assert "last 12 hours" in problem


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------


class _Resp:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return b"{}"


def _capture_resend(monkeypatch, bt) -> list[dict]:
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("RESEND_FROM_EMAIL", "alerts@zerogex.io")
    monkeypatch.setenv("BULLETIN_TWEET_EMAIL_TO", "me@example.com")
    monkeypatch.setenv("BULLETIN_TWEET_ADMIN_URL", "https://zerogex.io/admin/x-post")
    sent: list[dict] = []

    def _fake_urlopen(req, timeout=15):
        sent.append(json.loads(req.data))
        return _Resp()

    monkeypatch.setattr(bt, "urlopen", _fake_urlopen)
    return sent


def test_held_email_lists_the_reasons_and_attaches_the_card(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    sent = _capture_resend(monkeypatch, bt)
    # The routine emails can be switched off; this one can't.
    monkeypatch.setenv("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", "0")
    png = tmp_path / "bulletin-spy.png"
    png.write_bytes(_png_bytes())
    assert bt._send_xpost_held_email(
        "midday",
        "SPY",
        ["The post says CPI ran hot; no headline says that.", "Image: the card is blank."],
        _tweet(bt, GOOD_POST),
        png,
        posting=True,
    )
    email = sent[0]
    assert email["subject"] == "Midday X-Post NOT sent: $SPY"
    assert "Nothing was posted to X." in email["text"]
    assert "- The post says CPI ran hot; no headline says that." in email["text"]
    assert GOOD_POST in email["text"]
    assert "https://zerogex.io/admin/x-post" in email["text"]
    assert "&lt;" not in email["subject"]
    assert email["attachments"][0]["filename"] == "bulletin-spy.png"
    assert base64.b64decode(email["attachments"][0]["content"]) == _png_bytes()


def test_ready_email_carries_the_image(tmp_path, monkeypatch):
    from src.jobs import bulletin_tweet as bt

    sent = _capture_resend(monkeypatch, bt)
    monkeypatch.delenv("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", raising=False)
    png = tmp_path / "bulletin-spy.png"
    png.write_bytes(_png_bytes())
    assert bt._send_xpost_ready_email("close", png_path=png)
    assert sent[0]["subject"] == "Market Close X-Post Ready"
    assert "image is attached" in sent[0]["text"]
    assert sent[0]["attachments"][0]["filename"] == "bulletin-spy.png"


def test_sent_email_links_the_post_and_flags_a_failed_reply(monkeypatch):
    from src.jobs import bulletin_tweet as bt

    sent = _capture_resend(monkeypatch, bt)
    monkeypatch.delenv("BULLETIN_TWEET_ADMIN_EMAIL_ENABLED", raising=False)
    result = bt.PostResult(
        ok=True,
        tweet_id="123",
        reply_error="The post went out, but the threaded link reply failed.",
    )
    assert bt._send_xpost_sent_email("premarket", "SPY", result)
    assert sent[0]["subject"] == "Market Open X-Post Sent (link reply failed)"
    assert "https://x.com/i/web/status/123" in sent[0]["text"]
    assert "link reply failed" in sent[0]["text"]


# ---------------------------------------------------------------------------
# Regenerate (the review page's button)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_regenerate_reviews_the_post_without_an_image(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    from src.jobs import bulletin_llm
    from src.jobs import bulletin_tweet as bt

    db = MagicMock()
    db.get_latest_gex_summary = AsyncMock(
        return_value={
            "spot_price": 744.51,
            "gamma_flip": 747.29,
            "call_wall": 745.0,
            "put_wall": 740.0,
            "max_pain": 744.0,
            "net_gex_at_spot": -1.25e9,
        }
    )
    monkeypatch.setattr(bt, "_fetch_fresh_headlines", lambda: ([], "No CNBC headlines."))
    monkeypatch.setattr(
        bt,
        "_try_llm_post",
        lambda *a, **k: bulletin_llm.LlmPost(
            opening="SPY is chopping.", bottom_line="Patience.", reply="Watch 740."
        ),
    )
    reviews: list[dict] = []
    monkeypatch.setattr(
        bulletin_llm,
        "review_post",
        lambda **k: reviews.append(k) or bulletin_llm.Review(ran=True),
    )
    record = await bt.generate_and_store(db, "midday", "SPY", day=date(2026, 9, 24))
    assert record["status"] == "regenerated"
    assert record["post_text"].startswith("Midday Read - $SPY")
    # The missing news is reported; the missing image is expected here.
    assert record["problems"] == ["No CNBC headlines."]
    assert reviews[0]["card_png"] is None


# ---------------------------------------------------------------------------
# Claude calls: writer revision, the review, retries, request shape
# ---------------------------------------------------------------------------


def _claude(body: dict, stop_reason: str = "end_turn") -> dict:
    return {"content": [{"type": "text", "text": json.dumps(body)}], "stop_reason": stop_reason}


def test_generate_post_hands_back_the_draft_with_the_review(monkeypatch):
    from src.jobs import bulletin_llm

    calls = []

    def _fake_call(system, messages, *args):
        calls.append({"system": system, "messages": messages, "args": args})
        return _claude(
            {"opening": "SPY slid.", "bottom_line": "Patience.", "reply": "Watch the first test."}
        )

    monkeypatch.setattr(bulletin_llm, "_call_claude", _fake_call)
    draft = bulletin_llm.LlmPost(opening="SPY rallied.", bottom_line="Up.", reply="Nice.")
    post = bulletin_llm.generate_post(
        mode="midday",
        day=date(2026, 9, 24),
        symbols=[bulletin_llm.SymbolInput(symbol="SPY", spot=744.62)],
        api_key="k",
        revise_from=draft,
        feedback=["'rallied' but SPY is down 0.6% on the day."],
    )
    assert post.opening == "SPY slid."
    roles = [m["role"] for m in calls[0]["messages"]]
    assert roles == ["user", "assistant", "user"]
    assert json.loads(calls[0]["messages"][1]["content"])["opening"] == "SPY rallied."
    assert "'rallied' but SPY is down" in calls[0]["messages"][2]["content"]
    # The writer's reply is pinned to the post schema.
    assert calls[0]["args"][4] == bulletin_llm.POST_SCHEMA


def test_generate_post_reports_why_it_failed(monkeypatch):
    from src.jobs import bulletin_llm

    monkeypatch.setattr(
        bulletin_llm,
        "_call_claude",
        lambda system, messages, *args: {"content": [], "stop_reason": "refusal"},
    )
    errors: list[str] = []
    post = bulletin_llm.generate_post(
        mode="midday",
        day=date(2026, 9, 24),
        symbols=[bulletin_llm.SymbolInput(symbol="SPY", spot=744.62)],
        api_key="k",
        errors=errors,
    )
    assert post is None
    assert errors == ["the writing model declined the request"]


def test_review_post_returns_the_verdict_with_the_image(monkeypatch):
    from src.jobs import bulletin_llm

    seen = {}

    def _fake_call(system, messages, *args):
        seen["system"] = system
        seen["content"] = messages[0]["content"]
        seen["schema"] = args[4]
        return _claude({"problems": ["'rallied' is wrong."], "image_problems": []})

    monkeypatch.setattr(bulletin_llm, "_call_claude", _fake_call)
    review = bulletin_llm.review_post(
        mode="midday",
        day=date(2026, 9, 24),
        post_text="Midday Read - $SPY\n\nSPY rallied.",
        reply_text="Watch 740.\n\nhttps://zerogex.io",
        symbol=bulletin_llm.SymbolInput(symbol="SPY", spot=744.62, prior_close=749.10),
        headlines=[bulletin_llm.Headline(title="Stocks slip after jobs report")],
        card_png=_png_bytes(),
        api_key="k",
    )
    assert review.ran and not review.approved
    assert review.problems == ["'rallied' is wrong."]
    assert seen["system"] == bulletin_llm.REVIEW_SYSTEM_PROMPT
    assert seen["schema"] == bulletin_llm.REVIEW_SCHEMA
    image, text = seen["content"]
    assert image["type"] == "image" and image["source"]["media_type"] == "image/png"
    payload = json.loads(text["text"])
    assert payload["post"].startswith("Midday Read - $SPY")
    assert payload["levels"]["change_vs_prior_close_pct"] < 0
    assert payload["headlines"][0]["title"] == "Stocks slip after jobs report"


def test_review_post_that_cannot_run_is_not_an_approval(monkeypatch):
    from src.jobs import bulletin_llm

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    review = bulletin_llm.review_post(
        mode="midday",
        day=date(2026, 9, 24),
        post_text="x",
        reply_text="y",
        symbol=bulletin_llm.SymbolInput(symbol="SPY"),
    )
    assert not review.ran and not review.approved
    assert "ANTHROPIC_API_KEY" in review.error

    monkeypatch.setattr(
        bulletin_llm, "_call_claude", lambda *a: {"content": [], "stop_reason": "max_tokens"}
    )
    review = bulletin_llm.review_post(
        mode="midday",
        day=date(2026, 9, 24),
        post_text="x",
        reply_text="y",
        symbol=bulletin_llm.SymbolInput(symbol="SPY"),
        api_key="k",
    )
    assert not review.approved and "cut off" in review.error


class _Body:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self.payload


def test_call_claude_retries_once_when_the_api_is_busy(monkeypatch):
    from src.jobs import bulletin_llm

    attempts: list[dict] = []

    def _fake_urlopen(req, timeout=None):
        attempts.append({"body": json.loads(req.data), "headers": dict(req.header_items())})
        if len(attempts) == 1:
            raise HTTPError(req.full_url, 529, "Overloaded", {}, None)
        return _Body(json.dumps({"content": [], "stop_reason": "end_turn"}).encode())

    monkeypatch.setattr(bulletin_llm, "urlopen", _fake_urlopen)
    monkeypatch.setattr(bulletin_llm.time, "sleep", lambda s: None)
    out = bulletin_llm._call_claude(
        "sys", [{"role": "user", "content": "hi"}], "k", "claude-opus-5", 100, 5, {"type": "object"}
    )
    assert out == {"content": [], "stop_reason": "end_turn"}
    assert len(attempts) == 2
    body = attempts[0]["body"]
    # Opus 5 asks for the server-side refusal fallback, and the schema pins
    # the reply's shape.
    assert body["fallbacks"] == "default"
    assert attempts[0]["headers"]["Anthropic-beta"] == bulletin_llm.FALLBACK_BETA
    assert body["output_config"] == {
        "format": {"type": "json_schema", "schema": {"type": "object"}}
    }


def test_call_claude_gives_up_on_a_bad_request_with_the_reason(monkeypatch):
    from src.jobs import bulletin_llm

    def _fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 400, "Bad Request", {}, None)

    monkeypatch.setattr(bulletin_llm, "urlopen", _fake_urlopen)
    monkeypatch.setattr(bulletin_llm.time, "sleep", lambda s: pytest.fail("retried a 400"))
    errors: list[str] = []
    out = bulletin_llm._call_claude(
        "sys", [{"role": "user", "content": "hi"}], "k", "claude-sonnet-5", 100, 5, None, errors
    )
    assert out is None
    assert errors == ["the Claude API returned HTTP 400"]


# ---------------------------------------------------------------------------
# X client: signed posting, image upload with the v2 fallback
# ---------------------------------------------------------------------------


def _creds():
    from src.jobs import x_media_client

    return x_media_client.OAuth1Credentials("ck", "cs", "at", "ats")


def test_post_tweet_is_signed_with_the_oauth1_keys(monkeypatch):
    from src.jobs import x_media_client

    seen = {}

    def _fake_urlopen(req, timeout=None):
        seen["url"] = req.full_url
        seen["auth"] = req.get_header("Authorization")
        seen["body"] = json.loads(req.data)
        return _Body(b'{"data": {"id": "999"}}')

    monkeypatch.setattr(x_media_client, "urlopen", _fake_urlopen)
    tweet_id = x_media_client.post_tweet("hello", _creds(), media_ids=["m1"], reply_to="42")
    assert tweet_id == "999"
    assert seen["url"] == "https://api.x.com/2/tweets"
    assert seen["auth"].startswith("OAuth ") and 'oauth_consumer_key="ck"' in seen["auth"]
    assert seen["body"] == {
        "text": "hello",
        "media": {"media_ids": ["m1"]},
        "reply": {"in_reply_to_tweet_id": "42"},
    }


def test_post_tweet_error_says_what_x_answered(monkeypatch):
    from io import BytesIO

    from src.jobs import x_media_client

    def _fake_urlopen(req, timeout=None):
        raise HTTPError(
            req.full_url, 403, "Forbidden", {}, BytesIO(b'{"detail": "You are not permitted"}')
        )

    monkeypatch.setattr(x_media_client, "urlopen", _fake_urlopen)
    with pytest.raises(x_media_client.XApiError, match="HTTP 403.*not permitted"):
        x_media_client.post_tweet("hello", _creds())


def test_upload_image_falls_back_to_v2(tmp_path, monkeypatch):
    from src.jobs import x_media_client

    png = tmp_path / "card.png"
    png.write_bytes(_png_bytes())
    urls: list[str] = []

    def _fake_urlopen(req, timeout=None):
        urls.append(req.full_url)
        if "1.1" in req.full_url:
            raise HTTPError(req.full_url, 403, "Forbidden", {}, None)
        return _Body(b'{"data": {"id": "v2-media"}}')

    monkeypatch.setattr(x_media_client, "urlopen", _fake_urlopen)
    assert x_media_client.upload_image(png, _creds()) == "v2-media"
    assert urls == [x_media_client.X_MEDIA_UPLOAD_URL, x_media_client.X_MEDIA_UPLOAD_V2_URL]


def test_upload_image_reports_both_endpoints_when_both_fail(tmp_path, monkeypatch):
    from src.jobs import x_media_client

    png = tmp_path / "card.png"
    png.write_bytes(_png_bytes())

    def _fake_urlopen(req, timeout=None):
        raise HTTPError(req.full_url, 401, "Unauthorized", {}, None)

    monkeypatch.setattr(x_media_client, "urlopen", _fake_urlopen)
    with pytest.raises(x_media_client.XApiError, match="v1.1 upload HTTP 401; v2 upload HTTP 401"):
        x_media_client.upload_image(png, _creds())


# ---------------------------------------------------------------------------
# The review page's image endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_image_endpoint_serves_the_attached_card(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from src.api.routers import admin_xpost as ax
    from src.jobs import bulletin_tweet as bt

    root = tmp_path / "artifacts"
    png = root / "midday" / "2026-09-24" / "bulletin-spy.png"
    png.parent.mkdir(parents=True)
    png.write_bytes(_png_bytes())
    monkeypatch.setattr(bt, "_artifact_root_candidates", lambda explicit=None: [root])

    monkeypatch.setattr(bt, "read_latest_record", lambda sym, mode: {"media": {"png": str(png)}})
    resp = await ax.image(symbol="spy", mode="midday")
    assert Path(resp.path) == png.resolve()
    assert resp.media_type == "image/png"

    # A record pointing outside the artifact roots is refused.
    outside = tmp_path / "elsewhere.png"
    outside.write_bytes(_png_bytes())
    monkeypatch.setattr(
        bt, "read_latest_record", lambda sym, mode: {"media": {"png": str(outside)}}
    )
    with pytest.raises(HTTPException) as exc:
        await ax.image(symbol="SPY", mode="midday")
    assert exc.value.status_code == 404

    monkeypatch.setattr(bt, "read_latest_record", lambda sym, mode: None)
    with pytest.raises(HTTPException) as exc:
        await ax.image(symbol="SPY", mode="midday")
    assert exc.value.status_code == 404


def test_a_crash_on_a_scheduled_run_still_emails_the_operator(monkeypatch):
    from src.jobs import bulletin_tweet as bt

    async def _boom(args):
        raise RuntimeError("disk full")

    held: list[dict] = []
    monkeypatch.setattr(bt, "_run", _boom)
    monkeypatch.setattr(
        bt,
        "_send_xpost_held_email",
        lambda mode, symbol, problems, tweet, png, posting: held.append(
            {"mode": mode, "symbol": symbol, "problems": problems, "posting": posting}
        ),
    )
    monkeypatch.delenv("BULLETIN_TWEET_AUTOPILOT", raising=False)
    assert bt.main(["--mode", "close", "--stage"]) == 1
    assert held[0]["symbol"] == "SPY"
    assert "disk full" in held[0]["problems"][0]
    assert held[0]["posting"] is False

    # A manual preview only logs.
    held.clear()
    assert bt.main(["--mode", "close"]) == 1
    assert held == []


def test_call_claude_drops_the_fallback_option_if_the_account_is_refused_it(monkeypatch):
    from io import BytesIO

    from src.jobs import bulletin_llm

    bodies: list[dict] = []

    def _fake_urlopen(req, timeout=None):
        bodies.append(json.loads(req.data))
        if len(bodies) == 1:
            raise HTTPError(
                req.full_url,
                400,
                "Bad Request",
                {},
                BytesIO(b'{"error": {"message": "fallbacks: not enabled for this organization"}}'),
            )
        return _Body(json.dumps({"content": [], "stop_reason": "end_turn"}).encode())

    monkeypatch.setattr(bulletin_llm, "urlopen", _fake_urlopen)
    monkeypatch.setattr(bulletin_llm.time, "sleep", lambda s: pytest.fail("slept"))
    out = bulletin_llm._call_claude(
        "sys", [{"role": "user", "content": "hi"}], "k", "claude-opus-5", 100, 5
    )
    assert out == {"content": [], "stop_reason": "end_turn"}
    assert "fallbacks" in bodies[0] and "fallbacks" not in bodies[1]
