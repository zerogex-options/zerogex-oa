"""The bulletin X-posts' shape after the 2026-10 copy changes.

* Two fires a day: the Morning Read and the Post-Market Read (no midday).
* Numbers rounded the way a person says them: "$6B", "0.4%", "~747".
* The Morning Read lists the levels twice, all expirations and 0DTE only.
* The close read introduces its levels as tomorrow's map once, in the heading,
  and the writer is told not to explain the 0DTE roll-off in the prose.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.jobs import bulletin_format as fmt
from tests.test_bulletin_tweet_job import (
    _passing_run,
    _reload_module,
    _stub_writer,
    _summary_row,
)

ET = ZoneInfo("America/New_York")


def _spy(mod, **overrides):
    b = mod._shape_bulletin(
        _summary_row(
            "SPY", spot=744.51, gamma_flip=747.29, call_wall=750.0, put_wall=740.0, max_pain=744.0
        ),
        "SPY",
    )
    for k, v in overrides.items():
        setattr(b, k, v)
    return b


# ---------------------------------------------------------------------------
# Rounding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (745.0, "745"),
        (747.29, "~747"),
        (744.51, "~745"),
        (744.996, "745"),
        (7482.71, "~7,483"),
        (7500.0, "7,500"),
    ],
)
def test_prices_round_to_the_dollar_with_a_tilde_when_approximate(value, expected):
    assert fmt.price(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (6.28e9, "$6B"),
        (1.2e9, "$1B"),
        (999.6e6, "$1B"),
        (340.4e6, "$340M"),
        (72.3e6, "$72M"),
        (12_000, "$12K"),
        (500, "$500"),
    ],
)
def test_dollar_amounts_round_to_whole_units(value, expected):
    assert fmt.dollars(value) == expected
    assert fmt.dollars(-value) == expected
    assert fmt.signed_dollars(-value) == f"-{expected}"


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.39, "+0.4%"), (-0.62, "-0.6%"), (1.25, "+1.3%"), (0.04, "0.0%"), (-0.04, "0.0%")],
)
def test_percent_changes_take_one_decimal(value, expected):
    assert fmt.pct(value) == expected


def test_the_writer_is_handed_the_rounded_numbers():
    from src.jobs import bulletin_llm

    s = bulletin_llm.SymbolInput(
        symbol="SPY",
        spot=744.51,
        prior_close=741.62,
        gamma_flip=747.29,
        call_wall=750.0,
        put_wall=740.0,
        net_gex=6.28e9,
        zero_dte={"put_wall": 742.0, "call_wall": 748.0, "gamma_flip": 745.6},
    )
    display = s.to_prompt_dict()["display"]
    assert display["spot"] == "~745"
    assert display["gamma_flip"] == "~747"
    assert display["call_wall"] == "750"
    assert display["net_gex"] == "+$6B"
    assert display["change_vs_prior_close"] == "+0.4%"
    assert display["zero_dte"] == {"put_wall": "742", "call_wall": "748", "gamma_flip": "~746"}
    # The unrounded net gamma no longer rides along as something to quote.
    assert "net_gex_display" not in s.to_prompt_dict()


def test_a_rounded_flip_still_counts_as_the_flip():
    from src.jobs import bulletin_llm

    s = bulletin_llm.SymbolInput(symbol="SPY", spot=744.51, gamma_flip=747.29, call_wall=750.0)
    post = bulletin_llm.LlmPost(
        opening="SPY sits under the ~747 gamma flip, with the call wall at 750.",
        bottom_line="",
        reply="Watch ~745.",
    )
    assert bulletin_llm._post_problems(post, [s]) == []


def test_the_prompts_carry_the_house_style():
    from src.jobs import bulletin_llm

    for prompt in (bulletin_llm.SYSTEM_PROMPT, bulletin_llm.REVIEW_SYSTEM_PROMPT):
        assert "midday" not in prompt.lower()
        assert "$6B" in prompt and "0.4%" in prompt and "~747" in prompt
    # The writer is told not to explain the roll-off.  The fact-check isn't
    # asked to police it: on 2026-10-09 it flagged the levels heading itself,
    # which the writer can't change, and held the close post.  It's told to
    # leave that heading alone instead.
    assert "Don't explain that the day's 0DTE options expired" in bulletin_llm.SYSTEM_PROMPT
    assert "rolling off" not in bulletin_llm.REVIEW_SYSTEM_PROMPT.split("Don't flag")[0]
    assert "never flag it" in bulletin_llm.REVIEW_SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# No midday fire
# ---------------------------------------------------------------------------


def test_there_is_no_midday_mode():
    mod = _reload_module()
    assert mod.MODES == ("premarket", "close")
    with pytest.raises(SystemExit):
        mod._parse_args(["--mode", "midday"])


@pytest.mark.parametrize(
    ("hour", "minute", "mode"),
    [
        (9, 14, "close"),  # before the morning fire: still the prior close
        (9, 15, "premarket"),
        (12, 30, "premarket"),  # where the midday read used to take over
        (16, 4, "premarket"),
        (16, 5, "close"),
        (23, 0, "close"),
    ],
)
def test_the_admin_page_holds_the_morning_read_until_the_close(hour, minute, mode):
    mod = _reload_module()
    assert mod.resolve_current_mode(datetime(2026, 10, 8, hour, minute, tzinfo=ET)) == mode


# ---------------------------------------------------------------------------
# The close read's heading
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "when"),
    [
        (date(2026, 10, 7), "tomorrow"),  # Wednesday
        (date(2026, 10, 9), "Monday"),  # Friday
    ],
)
def test_the_close_heading_frames_the_roll_off_as_the_lead_in(day, when):
    mod = _reload_module()
    assert mod._levels_heading("close", day) == (
        f"With today's 0DTE rolling off, here is the map for {when}:"
    )


# ---------------------------------------------------------------------------
# The Morning Read's 0DTE-only map
# ---------------------------------------------------------------------------


def test_the_morning_read_lists_both_maps(monkeypatch):
    mod = _reload_module()
    _stub_writer(monkeypatch, mod)
    spy = _spy(mod, zero_dte=mod.ZeroDteLevels(put_wall=742.0, call_wall=748.0, gamma_flip=745.6))
    body = mod.build_tweet_body("premarket", date(2026, 10, 8), [spy], lead_symbol="SPY")
    assert (
        "Key levels (all expirations):\n• 740 put wall\n• 750 call wall\n• ~747 gamma flip\n\n"
        "Key levels (0DTE only):\n• 742 put wall\n• 748 call wall\n• ~746 gamma flip\n\n"
        "Bottom line:"
    ) in body.text
    assert mod._text_problems(body, spy, "premarket") == []


def test_without_a_0dte_map_the_morning_read_keeps_one_list(monkeypatch):
    mod = _reload_module()
    _stub_writer(monkeypatch, mod)
    body = mod.build_tweet_body("premarket", date(2026, 10, 8), [_spy(mod)], lead_symbol="SPY")
    assert "Key levels:\n• 740 put wall" in body.text
    assert "0DTE" not in body.text


def test_a_post_missing_its_0dte_list_is_caught(monkeypatch):
    mod = _reload_module()
    _stub_writer(monkeypatch, mod)
    spy = _spy(mod, zero_dte=mod.ZeroDteLevels(put_wall=742.0, call_wall=748.0, gamma_flip=745.6))
    body = mod.build_tweet_body("premarket", date(2026, 10, 8), [spy], lead_symbol="SPY")
    body.text = body.text.replace("• 748 call wall", "• 749 call wall")
    assert any("0DTE-only" in p for p in mod._text_problems(body, spy, "premarket"))


@pytest.mark.asyncio
async def test_0dte_levels_come_from_todays_expiration_alone():
    mod = _reload_module()
    db = MagicMock()
    db.get_strike_profile_timeseries = AsyncMock(
        return_value=[{"put_wall": 742.0, "call_wall": 748.0, "gamma_flip": 745.6}]
    )
    b = _spy(mod)
    await mod._attach_zero_dte_levels(db, b, date(2026, 10, 8), "premarket")
    assert b.zero_dte == mod.ZeroDteLevels(put_wall=742.0, call_wall=748.0, gamma_flip=745.6)
    kwargs = db.get_strike_profile_timeseries.await_args.kwargs
    assert kwargs["symbol"] == "SPY"
    assert kwargs["expirations"] == [date(2026, 10, 8)]


@pytest.mark.asyncio
async def test_0dte_levels_skip_a_minute_whose_price_has_not_landed():
    """The newest minute comes back without walls until its price bar is in;
    the read falls back to the newest minute that has them."""
    mod = _reload_module()
    db = MagicMock()
    db.get_strike_profile_timeseries = AsyncMock(
        return_value=[
            {"put_wall": 741.0, "call_wall": 747.0, "gamma_flip": 744.9},
            {"put_wall": 742.0, "call_wall": 748.0, "gamma_flip": 745.6},
            {"put_wall": None, "call_wall": None, "gamma_flip": None},
        ]
    )
    b = _spy(mod)
    await mod._attach_zero_dte_levels(db, b, date(2026, 10, 8), "premarket")
    assert b.zero_dte == mod.ZeroDteLevels(put_wall=742.0, call_wall=748.0, gamma_flip=745.6)


@pytest.mark.asyncio
async def test_the_close_read_has_no_0dte_map():
    mod = _reload_module()
    db = MagicMock()
    db.get_strike_profile_timeseries = AsyncMock(return_value=[{"put_wall": 742.0}])
    b = _spy(mod)
    await mod._attach_zero_dte_levels(db, b, date(2026, 10, 8), "close")
    assert b.zero_dte is None
    db.get_strike_profile_timeseries.assert_not_awaited()


@pytest.mark.parametrize(
    "result",
    [
        RuntimeError("pool exhausted"),
        [],
        [{"put_wall": None, "call_wall": None, "gamma_flip": None}],
    ],
)
@pytest.mark.asyncio
async def test_a_missing_0dte_map_never_holds_the_post(result):
    mod = _reload_module()
    db = MagicMock()
    if isinstance(result, Exception):
        db.get_strike_profile_timeseries = AsyncMock(side_effect=result)
    else:
        db.get_strike_profile_timeseries = AsyncMock(return_value=result)
    b = _spy(mod)
    await mod._attach_zero_dte_levels(db, b, date(2026, 10, 8), "premarket")
    assert b.zero_dte is None


def test_the_writer_can_name_a_0dte_level_without_widening_the_flip():
    from src.jobs import bulletin_llm

    s = bulletin_llm.SymbolInput(
        symbol="SPY",
        spot=744.51,
        gamma_flip=747.29,
        call_wall=750.0,
        put_wall=740.0,
        zero_dte={"put_wall": 742.0, "call_wall": 748.0, "gamma_flip": 752.4},
    )

    def _problems(opening: str) -> list[str]:
        post = bulletin_llm.LlmPost(opening=opening, bottom_line="", reply="Watch the open.")
        return bulletin_llm._post_problems(post, [s])

    assert _problems("The 0DTE call wall at 748 sits closer than the 750 call wall.") == []
    assert _problems("The 0DTE gamma flip at ~752 is above the ~747 flip.") == []
    # 750 lies between the two flips, but neither flip was ever 750.
    problems = _problems("SPY is under the gamma flip at 750.")
    assert problems and "0DTE: 752.4" in problems[0]


@pytest.mark.asyncio
async def test_a_morning_fire_records_and_posts_both_maps(tmp_path, monkeypatch):
    mod = _reload_module()
    stubs = _passing_run(monkeypatch, mod)
    stubs["db"].get_strike_profile_timeseries = AsyncMock(
        return_value=[{"put_wall": 742.0, "call_wall": 748.0, "gamma_flip": 745.6}]
    )
    args = mod._parse_args(
        [
            "--mode",
            "premarket",
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
            "--allow-non-trading-day",
        ]
    )
    assert await mod._run(args) == 0
    day_dir = tmp_path / "premarket" / "2026-07-06"
    text = (day_dir / "tweet_text.md").read_text()
    assert "Key levels (all expirations):\n• 740 put wall" in text
    assert "Key levels (0DTE only):\n• 742 put wall\n• 748 call wall\n• ~746 gamma flip" in text
    manifest = json.loads((day_dir / "manifest.json").read_text())
    assert manifest["bulletins"][0]["zero_dte"] == {
        "put_wall": 742.0,
        "call_wall": 748.0,
        "gamma_flip": 745.6,
    }
    # The writer and the fact-check both saw the 0DTE map.
    assert stubs["writer"][0]["present"][0].zero_dte is not None
    assert stubs["review"][0]["symbol"].zero_dte == {
        "put_wall": 742.0,
        "call_wall": 748.0,
        "gamma_flip": 745.6,
    }


# ---------------------------------------------------------------------------
# Autopilot: one switch per post, both off by default
# ---------------------------------------------------------------------------

SWITCHES = ("BULLETIN_TWEET_AUTOPILOT_MORNING", "BULLETIN_TWEET_AUTOPILOT_CLOSE")


async def _scheduled_fire(mod, monkeypatch, tmp_path, mode: str) -> dict:
    """Run one timer fire (--stage) with everything outside stubbed, and
    report whether it posted or emailed "X-Post Ready"."""
    _passing_run(monkeypatch, mod)
    monkeypatch.setattr(mod, "_deadline_problem", lambda *a, **k: None)
    monkeypatch.delenv("BULLETIN_TWEET_NOTIFY_HOOK", raising=False)
    out: dict = {"posted": [], "ready": [], "sent": []}

    def _fake_post(**kwargs):
        out["posted"].append(kwargs)
        return mod.PostResult(ok=True, tweet_id="t-1", reply_id="r-1")

    monkeypatch.setattr(mod, "post_bulletin", _fake_post)
    monkeypatch.setattr(
        mod,
        "_send_xpost_ready_email",
        lambda mode, png_path=None: out["ready"].append(mode) or True,
    )
    monkeypatch.setattr(mod, "_send_xpost_sent_email", lambda *a: out["sent"].append(a) or True)
    args = mod._parse_args(
        [
            "--mode",
            mode,
            "--date",
            "2026-07-06",  # Monday
            "--artifact-dir",
            str(tmp_path),
            "--stage",
            "--allow-non-trading-day",
        ]
    )
    out["rc"] = await mod._run(args)
    return out


@pytest.mark.parametrize("mode", ["premarket", "close"])
@pytest.mark.asyncio
async def test_by_default_every_post_waits_for_you(tmp_path, monkeypatch, mode):
    """Both switches off (the default): each fire stages the post and emails
    "X-Post Ready"; nothing goes to X."""
    mod = _reload_module()
    for switch in SWITCHES:
        monkeypatch.delenv(switch, raising=False)
    out = await _scheduled_fire(mod, monkeypatch, tmp_path, mode)
    assert out["rc"] == 0
    assert out["posted"] == []
    assert out["ready"] == [mode]


@pytest.mark.asyncio
async def test_the_morning_switch_posts_the_morning_read(tmp_path, monkeypatch):
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT_MORNING", "1")
    monkeypatch.delenv("BULLETIN_TWEET_AUTOPILOT_CLOSE", raising=False)
    out = await _scheduled_fire(mod, monkeypatch, tmp_path, "premarket")
    assert out["rc"] == 0
    assert len(out["posted"]) == 1
    assert out["posted"][0]["media"].png_path.name == "bulletin-spy.png"
    assert out["ready"] == []
    assert len(out["sent"]) == 1


@pytest.mark.asyncio
async def test_the_morning_switch_leaves_the_close_post_waiting(tmp_path, monkeypatch):
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT_MORNING", "1")
    monkeypatch.delenv("BULLETIN_TWEET_AUTOPILOT_CLOSE", raising=False)
    out = await _scheduled_fire(mod, monkeypatch, tmp_path, "close")
    assert out["posted"] == []
    assert out["ready"] == ["close"]


@pytest.mark.parametrize("mode", ["premarket", "close"])
@pytest.mark.asyncio
async def test_the_old_shared_switch_turns_nothing_on(tmp_path, monkeypatch, caplog, mode):
    """A BULLETIN_TWEET_AUTOPILOT=1 left in .env must not put either post on
    autopilot; the log says which switch to use instead."""
    mod = _reload_module()
    for switch in SWITCHES:
        monkeypatch.delenv(switch, raising=False)
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT", "1")
    with caplog.at_level("WARNING", logger="zerogex.bulletin_tweet"):
        out = await _scheduled_fire(mod, monkeypatch, tmp_path, mode)
    assert out["posted"] == []
    assert out["ready"] == [mode]
    assert mod.AUTOPILOT_SWITCHES[mode] in caplog.text


@pytest.mark.parametrize(
    ("value", "on"),
    [("1", True), ("true", True), ("YES", True), (" on ", True), ("0", False), ("", False)],
)
def test_switch_values(monkeypatch, value, on):
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT_MORNING", value)
    assert mod.autopilot_on("premarket") is on


def test_a_manual_post_flag_still_posts_without_any_switch(monkeypatch):
    mod = _reload_module()
    for switch in SWITCHES:
        monkeypatch.delenv(switch, raising=False)
    assert mod._will_post(mod._parse_args(["--mode", "close", "--post"])) is True
    assert mod._will_post(mod._parse_args(["--mode", "close", "--stage"])) is False


def test_the_status_report_says_what_is_on(monkeypatch):
    mod = _reload_module()
    monkeypatch.setenv("BULLETIN_TWEET_AUTOPILOT_MORNING", "1")
    monkeypatch.delenv("BULLETIN_TWEET_AUTOPILOT_CLOSE", raising=False)
    monkeypatch.delenv("BULLETIN_TWEET_AUTOPILOT", raising=False)
    for key in ("X_BOT_API_KEY", "X_BOT_API_SECRET", "X_BOT_ACCESS_TOKEN"):
        monkeypatch.setenv(key, "x")
    monkeypatch.delenv("X_BOT_ACCESS_TOKEN_SECRET", raising=False)
    report = mod.autopilot_report().splitlines()
    assert report[0].startswith("Morning Read: ON")
    assert report[1].startswith("Post-Market Read: off")
    assert "MISSING" in report[2] and "X_BOT_ACCESS_TOKEN_SECRET" in report[2]


# ---------------------------------------------------------------------------
# A fact-check finding about text the job writes can't hold the post
# ---------------------------------------------------------------------------

# What the fact-check said about the 2026-10-09 close post, after the rewrite.
HEADING_FINDING = (
    '"With today\'s 0DTE rolling off, here is the map for Monday" - explaining the 0DTE '
    "expiry/roll-off is redundant boilerplate the levels heading already covers."
)


def _close_post(mod, monkeypatch, opening="SPY opened near 776, dipped to 775 and held."):
    from src.jobs import bulletin_llm

    post = bulletin_llm.LlmPost(
        opening=opening,
        bottom_line="Bottom line: a grind inside 775 to 780 until one side gives.",
        reply="Watch the first test of 775.",
    )
    monkeypatch.setattr(mod, "_try_llm_post", lambda *a, **k: post)
    spy = _spy(mod, gamma_flip=772.07, call_wall=780.0, put_wall=775.0)
    tweet = mod.build_tweet_body("close", date(2026, 10, 9), [spy], lead_symbol="SPY")
    return tweet, spy


def test_a_finding_that_quotes_the_levels_heading_is_set_aside(monkeypatch):
    mod = _reload_module()
    tweet, _ = _close_post(mod, monkeypatch)
    assert mod._quotes_only_caller_text(HEADING_FINDING, tweet)
    assert mod._quotes_only_caller_text('"Post-Market Read - $SPY" is a bare header.', tweet)
    # A levels-list line is the job's too; the job checks those numbers itself.
    assert mod._quotes_only_caller_text('"• 775 put wall" repeats the prose.', tweet)
    # "775" is in the list AND in the prose, so it may be about the prose.
    assert mod._quotes_only_caller_text('"775" is the wrong level.', tweet) is False


@pytest.mark.parametrize(
    "finding",
    [
        # The writer's own words.
        '"dipped to 775 and held" is contradicted by the session low of 774.10.',
        # The bottom line, quoted with the label the job adds in front of it.
        '"Bottom line: a grind inside 775 to 780" reads as a trade call.',
        # A quote from the reply.
        '"Watch the first test of 775" restates the bottom line.',
        # One quote from the heading, one from the prose: the prose one stands.
        '"here is the map for Monday" and "dipped to 775" disagree.',
        # Nothing quoted: can't tell, so it stands.
        "The post never mentions the jobs report headline.",
    ],
)
def test_findings_about_the_writers_words_still_count(monkeypatch, finding):
    mod = _reload_module()
    tweet, _ = _close_post(mod, monkeypatch)
    assert mod._quotes_only_caller_text(finding, tweet) is False


def test_the_heading_finding_no_longer_holds_the_close_post(monkeypatch):
    """Replays 2026-10-09: the fact-check's only finding was about the levels
    heading, so the post passes instead of being held."""
    from src.jobs import bulletin_llm

    mod = _reload_module()
    tweet, spy = _close_post(mod, monkeypatch)
    monkeypatch.setattr(
        bulletin_llm,
        "review_post",
        lambda **kwargs: bulletin_llm.Review(ran=True, problems=[HEADING_FINDING]),
    )
    result = mod._review("close", date(2026, 10, 9), tweet, spy, [], b"png")
    assert result.problems == []

    # A real finding about the prose still goes back to the writer.
    real = '"opened near 776" is contradicted by the 776.25 open.'
    monkeypatch.setattr(
        bulletin_llm,
        "review_post",
        lambda **kwargs: bulletin_llm.Review(ran=True, problems=[HEADING_FINDING, real]),
    )
    result = mod._review("close", date(2026, 10, 9), tweet, spy, [], b"png")
    assert result.fixable == [real]
