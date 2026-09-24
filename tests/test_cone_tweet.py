"""Tests for the intraday cone auto-tweet.

The tweet builder is the easy half.  The load-bearing tests here are the two
ways this job could publish something dishonest without looking broken:

  * the publication gate letting a record through that has no skill behind it,
    which would spend the credibility the graded receipt exists to build;
  * the preview quoting TODAY's cumulative totals on a past session, which
    would show a record that did not exist yet - the same lookahead that made
    the first cone backfill worthless.

Both are asserted directly rather than inferred from output shape.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.jobs.cone_tweet import (
    MIN_BRIER_SKILL,
    MIN_CLAIMS_FOR_PUBLICATION,
    MIN_SESSIONS_FOR_PUBLICATION,
    TWEET_MAX_LEN,
    _split,
    build_receipt_tweet,
    per_symbol_breakdown,
    publication_gate,
    summarize,
)

SESSION = date(2026, 9, 21)


def _claims(
    n: int,
    hold_prob: float,
    hold_rate: float,
    *,
    session: date = SESSION,
    symbol: str = "SPY",
    sessions: int = 1,
) -> list[dict]:
    """``n`` claims at a fixed committed probability, ``hold_rate`` of them held.

    Spread over ``sessions`` consecutive days so the session floor in the gate
    can be exercised independently of the claim floor.
    """
    out = []
    held_target = round(n * hold_rate)
    for i in range(n):
        out.append(
            {
                "session_date": session - timedelta(days=i % sessions),
                "symbol": symbol,
                "horizon_min": 30,
                "hold_prob": hold_prob,
                "held": i < held_target,
            }
        )
    return out


def _skillful(n: int = 400, sessions: int = 8) -> list[dict]:
    """A set whose per-claim confidence actually discriminates.

    Half committed at 95% and held 95% of the time, half at 65% and held 65%.
    Base rate is 80% either way, so the constant-base-rate strawman scores
    0.16 while the cone scores ~0.1375 - a real edge, not just a high hit rate.
    """
    confident = _claims(n // 2, 0.95, 0.95, sessions=sessions)
    hedged = _claims(n // 2, 0.65, 0.65, sessions=sessions)
    return confident + hedged


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def test_summarize_reports_no_skill_when_prediction_is_the_base_rate():
    """Always predicting the realized base rate must NOT read as skill."""
    stats = summarize(_claims(400, 0.8, 0.8, sessions=8))
    assert stats["n"] == 400
    assert stats["brier"] == pytest.approx(0.16, abs=1e-6)
    assert stats["baseline_brier"] == pytest.approx(0.16, abs=1e-6)
    assert stats["beats_baseline"] is False


def test_a_dead_heat_with_the_baseline_is_not_skill():
    """Regression.  An earlier revision compared an unrounded Brier against a
    rounded baseline, so ``0.15999999999999998 < 0.16`` read as a win - and
    the symbols that lands on in live data (SPX, NDX) are exactly the ones
    sitting within a ten-thousandth of their baseline.  A tie is not a win."""
    stats = summarize(_claims(400, 0.8, 0.8, sessions=8))
    assert stats["brier_skill"] == pytest.approx(0.0, abs=1e-9)
    assert stats["beats_baseline"] is False


def test_skill_margin_must_clear_the_floor_not_merely_be_positive():
    stats = summarize(_skillful())
    assert stats["brier_skill"] > MIN_BRIER_SKILL
    # ...and the floor is what the verdict consults, not the raw sign.
    borderline = dict(stats, brier_skill=MIN_BRIER_SKILL / 2, beats_baseline=False)
    # Empty breakdown: the aggregate check fires first and never reaches it.
    ok, reason = publication_gate(borderline, [])
    assert ok is False
    assert "below the" in reason


def test_summarize_detects_skill_when_confidence_discriminates():
    stats = summarize(_skillful())
    assert stats["brier"] < stats["baseline_brier"]
    assert stats["beats_baseline"] is True


def test_summarize_counts_distinct_sessions_not_claims():
    stats = summarize(_claims(400, 0.8, 0.8, sessions=8))
    assert stats["sessions"] == 8


def test_summarize_empty_is_not_an_error():
    stats = summarize([])
    assert stats["n"] == 0
    assert stats["beats_baseline"] is None


# ---------------------------------------------------------------------------
# The publication gate
# ---------------------------------------------------------------------------


def _gate(claims):
    """The gate exactly as the job calls it — aggregate plus breakdown."""
    return publication_gate(summarize(claims), per_symbol_breakdown(claims))


def test_gate_blocks_a_record_with_no_skill_over_the_base_rate():
    """The whole point of the gate.  Big sample, many sessions, calibrated -
    and still refused, because none of that is evidence of skill."""
    ok, reason = _gate(_claims(800, 0.8, 0.8, sessions=20))
    assert ok is False
    assert "too close to the strawman" in reason


def test_gate_blocks_a_thin_sample_even_when_it_beats_baseline():
    few = _skillful(n=MIN_CLAIMS_FOR_PUBLICATION - 40, sessions=8)
    assert summarize(few)["beats_baseline"] is True
    ok, reason = _gate(few)
    assert ok is False
    assert "sample too thin" in reason


def test_gate_blocks_one_busy_session_masquerading_as_a_record():
    """Enough claims, but all from too few days - they are not independent."""
    claims = _skillful(n=600, sessions=MIN_SESSIONS_FOR_PUBLICATION - 1)
    stats = summarize(claims)
    assert stats["n"] >= MIN_CLAIMS_FOR_PUBLICATION
    assert stats["beats_baseline"] is True
    ok, reason = _gate(claims)
    assert ok is False
    assert "too few sessions" in reason


def test_gate_passes_a_real_record():
    ok, reason = _gate(_skillful())
    assert ok is True
    assert "over the base rate" in reason
    assert "every symbol clears" in reason


def test_gate_blocks_empty():
    ok, _ = _gate([])
    assert ok is False


# ---------------------------------------------------------------------------
# Every symbol has to earn its place, not just the pool
# ---------------------------------------------------------------------------


def _mixed_pool():
    """The 2026-09-24 shape: one symbol carrying a symbol that is a dead heat
    with its own base rate.  SPY +7.9% and QQQ +3.6% pooled with SPX +0.02%
    and NDX -0.04% is what the live data looked like when this gate was
    tightened."""
    carried = _skillful(n=800, sessions=8)
    for c in carried:
        c["symbol"] = "SPY"
    return carried + _claims(800, 0.8, 0.8, symbol="NDX", sessions=8)


def test_gate_blocks_a_dead_heat_symbol_the_aggregate_would_have_carried():
    claims = _mixed_pool()
    # The pool on its own looks like a track record...
    assert summarize(claims)["beats_baseline"] is True
    # ...and the gate must still refuse it.
    ok, reason = _gate(claims)
    assert ok is False
    assert "NDX" in reason


def test_gate_names_only_the_symbols_that_fell_short():
    ok, reason = _gate(_mixed_pool())
    assert ok is False
    assert "NDX" in reason
    assert "SPY" not in reason


def test_gate_blocks_a_symbol_with_too_little_data_rather_than_skipping_it():
    """"We do not know yet" is a reason not to publish, not a reason to look
    away — the thin symbol is still inside the number being tweeted."""
    claims = _skillful(n=800, sessions=8)
    for c in claims:
        c["symbol"] = "SPY"
    newcomer = _claims(20, 0.8, 0.8, symbol="QQQ", sessions=4)
    ok, reason = _gate(claims + newcomer)
    assert ok is False
    assert "QQQ" in reason
    assert "claims" in reason


def test_gate_passes_when_every_symbol_clears_on_its_own():
    claims = []
    for sym in ("SPY", "QQQ"):
        part = _skillful(n=800, sessions=8)
        for c in part:
            c["symbol"] = sym
        claims.extend(part)
    ok, reason = _gate(claims)
    assert ok is True
    assert "SPY" in reason and "QQQ" in reason


def test_per_symbol_argument_is_required_so_no_lenient_path_exists():
    """An optional breakdown would leave a lenient gate a caller could reach
    by forgetting an argument, and the lenient gate is the one that publishes
    something unearned."""
    with pytest.raises(TypeError):
        publication_gate(summarize(_skillful()))


# ---------------------------------------------------------------------------
# No lookahead
# ---------------------------------------------------------------------------


def test_cumulative_never_includes_a_later_session():
    """A preview of an old session must report the record as it stood THEN."""
    early = _claims(40, 0.8, 0.8, session=date(2026, 9, 14))
    middle = _claims(40, 0.8, 0.8, session=date(2026, 9, 15))
    late = _claims(40, 0.8, 0.8, session=date(2026, 9, 16))

    today, cume = _split(early + middle + late, date(2026, 9, 15))

    assert all(c["session_date"] == date(2026, 9, 15) for c in today)
    assert len(today) == 40
    assert len(cume) == 80
    assert max(c["session_date"] for c in cume) == date(2026, 9, 15)


def test_split_on_a_session_with_nothing_graded_yields_no_day_claims():
    today, cume = _split(_claims(40, 0.8, 0.8), SESSION + timedelta(days=1))
    assert today == []
    assert len(cume) == 40  # the record still stands


# ---------------------------------------------------------------------------
# Tweet copy
# ---------------------------------------------------------------------------


def _tweet(day_n: int = 84, cume_n: int = 3940, sessions: int = 12) -> str:
    day = summarize(_claims(day_n, 0.74, 0.73))
    cume = summarize(_claims(cume_n, 0.82, 0.81, sessions=sessions))
    return build_receipt_tweet(day, cume)


def test_tweet_fits_the_character_limit():
    assert len(_tweet()) <= TWEET_MAX_LEN


def test_tweet_fits_even_with_implausibly_large_counts():
    assert len(_tweet(day_n=999, cume_n=999999, sessions=999)) <= TWEET_MAX_LEN


def test_tweet_leads_with_the_record_and_nests_the_day_inside_it():
    text = _tweet()
    assert "Record (12 sessions)" in text
    assert "Today:" in text
    # The day is reported, but the record is the claim - so it is not the
    # only number present, and it is not the last word before the link.
    assert text.index("Today:") < text.index("Record (")


def test_tweet_always_keeps_the_link():
    text = _tweet()
    assert text.rstrip().endswith("/forecast/cone")


def test_tweet_keeps_the_link_when_a_long_site_url_forces_trimming():
    day = summarize(_claims(84, 0.74, 0.73))
    cume = summarize(_claims(3940, 0.82, 0.81, sessions=12))
    long_host = "https://" + ("x" * 150) + ".example"
    text = build_receipt_tweet(day, cume, site_url=long_host)
    assert text.rstrip().endswith("/forecast/cone")


def test_tweet_omits_the_day_line_when_nothing_graded_today():
    text = build_receipt_tweet(
        summarize([]), summarize(_claims(3940, 0.82, 0.81, sessions=12))
    )
    assert "Today:" not in text
    assert "Record (" in text


def test_tweet_says_session_singular_for_one_session():
    text = build_receipt_tweet(
        summarize([]), summarize(_claims(300, 0.8, 0.8, sessions=1))
    )
    assert "(1 session)" in text


# ---------------------------------------------------------------------------
# The aggregate must not be able to hide a biased symbol
# ---------------------------------------------------------------------------


def test_breakdown_surfaces_a_symbol_with_no_skill_that_the_aggregate_hides():
    """Two symbols: one with real skill, one with none.  The aggregate can
    read fine; the breakdown has to name the one that does not."""
    good = _skillful(n=400, sessions=8)
    for c in good:
        c["symbol"] = "SPY"
    flat = _claims(400, 0.8, 0.8, symbol="NDX", sessions=8)

    rows = {r["symbol"]: r for r in per_symbol_breakdown(good + flat)}

    assert set(rows) == {"SPY", "NDX"}
    assert rows["SPY"]["beats_baseline"] is True
    assert rows["NDX"]["beats_baseline"] is False


def test_breakdown_covers_every_symbol_present():
    claims = []
    for sym in ("SPY", "QQQ", "SPX", "NDX"):
        claims.extend(_claims(40, 0.8, 0.8, symbol=sym, sessions=4))
    assert [r["symbol"] for r in per_symbol_breakdown(claims)] == [
        "NDX", "QQQ", "SPX", "SPY",
    ]


def test_tweet_keeps_its_paragraph_breaks():
    """Regression.  "" (blank line) and None (omitted line) are both falsy, so
    a truthiness filter in the assembler collapsed every paragraph break and
    the copy went out as a wall of text."""
    text = _tweet()
    assert text.count("\n\n") >= 2, f"paragraph breaks lost:\n{text}"


def test_trimmed_tweet_still_fits_with_its_paragraph_breaks():
    """The blank lines cost characters too — the limit check must see them."""
    day = summarize(_claims(999, 0.74, 0.73))
    cume = summarize(_claims(999999, 0.82, 0.81, sessions=999))
    assert len(build_receipt_tweet(day, cume)) <= TWEET_MAX_LEN
