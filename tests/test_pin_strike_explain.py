"""Tests for the Pin Strike explain tool (:mod:`src.tools.pin_strike_explain`).

The tool recomputes a historical pin so support can answer "why did this strike
score that way?" with the per-candidate table ``gex_summary`` does not persist.
Its output is meant to be quoted to a member, which makes two pieces of pure
logic safety-critical and worth pinning here:

* ``_check_against_persisted`` — the guard that catches a recompute which no
  longer reproduces the row that actually shipped. A silent false "matches"
  would send a member numbers that were never on their screen, so every drift
  shape is covered.
* ``_parse_at`` — ``ET`` is a **pytz** zone, where ``replace(tzinfo=ET)``
  attaches the 1883 LMT offset (-04:56) instead of the real one. That mistake
  is invisible in the output and would shift the frame lookup by four minutes,
  so the correct offset is asserted directly.

The DB loaders are not covered: they are thin wrappers around the engine's own
snapshot SQL and need a populated database to say anything.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.analytics.pin_strike import PinCandidate, PinStrikeResult, STATUS_ACTIVE
from src.tools.pin_strike_explain import (
    _bucket,
    _check_against_persisted,
    _fmt_money,
    _parse_at,
)


# --------------------------------------------------------------------------- #
# Timestamp parsing.
# --------------------------------------------------------------------------- #
def test_et_parse_uses_the_real_offset_not_pytz_lmt():
    """``ET`` is pytz: localize(), never replace(tzinfo=ET).

    ``replace`` would yield -04:56 (US/Eastern's 1883 local mean time), quietly
    moving the frame lookup nearly five minutes and returning the wrong minute's
    pin. Sep 3 is EDT, so the only correct answer is -04:00.
    """
    t = _parse_at("2026-09-03 14:30", as_utc=False)
    assert t.utcoffset().total_seconds() == -4 * 3600


def test_et_parse_tracks_dst():
    """January is EST (-05:00); the same call in September is EDT (-04:00)."""
    winter = _parse_at("2026-01-14 14:30", as_utc=False)
    summer = _parse_at("2026-07-14 14:30", as_utc=False)
    assert winter.utcoffset().total_seconds() == -5 * 3600
    assert summer.utcoffset().total_seconds() == -4 * 3600


def test_utc_flag_reads_the_instant_as_utc():
    assert _parse_at("2026-09-03 18:30", as_utc=True) == datetime(
        2026, 9, 3, 18, 30, tzinfo=timezone.utc
    )


@pytest.mark.parametrize(
    "raw",
    ["2026-09-03 14:30:15", "2026-09-03 14:30", "2026-09-03T14:30:15", "2026-09-03T14:30"],
)
def test_accepted_timestamp_shapes(raw):
    assert _parse_at(raw, as_utc=False) is not None


def test_unparseable_timestamp_is_rejected():
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_at("last Tuesday", as_utc=False)


# --------------------------------------------------------------------------- #
# Presentation.
# --------------------------------------------------------------------------- #
def test_buckets_match_the_ui_thresholds():
    """Mirrors core/pinStrike.ts — >= 0.50 Strong, >= 0.33 Moderate, else Weak.

    The tool exists to explain a label the member saw, so a bucket that
    disagreed with the one rendered on their screen would defeat the purpose.
    """
    assert _bucket(0.50) == "Strong"
    assert _bucket(0.4999) == "Moderate"
    assert _bucket(0.33) == "Moderate"
    assert _bucket(0.3299) == "Weak"
    assert _bucket(0.0) == "Weak"
    assert _bucket(None) == "—"


def test_money_formatting_uses_the_units_the_dashboard_does():
    assert _fmt_money(949.2e6) == "949.2m"
    assert _fmt_money(-141.8e6) == "-141.8m"
    assert _fmt_money(1.2086e9) == "1.2b"
    assert _fmt_money(-3.1e3) == "-3.1k"
    assert _fmt_money(12.0) == "12.0"


# --------------------------------------------------------------------------- #
# The recompute guard.
# --------------------------------------------------------------------------- #
def _active(strike=29500.0, score=8.0e10, confidence=0.317) -> PinStrikeResult:
    return PinStrikeResult(
        pin_strike=strike,
        pin_score=score,
        pin_confidence=confidence,
        local_gex=1.2e11,
        restoring_gex=1.2e11,
        reachability=0.5,
        status=STATUS_ACTIVE,
        candidates=[PinCandidate(strike, 1.2e11, 1.2e11, 0.5, score)],
    )


def _frame(**over):
    base = {
        "timestamp": _parse_at("2026-09-03 14:30", as_utc=False),
        "pin_strike": 29500.0,
        "pin_score": 8.0e10,
        "pin_confidence": 0.317,
        "pin_strike_reason": None,
    }
    base.update(over)
    return base


def test_faithful_recompute_reports_no_drift():
    ok, notes = _check_against_persisted(_active(), _frame())
    assert ok is True
    assert notes == []


def test_a_different_strike_is_drift():
    """The strike is a discrete choice: off by one strike is a different pin."""
    ok, notes = _check_against_persisted(_active(strike=29490.0), _frame())
    assert ok is False
    assert any("strike differs" in n for n in notes)


def test_score_round_tripping_is_tolerated_but_a_real_gap_is_not():
    """NUMERIC/float round-tripping moves the score a hair; 2x does not."""
    ok, _ = _check_against_persisted(_active(score=8.0e10 * 1.001), _frame())
    assert ok is True

    ok, notes = _check_against_persisted(_active(score=1.6e11), _frame())
    assert ok is False
    assert any("score differs" in n for n in notes)


def test_confidence_drift_is_caught():
    ok, notes = _check_against_persisted(_active(confidence=0.62), _frame())
    assert ok is False
    assert any("confidence differs" in n for n in notes)


def test_recomputing_a_pin_where_none_shipped_is_drift():
    """The shape that matters most: inventing a level the member never saw."""
    ok, notes = _check_against_persisted(
        _active(),
        _frame(
            pin_strike=None,
            pin_score=None,
            pin_confidence=None,
            pin_strike_reason="NO_POSITIVE_RESTORING_GAMMA",
        ),
    )
    assert ok is False
    assert any("active-state differs" in n for n in notes)


def test_recomputing_no_pin_where_one_shipped_is_drift():
    inactive = PinStrikeResult(reason="NO_POSITIVE_RESTORING_GAMMA")
    ok, notes = _check_against_persisted(inactive, _frame())
    assert ok is False
    assert any("active-state differs" in n for n in notes)


def test_matching_inactive_pins_agree_on_the_reason():
    inactive = PinStrikeResult(reason="INSUFFICIENT_IV_DATA")
    frame = _frame(
        pin_strike=None,
        pin_score=None,
        pin_confidence=None,
        pin_strike_reason="INSUFFICIENT_IV_DATA",
    )
    ok, notes = _check_against_persisted(inactive, frame)
    assert ok is True
    assert notes == []


def test_a_different_inactive_reason_is_drift():
    inactive = PinStrikeResult(reason="EXPIRED")
    frame = _frame(
        pin_strike=None,
        pin_score=None,
        pin_confidence=None,
        pin_strike_reason="NO_0DTE_EXPIRATION",
    )
    ok, notes = _check_against_persisted(inactive, frame)
    assert ok is False
    assert any("reason differs" in n for n in notes)
