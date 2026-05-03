"""History-aware tests for PR-12 history loader.

Covers SignalSnapshot helpers (history_by_day, daily_max_abs,
daily_signed_max) plus the three pattern paths that consume history:
squeeze_breakout 2-day sustained, vanna_charm_glide 2-day sustained
sign, skew_inversion_reversal 20-day mean target + new-low predicate.
"""

from datetime import datetime, timezone
from typing import Optional

import pytz

from src.signals.components.base import MarketContext
from src.signals.playbook.context import PlaybookContext, SignalSnapshot
from src.signals.playbook.patterns.squeeze_breakout import PATTERN as SQB
from src.signals.playbook.patterns.vanna_charm_glide import PATTERN as VCG
from src.signals.playbook.patterns.skew_inversion_reversal import PATTERN as SIR

_ET = pytz.timezone("America/New_York")


def _et_ts(year: int, month: int, day: int, hour: int = 12, minute: int = 0) -> datetime:
    """ET-localized timestamp helper."""
    return _ET.localize(datetime(year, month, day, hour, minute))


# ----------------------------------------------------------------------
# SignalSnapshot helpers
# ----------------------------------------------------------------------


def test_history_by_day_groups_by_et_date():
    snap = SignalSnapshot(
        name="x",
        score=0.0,
        clamped_score=0.0,
        score_history=[
            (_et_ts(2026, 5, 1, 10, 0), 0.10),
            (_et_ts(2026, 5, 1, 14, 0), 0.30),
            (_et_ts(2026, 5, 2, 11, 0), -0.20),
        ],
    )
    by_day = snap.history_by_day()
    assert by_day["2026-05-01"] == [0.10, 0.30]
    assert by_day["2026-05-02"] == [-0.20]


def test_daily_max_abs_and_signed_max():
    snap = SignalSnapshot(
        name="x",
        score=0.0,
        clamped_score=0.0,
        score_history=[
            (_et_ts(2026, 5, 1, 10, 0), 0.50),
            (_et_ts(2026, 5, 1, 14, 0), -0.80),  # most extreme on day 1
            (_et_ts(2026, 5, 2, 11, 0), 0.30),
        ],
    )
    max_abs = snap.daily_max_abs()
    assert max_abs == [("2026-05-01", 0.80), ("2026-05-02", 0.30)]
    signed = snap.daily_signed_max()
    assert signed == [("2026-05-01", -0.80), ("2026-05-02", 0.30)]


# ----------------------------------------------------------------------
# squeeze_breakout 2-day-sustained gate
# ----------------------------------------------------------------------


def _sqb_ctx_with_history(history: list[tuple[datetime, float]]):
    from tests.test_playbook_squeeze_breakout import _ctx as base_ctx

    ctx = base_ctx()
    snap = ctx.signal("squeeze_setup")
    snap.score_history = history
    return ctx


def test_squeeze_breakout_blocks_when_only_one_day_sustained():
    history = [
        # Day 1 only — barely below threshold every other day
        (_et_ts(2026, 4, 30, 10, 0), 0.40),
        (_et_ts(2026, 4, 30, 11, 0), 0.45),
        # Day 0 (today) — inherits current trigger via current snapshot
        (_et_ts(2026, 5, 1, 10, 0), 0.05),
    ]
    ctx = _sqb_ctx_with_history(history)
    assert SQB.match(ctx) is None


def test_squeeze_breakout_passes_when_two_days_sustained():
    history = [
        (_et_ts(2026, 4, 30, 10, 0), 0.30),
        (_et_ts(2026, 4, 30, 14, 0), 0.40),
        (_et_ts(2026, 5, 1, 10, 0), 0.30),
    ]
    ctx = _sqb_ctx_with_history(history)
    card = SQB.match(ctx)
    assert card is not None
    assert card.pattern == "squeeze_breakout"


def test_squeeze_breakout_no_history_falls_back_to_current_trigger():
    """Empty history must NOT block (preserves PR-8 behavior)."""
    from tests.test_playbook_squeeze_breakout import _ctx as base_ctx

    ctx = base_ctx()
    snap = ctx.signal("squeeze_setup")
    snap.score_history = []  # explicit empty
    card = SQB.match(ctx)
    assert card is not None


# ----------------------------------------------------------------------
# vanna_charm_glide 2-day-sustained, same-sign gate
# ----------------------------------------------------------------------


def _vcg_ctx_with_history(history: list[tuple[datetime, float]]):
    from tests.test_playbook_vanna_charm_glide import _ctx as base_ctx

    ctx = base_ctx()  # default: Wed, vcf=+50, etc.
    snap = ctx.signal("vanna_charm_flow")
    snap.score_history = history
    return ctx


def test_vcg_blocks_when_only_one_day_same_sign():
    history = [
        # Yesterday: opposite-sign extreme.  Doesn't count for bullish drift.
        (_et_ts(2026, 4, 28, 11, 0), -0.55),
        (_et_ts(2026, 4, 29, 11, 0), 0.50),
    ]
    ctx = _vcg_ctx_with_history(history)
    assert VCG.match(ctx) is None


def test_vcg_passes_when_two_days_same_sign_above_threshold():
    history = [
        (_et_ts(2026, 4, 28, 11, 0), 0.60),
        (_et_ts(2026, 4, 29, 11, 0), 0.50),
    ]
    ctx = _vcg_ctx_with_history(history)
    card = VCG.match(ctx)
    assert card is not None
    assert card.pattern == "vanna_charm_glide"


def test_vcg_no_history_falls_back_to_current_trigger():
    from tests.test_playbook_vanna_charm_glide import _ctx as base_ctx

    ctx = base_ctx()
    snap = ctx.signal("vanna_charm_flow")
    snap.score_history = []
    card = VCG.match(ctx)
    assert card is not None


# ----------------------------------------------------------------------
# skew_inversion_reversal 20-day-mean target + new-low predicate
# ----------------------------------------------------------------------


def _sir_ctx_with_history(history: list[tuple[datetime, float]]):
    from tests.test_playbook_skew_inversion_reversal import _ctx as base_ctx

    ctx = base_ctx()
    snap = ctx.signal("skew_delta")
    snap.score_history = history
    return ctx


def test_sir_target_uses_history_mean_when_available():
    """Deeper sustained fear (more negative mean) → larger target offset."""
    shallow_history = [
        (_et_ts(2026, 4, 28, 11, 0), -0.55),
        (_et_ts(2026, 4, 29, 11, 0), -0.55),
        (_et_ts(2026, 4, 30, 11, 0), -0.55),
        (_et_ts(2026, 5, 1, 11, 0), -0.65),
    ]
    deep_history = [
        (_et_ts(2026, 4, 28, 11, 0), -0.85),
        (_et_ts(2026, 4, 29, 11, 0), -0.85),
        (_et_ts(2026, 4, 30, 11, 0), -0.85),
        (_et_ts(2026, 5, 1, 11, 0), -0.65),
    ]
    shallow = SIR.match(_sir_ctx_with_history(shallow_history))
    deep = SIR.match(_sir_ctx_with_history(deep_history))
    assert shallow is not None and deep is not None
    assert deep.target.ref_price > shallow.target.ref_price
    # Both Cards should report the history source.
    assert shallow.context["skew_intensity_source"] == "history_20d_mean"
    assert deep.context["skew_intensity_source"] == "history_20d_mean"


def test_sir_target_falls_back_to_current_when_no_history():
    """Empty history → uses current skew magnitude (PR-9 behavior)."""
    from tests.test_playbook_skew_inversion_reversal import _ctx as base_ctx

    ctx = base_ctx()
    snap = ctx.signal("skew_delta")
    snap.score_history = []
    card = SIR.match(ctx)
    assert card is not None
    assert card.context["skew_intensity_source"] == "current_skew"


def test_sir_new_20d_low_predicate_true_when_current_is_lowest():
    history = [
        (_et_ts(2026, 4, 28, 11, 0), -0.50),
        (_et_ts(2026, 4, 29, 11, 0), -0.55),
        (_et_ts(2026, 4, 30, 11, 0), -0.60),
        (_et_ts(2026, 5, 1, 11, 0), -0.85),  # current — new low
    ]
    ctx = _sir_ctx_with_history(history)
    # Override current snapshot's clamped_score to match the latest history.
    ctx.signal("skew_delta").clamped_score = -0.85
    card = SIR.match(ctx)
    assert card is not None
    assert card.context["skew_is_new_20d_low"] is True


def test_sir_new_20d_low_predicate_false_when_current_above_prior_min():
    history = [
        (_et_ts(2026, 4, 28, 11, 0), -0.85),  # prior min
        (_et_ts(2026, 4, 29, 11, 0), -0.70),
        (_et_ts(2026, 5, 1, 11, 0), -0.65),
    ]
    ctx = _sir_ctx_with_history(history)
    ctx.signal("skew_delta").clamped_score = -0.65
    card = SIR.match(ctx)
    assert card is not None
    assert card.context["skew_is_new_20d_low"] is False
