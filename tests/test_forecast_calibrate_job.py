"""Tests for the Layer-2 forecast calibration cron — update rule + bounds."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

import pytest


def _reload_module():
    for mod in list(sys.modules):
        if mod.startswith("src.jobs.forecast_calibrate") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.jobs import forecast_calibrate  # noqa: WPS433

    return forecast_calibrate


def _receipt(
    projected_low: float,
    projected_high: float,
    raw_low: float,
    raw_high: float,
    actual_low: float,
    actual_high: float,
    actual_close: float,
    raw_pin_hit: bool | None = None,
    day: date = date(2026, 6, 29),
    implied_move: float | None = None,
) -> dict:
    return {
        "date": day,
        "projected_low": projected_low,
        "projected_high": projected_high,
        "raw_projected_low": raw_low,
        "raw_projected_high": raw_high,
        "actual_low": actual_low,
        "actual_high": actual_high,
        "actual_close": actual_close,
        "raw_pin_hit": raw_pin_hit,
        "implied_move": implied_move,
        "receipt_ts": datetime(day.year, day.month, day.day, 20, 5, tzinfo=timezone.utc),
    }


def _neutral_state() -> dict:
    return {
        "symbol": "SPY",
        "band_width_mult": 1.0,
        "pin_tolerance_mult": 1.0,
        "upside_lean": 0.0,
        "downside_lean": 0.0,
        "vol_range_basis_mult": 1.0,
        "n_receipts_used": 0,
        "last_calibrated_ts": None,
    }


def test_cold_start_holds_neutral_when_receipts_below_threshold():
    mod = _reload_module()
    receipts = [
        _receipt(590, 610, 592, 608, 595, 605, 600) for _ in range(5)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["band_width_mult"] == 1.0
    assert updates["pin_tolerance_mult"] == 1.0
    assert updates["upside_lean"] == 0.0
    assert updates["downside_lean"] == 0.0
    assert updates["summary"]["action"] == "cold_start_hold"


def test_widens_band_when_coverage_below_target():
    """15 receipts, all broke both sides of the band → widen band_width_mult."""
    mod = _reload_module()
    receipts = [
        _receipt(590, 610, 592, 608, 585, 615, 600)  # actuals outside raw band
        for _ in range(15)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    # Coverage = 0/15 = 0.0; target = 0.9; error = 0.9 → widen 0.9 * 0.05 * 5 = 0.225.
    # Bounded to [0.7, 1.5] so ends at 1.225.
    assert updates["band_width_mult"] > 1.0
    assert updates["band_width_mult"] <= 1.50
    assert updates["summary"]["coverage"] == 0.0


def test_tightens_band_when_coverage_above_target():
    """15 receipts, all inside → tighten."""
    mod = _reload_module()
    receipts = [
        _receipt(590, 610, 590, 610, 595, 605, 600)  # actuals inside raw band
        for _ in range(15)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["band_width_mult"] < 1.0
    assert updates["band_width_mult"] >= 0.70


def test_upside_lean_widens_when_upside_breaks_more():
    """Upside breaks 10/15, downside 0/15 → widen upside, tighten downside."""
    mod = _reload_module()
    receipts = [
        _receipt(590, 610, 592, 608, 595, 615, 605)  # actual high breaks upside
        for _ in range(10)
    ] + [
        _receipt(590, 610, 592, 608, 595, 605, 600)  # inside
        for _ in range(5)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["upside_lean"] > 0.0
    assert updates["downside_lean"] < 0.0


def test_pin_tolerance_widens_when_hits_below_target():
    """Pin_hit_rate = 0/15 = 0; target 0.5 → widen tolerance."""
    mod = _reload_module()
    receipts = [
        _receipt(590, 610, 592, 608, 595, 605, 600, raw_pin_hit=False)
        for _ in range(15)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["pin_tolerance_mult"] > 1.0


def test_bounds_never_exceeded_even_on_persistent_signal():
    """A tenuous run of misses can't push scalars past their bounds."""
    mod = _reload_module()
    state = _neutral_state()
    # 100 consecutive rounds, each with maximally-bad coverage.
    receipts = [
        _receipt(590, 610, 592, 608, 500, 700, 600) for _ in range(15)
    ]
    for _ in range(100):
        updates = mod._compute_updates(receipts, state)
        state["band_width_mult"] = updates["band_width_mult"]
        state["pin_tolerance_mult"] = updates["pin_tolerance_mult"]
        state["upside_lean"] = updates["upside_lean"]
        state["downside_lean"] = updates["downside_lean"]
    lo, hi = mod.BOUNDS["band_width_mult"]
    assert lo <= state["band_width_mult"] <= hi
    lo, hi = mod.BOUNDS["pin_tolerance_mult"]
    assert lo <= state["pin_tolerance_mult"] <= hi


def test_learns_vol_basis_from_realized_median():
    """When the symbol's realized ranges cluster below the Parkinson expectation
    (the variance-risk premium), the cron drifts vol_range_basis_mult toward the
    median so a typical day re-centers at ratio 1.0 instead of reading
    'compression' forever."""
    mod = _reload_module()
    implied = 50.0
    # Each day realizes 0.85× a normal day's range → median raw ratio 0.85.
    day_range = 0.85 * mod.RANGE_OVER_SIGMA * implied
    receipts = [
        _receipt(
            590, 610, 592, 608,
            actual_low=600 - day_range / 2, actual_high=600 + day_range / 2,
            actual_close=600, implied_move=implied,
        )
        for _ in range(15)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    # Nudged down from 1.0 toward the 0.85 target, bounded, and reported.
    assert 0.85 <= updates["vol_range_basis_mult"] < 1.0
    lo, hi = mod.BOUNDS["vol_range_basis_mult"]
    assert lo <= updates["vol_range_basis_mult"] <= hi
    assert updates["summary"]["vol_basis_target"] == pytest.approx(0.85, abs=1e-3)


def test_vol_basis_converges_and_stays_bounded():
    """Repeated rounds converge the basis toward the realized median and never
    escape the bounds — the grading goalposts can drift but not run away."""
    mod = _reload_module()
    implied = 50.0
    day_range = 0.70 * mod.RANGE_OVER_SIGMA * implied  # persistent low-vol regime
    receipts = [
        _receipt(
            590, 610, 592, 608,
            actual_low=600 - day_range / 2, actual_high=600 + day_range / 2,
            actual_close=600, implied_move=implied,
        )
        for _ in range(15)
    ]
    state = _neutral_state()
    for _ in range(100):
        updates = mod._compute_updates(receipts, state)
        state["vol_range_basis_mult"] = updates["vol_range_basis_mult"]
    lo, hi = mod.BOUNDS["vol_range_basis_mult"]
    assert lo <= state["vol_range_basis_mult"] <= hi
    assert state["vol_range_basis_mult"] == pytest.approx(0.70, abs=1e-2)


def test_ignores_legacy_receipts_without_raw_fields():
    """Rows written under heuristic_v1 (no raw_* columns) are skipped so
    the calibration doesn't grade its own noise."""
    mod = _reload_module()
    receipts = [
        # 5 v1.2 rows
        _receipt(590, 610, 592, 608, 585, 615, 600)
        for _ in range(5)
    ] + [
        # 20 legacy rows (raw fields None)
        {
            "date": date(2026, 6, 20),
            "projected_low": 590, "projected_high": 610,
            "raw_projected_low": None, "raw_projected_high": None,
            "actual_low": 585, "actual_high": 615, "actual_close": 600,
            "raw_pin_hit": None,
            "receipt_ts": datetime(2026, 6, 20, 20, tzinfo=timezone.utc),
        }
        for _ in range(20)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    # Only 5 usable v1.2 rows → below MIN_RECEIPTS → cold-start hold.
    assert updates["summary"]["action"] == "cold_start_hold"
    assert updates["n_receipts_used"] == 5


@pytest.mark.asyncio
async def test_recalibrate_one_dry_run_does_not_write():
    mod = _reload_module()
    receipts = [
        _receipt(590, 610, 590, 610, 595, 605, 600, raw_pin_hit=True)
        for _ in range(15)
    ]

    class _FakeDB:
        upsert_called: list = []
        async def get_forecast_calibration(self, symbol):
            return _neutral_state()
        async def get_daily_forecast_history(self, symbol, limit):
            return receipts
        async def upsert_forecast_calibration(self, **kwargs):
            self.upsert_called.append(kwargs)
            return True

    db = _FakeDB()
    args = mod._parse_args(["--symbols", "SPY", "--dry-run"])
    result = await mod._recalibrate_one(db, "SPY", args.dry_run)
    assert result["symbol"] == "SPY"
    assert db.upsert_called == []


@pytest.mark.asyncio
async def test_recalibrate_swallows_db_errors():
    """DB layer completely broken → cron exits 0 with a warning."""
    mod = _reload_module()

    class _BrokenDB:
        async def connect(self):
            raise RuntimeError("pool down")
        async def disconnect(self):
            return None

    monkeypatched_writer = _BrokenDB()
    import src.jobs.forecast_calibrate as fc_mod

    orig = fc_mod.DatabaseManager
    fc_mod.DatabaseManager = lambda: monkeypatched_writer  # type: ignore[assignment]
    try:
        args = mod._parse_args(["--symbols", "SPY"])
        rc = await mod._run(args)
        assert rc == 0
    finally:
        fc_mod.DatabaseManager = orig  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# The coverage loop must be CLOSED — it steers on the committed band.
#
# It used to measure raw_projected_low/high, which compute_forecast snapshots
# BEFORE band_width_mult is applied. The raw band therefore does not move when
# the multiplier moves, so the error kept its sign forever and the multiplier
# walked to a bound and stayed there instead of converging. None of the tests
# above caught it, because in all of them the raw and committed bands agree.
# These deliberately disagree.
# ---------------------------------------------------------------------------


def _split_band_receipts(
    n: int, *, committed_contains: bool, raw_contains: bool
) -> list[dict]:
    """Receipts where the RAW and COMMITTED verdicts deliberately differ.

    Raw band is always [80, 120]; committed is the narrower [95, 105]. The
    day's range is placed to fall inside one and outside the other.
    """
    out = []
    for i in range(n):
        if committed_contains:
            lo, hi = 96.0, 104.0            # inside committed, so inside raw too
        elif raw_contains:
            lo, hi = 85.0, 115.0            # outside committed, inside raw
        else:
            lo, hi = 70.0, 130.0            # outside both
        out.append(
            _receipt(
                projected_low=95.0, projected_high=105.0,
                raw_low=80.0, raw_high=120.0,
                actual_low=lo, actual_high=hi, actual_close=100.0,
                day=date(2026, 6, 1 + (i % 28)),
            )
        )
    return out


def test_band_narrows_when_the_COMMITTED_band_over_covers():
    mod = _reload_module()
    receipts = _split_band_receipts(20, committed_contains=True, raw_contains=True)
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["summary"]["coverage"] == 1.0
    assert updates["band_width_mult"] < 1.0, "100% committed coverage must tighten the band"


def test_band_widens_when_the_COMMITTED_band_misses_even_though_RAW_holds():
    """The regression. Raw coverage is 100%, committed coverage is 0%.

    Steering on raw would TIGHTEN an already-too-narrow band. Steering on the
    committed band — the one that was published and graded — widens it.
    """
    mod = _reload_module()
    receipts = _split_band_receipts(20, committed_contains=False, raw_contains=True)
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["summary"]["coverage"] == 0.0, "committed band contained nothing"
    assert updates["summary"]["raw_coverage"] == 1.0, "raw band contained everything"
    assert updates["band_width_mult"] > 1.0, (
        "a band that missed every single day must widen; steering on raw_coverage "
        "would have narrowed it instead"
    )


def test_raw_coverage_is_reported_but_no_longer_steers():
    mod = _reload_module()
    same = mod._compute_updates(
        _split_band_receipts(20, committed_contains=True, raw_contains=True),
        _neutral_state(),
    )
    # Identical committed verdicts, opposite raw verdicts -> identical band.
    flipped = _split_band_receipts(20, committed_contains=True, raw_contains=True)
    for r in flipped:
        r["raw_projected_low"], r["raw_projected_high"] = 99.0, 101.0  # raw now misses
    other = mod._compute_updates(flipped, _neutral_state())
    assert other["summary"]["raw_coverage"] != same["summary"]["raw_coverage"]
    assert other["band_width_mult"] == same["band_width_mult"], (
        "raw coverage is a diagnostic; it must not move the multiplier"
    )


def test_target_is_the_middle_of_the_advertised_band():
    mod = _reload_module()
    assert mod.TARGET_COVERAGE == 0.85, "middle of the advertised 80-90% band"
    # The floor stays 0.70. Production pinned the CEILING (SPX 1.45, NDX 1.50),
    # so the floor was never the binding constraint; with the loop closed the
    # multiplier should settle near 1.0.
    assert mod.BOUNDS["band_width_mult"] == (0.70, 1.50)


def test_leans_follow_the_COMMITTED_band_not_the_raw_one():
    """Same open loop as coverage, in the lean terms.

    Raw band is symmetric and contains everything; the COMMITTED band is
    shifted so only the upside breaks. Steering on raw sees no imbalance and
    leaves the leans alone; steering on the committed band leans upside out.
    """
    mod = _reload_module()
    receipts = [
        _receipt(
            projected_low=90.0, projected_high=101.0,   # upside breaks, downside holds
            raw_low=50.0, raw_high=150.0,               # raw contains everything
            actual_low=95.0, actual_high=110.0, actual_close=100.0,
            day=date(2026, 6, 1 + (i % 28)),
        )
        for i in range(20)
    ]
    updates = mod._compute_updates(receipts, _neutral_state())
    assert updates["summary"]["up_break_rate"] == 1.0
    assert updates["summary"]["down_break_rate"] == 0.0
    assert updates["upside_lean"] > 0.0, "every day broke the upside; lean must widen it"
    assert updates["downside_lean"] < 0.0


def test_pin_tolerance_follows_the_committed_verdict():
    mod = _reload_module()
    hit = [
        _receipt(95.0, 105.0, 95.0, 105.0, 96.0, 104.0, 100.0,
                 raw_pin_hit=False, day=date(2026, 6, 1 + (i % 28)))
        for i in range(20)
    ]
    for r in hit:
        r["pin_hit"] = True          # committed says hit, raw says miss
    updates = mod._compute_updates(hit, _neutral_state())
    assert updates["summary"]["pin_hit_rate"] == 1.0, "must read the committed verdict"
    assert updates["pin_tolerance_mult"] < 1.0, "pinning every day means tolerance is too wide"


def test_pin_tolerance_falls_back_to_raw_when_committed_is_absent():
    mod = _reload_module()
    rows = [
        _receipt(95.0, 105.0, 95.0, 105.0, 96.0, 104.0, 100.0,
                 raw_pin_hit=True, day=date(2026, 6, 1 + (i % 28)))
        for i in range(20)
    ]  # no "pin_hit" key at all — pre-backfill rows
    updates = mod._compute_updates(rows, _neutral_state())
    assert updates["summary"]["pin_hit_rate"] == 1.0, "must degrade to raw_pin_hit, not to 0"
