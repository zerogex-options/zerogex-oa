"""Tests for the read-only gamma-flip threshold re-validation tool."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from src.signals.components.base import MarketContext
from src.signals.components.flip_distance import (
    FlipDistanceComponent,
    _FLIP_FALLBACK_PCT,
)
from src.tools.gamma_flip_revalidation import (
    EraStats,
    MIN_ERA_SAMPLES,
    _resolve_deploy_cutoff,
    _subscore,
    _summarize_era,
    _verdict,
    analyze,
)

_CUTOFF = _resolve_deploy_cutoff("2026-05-16")  # 2026-05-16 00:00 ET == 04:00Z


# --- _subscore is the live FlipDistanceComponent core -------------------------


def _ctx(flip_distance: float) -> MarketContext:
    return MarketContext(
        timestamp=datetime(2026, 5, 18, 18, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=-1.0e8,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[],  # empty -> component uses fallback saturation
        iv_rank=None,
        extra={"flip_distance": flip_distance},
    )


@pytest.mark.parametrize("fd", [0.0, 0.002, -0.006, 0.02, 0.05, -0.2])
def test_subscore_matches_flip_distance_component_fallback_path(fd):
    """Pin the reproduced score to the real component on its
    fallback-saturation path (empty recent_closes -> _FLIP_FALLBACK_PCT)."""
    expected = FlipDistanceComponent().compute(_ctx(fd))
    assert _subscore(fd, _FLIP_FALLBACK_PCT) == pytest.approx(expected)


def test_subscore_clamps_and_handles_nonpositive_sat():
    assert _subscore(0.0, 0.02) == 1.0
    assert _subscore(1.0, 0.02) == -1.0  # |fd| huge -> clamp floor
    assert _subscore(-1.0, 0.02) == -1.0  # sign-independent
    assert _subscore(0.001, 0.0) == 0.0  # non-positive saturation


# --- fake-cursor harness (repo record-the-SQL style) --------------------------


class _RoCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _RoConn:
    def __init__(self, rows):
        self._cur = _RoCursor(rows)

    def cursor(self):
        return self._cur

    def rollback(self):
        pass


def _ts(day, hour=15):
    return datetime(2026, 5, day, hour, 0, tzinfo=timezone.utc)


def test_analyze_splits_at_deploy_boundary_and_drops_nulls():
    # 35 pre (2026-05-14, before cutoff) + 32 post (2026-05-18) + 1 NULL.
    rows = (
        [(_ts(14), 0.001) for _ in range(35)]
        + [(_ts(18), 0.02) for _ in range(32)]
        + [(_ts(18), None)]  # NULL flip_distance must be dropped
    )
    conn = _RoConn(rows)
    pre, post, total = analyze(
        conn,
        "SPY",
        window_days=30,
        deploy_cutoff=_CUTOFF,
        flip_min=0.6,
        not_near_pct=0.006,
    )
    assert total == 67  # NULL row excluded by _fetch_flip_distances
    assert pre is not None and pre.n == 35
    assert post is not None and post.n == 32
    # Read-only contract: only SELECT was ever issued.
    for sql, _ in conn._cur.executed:
        u = " ".join(sql.split()).upper()
        assert u.startswith("SELECT")
        assert not any(w in u for w in ("INSERT", "UPDATE", "DELETE", "UPSERT"))


def test_analyze_returns_none_for_thin_era():
    rows = [(_ts(14), 0.001) for _ in range(MIN_ERA_SAMPLES - 1)] + [
        (_ts(18), 0.02) for _ in range(MIN_ERA_SAMPLES)
    ]
    pre, post, _ = analyze(
        _RoConn(rows),
        "SPY",
        window_days=30,
        deploy_cutoff=_CUTOFF,
        flip_min=0.6,
        not_near_pct=0.006,
    )
    assert pre is None  # below MIN_ERA_SAMPLES
    assert post is not None


# --- distribution + firing-rate math ------------------------------------------


def test_summarize_era_percentiles_and_not_near_share():
    abs_fd = np.array([0.001] * 30 + [0.10] * 10, dtype=float)  # n=40
    s = _summarize_era("x", abs_fd, flip_min=0.6, not_near_pct=0.006)
    assert s is not None and s.n == 40
    assert s.p50 == pytest.approx(0.001)
    assert s.p95 == pytest.approx(0.10)
    # 10/40 are >= 0.006.
    assert s.pct_not_near == pytest.approx(0.25)
    # At fallback sat 0.02: 0.001 -> 0.95 (>=0.6), 0.10 -> clamp -1 (<0.6).
    assert s.fire_at_fallback_sat == pytest.approx(0.75)


def test_firing_rate_is_monotonic_in_saturation():
    # 0.001 fires at every sat; 0.006 fires at fallback/max but not min;
    # 0.10 never fires.
    abs_fd = np.array([0.001] * 10 + [0.006] * 10 + [0.10] * 10, dtype=float)
    s = _summarize_era("x", abs_fd, flip_min=0.6, not_near_pct=0.006)
    assert s is not None
    assert s.fire_at_min_sat <= s.fire_at_fallback_sat <= s.fire_at_max_sat
    assert s.fire_at_min_sat == pytest.approx(10 / 30)
    assert s.fire_at_fallback_sat == pytest.approx(20 / 30)


# --- verdict heuristics -------------------------------------------------------


def _era(p50, fire_fb, *, era="e", n=100):
    return EraStats(
        era=era,
        n=n,
        p05=0.0,
        p25=0.0,
        p50=p50,
        p75=0.0,
        p95=0.0,
        pct_not_near=0.0,
        fire_at_min_sat=fire_fb,
        fire_at_fallback_sat=fire_fb,
        fire_at_max_sat=fire_fb,
    )


def test_verdict_insufficient_post():
    assert "INSUFFICIENT POST-DEPLOY DATA" in _verdict(_era(0.01, 0.5), None)


def test_verdict_no_pre_baseline():
    assert "NO PRE-DEPLOY BASELINE" in _verdict(None, _era(0.01, 0.5))


def test_verdict_no_material_shift():
    msg = _verdict(_era(0.010, 0.50), _era(0.011, 0.54))
    assert "NO MATERIAL SHIFT" in msg


def test_verdict_material_on_fire_delta():
    msg = _verdict(_era(0.010, 0.50), _era(0.011, 0.72))  # +22pp
    assert "MATERIAL SHIFT" in msg
    assert "Do NOT auto-adjust" in msg


def test_verdict_material_on_p50_ratio():
    msg = _verdict(_era(0.010, 0.50), _era(0.035, 0.52))  # x3.5 median
    assert "MATERIAL SHIFT" in msg
