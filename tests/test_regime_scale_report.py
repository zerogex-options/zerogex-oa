"""Tests for the Gamma Regime scale diagnostic (:mod:`src.tools.regime_scale_report`).

The tool exists because two successive arguments about which denominator the
QUIET cut should use turned out to be wrong when measured.  So the thing worth
testing is that it would actually have CAUGHT them: that its shape statistics
separate a well-behaved chain from a pathological one, and that its graded
QUIET rates reproduce the production symptom.
"""

from __future__ import annotations

import asyncio
import math
import random
from typing import Any, Dict, List

import pytest

from src.analytics import regime_shift as rs
from src.tools import regime_scale_report as tool


class FakeDb:
    """Serves stored rows from a {symbol -> [(lean, stability)]} map."""

    def __init__(self, chains: Dict[str, List[tuple]]):
        self.chains = chains

    async def get_regime_sessions(self, symbol, limit=60, before_date=None):
        rows = self.chains.get(symbol.upper(), [])
        return [{"lean_raw": a, "stability_raw": b} for a, b in rows][:limit]


def _chain(rng, n, tail_p=0.0, tail_x=1.0, drift=0.0):
    out = []
    for _ in range(n):
        m = tail_x if rng.random() < tail_p else 1.0
        out.append((rng.gauss(0, 1) * m + drift, rng.gauss(0, 1) * m + drift))
    return out


# --------------------------------------------------------------------------- #
# The median-absolute candidate
# --------------------------------------------------------------------------- #
class TestMedianAbsScale:
    def test_agrees_with_the_others_on_gaussian_data(self):
        rng = random.Random(5)
        sample = [rng.gauss(0, 1) for _ in range(4000)]
        assert tool.median_abs_scale(sample) == pytest.approx(rs.stdev(sample), rel=0.05)

    def test_is_unmoved_by_how_large_the_largest_sessions_are(self):
        """The property that separates it from a mean-based scale: a median
        does not care how far out the tail goes, only how many are in it."""
        ordinary = [1.0, -1.0, 1.2, -1.2, 0.8, -0.8, 1.1, -1.1, 0.9, -0.9]
        with_tail = ordinary + [500.0, -500.0]

        assert tool.median_abs_scale(with_tail) == pytest.approx(
            tool.median_abs_scale(ordinary), rel=0.15
        )
        # ...where both of the shipped candidates move a lot.
        assert rs.stdev(with_tail) > 30 * rs.stdev(ordinary)
        assert rs.robust_scale(with_tail) > 5 * rs.robust_scale(ordinary)

    def test_has_no_scale_when_nothing_moved(self):
        assert tool.median_abs_scale([]) is None
        assert tool.median_abs_scale([1.0]) is None
        assert tool.median_abs_scale([0.0, 0.0, 0.0, 0.0]) is None


# --------------------------------------------------------------------------- #
# The shape statistics
# --------------------------------------------------------------------------- #
class TestDescribe:
    def test_tail_weight_is_the_gaussian_constant_on_gaussian_data(self):
        rng = random.Random(5)
        shape = tool.describe([rng.gauss(0, 1) for _ in range(4000)])
        assert shape["tail_weight"] == pytest.approx(math.sqrt(math.pi / 2), rel=0.03)
        assert shape["skew"] == pytest.approx(1.18, rel=0.05)

    def test_a_heavy_tail_lifts_both_tail_weight_and_skew(self):
        rng = random.Random(3)
        heavy = [v for v, _ in _chain(rng, 400, tail_p=0.10, tail_x=9.0)]
        shape = tool.describe(heavy)
        assert shape["tail_weight"] > 1.8
        # The one that matters: the typical session is far below the average
        # one, so a MEAN-based scale still reports it as nothing happening.
        assert shape["skew"] > 1.8

    def test_drift_share_separates_a_persistent_shift_from_variation(self):
        """What zero-centring charges for and mean-centring hides."""
        rng = random.Random(9)
        steady = [v for v, _ in _chain(rng, 300, drift=5.0)]
        assert tool.describe(steady)["drift_share"] > 0.8
        noisy = [v for v, _ in _chain(rng, 300)]
        assert tool.describe(noisy)["drift_share"] < 0.2


# --------------------------------------------------------------------------- #
# The graded QUIET rates
# --------------------------------------------------------------------------- #
class TestReport:
    def test_reproduces_the_production_symptom(self):
        """Two chains, same index, differing only in tail: the near-Gaussian
        one is unaffected by the choice of denominator and the heavy one is
        transformed by it.  That asymmetry IS the bug this tool exists for."""
        rng = random.Random(3)
        db = FakeDb(
            {
                "SPY": _chain(rng, 42),
                "SPX": _chain(rng, 42, tail_p=0.10, tail_x=9.0),
            }
        )

        data = asyncio.run(tool.report(db, ["SPY", "SPX"], 60))
        spy = {k: v["quiet_rate"] for k, v in data["SPY"]["candidates"].items()}
        spx = {k: v["quiet_rate"] for k, v in data["SPX"]["candidates"].items()}

        # The well-behaved chain does not care which denominator is used.
        assert max(spy.values()) - min(spy.values()) < 0.05
        # The heavy one cares enormously, and only the median-based scale
        # brings it back into line with its own index's other listing.
        assert spx["stdev"] > 0.55
        assert spx["mean_abs"] < spx["stdev"]
        assert abs(spx["median_abs"] - spy["median_abs"]) < 0.12

    def test_says_so_rather_than_guessing_on_a_thin_history(self):
        db = FakeDb({"NEW": _chain(random.Random(1), 4)})
        data = asyncio.run(tool.report(db, ["NEW"], 60))
        assert data["NEW"]["sessions"] == 4
        assert "not enough" in data["NEW"]["note"]

    def test_renders_without_a_usable_symbol(self):
        db = FakeDb({"NEW": []})
        out = tool.render(asyncio.run(tool.report(db, ["NEW"], 60)))
        assert "NEW" in out
