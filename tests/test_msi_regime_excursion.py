"""The MSI-versus-realized-excursion study.

Three things need pinning, because the study's conclusion rests on them.

**The bands under test must be the shipped bands.** If ``bands.py`` and
``ScoringEngine._regime_label`` ever drift apart, the study silently measures
something the product does not show anyone. That is asserted against the
production function directly, boundary by boundary.

**The excursion arithmetic** is checked against a hand-built price path where
the right answer can be worked out on paper, including the rule that the entry
bar is never part of its own forward window.

**The statistics** are checked for the properties that keep a null honest: the
block bootstrap must not reject a true null just because the rows are
autocorrelated, and it must still find a real difference.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta

import pytest

from research.msi_regime_excursion import stats as st
from research.msi_regime_excursion.bands import BAND_KEYS, BANDS, band_for
from research.msi_regime_excursion.decompose import (
    AXIS,
    read_components,
    reconstruct,
    variant_scores,
)
from research.msi_regime_excursion.excursion import (
    ET,
    REST_OF_SESSION,
    Bar,
    BarSeries,
    compute_excursion,
)


# ---------------------------------------------------------------------------
# Bands
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "score",
    [0.0, 1.0, 19.999, 20.0, 20.001, 39.999, 40.0, 40.001,
     69.999, 70.0, 70.001, 99.999, 100.0],
)
def test_bands_match_the_production_labeller_exactly(score):
    """The study must band readings the way the engine that wrote them did."""
    from src.signals.scoring_engine import ScoringEngine

    assert band_for(score) == ScoringEngine._regime_label(score)


def test_band_keys_run_weakest_to_strongest():
    assert BAND_KEYS == (
        "high_risk_reversal", "chop_range", "controlled_trend", "trend_expansion"
    )
    lows = [b.lo for b in BANDS]
    assert lows == sorted(lows)


def test_band_copy_matches_the_frontend_strings():
    """These are the customer-facing promises the study scores. Pin them."""
    by_key = {b.key: b.copy for b in BANDS}
    assert by_key["trend_expansion"] == (
        "Strong directional regime — favor trades in the prevailing bias."
    )
    assert by_key["chop_range"] == "Range-bound — fade extremes, avoid trend trades."
    assert by_key["high_risk_reversal"] == (
        "Mean-reversion only — extreme move risk elevated."
    )


def test_band_for_rejects_junk():
    assert band_for(None) is None
    assert band_for(float("nan")) is None
    assert band_for(-1.0) is None
    assert band_for("not a number") is None


# ---------------------------------------------------------------------------
# Excursion arithmetic
# ---------------------------------------------------------------------------

def _series(specs, start=None):
    """specs: list of (high, low, close). Entry bar is specs[0]."""
    base = start or datetime(2026, 6, 1, 9, 30, tzinfo=ET)
    bars = [
        Bar(base + timedelta(minutes=i), close, high, low, close)
        for i, (high, low, close) in enumerate(specs)
    ]
    return BarSeries(bars), base


def test_excursion_is_arithmetic_on_a_hand_built_path():
    # entry 100; over the next 5 minutes: high 102, low 99, close 101.
    series, base = _series([
        (100, 100, 100),          # entry bar -- must NOT count
        (101, 99, 100.5),
        (102, 100, 101.0),
        (101.5, 100.5, 101.0),
        (101.2, 100.2, 100.8),
        (101.0, 100.0, 101.0),
    ])
    exc = compute_excursion(series, base, horizons=[5], include_rest_of_session=False)
    assert exc.entry == 100
    assert exc.bars_in_window[5] == 5
    assert exc.max_up_pts[5] == pytest.approx(2.0)      # 102 - 100
    assert exc.max_down_pts[5] == pytest.approx(1.0)    # 100 - 99
    assert exc.max_up_bps[5] == pytest.approx(200.0)
    assert exc.max_down_bps[5] == pytest.approx(100.0)
    assert exc.range_bps[5] == pytest.approx(300.0)     # (102 - 99) / 100
    assert exc.ret_bps[5] == pytest.approx(100.0)       # close 101
    assert exc.abs_ret_bps[5] == pytest.approx(100.0)


def test_the_entry_bar_is_never_part_of_its_own_window():
    """An extreme on the entry bar must not be scored as forward excursion."""
    series, base = _series([
        (500, 1, 100),            # a wild entry bar
        (100.5, 99.5, 100.0),
        (100.5, 99.5, 100.0),
    ])
    exc = compute_excursion(series, base, horizons=[2], include_rest_of_session=False)
    # Only the two following bars count: up 0.5, down 0.5.
    assert exc.max_up_pts[2] == pytest.approx(0.5)
    assert exc.max_down_pts[2] == pytest.approx(0.5)


def test_excursions_floor_at_zero_rather_than_going_negative():
    """Price that only ever falls has zero UPWARD excursion, not a negative one."""
    series, base = _series([
        (100, 100, 100),
        (99.0, 98.0, 98.5),
        (98.5, 97.0, 97.5),
    ])
    exc = compute_excursion(series, base, horizons=[2], include_rest_of_session=False)
    assert exc.max_up_pts[2] == 0.0
    assert exc.max_down_pts[2] == pytest.approx(3.0)
    assert exc.ret_bps[2] < 0


def test_bias_is_read_backwards_and_flips_mfe_and_mae():
    """MFE/MAE are taken against the prevailing bias, which is prior-only."""
    # 40 minutes of downtrend, then the reading, then a rally.
    bars = []
    base = datetime(2026, 6, 1, 9, 0, tzinfo=ET)
    price = 110.0
    for i in range(40):
        bars.append(Bar(base + timedelta(minutes=i), price, price, price, price))
        price -= 0.25
    entry_ts = base + timedelta(minutes=40)
    entry = price
    bars.append(Bar(entry_ts, entry, entry, entry, entry))
    for i in range(1, 6):
        p = entry + i
        bars.append(Bar(entry_ts + timedelta(minutes=i), p, p, p - 0.5, p))
    series = BarSeries(bars)

    exc = compute_excursion(series, entry_ts, horizons=[5], include_rest_of_session=False)
    assert exc.bias == -1                     # the prior 30 minutes fell
    # Bias is short, so the FAVOURABLE direction is down and the market rallied:
    # mfe tracks the downside excursion, mae the upside one.
    assert exc.mfe_bps[5] == exc.max_down_bps[5]
    assert exc.mae_bps[5] == exc.max_up_bps[5]
    assert exc.mae_bps[5] > exc.mfe_bps[5]    # the rally went against the bias


def test_flat_prior_path_leaves_mfe_and_mae_undefined():
    series, base = _series([(100, 100, 100)] * 40)
    exc = compute_excursion(series, base + timedelta(minutes=35),
                            horizons=[3], include_rest_of_session=False)
    assert exc.bias == 0
    assert exc.mfe_bps[3] is None
    assert exc.mae_bps[3] is None


def test_a_window_running_past_the_archive_is_dropped_not_truncated():
    """Otherwise the tail of every extract enters as an artificially small move."""
    series, base = _series([(100, 100, 100)] * 4)
    exc = compute_excursion(series, base, horizons=[60], include_rest_of_session=False)
    assert exc.max_up_bps[60] is None
    assert exc.bars_in_window[60] == 0


def test_rest_of_session_stops_at_the_close_and_is_empty_after_it():
    base = datetime(2026, 6, 1, 15, 55, tzinfo=ET)
    bars = [
        Bar(base + timedelta(minutes=i), 100 + i, 100 + i, 100, 100 + i)
        for i in range(10)      # 15:55 .. 16:04 ET; only through 16:00 counts
    ]
    series = BarSeries(bars)
    exc = compute_excursion(series, base, horizons=[1])
    # 15:56, 57, 58, 59, 16:00 -> five bars.
    assert exc.bars_in_window[REST_OF_SESSION] == 5
    # A reading at the last in-session bar has no forward path left.
    late = compute_excursion(series, base + timedelta(minutes=5), horizons=[1])
    assert late.bars_in_window[REST_OF_SESSION] == 0
    assert late.range_bps[REST_OF_SESSION] is None


# ---------------------------------------------------------------------------
# Decomposition
# ---------------------------------------------------------------------------

def test_reconstruction_reproduces_the_production_composite():
    """A payload with no abstentions must rebuild the engine's own number."""
    from src.signals.scoring_engine import ScoringEngine

    payload = {
        "gamma_anchor": {"score": -0.5, "max_points": 30},
        "net_gex_sign": {"score": 0.25, "max_points": 16},
        "put_call_ratio": {"score": 0.1, "max_points": 12},
        "volatility_regime": {"score": -0.2, "max_points": 6},
        "order_flow_imbalance": {"score": 0.8, "max_points": 19},
        "dealer_delta_pressure": {"score": -0.4, "max_points": 17},
    }
    offset = (30 * -0.5) + (16 * 0.25) + (12 * 0.1) + (6 * -0.2) + (19 * 0.8) + (17 * -0.4)
    expected = 50.0 + 50.0 * math.tanh(offset / 50.0)
    assert reconstruct(read_components(payload)) == pytest.approx(expected)
    # And the band that number lands in is the shipped one.
    assert band_for(expected) == ScoringEngine._regime_label(expected)


def test_zero_weight_components_are_excluded_from_the_rebuild():
    """The retired gamma-cluster entries are display-only; counting them shifts
    the renormalization and would rebuild a number nobody was ever shown."""
    payload = {
        "net_gex_sign": {"score": 0.5, "max_points": 16},
        "flip_distance": {"score": 1.0, "max_points": 0},
        "local_gamma": {"score": -1.0, "max_points": 0},
        "__aggregation__": {"mode": "market_state_index"},
    }
    comps = read_components(payload)
    assert set(comps) == {"net_gex_sign"}


def test_component_axis_table_covers_every_scored_component():
    from src.signals.scoring_engine import ScoringEngine

    assert set(AXIS) == set(ScoringEngine.COMPONENT_POINTS)


def test_directional_components_carry_at_least_a_third_of_the_scale():
    """The premise of the study's structural arm, pinned so it cannot rot."""
    from src.signals.scoring_engine import ScoringEngine

    points = ScoringEngine.COMPONENT_POINTS
    directional = sum(points[k] for k, v in AXIS.items() if v == "direction")
    assert directional >= 33.0
    assert sum(points.values()) == pytest.approx(100.0)


def test_folded_variant_is_symmetric_about_neutral():
    comps = read_components({"net_gex_sign": {"score": 0.0, "max_points": 16}})
    assert variant_scores(comps, 50.0)["msi_folded"] == pytest.approx(0.0)
    assert variant_scores(comps, 20.0)["msi_folded"] == pytest.approx(60.0)
    assert variant_scores(comps, 80.0)["msi_folded"] == pytest.approx(60.0)


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def test_spearman_matches_the_closed_form():
    # rho = 1 - 6*sum(d^2)/(n*(n^2-1)); for these ranks sum(d^2) = 26, n = 5.
    assert st.spearman([1, 2, 3, 4, 5], [5, 1, 4, 2, 3]) == pytest.approx(-0.3)
    assert st.spearman([1, 2, 3], [1, 2, 3]) == pytest.approx(1.0)
    assert st.spearman([1, 2, 3], [3, 2, 1]) == pytest.approx(-1.0)


def test_cliffs_delta_spans_the_full_range():
    assert st.cliffs_delta([5, 6, 7], [1, 2, 3]) == pytest.approx(1.0)
    assert st.cliffs_delta([1, 2, 3], [5, 6, 7]) == pytest.approx(-1.0)
    assert st.cliffs_delta([1, 2, 3], [1, 2, 3]) == pytest.approx(0.0)


def test_welch_matches_a_hand_computed_t():
    a, b = [1, 2, 3, 4, 5], [2, 4, 6, 8, 10]
    # means 3 and 6; vars 2.5 and 10; se = sqrt(2.5/5 + 10/5) = sqrt(2.5)
    t, p = st._welch(a, b)
    assert t == pytest.approx((3 - 6) / math.sqrt(2.5), rel=1e-9)
    assert 0.0 < p < 1.0


def test_benjamini_hochberg_is_step_up():
    # Every p sits exactly on its threshold, so all reject.
    assert all(st.benjamini_hochberg([0.05 * k / 8 for k in range(1, 9)], 0.05))
    # A single tiny p among large ones rejects alone.
    assert st.benjamini_hochberg([0.001, 0.9, 0.9, 0.9], 0.05) == [True, False, False, False]
    # Missing p-values never reject and never inflate m.
    assert st.benjamini_hochberg([None, 0.001, None], 0.05) == [False, True, False]


def test_block_bootstrap_does_not_reject_a_session_clustered_null():
    """The trap this whole design exists to avoid.

    Bucket membership is assigned per session and is unrelated to the values,
    which carry a session-level common shock. A row-level test sees tens of
    thousands of 'independent' observations and rejects almost always; the
    session-level bootstrap must not.
    """
    rng = random.Random(11)
    rejections = 0
    naive_rejections = 0
    trials = 30
    for trial in range(trials):
        values, sessions, flags = [], [], []
        for day in range(40):
            level = rng.gauss(0, 1)
            hot = rng.random() < 0.3
            for _ in range(60):
                values.append(level + rng.gauss(0, 0.5))
                sessions.append(day)
                flags.append(hot)
        c = st.compare(values, sessions, flags, iterations=300, seed=500 + trial)
        if c.p_block is not None and c.p_block <= 0.05:
            rejections += 1
        if c.p_naive is not None and c.p_naive <= 0.05:
            naive_rejections += 1
    # Generous bound: the point is that it is nothing like the naive rate.
    assert rejections <= trials * 0.25, f"block bootstrap over-rejects: {rejections}/{trials}"
    assert naive_rejections >= trials * 0.4, (
        "the naive test is supposed to be badly over-confident here; if it is not, "
        "this test no longer demonstrates why the block bootstrap is needed"
    )


def test_block_bootstrap_still_finds_a_real_difference():
    rng = random.Random(5)
    values, sessions, flags = [], [], []
    for day in range(40):
        level = rng.gauss(0, 1)
        for _ in range(60):
            hot = rng.random() < 0.3
            values.append(level + rng.gauss(0, 1) + (2.0 if hot else 0.0))
            sessions.append(day)
            flags.append(hot)
    c = st.compare(values, sessions, flags, iterations=500)
    assert c.p_block is not None and c.p_block < 0.05
    assert c.ci_lo_block is not None and c.ci_lo_block > 0
    assert c.diff == pytest.approx(2.0 * 0.7, abs=0.25)


def test_compare_baselines_against_the_pooled_sample_not_the_complement():
    """The bucket is part of its own baseline -- that is the unconditional rate."""
    values = [1.0, 1.0, 5.0, 5.0]
    sessions = ["a", "b", "a", "b"]
    flags = [False, False, True, True]
    c = st.compare(values, sessions, flags, iterations=50)
    assert c.n == 2 and c.n_base == 4
    assert c.mean == pytest.approx(5.0)
    assert c.mean_base == pytest.approx(3.0)   # pooled, not the complement's 1.0


def test_wilson_interval_matches_the_closed_form():
    lo, hi = st.wilson_ci(5, 20)
    z = 1.959963984540054
    p, n = 0.25, 20
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    assert lo == pytest.approx(centre - half)
    assert hi == pytest.approx(centre + half)


# ---------------------------------------------------------------------------
# The structural finding
# ---------------------------------------------------------------------------

def test_flow_direction_alone_moves_the_regime_band():
    """The study's data-free finding, pinned against the real engine.

    Gamma structure is held fixed across the sweep, so every magnitude-axis
    input -- net GEX, VIX, local gamma, the flip, the max-gamma strike -- is
    identical in every row. Only the direction of options flow varies. If the
    MSI were "deliberately directionless" as the frontend states, the score
    could not move at all.

    The sweep runs at the MEASURED dealer-delta scale (``cli dni``), not the
    range the formula permits, so these are numbers the shipped system can
    actually reach. At that scale the ``free`` structure saturates near the top
    of the range and stops crossing boundaries -- which is itself worth pinning,
    because an earlier version of this test asserted every structure crossed a
    band and only did so because the sweep was driving dealer delta ~9x beyond
    anything production produces.

    If this fails because the spans shrank, that is good news and the thresholds
    should be tightened, not deleted.
    """
    from research.msi_regime_excursion.structural import STRUCTURES, run_sweep, summarize

    summaries = {}
    for name in STRUCTURES:
        rows = run_sweep(structure=name, steps=9)
        components = [r.components for r in rows]

        # Every magnitude-axis component is identical across the sweep.
        for key in ("net_gex_sign", "gamma_anchor", "volatility_regime", "put_call_ratio"):
            values = {round(c[key], 6) for c in components}
            assert len(values) == 1, f"{key} varied across the {name} sweep: {values}"

        summaries[name] = summarize(name, rows)

    # Direction alone moves the score materially under every structure.
    for name, summary in summaries.items():
        assert summary["msi_span"] > 15.0, (
            f"{name}: flow direction moved the MSI only {summary['msi_span']:.1f} points"
        )

    # And it carries the label across a boundary under most of them.
    crossing = [n for n, s in summaries.items() if s["distinct_bands"] >= 2]
    assert len(crossing) >= 2, (
        f"flow direction crossed a band boundary in only {crossing} — "
        "the regime label is no longer direction-sensitive, which would be a fix"
    )


def test_the_neutral_structure_crosses_three_bands_on_direction_alone():
    """The headline number in the write-up. Pinned so the claim stays true."""
    from research.msi_regime_excursion.structural import run_sweep, summarize

    summary = summarize("neutral", run_sweep(structure="neutral", steps=9))
    assert summary["distinct_bands"] == 3
    # Measured at the REAL dealer-delta scale (cli dni), not the formula's range.
    assert summary["msi_span"] > 35.0
    assert summary["band_path"][0] == "Chop / Range"
    assert summary["band_path"][-1] == "Trend / Expansion"


# ---------------------------------------------------------------------------
# The shadow magnitude score published by the engine
# ---------------------------------------------------------------------------

def _fake_engine(scores: dict):
    """A ScoringEngine whose components return exactly ``scores``."""
    from src.signals.components.base import ComponentBase
    from src.signals.scoring_engine import ScoringEngine

    class Fake(ComponentBase):
        def __init__(self, name, value):
            self.name = name
            self.weight = 0.0
            self._v = value

        def compute(self, ctx):
            return self._v

        def context_values(self, ctx):
            return {}

    return ScoringEngine("SPX", [Fake(n, v) for n, v in scores.items()])


def _flat_context():
    from datetime import timezone
    from src.signals.components.base import MarketContext

    return MarketContext(
        datetime(2026, 6, 1, 15, 0, tzinfo=timezone.utc), "SPX", 5000.0, 0.0, 4990.0,
        1.0, 5000.0, 0.0, 0.0, [5000.0] * 40, 50.0, 0.0, 5000.0, 0.0, None, 800_000,
        [5000.0] * 40, [5000.0] * 40, {},
    )


def test_publishing_the_shadow_score_leaves_the_composite_bit_identical():
    """The whole point of the shadow score is that it changes nothing.

    This recomputes the composite with the formula as it stood before the
    shadow score was added and requires an exact match, so a future edit to
    ``_saturate`` cannot quietly move the shipped number.
    """
    from src.signals.scoring_engine import ScoringEngine, _COMPOSITE_SAT_SCALE
    from src.signals.components.spectrum import _ABSTAIN_THRESHOLD

    points = ScoringEngine.COMPONENT_POINTS
    ctx = _flat_context()
    rng = random.Random(11)

    for _ in range(400):
        scores = {
            n: (0.0 if rng.random() < 0.25 else round(rng.uniform(-1, 1), 6))
            for n in points
        }
        # The pre-change formula, inline.
        offset = active = total = 0.0
        for name, value in scores.items():
            clamped = max(-1.0, min(1.0, value))
            total += points[name]
            if abs(clamped) >= _ABSTAIN_THRESHOLD:
                offset += points[name] * clamped
                active += points[name]
        full = offset * (total / active) if active > 0 else 0.0
        expected = max(0.0, min(100.0, 50.0 + 50.0 * math.tanh(full / _COMPOSITE_SAT_SCALE)))

        snapshot, _ = _fake_engine(scores).score(ctx)
        # ScoreSnapshot rounds to 6dp on the way out; compare like for like.
        assert snapshot.composite_score == round(expected, 6)


def test_shadow_score_is_published_without_being_used():
    from src.signals.scoring_engine import ScoringEngine

    scores = {n: 0.5 for n in ScoringEngine.COMPONENT_POINTS}
    snapshot, _ = _fake_engine(scores).score(_flat_context())
    agg = snapshot.aggregation
    assert "magnitude_score" in agg and "magnitude_direction" in agg
    assert 0.0 <= agg["magnitude_score"] <= 100.0
    # The shipped label still comes from the composite, not the shadow.
    assert snapshot.direction == ScoringEngine._regime_label(snapshot.composite_score)
    # Band components only: 16 + 30 + 6 + 12.
    assert agg["magnitude_active_points"] == pytest.approx(64.0)


def test_band_candidate_set_is_the_researched_variant():
    """The engine's subset must be the one the study actually scored."""
    from src.signals.scoring_engine import ScoringEngine
    from research.msi_regime_excursion.decompose import VARIANTS

    assert set(ScoringEngine.BAND_CANDIDATE_COMPONENTS) == set(VARIANTS["msi_magnitude_pcr"])


def test_shadow_score_agrees_with_the_study_exactly_when_nothing_abstains():
    """And diverges only where the study's inputs could not have known better.

    The persisted payload stores each component's *display* score, which for an
    abstaining component is a small regime tilt rather than zero -- so
    ``decompose.variant_scores`` cannot tell an abstainer from a genuine small
    reading and includes it. The engine applies the real abstention rule, so it
    is the more correct of the two. They agree exactly on rows where no band
    component abstained, which is precisely the ``--clean-only`` sample the
    study's conclusions were drawn from.
    """
    from src.signals.scoring_engine import ScoringEngine
    from src.signals.components.spectrum import _ABSTAIN_THRESHOLD
    from research.msi_regime_excursion.decompose import read_components, variant_scores

    band = ScoringEngine.BAND_CANDIDATE_COMPONENTS
    ctx = _flat_context()
    rng = random.Random(11)
    agreed_clean = total_clean = 0

    for _ in range(400):
        scores = {
            n: (0.0 if rng.random() < 0.25 else round(rng.uniform(-1, 1), 6))
            for n in ScoringEngine.COMPONENT_POINTS
        }
        snapshot, _ = _fake_engine(scores).score(ctx)
        rebuilt = variant_scores(
            read_components(dict(snapshot.components)), snapshot.composite_score
        )["msi_magnitude_pcr"]
        shadow = snapshot.aggregation["magnitude_score"]

        band_abstained = any(
            abs(scores[n]) < _ABSTAIN_THRESHOLD for n in band
        )
        if not band_abstained:
            total_clean += 1
            if rebuilt is not None and abs(shadow - rebuilt) <= 1e-4:
                agreed_clean += 1

    assert total_clean > 50, "not enough abstention-free draws to be meaningful"
    assert agreed_clean == total_clean, (
        f"shadow and study disagree on {total_clean - agreed_clean} abstention-free rows"
    )


def test_shadow_score_reaches_the_persisted_payload():
    """score() computing it is not enough — it has to survive persist().

    ``_persist_inner`` rebuilds the payload as
    ``dict(score.components)`` plus ``__aggregation__``, so a value that lives
    only on the snapshot would be silently dropped on the way to the database.
    This drives the real ``score_and_persist`` against a capturing connection
    and inspects the JSON that would have been written.
    """
    import json
    from src.signals.scoring_engine import ScoringEngine

    captured: list = []

    class _Cursor:
        def execute(self, sql, params=None):
            captured.append((sql, params))

    class _Conn:
        def cursor(self):
            return _Cursor()

        def commit(self):
            pass

    scores = {n: 0.4 for n in ScoringEngine.COMPONENT_POINTS}
    snapshot = _fake_engine(scores).score_and_persist(_flat_context(), conn=_Conn())

    insert = next(
        (p for sql, p in captured if "INSERT INTO signal_scores" in sql), None
    )
    assert insert is not None, "no signal_scores insert was issued"
    payload = insert[5]
    if isinstance(payload, str):
        payload = json.loads(payload)
    agg = payload.get("__aggregation__", {})
    assert agg.get("mode") == "market_state_index"
    assert agg.get("magnitude_score") == pytest.approx(
        snapshot.aggregation["magnitude_score"]
    )
    assert agg.get("magnitude_direction") == snapshot.aggregation["magnitude_direction"]
