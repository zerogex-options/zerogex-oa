"""Forward outcomes and the statistics toolkit.

Outcome definitions decide what "better" means in this experiment, so they are
pinned against a hand-built price path where the right answer is arithmetic.
The statistics tests check the properties that keep the study honest: HAC
standard errors widen with overlap, a nested model with a useless predictor
shows no gain, and walk-forward splits never leak the test period into the fit.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone

import numpy as np
import pytest

from research.mm_attributed_gex import stats as st
from research.mm_attributed_gex.outcomes import Bar, BarSeries, compute_outcomes

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")


def _bars(closes, session=date(2026, 6, 15), start_hour=9, start_minute=30, wick=0.5):
    """Minute bars from a close series. ``wick=0`` gives zero-range bars.

    The wick matters for excursion-based measures (range, mean reversion), so
    tests that pin those exactly build bars without one.
    """
    base = datetime.combine(session, time(start_hour, start_minute), tzinfo=ET).astimezone(
        timezone.utc
    )
    return [
        Bar(base + timedelta(minutes=i), c, c + wick, c - wick, c) for i, c in enumerate(closes)
    ]


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


def test_forward_return_is_measured_from_the_entry_bar_close():
    """100 -> 101 over 5 minutes is +100 bps."""
    series = BarSeries(_bars([100.0, 100.0, 100.0, 100.0, 100.0, 101.0]))
    entry_ts = series.bars[0].ts
    outcome = compute_outcomes(series, entry_ts, horizons=(5,))
    assert outcome is not None
    assert outcome.entry == 100.0
    assert outcome.ret_bps[5] == pytest.approx(100.0)
    assert outcome.abs_ret_bps[5] == pytest.approx(100.0)


def test_the_entry_bar_is_excluded_from_the_forward_window():
    """A spike on the entry bar itself must not count as a forward move."""
    bars = _bars([100.0, 100.0, 100.0])
    bars[0] = Bar(bars[0].ts, 100.0, 200.0, 50.0, 100.0)  # huge range on entry bar
    series = BarSeries(bars)
    outcome = compute_outcomes(series, bars[0].ts, horizons=(2,))
    assert outcome.range_bps[2] == pytest.approx(10_000.0 * 1.0 / 100.0)


def test_realized_volatility_is_the_root_sum_of_squared_log_returns():
    series = BarSeries(_bars([100.0, 101.0, 102.0, 103.0]))
    outcome = compute_outcomes(series, series.bars[0].ts, horizons=(3,))
    closes = np.array([100.0, 101.0, 102.0, 103.0])
    expected = 10_000.0 * math.sqrt(float(np.sum(np.diff(np.log(closes)) ** 2)))
    assert outcome.realized_vol_bps[3] == pytest.approx(expected)


def test_mean_reversion_is_one_when_the_window_fully_retraces():
    """Up then all the way back = fully retraced excursion."""
    series = BarSeries(_bars([100.0, 105.0, 100.0], wick=0.0))
    outcome = compute_outcomes(series, series.bars[0].ts, horizons=(2,))
    assert outcome.mean_reversion[2] == pytest.approx(1.0)


def test_mean_reversion_is_zero_when_the_window_closes_at_its_extreme():
    """A monotone run gives back none of its excursion."""
    series = BarSeries(_bars([100.0, 101.0, 102.0], wick=0.0))
    outcome = compute_outcomes(series, series.bars[0].ts, horizons=(2,))
    assert outcome.mean_reversion[2] == pytest.approx(0.0)


def test_continuation_needs_a_prior_move_and_follows_its_sign():
    """Trend persistence is measured against the move BEFORE the reading."""
    series = BarSeries(_bars([100.0] * 5 + [95.0] + [94.0, 93.0, 92.0, 91.0]))
    entry = series.bars[5].ts  # after a down move
    outcome = compute_outcomes(series, entry, horizons=(4,), lookback_minutes=5)
    assert outcome.continuation[4] is True  # kept falling

    flat = BarSeries(_bars([100.0] * 10))
    outcome_flat = compute_outcomes(flat, flat.bars[5].ts, horizons=(4,), lookback_minutes=5)
    assert outcome_flat.continuation[4] is None  # no prior direction to persist


def test_large_move_flag_uses_the_configured_threshold():
    series = BarSeries(_bars([100.0, 100.0, 100.6]))
    strict = compute_outcomes(series, series.bars[0].ts, horizons=(2,), large_move_bps=50.0)
    loose = compute_outcomes(series, series.bars[0].ts, horizons=(2,), large_move_bps=100.0)
    assert strict.large_move[2] is True
    assert loose.large_move[2] is False


def test_remainder_of_session_runs_to_the_last_bar_of_the_day():
    series = BarSeries(_bars([100.0, 101.0, 102.0, 103.0]))
    outcome = compute_outcomes(series, series.bars[0].ts, horizons=(1,))
    assert outcome.ret_to_close_bps == pytest.approx(300.0)


def test_close_to_close_spans_two_sessions():
    day1 = _bars([100.0, 101.0], session=date(2026, 6, 15))
    day2 = _bars([102.0, 104.0], session=date(2026, 6, 16))
    series = BarSeries(day1 + day2)
    outcome = compute_outcomes(series, day1[0].ts, horizons=(1,))
    # Session 1 closes at 101, session 2 at 104.
    assert outcome.close_to_close_bps == pytest.approx(10_000.0 * 3.0 / 101.0)


def test_a_reading_with_no_nearby_bar_returns_none():
    """An off-session timestamp is dropped rather than scored against stale data."""
    series = BarSeries(_bars([100.0, 101.0]))
    stale = series.bars[-1].ts + timedelta(hours=6)
    assert compute_outcomes(series, stale, horizons=(5,)) is None


def test_bar_series_lookups_are_binary_searches_over_sorted_stamps():
    series = BarSeries(_bars([100.0 + i for i in range(10)]))
    mid = series.bars[5].ts
    assert series.index_at_or_before(mid) == 5
    assert series.index_at_or_before(mid + timedelta(seconds=30)) == 5
    assert series.index_at_or_after(mid + timedelta(seconds=30)) == 6
    assert series.close_at_or_before(mid) == 105.0


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def test_describe_reports_a_confidence_interval_that_brackets_the_mean():
    s = st.describe([1.0, 2.0, 3.0, 4.0, 5.0])
    assert s.n == 5
    assert s.mean == pytest.approx(3.0)
    assert s.median == pytest.approx(3.0)
    assert s.ci_low < s.mean < s.ci_high


def test_describe_handles_empty_and_single_values():
    assert st.describe([]).n == 0
    single = st.describe([7.0])
    assert single.n == 1 and single.mean == 7.0 and single.std == 0.0


def test_welch_detects_a_real_difference_and_reports_an_effect_size():
    rng = np.random.default_rng(11)
    a = rng.normal(1.0, 1.0, 500)
    b = rng.normal(0.0, 1.0, 500)
    cmp_ = st.compare_means(a, b)
    assert cmp_.p_value < 1e-10
    assert cmp_.cohens_d == pytest.approx(1.0, abs=0.2)
    assert cmp_.ci_low > 0


def test_welch_finds_nothing_when_there_is_nothing():
    rng = np.random.default_rng(12)
    a = rng.normal(0.0, 1.0, 400)
    b = rng.normal(0.0, 1.0, 400)
    assert st.compare_means(a, b).p_value > 0.05


def test_proportion_test_matches_a_hand_computation():
    cmp_ = st.compare_proportions(60, 100, 40, 100)
    assert cmp_.rate_a == 0.6 and cmp_.rate_b == 0.4
    assert cmp_.diff == pytest.approx(0.2)
    assert cmp_.p_value < 0.01


def test_hac_standard_errors_widen_when_residuals_are_autocorrelated():
    """The correction that stops overlapping windows faking significance.

    A strongly autocorrelated residual makes plain OLS standard errors far too
    small. Newey-West with a real lag window has to report a larger SE than the
    zero-lag case, which is the whole reason it is used here.
    """
    rng = np.random.default_rng(13)
    n = 600
    x = rng.normal(size=n)
    noise = np.zeros(n)
    for i in range(1, n):
        noise[i] = 0.9 * noise[i - 1] + rng.normal(scale=0.4)
    y = 0.2 * x + noise

    naive = st.ols_hac(y, {"x": x}, hac_lags=0)
    corrected = st.ols_hac(y, {"x": x}, hac_lags=30)
    i_naive = naive.names.index("intercept")
    i_corr = corrected.names.index("intercept")
    assert corrected.se[i_corr] > naive.se[i_naive] * 1.5
    assert corrected.coef == pytest.approx(naive.coef, rel=1e-9)  # same point estimate


def test_nested_model_reports_no_gain_for_a_useless_predictor():
    rng = np.random.default_rng(14)
    n = 800
    x1 = rng.normal(size=n)
    junk = rng.normal(size=n)
    y = 0.5 * x1 + rng.normal(scale=0.5, size=n)
    out = st.nested_model_test(y, {"x1": x1}, {"x1": x1, "junk": junk})
    assert out["ok"]
    assert out["delta_adj_r2"] < 0.005
    assert out["f_p_value"] > 0.05


def test_nested_model_detects_a_genuinely_useful_predictor():
    rng = np.random.default_rng(15)
    n = 800
    x1 = rng.normal(size=n)
    x2 = rng.normal(size=n)
    y = 0.5 * x1 + 0.5 * x2 + rng.normal(scale=0.5, size=n)
    out = st.nested_model_test(y, {"x1": x1}, {"x1": x1, "x2": x2})
    assert out["delta_adj_r2"] > 0.05
    assert out["f_p_value"] < 1e-6


def test_nested_model_rejects_an_enhanced_model_that_drops_a_baseline_term():
    x = np.arange(50.0)
    with pytest.raises(ValueError, match="every baseline term"):
        st.nested_model_test(x, {"a": x}, {"b": x})


def test_logistic_regression_recovers_a_known_coefficient_sign():
    rng = np.random.default_rng(16)
    n = 900
    x = rng.normal(size=n)
    p = 1.0 / (1.0 + np.exp(-(0.8 * x)))
    y = (rng.uniform(size=n) < p).astype(float)
    fit = st.logistic_regression(y, {"x": x})
    assert fit.converged
    assert fit.coef[fit.names.index("x")] > 0.4
    assert st.auc(y, fit.fitted) > 0.6


def test_logistic_regression_declines_a_single_class_target():
    x = np.arange(100.0)
    assert st.logistic_regression(np.ones(100), {"x": x}) is None


def test_auc_is_one_for_a_perfect_score_and_half_for_a_constant_one():
    y = np.array([0, 0, 1, 1], dtype=float)
    assert st.auc(y, np.array([0.1, 0.2, 0.8, 0.9])) == pytest.approx(1.0)
    assert st.auc(y, np.array([0.5, 0.5, 0.5, 0.5])) == pytest.approx(0.5)


def test_brier_and_log_loss_reward_a_confident_correct_forecast():
    y = [1.0, 0.0]
    good = st.brier_score(y, [0.9, 0.1])
    bad = st.brier_score(y, [0.5, 0.5])
    assert good < bad
    assert st.log_loss(y, [0.9, 0.1]) < st.log_loss(y, [0.5, 0.5])


def test_calibration_bins_partition_the_probability_range():
    rng = np.random.default_rng(17)
    p = rng.uniform(size=500)
    y = (rng.uniform(size=500) < p).astype(float)
    bins = st.calibration_bins(y, p, n_bins=5)
    assert len(bins) == 5
    assert sum(b["n"] for b in bins) == 500


def test_walk_forward_splits_expand_and_never_overlap():
    splits = st.walk_forward_splits(1000, n_folds=4, min_train=200, embargo=0)
    assert len(splits) == 4
    prev_train_end = 0
    for train, test in splits:
        assert train.start == 0
        assert train.stop > prev_train_end
        assert test.start >= train.stop  # test always after train
        prev_train_end = train.stop


def test_walk_forward_embargo_leaves_a_gap_between_train_and_test():
    """Without the embargo, an overlapping forward window leaks into the test."""
    splits = st.walk_forward_splits(1000, n_folds=3, min_train=200, embargo=12)
    for train, test in splits:
        assert test.start - train.stop == 12


def test_block_bootstrap_widens_the_interval_for_dependent_data():
    """Serial dependence must not be resampled away."""
    rng = np.random.default_rng(18)
    n = 800
    series = np.zeros(n)
    for i in range(1, n):
        series[i] = 0.95 * series[i - 1] + rng.normal(scale=0.3)
    iid = st.bootstrap_ci(series, n_boot=400, block_size=1, seed=1)
    blocked = st.bootstrap_ci(series, n_boot=400, block_size=50, seed=1)
    assert (blocked["ci_high"] - blocked["ci_low"]) > (iid["ci_high"] - iid["ci_low"])


def test_permutation_test_rejects_a_real_difference_and_keeps_a_null():
    rng = np.random.default_rng(19)
    real = st.permutation_test(rng.normal(1.0, 1.0, 300), rng.normal(0.0, 1.0, 300), n_perm=400)
    null = st.permutation_test(rng.normal(0.0, 1.0, 300), rng.normal(0.0, 1.0, 300), n_perm=400)
    assert real["p_value"] < 0.01
    assert null["p_value"] > 0.05


def test_benjamini_hochberg_controls_the_family_not_each_test():
    """A p-value that clears 0.05 alone can still fail once the family is counted."""
    p_values = [0.001, 0.02, 0.04, 0.3, 0.9]
    flags = st.benjamini_hochberg(p_values, alpha=0.05)
    assert flags == [True, True, False, False, False]
    # All-null family: nothing survives.
    assert st.benjamini_hochberg([0.4, 0.5, 0.6]) == [False, False, False]


def test_correlation_reports_both_pearson_and_spearman():
    x = np.arange(50.0)
    y = x**2  # monotone but not linear
    out = st.correlation(x, y)
    assert out["spearman"] == pytest.approx(1.0)
    assert out["pearson"] < 1.0
