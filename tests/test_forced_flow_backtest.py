"""Unit tests for the Charm-into-Close track record aggregation.

``charm_backtest_summary`` is the honest hit-rate scorer behind the
/forced-flow/backtest endpoint: given one row per session (morning charm
read + noon and close spot), does the charm sign lean the same way as the
noon->close return? These tests pin the sign convention, the baseline honesty,
and the exclusion rules -- all without a database.
"""

from __future__ import annotations

from src.analytics.forced_flow import charm_backtest_summary


def _session(date, charm, noon, close):
    return {
        "session_date": date,
        "charm_flow": charm,
        "noon_px": noon,
        "close_px": close,
    }


def test_empty_input_is_all_none():
    s = charm_backtest_summary([])
    assert s["total_sessions"] == 0
    assert s["evaluated_sessions"] == 0
    assert s["hits"] == 0
    assert s["hit_rate"] is None
    assert s["baseline_rate"] is None
    assert s["edge"] is None
    assert s["signal_mean_return"] is None
    assert s["records"] == []


def test_perfect_predictor_scores_one():
    # charm>0 -> up day, charm<0 -> down day, every time.
    sessions = [
        _session("2026-07-01", 5_000_000, 100.0, 101.0),  # buy -> up  hit
        _session("2026-07-02", -5_000_000, 100.0, 99.0),  # sell -> down hit
        _session("2026-07-03", 2_000_000, 100.0, 100.5),  # buy -> up  hit
    ]
    s = charm_backtest_summary(sessions)
    assert s["total_sessions"] == 3
    assert s["evaluated_sessions"] == 3
    assert s["hits"] == 3
    assert s["hit_rate"] == 1.0
    # Mean of predicted_dir * return, all positive here.
    assert s["signal_mean_return"] > 0


def test_sign_convention_positive_charm_predicts_up():
    # A single up day with positive charm must be a hit; the same up day with
    # negative charm must be a miss. Locks the buy=+ / bullish mapping.
    up_hit = charm_backtest_summary([_session("2026-07-01", 1.0, 100.0, 101.0)])
    up_miss = charm_backtest_summary([_session("2026-07-01", -1.0, 100.0, 101.0)])
    assert up_hit["hits"] == 1 and up_hit["records"][0]["hit"] is True
    assert up_miss["hits"] == 0 and up_miss["records"][0]["hit"] is False


def test_flat_charm_and_flat_afternoon_excluded_from_evaluation():
    sessions = [
        _session("2026-07-01", 0.0, 100.0, 101.0),  # flat charm -> no call
        _session("2026-07-02", 5.0, 100.0, 100.0),  # flat afternoon -> unscorable
        _session("2026-07-03", 5.0, 100.0, 101.0),  # decisive hit
    ]
    s = charm_backtest_summary(sessions)
    assert s["total_sessions"] == 3
    assert s["evaluated_sessions"] == 1
    assert s["hits"] == 1
    assert s["hit_rate"] == 1.0


def test_nonpositive_noon_and_missing_charm_are_dropped_entirely():
    sessions = [
        _session("2026-07-01", 5.0, 0.0, 101.0),  # noon <= 0 -> dropped
        _session("2026-07-02", None, 100.0, 101.0),  # no charm -> dropped
        _session("2026-07-03", 5.0, 100.0, 101.0),  # kept
    ]
    s = charm_backtest_summary(sessions)
    assert s["total_sessions"] == 1
    assert s["evaluated_sessions"] == 1


def test_baseline_exposes_a_coin_flip():
    # Afternoons are up 3 of 4 sessions. A predictor that always says "buy"
    # is right 75% of the time -- but so is the naive baseline, so edge is 0.
    sessions = [
        _session("2026-07-01", 1.0, 100.0, 101.0),  # up  hit
        _session("2026-07-02", 1.0, 100.0, 102.0),  # up  hit
        _session("2026-07-03", 1.0, 100.0, 103.0),  # up  hit
        _session("2026-07-04", 1.0, 100.0, 99.0),  # down miss
    ]
    s = charm_backtest_summary(sessions)
    assert s["evaluated_sessions"] == 4
    assert s["hit_rate"] == 0.75
    assert s["baseline_rate"] == 0.75  # always-up guess also 75%
    assert abs(s["edge"]) < 1e-12  # no edge over the naive rule


def test_records_are_most_recent_first_and_capped():
    sessions = [_session(f"2026-07-{d:02d}", 1.0, 100.0, 101.0) for d in range(1, 11)]
    s = charm_backtest_summary(sessions, recent_limit=3)
    dates = [r["date"] for r in s["records"]]
    assert dates == ["2026-07-10", "2026-07-09", "2026-07-08"]
    # Summary still reflects every session, not just the shown window.
    assert s["evaluated_sessions"] == 10


def test_signal_mean_return_can_be_negative():
    # A predictor that leans the wrong way loses money by construction.
    sessions = [
        _session("2026-07-01", 1.0, 100.0, 99.0),  # buy -> down
        _session("2026-07-02", -1.0, 100.0, 101.0),  # sell -> up
    ]
    s = charm_backtest_summary(sessions)
    assert s["hits"] == 0
    assert s["signal_mean_return"] < 0
