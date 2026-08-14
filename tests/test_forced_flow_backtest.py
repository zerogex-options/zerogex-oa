"""Unit tests for the Charm-into-Close track record aggregation.

``charm_backtest_summary`` scores the charm read RELATIVE to its own trailing
baseline, not by raw sign -- because for an index book the raw sign is
structurally near-constant ("buy" almost every day), so scoring it just
reproduces the naive baseline and tests nothing. These tests pin the
trailing-baseline logic, its look-ahead safety, the warm-up accounting, the
median robustness, and the exclusion rules -- all without a database.
"""

from __future__ import annotations

from src.analytics.forced_flow import charm_backtest_summary


def _session(date, charm, noon, close, smooth=None):
    row = {
        "session_date": date,
        "charm_flow": charm,
        "noon_px": noon,
        "close_px": close,
    }
    if smooth is not None:
        row["charm_flow_smooth"] = smooth
    return row


# Compact params so a handful of sessions exercise real scoring (production uses
# a 20-session window and a 10-session warm-up).
_W = dict(baseline_window=10, min_baseline_history=2)


def test_empty_input_is_all_none():
    s = charm_backtest_summary([])
    assert s["total_sessions"] == 0
    assert s["evaluated_sessions"] == 0
    assert s["warmup_sessions"] == 0
    assert s["hits"] == 0
    assert s["hit_rate"] is None
    assert s["baseline_rate"] is None
    assert s["edge"] is None
    assert s["signal_mean_return"] is None
    assert s["records"] == []
    # Statistics degrade to None / not-significant on no data.
    assert s["hit_rate_ci_low"] is None
    assert s["hit_rate_ci_high"] is None
    assert s["edge_p_value"] is None
    assert s["signal_t_stat"] is None
    assert s["significant"] is False


def test_scores_deviation_from_trailing_baseline_not_raw_sign():
    # Every charm value is POSITIVE (raw sign is always "buy"), yet the call is
    # the sign of (charm - trailing median). d1/d2 warm up; d3 is above its
    # baseline -> Buy, d4 below -> Sell, d5 below -> Sell.
    sessions = [
        _session("2026-01-01", 100.0, 100.0, 100.0),  # warm-up
        _session("2026-01-02", 100.0, 100.0, 100.0),  # warm-up
        _session("2026-01-03", 120.0, 100.0, 101.0),  # base 100, +20 Buy, up   -> hit
        _session("2026-01-04", 80.0, 100.0, 99.0),    # base 100, -20 Sell, down -> hit
        _session("2026-01-05", 90.0, 100.0, 101.0),   # base 100, -10 Sell, up   -> miss
    ]
    s = charm_backtest_summary(sessions, **_W)
    assert s["total_sessions"] == 5
    assert s["warmup_sessions"] == 2
    assert s["evaluated_sessions"] == 3
    assert s["hits"] == 2
    by_date = {r["date"]: r for r in s["records"]}
    assert by_date["2026-01-03"]["predicted_dir"] == 1
    assert by_date["2026-01-03"]["charm_baseline"] == 100.0
    assert by_date["2026-01-03"]["charm_deviation"] == 20.0
    assert by_date["2026-01-04"]["predicted_dir"] == -1
    assert by_date["2026-01-05"]["predicted_dir"] == -1 and by_date["2026-01-05"]["hit"] is False


def test_constant_signal_scores_no_calls_not_a_baseline_match():
    # THE fix. A structurally constant "buy" signal used to score hit_rate ==
    # baseline (a vacuous coin flip that reads as "no edge"). Demeaned, every
    # deviation is exactly zero, so there is simply no directional call to make:
    # zero evaluated, hit_rate None -- honestly "nothing to score", not a match.
    sessions = [
        _session(f"2026-02-{d:02d}", 5_000_000.0, 100.0, 101.0 if d % 2 else 99.0)
        for d in range(1, 21)
    ]
    s = charm_backtest_summary(sessions, **_W)
    assert s["total_sessions"] == 20
    assert s["warmup_sessions"] == 2
    assert s["evaluated_sessions"] == 0  # constant -> no deviation -> no call
    assert s["hit_rate"] is None
    assert s["edge"] is None
    assert s["significant"] is False


def test_baseline_uses_median_not_mean():
    # One huge day in the window must not drag the baseline (a pin day can be
    # 100x a normal one). Median ignores it; a mean would flip the call.
    sessions = [
        _session("2026-03-01", 10.0, 100.0, 100.0),    # warm-up
        _session("2026-03-02", 10.0, 100.0, 100.0),    # warm-up
        _session("2026-03-03", 1000.0, 100.0, 100.0),  # warm-up (outlier)
        _session("2026-03-04", 10.0, 100.0, 100.0),    # warm-up
        # prior window [10, 10, 1000, 10] -> median 10 (mean 257.5).
        _session("2026-03-05", 11.0, 100.0, 101.0),    # +1 vs median 10 -> Buy, up -> hit
    ]
    s = charm_backtest_summary(sessions, baseline_window=10, min_baseline_history=4)
    rec = s["records"][0]
    assert rec["charm_baseline"] == 10.0
    assert rec["predicted_dir"] == 1 and rec["hit"] is True


def test_baseline_is_look_ahead_free():
    # The window only sees STRICTLY-prior sessions, so a later spike cannot
    # change an earlier call. Same early sessions, opposite futures -> identical
    # early record.
    early = [
        _session("2026-04-01", 10.0, 100.0, 100.0),
        _session("2026-04-02", 10.0, 100.0, 100.0),
        _session("2026-04-03", 12.0, 100.0, 101.0),  # scored vs [10, 10]
    ]
    a = charm_backtest_summary(early + [_session("2026-04-04", 999.0, 100.0, 101.0)], **_W)
    b = charm_backtest_summary(early + [_session("2026-04-04", -999.0, 100.0, 101.0)], **_W)
    ra = next(r for r in a["records"] if r["date"] == "2026-04-03")
    rb = next(r for r in b["records"] if r["date"] == "2026-04-03")
    assert ra == rb  # the 04-03 call is untouched by whatever 04-04 does


def test_trailing_window_forgets_old_regime():
    # window=2 -> only the last two priors define "normal".
    sessions = [
        _session("2026-05-01", 1.0, 100.0, 100.0),
        _session("2026-05-02", 100.0, 100.0, 100.0),
        _session("2026-05-03", 100.0, 100.0, 100.0),
        # prior [100, 100] -> median 100; charm 50 -> below -> Sell, down -> hit.
        _session("2026-05-04", 50.0, 100.0, 99.0),
    ]
    s = charm_backtest_summary(sessions, baseline_window=2, min_baseline_history=2)
    rec = s["records"][0]
    assert rec["date"] == "2026-05-04"
    assert rec["charm_baseline"] == 100.0
    assert rec["predicted_dir"] == -1 and rec["hit"] is True


def test_input_need_not_be_pre_sorted():
    ordered = [
        _session("2026-06-01", 10.0, 100.0, 100.0),
        _session("2026-06-02", 10.0, 100.0, 100.0),
        _session("2026-06-03", 20.0, 100.0, 101.0),
    ]
    shuffled = [ordered[2], ordered[0], ordered[1]]
    a = charm_backtest_summary(ordered, **_W)
    b = charm_backtest_summary(shuffled, **_W)
    assert a["records"] == b["records"]
    assert a["evaluated_sessions"] == 1 and a["records"][0]["predicted_dir"] == 1


def test_charm_key_demeans_each_predictor_independently():
    # full rises above its baseline while smooth falls below its own -> opposite
    # calls on the same session, proving each is demeaned against its OWN history.
    sessions = [
        _session("2027-01-01", 10.0, 100.0, 100.0, smooth=10.0),
        _session("2027-01-02", 10.0, 100.0, 100.0, smooth=10.0),
        _session("2027-01-03", 20.0, 100.0, 101.0, smooth=5.0),  # full +Buy, smooth -Sell; up
    ]
    full = charm_backtest_summary(sessions, charm_key="charm_flow", **_W)
    smooth = charm_backtest_summary(sessions, charm_key="charm_flow_smooth", **_W)
    assert full["records"][0]["predicted_dir"] == 1 and full["hits"] == 1
    assert smooth["records"][0]["predicted_dir"] == -1 and smooth["hits"] == 0
    # A missing smooth value drops only that session from the smooth series.
    sessions[2].pop("charm_flow_smooth")
    smooth2 = charm_backtest_summary(sessions, charm_key="charm_flow_smooth", **_W)
    assert smooth2["total_sessions"] == 2


def test_significance_requires_edge_and_a_real_sample():
    # A perfect demeaned predictor on a short sample: real edge, too few to
    # certify. charm alternates above/below a stable baseline; the close follows.
    sessions = [_session("2000-01-01", 10.0, 100.0, 100.0), _session("2000-01-02", 10.0, 100.0, 100.0)]
    d = 3
    for _ in range(12):
        sessions.append(_session(f"2000-01-{d:02d}", 20.0, 100.0, 101.0))  # above -> up
        d += 1
        sessions.append(_session(f"2000-01-{d:02d}", 5.0, 100.0, 99.0))  # below -> down
        d += 1
    tiny = charm_backtest_summary(sessions, **_W)
    assert tiny["hit_rate"] == 1.0
    assert tiny["significant"] is False  # evaluated < _MIN_SIGNIFICANT_N

    # A coin-flip-vs-baseline predictor over a big sample stays non-significant:
    # the demeaned call alternates while the close direction is independent of it.
    rows = [_session("2001-01-01", 10.0, 100.0, 100.0), _session("2001-01-02", 10.0, 100.0, 100.0)]
    for i in range(200):
        charm = 20.0 if i % 2 == 0 else 5.0  # call alternates Buy / Sell
        close = 101.0 if (i // 2) % 2 == 0 else 99.0  # close direction independent of the call
        mo = (i // 28) + 2
        rows.append(_session(f"2001-{mo:02d}-{(i % 28) + 1:02d}", charm, 100.0, close))
    coin = charm_backtest_summary(rows, **_W)
    assert coin["evaluated_sessions"] >= 30
    assert coin["significant"] is False
    assert coin["edge_p_value"] is not None and coin["edge_p_value"] > 0.05


def test_signal_mean_return_can_be_negative():
    # A demeaned call that leans the wrong way loses money by construction.
    sessions = [
        _session("2002-01-01", 10.0, 100.0, 100.0),
        _session("2002-01-02", 10.0, 100.0, 100.0),
        _session("2002-01-03", 20.0, 100.0, 99.0),  # Buy, down -> lose
        _session("2002-01-04", 5.0, 100.0, 101.0),  # Sell, up -> lose
    ]
    s = charm_backtest_summary(sessions, **_W)
    assert s["hits"] == 0
    assert s["signal_mean_return"] < 0


def test_flat_afternoon_excluded_but_counted():
    sessions = [
        _session("2026-08-01", 10.0, 100.0, 100.0),  # warm-up
        _session("2026-08-02", 10.0, 100.0, 100.0),  # warm-up
        _session("2026-08-03", 20.0, 100.0, 100.0),  # Buy but flat afternoon -> unscorable
        _session("2026-08-04", 20.0, 100.0, 101.0),  # Buy, up -> hit
    ]
    s = charm_backtest_summary(sessions, **_W)
    assert s["total_sessions"] == 4
    assert s["warmup_sessions"] == 2
    assert s["evaluated_sessions"] == 1
    assert s["hits"] == 1


def test_nonpositive_noon_and_missing_charm_dropped_entirely():
    sessions = [
        _session("2026-09-01", 10.0, 100.0, 100.0),  # warm-up
        _session("2026-09-02", 10.0, 100.0, 100.0),  # warm-up
        _session("2026-09-03", 5.0, 0.0, 101.0),  # noon <= 0 -> dropped, not counted
        _session("2026-09-04", None, 100.0, 101.0),  # no charm -> dropped, not counted
        _session("2026-09-05", 20.0, 100.0, 101.0),  # kept: Buy, up -> hit
    ]
    s = charm_backtest_summary(sessions, **_W)
    assert s["total_sessions"] == 3  # the two bad rows never enter the series
    assert s["warmup_sessions"] == 2
    assert s["evaluated_sessions"] == 1
    assert s["hits"] == 1


def test_records_most_recent_first_capped_and_carry_baseline():
    sessions = [
        _session("2026-07-01", 10.0, 100.0, 100.0),
        _session("2026-07-02", 10.0, 100.0, 100.0),
    ]
    for d in range(3, 13):  # 10 rising, decisive sessions
        sessions.append(_session(f"2026-07-{d:02d}", 10.0 + d, 100.0, 101.0))
    s = charm_backtest_summary(sessions, recent_limit=3, baseline_window=10, min_baseline_history=2)
    dates = [r["date"] for r in s["records"]]
    assert dates == ["2026-07-12", "2026-07-11", "2026-07-10"]  # most-recent first, capped
    assert set(s["records"][0]) == {
        "date",
        "charm_flow",
        "charm_baseline",
        "charm_deviation",
        "return_pct",
        "predicted_dir",
        "realized_dir",
        "hit",
    }
    # Summary still reflects every scored session, not just the shown window.
    assert s["evaluated_sessions"] == 10
    assert s["warmup_sessions"] == 2
