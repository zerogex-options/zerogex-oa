"""Fleet performance-trend computation (src.tradeworkz.trend.build_trend).

Locks the rolling-window math (win rate / profit factor / expectancy), the
window-start rebased cumulative line, and the SPY buy-hold benchmark — the
pieces that turn the raw trade ledger into "is performance improving?" rather
than the since-inception NAV. Pure function, no DB.
"""

from __future__ import annotations

import pytest

from src.tradeworkz.trend import build_trend


def _day(d, realized, trades, wins, losses, gw, gl):
    return {
        "session_date": d, "realized_pnl": realized, "trades": trades,
        "wins": wins, "losses": losses, "gross_win": gw, "gross_loss": gl,
    }


# day1 +100 (1W/1L, gw300/gl200); day2 -50 (0W/1L, gl50); day3 +200 (2W, gw200)
_SERIES = [
    _day("2026-08-03", 100.0, 2, 1, 1, 300.0, 200.0),
    _day("2026-08-04", -50.0, 1, 0, 1, 0.0, 50.0),
    _day("2026-08-05", 200.0, 2, 2, 0, 200.0, 0.0),
]


def test_empty_series_is_safe():
    out = build_trend([])
    assert out["series"] == []
    assert out["headline"] == {}


def test_cumulative_is_running_sum_from_window_start():
    out = build_trend(_SERIES)
    cum = [p["cumulative_pnl"] for p in out["series"]]
    assert cum == [100.0, 50.0, 250.0]


def test_rolling_window_aggregates_trailing_sessions():
    out = build_trend(_SERIES, windows=[2])
    roll = out["series"][2]["rolling"]["2"]  # trailing 2 sessions: day2+day3
    assert roll["realized_pnl"] == 150.0
    assert roll["trades"] == 3
    assert roll["win_rate"] == pytest.approx(2 / 3)
    assert roll["profit_factor"] == pytest.approx(200.0 / 50.0)  # gw200 / gl50
    assert roll["expectancy"] == pytest.approx(150.0 / 3)


def test_rolling_window_truncates_at_series_start():
    out = build_trend(_SERIES, windows=[10])
    first = out["series"][0]["rolling"]["10"]
    assert first["sessions"] == 1          # only day1 available
    assert first["realized_pnl"] == 100.0
    full = out["series"][2]["rolling"]["10"]
    assert full["sessions"] == 3
    assert full["win_rate"] == pytest.approx(3 / 5)
    assert full["profit_factor"] == pytest.approx(500.0 / 250.0)


def test_profit_factor_none_without_losses():
    # day3 alone has zero losing P&L -> profit factor is undefined (n/a), not inf.
    out = build_trend(_SERIES, windows=[1])
    assert out["series"][2]["rolling"]["1"]["profit_factor"] is None


def test_win_rate_and_expectancy_none_on_zero_trades():
    out = build_trend([_day("2026-08-05", 0.0, 0, 0, 0, 0.0, 0.0)], windows=[5])
    roll = out["series"][0]["rolling"]["5"]
    assert roll["win_rate"] is None
    assert roll["expectancy"] is None


def test_fleet_cum_return_pct_uses_start_capital():
    out = build_trend(_SERIES, fleet_start_capital=1000.0)
    pcts = [p["fleet_cum_return_pct"] for p in out["series"]]
    assert pcts == pytest.approx([0.10, 0.05, 0.25])


def test_fleet_cum_return_pct_none_without_capital():
    out = build_trend(_SERIES, fleet_start_capital=0.0)
    assert out["series"][0]["fleet_cum_return_pct"] is None


def test_spy_benchmark_rebased_to_first_available_close():
    spy = {"2026-08-03": 100.0, "2026-08-04": 110.0, "2026-08-05": 99.0}
    out = build_trend(_SERIES, spy_closes=spy)
    rets = [p["spy_cum_return_pct"] for p in out["series"]]
    assert rets == pytest.approx([0.0, 0.10, -0.01])


def test_spy_benchmark_none_when_unavailable():
    out = build_trend(_SERIES, spy_closes={})
    assert all(p["spy_cum_return_pct"] is None for p in out["series"])


def test_headline_reflects_latest_session():
    out = build_trend(_SERIES, windows=[2, 10], fleet_start_capital=1000.0)
    h = out["headline"]
    assert h["as_of"] == "2026-08-05"
    assert h["cumulative_pnl"] == 250.0
    assert h["fleet_cum_return_pct"] == pytest.approx(0.25)
    # Both requested windows are present, keyed by string.
    assert set(h["windows"].keys()) == {"2", "10"}
    assert h["windows"]["10"]["win_rate"] == pytest.approx(3 / 5)
