"""Whether the Trade Bias states say how far price moves (research/trade_bias_movement).

Research-only, but its verdicts decide whether the panel can be presented as
a read on movement, so the parts that could quietly lie are pinned here:

* recent movement is read only from bars the reading had seen, within its
  own session;
* the ratio is exactly the least-squares coefficient it claims to be;
* the verdict follows the pre-registered rule, with Holm's correction always
  across all four state groups;
* the machinery returns the right verdicts on worlds whose answer is known,
  including one where the states only echo the clock and the tape, and the
  command line runs end to end.
"""

from __future__ import annotations

import json
import math
import random
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pytest

from research.msi_regime_excursion.excursion import Bar
from research.short_gamma_trend.outcomes import Horizon, SessionBars
from research.short_gamma_trend.rule import current_state
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import Estimate
from research.trade_bias_movement.study import (
    BY_SYMBOL,
    COMPARABLE,
    CONFIRM_HORIZON,
    GROUPS,
    MIN_MINUTES,
    MIN_SESSIONS,
    MOVES_LESS,
    MOVES_MORE,
    NOTHING,
    PRIMARY_HORIZON,
    SLIGHTLY_LESS,
    SLIGHTLY_MORE,
    STATE_TITLES,
    TOO_RARE,
    GroupResult,
    MoveFrame,
    RatioCell,
    RecentMovement,
    apply_verdicts,
    build,
    matches_copy,
    time_slot,
)
from src.signals.trade_bias.bias import BiasInput

# 2026-09-23 (Wed, EDT): 09:30 ET == 13:30 UTC.
OPEN = datetime(2026, 9, 23, 13, 30, tzinfo=timezone.utc)
SYMBOLS = ["SPY", "SPX"]


def _bars(start: datetime, n: int, rng: random.Random, price: float = 100.0) -> list[Bar]:
    out = []
    for i in range(n):
        close = price * (1 + rng.gauss(0, 0.001))
        out.append(
            Bar(
                ts=start + timedelta(minutes=i),
                open=price,
                high=max(price, close) + rng.uniform(0.0, 0.05),
                low=min(price, close) - rng.uniform(0.0, 0.05),
                close=close,
            )
        )
        price = close
    return out


# ---------------------------------------------------------------------------
# Recent movement and the clock
# ---------------------------------------------------------------------------


def test_recent_movement_reads_only_seen_bars_of_its_own_session():
    rng = random.Random(1)
    tue = _bars(OPEN - timedelta(days=1), 390, rng)
    wed = _bars(OPEN, 390, rng)
    recent = RecentMovement(SessionBars(tue + wed))
    raw = tue + wed

    def brute(i: int, start: int) -> tuple[float, ...]:
        scale = 10_000.0 / raw[i].close

        def avg(n: int) -> float:
            window = raw[max(start, i - n + 1) : i + 1]
            return float(scale * sum(b.high - b.low for b in window) / len(window))

        swing = raw[max(start, i - 29) : i + 1]
        session = raw[start : i + 1]
        return (
            avg(5),
            avg(15),
            avg(60),
            scale * (max(b.high for b in swing) - min(b.low for b in swing)),
            scale * (max(b.high for b in session) - min(b.low for b in session)),
        )

    for i in (0, 3, 40, 200, 389):  # Tuesday
        assert recent.at(i) == pytest.approx(brute(i, 0))
    for i in (390, 392, 420, 700):  # Wednesday: never reaches back into Tuesday
        assert recent.at(i) == pytest.approx(brute(i, 390))
    first = recent.at(390)
    one = 10_000.0 * (raw[390].high - raw[390].low) / raw[390].close
    assert first == pytest.approx((one,) * 5)


def test_time_slots_are_five_minutes_through_the_first_half_hour():
    def slot(h: int, m: int) -> int:
        return time_slot(OPEN.replace(hour=h, minute=m))

    # 13:30 UTC == 09:30 ET on 2026-09-23.
    assert [slot(13, 30), slot(13, 34), slot(13, 35), slot(13, 59)] == [0, 0, 1, 5]
    assert [slot(14, 0), slot(14, 29), slot(14, 30), slot(19, 59)] == [6, 6, 7, 17]


# ---------------------------------------------------------------------------
# The ratio
# ---------------------------------------------------------------------------


def _frame(n_days: int = 6, iterations: int = 50) -> MoveFrame:
    rng = random.Random(7)
    rows = []
    for d in range(n_days):
        start = OPEN - timedelta(days=d)
        for sym in SYMBOLS:
            bars = _bars(start, 390, rng)
            readings = [
                Reading(
                    timestamp=start + timedelta(minutes=m),
                    inputs=BiasInput(
                        netGEX=rng.choice((-50.0, 50.0)),
                        gexGradient=0.0,
                        tapeFlow=rng.choice((-50.0, 0.0, 50.0)),
                        vannaCharm=30.0,
                        odtePositioning=0.0,
                        positioningTrap=0.0,
                        trapDetection=0.0,
                        gammaVWAP=0.0,
                        msi=50.0,
                    ),
                    stored_state=None,
                )
                for m in range(390)
            ]
            r, _ = build(sym, readings, SessionBars(bars))
            rows.extend(r)
    return MoveFrame(rows, SYMBOLS, iterations=iterations)


def test_the_ratio_is_the_least_squares_coefficient():
    frame = _frame()
    mask = frame.state_mask(("TREND_UP", "TREND_DOWN"))
    assert 0 < mask.sum() < len(mask)
    design = frame.design(PRIMARY_HORIZON, COMPARABLE)
    coef, _, _ = design.fit(mask)
    x = np.hstack([design.x, mask[design.keep].astype(float)[:, None]])
    direct = np.linalg.lstsq(x, design.ly, rcond=None)[0][-1]
    assert float(coef) == pytest.approx(direct, abs=1e-7)


def test_a_state_that_is_only_the_clock_cannot_be_measured_and_does_not_crash():
    # Every minute of 09:30-09:34 and nothing else: after the time-of-day
    # columns nothing is left of it, so there is no ratio -- and no error.
    frame = _frame()
    point, boot, count = frame.design(PRIMARY_HORIZON, COMPARABLE).fit(frame.slot == 0)
    assert count.sum() > 0
    assert math.isnan(float(point)) and np.isnan(boot).all()
    # Plenty of minutes and days, but no ratio: the reason must say why.
    group = GroupResult("clock", "clock", None, {}, {})
    for scope in ("pooled", *SYMBOLS):
        group.cells[(COMPARABLE, PRIMARY_HORIZON, scope)] = RatioCell(
            COMPARABLE, PRIMARY_HORIZON, scope, MIN_MINUTES, MIN_SESSIONS, Estimate(None)
        )
    group.cells[(COMPARABLE, CONFIRM_HORIZON, "pooled")] = group.cells[
        (COMPARABLE, PRIMARY_HORIZON, "pooled")
    ]
    apply_verdicts([group], SYMBOLS)
    assert group.verdict is not None and group.verdict.decision == TOO_RARE
    assert "cannot be told apart" in group.verdict.reasons[0]


def test_without_adjustment_the_ratio_is_the_geometric_mean_ratio():
    frame = _frame()
    mask = frame.state_mask(("CHOP",)) & (frame.sym_idx == 0)
    coef, _, _ = frame.design(PRIMARY_HORIZON, BY_SYMBOL, "SPY").fit(mask)
    y = frame.range[PRIMARY_HORIZON]
    spy = (frame.sym_idx == 0) & np.isfinite(y)
    logs = np.log(y)
    expected = logs[spy & mask].mean() - logs[spy & ~mask].mean()
    assert float(coef) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


def _cell(value: float, p: float = 0.001, minutes: int = 5000, days: int = 40) -> RatioCell:
    return RatioCell(
        COMPARABLE,
        PRIMARY_HORIZON,
        "pooled",
        minutes,
        days,
        Estimate(value, value * 0.9, value * 1.1, p),
    )


def _group(
    pooled: float,
    spy: float,
    spx: float,
    confirm: float,
    *,
    p: float = 0.001,
    minutes: int = 5000,
    days: int = 40,
    key: str = "trap",
) -> GroupResult:
    cells: dict[tuple[str, Horizon, str], RatioCell] = {
        (COMPARABLE, PRIMARY_HORIZON, "pooled"): _cell(pooled, p, minutes, days),
        (COMPARABLE, PRIMARY_HORIZON, "SPY"): _cell(spy),
        (COMPARABLE, PRIMARY_HORIZON, "SPX"): _cell(spx),
        (COMPARABLE, CONFIRM_HORIZON, "pooled"): _cell(confirm),
    }
    return GroupResult(key, key, None, {}, cells)


def _decide(*groups: GroupResult) -> list[str]:
    fillers = [_group(1.0, 1.0, 1.0, 1.0, p=0.9, key=f"f{i}") for i in range(4 - len(groups))]
    family = [*groups, *fillers]
    apply_verdicts(family, SYMBOLS)
    return [g.verdict.decision if g.verdict else "" for g in family[: len(groups)]]


def test_verdict_rule_is_the_pre_registered_one():
    assert _decide(_group(1.30, 1.2, 1.4, 1.2)) == [MOVES_MORE]
    assert _decide(_group(1.10, 1.1, 1.1, 1.1)) == [MOVES_MORE]
    assert _decide(_group(1.06, 1.05, 1.07, 1.02)) == [SLIGHTLY_MORE]
    assert _decide(_group(0.80, 0.8, 0.8, 0.9)) == [MOVES_LESS]
    assert _decide(_group(0.95, 0.9, 0.97, 0.99)) == [SLIGHTLY_LESS]
    # A symbol or the 60-minute ratio on the other side: nothing.
    assert _decide(_group(1.30, 0.99, 1.4, 1.2)) == [NOTHING]
    assert _decide(_group(1.30, 1.2, 1.4, 0.98)) == [NOTHING]
    # Not significant: nothing, however large.
    assert _decide(_group(1.30, 1.2, 1.4, 1.2, p=0.2)) == [NOTHING]
    assert _decide(_group(1.30, 1.2, 1.4, 1.2, days=MIN_SESSIONS - 1)) == [TOO_RARE]
    assert _decide(_group(1.30, 1.2, 1.4, 1.2, minutes=MIN_MINUTES - 1)) == [TOO_RARE]


def test_holm_is_always_across_all_four():
    # With m = 4 the second-smallest p must clear 0.05 / 3 = 0.0167, so 0.02
    # fails; had the too-rare group been dropped (m = 3) the bar would be
    # 0.025 and it would pass.
    rare = _group(2.0, 2.0, 2.0, 2.0, p=0.0005, days=MIN_SESSIONS - 1, key="rare")
    strong = _group(1.3, 1.2, 1.4, 1.2, p=0.001, key="strong")
    marginal = _group(1.3, 1.2, 1.4, 1.2, p=0.02, key="marginal")
    assert _decide(rare, strong, marginal) == [TOO_RARE, MOVES_MORE, NOTHING]


def test_matches_copy():
    assert matches_copy("more", MOVES_MORE) == "yes"
    assert matches_copy("more", SLIGHTLY_MORE) == "only slightly"
    assert matches_copy("less", MOVES_MORE) == "opposite"
    assert matches_copy("less", NOTHING) == "no"
    assert matches_copy("less", TOO_RARE) == "can't tell"
    assert matches_copy(None, MOVES_MORE) == "no claim"
    assert [g.claim for g in GROUPS] == [None, "more", "less", "less"]


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------


def test_selftest_inputs_produce_the_states_they_are_meant_to():
    from research.trade_bias_movement.selftest import _inputs

    rng = random.Random(3)
    for state in STATE_TITLES:
        for short in (True, False):
            assert current_state(_inputs(state, short, rng)) == state


def test_machinery_returns_the_right_verdicts_on_known_worlds():
    from research.trade_bias_movement.selftest import check, run_selftest

    results = run_selftest()
    for mode, result, passed in results:
        assert passed, (mode, [c for c in check(mode, result) if not c[3]])
    assert [mode for mode, _, _ in results] == ["informative", "confounded", "null"]


def test_a_context_cell_on_too_few_days_prints_no_interval():
    from research.trade_bias_movement.report import _cell as render_cell

    few = RatioCell(COMPARABLE, 30, "pooled", 160, 8, Estimate(1.52, 1.1, 2.0, 0.01))
    assert render_cell(few) == "1.52 (160 min, 8 days)"
    enough = RatioCell(COMPARABLE, 30, "pooled", 900, MIN_SESSIONS, Estimate(1.2, 1.1, 1.3, 0.01))
    assert render_cell(enough) == "1.20 [1.10,1.30] (900)"


def test_cli_run_end_to_end_on_a_faked_database(monkeypatch, capsys, tmp_path):
    from research.short_gamma_trend import sources
    from research.trade_bias_movement import cli
    from research.trade_bias_movement.selftest import generate_world

    readings, bars = generate_world("informative", sessions=16, seed=7)
    first = readings["SPY"][0].timestamp
    last = readings["SPY"][-1].timestamp

    def span(conn, symbol: str) -> tuple[Optional[datetime], Optional[datetime], int]:
        return first, last, len(readings[symbol])

    monkeypatch.setattr(cli, "_connect", lambda: nullcontext(object()))
    monkeypatch.setattr(sources, "archive_span", span)
    monkeypatch.setattr(sources, "load_readings", lambda conn, sym, start, end: readings[sym])
    monkeypatch.setattr(sources, "load_bars", lambda conn, sym, start, end: bars[sym])

    out_json = tmp_path / "movement.json"
    assert cli.main(["run", "--iterations", "200", "--json", str(out_json)]) == 0
    text = capsys.readouterr().out
    assert "VERDICTS" in text
    for g in GROUPS:
        assert g.title in text
    payload = json.loads(out_json.read_text())
    assert [g["key"] for g in payload["groups"]] == [g.key for g in GROUPS]
    assert payload["counts"]["rows"] == 2 * 16 * 390
    assert all(
        math.isfinite(g["cells"]["comparable|30|pooled"]["ratio"]["value"])
        for g in payload["groups"][:3]
    )
