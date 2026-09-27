"""The nine-input direction study (research/trade_bias_inputs).

Research-only, but its verdicts decide which inputs the Trade Bias panel
should trust, so the parts that could quietly lie are pinned here:

* the bars are production's defaults, and read the same payload keys the
  replay parses;
* a call is strictly past its bar, and the reading bands split exactly where
  the call and production's regime labels do;
* a regime's excess is measured against that regime's own drift;
* the verdict follows the pre-registered rule, with Holm's correction always
  across all nine;
* the machinery returns the right verdicts on worlds whose answer is known,
  and the command line runs end to end.
"""

from __future__ import annotations

import json
import random
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pytest

from research.msi_regime_excursion.excursion import Bar
from research.short_gamma_trend.outcomes import Horizon, SessionBars
from research.short_gamma_trend.rule import _INPUT_FIELDS
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import Cell, Estimate, Frame, build_rows, score_blocks
from research.trade_bias_inputs import calls
from research.trade_bias_inputs.calls import INPUTS, INPUTS_BY_KEY
from research.trade_bias_inputs.study import (
    CONFIRM_HORIZON,
    MIN_CALLS,
    MIN_SESSIONS,
    NOTHING,
    PREDICTS,
    PRIMARY_HORIZON,
    TOO_RARE,
    WRONG_WAY,
    InputResult,
    apply_verdicts,
    holm,
    prior_move_splits,
    regime_pools,
)
from src.signals.scoring_engine import ScoringEngine
from src.signals.trade_bias import bias
from src.signals.trade_bias.bias import BiasInput

# 2026-09-23 (Wed, EDT): 09:30 ET == 13:30 UTC.
OPEN = datetime(2026, 9, 23, 13, 30, tzinfo=timezone.utc)
SYMBOLS = ["SPY", "SPX"]


# ---------------------------------------------------------------------------
# The calls
# ---------------------------------------------------------------------------


def test_bars_are_production_defaults_and_keys_match_the_replay_parser():
    assert (calls.STRONG, calls.MODERATE, calls.DOMINANT) == (
        bias.STRONG,
        bias.MODERATE,
        bias.DOMINANT,
    )
    assert {r.key: r.field for r in INPUTS} == _INPUT_FIELDS
    assert len(INPUTS) == 9


def test_a_call_is_strictly_past_the_bar():
    tape = INPUTS_BY_KEY["tape_flow"]
    assert [tape.call(v) for v in (25.0, 25.01, -25.0, -25.01, 0.0, None)] == [0, 1, 0, -1, 0, 0]
    vanna = INPUTS_BY_KEY["vanna_charm"]
    assert [vanna.call(v) for v in (12.0, 12.5, -12.5)] == [0, 1, -1]
    gex = INPUTS_BY_KEY["net_gex"]
    assert [gex.call(v) for v in (50.0, -50.0)] == [1, -1]
    msi = INPUTS_BY_KEY["msi"]
    assert [msi.call(v) for v in (62.0, 62.5, 50.0, 38.0, 37.5)] == [0, 1, 0, 0, -1]
    assert msi.value(BiasInput(msi=float("nan"))) is None
    assert tape.value(None) is None


def test_bands_split_where_the_call_and_production_do():
    rng = random.Random(3)
    for rule in INPUTS:
        for _ in range(2_000):
            if rule.key == "net_gex":
                v = rng.choice((-50.0, 50.0))
                assert rule.band(v) == (0 if v < 0 else 1)
                continue
            if rule.key == "msi":
                v = rng.uniform(0.0, 100.0)
                labels = ("high_risk_reversal", "chop_range", "controlled_trend", "trend_expansion")
                assert labels[rule.band(v)] == ScoringEngine._regime_label(v)
                continue
            v = rng.choice((1, -1)) * rng.choice(
                (rng.uniform(0, rule.bar), rng.uniform(rule.bar, 65), rng.uniform(65, 100))
            )
            b = rule.band(v)
            assert {0: -1, 1: -1, 2: 0, 3: 1, 4: 1}[b] == rule.call(v), (rule.key, v)
            assert (b in (0, 4)) == (abs(v) > calls.DOMINANT), (rule.key, v)
        assert rule.band(None) is None
        assert len(rule.band_labels) == (
            2 if rule.key == "net_gex" else 4 if rule.key == "msi" else 5
        )


def test_prior_move_splits_partition_the_calls():
    rng = np.random.default_rng(5)
    dirs = rng.integers(-1, 2, size=500).astype(np.int8)
    prior = rng.integers(-1, 2, size=500).astype(np.int8)
    splits = prior_move_splits(dirs, prior)
    total = sum(np.abs(s) for s in splits.values())
    assert np.array_equal(total, np.abs(dirs))
    for split in splits.values():
        on = split != 0
        assert np.array_equal(split[on], dirs[on])
    agree = splits["with the move"] != 0
    assert np.all(dirs[agree] == prior[agree])
    against = splits["against it"] != 0
    assert np.all(dirs[against] == -prior[against])
    assert np.all(prior[splits["no prior move"] != 0] == 0)


# ---------------------------------------------------------------------------
# The estimate
# ---------------------------------------------------------------------------


def _bars(start: datetime, closes: list[float]) -> list[Bar]:
    out = []
    prev = closes[0]
    for i, c in enumerate(closes):
        out.append(
            Bar(
                ts=start + timedelta(minutes=i),
                open=prev,
                high=max(prev, c) + 0.1,
                low=min(prev, c) - 0.1,
                close=c,
            )
        )
        prev = c
    return out


def _rows(n_days: int = 3):
    rng = random.Random(9)
    rows = []
    for d in range(n_days):
        start = OPEN - timedelta(days=d)
        for sym in SYMBOLS:
            closes = [100.0]
            for m in range(389):
                # Short gamma in the morning, drifting down; long gamma after.
                drift = -0.0002 if m < 200 else 0.0001
                closes.append(closes[-1] * (1 + drift + rng.gauss(0, 0.0008)))
            readings = []
            for m in range(390):
                readings.append(
                    Reading(
                        timestamp=start + timedelta(minutes=m),
                        inputs=BiasInput(
                            netGEX=-50.0 if m < 200 else 50.0,
                            gexGradient=0.0,
                            tapeFlow=rng.choice((-40.0, 0.0, 40.0)),
                            vannaCharm=0.0,
                            odtePositioning=0.0,
                            positioningTrap=0.0,
                            trapDetection=0.0,
                            gammaVWAP=0.0,
                            msi=50.0,
                        ),
                        stored_state=None,
                    )
                )
            r, _ = build_rows(sym, readings, SessionBars(_bars(start, closes)))
            rows.extend(r)
    return rows


def test_a_regime_is_scored_against_its_own_drift():
    frame = Frame(_rows(), SYMBOLS, iterations=50)
    rule = INPUTS_BY_KEY["tape_flow"]
    dirs = np.array([rule.call(rule.value(r.inputs)) for r in frame.rows], dtype=np.int8)
    pool = regime_pools(frame.rows)["short gamma"]
    assert 0 < np.count_nonzero(pool) < len(frame.rows)
    cell = score_blocks(frame, "t", frame.blocks_for("t", dirs, 30, pool=pool), 30)
    total, n = 0.0, 0
    for sym in SYMBOLS:
        in_pool = [
            r.out.ret[30]
            for r, p in zip(frame.rows, pool)
            if p and r.symbol == sym and r.out.ret[30] is not None
        ]
        drift = sum(in_pool) / len(in_pool)
        for r, p, d in zip(frame.rows, pool, dirs):
            if p and d and r.symbol == sym and r.out.ret[30] is not None:
                total += d * (r.out.ret[30] - drift)
                n += 1
    assert cell.n_calls == n
    assert cell.excess_bps.value == pytest.approx(total / n)


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


def _cell(value, p=0.001, n=5000, days=40, horizon=30, scope="pooled") -> Cell:
    e = Estimate(value, value - 1.0, value + 1.0, p)
    return Cell("x", horizon, scope, n, days, e, e, Estimate(0.5), Estimate(1.0), value)


def _result(
    pooled: float,
    spy: float,
    spx: float,
    confirm: float,
    *,
    p: float = 0.001,
    n: int = 5000,
    days: int = 40,
    key: str = "tape_flow",
) -> InputResult:
    cells: dict[tuple[Horizon, str], Cell] = {
        (PRIMARY_HORIZON, "pooled"): _cell(pooled, p, n, days),
        (PRIMARY_HORIZON, "SPY"): _cell(spy, scope="SPY"),
        (PRIMARY_HORIZON, "SPX"): _cell(spx, scope="SPX"),
        (CONFIRM_HORIZON, "pooled"): _cell(confirm, horizon=CONFIRM_HORIZON),
    }
    return InputResult(key, key, "", {}, {}, cells, Estimate(None), [], {}, {})


def _decide(*results: InputResult) -> list[str]:
    fillers = [_result(0.1, 0.1, 0.1, 0.1, p=0.9, key=f"noise{i}") for i in range(9 - len(results))]
    family = [*results, *fillers]
    apply_verdicts(family, SYMBOLS)
    return [r.verdict.decision if r.verdict else "" for r in family[: len(results)]]


def test_verdict_rule_is_the_pre_registered_one():
    assert _decide(_result(2.0, 1.0, 3.0, 1.0)) == [PREDICTS]
    assert _decide(_result(-2.0, -1.0, -3.0, -1.0)) == [WRONG_WAY]
    # One symbol against it, or the 60m confirmation missing: nothing.
    assert _decide(_result(2.0, -0.1, 3.0, 1.0)) == [NOTHING]
    assert _decide(_result(2.0, 1.0, 3.0, -0.2)) == [NOTHING]
    assert _decide(_result(-2.0, -1.0, 0.3, -1.0)) == [NOTHING]
    # Not significant: nothing, however it leans.
    assert _decide(_result(2.0, 1.0, 3.0, 1.0, p=0.2)) == [NOTHING]
    assert _decide(_result(2.0, 1.0, 3.0, 1.0, days=MIN_SESSIONS - 1)) == [TOO_RARE]
    assert _decide(_result(2.0, 1.0, 3.0, 1.0, n=MIN_CALLS - 1)) == [TOO_RARE]


def test_holm_steps_down_and_stops_at_the_first_miss():
    # m = 4: the bars are 0.05/4, 0.05/3, 0.05/2, 0.05.
    assert holm([0.001, 0.0062, 0.0072, 0.9]) == [True, True, True, False]
    # 0.02 misses 0.05/3, so 0.049 is never reached although it clears 0.05.
    assert holm([0.02, 0.001, 0.049, 0.03]) == [False, True, False, False]
    assert holm([]) == []


def test_holm_is_always_across_all_nine():
    # With m = 9 the second-smallest p must clear 0.05 / 8 = 0.00625, so 0.0065
    # fails; had the too-rare input been dropped (m = 8) the bar would be
    # 0.05 / 7 = 0.00714 and it would pass.
    rare = _result(5.0, 5.0, 5.0, 5.0, p=0.0005, days=MIN_SESSIONS - 1, key="rare")
    strong = _result(2.0, 1.0, 3.0, 1.0, p=0.001, key="strong")
    marginal = _result(2.0, 1.0, 3.0, 1.0, p=0.0065, key="marginal")
    assert _decide(rare, strong, marginal) == [TOO_RARE, PREDICTS, NOTHING]
    assert rare.p_primary is None and not rare.significant
    assert _decide(strong, _result(2.0, 1.0, 3.0, 1.0, p=0.006, key="m2")) == [PREDICTS, PREDICTS]


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------


def test_machinery_returns_the_right_verdicts_on_known_worlds():
    from research.trade_bias_inputs.selftest import check, run_selftest

    results = run_selftest()
    for mode, result, passed in results:
        assert passed, (mode, [c for c in check(mode, result) if not c[3]])
    assert [mode for mode, _, _ in results] == ["mixed", "null"]


def test_cli_run_end_to_end_on_a_faked_database(monkeypatch, capsys, tmp_path):
    from research.short_gamma_trend import sources
    from research.trade_bias_inputs import cli
    from research.trade_bias_inputs.selftest import generate_world

    readings, bars = generate_world("mixed", sessions=16, seed=7)
    first = readings["SPY"][0].timestamp
    last = readings["SPY"][-1].timestamp

    def span(conn, symbol: str) -> tuple[Optional[datetime], Optional[datetime], int]:
        return first, last, len(readings[symbol])

    monkeypatch.setattr(cli, "_connect", lambda: nullcontext(object()))
    monkeypatch.setattr(sources, "archive_span", span)
    monkeypatch.setattr(sources, "load_readings", lambda conn, sym, start, end: readings[sym])
    monkeypatch.setattr(sources, "load_bars", lambda conn, sym, start, end: bars[sym])

    out_json = tmp_path / "inputs.json"
    assert cli.main(["run", "--iterations", "200", "--json", str(out_json)]) == 0
    text = capsys.readouterr().out
    assert "VERDICTS" in text
    for rule in INPUTS:
        assert rule.title in text
    payload = json.loads(out_json.read_text())
    assert [i["key"] for i in payload["inputs"]] == [r.key for r in INPUTS]
    assert payload["counts"]["rows"] == 2 * 16 * 390


def test_a_context_cell_on_too_few_days_prints_no_interval():
    from research.trade_bias_inputs.report import _cell_n

    few = _cell(22.2, n=1, days=1)
    assert _cell_n(few) == "+22.2 (1 min, 1 day)"
    assert _cell_n(_cell(-1.5, n=801, days=9)) == "-1.5 (801 min, 9 days)"
    enough = _cell(1.0, n=900, days=MIN_SESSIONS)
    assert _cell_n(enough) == "+1.0 [+0.0,+2.0] (900)"
