"""The short-gamma trend replay (research/short_gamma_trend).

Research-only, but its verdict decides a customer-facing label, so the parts
that could quietly lie are pinned here:

* the vote mirror reproduces production's ``compute_bias`` exactly, so the
  replay differs from the live panel only in the rows under test;
* the candidate never touches a non-CHOP state;
* outcome windows never include the entry bar, never cross the close or the
  overnight gap;
* the drift-adjusted estimate is the arithmetic it claims to be;
* the verdict follows the pre-registered rule, and the machinery returns the
  right verdict on worlds whose answer is known.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pytest

from research.msi_regime_excursion.excursion import Bar
from research.short_gamma_trend.outcomes import REST, SessionBars, measure
from research.short_gamma_trend.rule import (
    VARIANTS,
    bias_input_from_payload,
    candidate_state,
    current_state,
    legacy_state,
    votes,
)
from research.short_gamma_trend.sources import Reading
from research.short_gamma_trend.study import (
    LABELS_BY_KEY,
    MIN_ROWS,
    MIN_SESSIONS,
    Cell,
    Estimate,
    Frame,
    build_rows,
    decide,
    directions,
    score,
)
from src.signals.trade_bias.bias import BiasInput

# 2026-09-23 (Wed, EDT): 09:30 ET == 13:30 UTC.
OPEN = datetime(2026, 9, 23, 13, 30, tzinfo=timezone.utc)
_FIELDS = (
    "netGEX",
    "gexGradient",
    "tapeFlow",
    "vannaCharm",
    "odtePositioning",
    "positioningTrap",
    "trapDetection",
    "gammaVWAP",
    "msi",
)


def _random_input(rng: random.Random) -> BiasInput:
    def val(name: str) -> Optional[float]:
        if rng.random() < 0.12:
            return None
        if name == "netGEX":
            return rng.choice((-50.0, 50.0))
        if name == "msi":
            return rng.uniform(0.0, 100.0)
        # Cluster around the vote thresholds (12, 25, 65) so every branch is hit.
        return rng.choice((1, -1)) * rng.choice(
            (rng.uniform(0, 12), rng.uniform(12, 25), rng.uniform(25, 65), rng.uniform(65, 100))
        )

    return BiasInput(**{f: val(f) for f in _FIELDS})


def _state_from_votes(inp: BiasInput) -> str:
    v = votes(inp)
    if v.is_short_gamma and v.bullish_flow and v.bearish_structure:
        return "TRAP_REVERSAL"
    if v.is_short_gamma and v.bearish_flow and v.bullish_structure:
        return "TRAP_SQUEEZE"
    if v.is_long_gamma and v.bullish_flow:
        return "TREND_UP"
    if v.is_long_gamma and v.bearish_flow:
        return "TREND_DOWN"
    return "CHOP" if v.available >= 4 else "UNKNOWN"


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


def test_vote_mirror_reproduces_production_exactly():
    rng = random.Random(7)
    for _ in range(20_000):
        inp = _random_input(rng)
        assert _state_from_votes(inp) == current_state(inp), inp


def test_candidate_never_touches_a_non_chop_state():
    rng = random.Random(11)
    relabeled = 0
    for _ in range(20_000):
        inp = _random_input(rng)
        cur = current_state(inp)
        for variant in VARIANTS:
            for prior in (-1, 0, 1):
                cand = candidate_state(inp, variant, current=cur, prior_move=prior)
                if cur != "CHOP":
                    assert cand == cur
                elif cand != cur:
                    relabeled += 1
                    assert cand in ("SG_TREND_UP", "SG_TREND_DOWN")
                    assert votes(inp).is_short_gamma
    assert relabeled > 0


def test_flow_variant_relabels_the_2026_09_23_spy_minute():
    # SPY 12:50 ET from the session readout: total net GEX negative, tape and
    # 0DTE bearish, vanna/charm flat. Production: CHOP.
    inp = BiasInput(
        netGEX=-50,
        gexGradient=-4,
        tapeFlow=-26,
        vannaCharm=9,
        odtePositioning=-15,
        positioningTrap=0,
        trapDetection=0,
        gammaVWAP=0,
        msi=59.1,
    )
    assert current_state(inp) == "CHOP"
    assert candidate_state(inp, "flow") == "SG_TREND_DOWN"
    assert candidate_state(inp, "aligned") == "CHOP"  # structure does not agree
    assert candidate_state(inp, "momentum", prior_move=-1) == "SG_TREND_DOWN"
    assert candidate_state(inp, "momentum", prior_move=0) == "CHOP"


def test_gradient_veto_keeps_the_candidate_out():
    # SPY 10:10 ET: net GEX negative but the gradient (+13) is past MODERATE,
    # so production reads neither short nor long gamma -- and neither does
    # the candidate, whatever the flow says.
    inp = BiasInput(
        netGEX=-50,
        gexGradient=13,
        tapeFlow=-29,
        vannaCharm=15,
        odtePositioning=-17,
        positioningTrap=0,
        trapDetection=0,
        gammaVWAP=0,
        msi=42.3,
    )
    assert not votes(inp).is_short_gamma
    assert current_state(inp) == "CHOP"
    for variant in VARIANTS:
        assert candidate_state(inp, variant, prior_move=-1) == "CHOP"


def test_legacy_state_is_the_pre_fix_msi_gate():
    down = dict(netGEX=50, gexGradient=60, tapeFlow=-80, vannaCharm=-60, odtePositioning=-60)
    assert current_state(BiasInput(**down, msi=35)) == "TREND_DOWN"
    assert legacy_state(BiasInput(**down, msi=35, positioningTrap=0)) == "CHOP"
    assert legacy_state(BiasInput(**down, msi=5)) == "TREND_DOWN"
    # Fewer than four inputs: the old fall-through was UNKNOWN, not CHOP. (A
    # dominant tape reading carries the two-vote flow majority on its own.)
    three = BiasInput(netGEX=50, tapeFlow=-80, msi=35)
    assert current_state(three) == "TREND_DOWN"
    assert legacy_state(three) == "UNKNOWN"
    up = dict(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=60, odtePositioning=60, msi=35)
    assert legacy_state(BiasInput(**up)) == "TREND_UP"


def test_payload_inputs_parse_and_tolerate_junk():
    inp = bias_input_from_payload(
        {"net_gex": -50, "tape_flow": "-29.5", "msi": None, "gamma_vwap": "junk"}
    )
    assert inp is not None
    assert (inp.netGEX, inp.tapeFlow, inp.msi, inp.gammaVWAP) == (-50.0, -29.5, None, None)
    assert bias_input_from_payload(None) is None
    assert bias_input_from_payload({}) is None


# ---------------------------------------------------------------------------
# Outcomes
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


def test_window_excludes_the_entry_bar_and_needs_to_fit_before_the_close():
    closes = [100.0 + 0.01 * i for i in range(390)]
    bars = SessionBars(_bars(OPEN, closes))
    out = measure(bars, OPEN + timedelta(minutes=100))
    assert out is not None
    assert out.entry == closes[100]
    # 30m return runs from the entry close to bar 130's close.
    assert out.ret[30] == pytest.approx(1e4 * (closes[130] - closes[100]) / closes[100])
    late = measure(bars, OPEN + timedelta(minutes=370))  # 15:40 ET: 30m does not fit
    assert late is not None and late.ret[30] is None and late.ret[15] is not None
    last = measure(bars, OPEN + timedelta(minutes=389))  # 15:59: nothing forward
    assert last is not None and last.ret[REST] is None


def test_rest_of_session_matches_a_brute_force_scan():
    rng = random.Random(3)
    closes = [100.0]
    for _ in range(389):
        closes.append(closes[-1] * (1 + rng.gauss(0, 0.001)))
    raw = _bars(OPEN, closes)
    bars = SessionBars(raw)
    for i in (0, 57, 200, 388):
        out = measure(bars, raw[i].ts)
        window = raw[i + 1 :]
        hi, lo = max(b.high for b in window), min(b.low for b in window)
        entry = closes[i]
        assert out.up[REST] == pytest.approx(1e4 * max(0.0, hi - entry) / entry)
        assert out.down[REST] == pytest.approx(1e4 * max(0.0, entry - lo) / entry)
        assert out.ret[REST] == pytest.approx(1e4 * (closes[-1] - entry) / entry)


def test_extended_hours_and_the_overnight_gap_are_not_outcomes():
    # Tuesday session, Tuesday post-market, Wednesday session.
    tue = OPEN - timedelta(days=1)
    bars = _bars(tue, [100.0] * 390)
    bars += _bars(tue + timedelta(minutes=390), [120.0] * 60)  # 16:00-16:59 ET
    bars += _bars(OPEN, [90.0] * 390)
    sb = SessionBars(bars)
    out = measure(sb, tue + timedelta(minutes=380))  # 15:50 Tuesday
    assert out.ret[REST] == pytest.approx(0.0)  # never sees 120 or 90
    first = measure(sb, OPEN + timedelta(minutes=10))  # 09:40 Wednesday
    assert first.prior_move == 0  # no look back across the night


def test_stale_entry_bar_is_not_scored():
    bars = SessionBars(_bars(OPEN, [100.0] * 30))
    assert measure(bars, OPEN + timedelta(minutes=45)) is None


# ---------------------------------------------------------------------------
# The estimate
# ---------------------------------------------------------------------------


def _study_rows(n_days: int = 3) -> list:
    rng = random.Random(5)
    rows = []
    for d in range(n_days):
        start = OPEN - timedelta(days=d)
        for sym in ("SPY", "SPX"):
            closes = [100.0]
            for _ in range(389):
                closes.append(closes[-1] * (1 + rng.gauss(0, 0.0008)))
            readings = []
            for m in range(390):
                flow = rng.choice((-1, 0, 1))
                readings.append(
                    Reading(
                        timestamp=start + timedelta(minutes=m),
                        inputs=BiasInput(
                            netGEX=rng.choice((-50.0, 50.0)),
                            gexGradient=0.0,
                            tapeFlow=50.0 * flow,
                            vannaCharm=30.0 * flow,
                            odtePositioning=30.0 * flow,
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


def test_excess_is_the_drift_adjusted_mean_it_claims_to_be():
    rows = _study_rows()
    frame = Frame(rows, ["SPY", "SPX"], iterations=50)
    label = LABELS_BY_KEY["flow"]
    cell = score(frame, label, 30)
    dirs = directions(frame.rows, label)
    total = 0.0
    n = 0
    for sym in ("SPY", "SPX"):
        rets = [r.out.ret[30] for r in frame.rows if r.symbol == sym and r.out.ret[30] is not None]
        drift = sum(rets) / len(rets)
        for row, d in zip(frame.rows, dirs):
            if row.symbol == sym and d and row.out.ret[30] is not None:
                total += d * (row.out.ret[30] - drift)
                n += 1
    assert cell.n_calls == n
    assert cell.excess_bps.value == pytest.approx(total / n)


def test_entries_are_the_first_minute_of_each_episode():
    rows = _study_rows(1)
    frame = Frame(rows, ["SPY", "SPX"], iterations=10)
    every = directions(frame.rows, LABELS_BY_KEY["flow"])
    first = directions(frame.rows, LABELS_BY_KEY["flow_entry"])
    assert np.count_nonzero(first) < np.count_nonzero(every)
    for i in np.flatnonzero(first):
        prev = frame.rows[i - 1] if i else None
        assert not (
            prev is not None
            and prev.symbol == frame.rows[i].symbol
            and every[i - 1] == every[i]
            and prev.states["flow"] == frame.rows[i].states["flow"]
        )


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


def _cell(value, lo, hi, n=5000, days=40, horizon=30, scope="pooled") -> Cell:
    e = Estimate(value, lo, hi, 0.01)
    return Cell("flow", horizon, scope, n, days, e, e, Estimate(0.5), Estimate(1.0), value)


def _cells(pooled30, spy, spx, pooled60, n=5000, days=40):
    return {
        ("flow", 30, "pooled"): _cell(*pooled30, n=n, days=days),
        ("flow", 30, "SPY"): _cell(spy, None, None, scope="SPY"),
        ("flow", 30, "SPX"): _cell(spx, None, None, scope="SPX"),
        ("flow", 60, "pooled"): _cell(pooled60, None, None, horizon=60),
    }


def test_verdict_rule_is_the_pre_registered_one():
    syms = ["SPY", "SPX"]
    assert decide(_cells((2.0, 0.5, 3.5), 1.0, 3.0, 1.0), syms).decision == "SHIP"
    # One symbol against it, or the 60m confirmation missing: not SHIP.
    assert decide(_cells((2.0, 0.5, 3.5), -0.1, 3.0, 1.0), syms).decision == "NO EVIDENCE"
    assert decide(_cells((2.0, 0.5, 3.5), 1.0, 3.0, -0.2), syms).decision == "NO EVIDENCE"
    assert decide(_cells((1.0, -0.5, 2.5), 1.0, 1.0, 1.0), syms).decision == "NO EVIDENCE"
    assert decide(_cells((-2.0, -3.5, -0.5), -1.0, -3.0, -1.0), syms).decision == "DON'T SHIP"
    few_days = _cells((2.0, 0.5, 3.5), 1.0, 3.0, 1.0, days=MIN_SESSIONS - 1)
    assert decide(few_days, syms).decision == "INSUFFICIENT"
    few_rows = _cells((2.0, 0.5, 3.5), 1.0, 3.0, 1.0, n=MIN_ROWS - 1)
    assert decide(few_rows, syms).decision == "INSUFFICIENT"


def test_machinery_returns_the_right_verdict_on_known_worlds():
    from research.short_gamma_trend.selftest import run_selftest

    results = run_selftest(iterations=400)
    assert [(mode, passed) for mode, _, passed in results] == [
        ("continuation", True),
        ("null", True),
        ("reversion", True),
    ]
