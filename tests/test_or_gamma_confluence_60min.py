"""The 60-minute follow-up: what a longer opening range changed in the harness.

Each test pins one thing that either broke, or silently changed meaning, when
the opening range went from 5 minutes to 60 -- or one piece of reporting the
follow-up needs (the customer's named rungs, H2's power, the cost hurdle at
each range length, the pre-registered verdict).  No database: every input is
invented, and no number here describes a market.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pytest

from research.msi_regime_excursion.excursion import ET, Bar
from research.or_gamma_confluence import sources
from research.or_gamma_confluence.cohorts import (
    MIN_TESTABLE_SESSIONS,
    build_cohorts,
    compare_to_baseline,
    on_rung,
)
from research.or_gamma_confluence.compare import (
    BUILD,
    DEFER,
    DONT_BUILD,
    UNTESTABLE,
    combined_bh,
    decide,
)
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.dataset import (
    DatasetWriter,
    build_datasets,
    build_session,
    read_jsonl,
    read_meta,
    read_null,
)
from research.or_gamma_confluence.events import (
    OUTCOME_CENSORED,
    OUTCOME_CONTINUATION,
    OUTCOME_REVERSAL,
    RESOLVED_OUTCOMES,
    extract_touch_events,
)
from research.or_gamma_confluence.instruments import spec
from research.or_gamma_confluence.null import gamblers_ruin_null, sign_flip_bars, surrogate_rows
from research.or_gamma_confluence.power import (
    min_detectable_effect,
    power_two_sided,
    se_from_ci,
    sessions_needed,
)
from research.or_gamma_confluence.ranges import build_ladder, build_opening_range
from research.or_gamma_confluence.report import _cost_hurdle, build_summary, render_markdown
from research.or_gamma_confluence.selftest import synthetic_session

SESSION = date(2026, 9, 8)


def _open() -> datetime:
    return datetime.combine(SESSION, time(9, 30), tzinfo=ET)


def _walk(n: int, seed: int, *, base: float = 6000.0, sigma: float = 2.0) -> list[Bar]:
    """A driftless minute walk with real intra-bar highs and lows."""
    rng = random.Random(seed)
    p, out = base, []
    for i in range(n):
        o = hi = lo = p
        for _ in range(10):
            p += rng.gauss(0.0, sigma / 3.0)
            hi, lo = max(hi, p), min(lo, p)
        out.append(Bar(_open() + timedelta(minutes=i), o, hi, lo, p))
    return out


# ── The surrogate null ───────────────────────────────────────────────


def test_sign_flip_keeps_the_prefix_and_every_bar_magnitude():
    bars = _walk(390, seed=1)
    synth = sign_flip_bars(bars, 60, random.Random(5))
    assert synth[:60] == bars[:60]
    assert len(synth) == len(bars)
    for a, b in zip(bars[60:], synth[60:]):
        assert b.ts == a.ts
        assert b.high - b.low == pytest.approx(a.high - a.low)
        assert abs(b.close - b.open) == pytest.approx(abs(a.close - a.open))
        assert b.low <= min(b.open, b.close) + 1e-9
        assert b.high >= max(b.open, b.close) - 1e-9


def test_sign_flip_chains_each_bar_from_the_previous_rebuilt_close():
    bars = _walk(120, seed=2)
    synth = sign_flip_bars(bars, 60, random.Random(9))
    assert synth[60].open == pytest.approx(bars[59].close)
    for prev, cur in zip(synth[60:], synth[61:]):
        assert cur.open == pytest.approx(prev.close)


def test_sign_flip_actually_flips_some_bars_and_keeps_others():
    bars = _walk(390, seed=3)
    synth = sign_flip_bars(bars, 60, random.Random(11))
    same = sum(
        1 for a, b in zip(bars[60:], synth[60:]) if (a.close - a.open) * (b.close - b.open) > 0
    )
    assert 100 < same < 230  # 330 coins; 165 expected


def test_surrogates_are_reproducible_and_seeded_per_session_not_per_run():
    cfg = ResearchConfig(opening_range_minutes=60, null_replicas=4)
    bars = _walk(390, seed=4)
    orange, _ = build_opening_range(bars, SESSION, "SPX", cfg)
    ladder = build_ladder(orange, cfg)
    a = surrogate_rows("SPX", bars, orange, ladder, cfg)
    b = surrogate_rows("SPX", bars, orange, ladder, cfg)
    assert a == b and a
    assert {r["replica"] for r in a} <= set(range(4))
    other = surrogate_rows("SPX", bars, orange, ladder, cfg.variant(null_seed=1))
    assert other != a


def test_surrogates_measure_against_the_real_ladder():
    """The range a trader saw is the range every replay is judged on."""
    cfg = ResearchConfig(opening_range_minutes=60, null_replicas=3)
    bars = _walk(390, seed=5)
    orange, _ = build_opening_range(bars, SESSION, "SPX", cfg)
    ladder = build_ladder(orange, cfg)
    rows = surrogate_rows("SPX", bars, orange, ladder, cfg)
    assert rows
    assert {r["or_width"] for r in rows} == {orange.width}
    rung_ks = {r.k for r in ladder.rungs}
    for r in rows:
        assert r["rung_k"] in rung_ks
        assert r["extension_k"] == r["rung_k"]
        assert r["minutes_since_or"] >= 0


def test_zero_replicas_builds_no_surrogates():
    cfg = ResearchConfig(opening_range_minutes=60, null_replicas=0)
    bars = _walk(390, seed=6)
    orange, _ = build_opening_range(bars, SESSION, "SPX", cfg)
    assert surrogate_rows("SPX", bars, orange, build_ladder(orange, cfg), cfg) == []


def test_negative_replicas_are_refused():
    with pytest.raises(ValueError, match="null_replicas"):
        ResearchConfig(null_replicas=-1)


def test_on_a_driftless_walk_the_surrogate_null_matches_the_real_rate():
    """Both paths are the same random walk, so their resolved reversal rates
    can only differ by noise. A 5-minute range gives enough touches to hold
    them within a few points on a fixed seed."""
    cfg = ResearchConfig(opening_range_minutes=5, null_replicas=3)
    real = surr = 0
    real_n = surr_n = 0
    for i in range(120):
        bars = _walk(390, seed=100 + i)
        d = SESSION + timedelta(days=i)
        bars = [Bar(b.ts + timedelta(days=i), b.open, b.high, b.low, b.close) for b in bars]
        orange, _ = build_opening_range(bars, d, "SPX", cfg)
        if orange is None:
            continue
        ladder = build_ladder(orange, cfg)
        for ev in extract_touch_events("SPX", orange, ladder, bars, cfg):
            if ev.outcome in RESOLVED_OUTCOMES:
                real_n += 1
                real += ev.outcome == OUTCOME_REVERSAL
        for r in surrogate_rows("SPX", bars, orange, ladder, cfg):
            if r["outcome"] in RESOLVED_OUTCOMES:
                surr_n += 1
                surr += r["outcome"] == OUTCOME_REVERSAL
    assert real_n > 500 and surr_n > 1500
    assert abs(real / real_n - surr / surr_n) < 0.05


def test_gamblers_ruin_null_is_clipped_and_needs_an_offset():
    assert gamblers_ruin_null(0.0, 0.5) == 0.5
    assert gamblers_ruin_null(0.026, 0.5) == pytest.approx(0.526)
    assert gamblers_ruin_null(2.0, 0.5) == 1.0
    assert gamblers_ruin_null(-2.0, 0.5) == 0.0
    assert gamblers_ruin_null(None, 0.5) is None


# ── The customer's rungs ─────────────────────────────────────────────


def test_named_rungs_match_exactly_and_by_side():
    down = {"extension_k": -1.5, "side": "down"}
    up = {"extension_k": 1.5, "side": "up"}
    near = {"extension_k": -1.25, "side": "down"}
    assert on_rung(down, 1.5) and on_rung(up, 1.5)
    assert on_rung(down, 1.5, "down") and not on_rung(up, 1.5, "down")
    assert not on_rung(near, 1.5)


def test_named_rung_cohorts_sit_inside_the_multiplicity_family():
    keys = [c.key for c in build_cohorts(confluence_distance=10.0, min_extension=2.0, min_broken=2)]
    for key in (
        "rung_named",
        "rung_100",
        "rung_150",
        "rung_200",
        "rung_m100",
        "rung_m150",
        "rung_m200",
    ):
        assert key in keys


def test_a_rung_the_step_cannot_reach_is_reported_as_such():
    rows = _summary_rows()
    summary = build_summary(rows, ResearchConfig(extension_step=1.0))
    by = {r["rung"]: r for r in summary["named_rungs"]}
    assert by["±150%"]["on_ladder"] is False
    assert by["±100%"]["on_ladder"] is True
    assert "not a rung at this step" in render_markdown(summary)


# ── Thin cohorts are not tested ──────────────────────────────────────


def _rows(n_sessions: int, per_session: int, cohort_sessions: int) -> list[dict]:
    rows = []
    rng = random.Random(1)
    for s in range(n_sessions):
        for j in range(per_session):
            rows.append(
                {
                    "session": f"2026-07-{s + 1:02d}",
                    "outcome": OUTCOME_REVERSAL if rng.random() < 0.5 else OUTCOME_CONTINUATION,
                    "flag": s < cohort_sessions,
                }
            )
    return rows


def test_a_cohort_living_in_one_session_gets_no_p_value():
    """On synthetic data a two-touch, one-day cohort scored p=0.001 and
    survived Benjamini-Hochberg. The resample collapses; the test is fiction."""
    rows = _rows(30, 40, cohort_sessions=1)
    out = compare_to_baseline(rows, lambda r: r["flag"])
    assert out["testable"] is False
    assert out["clustered_p"] is None and out["clustered_ci_low"] is None
    assert out["clustered_diff"] is not None  # the point estimate still prints


def test_a_cohort_over_enough_sessions_is_tested():
    rows = _rows(30, 10, cohort_sessions=MIN_TESTABLE_SESSIONS)
    out = compare_to_baseline(rows, lambda r: r["flag"])
    assert out["testable"] is True
    assert out["clustered_p"] is not None


# ── Power ────────────────────────────────────────────────────────────


def test_power_reproduces_the_september_h2_interval():
    """-6.0 points at p=0.256 implies se ~5.3 points; 49 sessions."""
    se = 0.06 / 1.1357
    assert power_two_sided(-0.06, se) == pytest.approx(0.206, abs=0.01)
    assert min_detectable_effect(se) == pytest.approx(0.148, abs=0.002)
    assert sessions_needed(49, se, -0.06) == pytest.approx(299, abs=2)


def test_power_reaches_its_target_at_the_sessions_it_asks_for():
    se = 0.05
    need = sessions_needed(40, se, 0.04)
    se_then = se * (40 / need) ** 0.5
    assert power_two_sided(0.04, se_then) == pytest.approx(0.80, abs=0.01)


def test_se_from_a_95_interval():
    assert se_from_ci(-0.0196, 0.0196) == pytest.approx(0.01, rel=1e-3)
    assert se_from_ci(None, 0.1) is None
    assert se_from_ci(0.1, 0.1) is None


# ── Cost hurdle ──────────────────────────────────────────────────────


def test_cost_hurdle_reproduces_the_september_figure():
    """60-point NQ range, 0.5R step, $4.50 + one tick a side: 0.725 points,
    a 1.21-point hurdle."""
    rows = [{"symbol": "NQ", "session": f"2026-07-{d:02d}", "or_width": 60.0} for d in (1, 2, 3)]
    h = _cost_hurdle(rows, ResearchConfig())
    assert h["cost_pts"] == pytest.approx(0.725)
    assert h["cost_dollars"] == pytest.approx(14.50)
    assert h["hurdle"] == pytest.approx(0.725 / 60.0)


def test_a_wider_range_lowers_the_hurdle():
    narrow = [{"symbol": "NQ", "session": "2026-07-01", "or_width": 60.0}]
    wide = [{"symbol": "NQ", "session": "2026-07-01", "or_width": 150.0}]
    cfg = ResearchConfig()
    assert _cost_hurdle(wide, cfg)["hurdle"] < _cost_hurdle(narrow, cfg)["hurdle"]


def test_no_nq_rows_means_no_hurdle():
    assert (
        _cost_hurdle([{"symbol": "SPY", "or_width": 2.0}], ResearchConfig())["available"] is False
    )


# ── Coverage ─────────────────────────────────────────────────────────


def test_usable_sessions_is_the_intersection_by_date():
    cfg = ResearchConfig(opening_range_minutes=60)
    d1, d2, d3 = date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10)
    gamma = {d1: (390, 60), d2: (390, 60), d3: (10, 5)}  # d3: too few frames
    bars = {d1: (390, 60), d2: (390, 20), d3: (390, 60)}  # d2: OR window not covered
    u = sources.usable_sessions(gamma, bars, cfg, fresh_from=d2)
    assert u["usable"] == 1 and u["first"] == d1.isoformat()
    assert u["gamma_sessions"] == 2 and u["bar_sessions"] == 2
    assert u["usable_fresh"] == 0


def test_usable_sessions_says_unknown_when_a_read_failed():
    assert sources.usable_sessions(None, {}, ResearchConfig())["available"] is False


def test_no_query_double_shifts_a_timestamptz_into_the_wrong_date():
    """``timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'`` is the
    idiom for a NAIVE column. These columns are TIMESTAMPTZ, where it shifts
    the instant twice and an ETF's post-close frames land on the next date:
    every Friday became a Saturday "session" in coverage."""
    for sql in (sources._SESSIONS_SQL, sources._SPAN_SQL, sources._RTH_SQL):
        assert "AT TIME ZONE 'UTC'" not in sql
        assert "AT TIME ZONE 'America/New_York'" in sql


# ── One pass, several range lengths ──────────────────────────────────


class _FakeSource:
    """Invented sessions behind the seven readers ``build_session`` calls."""

    def __init__(self, n: int = 6) -> None:
        rng = random.Random(3)
        self.data = {}
        self.reads = 0
        for i in range(n):
            d = date(2026, 7, 1) + timedelta(days=i)
            bars, frames, _ = synthetic_session(d, rng, ResearchConfig())
            self.data[d] = (bars, frames)

    def fetch_sessions(self, conn, gamma_symbol, start, end):
        return sorted(self.data)

    def fetch_bars(self, conn, inst, session, cfg):
        self.reads += 1
        return self.data[session][0]

    def probe_bars(self, conn, inst, session):
        return {"probe": "no_rows_on_date"}

    def fetch_summary_frames(self, conn, gamma_symbol, session, cfg):
        self.reads += 1
        return self.data[session][1]

    def fetch_index_bars(self, conn, gamma_symbol, session, cfg):
        return self.data[session][0]

    def fetch_strike_frames(self, conn, gamma_symbol, session, cfg):
        return {}

    def fetch_volumes(self, conn, inst, session, cfg):
        return {}, None

    def fetch_trade_bias(self, conn, gamma_symbol, session, cfg):
        return []


def test_a_shared_build_produces_exactly_what_solo_builds_produce(tmp_path: Path):
    """The multi-length build reads each session once; that must change
    nothing about any row, real or surrogate."""
    fake = _FakeSource()
    cfgs = [ResearchConfig(null_replicas=2).variant(opening_range_minutes=m) for m in (5, 60)]
    writers = [DatasetWriter(tmp_path / f"or{c.opening_range_minutes}.jsonl", c) for c in cfgs]
    build_datasets(
        None, ["NDX"], date(2026, 7, 1), date(2026, 7, 9), cfgs, writers, fetch_base=fake
    )
    for w in writers:
        w.close(symbols=["NDX"], start=date(2026, 7, 1), end=date(2026, 7, 9))
    assert fake.reads == 2 * len(fake.data)  # bars + frames, once per session

    inst = spec("NDX")
    for c, w in zip(cfgs, writers):
        solo_rows, solo_null = [], []
        for d in sorted(fake.data):
            res = build_session(None, inst, d, c, fetch=_FakeSource())
            solo_rows += res.rows
            solo_null += res.null_rows
        shared = read_jsonl(w.path)
        assert len(shared) == len(solo_rows) > 0
        for a, b in zip(shared, solo_rows):
            assert a["touched_at"] == b["touched_at"] and a["outcome"] == b["outcome"]
            assert a["config_fingerprint"] == c.fingerprint()
        assert read_null(w.path) == solo_null
        meta = read_meta(w.path)
        assert meta["null_rows"] == len(solo_null)


def test_configs_that_disagree_on_the_session_window_are_refused(tmp_path: Path):
    a = ResearchConfig()
    b = ResearchConfig(session_end=time(15, 0))
    with pytest.raises(ValueError, match="session window"):
        build_datasets(
            None,
            ["NDX"],
            date(2026, 7, 1),
            date(2026, 7, 2),
            [a, b],
            [DatasetWriter(tmp_path / "a.jsonl", a), DatasetWriter(tmp_path / "b.jsonl", b)],
            fetch_base=_FakeSource(1),
        )


def test_a_half_written_dataset_has_no_meta(tmp_path: Path):
    """The meta file is the 'build finished' marker compare-or reuses on."""
    cfg = ResearchConfig()
    path = tmp_path / "x.jsonl"
    (tmp_path / "x.jsonl.meta.json").write_text("{}")
    DatasetWriter(path, cfg)
    assert read_meta(path) is None


# ── Summary and report ───────────────────────────────────────────────


def _summary_rows() -> list[dict]:
    """Rows from the real build path over invented sessions."""
    fake = _FakeSource(n=12)
    rows: list[dict] = []
    for d in sorted(fake.data):
        rows += build_session(None, spec("NDX"), d, ResearchConfig(), fetch=fake).rows
    return rows


def test_summary_carries_the_followup_sections_and_renders_them():
    fake = _FakeSource(n=12)
    cfg = ResearchConfig(opening_range_minutes=60, null_replicas=3)
    rows, nulls = [], []
    for d in sorted(fake.data):
        res = build_session(None, spec("NDX"), d, cfg, fetch=fake)
        rows += res.rows
        nulls += res.null_rows
    s = build_summary(rows, cfg, null_rows=nulls, fresh_from="2026-07-06")
    for key in ("named_rungs", "h2", "hypotheses", "cost_hurdle", "depth_rho"):
        assert key in s
    assert s["mechanical_null"]["headline"]["surrogate_null"] is not None
    assert s["h2"]["fresh"] is not None
    assert set(s["hypotheses"]) == {"H1", "H2", "H3"}
    md = render_markdown(s)
    assert "The same null, measured" in md
    assert "-100%, -150%, -200%" in md
    assert "H2 — broken ladders: effect and power" in md


def test_without_surrogates_the_report_says_the_null_is_the_formula():
    rows = _summary_rows()
    md = render_markdown(build_summary(rows, ResearchConfig()))
    assert "every null" in md and "formula alone" in md


# ── The pre-registered verdict ───────────────────────────────────────


def _hyp(
    *,
    sign: int = 1,
    net: float = 0.05,
    lo: float = 0.03,
    hi: float = 0.07,
    oos: tuple = (0.04, 0.03, 0.05),
    n_split: int = 40,
    testable: bool = True,
    pooling: str = "consistent",
) -> dict:
    return {
        "sign": sign,
        "n": 300,
        "vs_baseline": {"net_of_null_diff": net, "testable": testable, "cohort_sessions": 40},
        "own_null": {"excess": (lo + hi) / 2, "excess_ci_low": lo, "excess_ci_high": hi},
        "oos": {
            name: {"n": n_split, "excess_vs_column": v}
            for name, v in zip(("discovery", "validation", "test"), oos)
        },
        "pooling": pooling,
    }


def test_verdict_build_needs_every_criterion():
    assert decide(_hyp(), hurdle=0.012, survives_bh=True)[0] == BUILD
    assert decide(_hyp(), hurdle=0.012, survives_bh=False)[0] == DEFER
    assert decide(_hyp(lo=0.005), hurdle=0.012, survives_bh=True)[0] == DEFER
    assert decide(_hyp(n_split=10), hurdle=0.012, survives_bh=True)[0] == DEFER
    assert decide(_hyp(pooling="books_disagree"), hurdle=0.012, survives_bh=True)[0] == DEFER


def test_verdict_dont_build_on_any_disqualifier():
    assert decide(_hyp(net=-0.01), hurdle=0.012, survives_bh=True)[0] == DONT_BUILD
    assert decide(_hyp(lo=-0.03, hi=0.01), hurdle=0.012, survives_bh=False)[0] == DONT_BUILD
    assert decide(_hyp(oos=(0.04, -0.02, 0.05)), hurdle=0.012, survives_bh=True)[0] == DONT_BUILD


def test_a_thin_split_cannot_reverse_a_verdict():
    """A sign flip on twelve touches is noise, not a reversal."""
    verdict, _ = decide(_hyp(oos=(0.04, -0.2, 0.05), n_split=12), hurdle=0.012, survives_bh=False)
    assert verdict == DEFER


def test_verdict_respects_a_continuation_claim():
    """H2 claims LESS reversion, so its numbers are read with the sign flipped."""
    h2 = _hyp(sign=-1, net=-0.05, lo=-0.07, hi=-0.03, oos=(-0.04, -0.03, -0.05))
    assert decide(h2, hurdle=0.012, survives_bh=True)[0] == BUILD
    assert (
        decide(
            {**h2, "vs_baseline": {**h2["vs_baseline"], "net_of_null_diff": 0.02}},
            hurdle=0.012,
            survives_bh=True,
        )[0]
        == DONT_BUILD
    )


def test_an_untested_cohort_is_untestable_not_a_verdict():
    verdict, reasons = decide(_hyp(testable=False), hurdle=0.012, survives_bh=True)
    assert verdict == UNTESTABLE
    assert "a test needs" in reasons[0]


def test_combined_bh_spans_every_range_length():
    summaries = {
        5: {"cohorts": [{"key": "a", "clustered_p": 0.001}, {"key": "b", "clustered_p": 0.5}]},
        60: {"cohorts": [{"key": "a", "clustered_p": 0.02}, {"key": "c", "clustered_p": None}]},
    }
    bh = combined_bh(summaries)
    assert bh["family_size"] == 3
    assert "5m:a" in bh["survivors"]
    assert "60m:a" in bh["survivors"]  # 0.02 <= 0.05 * 2 / 3
    assert "5m:b" not in bh["survivors"]


def test_censored_touches_never_enter_a_null_rate():
    from research.or_gamma_confluence.report import _surrogate_rate

    rows = [
        {"outcome": OUTCOME_REVERSAL},
        {"outcome": OUTCOME_CONTINUATION},
        {"outcome": OUTCOME_CENSORED},
    ]
    out = _surrogate_rate(rows)
    assert out["rate"] == 0.5 and out["n"] == 2
    assert out["censored_share"] == pytest.approx(1 / 3)
