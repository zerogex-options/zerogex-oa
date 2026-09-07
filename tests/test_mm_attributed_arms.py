"""Three arms in one dataset, one battery, one verdict.

What these pin, beyond the per-module tests: that the builder runs with a tape
and no exchange file (A vs B), with both (A vs B vs C), that a session failing
its aggressor gate contributes diagnostics and nothing else, that the attributed
flow-since-open differences against the cash open rather than the window
start, that the battery reports every arm on identical rows, and that the
three-arm verdict fires its gates in the documented order and never forces a
winner.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import numpy as np
import pytest

from research.mm_attributed_gex.aggressor import AggressorBucket, AggressorGateConfig
from research.mm_attributed_gex.backtest import (
    ARMS,
    ExperimentConfig,
    ExperimentResult,
    arms_present,
    run_experiment,
)
from research.mm_attributed_gex.dataset import DatasetSpec, build_dataset
from research.mm_attributed_gex.gex import ChainQuote, build_engine
from research.mm_attributed_gex.outcomes import Bar, BarSeries
from research.mm_attributed_gex.report import (
    ArmThresholds,
    decide_arms,
    render_arms_markdown,
    render_markdown,
    write_report,
)
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
)

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")

SYMBOL = "SPX"
SESSION = date(2026, 6, 15)
PREV_SESSION = date(2026, 6, 12)
NEXT_SESSION = date(2026, 6, 16)
EXPIRATION = date(2026, 6, 19)
RELAXED = AggressorGateConfig(min_classified_share=0.0, min_buckets=1, min_series=1)


def _et(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm)).replace(tzinfo=ET).astimezone(timezone.utc)


def _bucket(
    hh, mm, *, buyer=0, seller=0, unknown=0, strike=6000.0, option_type="C", session=SESSION
):
    return AggressorBucket(
        symbol=SYMBOL,
        option_symbol=f"SPXW {EXPIRATION:%y%m%d}{option_type}{int(strike):08d}",
        expiration=EXPIRATION,
        strike=strike,
        option_type=option_type,
        timestamp=_et(session, hh, mm),
        trading_date=session,
        buyer_initiated=buyer,
        seller_initiated=seller,
        unclassified=unknown,
        gamma=0.001,
    )


def _rec(hh, contracts, *, side=Side.BUY, strike=6000.0, option_type="C", session=SESSION):
    return ParticipantActivity(
        exchange=Exchange.CBOE_C1,
        symbol=SYMBOL,
        expiration=EXPIRATION,
        strike=strike,
        option_type=option_type,
        participant=ParticipantType.MARKET_MAKER,
        side=side,
        position_effect=PositionEffect.OPEN,
        contracts=contracts,
        timestamp=_et(session, hh),
        trading_date=session,
        interval=Interval.MINUTE_30,
        option_root="SPXW",
    )


def _quote(strike, option_type, oi=1_000, iv=0.15):
    return ChainQuote(
        option_symbol=f"SPXW {EXPIRATION:%y%m%d}{option_type}{int(strike):08d}",
        strike=strike,
        expiration=EXPIRATION,
        option_type=option_type,
        implied_volatility=iv,
        gamma=None,
        open_interest=oi,
        volume=100,
    )


CHAIN = [
    _quote(5900.0, "P", 1_500),
    _quote(5950.0, "P", 900),
    _quote(6000.0, "C", 1_200),
    _quote(6050.0, "C", 700),
    _quote(6100.0, "C", 400),
]


def _build(*, buckets=None, records=None, stamps=None, gates=RELAXED, chain=CHAIN):
    stamps = stamps or [_et(SESSION, 11), _et(SESSION, 13)]
    return build_dataset(
        (lambda: iter(records)) if records is not None else None,
        stamps,
        lambda ts: chain,
        lambda ts: 6000.0,
        spec=DatasetSpec(
            symbol=SYMBOL, headline_universe="all", clean_only=False, aggressor_gates=gates
        ),
        engine=build_engine(SYMBOL),
        aggressor_factory=(lambda: iter(buckets)) if buckets is not None else None,
    )


# ---------------------------------------------------------------------------
# Dataset builder
# ---------------------------------------------------------------------------


def test_builder_runs_with_a_tape_and_no_exchange_file():
    rows, provenance = _build(
        buckets=[
            _bucket(10, 0, buyer=300),
            _bucket(10, 5, seller=100, strike=5900.0, option_type="P"),
        ]
    )
    assert provenance["arms"] == {
        "production": True,
        "aggressor_inferred": True,
        "mm_attributed": False,
    }
    assert provenance["reconstruction"] is None
    row = rows[0]
    assert row.existing_dealer_gamma_at_spot is not None
    assert row.mm_attributed_gamma_at_spot is None
    assert row.aggressor_session_gate_passed is True
    assert row.aggressor_mm_flow_gamma_at_spot is not None
    assert row.aggressor_mm_flow_net_contracts == pytest.approx(-200.0)  # −300 + 100
    assert row.production_anchored_aggressor_gamma_at_spot is not None
    assert row.production_anchored_aggressor_gamma_flip is not None
    assert row.aggressor_series_matched_to_chain == 2
    assert (
        "aggressor_flow" in row.universes["all"] and "production_anchored" in row.universes["all"]
    )
    assert row.aggressor_classified_share == 1.0


def test_anchored_arm_equals_production_when_the_tape_is_unclassified():
    """Only unclassified volume → no assumed MM change → B2 must equal A exactly."""
    rows, _ = _build(buckets=[_bucket(10, 0, unknown=500)])
    row = rows[0]
    assert row.existing_source == "recomputed"
    assert row.aggressor_mm_flow_gamma_at_spot is None  # nothing signed to price
    assert row.production_anchored_aggressor_gamma_at_spot == pytest.approx(
        row.existing_dealer_gamma_at_spot, rel=1e-12
    )
    assert row.production_anchored_aggressor_gamma_flip == pytest.approx(
        row.existing_gamma_flip, rel=1e-12
    )
    assert row.production_anchored_aggressor_call_wall == row.existing_call_wall
    assert row.production_anchored_aggressor_put_wall == row.existing_put_wall


def test_classified_flow_moves_the_anchored_arm_away_from_production():
    rows, _ = _build(buckets=[_bucket(10, 0, buyer=900)])  # assumed MM sold 900 calls
    row = rows[0]
    assert row.production_anchored_aggressor_gamma_at_spot < row.existing_dealer_gamma_at_spot


def test_a_session_failing_its_gate_gets_diagnostics_and_no_arm_values():
    rows, provenance = _build(
        buckets=[_bucket(10, 0, buyer=10, unknown=990)],
        gates=AggressorGateConfig(min_classified_share=0.5, min_buckets=1, min_series=1),
    )
    row = rows[0]
    assert row.aggressor_available is True
    assert row.aggressor_session_gate_passed is False
    assert any("classified share" in r for r in row.aggressor_session_gate_reasons)
    assert row.aggressor_classified_share == pytest.approx(0.01)
    assert row.production_anchored_aggressor_gamma_at_spot is None
    assert row.aggressor_mm_flow_gamma_at_spot is None
    assert provenance["aggressor"]["sessions_passed"] == 0
    assert SESSION.isoformat() in provenance["aggressor"]["sessions_failed"]


def test_aggressor_flow_resets_at_the_next_cash_open():
    """Day one's −300 must not leak into day two; day two's 09:40 bucket is known at 09:45."""
    buckets = [_bucket(10, 0, buyer=300), _bucket(9, 40, seller=40, session=NEXT_SESSION)]
    rows, _ = _build(
        buckets=buckets,
        stamps=[
            _et(SESSION, 11),
            _et(NEXT_SESSION, 9, 35),
            _et(NEXT_SESSION, 9, 45),
            _et(NEXT_SESSION, 11),
        ],
    )
    by_ts = {r.timestamp: r for r in rows}
    assert by_ts[_et(SESSION, 11).isoformat()].aggressor_mm_flow_net_contracts == -300.0
    early = by_ts[_et(NEXT_SESSION, 9, 35).isoformat()]
    assert early.aggressor_mm_flow_net_contracts == 0.0  # nothing observed yet this session
    assert early.aggressor_mm_flow_gamma_at_spot is None
    assert by_ts[_et(NEXT_SESSION, 9, 45).isoformat()].aggressor_mm_flow_net_contracts == 40.0
    assert by_ts[_et(NEXT_SESSION, 11).isoformat()].aggressor_mm_flow_net_contracts == 40.0


def test_all_three_arms_in_one_row():
    rows, provenance = _build(
        buckets=[_bucket(10, 0, buyer=300)],
        records=[_rec(10, 800), _rec(10, 600, strike=5900.0, option_type="P", side=Side.SELL)],
    )
    assert provenance["arms"] == {
        "production": True,
        "aggressor_inferred": True,
        "mm_attributed": True,
    }
    row = rows[0]
    assert row.existing_dealer_gamma_at_spot is not None
    assert row.production_anchored_aggressor_gamma_at_spot is not None
    assert row.mm_attributed_gamma_at_spot is not None
    assert row.mm_net_contracts_total == pytest.approx(200.0)
    assert arms_present([row.as_dict()]) == ["production", "aggressor_anchored", "mm_attributed"]


def test_attributed_flow_since_open_differences_against_the_cash_open_not_the_window():
    """+500 the previous afternoon, +800 at 10:00 today → flow since today's open is 800."""
    records = [_rec(15, 500, session=PREV_SESSION), _rec(10, 800)]
    rows, _ = _build(records=records, stamps=[_et(SESSION, 11)])
    row = rows[0]
    assert row.mm_net_contracts_total == pytest.approx(1_300.0)
    assert row.mm_attributed_flow_net_contracts == pytest.approx(800.0)
    assert row.mm_attributed_flow_gamma_at_spot is not None
    assert row.universes["all"]["mm_attributed_flow"]["net_contracts"] == pytest.approx(800.0)


def test_attributed_flow_records_a_full_reversal_to_zero():
    """Held 500 at the open, sold 500 by 10:00 → the series is flat but the change is −500."""
    records = [_rec(15, 500, session=PREV_SESSION), _rec(10, 500, side=Side.SELL)]
    rows, _ = _build(records=records, stamps=[_et(SESSION, 11)])
    assert rows[0].mm_attributed_flow_net_contracts == pytest.approx(-500.0)


# ---------------------------------------------------------------------------
# Battery
# ---------------------------------------------------------------------------


def _arm_rows(n_sessions: int = 3, per_session: int = 150, *, with_c: bool = True):
    rng = np.random.default_rng(11)
    bars: list[Bar] = []
    rows: list[dict] = []
    price = 6000.0
    session = SESSION
    for _ in range(n_sessions):
        while session.weekday() >= 5:
            session += timedelta(days=1)
        base = _et(session, 9, 30)
        start = len(bars)
        for i in range(per_session + 90):
            price = max(1.0, price + float(rng.normal(0, 1.2)))
            bars.append(Bar(base + timedelta(minutes=i), price, price + 0.5, price - 0.5, price))
        for i in range(0, per_session, 1):
            ts = base + timedelta(minutes=i)
            spot = bars[start + i].close
            sign = 1.0 if (i // 40) % 2 == 0 else -1.0
            row = {
                "timestamp": ts.isoformat(),
                "trading_date": session.isoformat(),
                "session_minute": i,
                "spot": spot,
                "existing_dealer_gamma_at_spot": sign * 1e9,
                "existing_gamma_flip": spot - sign * 20.0,
                "existing_net_gex": sign * 2e9,
                "existing_call_wall": spot + 25.0,
                "existing_put_wall": spot - 25.0,
                "production_anchored_aggressor_gamma_at_spot": sign * 9e8
                + float(rng.normal(0, 1e8)),
                "production_anchored_aggressor_gamma_flip": spot - sign * 18.0,
                "production_anchored_aggressor_net_gex": sign * 1.8e9,
                "production_anchored_aggressor_call_wall": spot + 25.0,
                "production_anchored_aggressor_put_wall": spot - 30.0,
                "production_anchored_aggressor_b_call_wall": spot + 30.0,
                "production_anchored_aggressor_b_put_wall": spot - 30.0,
                "production_anchored_aggressor_negative_gamma_share": 0.35,
                "production_anchored_aggressor_concentration_hhi": 0.1,
                "aggressor_mm_flow_gamma_at_spot": float(rng.normal(0, 3e8)),
                "aggressor_classified_share": 0.8,
                "aggressor_extrapolated": False,
                "inventory_confidence": 0.8,
                "percent_of_gamma_universe_reconstructed": 0.6,
                "contribution_0dte": 0.3,
                "dte_front": 4,
                "is_opex": False,
                "is_month_end": False,
                "vix_close": 15.0,
            }
            if with_c:
                row.update(
                    {
                        "mm_attributed_gamma_at_spot": sign * 8e8,
                        "mm_attributed_gamma_flip": spot - sign * 15.0,
                        "mm_attributed_net_gex": sign * 1.5e9,
                        "mm_attributed_call_wall": spot + 25.0,
                        "mm_attributed_put_wall": spot - 25.0,
                        "mm_attributed_b_call_wall": spot + 30.0,
                        "mm_attributed_b_put_wall": spot - 30.0,
                        "mm_negative_gamma_share": 0.4,
                        "mm_concentration_hhi": 0.12,
                        "mm_flip_unresolved": False,
                        "mm_attributed_flow_gamma_at_spot": float(rng.normal(0, 2e8)),
                    }
                )
            rows.append(row)
        session += timedelta(days=1)
    return rows, BarSeries(bars)


def test_battery_reports_every_arm_on_identical_rows():
    rows, series = _arm_rows(3, 150)
    result = run_experiment(
        rows, series, config=ExperimentConfig(horizons=(15, 30), n_boot=50), sampling_minutes=1
    )
    assert result.arms["present"] == ["production", "aggressor_anchored", "mm_attributed"]
    assert set(result.arms["regime"]["summary"]) == {"aggressor_anchored", "mm_attributed"}
    assert result.arms["regime"]["n_aligned"] == result.n_scored
    assert set(result.arms["incremental"]["arms"]) == {"aggressor_anchored", "mm_attributed"}
    assert set(result.arms["flip"]["agreement"]) == {"aggressor_anchored", "mm_attributed"}
    assert "call" in result.arms["walls"]["vs_production"]["aggressor_anchored"]
    assert set(result.hedge_pressure) == {"aggressor_flow", "mm_attributed_flow"}
    assert "15m" in result.hedge_pressure["aggressor_flow"]["level"]
    assert "15m" in result.hedge_pressure["aggressor_flow"]["change"]
    assert result.validation["available"] is False
    assert result.coverage["aggressor_row_share"] == 1.0
    assert result.coverage["aggressor_mean_classified_share"] == pytest.approx(0.8)
    assert result.multiplicity["n_tests"] > 0


def test_battery_without_the_attributed_arm_still_compares_a_and_b():
    rows, series = _arm_rows(2, 150, with_c=False)
    result = run_experiment(
        rows, series, config=ExperimentConfig(horizons=(15,), n_boot=30), sampling_minutes=1
    )
    assert result.arms["present"] == ["production", "aggressor_anchored"]
    assert "mm_attributed" not in result.arms["regime"]["summary"]
    assert result.hedge_pressure["mm_attributed_flow"]["note"] == "insufficient_sample"


def test_validation_segment_is_carved_by_sessions_when_there_are_enough():
    rows, series = _arm_rows(6, 60)
    result = run_experiment(
        rows,
        series,
        config=ExperimentConfig(horizons=(15,), n_boot=20, min_sessions_for_validation=5),
        sampling_minutes=1,
    )
    val = result.validation
    assert val["available"] is True
    assert val["development"]["n_rows"] + val["validation"]["n_rows"] == result.n_scored
    dev_end = date.fromisoformat(val["development_sessions"][1])
    val_start = date.fromisoformat(val["validation_sessions"][0])
    assert dev_end < val_start


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------


def _inc(delta_oos: float, horizons=("15m", "30m")):
    return {
        "ok": True,
        "horizons": {
            h: {
                "realized_vol_regression": {
                    "ok": True,
                    "delta_adj_r2": abs(delta_oos),
                    "f_p_value": 0.001 if delta_oos > 0 else 0.6,
                },
                "walk_forward_realized_vol": {
                    "mean_delta_oos_r2": delta_oos,
                    "folds_improved": 4 if delta_oos > 0 else 1,
                    "n_folds": 5,
                },
            }
            for h in horizons
        },
    }


def _regime(arm_better: int, production_better: int, tie: int = 0):
    return {
        "comparisons": arm_better + production_better + tie,
        "arm_better": arm_better,
        "production_better": production_better,
        "tie": tie,
        "undecided": 0,
        "arm_better_share": arm_better / max(1, arm_better + production_better + tie),
    }


def _result(
    *,
    n_scored=5_000,
    arms=("aggressor_anchored", "mm_attributed"),
    inc=None,
    regime=None,
    coverage=None,
    validation=None,
):
    inc = inc or {}
    regime = regime or {}
    cov = {
        "aggressor_row_share": 1.0,
        "aggressor_mean_classified_share": 0.8,
        "aggressor_extrapolated_share": 0.0,
        "mean_gamma_coverage": 0.6,
        "mean_inventory_confidence": 0.8,
    }
    cov.update(coverage or {})
    return ExperimentResult(
        n_rows=n_scored,
        n_scored=n_scored,
        coverage=cov,
        arms={
            "present": ["production", *arms],
            "labels": {a: ARMS[a].label for a in ("production", *arms)},
            "regime": {"summary": regime, "vs_production": {}, "arms": {}, "n_aligned": n_scored},
            "flip": {"vs_production": {}, "agreement": {}, "arms": {}},
            "walls": {"vs_production": {}, "arms": {}},
            "incremental": {"ok": True, "arms": inc},
        },
        validation=validation or {"available": False},
    )


def test_thin_sample_is_inconclusive_before_anything_else():
    assert decide_arms(_result(n_scored=50)).code == "INCONCLUSIVE"


def test_no_alternative_arm_is_inconclusive_data():
    assert decide_arms(_result(arms=())).code == "INCONCLUSIVE_DATA"


def test_failed_data_gates_on_every_arm_is_inconclusive_data():
    verdict = decide_arms(
        _result(
            arms=("aggressor_anchored",),
            inc={"aggressor_anchored": _inc(0.05)},
            regime={"aggressor_anchored": _regime(10, 0)},
            coverage={"aggressor_row_share": 0.1},
        )
    )
    assert verdict.code == "INCONCLUSIVE_DATA"
    assert "not evaluable" in " ".join(verdict.rationale)


def test_attributed_arm_clearing_the_floors_wins():
    verdict = decide_arms(
        _result(
            inc={"aggressor_anchored": _inc(0.0), "mm_attributed": _inc(0.03)},
            regime={"aggressor_anchored": _regime(5, 5), "mm_attributed": _regime(9, 1)},
        )
    )
    assert verdict.code == "ATTRIBUTED_BETTER"
    assert "reconstruction of the market-maker population" in " ".join(verdict.rationale)


def test_aggressor_arm_clearing_the_floors_wins():
    verdict = decide_arms(
        _result(
            inc={"aggressor_anchored": _inc(0.02), "mm_attributed": _inc(0.0)},
            regime={"aggressor_anchored": _regime(8, 2), "mm_attributed": _regime(4, 6)},
        )
    )
    assert verdict.code == "AGGRESSOR_BETTER"
    assert "still an assumption about participant identity" in " ".join(verdict.rationale)


def test_both_arms_materially_worse_means_production_better():
    verdict = decide_arms(
        _result(
            inc={"aggressor_anchored": _inc(-0.02), "mm_attributed": _inc(-0.03)},
            regime={"aggressor_anchored": _regime(1, 9), "mm_attributed": _regime(2, 8)},
        )
    )
    assert verdict.code == "PRODUCTION_BETTER"


def test_nothing_material_is_practically_equivalent():
    verdict = decide_arms(
        _result(
            inc={"aggressor_anchored": _inc(0.001), "mm_attributed": _inc(-0.001)},
            regime={"aggressor_anchored": _regime(5, 5), "mm_attributed": _regime(6, 4)},
        )
    )
    assert verdict.code == "PRACTICALLY_EQUIVALENT"


def test_a_full_sample_winner_that_reverses_on_validation_is_inconclusive():
    validation = {
        "available": True,
        "development": {
            "n_rows": 3000,
            "regime_summary": {"mm_attributed": _regime(9, 1)},
            "incremental_summary": {
                "mm_attributed": {"ok": True, "horizons": {"15m": {"mean_delta_oos_r2": 0.03}}}
            },
        },
        "validation": {
            "n_rows": 2000,
            "regime_summary": {"mm_attributed": _regime(2, 8)},
            "incremental_summary": {
                "mm_attributed": {"ok": True, "horizons": {"15m": {"mean_delta_oos_r2": -0.01}}}
            },
        },
    }
    verdict = decide_arms(
        _result(
            arms=("mm_attributed",),
            inc={"mm_attributed": _inc(0.03)},
            regime={"mm_attributed": _regime(9, 1)},
            validation=validation,
        )
    )
    assert verdict.code == "INCONCLUSIVE"
    assert "reverses direction" in " ".join(verdict.rationale)
    relaxed = decide_arms(
        _result(
            arms=("mm_attributed",),
            inc={"mm_attributed": _inc(0.03)},
            regime={"mm_attributed": _regime(9, 1)},
            validation=validation,
        ),
        ArmThresholds(require_validation_agreement=False),
    )
    assert relaxed.code == "ATTRIBUTED_BETTER"


def test_arms_report_keeps_the_categories_apart_and_never_says_true_dealer(tmp_path):
    rows, series = _arm_rows(3, 120)
    result = run_experiment(
        rows, series, config=ExperimentConfig(horizons=(15,), n_boot=20), sampling_minutes=1
    )
    section = render_arms_markdown(result)
    lowered = section.lower()
    for phrase in (
        "observed market data",
        "aggressor-classified trade direction",
        "aggressor-inferred mm positioning",
        "exchange-classified mm activity",
        "reconstructed mm inventory",
        "production modeled dealer positioning",
    ):
        assert phrase in lowered, phrase
    assert "true dealer" not in lowered
    assert "actual dealer book" not in lowered
    full = render_markdown(result)
    assert "Three-arm comparison" in full
    path = write_report(result, tmp_path / "r.md")
    import json

    payload = json.loads(path.with_suffix(".json").read_text())
    assert payload["arms_verdict"]["code"] in {
        "PRODUCTION_BETTER",
        "AGGRESSOR_BETTER",
        "ATTRIBUTED_BETTER",
        "PRACTICALLY_EQUIVALENT",
        "INCONCLUSIVE",
        "INCONCLUSIVE_DATA",
    }
