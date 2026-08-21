"""Dataset replay, verdict logic, and the end-to-end plumbing check.

Two properties matter most here and neither is visible from the unit tests:

* **Causality.** The replay must only ever see flow stamped at or before the
  snapshot. If a session-summary bucket could inform a mid-session snapshot of
  the same day, every forward test in the study would be contaminated.
* **Gating order.** The report must refuse to conclude on thin samples or
  incomplete reconstructions *before* it looks at any effect, so an
  under-reconstructed run reports ``INCONCLUSIVE_DATA`` rather than ``NO``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone

import pytest

from research.mm_attributed_gex.backtest import (
    ExperimentConfig,
    ExperimentResult,
    attach_outcomes,
    confluence_test,
    gamma_regime_test,
    run_experiment,
    wall_test,
)
from research.mm_attributed_gex.dataset import (
    DatasetSpec,
    InventoryTimeline,
    build_dataset,
    read_jsonl,
    write_csv,
    write_jsonl,
)
from research.mm_attributed_gex.gex import ChainQuote, build_engine
from research.mm_attributed_gex.outcomes import Bar, BarSeries
from research.mm_attributed_gex.report import decide, render_markdown, write_report
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
)
from research.mm_attributed_gex.selftest import run_pipeline_check

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")

SYMBOL = "SPX"
SESSION = date(2026, 6, 15)
EXPIRATION = date(2026, 6, 19)


def _et(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm), tzinfo=ET).astimezone(timezone.utc)


def _rec(
    hour: int,
    contracts: int,
    *,
    side=Side.BUY,
    strike=6000.0,
    option_type="C",
    trading_date=SESSION,
):
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
        timestamp=_et(trading_date, hour),
        trading_date=trading_date,
        interval=Interval.MINUTE_30,
        option_root="SPXW",
    )


def _quote(strike: float, option_type: str, iv: float = 0.15) -> ChainQuote:
    return ChainQuote(
        option_symbol=f"SPXW 260619{option_type}{int(strike):08d}",
        strike=strike,
        expiration=EXPIRATION,
        option_type=option_type,
        implied_volatility=iv,
        gamma=None,
        open_interest=2_000,
        volume=500,
    )


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def test_replay_freezes_the_running_inventory_at_each_requested_instant():
    """+100 at 10:00, +50 at 12:00 -> 100 at 11:00, 150 at 13:00."""
    records = [_rec(10, 100), _rec(12, 50)]
    timeline = InventoryTimeline(SYMBOL, censoring={})
    snapshots = dict(
        (ts, positions)
        for ts, positions in timeline.replay(records, [_et(SESSION, 11), _et(SESSION, 13)])
    )
    assert snapshots[_et(SESSION, 11)][0].net_contracts == 100
    assert snapshots[_et(SESSION, 13)][0].net_contracts == 150


def test_replay_never_uses_flow_stamped_after_the_snapshot():
    """The causality guarantee the whole forward study rests on."""
    records = [_rec(15, 999)]
    timeline = InventoryTimeline(SYMBOL, censoring={})
    ((_ts, positions),) = list(timeline.replay(records, [_et(SESSION, 10)]))
    assert positions == []


def test_a_bucket_stamped_exactly_at_the_snapshot_is_included():
    """A 10:00 bucket covers activity through 10:00, so it is known at 10:00."""
    timeline = InventoryTimeline(SYMBOL, censoring={})
    ((_ts, positions),) = list(timeline.replay([_rec(10, 100)], [_et(SESSION, 10)]))
    assert positions and positions[0].net_contracts == 100


def test_replay_excludes_series_that_have_already_settled():
    records = [_rec(10, 100)]
    timeline = InventoryTimeline(SYMBOL, censoring={})
    after_expiry = _et(EXPIRATION + timedelta(days=1), 10)
    snapshots = dict(timeline.replay(records, [_et(SESSION, 11), after_expiry]))
    assert len(snapshots[_et(SESSION, 11)]) == 1
    assert snapshots[after_expiry] == []


def test_replay_stamps_window_wide_censoring_verdicts_onto_every_snapshot():
    """A series later found incomplete is marked incomplete at all snapshots."""
    key = (SYMBOL, EXPIRATION, 6000.0, "C")
    timeline = InventoryTimeline(SYMBOL, censoring={key: (True, "listed_before_data_window")})
    ((_ts, positions),) = list(timeline.replay([_rec(10, 100)], [_et(SESSION, 11)]))
    assert positions[0].left_censored is True
    assert positions[0].left_censor_reason == "listed_before_data_window"


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


def _dataset_fixture(clean_only: bool = False):
    records = [
        _rec(10, 800, strike=6000.0, option_type="C"),
        _rec(10, 600, strike=5900.0, option_type="P", side=Side.SELL),
    ]
    chain = [_quote(6000.0, "C"), _quote(5900.0, "P"), _quote(6100.0, "C")]
    stamps = [_et(SESSION, 11), _et(SESSION, 12), _et(SESSION, 13)]
    return build_dataset(
        lambda: iter(records),
        stamps,
        lambda ts: chain,
        lambda ts: 6000.0,
        spec=DatasetSpec(symbol=SYMBOL, headline_universe="all", clean_only=clean_only),
        engine=build_engine(SYMBOL),
    )


def test_dataset_carries_both_methodologies_side_by_side():
    rows, provenance = _dataset_fixture()
    assert len(rows) == 3
    row = rows[0]
    # Existing methodology recomputed from the same chain.
    assert row.existing_dealer_gamma_at_spot is not None
    assert row.existing_source == "recomputed"
    # MM-attributed from the reconstructed inventory.
    assert row.mm_attributed_gamma_at_spot is not None
    assert row.mm_net_contracts_total == pytest.approx(200.0)  # +800 calls, −600 puts
    assert provenance["snapshots_built"] == 3


def test_dataset_reports_the_diagnostics_that_separate_data_from_methodology():
    rows, _ = _dataset_fixture()
    row = rows[0]
    for field in (
        "number_of_contracts",
        "number_of_cleanly_reconstructed_contracts",
        "percent_of_gamma_universe_reconstructed",
        "inventory_confidence",
        "inventory_confidence_band",
        "data_completeness",
        "contribution_0dte",
        "contribution_weekly",
    ):
        assert hasattr(row, field), field
    assert row.number_of_contracts == 2
    assert 0.0 <= row.percent_of_gamma_universe_reconstructed <= 1.0
    assert row.universe_gamma_abs > 0


def test_dataset_reports_every_expiration_universe():
    rows, _ = _dataset_fixture()
    assert set(rows[0].universes) == {"0dte", "nearest", "weekly", "near_term", "all"}
    assert "mm_attributed_gamma_at_spot" in rows[0].universes["all"]
    assert "mm_attributed_a_call_wall" in rows[0].universes["all"]


def test_clean_only_mode_excludes_censored_series_from_the_headline_numbers():
    """Day-1 series are censored by the heuristic, so clean-only sees nothing."""
    rows_all, _ = _dataset_fixture(clean_only=False)
    rows_clean, _ = _dataset_fixture(clean_only=True)
    assert rows_all[0].mm_attributed_gamma_at_spot is not None
    assert rows_clean[0].mm_attributed_gamma_at_spot is None
    # ...but the diagnostics still report what existed.
    assert rows_clean[0].number_of_contracts == 2
    assert rows_clean[0].number_of_cleanly_reconstructed_contracts == 0


def test_persisted_production_values_win_over_the_recomputation():
    """The comparand should be what the live system actually published."""
    records = [_rec(10, 500)]
    chain = [_quote(6000.0, "C")]
    stamps = [_et(SESSION, 11)]
    persisted = {
        "existing_dealer_gamma_at_spot": 1234.0,
        "existing_gamma_flip": 5990.0,
        "existing_call_wall": 6100.0,
    }
    rows, _ = build_dataset(
        lambda: iter(records),
        stamps,
        lambda ts: chain,
        lambda ts: 6000.0,
        spec=DatasetSpec(symbol=SYMBOL, headline_universe="all", clean_only=False),
        summary_provider=lambda ts: persisted,
        engine=build_engine(SYMBOL),
    )
    row = rows[0]
    assert row.existing_dealer_gamma_at_spot == 1234.0
    assert row.existing_source == "persisted"
    # The recomputation still runs, as an integrity check against production.
    assert row.existing_recompute_parity_at_spot is not None


def test_dataset_serializes_to_jsonl_and_csv(tmp_path):
    rows, _ = _dataset_fixture()
    jsonl = write_jsonl(rows, tmp_path / "ds.jsonl")
    csv_path = write_csv(rows, tmp_path / "ds.csv")
    loaded = read_jsonl(jsonl)
    assert len(loaded) == 3
    assert "universes" in loaded[0]
    # CSV holds the flat comparison columns only.
    header = csv_path.read_text().splitlines()[0]
    assert "mm_attributed_gamma_at_spot" in header
    assert "universes" not in header


# ---------------------------------------------------------------------------
# Experiment battery
# ---------------------------------------------------------------------------


def _scored_rows(n: int = 400, *, mm_signal: bool = False):
    """Synthetic dataset rows plus a price path, for battery smoke coverage."""
    import numpy as np

    rng = np.random.default_rng(3)
    bars: list[Bar] = []
    rows: list[dict] = []
    price = 6000.0
    base = _et(SESSION, 9, 30)
    for i in range(n + 200):
        price = max(1.0, price + float(rng.normal(0, 1.2)))
        bars.append(Bar(base + timedelta(minutes=i), price, price + 0.5, price - 0.5, price))
    for i in range(0, n, 1):
        ts = base + timedelta(minutes=i)
        sign = 1.0 if (i // 50) % 2 == 0 else -1.0
        rows.append(
            {
                "timestamp": ts.isoformat(),
                "trading_date": SESSION.isoformat(),
                "session_minute": i,
                "spot": bars[i].close,
                "existing_dealer_gamma_at_spot": sign * 1e9,
                "existing_gamma_flip": bars[i].close - sign * 20.0,
                "existing_net_gex": sign * 2e9,
                "existing_call_wall": bars[i].close + 25.0,
                "existing_put_wall": bars[i].close - 25.0,
                "mm_attributed_gamma_at_spot": (sign if not mm_signal else -sign) * 8e8,
                "mm_attributed_gamma_flip": bars[i].close - sign * 18.0,
                "mm_attributed_net_gex": sign * 1.5e9,
                "mm_attributed_call_wall": bars[i].close + 25.0,
                "mm_attributed_put_wall": bars[i].close - 25.0,
                "mm_attributed_b_call_wall": bars[i].close + 30.0,
                "mm_attributed_b_put_wall": bars[i].close - 30.0,
                "mm_negative_gamma_share": 0.4,
                "mm_concentration_hhi": 0.12,
                "mm_flip_unresolved": False,
                "inventory_confidence": 0.8,
                "percent_of_gamma_universe_reconstructed": 0.6,
                "contribution_0dte": 0.3,
                "dte_front": 4,
                "is_opex": False,
                "is_month_end": False,
                "vix_close": 15.0,
            }
        )
    return rows, BarSeries(bars)


def test_attach_outcomes_drops_rows_with_no_forward_window():
    rows, series = _scored_rows(50)
    rows.append({**rows[-1], "timestamp": (series.bars[-1].ts + timedelta(days=5)).isoformat()})
    scored = attach_outcomes(rows, series, config=ExperimentConfig(horizons=(15,)))
    assert len(scored) == 50
    assert all("rvol_15m_bps" in r for r in scored)


def test_battery_runs_end_to_end_and_reports_multiplicity():
    rows, series = _scored_rows(400)
    result = run_experiment(
        rows, series, config=ExperimentConfig(horizons=(15, 30), n_boot=100), sampling_minutes=1
    )
    assert result.n_scored > 0
    assert result.gamma_regime and result.gamma_flip and result.walls
    assert result.confluence["agreement_rate"] is not None
    assert result.multiplicity["n_tests"] > 0
    assert result.multiplicity["n_significant_bh"] <= result.multiplicity["n_tests"]


def test_regime_test_declines_to_pick_a_winner_on_a_thin_sample():
    rows, series = _scored_rows(60)
    scored = attach_outcomes(rows, series, config=ExperimentConfig(horizons=(15,)))
    out = gamma_regime_test(scored, ExperimentConfig(horizons=(15,)), sampling_minutes=1)
    assert out["existing"].get("note") == "insufficient_sample"
    assert all(v["better"] is None for v in out["separation"].values())


def test_confluence_buckets_partition_every_row():
    rows, series = _scored_rows(200)
    scored = attach_outcomes(rows, series, config=ExperimentConfig(horizons=(15,)))
    out = confluence_test(scored, ExperimentConfig(horizons=(15,)))
    assert sum(out["counts"].values()) == len(scored)


def test_wall_test_reports_both_definitions():
    rows, series = _scored_rows(300)
    scored = attach_outcomes(rows, series, config=ExperimentConfig(horizons=(30,)))
    out = wall_test(scored, ExperimentConfig(horizons=(30,)))
    assert set(out["arms"]) >= {
        "existing_call_wall",
        "mm_attributed_call_wall_a",
        "mm_attributed_call_wall_b",
    }
    assert "call" in out["comparison"] and "put" in out["comparison"]


# ---------------------------------------------------------------------------
# Verdict gating
# ---------------------------------------------------------------------------


def test_thin_sample_is_inconclusive_before_any_effect_is_examined():
    result = ExperimentResult(n_rows=50, n_scored=50)
    verdict = decide(result)
    assert verdict.code == "INCONCLUSIVE_SAMPLE"
    assert "not evidence of absence" in " ".join(verdict.rationale)


def test_incomplete_reconstruction_is_inconclusive_not_negative():
    """The distinction the whole diagnostic layer exists to make."""
    result = ExperimentResult(
        n_rows=5_000,
        n_scored=5_000,
        coverage={"mean_inventory_confidence": 0.8, "mean_gamma_coverage": 0.05},
    )
    verdict = decide(result)
    assert verdict.code == "INCONCLUSIVE_DATA"
    assert "not the methodology" in " ".join(verdict.rationale)


def test_low_confidence_also_gates_the_verdict():
    result = ExperimentResult(
        n_rows=5_000,
        n_scored=5_000,
        coverage={"mean_inventory_confidence": 0.2, "mean_gamma_coverage": 0.9},
    )
    assert decide(result).code == "INCONCLUSIVE_DATA"


def test_no_incremental_value_yields_a_negative_verdict():
    result = ExperimentResult(
        n_rows=5_000,
        n_scored=5_000,
        coverage={"mean_inventory_confidence": 0.8, "mean_gamma_coverage": 0.6},
        incremental={
            "ok": True,
            "horizons": {
                "15m": {
                    "realized_vol_regression": {
                        "ok": True,
                        "delta_adj_r2": 0.0001,
                        "f_p_value": 0.7,
                    },
                    "walk_forward_realized_vol": {
                        "mean_delta_oos_r2": -0.002,
                        "folds_improved": 1,
                        "n_folds": 5,
                    },
                }
            },
        },
        multiplicity={"n_tests": 100, "n_significant_bh": 0},
    )
    verdict = decide(result)
    assert verdict.code == "NO"
    assert "not improved on" in " ".join(verdict.rationale)


def test_incremental_value_surviving_out_of_sample_yields_yes():
    horizons = {
        f"{h}m": {
            "realized_vol_regression": {"ok": True, "delta_adj_r2": 0.03, "f_p_value": 0.001},
            "walk_forward_realized_vol": {
                "mean_delta_oos_r2": 0.02,
                "folds_improved": 4,
                "n_folds": 5,
            },
        }
        for h in (15, 30)
    }
    result = ExperimentResult(
        n_rows=5_000,
        n_scored=5_000,
        coverage={"mean_inventory_confidence": 0.8, "mean_gamma_coverage": 0.6},
        incremental={"ok": True, "horizons": horizons},
    )
    assert decide(result).code == "YES"


def test_confluence_only_effect_yields_the_confirmation_verdict():
    result = ExperimentResult(
        n_rows=5_000,
        n_scored=5_000,
        coverage={"mean_inventory_confidence": 0.8, "mean_gamma_coverage": 0.6},
        incremental={
            "ok": True,
            "horizons": {
                "15m": {
                    "realized_vol_regression": {"ok": True, "delta_adj_r2": 0.0, "f_p_value": 0.9},
                    "walk_forward_realized_vol": {
                        "mean_delta_oos_r2": -0.01,
                        "folds_improved": 0,
                        "n_folds": 5,
                    },
                }
            },
        },
        confluence={
            "agreement_rate": 0.7,
            "counts": {},
            "by_horizon": {
                "15m": {"disagree_vs_agree": {"cohens_d": 0.4, "p_value": 0.001}},
                "30m": {"disagree_vs_agree": {"cohens_d": 0.35, "p_value": 0.002}},
            },
        },
    )
    verdict = decide(result)
    assert verdict.code == "USEFUL_AS_CONFIRMATION"
    assert "confirmation, not replacement" in " ".join(verdict.rationale)


def test_report_never_calls_the_metric_true_dealer_gex():
    """Requirement 17, enforced in the rendered prose."""
    rows, series = _scored_rows(300)
    result = run_experiment(
        rows, series, config=ExperimentConfig(horizons=(15,), n_boot=50), sampling_minutes=1
    )
    text = render_markdown(result)
    lowered = text.lower()
    assert "true dealer gex" not in lowered.replace('"true dealer gex"', "")
    assert "market-maker attributed gex" in lowered
    assert "must not be described" in lowered


def test_report_writes_markdown_and_a_machine_readable_result(tmp_path):
    rows, series = _scored_rows(250)
    result = run_experiment(
        rows, series, config=ExperimentConfig(horizons=(15,), n_boot=50), sampling_minutes=1
    )
    path = write_report(result, tmp_path / "report.md", provenance={"reconstruction": {}})
    assert path.exists()
    payload = json.loads(path.with_suffix(".json").read_text())
    assert payload["verdict"]["code"]
    assert "coverage" in payload["result"]


# ---------------------------------------------------------------------------
# Full plumbing check
# ---------------------------------------------------------------------------


def test_pipeline_check_runs_every_layer_and_labels_itself_synthetic():
    payload = run_pipeline_check(n_sessions=3)
    assert payload["ok"] is True
    assert payload["synthetic"] is True
    assert payload["dataset_rows"] > 0
    assert payload["rows_with_mm_gamma"] > 0
    assert payload["rows_with_existing_gamma"] > 0
    assert payload["report_chars"] > 500
    assert "not evidence" in payload["warning"]
