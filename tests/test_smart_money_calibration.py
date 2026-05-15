"""Tests for distribution-based smart-money calibration (D6 follow-up).

The flow_smart_money score tiers are now driven by the per-symbol
rolling p95 of volume_delta / premium (from component_normalizer_cache)
when available, falling back per-field to the env-tunable static tiers
on a cold cache.  The IV / deep-OTM inclusion thresholds are per-symbol
env-tunable instead of hardcoded 0.4 / 0.15.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

# --- _smart_money_calibration -------------------------------------------------


def test_calibration_uses_distribution_when_p95_available():
    engine = AnalyticsEngine(underlying="SPY")
    vol_tiers, prem_tiers, mode = engine._smart_money_calibration(
        vol_p95=300.0, prem_p95=2_000_000.0, underlying_price=500.0
    )
    # Default multipliers 0.5 / 1 / 2 / 4 of p95.
    assert vol_tiers == (150, 300, 600, 1200)
    assert prem_tiers == (1_000_000.0, 2_000_000.0, 4_000_000.0, 8_000_000.0)
    assert mode == "vol=dist,prem=dist"


def test_calibration_falls_back_to_static_tiers_on_cold_cache():
    engine = AnalyticsEngine(underlying="SPY")
    vol_tiers, prem_tiers, mode = engine._smart_money_calibration(
        vol_p95=None, prem_p95=None, underlying_price=500.0
    )
    # Static volume tiers (raw contract counts).
    assert vol_tiers == (50, 100, 200, 500)
    # Static premium tiers = {1,2,5,10} x notional_per_contract (500*100).
    npc = 500.0 * 100.0
    assert prem_tiers == (1.0 * npc, 2.0 * npc, 5.0 * npc, 10.0 * npc)
    assert mode == "vol=tier,prem=tier"


def test_calibration_is_per_field_mixed_mode():
    """Volume p95 present but premium p95 missing -> vol=dist, prem=tier."""
    engine = AnalyticsEngine(underlying="SPY")
    vol_tiers, prem_tiers, mode = engine._smart_money_calibration(
        vol_p95=300.0, prem_p95=None, underlying_price=500.0
    )
    assert vol_tiers == (150, 300, 600, 1200)
    npc = 500.0 * 100.0
    assert prem_tiers == (1.0 * npc, 2.0 * npc, 5.0 * npc, 10.0 * npc)
    assert mode == "vol=dist,prem=tier"


def test_calibration_zero_p95_treated_as_cold():
    engine = AnalyticsEngine(underlying="SPY")
    vol_tiers, _, mode = engine._smart_money_calibration(
        vol_p95=0.0, prem_p95=-1.0, underlying_price=500.0
    )
    assert vol_tiers == (50, 100, 200, 500)
    assert mode == "vol=tier,prem=tier"


def test_calibration_tiny_p95_never_yields_zero_threshold():
    engine = AnalyticsEngine(underlying="SPY")
    vol_tiers, _, _ = engine._smart_money_calibration(
        vol_p95=0.4, prem_p95=None, underlying_price=500.0
    )
    # 0.5*0.4=0.2 -> rounds to 0 -> clamped to 1 (floor must admit
    # something meaningful, not every contract).
    assert vol_tiers[0] >= 1
    assert all(t >= 1 for t in vol_tiers)


def test_calibration_distribution_multipliers_env_tunable(monkeypatch):
    monkeypatch.setenv("SMART_MONEY_VOL_DIST_T1_P95_X", "0.25")
    monkeypatch.setenv("SMART_MONEY_PREM_DIST_T4_P95_X", "6.0")
    engine = AnalyticsEngine(underlying="SPY")
    vol_tiers, prem_tiers, _ = engine._smart_money_calibration(
        vol_p95=400.0, prem_p95=1_000_000.0, underlying_price=500.0
    )
    assert vol_tiers[0] == 100  # 0.25 * 400
    assert prem_tiers[3] == 6_000_000.0  # 6.0 * 1e6


# --- _symbol_tuned_float ------------------------------------------------------


def test_symbol_tuned_float_default_when_no_env():
    engine = AnalyticsEngine(underlying="SPY")
    assert engine._symbol_tuned_float("SMART_MONEY_IV_INCL", 0.4) == 0.4


def test_symbol_tuned_float_default_env_override(monkeypatch):
    monkeypatch.setenv("SMART_MONEY_IV_INCL_DEFAULT", "0.55")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine._symbol_tuned_float("SMART_MONEY_IV_INCL", 0.4) == 0.55


def test_symbol_tuned_float_per_symbol_beats_default(monkeypatch):
    monkeypatch.setenv("SMART_MONEY_IV_INCL_DEFAULT", "0.55")
    monkeypatch.setenv("SMART_MONEY_IV_INCL_SPX", "0.7")
    engine = AnalyticsEngine(underlying="SPX")
    assert engine._symbol_tuned_float("SMART_MONEY_IV_INCL", 0.4) == 0.7
    # A different symbol still uses the _DEFAULT, not SPX's value.
    other = AnalyticsEngine(underlying="SPY")
    assert other._symbol_tuned_float("SMART_MONEY_IV_INCL", 0.4) == 0.55


def test_symbol_tuned_float_ignores_garbage_and_nonpositive(monkeypatch):
    monkeypatch.setenv("SMART_MONEY_DEEP_OTM_DELTA_SPY", "not-a-number")
    monkeypatch.setenv("SMART_MONEY_DEEP_OTM_DELTA_DEFAULT", "-0.2")
    engine = AnalyticsEngine(underlying="SPY")
    # Garbage per-symbol + non-positive default -> hardcoded fallback.
    assert engine._symbol_tuned_float("SMART_MONEY_DEEP_OTM_DELTA", 0.15) == 0.15


# --- _fetch_smart_money_p95 ---------------------------------------------------


def test_fetch_smart_money_p95_parses_cache_rows():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("smart_money_volume_delta", 321.0),
        ("smart_money_premium", 1_500_000.0),
    ]
    vol_p95, prem_p95 = engine._fetch_smart_money_p95(cursor)
    assert vol_p95 == 321.0
    assert prem_p95 == 1_500_000.0


def test_fetch_smart_money_p95_cold_cache_returns_none():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    assert engine._fetch_smart_money_p95(cursor) == (None, None)


def test_fetch_smart_money_p95_swallows_errors():
    engine = AnalyticsEngine(underlying="SPY")
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("relation does not exist")
    # Must NOT raise — calibration falls back to static tiers.
    assert engine._fetch_smart_money_p95(cursor) == (None, None)


# --- end-to-end wiring into the flow_smart_money SQL --------------------------


def _scripted_cursor(cache_rows):
    cursor = MagicMock()
    cursor.rowcount = 0  # consumed by flow_by_contract %d logging
    cursor.fetchall.return_value = cache_rows
    return cursor


def _smart_money_call(cursor):
    for c in cursor.execute.call_args_list:
        sql = c[0][0]
        if "INSERT INTO flow_smart_money" in sql:
            return c
    raise AssertionError("flow_smart_money INSERT was never issued")


def test_refresh_flow_caches_wires_distribution_tiers_and_thresholds(monkeypatch):
    engine = AnalyticsEngine(underlying="SPY")
    engine._flow_cache_refresh_min_seconds = 0.0
    engine._last_flow_cache_ts = None

    cursor = _scripted_cursor(
        [
            ("smart_money_volume_delta", 300.0),
            ("smart_money_premium", 2_000_000.0),
        ]
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    @contextmanager
    def fake_db_connection():
        yield conn

    monkeypatch.setattr(main_engine, "db_connection", fake_db_connection)

    ts = datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc)
    engine._refresh_flow_caches(ts, underlying_price=500.0)

    params = _smart_money_call(cursor)[0][1]
    # Volume score tiers passed descending: t4, t3, t2, t1 at idx 4..7.
    assert params[4:8] == (1200, 600, 300, 150)
    # Premium score tiers descending at idx 8..11.
    assert params[8:12] == (8_000_000.0, 4_000_000.0, 2_000_000.0, 1_000_000.0)
    # Inclusion floors then IV / deep-OTM thresholds (defaults).
    assert params[14] == 150  # vol_t1 inclusion floor
    assert params[15] == 1_000_000.0  # prem_t1 inclusion floor
    assert params[16] == 0.4  # iv_incl default
    assert params[17] == 0.15  # deep_otm_delta default


def test_refresh_flow_caches_uses_per_symbol_inclusion_overrides(monkeypatch):
    monkeypatch.setenv("SMART_MONEY_IV_INCL_SPY", "0.65")
    monkeypatch.setenv("SMART_MONEY_DEEP_OTM_DELTA_SPY", "0.10")
    engine = AnalyticsEngine(underlying="SPY")
    engine._flow_cache_refresh_min_seconds = 0.0
    engine._last_flow_cache_ts = None

    cursor = _scripted_cursor([])  # cold cache -> static tiers
    conn = MagicMock()
    conn.cursor.return_value = cursor

    @contextmanager
    def fake_db_connection():
        yield conn

    monkeypatch.setattr(main_engine, "db_connection", fake_db_connection)

    ts = datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc)
    engine._refresh_flow_caches(ts, underlying_price=500.0)

    params = _smart_money_call(cursor)[0][1]
    # Cold cache -> static volume tiers.
    assert params[4:8] == (500, 200, 100, 50)
    # Per-symbol IV / deep-OTM overrides flow through.
    assert params[16] == 0.65
    assert params[17] == 0.10
