"""Session-aware Greeks underlying-staleness gate.

``_enrich_with_greeks`` refuses to compute Greeks against a stale
underlying price. The regular cash session has dense ~60s underlying
bars, but pre/after-hours an equity/ETF underlying trades thinly and its
1-minute bars are legitimately minutes apart — a single tight gate would
refuse Greeks for the entire extended session (the observed production
symptom: continuous ``Refusing Greeks: underlying price is ~100s stale
(threshold 90s)``). The gate is therefore session-aware; this pins that
mapping.
"""

import types
from datetime import datetime
from unittest.mock import MagicMock

import pytz

from src.ingestion import main_engine as me
from src.ingestion.main_engine import IngestionEngine, _greeks_max_age_for_session

ET = pytz.timezone("US/Eastern")

_BASE = 90.0
_EXTENDED = 300.0


def test_regular_session_uses_base_gate():
    assert _greeks_max_age_for_session("regular", _BASE, _EXTENDED) == _BASE


def test_closed_session_uses_base_gate():
    # "closed" still rejects outright stale prices via the base gate;
    # underlying_feed_expected separately downgrades the log to DEBUG.
    assert _greeks_max_age_for_session("closed", _BASE, _EXTENDED) == _BASE


def test_pre_market_uses_extended_gate():
    assert _greeks_max_age_for_session("pre-market", _BASE, _EXTENDED) == _EXTENDED


def test_after_hours_uses_extended_gate():
    assert _greeks_max_age_for_session("after-hours", _BASE, _EXTENDED) == _EXTENDED


def test_extended_gate_is_wider_so_sparse_extended_bars_pass():
    # The production regression: a ~100s-old underlying in after-hours was
    # refused under the 90s base gate but is well within the extended gate.
    sparse_after_hours_age = 105.0
    base = _greeks_max_age_for_session("regular", _BASE, _EXTENDED)
    extended = _greeks_max_age_for_session("after-hours", _BASE, _EXTENDED)
    assert sparse_after_hours_age > base
    assert sparse_after_hours_age <= extended


# ---------------------------------------------------------------------------
# In-session staleness WARNING is throttled by wall-clock, not reject count.
# A dense option stream produces thousands of rejects/min against one stale
# underlying; the previous per-100-reject gate flooded the journal at the open
# (the reported symptom: 124 warnings in ~11s). The reject COUNTER still
# advances every reject — only the WARNING cadence is rate-limited.
# ---------------------------------------------------------------------------


def _stale_greeks_engine():
    """Minimal engine wired so ``_enrich_with_greeks`` takes the in-session
    stale-underlying reject branch without a real DB or Greeks calculator."""
    e = IngestionEngine.__new__(IngestionEngine)
    e.greeks_calculator = MagicMock()
    e.latest_underlying_price = 100.0
    # 1 hour stale relative to the option timestamps used below.
    e.latest_underlying_timestamp = ET.localize(datetime(2026, 5, 15, 11, 0))
    e.greeks_max_underlying_age_seconds = _BASE
    e.greeks_max_underlying_age_seconds_extended = _EXTENDED
    e.greeks_stale_underlying_rejects = 0
    e.greeks_stale_warn_interval_seconds = 60.0
    e._greeks_stale_last_warn_mono = 0.0
    e.db_symbol = "SPX"
    e.greeks_calculated = 0
    return e


def test_greeks_stale_warning_throttled_by_time(monkeypatch):
    e = _stale_greeks_engine()
    # Friday noon ET: regular cash session, underlying feed expected for SPX.
    option_ts = ET.localize(datetime(2026, 5, 15, 12, 0))

    clock = {"t": 10_000.0}
    monkeypatch.setattr(me, "_time", types.SimpleNamespace(monotonic=lambda: clock["t"]))
    fake_logger = MagicMock()
    monkeypatch.setattr(me, "logger", fake_logger)

    # A burst of 200 rejects in the same instant => exactly one WARNING,
    # but every reject is still counted and every Greek is refused.
    for _ in range(200):
        out = e._enrich_with_greeks({"timestamp": option_ts, "option_symbol": "X"})
        assert out["delta"] is None
    assert fake_logger.warning.call_count == 1
    assert e.greeks_stale_underlying_rejects == 200

    # Once the interval elapses, the next reject warns again.
    clock["t"] += 61.0
    e._enrich_with_greeks({"timestamp": option_ts, "option_symbol": "X"})
    assert fake_logger.warning.call_count == 2
