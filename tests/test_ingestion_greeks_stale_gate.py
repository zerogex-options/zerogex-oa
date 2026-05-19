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

from src.ingestion.main_engine import _greeks_max_age_for_session

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
