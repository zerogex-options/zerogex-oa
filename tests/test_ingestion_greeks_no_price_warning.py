"""Regression coverage for the "no underlying price yet" warning volume.

Option quotes drain before the first underlying bar arrives, so on every
restart the whole tracked universe takes the no-price branch. The guard was
``if self.greeks_calculated == 0`` -- but that counter only advances on the
first SUCCESSFUL Greek, so it never suppressed anything during the outage it
was meant to cover. Production logged ~1,500 of these in a 14-second burst
per restart, onto a root volume running at 86%.

These tests pin one warning per outage, and that a later outage warns again.
"""

from __future__ import annotations

import logging

from src.ingestion.main_engine import IngestionEngine


class _Calc:
    """Greeks calculator that succeeds, writing a gamma the caller can see."""

    def enrich_option_data(self, data, price):
        return {**data, "delta": 0.5, "gamma": 0.01, "theta": -1.0, "vega": 2.0}


def _engine(price=None):
    eng = object.__new__(IngestionEngine)
    eng.greeks_calculator = _Calc()
    eng.latest_underlying_price = price
    # None short-circuits the staleness gate, which is not what these tests
    # are about -- they cover the no-price branch and its warning volume.
    eng.latest_underlying_timestamp = None
    eng.greeks_calculated = 0
    eng._greeks_no_price_warned = False
    eng._greeks_stale_episode_started_mono = None
    eng.greeks_stale_underlying_rejects = 0
    return eng


def _quote(symbol="SPY 260828C650"):
    return {"option_symbol": symbol, "strike": 650.0, "implied_volatility": 0.2}


def _enrich(eng, data):
    """Call the Greeks-enrichment path under test."""
    return eng._enrich_with_greeks(data)


def test_no_price_warns_once_across_a_whole_batch(caplog):
    eng = _engine(price=None)
    with caplog.at_level(logging.WARNING):
        for i in range(500):
            _enrich(eng, _quote(f"O{i}"))
    hits = [r for r in caplog.records if "no underlying price available yet" in r.message]
    assert len(hits) == 1, f"expected 1 warning for 500 contracts, got {len(hits)}"


def test_greeks_are_nulled_while_no_price():
    eng = _engine(price=None)
    out = _enrich(eng, _quote())
    assert out["gamma"] is None
    assert out["delta"] is None


def test_a_later_outage_warns_again(caplog):
    """The latch must re-arm once a price arrives, or a real second outage
    would go completely unreported."""
    eng = _engine(price=None)
    with caplog.at_level(logging.WARNING):
        _enrich(eng, _quote("A"))
        _enrich(eng, _quote("B"))

        eng.latest_underlying_price = 650.0
        assert _enrich(eng, _quote("C"))["gamma"] == 0.01

        eng.latest_underlying_price = None
        _enrich(eng, _quote("D"))

    hits = [r for r in caplog.records if "no underlying price available yet" in r.message]
    assert len(hits) == 2, f"expected one warning per outage, got {len(hits)}"


def test_the_counter_no_longer_gates_the_warning(caplog):
    """greeks_calculated advancing must not silence a genuine outage.

    This is the exact inversion of the old bug: the old guard tied warning
    suppression to a counter that has nothing to do with whether a price is
    currently available.
    """
    eng = _engine(price=None)
    eng.greeks_calculated = 5000
    with caplog.at_level(logging.WARNING):
        _enrich(eng, _quote())
    hits = [r for r in caplog.records if "no underlying price available yet" in r.message]
    assert len(hits) == 1
