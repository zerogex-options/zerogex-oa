"""Regression: the moneyness delta proxy rises with spot, like a real delta.

The dealer-net-delta estimate every consumer reads (production gex_by_strike
rows carry no per-contract delta x OI) used ``0.5 - 10 x (S - K) / S`` for a
call: in-the-money calls got LOW deltas and out-of-the-money calls HIGH ones,
and puts were mirrored the same wrong way. As price rallied through call open
interest the estimate said dealers were getting longer delta when they were
getting shorter, so dealer_delta_pressure, its MSI component, market_pressure's
dealer pillar, range_break_imminence and Trade Bias leaned the wrong way.
"""

from __future__ import annotations

import pytest

from src.signals.components.utils import moneyness_delta_proxy


def test_the_proxy_has_a_delta_shape():
    spot = 606.0
    itm_call, itm_put = moneyness_delta_proxy(spot, 594.0)
    otm_call, otm_put = moneyness_delta_proxy(spot, 618.0)
    atm_call, atm_put = moneyness_delta_proxy(spot, 606.0)
    assert atm_call == pytest.approx(0.5) and atm_put == pytest.approx(-0.5)
    assert itm_call == pytest.approx(0.698, abs=1e-3)  # was 0.302
    assert otm_call == pytest.approx(0.302, abs=1e-3)  # was 0.698
    # Puts: deep in the money near -1, out of the money near 0; put = call - 1.
    assert otm_put == pytest.approx(otm_call - 1.0)
    assert itm_put == pytest.approx(itm_call - 1.0)
    assert moneyness_delta_proxy(spot, spot * 0.94) == (1.0, -0.0)
    assert moneyness_delta_proxy(spot, spot * 1.06) == (0.0, -1.0)


def test_call_delta_rises_with_spot():
    deltas = [moneyness_delta_proxy(s, 600.0)[0] for s in (590.0, 595.0, 600.0, 605.0, 610.0)]
    assert deltas == sorted(deltas)
    assert deltas[0] < deltas[-1]


def test_a_rally_through_call_oi_makes_dealers_shorter_delta():
    from src.signals.unified_signal_engine import UnifiedSignalEngine

    rows = [{"strike": 600.0, "call_oi": 10_000, "put_oi": 0}]
    before = UnifiedSignalEngine._estimate_dealer_net_delta(rows, 600.0)
    after = UnifiedSignalEngine._estimate_dealer_net_delta(rows, 606.0)
    # Dealers short the calls hedge long; as the calls go in the money the
    # customer's delta rises, so the dealer's net delta falls.
    assert after < before


def test_every_consumer_uses_the_shared_proxy():
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src" / "signals"
    for rel in (
        "unified_signal_engine.py",
        "basic/dealer_delta_pressure.py",
        "advanced/market_pressure.py",
        "advanced/range_break_imminence.py",
    ):
        text = (src / rel).read_text()
        assert "moneyness_delta_proxy(" in text, rel
        assert "0.5 - distance_pct * 10" not in text, rel
