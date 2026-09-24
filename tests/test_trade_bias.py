"""Parity tests for the backend Trade Bias port.

Ported 1:1 from the front-end suite (frontend/tests/tradeBias.test.ts) so the
engine's regime/vote/state/confidence logic is provably identical to the
dashboard's. Keep the two suites in lockstep.
"""

from __future__ import annotations

import pytest

from src.signals.trade_bias.bias import BiasInput, compute_bias


def _bias(**kwargs):
    # BiasInput defaults every field to None (an unavailable signal).
    return compute_bias(BiasInput(**kwargs))


def test_trend_up_long_gamma_bullish_flow():
    r = _bias(
        netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=60, odtePositioning=60,
        positioningTrap=40, trapDetection=60, gammaVWAP=40, msi=50,
    )
    assert r.marketState == "TREND_UP"
    assert r.trend == "bullish"
    assert r.bias == "BUY_DIPS"
    assert r.confidence > 0
    assert r.hasData is True


def test_trend_down_long_gamma_bearish_flow():
    # msi is the 0-100 composite. 35 used to block TREND_DOWN outright.
    r = _bias(
        netGEX=50, gexGradient=60, tapeFlow=-80, vannaCharm=-60, odtePositioning=-60,
        positioningTrap=-40, trapDetection=-60, gammaVWAP=-40, msi=35,
    )
    assert r.marketState == "TREND_DOWN"
    assert r.trend == "bearish"
    assert r.bias == "SELL_RIPS"


def test_trap_reversal_short_gamma_bullish_flow_bearish_structure():
    r = _bias(
        netGEX=-50, gexGradient=-60, tapeFlow=80, vannaCharm=60, odtePositioning=60,
        positioningTrap=-40, trapDetection=-60, gammaVWAP=-40, msi=0,
    )
    assert r.marketState == "TRAP_REVERSAL"
    assert r.trend == "bearish"
    assert r.bias == "FADE_STRENGTH"


def test_trap_squeeze_short_gamma_bearish_flow_bullish_structure():
    r = _bias(
        netGEX=-50, gexGradient=-60, tapeFlow=-80, vannaCharm=-60, odtePositioning=-60,
        positioningTrap=40, trapDetection=60, gammaVWAP=40, msi=0,
    )
    assert r.marketState == "TRAP_SQUEEZE"
    assert r.trend == "bullish"
    assert r.bias == "FADE_WEAKNESS"


def test_chop_mixed_signals_with_four_inputs():
    r = _bias(netGEX=50, gexGradient=-60, tapeFlow=10, vannaCharm=-10, odtePositioning=5)
    assert r.marketState == "CHOP"
    assert r.trend == "neutral"
    assert r.bias == "RANGE_FADE"


def test_chop_confidence_calm_scores_higher_than_noisy():
    calm = _bias(
        netGEX=50, gexGradient=-60, tapeFlow=5, vannaCharm=-5, odtePositioning=5,
        positioningTrap=5, trapDetection=-5, gammaVWAP=5, msi=0,
    )
    noisy = _bias(
        netGEX=50, gexGradient=-60, tapeFlow=90, vannaCharm=-90, odtePositioning=90,
        positioningTrap=-90, trapDetection=90, gammaVWAP=-90, msi=90,
    )
    assert calm.marketState == "CHOP"
    assert noisy.marketState == "CHOP"
    assert calm.confidence > 0
    assert calm.confidence > noisy.confidence


def test_unknown_too_few_inputs():
    r = _bias(tapeFlow=10, vannaCharm=-10)
    assert r.marketState == "UNKNOWN"
    assert r.trend == "neutral"
    assert r.bias == "WAIT"
    assert r.hasData is False


def test_has_data_flips_true_at_three_inputs():
    two = _bias(tapeFlow=10, vannaCharm=-10)
    three = _bias(tapeFlow=10, vannaCharm=-10, netGEX=50)
    assert two.hasData is False
    assert three.hasData is True


@pytest.mark.parametrize("msi", [None, 0, 10, 11, 35, 65, 100])
def test_msi_never_gates_either_trend_state(msi):
    """The MSI is 0-100 regime strength. It used to gate the trend states as
    if it ran -100..+100: TREND_UP's ``msi >= -10`` always passed, while
    TREND_DOWN's ``msi <= 10`` needed the gauge at 10 or below."""
    up = _bias(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=60, odtePositioning=60, msi=msi)
    down = _bias(
        netGEX=50, gexGradient=60, tapeFlow=-80, vannaCharm=-60, odtePositioning=-60, msi=msi
    )
    assert up.marketState == "TREND_UP"
    assert down.marketState == "TREND_DOWN"


@pytest.mark.parametrize("msi", [5, 35, 80])
def test_trend_down_is_the_mirror_of_trend_up(msi):
    up = _bias(
        netGEX=50,
        gexGradient=60,
        tapeFlow=80,
        vannaCharm=60,
        odtePositioning=60,
        positioningTrap=40,
        trapDetection=60,
        gammaVWAP=40,
        msi=msi,
    )
    down = _bias(
        netGEX=50,
        gexGradient=60,
        tapeFlow=-80,
        vannaCharm=-60,
        odtePositioning=-60,
        positioningTrap=-40,
        trapDetection=-60,
        gammaVWAP=-40,
        msi=msi,
    )
    assert (up.bias, down.bias) == ("BUY_DIPS", "SELL_RIPS")
    assert down.confidence == up.confidence
    assert down.convictionDriven == up.convictionDriven


def test_2026_09_23_spx_late_morning_reads_trend_down():
    """SPX at 11:30 ET on 2026-09-23 (the session readout): long gamma by
    total net GEX, tape and vanna/charm both leaning bearish, MSI 14.5 --
    and the panel said Range-Bound while SPX fell another 15 points."""
    r = _bias(netGEX=50, gexGradient=-7, tapeFlow=-35, vannaCharm=-19, odtePositioning=-6, msi=14.5)
    assert r.marketState == "TREND_DOWN"
    assert r.bias == "SELL_RIPS"


def test_single_dominant_flow_signal_carries_majority():
    r = _bias(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=-15, odtePositioning=-5, msi=20)
    assert r.marketState == "TREND_UP"
    assert r.bias == "BUY_DIPS"
    assert r.convictionDriven is True


def test_broad_consensus_not_flagged_conviction_driven():
    r = _bias(netGEX=50, gexGradient=60, tapeFlow=40, vannaCharm=30, odtePositioning=30, msi=30)
    assert r.marketState == "TREND_UP"
    assert r.convictionDriven is False


def test_chop_never_flags_conviction_driven():
    r = _bias(netGEX=50, gexGradient=-60, tapeFlow=5, vannaCharm=-5, odtePositioning=5, msi=0)
    assert r.marketState == "CHOP"
    assert r.convictionDriven is False


def test_chop_surfaces_watching_entry_per_conviction_signal():
    r = _bias(netGEX=50, gexGradient=-60, tapeFlow=80, vannaCharm=-80, odtePositioning=5, msi=0)
    assert r.marketState == "CHOP"
    tape = next((w for w in r.watching if w.key == "tapeFlow"), None)
    vanna = next((w for w in r.watching if w.key == "vannaCharm"), None)
    assert tape is not None and tape.direction == "bullish"
    assert vanna is not None and vanna.direction == "bearish"
    assert len(r.watching) == 2


def test_directional_regimes_do_not_surface_watching():
    r = _bias(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=60, odtePositioning=60, msi=30)
    assert r.marketState == "TREND_UP"
    assert len(r.watching) == 0


def test_chop_with_no_conviction_signals_has_empty_watching():
    r = _bias(netGEX=50, gexGradient=-60, tapeFlow=30, vannaCharm=-30, odtePositioning=5, msi=0)
    assert r.marketState == "CHOP"
    assert len(r.watching) == 0


def test_single_dominant_structure_signal_carries_majority():
    r = _bias(
        netGEX=-50, gexGradient=-60, tapeFlow=80, vannaCharm=60, odtePositioning=60,
        positioningTrap=-10, trapDetection=-80, gammaVWAP=5, msi=0,
    )
    assert r.marketState == "TRAP_REVERSAL"


def test_opposing_dominant_flow_signals_cancel():
    r = _bias(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=-80, odtePositioning=-5, msi=0)
    assert r.marketState == "CHOP"


def test_confidence_clamps_into_range():
    r = _bias(
        netGEX=50, gexGradient=60, tapeFlow=100, vannaCharm=100, odtePositioning=100,
        positioningTrap=100, trapDetection=100, gammaVWAP=100, msi=100,
    )
    assert r.confidence >= 0
    assert r.confidence <= r.maxConfidence


def test_checklist_reports_short_gamma_trap_and_divergence_flags():
    r = _bias(
        netGEX=-50, gexGradient=-60, tapeFlow=80, vannaCharm=60, odtePositioning=60,
        positioningTrap=-40, trapDetection=-60, gammaVWAP=-40,
    )
    passed = {c.label: c.passed for c in r.checklist}
    assert passed["Short-gamma regime"] is True
    assert passed["Call-heavy tape flow"] is True
    assert passed["Trap detection triggered"] is True
    assert passed["Structure/flow divergence"] is True


def test_just_below_threshold_flow_does_not_trigger_trend_up():
    r = _bias(netGEX=50, gexGradient=60, tapeFlow=25, vannaCharm=12, odtePositioning=12, msi=50)
    assert r.marketState != "TREND_UP"


def test_trend_up_fires_with_two_of_three_flow_majority():
    r = _bias(netGEX=50, gexGradient=60, tapeFlow=80, vannaCharm=60, odtePositioning=-5, msi=30)
    assert r.marketState == "TREND_UP"
    assert r.bias == "BUY_DIPS"


def test_trap_reversal_fires_with_two_of_three_structure_majority():
    r = _bias(
        netGEX=-50, gexGradient=-60, tapeFlow=80, vannaCharm=60, odtePositioning=60,
        positioningTrap=-40, trapDetection=-60, gammaVWAP=5, msi=0,
    )
    assert r.marketState == "TRAP_REVERSAL"
    assert r.bias == "FADE_STRENGTH"


def test_strongly_contradicting_gradient_blocks_long_gamma():
    r = _bias(netGEX=50, gexGradient=-60, tapeFlow=80, vannaCharm=60, odtePositioning=60, msi=30)
    assert r.marketState != "TREND_UP"
    assert r.marketState == "CHOP"


def test_mildly_contradicting_gradient_does_not_block_long_gamma():
    r = _bias(netGEX=50, gexGradient=-10, tapeFlow=80, vannaCharm=60, odtePositioning=60, msi=30)
    assert r.marketState == "TREND_UP"
