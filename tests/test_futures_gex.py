"""Futures GEX pipeline + dealer-sign policy tests (src/futures/gex.py)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.futures.contracts import PutCall
from src.futures.gex import (
    CALCULATION_VERSION,
    FuturesGexEngine,
    OptionInput,
    aggregate_by_underlying,
)
from src.futures.sign_policy import (
    InvertedSignPolicy,
    LongCallShortPutSignPolicy,
    get_sign_policy,
)

CT = ZoneInfo("America/Chicago")
NOW = datetime(2026, 3, 2, 10, 0, tzinfo=CT)
EXP = datetime(2026, 3, 20, 8, 30, tzinfo=CT)  # ~18 DTE


def _chain(multiplier: int = 50, oi: int = 100, iv: float = 0.15, contract="ESH26"):
    opts = []
    for k in range(5800, 6205, 25):
        for pc in (PutCall.CALL, PutCall.PUT):
            opts.append(
                OptionInput(
                    strike=float(k),
                    put_call=pc,
                    open_interest=oi,
                    expiration=EXP,
                    underlying_contract_id=contract,
                    multiplier=multiplier,
                    iv=iv,
                )
            )
    return opts


def _engine():
    return FuturesGexEngine(risk_free_rate=0.04)


def test_metadata_is_complete():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(),
        futures_price=6000.0,
        now=NOW,
        contract_expiration=EXP,
        open_interest_as_of=NOW,
    )
    assert res.analytics_source == "cme_futures_options"
    assert res.calculation_version == CALCULATION_VERSION
    assert res.dealer_sign_policy == "v1_calls_long_puts_short"
    assert res.gex_unit == "dollar_gamma_per_1pct_move"
    assert res.multiplier == 50
    assert res.resolved_contract == "ESH26"
    assert res.contract_expiration == EXP.isoformat()
    assert res.quality_status == "good"
    assert res.quality_reasons == []


def test_call_gex_positive_put_gex_negative():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(),
        futures_price=6000.0,
        now=NOW,
    )
    assert res.call_gex > 0
    assert res.put_gex < 0
    assert res.net_gex == pytest.approx(res.call_gex + res.put_gex)


def test_multiplier_awareness_scales_gex():
    # Same chain, multiplier 100 vs 50 -> exactly 2x GEX (the whole point).
    e = _engine()
    r50 = e.compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(multiplier=50),
        futures_price=6000.0,
        now=NOW,
    )
    r100 = e.compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(multiplier=100),
        futures_price=6000.0,
        now=NOW,
    )
    assert r100.net_gex == pytest.approx(2.0 * r50.net_gex, rel=1e-9)
    assert r100.call_gex == pytest.approx(2.0 * r50.call_gex, rel=1e-9)


def test_sign_policy_inversion_flips_sign():
    normal = FuturesGexEngine(sign_policy=LongCallShortPutSignPolicy())
    inverted = FuturesGexEngine(sign_policy=InvertedSignPolicy())
    chain = _chain()
    a = normal.compute(
        logical_symbol="ES", resolved_contract="ESH26", options=chain, futures_price=6000.0, now=NOW
    )
    b = inverted.compute(
        logical_symbol="ES", resolved_contract="ESH26", options=chain, futures_price=6000.0, now=NOW
    )
    assert b.net_gex == pytest.approx(-a.net_gex, rel=1e-9)
    assert b.dealer_sign_policy == "v1_calls_short_puts_long"


def test_gamma_flip_within_band_near_spot():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(),
        futures_price=6000.0,
        now=NOW,
    )
    assert res.gamma_flip is not None
    # Symmetric chain -> flip should be within the ±20% profile band.
    assert 4800.0 <= res.gamma_flip <= 7200.0


def test_max_pain_is_a_listed_strike():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(),
        futures_price=6000.0,
        now=NOW,
    )
    assert res.max_pain is not None
    assert res.max_pain % 25 == 0


def test_walls_bracket_spot():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(),
        futures_price=6000.0,
        now=NOW,
    )
    if res.call_wall.strike is not None:
        assert res.call_wall.strike >= 6000.0
    if res.put_wall.strike is not None:
        assert res.put_wall.strike <= 6000.0


def test_zero_oi_chain_is_unavailable():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(oi=0),
        futures_price=6000.0,
        now=NOW,
    )
    assert res.quality_status == "unavailable"
    assert any("open interest" in r for r in res.quality_reasons)
    assert res.net_gex == 0.0


def test_iv_solved_from_market_price():
    # Provide market price instead of IV; engine must solve Black-76 IV.
    from src.futures import black76

    price = black76.price(
        6000.0, 6000.0, black76.years_to_expiration(NOW, EXP), 0.15, is_call=True, rate=0.04
    )
    opt = OptionInput(
        strike=6000.0,
        put_call=PutCall.CALL,
        open_interest=500,
        expiration=EXP,
        underlying_contract_id="ESH26",
        multiplier=50,
        market_price=price,
    )
    res = _engine().compute(
        logical_symbol="ES", resolved_contract="ESH26", options=[opt], futures_price=6000.0, now=NOW
    )
    assert res.call_gex > 0
    assert res.quality_status == "good"


def test_by_underlying_and_expiration_breakdowns():
    res = _engine().compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(),
        futures_price=6000.0,
        now=NOW,
    )
    assert set(res.by_underlying_contract) == {"ESH26"}
    assert set(res.by_expiration) == {"2026-03-20"}


def test_aggregate_by_underlying_keeps_contracts_separate():
    e = _engine()
    r1 = e.compute(
        logical_symbol="ES",
        resolved_contract="ESH26",
        options=_chain(contract="ESH26"),
        futures_price=6000.0,
        now=NOW,
    )
    r2 = e.compute(
        logical_symbol="ES",
        resolved_contract="ESM26",
        options=_chain(contract="ESM26"),
        futures_price=6050.0,
        now=NOW,
    )
    agg = aggregate_by_underlying([r1, r2])
    assert set(agg) == {"ESH26", "ESM26"}


def test_get_sign_policy_resolves_and_defaults():
    assert get_sign_policy("v1_calls_short_puts_long").version == "v1_calls_short_puts_long"
    assert get_sign_policy(None).version == "v1_calls_long_puts_short"
    assert get_sign_policy("bogus").version == "v1_calls_long_puts_short"
