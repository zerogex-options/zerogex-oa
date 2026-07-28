"""Tests for futures security definitions, contract domain, and the resolver."""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.futures import security_definitions as sd
from src.futures.contracts import (
    FuturesOptionContract,
    PutCall,
    contract_label,
    month_code,
    notional_value,
    tick_value,
)
from src.futures.resolver import ContractResolver, RollPolicy

CT = ZoneInfo("America/Chicago")


# -- domain helpers ---------------------------------------------------------


def test_month_codes_and_labels():
    assert month_code(3) == "H"
    assert month_code(12) == "Z"
    assert contract_label("ES", 2026, 3) == "ESH26"
    assert contract_label("NQ", 2025, 12) == "NQZ25"


def test_decimal_safe_economics():
    # ES 6000.25 * $50 == exactly 300012.50 (no float drift).
    assert notional_value(6000.25, 50) == Decimal("300012.50")
    assert tick_value(0.25, 50) == Decimal("12.50")
    assert tick_value(0.25, 20) == Decimal("5.00")


def test_third_friday():
    assert sd.third_friday(2026, 3) == date(2026, 3, 20)
    assert sd.third_friday(2026, 6) == date(2026, 6, 19)
    assert sd.third_friday(2025, 12) == date(2025, 12, 19)


def test_expiration_instant_is_tz_aware_0830_ct():
    exp = sd.quarterly_expiration_instant(2026, 3)
    assert exp.tzinfo is not None
    assert exp == datetime(2026, 3, 20, 8, 30, tzinfo=CT)


def test_contract_requires_tz_aware_expiration():
    with pytest.raises(ValueError):
        FuturesOptionContract(
            series_id="s",
            product_symbol="ES",
            strike=6000,
            put_call=PutCall.CALL,
            expiration=datetime(2026, 3, 20, 8, 30),  # naive -> reject
            underlying_contract_id="ESH26",
            multiplier=50,
        )


# -- quarterly generation ---------------------------------------------------


def test_generate_quarterly_futures_front_next_flags():
    futs = sd.generate_quarterly_futures("ES", date(2026, 1, 5), count=4)
    labels = [c.label for c in futs]
    assert labels == ["ESH26", "ESM26", "ESU26", "ESZ26"]
    assert futs[0].is_front_contract and not futs[0].is_next_contract
    assert futs[1].is_next_contract and not futs[1].is_front_contract
    assert all(c.multiplier == 50 and c.tick_size == 0.25 for c in futs)


def test_generate_skips_past_quarterlies():
    # After March expiry, the front listed quarterly is June.
    futs = sd.generate_quarterly_futures("ES", date(2026, 4, 1), count=2)
    assert [c.label for c in futs] == ["ESM26", "ESU26"]


# -- mock provider (async) --------------------------------------------------


def test_mock_provider_end_to_end():
    provider = sd.MockFuturesSecurityDefinitionProvider(as_of=date(2026, 1, 5))

    async def _run():
        products = await provider.fetch_products()
        assert {p.symbol for p in products} == {"ES", "NQ"}
        futs = await provider.fetch_futures_contracts("ES")
        assert futs[0].label == "ESH26"
        series = await provider.fetch_option_series("ES")
        assert series[0].underlying_contract_id == "ESH26"
        opts = await provider.fetch_option_contracts(series[0].series_id)
        assert opts, "expected a strike ladder"
        # Every option pins its exact underlying dated future.
        assert all(o.underlying_contract_id == "ESH26" for o in opts)
        assert all(o.multiplier == 50 for o in opts)
        return opts

    opts = asyncio.run(_run())
    calls = [o for o in opts if o.is_call]
    puts = [o for o in opts if not o.is_call]
    assert len(calls) == len(puts)


# -- idempotent upserts (mock cursor) ---------------------------------------


def test_upsert_functions_execute_expected_rows():
    cursor = MagicMock()
    futs = sd.generate_quarterly_futures("ES", date(2026, 1, 5), count=4)
    n = sd.upsert_futures_contracts(cursor, futs)
    assert n == 4
    cursor.executemany.assert_called_once()
    _, rows = cursor.executemany.call_args[0]
    assert len(rows) == 4

    cursor2 = MagicMock()
    assert sd.upsert_products(cursor2, sd.all_products()) == 2
    cursor2.executemany.assert_called_once()


def test_upsert_empty_is_noop():
    cursor = MagicMock()
    assert sd.upsert_futures_contracts(cursor, []) == 0
    cursor.executemany.assert_not_called()


# -- resolver ---------------------------------------------------------------


@pytest.fixture
def es_contracts():
    return sd.generate_quarterly_futures("ES", date(2026, 1, 5), count=4)


def test_resolve_front_next_and_specific(es_contracts):
    r = ContractResolver(es_contracts)
    t = datetime(2026, 1, 15, 12, 0, tzinfo=CT)
    assert r.resolve_front_contract("ES", t).label == "ESH26"
    assert r.resolve_next_contract("ES", t).label == "ESM26"
    assert r.resolve_contract("ES", "front", t).label == "ESH26"
    assert r.resolve_contract("ES", "next", t).label == "ESM26"
    assert r.resolve_contract("ES", "ESU26", t).label == "ESU26"
    assert r.resolve_contract("ES", "NOPE", t) is None


def test_front_advances_after_expiration(es_contracts):
    r = ContractResolver(es_contracts)
    before = datetime(2026, 3, 19, 12, 0, tzinfo=CT)
    after = datetime(2026, 3, 20, 12, 0, tzinfo=CT)  # past 08:30 CT SOQ
    assert r.resolve_front_contract("ES", before).label == "ESH26"
    assert r.resolve_front_contract("ES", after).label == "ESM26"


def test_resolve_option_underlying_is_explicit(es_contracts):
    r = ContractResolver(es_contracts)
    opt = FuturesOptionContract(
        series_id="s",
        product_symbol="ES",
        strike=6000,
        put_call=PutCall.CALL,
        expiration=datetime(2026, 6, 19, 8, 30, tzinfo=CT),
        underlying_contract_id="ESM26",
        multiplier=50,
    )
    underlying = r.resolve_option_underlying(opt)
    assert underlying.label == "ESM26"


def test_calendar_roll_window(es_contracts):
    r = ContractResolver(es_contracts, policy=RollPolicy.CALENDAR, roll_days_before=8)
    # 5 days before March expiry -> in window -> recommend next (June).
    t = datetime(2026, 3, 15, 8, 30, tzinfo=CT)
    st = r.get_roll_state("ES", t)
    assert st.roll_window_active is True
    assert st.recommended_display_contract.label == "ESM26"
    # 20 days before -> outside window -> hold front.
    t2 = datetime(2026, 2, 28, 8, 30, tzinfo=CT)
    st2 = r.get_roll_state("ES", t2)
    assert st2.roll_window_active is False
    assert st2.recommended_display_contract.label == "ESH26"


def test_volume_policy_crossover(es_contracts):
    front = dataclasses.replace(es_contracts[0], volume=1000, open_interest=5000)
    nxt = dataclasses.replace(es_contracts[1], volume=1500, open_interest=2000)
    r = ContractResolver([front, nxt, *es_contracts[2:]], policy=RollPolicy.VOLUME)
    t = datetime(2026, 3, 10, 8, 30, tzinfo=CT)
    st = r.get_roll_state("ES", t)
    assert st.volume_ratio == pytest.approx(1.5)
    assert st.recommended_display_contract.label == "ESM26"  # volume crossed


def test_open_interest_policy_no_crossover(es_contracts):
    front = dataclasses.replace(es_contracts[0], volume=1000, open_interest=8000)
    nxt = dataclasses.replace(es_contracts[1], volume=1500, open_interest=2000)
    r = ContractResolver([front, nxt, *es_contracts[2:]], policy=RollPolicy.OPEN_INTEREST)
    t = datetime(2026, 3, 10, 8, 30, tzinfo=CT)
    st = r.get_roll_state("ES", t)
    assert st.open_interest_ratio == pytest.approx(0.25)
    assert st.recommended_display_contract.label == "ESH26"  # OI hasn't crossed


def test_hybrid_hard_roll_without_volume_data(es_contracts):
    # No volume/OI on the generated contracts, but 1 day to expiry -> hard roll.
    r = ContractResolver(es_contracts, policy=RollPolicy.HYBRID, hard_roll_days_before=2)
    t = datetime(2026, 3, 19, 8, 30, tzinfo=CT)  # ~1 day to March expiry
    st = r.get_roll_state("ES", t)
    assert st.recommended_display_contract.label == "ESM26"
    assert "hard roll" in st.reason


def test_hybrid_waits_for_crossover_in_window(es_contracts):
    # In the calendar window but > hard-roll days and no crossover -> hold.
    r = ContractResolver(es_contracts, policy=RollPolicy.HYBRID, roll_days_before=8)
    t = datetime(2026, 3, 14, 8, 30, tzinfo=CT)  # ~6 days out, no vol/OI
    st = r.get_roll_state("ES", t)
    assert st.recommended_display_contract.label == "ESH26"
    assert "no volume/OI crossover" in st.reason


def test_calendar_spread_from_prices(es_contracts):
    r = ContractResolver(es_contracts)
    t = datetime(2026, 3, 15, 8, 30, tzinfo=CT)
    st = r.get_roll_state("ES", t, prices={"ESH26": 6000.0, "ESM26": 6012.5})
    assert st.calendar_spread == pytest.approx(12.5)
