"""Coverage for ``INGEST_MONTHLY_EXPIRATIONS`` / monthly chain expansion.

The primary (weekly) chain selected via ``SYMBOL_ALIASES`` +
``OPTION_ROOT_ALIASES`` lists only weekly expirations for index
underlyings -- SPX's AM-settled monthlies live under a separate TS
chain (``$SPX.X`` -> root ``SPX``).  Bumping ``INGEST_EXPIRATIONS``
does not reach them because the chain itself doesn't list them.

The monthly chain expansion adds N extra expirations from the chain
mapped per-symbol via ``INGEST_MONTHLY_UNDERLYING_ALIASES``.  These
tests pin:

  * Monthly expirations are fetched from the mapped chain (not the
    primary one) and merged on top of the weekly window.
  * Each expiration's strikes/option-symbols use the chain it came from
    so a weekly SPXW date and a monthly SPX date produce option symbols
    with the matching root prefix.
  * Disabling the monthly expansion (default ``num_monthly_expirations
    = 0``) preserves the prior behavior exactly.
  * A misconfigured setup (count > 0 but no monthly mapping) logs a
    warning and DEGRADES TO WEEKLY-ONLY rather than crashing.
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional
from unittest.mock import MagicMock

import pytest

from src.ingestion.stream_manager import StreamManager
from src import symbols as symbols_module


WEEKLY_TS = "$SPXW.X"
MONTHLY_TS = "$SPX.X"
DB_SYM = "SPX"


@pytest.fixture(autouse=True)
def configure_aliases(monkeypatch):
    """Set the SPX weekly+monthly chain config for every test in the module."""
    monkeypatch.setenv("SYMBOL_ALIASES", f"SPX={WEEKLY_TS}")
    monkeypatch.setenv("OPTION_ROOT_ALIASES", f"{WEEKLY_TS}=SPXW,{MONTHLY_TS}=SPX")
    monkeypatch.setenv("INGEST_MONTHLY_UNDERLYING_ALIASES", f"SPX={MONTHLY_TS}")


def _build_manager(
    *,
    num_expirations: int,
    num_monthly_expirations: int,
    monthly_underlying: Optional[str],
    weekly_dates: List[date],
    monthly_dates: List[date],
    strikes: List[float] = (5000.0, 5050.0, 5100.0),  # type: ignore[assignment]
):
    """Build a StreamManager with a fake client whose expirations/strikes
    endpoints are stubbed to return the requested chain payloads."""
    fake_client = MagicMock()

    def get_option_expirations(ts_symbol, strike_price=None):
        if ts_symbol == WEEKLY_TS:
            return list(weekly_dates)
        if ts_symbol == MONTHLY_TS:
            return list(monthly_dates)
        return []

    def get_option_strikes(ts_symbol, expiration=None):
        return list(strikes)

    def build_option_symbol(underlying, expiration, opt_type, strike):
        root = symbols_module.resolve_option_root(underlying)
        return f"{root} {expiration:%y%m%d}{opt_type}{int(strike)}"

    fake_client.get_option_expirations.side_effect = get_option_expirations
    fake_client.get_option_strikes.side_effect = get_option_strikes
    fake_client.build_option_symbol.side_effect = build_option_symbol

    mgr = StreamManager(
        client=fake_client,
        underlying=WEEKLY_TS,
        db_underlying=DB_SYM,
        num_expirations=num_expirations,
        strike_count_max=40,
        strike_pct_range=3.0,
        num_monthly_expirations=num_monthly_expirations,
        monthly_underlying=monthly_underlying,
    )
    mgr.current_price = 5050.0
    return mgr


def test_monthly_expirations_layered_on_top_of_weeklies():
    """3 weeklies + 2 monthlies layered. Collision date tracks BOTH chains."""
    weekly = [date(2026, 6, 17), date(2026, 6, 19), date(2026, 6, 24)]
    monthly = [date(2026, 6, 19), date(2026, 7, 17), date(2026, 8, 21)]
    mgr = _build_manager(
        num_expirations=3,
        num_monthly_expirations=2,
        monthly_underlying=MONTHLY_TS,
        weekly_dates=weekly,
        monthly_dates=monthly,
    )

    exps = mgr._get_target_expirations()

    expected_dates = sorted(set(weekly) | set(monthly[:2]))
    assert exps == expected_dates

    # Pure-weekly dates: only the primary chain mapped.
    assert mgr._expiration_underlying[date(2026, 6, 17)] == [WEEKLY_TS]
    assert mgr._expiration_underlying[date(2026, 6, 24)] == [WEEKLY_TS]

    # Pure-monthly date: only the monthly chain mapped.
    assert mgr._expiration_underlying[date(2026, 7, 17)] == [MONTHLY_TS]

    # Collision date (e.g. Juneteenth-displaced SPX June OPEX falling on
    # a SPXW weekly): BOTH chains are tracked so both contracts ingest.
    assert mgr._expiration_underlying[date(2026, 6, 19)] == [WEEKLY_TS, MONTHLY_TS]


def test_monthly_off_by_default_preserves_legacy_behavior():
    """num_monthly_expirations=0 must produce a weekly-only universe."""
    weekly = [date(2026, 6, 17), date(2026, 6, 19), date(2026, 6, 24)]
    monthly = [date(2026, 7, 17), date(2026, 8, 21)]
    mgr = _build_manager(
        num_expirations=3,
        num_monthly_expirations=0,
        monthly_underlying=MONTHLY_TS,
        weekly_dates=weekly,
        monthly_dates=monthly,
    )

    exps = mgr._get_target_expirations()

    assert exps == weekly
    assert all(mgr._expiration_underlying[d] == [WEEKLY_TS] for d in exps)


def test_monthly_count_without_mapping_silently_uses_primary_chain(caplog):
    """count > 0 but no monthly_underlying -> primary chain only, NO WARN.

    The env var is a single global knob applied across every worker;
    equity ETFs (SPY, QQQ, ...) share weeklies+monthlies on one chain
    so the absence of a per-symbol mapping is the EXPECTED config for
    them, not a misconfiguration. Emitting WARN per ETF worker on every
    startup was actionable-looking noise.
    """
    weekly = [date(2026, 6, 17), date(2026, 6, 19), date(2026, 6, 24)]
    with caplog.at_level("WARNING"):
        mgr = _build_manager(
            num_expirations=3,
            num_monthly_expirations=2,
            monthly_underlying=None,
            weekly_dates=weekly,
            monthly_dates=[date(2026, 7, 17), date(2026, 8, 21)],
        )

    # Init must NOT emit a warning here.
    assert not any(
        "INGEST_MONTHLY_EXPIRATIONS" in rec.message and rec.levelname == "WARNING"
        for rec in caplog.records
    ), [rec.message for rec in caplog.records if rec.levelname == "WARNING"]

    exps = mgr._get_target_expirations()
    assert exps == weekly
    assert all(mgr._expiration_underlying[d] == [WEEKLY_TS] for d in exps)


def test_collision_date_produces_both_chain_option_symbols():
    """Regression: 2026-06-18 is both a SPXW weekly and (Juneteenth-displaced)
    SPX June 2026 monthly OPEX. Both must yield contracts -- earlier code
    deduped and dropped the AM-settled monthly OPEX, the exact contract
    the user most needs ingested during OPEX week."""
    shared = date(2026, 6, 18)
    weekly = [date(2026, 6, 17), shared, date(2026, 6, 22)]
    monthly = [shared, date(2026, 7, 17), date(2026, 8, 21)]
    mgr = _build_manager(
        num_expirations=3,
        num_monthly_expirations=3,
        monthly_underlying=MONTHLY_TS,
        weekly_dates=weekly,
        monthly_dates=monthly,
        strikes=[5050.0],
    )

    mgr.target_expirations = mgr._get_target_expirations()
    syms = mgr._build_option_symbols()

    # On 2026-06-18 we must have BOTH SPXW and SPX option symbols.
    shared_weekly = [s for s in syms if "260618" in s and s.startswith("SPXW ")]
    shared_monthly = [s for s in syms if "260618" in s and s.startswith("SPX ") and not s.startswith("SPXW ")]
    assert shared_weekly, syms
    assert shared_monthly, syms
    assert len(shared_weekly) == 2  # C + P
    assert len(shared_monthly) == 2


def test_option_symbols_use_chain_root_per_expiration():
    """SPXW prefix for weekly dates, SPX prefix for monthly-only dates."""
    weekly = [date(2026, 6, 17)]
    monthly = [date(2026, 7, 17)]  # not in weekly -> uses monthly chain
    mgr = _build_manager(
        num_expirations=1,
        num_monthly_expirations=1,
        monthly_underlying=MONTHLY_TS,
        weekly_dates=weekly,
        monthly_dates=monthly,
        strikes=[5050.0],
    )

    mgr.target_expirations = mgr._get_target_expirations()
    syms = mgr._build_option_symbols()

    weekly_syms = [s for s in syms if s.startswith("SPXW ")]
    monthly_syms = [s for s in syms if s.startswith("SPX ") and not s.startswith("SPXW ")]
    assert weekly_syms, syms
    assert monthly_syms, syms

    # 2 contracts per (expiration, strike) — C and P.
    assert len(weekly_syms) == 2
    assert len(monthly_syms) == 2

    # Built option symbol embeds the expiration date in YYMMDD.
    assert all("260617" in s for s in weekly_syms)
    assert all("260717" in s for s in monthly_syms)


def test_monthly_underlying_matching_primary_is_treated_as_disabled(caplog):
    """If the operator passes monthly_underlying equal to the primary chain,
    treat it as no second chain (no double-fetching the same symbol)."""
    weekly = [date(2026, 6, 17), date(2026, 6, 19), date(2026, 6, 24)]
    monthly = [date(2026, 7, 17), date(2026, 8, 21)]

    mgr = _build_manager(
        num_expirations=3,
        num_monthly_expirations=2,
        monthly_underlying=WEEKLY_TS,  # same as primary
        weekly_dates=weekly,
        monthly_dates=monthly,
    )

    assert mgr.monthly_underlying is None

    exps = mgr._get_target_expirations()
    assert exps == weekly  # no monthly expansion happened.
