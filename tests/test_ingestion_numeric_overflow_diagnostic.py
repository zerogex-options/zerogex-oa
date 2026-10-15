"""Naming the column and the contract behind a numeric field overflow.

On 2026-10-14 an NDX batch on the ThetaData feed failed to write at 15:51 ET
with Postgres's "numeric field overflow". The message names no column, no row
and no value. Every retry re-submitted the same poisoned batch and failed the
same way, so NDX stopped persisting for the last nine minutes of the session
while the retained buffer grew from 2,747 rows to 14,281. Production, on
TradeStation, wrote NDX to 15:59 without trouble.

Nine minutes of a symbol silently vanishing is bad on a rehearsal and worse in
production, where NDX expires daily and the end of the session is when its flow
matters most. These tests cover the diagnostic that turns that message into a
named contract and field.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.ingestion.main_engine import (
    OPTION_ROW_NUMERIC_BOUNDS,
    _numeric_overflow_offenders,
)

SCHEMA = Path(__file__).resolve().parents[1] / "setup" / "database" / "schema.sql"


def _schema_option_chain_numerics() -> dict[str, tuple[int, int]]:
    """Every constrained NUMERIC column on option_chains, from the schema."""
    sql = SCHEMA.read_text(encoding="utf-8")
    start = sql.index("CREATE TABLE IF NOT EXISTS option_chains")
    end = sql.index("CREATE TABLE", start + 10)
    cols = {
        m.group(1): (int(m.group(2)), int(m.group(3)))
        for m in re.finditer(r"^\s*(\w+)\s+NUMERIC\((\d+),\s*(\d+)\)", sql[start:end], re.M)
    }
    for m in re.finditer(
        r"ALTER TABLE option_chains ADD COLUMN IF NOT EXISTS (\w+) NUMERIC\((\d+),\s*(\d+)\)",
        sql,
    ):
        cols.setdefault(m.group(1), (int(m.group(2)), int(m.group(3))))
    return cols


def test_bounds_match_the_schema_exactly():
    """The table is maintained by hand, so pin it to its source.

    A migration that widens or narrows a column without touching this table
    would make the diagnostic point at the wrong field, which is worse than no
    diagnostic: it sends the next reader somewhere confidently wrong.
    """
    schema = _schema_option_chain_numerics()
    assert set(schema) == set(OPTION_ROW_NUMERIC_BOUNDS), (
        f"missing from bounds: {sorted(set(schema) - set(OPTION_ROW_NUMERIC_BOUNDS))}; "
        f"not in schema: {sorted(set(OPTION_ROW_NUMERIC_BOUNDS) - set(schema))}"
    )
    for column, (precision, scale) in schema.items():
        expected = 10 ** (precision - scale)
        assert OPTION_ROW_NUMERIC_BOUNDS[column] == expected, (
            f"{column} is NUMERIC({precision},{scale}) so values must stay under "
            f"{expected}, but the bounds table says {OPTION_ROW_NUMERIC_BOUNDS[column]}"
        )


def test_finds_the_contract_and_the_field():
    rows = [
        {"option_symbol": "NDXP261014C30000", "theta": -12.5, "gamma": 0.0001},
        {"option_symbol": "NDXP261014C30500", "theta": -48213.7, "gamma": 0.0002},
        {"option_symbol": "NDXP261014P30500", "theta": -9.0, "gamma": 0.0003},
    ]
    offenders = _numeric_overflow_offenders(rows)
    assert len(offenders) == 1
    symbol, column, value, bound = offenders[0]
    assert symbol == "NDXP261014C30500"
    assert column == "theta"
    assert value == -48213.7
    assert bound == 1e4


def test_an_expiry_day_theta_on_a_30000_point_index_is_the_shape_we_expect():
    """Theta is NUMERIC(10,6): four digits before the point, so |theta| < 10000.

    Per-day theta for an at-the-money NDX option minutes from expiry runs to
    hundreds; with one of the 250%-plus solved IVs this record has seen before,
    it clears 10,000. SPY never approaches it, which is why this surfaced only
    once NDX was ingesting.
    """
    rows = [{"option_symbol": "NDXP261014C30550", "theta": -10092.4}]
    offenders = _numeric_overflow_offenders(rows)
    assert offenders and offenders[0][1] == "theta"

    # Just inside the bound must NOT be reported.
    assert not _numeric_overflow_offenders([{"option_symbol": "X", "theta": -9999.9}])


def test_worst_offender_comes_first_so_a_truncated_list_still_carries_the_cause():
    rows = [
        {"option_symbol": "A", "theta": -10001.0},  # 1.0001x its bound
        {"option_symbol": "B", "implied_volatility": 5000.0},  # 50x its bound
        {"option_symbol": "C", "vega": -20000.0},  # 2x its bound
    ]
    assert [o[0] for o in _numeric_overflow_offenders(rows)] == ["B", "C", "A"]


def test_the_limit_caps_the_report():
    rows = [{"option_symbol": f"S{i}", "theta": -20000.0} for i in range(50)]
    assert len(_numeric_overflow_offenders(rows)) == 10
    assert len(_numeric_overflow_offenders(rows, limit=3)) == 3


def test_a_clean_batch_reports_nothing():
    rows = [
        {
            "option_symbol": "SPY261014C600",
            "strike": 600.0,
            "last": 1.23,
            "implied_volatility": 0.18,
            "delta": 0.52,
            "gamma": 0.0412,
            "theta": -0.35,
            "vega": 0.11,
            "charm": 0.0001,
            "vanna": -0.0002,
        },
    ]
    assert _numeric_overflow_offenders(rows) == []


def test_boundary_is_at_or_above_not_merely_above():
    """Postgres refuses a value EQUAL to 10^(precision-scale), not just above."""
    assert _numeric_overflow_offenders([{"option_symbol": "X", "theta": 10000.0}])
    assert not _numeric_overflow_offenders([{"option_symbol": "X", "theta": 9999.999999}])


def test_nulls_and_unparseable_values_are_skipped_not_reported():
    """A non-numeric in a numeric column fails with a DIFFERENT Postgres error.

    Reporting it here would send the reader after the wrong fault.
    """
    rows = [
        {"option_symbol": "A", "theta": None, "gamma": None},
        {"option_symbol": "B", "theta": "not-a-number"},
        {"option_symbol": "C", "theta": float("nan")},
    ]
    assert _numeric_overflow_offenders(rows) == []


def test_a_row_with_no_symbol_still_reports_its_field():
    """Never drop an offender just because its identity is missing."""
    offenders = _numeric_overflow_offenders([{"theta": -50000.0}])
    assert len(offenders) == 1
    assert offenders[0][0] == "?"
    assert offenders[0][1] == "theta"


def test_several_bad_fields_on_one_row_are_all_reported():
    rows = [{"option_symbol": "NDXP261014C31000", "theta": -50000.0, "vega": 30000.0}]
    assert {o[1] for o in _numeric_overflow_offenders(rows)} == {"theta", "vega"}
