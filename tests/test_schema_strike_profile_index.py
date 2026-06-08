"""Regression: setup/database/schema.sql must create the composite index
``(underlying, expiration, timestamp DESC)`` on gex_by_strike that the
strike-profile timeseries endpoint relies on for per-expiration mode.

``/api/gex/strike-profile-timeseries?expirations=<date>`` JOINs
gex_by_strike at ~window_units rep_ts values and filters on expiration.
Without this composite index PG can only use ``(underlying, timestamp,
strike)`` and fetches every strike at each rep_ts, then filters in
memory — that's ~30x more rows read than necessary and turns an
otherwise sub-second query into a multi-second one on a long-window
request.

Pure/hermetic: parses the SQL text, no database required.
"""

from __future__ import annotations

from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "setup" / "database" / "schema.sql"


def test_strike_profile_composite_index_declared():
    """The composite (underlying, expiration, timestamp DESC) index must
    appear in schema.sql.  An ``IF NOT EXISTS`` guard is fine — that's
    how every other gex_by_strike index is declared, and idempotency
    means it ships safely to existing deployments."""
    sql = SCHEMA_PATH.read_text()
    assert (
        "idx_gex_by_strike_underlying_expiration_timestamp" in sql
    ), "the per-expiration filter index is missing from schema.sql"
    # Spot-check the index definition itself, not just the name, so a
    # rename + accidental reorder of columns doesn't slip through.
    assert (
        "ON gex_by_strike(underlying, expiration, timestamp DESC)" in sql
    ), "the per-expiration filter index has the wrong column order"
