"""Invariants for the database maintenance targets in the Makefile.

``option_chains_latest`` holds one row per contract, not a time series. Its
``timestamp`` column means "when this contract last ticked", which makes the
DATA_RETENTION_DAYS rule applied to DB_MAINTAIN_TABLES the wrong tool twice
over: it would delete a still-live but quietly-quoted contract, and it lets an
expired contract linger for 90 days after its last tick.

Before the expiry prune existed, nothing removed expired contracts from that
table at all -- a production NDX read found 5,304 of 6,811 strikes (78%)
belonging to 21 already-expired expirations, inflating every scan of the
table. These tests pin the split so a future maintainer tidying the Makefile
doesn't collapse the two lists back together.
"""

from __future__ import annotations

import re
from pathlib import Path

MAKEFILE = Path(__file__).resolve().parents[1] / "Makefile"


def _make_var(name: str) -> list[str]:
    """Read a (possibly backslash-continued) simple Make variable."""
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(rf"^{re.escape(name)}\s*=\s*((?:.*\\\n)*.*)$", text, re.M)
    assert match, f"{name} not found in Makefile"
    return match.group(1).replace("\\\n", " ").split()


def _recipe(target: str) -> str:
    """Return the recipe body (tab-indented lines) for a Make target."""
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(rf"^{re.escape(target)}:.*\n((?:(?:\t.*)?\n)*)", text, re.M)
    assert match, f"{target} recipe not found in Makefile"
    return match.group(1)


def test_option_chains_latest_is_pruned_by_expiration():
    assert "option_chains_latest" in _make_var("DB_EXPIRY_PRUNE_TABLES")


def test_option_chains_latest_is_not_pruned_by_row_age():
    """The timestamp DELETE would remove a live contract that quotes quietly."""
    assert "option_chains_latest" not in _make_var("DB_MAINTAIN_TABLES")


def test_db_prune_deletes_expired_contracts():
    recipe = _recipe("db-prune")
    assert "DB_EXPIRY_PRUNE_TABLES" in recipe
    assert "expiration < CURRENT_DATE" in recipe


def test_db_prune_still_applies_the_retention_window():
    recipe = _recipe("db-prune")
    assert "DB_MAINTAIN_TABLES" in recipe
    assert "DATA_RETENTION_DAYS" in recipe


def test_expiry_pruned_tables_are_still_vacuumed():
    """option_chains_latest takes heavy ON CONFLICT DO UPDATE traffic."""
    assert "DB_EXPIRY_PRUNE_TABLES" in _recipe("vacuum")
    assert "DB_EXPIRY_PRUNE_TABLES" in _recipe("db-maintain")


def test_backtest_archive_stays_out_of_both_lists():
    """Documented invariant: it is the retention-exempt copy backtests read."""
    assert "option_chains_archive" not in _make_var("DB_MAINTAIN_TABLES")
    assert "option_chains_archive" not in _make_var("DB_EXPIRY_PRUNE_TABLES")
