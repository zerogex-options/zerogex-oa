"""Pin the safety guard inside ``tests/fixtures/seed_flow_series_parity``.

The seed script's first action is a TRUNCATE CASCADE across the
analytics + flow + signals + signal_trades tables.  Pointed at the
wrong DSN (e.g. an AWS RDS production instance), that wipes live data.
``_assert_dsn_is_safe_to_truncate`` is the only defense; this file
asserts the defense itself does not regress.

Lives in the regular tests/ tree (not under tests/fixtures/) so it
runs in the default pytest collection -- the guard MUST stay green
even on PRs that touch nothing flow-series-related.
"""

from __future__ import annotations

import pytest

from tests.fixtures.seed_flow_series_parity import _assert_dsn_is_safe_to_truncate


def test_guard_accepts_dbname_containing_test():
    """Canonical CI / dev convention: ``zerogex_test``.  The substring
    ``test`` in the dbname is a documented opt-in to destructive
    operations."""
    dsn = "postgresql://u:p@localhost:5432/zerogex_test"
    assert _assert_dsn_is_safe_to_truncate(dsn) == "zerogex_test"


def test_guard_accepts_case_insensitive_test():
    """Defense against ``TEST``-shaped dbnames written in caps; the
    substring match is case-insensitive on purpose so a typo'd
    convention doesn't slip through OR get unexpectedly refused."""
    dsn = "postgresql://u:p@localhost:5432/MyTestDB"
    assert _assert_dsn_is_safe_to_truncate(dsn) == "MyTestDB"


def test_guard_refuses_prod_shaped_dbname_without_override(monkeypatch):
    """The production database (``zerogexdb`` on RDS) MUST be refused
    without explicit opt-in.  Refusal exits with code 3 so the wrapper
    (make ci-parity / CI job) surfaces a clear failure rather than
    silently partial-running."""
    monkeypatch.delenv("CI_PARITY_ALLOW_NON_TEST_DB", raising=False)
    dsn = "postgresql://u:p@some-rds-endpoint.amazonaws.com:5432/zerogexdb"
    with pytest.raises(SystemExit) as excinfo:
        _assert_dsn_is_safe_to_truncate(dsn)
    assert excinfo.value.code == 3


def test_guard_allows_override_explicitly(monkeypatch):
    """The override env var is a deliberate human action -- the user
    has confirmed (by typing the variable) that the target IS
    disposable despite its name.  The guard logs a banner and proceeds."""
    monkeypatch.setenv("CI_PARITY_ALLOW_NON_TEST_DB", "yes")
    dsn = "postgresql://u:p@some-rds-endpoint.amazonaws.com:5432/zerogexdb"
    # Must NOT raise.
    assert _assert_dsn_is_safe_to_truncate(dsn) == "zerogexdb"


def test_guard_override_requires_exact_yes(monkeypatch):
    """A truthy-looking but non-``yes`` value (``true``, ``1``, ``on``)
    is NOT accepted -- the override must be explicit and unambiguous so
    misconfigured CI / env exports cannot accidentally permit
    destruction."""
    monkeypatch.setenv("CI_PARITY_ALLOW_NON_TEST_DB", "true")
    dsn = "postgresql://u:p@some-rds-endpoint.amazonaws.com:5432/zerogexdb"
    with pytest.raises(SystemExit):
        _assert_dsn_is_safe_to_truncate(dsn)


def test_guard_handles_default_dbname_field(monkeypatch):
    """A DSN with no explicit dbname (uses libpq default) parses to
    empty/missing dbname.  Empty dbname does not contain ``test`` so
    the guard refuses (correct: default-named DBs are not safe to
    truncate).  Pins this branch so refactors don't accidentally treat
    missing-dbname as 'safe by default'."""
    monkeypatch.delenv("CI_PARITY_ALLOW_NON_TEST_DB", raising=False)
    dsn = "postgresql://u:p@localhost:5432/"
    with pytest.raises(SystemExit):
        _assert_dsn_is_safe_to_truncate(dsn)
