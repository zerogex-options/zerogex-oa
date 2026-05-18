"""Regression: MAX_PAIN_BACKGROUND_REFRESH_ENABLED tolerates 1/yes/on.

The flag was hand-parsed as ``os.getenv(...).lower() == "true"`` in two
independent places (src/config.py — the background-loop gate — and
DatabaseManager.__init__ — the request-path skip_inline_refresh gate).
Any value that isn't the exact string ``true`` (e.g. ``1``, ``yes``,
``on``) silently disabled background refresh, forcing every
/api/max-pain/current onto the heavy inline recompute.  Both now use the
shared, tolerant src.config._getenv_bool, so they can never diverge and
accept the common boolean spellings.

Note: this does NOT change behaviour for an explicit ``false`` — that
still disables the loop by design; re-enabling it is an env decision.

Pure/hermetic: monkeypatches the env, constructs no DB connection.
"""

from __future__ import annotations

import pytest

from src.api.database import DatabaseManager
from src.config import _getenv_bool

_ENV = "MAX_PAIN_BACKGROUND_REFRESH_ENABLED"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1", True),
        ("yes", True),
        ("on", True),
        ("true", True),
        ("TRUE", True),
        ("  True  ", True),
        ("0", False),
        ("no", False),
        ("off", False),
        ("false", False),
        ("", False),
    ],
)
def test_getenv_bool_accepts_common_spellings(monkeypatch, raw, expected):
    monkeypatch.setenv(_ENV, raw)
    assert _getenv_bool(_ENV, True) is expected


def test_getenv_bool_unset_and_garbage_fall_back_to_default(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert _getenv_bool(_ENV, True) is True
    assert _getenv_bool(_ENV, False) is False
    monkeypatch.setenv(_ENV, "maybe")
    assert _getenv_bool(_ENV, True) is True


def test_database_manager_skip_gate_honors_tolerant_value(monkeypatch):
    """The exact prod scenario: a value the old ``== 'true'`` check rejected
    must now enable the request-path skip so SPY does NOT take the heavy
    inline recompute; an explicit 'false' still disables it."""
    monkeypatch.setenv(_ENV, "1")
    assert DatabaseManager()._max_pain_background_refresh_enabled is True

    monkeypatch.setenv(_ENV, "false")
    assert DatabaseManager()._max_pain_background_refresh_enabled is False
