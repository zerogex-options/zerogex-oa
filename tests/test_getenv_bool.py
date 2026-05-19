"""src.config._getenv_bool: tolerant boolean env parsing.

Shared helper behind six ``SIGNALS_*_ENABLED`` flags (and historically the
max-pain refresh gate).  A regression to a strict ``== "true"`` parse would
silently disable any flag whose operator value is ``1``/``yes``/``on``, so
pin the accepted spellings and the garbage-falls-back-to-default contract.
"""

from __future__ import annotations

import pytest

from src.config import _getenv_bool

_ENV = "ZGX_TEST_BOOL_FLAG"


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
def test_accepts_common_spellings(monkeypatch, raw, expected):
    monkeypatch.setenv(_ENV, raw)
    assert _getenv_bool(_ENV, True) is expected
    # Default is irrelevant once the value is recognized.
    assert _getenv_bool(_ENV, False) is expected


def test_unset_and_garbage_fall_back_to_default(monkeypatch):
    monkeypatch.delenv(_ENV, raising=False)
    assert _getenv_bool(_ENV, True) is True
    assert _getenv_bool(_ENV, False) is False

    monkeypatch.setenv(_ENV, "maybe")
    assert _getenv_bool(_ENV, True) is True
    assert _getenv_bool(_ENV, False) is False
