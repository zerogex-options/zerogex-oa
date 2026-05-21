"""Regression: numeric env-var helpers must tolerate inline ``# comment``
tails preserved by python-dotenv.

Failure mode in production (logs from May 21):

    File ".../analytics/main_engine.py", line 98, in __init__
        1.0 / 12.0, float(os.getenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "2"))
    ValueError: could not convert string to float:
        '1                         # was 2; halves the cold I/O'

python-dotenv preserves everything after ``=`` literally, including
inline ``# annotations``.  Any code path that does a raw
``int(os.getenv(...))`` / ``float(os.getenv(...))`` on a numeric env
var will crash the service at startup if the operator put an inline
comment in .env.  The fix routes these reads through
``src.config._getenv_int`` / ``_getenv_float`` / ``_getenv_bool``,
which strip the inline tail before parsing.  This test pins that
contract so a refactor can't silently regress it.
"""

from __future__ import annotations

import pytest

from src.config import _getenv_bool, _getenv_float, _getenv_int, _strip_env_value

_ENV = "ZGX_TEST_INLINE_COMMENT_TOLERANCE"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1", "1"),
        ("1  ", "1"),
        ("  1  ", "1"),
        ("1 # was 2", "1"),
        ("1                         # was 2; halves the cold I/O", "1"),
        ("90000  # default 90s", "90000"),
        ("true  #flag", "true"),
        ("#all-comment", ""),
        ("", ""),
        ("   ", ""),
    ],
)
def test_strip_env_value_handles_inline_comments(raw, expected):
    assert _strip_env_value(raw) == expected


def test_strip_env_value_none_passes_through():
    assert _strip_env_value(None) is None


def test_getenv_float_tolerates_inline_comment(monkeypatch):
    monkeypatch.setenv(_ENV, "0.5  # 30 min")
    assert _getenv_float(_ENV, 2.0) == 0.5


def test_getenv_float_repro_of_production_crash(monkeypatch):
    """Exact reproduction of the May 21 startup crash -- the value the
    operator's .env file would have produced before the fix landed."""
    monkeypatch.setenv(_ENV, "1                         # was 2; halves the cold I/O")
    # Before the fix: float() raises ValueError and the analytics workers
    # crash on startup with a confusing message.  After the fix: parses
    # cleanly to 1.0.
    assert _getenv_float(_ENV, 2.0) == 1.0


def test_getenv_int_tolerates_inline_comment(monkeypatch):
    monkeypatch.setenv(_ENV, "300000  # 5 min, was 180s")
    assert _getenv_int(_ENV, 180000) == 300000


def test_getenv_int_fallback_on_unparseable_tail(monkeypatch):
    """If the cleaned value is still not a valid int (e.g. operator left
    a unit suffix like ``5m`` after stripping comments), fall back to
    default + log an error rather than crashing."""
    monkeypatch.setenv(_ENV, "5m  # five minutes")
    assert _getenv_int(_ENV, 180000) == 180000


def test_getenv_bool_tolerates_inline_comment(monkeypatch):
    monkeypatch.setenv(_ENV, "true  # off-hours mode")
    assert _getenv_bool(_ENV, False) is True
    monkeypatch.setenv(_ENV, "false  # disable")
    assert _getenv_bool(_ENV, True) is False


def test_getenv_numeric_helpers_handle_all_comment_value(monkeypatch):
    """A line that is entirely a comment (after =) cleans to empty and
    the numeric helpers fall back to the default rather than crashing
    on int('')/float('').  The bool helper preserves its pre-existing
    ``empty -> False`` contract (see
    ``test_getenv_bool.py::test_accepts_common_spellings``) and is
    intentionally NOT asserted here."""
    monkeypatch.setenv(_ENV, "  # accidentally commented out")
    assert _getenv_int(_ENV, 42) == 42
    assert _getenv_float(_ENV, 3.14) == 3.14
