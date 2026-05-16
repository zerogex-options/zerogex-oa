"""Pytest session bootstrap: pin operator-tunable config to code defaults.

This must run before any test module imports ``src`` (pytest loads
conftest.py before collecting tests, so the module-level assignments
below execute first — in particular before ``src.config`` reads its
module-level ``SIGNALS_*`` constants).

Why this exists: the Makefile does ``-include .env`` then ``export``,
which pushes every key in an operator's ``.env`` into the environment of
``pytest``; ``src/config.py`` additionally calls ``load_dotenv()``. On a
configured machine that lets live tuning (feature flags, thresholds,
multipliers) override the code defaults that unit tests assert against,
so ``make test`` fails on the server while CI (which has no ``.env``)
stays green. Forcing these keys to the code defaults makes the unit
suite hermetic and identical to CI regardless of the local ``.env``.

Each value mirrors the in-code default at the cited source location;
keep them in sync if the defaults change. Tests that exercise non-default
behavior set these per-test (via monkeypatch or instance attributes), so
pinning the process-wide default here does not constrain them.
"""

import os

# key -> (default, source of the in-code default)
_PINNED_DEFAULTS = {
    # src/api/database.py: DatabaseManager.__init__
    "FLOW_SERIES_USE_SNAPSHOT": "false",
    "MAX_PAIN_BACKGROUND_REFRESH_ENABLED": "true",
    "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS": "SPY,SPX,QQQ",
    # src/config.py module-level constants
    "SIGNALS_BREAKOUT_SIZE_MULTIPLIER": "1.50",
    "SIGNALS_TRIGGER_THRESHOLD": "0.50",
}

for _key, _default in _PINNED_DEFAULTS.items():
    os.environ[_key] = _default
