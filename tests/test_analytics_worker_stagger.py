"""Tests for the analytics-engine multi-worker startup stagger.

Multi-symbol deployments fork one ``Process`` per symbol; without an
offset they all enter ``_get_snapshot`` at the same wall-clock instant
on every cycle and crowd the buffer pool, producing the ``DataFileRead``
contention pattern seen in production.  ``_compute_worker_stagger``
resolves the per-worker delay so cycles are spread evenly across the
configured interval.
"""

import os
from unittest.mock import patch

from src.analytics.main_engine import _compute_worker_stagger


def _clear_env():
    """Helper: drop the stagger env var so tests see a clean default."""
    return patch.dict(os.environ, {}, clear=False)


def test_single_worker_never_staggers():
    """A 1-symbol deployment has no concurrency to spread."""
    with _clear_env():
        os.environ.pop("ANALYTICS_WORKER_STAGGER_SECONDS", None)
        assert _compute_worker_stagger(60, 1) == 0.0


def test_zero_workers_never_staggers():
    """Defensive: ``num_workers == 0`` must not divide by zero."""
    with _clear_env():
        os.environ.pop("ANALYTICS_WORKER_STAGGER_SECONDS", None)
        assert _compute_worker_stagger(60, 0) == 0.0


def test_auto_mode_spreads_workers_evenly():
    """Default mode: each worker waits ``i * (interval / N)`` seconds."""
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "auto"}):
        # 60s interval, 3 workers -> 20s between workers
        assert _compute_worker_stagger(60, 3) == 20.0
        # 90s interval, 4 workers -> 22.5s between workers
        assert _compute_worker_stagger(90, 4) == 22.5


def test_unset_env_defaults_to_auto():
    """Missing env var = auto behavior, no surprise zero-delay regression."""
    with _clear_env():
        os.environ.pop("ANALYTICS_WORKER_STAGGER_SECONDS", None)
        assert _compute_worker_stagger(60, 3) == 20.0


def test_explicit_zero_disables_stagger():
    """Operators can opt out without code changes."""
    for raw in ("0", "off", "false", "disabled", ""):
        with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": raw}):
            assert _compute_worker_stagger(60, 3) == 0.0, f"raw={raw!r}"


def test_explicit_positive_value_is_used_verbatim():
    """A numeric override wins over the auto computation."""
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "5"}):
        assert _compute_worker_stagger(60, 3) == 5.0
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "12.5"}):
        assert _compute_worker_stagger(60, 3) == 12.5


def test_invalid_value_falls_back_to_auto():
    """Garbage input must not crash startup -- log + fall back to auto."""
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "garbage"}):
        assert _compute_worker_stagger(60, 3) == 20.0


def test_negative_value_clamps_to_zero():
    """Negative numbers clamp to 0 rather than scheduling work in the past."""
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "-5"}):
        assert _compute_worker_stagger(60, 3) == 0.0


def test_inline_comment_in_env_value_is_tolerated():
    """python-dotenv preserves trailing ``# comments`` verbatim; the helper
    strips them rather than failing the parse (parity with the
    `_getenv_int`/`_getenv_float` helpers used elsewhere in the engine)."""
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "auto # spread evenly"}):
        assert _compute_worker_stagger(60, 3) == 20.0
    with patch.dict(os.environ, {"ANALYTICS_WORKER_STAGGER_SECONDS": "10 # explicit"}):
        assert _compute_worker_stagger(60, 3) == 10.0
