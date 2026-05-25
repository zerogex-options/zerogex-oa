"""Regression: the ``prev_net_gex`` lookup in ``_fetch_market_context``
must be bounded in time.

Without a lower-bound clause, the first cycle of a Tuesday morning after a
long weekend (Memorial Day, etc.) compares the fresh Tuesday-open
``total_net_gex`` against Friday's 16:00 ET row -- a ~65-hour gap -- and
the resulting ``net_gex_delta_pct`` is a structurally-spurious shock
value.  Downstream, ``trap_detection``'s ``strengthening_factor`` and
``gex_boost`` amplify it, so the trap signal can fire abnormally hot on
the very first ticks after open.

The fix: add ``AND timestamp >= %s - INTERVAL '30 minutes'`` to the SQL
so a stale row is rejected (``prev_net_gex`` stays None and
``net_gex_delta_pct`` defaults to 0.0 -- the correct behavior when no
recent reference is available).
"""

from __future__ import annotations

from pathlib import Path

_ENGINE_PATH = (
    Path(__file__).resolve().parent.parent / "src" / "signals" / "unified_signal_engine.py"
)


def test_prev_net_gex_sql_has_lower_bound_clause():
    """The prev_net_gex SELECT must filter both upper and lower timestamp
    bounds. A regression that removed the lower bound would let a 65h-stale
    Friday close compare against Tuesday-open and feed spurious shock into
    trap_detection.
    """
    src = _ENGINE_PATH.read_text()

    # The fix puts these three pieces together in one SELECT block.
    # We don't pin exact whitespace -- just structural correctness.
    assert "FROM gex_summary" in src
    assert "AND timestamp < %s" in src
    assert "AND timestamp >= %s - INTERVAL '30 minutes'" in src, (
        "The prev_net_gex lookup must reject rows older than 30 minutes. "
        "Without the lower bound, the first cycle after a long weekend "
        "compares Tuesday-open against Friday-16:00 net_gex and produces "
        "a spurious net_gex_delta_pct shock that trap_detection amplifies."
    )


def test_net_gex_delta_pct_defaults_to_zero_when_prev_is_none():
    """Sanity: confirm the downstream math safely handles the None case.
    The bounded-window SQL returns no row across the holiday gap;
    ``prev_net_gex = None`` should map to ``net_gex_delta_pct = 0.0``.
    """
    src = _ENGINE_PATH.read_text()

    # The "is not None" check + the "else 0.0" assignment must exist
    # so trap_detection sees a neutral signal on the first post-holiday cycle.
    assert "if prev_net_gex is not None:" in src
    assert "net_gex_delta_pct = 0.0" in src
