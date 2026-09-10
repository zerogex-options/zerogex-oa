"""Re-export of the production labelling primitives.

The labelling moved to :mod:`src.analytics.wall_breaks` when the product began
publishing wall-break statistics. It lives there rather than here so the
shipped number and the exploratory study cannot drift apart: a change to what
counts as a "break" changes both, or neither.

This module stays as the study's import surface — every existing caller and
test keeps working, and the study still reads as a self-contained package.
"""

from __future__ import annotations

from src.analytics.wall_breaks import (
    ET,
    SESSION_END,
    SESSION_MINUTES,
    SESSION_START,
    EventConfig,
    PriceBar,
    StepSeries,
    WallFrame,
    WallTest,
    extract_wall_tests,
)

__all__ = [
    "ET",
    "SESSION_START",
    "SESSION_END",
    "SESSION_MINUTES",
    "EventConfig",
    "PriceBar",
    "WallFrame",
    "WallTest",
    "StepSeries",
    "extract_wall_tests",
]
