"""P(break | tested) — how often a tested gamma wall actually gives way.

Research-only. **Changes no production behaviour**: it imports from ``src``,
``src`` imports nothing from here, every database statement is a ``SELECT``,
and outputs go to files.

The question, stated precisely
------------------------------
Given that price has come to the call wall (or the put wall), what is the
probability it breaks through and stays through — and which observable inputs
move that probability?

This is deliberately **not** ``P(the wall gets tested)``.  Production already
models that with the reflection principle in
``src/jobs/forecast_range_model.py``: ``P = 2·(1 − Φ(d/σ))``, tilted by the
dealer regime.  Once the test has happened, distance is ~0 by construction and
carries nothing; what remains is whether the wall is being consumed, whether
the tape is pushing, and how much of the session is left.

See ``docs/design/wall-break-odds.md`` for the methodology and
``research/wall_break_odds/README.md`` for how to run it.
"""

from research.wall_break_odds.events import EventConfig, WallTest, extract_wall_tests
from research.wall_break_odds.features import FEATURE_NAMES, build_features
from research.wall_break_odds.model import Row, base_rate, evaluate, univariate_screen

__all__ = [
    "EventConfig",
    "WallTest",
    "extract_wall_tests",
    "FEATURE_NAMES",
    "build_features",
    "Row",
    "base_rate",
    "evaluate",
    "univariate_screen",
]
