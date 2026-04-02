"""Compatibility shim for moved PositionOptimizerEngine.

The canonical implementation now lives in src.signals.position_optimizer_engine
as part of the standalone Signal Engine service.
"""

from src.signals.position_optimizer_engine import *  # noqa: F401,F403
