"""Analytics package.

Avoid eager import of `main_engine` so `python -m src.analytics.main_engine`
does not trigger runpy warnings.
"""

from src.analytics.walls import compute_call_put_walls

__all__ = ["AnalyticsEngine", "compute_call_put_walls"]


def __getattr__(name):
    """Lazy-load analytics components."""
    if name == "AnalyticsEngine":
        from src.analytics.main_engine import AnalyticsEngine

        return AnalyticsEngine

    raise AttributeError(f"module 'src.analytics' has no attribute '{name}'")
