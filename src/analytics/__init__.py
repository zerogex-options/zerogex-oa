"""Analytics package.

Avoid eager import of `main_engine` so `python -m src.analytics.main_engine`
does not trigger runpy warnings.
"""

__all__ = ["AnalyticsEngine", "SignalEngine"]


def __getattr__(name):
    """Lazy-load analytics components."""
    if name == "AnalyticsEngine":
        from src.analytics.main_engine import AnalyticsEngine
        return AnalyticsEngine
    if name == "SignalEngine":
        from src.analytics.signal_engine import SignalEngine
        return SignalEngine

    raise AttributeError(f"module 'src.analytics' has no attribute '{name}'")
