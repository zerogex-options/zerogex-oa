"""Customer-facing backtesting platform.

Productizes the single-pattern CLI harness in
``src/signals/playbook/backtest.py`` into a reusable engine that prices real
option legs at the quoted spread and an async run lifecycle persisted to the
``backtest_runs`` / ``backtest_trades`` / ``backtest_equity`` tables.

See ``docs/design/backtesting-platform.md`` for the full design.
"""

from src.backtesting.models import (
    BacktestSpec,
    FillModel,
    Sizing,
    ExitRules,
    TradeResult,
    RunResult,
)

__all__ = [
    "BacktestSpec",
    "FillModel",
    "Sizing",
    "ExitRules",
    "TradeResult",
    "RunResult",
]
