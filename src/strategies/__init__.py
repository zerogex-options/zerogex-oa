"""The consolidated ZeroGEX strategy catalog.

One catalog, three consumers:

* ``src/tradeworkz/registry.py`` resolves bot classes and per-bot tuning.
* ``src/backtesting/meta.py`` publishes the testable universe.
* ``src/backtesting/queries.py`` folds measured stats onto catalog ids.

Import from here rather than reaching into the submodules::

    from src.strategies import all_strategies, get, Stage

See ``docs/design/strategy-catalog.md``.
"""

from src.strategies.catalog import (
    STRATEGIES,
    all_strategies,
    bot_bound,
    by_family,
    by_stage,
    canonical_id,
    find,
    get,
    iter_aliases,
    pattern_bound,
    provisionable,
)
from src.strategies.models import (
    BOT_HARNESSES,
    CONCLUSIVE_AGAINST,
    FAMILY_LABELS,
    Engine,
    Family,
    ResearchRun,
    Retirement,
    Stage,
    StrategyEntry,
    Verdict,
)
from src.strategies.policy import (
    PROMOTION_MIN_PROFIT_FACTOR,
    PROMOTION_MIN_TRADES,
    RETIREMENT_MIN_HISTORY_DAYS,
    RETIREMENT_MIN_TRADES,
    RETIREMENT_MIN_TUNING_GENERATIONS,
    Eligibility,
    can_promote,
    can_retire,
    retirement_shortfall,
    validate_stage,
)

__all__ = [
    "STRATEGIES",
    "all_strategies",
    "bot_bound",
    "by_family",
    "by_stage",
    "canonical_id",
    "find",
    "get",
    "iter_aliases",
    "pattern_bound",
    "provisionable",
    "BOT_HARNESSES",
    "CONCLUSIVE_AGAINST",
    "FAMILY_LABELS",
    "Engine",
    "Family",
    "ResearchRun",
    "Retirement",
    "Stage",
    "StrategyEntry",
    "Verdict",
    "PROMOTION_MIN_PROFIT_FACTOR",
    "PROMOTION_MIN_TRADES",
    "RETIREMENT_MIN_HISTORY_DAYS",
    "RETIREMENT_MIN_TRADES",
    "RETIREMENT_MIN_TUNING_GENERATIONS",
    "Eligibility",
    "can_promote",
    "can_retire",
    "retirement_shortfall",
    "validate_stage",
]
