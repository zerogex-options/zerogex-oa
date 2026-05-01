"""Built-in pattern implementations.

Each module in this package exports a single ``PATTERN: PatternBase``
instance.  The PlaybookEngine's discovery walks this directory at
startup and loads every ``PATTERN`` it finds.

PR-2 ships the canonical example: ``gamma_vwap_confluence``.  The full
catalog (12 patterns) lands in PR-3+ as each is implemented and
backtest-validated against the legacy advanced-signal direct-trigger
trades.

See ``docs/playbook_catalog.md`` §7 for the full catalog.
"""
