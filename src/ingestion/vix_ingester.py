"""
VIX Ingester (thin wrapper)

Spawn entry point for the ``$VIX.X`` streaming ingester.  All streaming,
parsing, upsert, retention, and reconnect logic lives in
:mod:`src.ingestion.volatility_index_ingester`; this module only supplies
VIX-specific configuration (symbol, table name, env-var-driven knobs) so
that adding a new volatility index (e.g. VXN) is a parallel thin wrapper
rather than a copy of the streaming loop.

Tunable env vars (kept VIX-prefixed for backward compat with operators
who already set them):
- ``VIX_INITIAL_BARSBACK`` — bars requested on first connect (default 160,
  ≈ 2 trading sessions of 5-min bars).
- ``VIX_POLL_BARSBACK`` — bars requested on each reconnect (default 3,
  just enough to replay short outages).
- ``VIX_BARS_RETENTION_DAYS`` — rows older than this are pruned (default 7).
"""

from __future__ import annotations

from src.config import _getenv_int
from src.ingestion.volatility_index_ingester import run_ingester

VIX_SYMBOL = "$VIX.X"
VIX_TABLE = "vix_bars"


def main() -> None:
    """Entry point used when spawned as a child process from main_engine."""
    run_ingester(
        ticker="VIX",
        symbol=VIX_SYMBOL,
        table_name=VIX_TABLE,
        initial_barsback=_getenv_int("VIX_INITIAL_BARSBACK", 160),
        poll_barsback=_getenv_int("VIX_POLL_BARSBACK", 3),
        retention_days=_getenv_int("VIX_BARS_RETENTION_DAYS", 7),
    )


if __name__ == "__main__":
    main()
