"""
VXN Ingester (thin wrapper)

Spawn entry point for the ``$VXN.X`` streaming ingester — the Nasdaq-100
counterpart to the VIX ingester.  Streaming, parsing, upsert, retention,
and reconnect logic all live in :mod:`src.ingestion.volatility_index_ingester`;
this module only supplies VXN-specific configuration so the VXN process
follows the same spawn → seed → stream → prune lifecycle as VIX.

Tunable env vars:
- ``VXN_INITIAL_BARSBACK`` — bars requested on first connect (default 160,
  ≈ 2 trading sessions of 5-min bars).
- ``VXN_POLL_BARSBACK`` — bars requested on each reconnect (default 3,
  just enough to replay short outages).
- ``VXN_BARS_RETENTION_DAYS`` — rows older than this are pruned (default 7).
"""

from __future__ import annotations

from src.config import _getenv_int
from src.ingestion.volatility_index_ingester import run_ingester

VXN_SYMBOL = "$VXN.X"
VXN_TABLE = "vxn_bars"


def main() -> None:
    """Entry point used when spawned as a child process from main_engine."""
    run_ingester(
        ticker="VXN",
        symbol=VXN_SYMBOL,
        table_name=VXN_TABLE,
        initial_barsback=_getenv_int("VXN_INITIAL_BARSBACK", 160),
        poll_barsback=_getenv_int("VXN_POLL_BARSBACK", 3),
        retention_days=_getenv_int("VXN_BARS_RETENTION_DAYS", 7),
    )


if __name__ == "__main__":
    main()
