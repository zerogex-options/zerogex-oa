"""First-class CME equity-index futures support for ZeroGEX.

This package builds the *foundations* for treating ES / NQ as a distinct
asset class — dated contracts, security definitions, contract-roll logic,
Black-76 option Greeks, contract-aware GEX, an interim SPX-derived reference
mode, and vendor-neutral market-data ingestion interfaces — layered
additively on top of the existing (display-only) futures plumbing without
disturbing the cash-index/ETF analytics that ship today.

Nothing in this package fabricates CME market data.  The provider protocols
carry mock/fixture implementations for tests and development; the real,
licensed CME adapter is intentionally left pending (see
``docs/futures-support-architecture.md``).  Analytics computed here always
carry explicit source + methodology metadata so a cash-index projection is
never mistaken for genuine CME futures-options GEX.
"""

from __future__ import annotations

__all__ = [
    "contracts",
    "black76",
    "calendar",
    "resolver",
    "security_definitions",
    "providers",
    "gex",
    "reference_mode",
]
