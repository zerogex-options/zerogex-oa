"""Dealer position-sign policy for futures GEX.

Dealer-sign is the assumption about which way market makers are positioned in
each contract — it flips the sign of every exposure and therefore the entire
GEX regime.  The spec is explicit that the SPY/SPX/QQQ calibration must NOT be
assumed correct for ES/NQ, and that the policy must be **versioned**,
**documented**, **independently configurable for CME products**, and **exposed
in calculation metadata** (spec Phase 7).

This module provides the :class:`DealerPositionSignPolicy` protocol and a
versioned default that mirrors the existing ZeroGEX convention (dealers modeled
net-long calls / net-short puts), so futures GEX starts from the same baseline
the rest of the platform uses while remaining independently swappable.
"""

from __future__ import annotations

from typing import Dict, Protocol, Type, cast, runtime_checkable

from src.futures.contracts import PutCall


@runtime_checkable
class DealerPositionSignPolicy(Protocol):
    """Maps a contract to the dealer's assumed position sign (+1 / -1)."""

    #: Stable identifier surfaced in GEX result metadata.
    version: str

    def sign_for_contract(self, put_call: PutCall) -> int: ...


class LongCallShortPutSignPolicy:
    """Default: dealers modeled net-long calls (+1), net-short puts (-1).

    This is the same modeled convention the equity/index GEX engine uses
    (``src/analytics/main_engine.py``: ``call_gex`` positive, ``put_gex``
    negative).  It is applied to ES/NQ as a *starting* calibration, explicitly
    flagged as such in metadata via :attr:`version` so a future CME-specific
    recalibration is a drop-in replacement, not a silent change.
    """

    version = "v1_calls_long_puts_short"

    def sign_for_contract(self, put_call: PutCall) -> int:
        return 1 if put_call is PutCall.CALL else -1


class InvertedSignPolicy:
    """Dealers net-short calls / net-long puts — the mirror calibration.

    Provided so an operator can flip the whole book's assumed sign for a CME
    product without editing engine code (config selects the policy).
    """

    version = "v1_calls_short_puts_long"

    def sign_for_contract(self, put_call: PutCall) -> int:
        return -1 if put_call is PutCall.CALL else 1


_POLICIES: Dict[str, Type[DealerPositionSignPolicy]] = {
    LongCallShortPutSignPolicy.version: LongCallShortPutSignPolicy,
    InvertedSignPolicy.version: InvertedSignPolicy,
}

DEFAULT_SIGN_POLICY = LongCallShortPutSignPolicy()


def get_sign_policy(version: str | None = None) -> DealerPositionSignPolicy:
    """Resolve a policy by version id, defaulting to the standard one."""
    if not version:
        return DEFAULT_SIGN_POLICY
    cls = _POLICIES.get(version)
    return cast(DealerPositionSignPolicy, cls()) if cls else DEFAULT_SIGN_POLICY
