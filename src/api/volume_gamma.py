"""Per-strike dealer gamma weighted by today's traded volume.

The gamma ladders weight each contract's gamma by its open interest:
``γ × OI × 100 × S² × 0.01`` per 1% move, calls positive and puts negative.
Open interest is published once a day, so that ladder cannot see positions
opened during the session. This is the same per-strike gamma weighted by the
contracts traded today instead: ``γ × volume × 100 × S² × 0.01``, with the
same sign convention, so the two ladders read on one scale.

``gex_by_strike`` does not store per-contract gamma. It stores, per
(strike, expiration), the open-interest-weighted sum ``Σ γ × OI`` for each
side, the side's open interest and its volume. The chain is ingested with one
contract per (strike, expiration, side), so ``call_gamma / call_oi`` is that
contract's gamma exactly, and the volume-weighted figure is
``call_gamma × call_volume / call_oi``. Two consequences:

* A side with no open interest has no recoverable gamma and adds nothing to
  the dollar figures. Its traded contracts still count in the volumes.
* Where two contracts share a (strike, expiration, side), an AM monthly beside
  a PM weekly, the ratio is their open-interest-weighted average gamma.

Volume cannot tell an opening trade from a closing one, or who was on each
side. This is a map of where today's gamma is trading, not of positioning.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional


def _as_float(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return out if out == out else 0.0  # NaN -> 0


def _as_count(value: Any) -> int:
    return max(0, int(_as_float(value)))


def volume_weighted_strike_gamma(
    rows: Iterable[Mapping[str, Any]], spot: Optional[float]
) -> List[Dict[str, Any]]:
    """Sum (strike, expiration) rows into one volume-weighted row per strike.

    ``rows`` carry ``strike``, ``call_gamma``, ``put_gamma`` (the stored
    ``Σ γ × OI`` sums, both positive), ``call_oi``, ``put_oi``,
    ``call_volume`` and ``put_volume``. Returns rows ascending by strike with
    ``call_gex`` (+), ``put_gex`` (−) and ``net_gex`` in dollars per 1% move
    at ``spot``, plus the strike's traded ``call_volume`` / ``put_volume``.
    Without a usable spot there is nothing to dollarize, so the result is
    empty.
    """
    if spot is None or not spot > 0:
        return []
    scale = 100.0 * spot * spot * 0.01

    acc: Dict[float, List[float]] = {}
    for row in rows:
        strike = _as_float(row.get("strike"))
        if strike <= 0:
            continue
        call_oi = _as_count(row.get("call_oi"))
        put_oi = _as_count(row.get("put_oi"))
        call_volume = _as_count(row.get("call_volume"))
        put_volume = _as_count(row.get("put_volume"))
        call_gamma = _as_float(row.get("call_gamma")) / call_oi if call_oi > 0 else 0.0
        put_gamma = _as_float(row.get("put_gamma")) / put_oi if put_oi > 0 else 0.0

        bucket = acc.setdefault(strike, [0.0, 0.0, 0, 0])
        bucket[0] += call_gamma * call_volume
        bucket[1] += put_gamma * put_volume
        bucket[2] += call_volume
        bucket[3] += put_volume

    out: List[Dict[str, Any]] = []
    for strike in sorted(acc):
        call_sum, put_sum, call_volume, put_volume = acc[strike]
        call_gex = call_sum * scale
        put_gex = -put_sum * scale
        out.append(
            {
                "strike": strike,
                "call_gex": call_gex,
                "put_gex": put_gex,
                "net_gex": call_gex + put_gex,
                "call_volume": int(call_volume),
                "put_volume": int(put_volume),
            }
        )
    return out
