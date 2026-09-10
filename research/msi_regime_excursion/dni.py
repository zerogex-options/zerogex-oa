"""Is ``dealer_delta_pressure`` mis-scaled, and what should its normalizer be?

``cli components`` showed this component feeding the NDX composite on only 60%
of readings, flat across every hour of the session. It abstains when its score
lands within ``_ABSTAIN_THRESHOLD`` (1e-3) of zero, and since the score is
``-clamp(dni / _DNI_NORM, -1, 1)`` with ``_DNI_NORM = 3.0e8``, that means it
abstains whenever::

    |dealer net delta| < 300,000 shares-equivalent

``_DNI_NORM`` is a module constant read from one env var. It is the only MSI
component without a per-symbol calibration path -- ``net_gex_sign``,
``put_call_ratio_state`` and ``local_gamma`` all consult
``ctx.extra["normalizers"]`` first. So every symbol is measured against a
threshold sized for one of them.

That is a hypothesis about scale, and it is checkable, because the component
persists its own raw estimate: ``context_values`` writes
``dealer_net_delta_estimated`` and ``source`` into the components payload every
cycle. This reads them back and reports, per symbol, how the estimate is
distributed against the 300,000 floor -- and what a per-symbol normalizer would
have to be to put the component on the same footing everywhere.

    python -m research.msi_regime_excursion.cli dni --symbols SPY,SPX,QQQ,NDX

Read the ``% below floor`` column: that is how often the component is silently
dropping out. The suggested normalizer is the p95 of |dni|, matching how
``normalizer_cache_refresh`` calibrates the other components.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

__all__ = ["render_dni"]

#: ``_ABSTAIN_THRESHOLD`` in components/spectrum.py.
ABSTAIN_THRESHOLD = 1e-3
#: ``_DNI_NORM`` in basic/dealer_delta_pressure.py.
DNI_NORM = 3.0e8
#: |dni| below this abstains under the shipped constant.
ABSTAIN_FLOOR = DNI_NORM * ABSTAIN_THRESHOLD


def _as_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _quantile(sorted_vals: list[float], q: float) -> Optional[float]:
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def _load(conn, symbol: str, days: int) -> tuple[list[float], dict[str, int], int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT components
        FROM signal_scores
        WHERE underlying = %s AND timestamp >= %s
        """,
        (symbol.upper(), datetime.now(timezone.utc) - timedelta(days=days)),
    )
    values: list[float] = []
    sources: dict[str, int] = {}
    missing = 0
    for (payload,) in cur.fetchall():
        entry = _as_dict(_as_dict(payload).get("dealer_delta_pressure"))
        context = _as_dict(entry.get("context"))
        source = context.get("source") or "unknown"
        sources[source] = sources.get(source, 0) + 1
        raw = context.get("dealer_net_delta_estimated")
        if raw is None:
            missing += 1
            continue
        try:
            values.append(abs(float(raw)))
        except (TypeError, ValueError):
            missing += 1
    return values, sources, missing


def render_dni(per_symbol: dict[str, tuple[list[float], dict[str, int], int]]) -> str:
    out: list[str] = []
    out.append("=" * 96)
    out.append("DEALER_DELTA_PRESSURE — is its normalizer sized for every symbol?")
    out.append("=" * 96)
    out.append("")
    out.append(f"The shipped constant abstains below |dni| = {ABSTAIN_FLOOR:,.0f} "
               f"(_DNI_NORM {DNI_NORM:,.0f} x threshold {ABSTAIN_THRESHOLD}).")
    out.append("")
    header = (f"{'symbol':<7} {'n':>8} {'median |dni|':>14} {'p95 |dni|':>14} "
              f"{'% below floor':>14} {'suggested norm':>15}")
    out.append(header)
    out.append("-" * len(header))
    for symbol, (values, _sources, _missing) in per_symbol.items():
        if not values:
            out.append(f"{symbol:<7} {0:>8} {'—':>14} {'—':>14} {'—':>14} {'—':>15}")
            continue
        vals = sorted(values)
        med = _quantile(vals, 0.5) or 0.0
        p95 = _quantile(vals, 0.95) or 0.0
        below = sum(1 for v in vals if v < ABSTAIN_FLOOR)
        # Match normalizer_cache_refresh: p95 of |value| is the saturation level.
        out.append(
            f"{symbol:<7} {len(vals):>8,} {med:>14,.0f} {p95:>14,.0f} "
            f"{100.0 * below / len(vals):>13.1f}% {p95:>15,.0f}"
        )

    out.append("")
    out.append("ESTIMATION PATH USED")
    for symbol, (_values, sources, missing) in per_symbol.items():
        total = sum(sources.values()) or 1
        parts = ", ".join(
            f"{k} {100.0 * v / total:.0f}%" for k, v in sorted(sources.items(), key=lambda t: -t[1])
        )
        tail = f"   (no estimate on {missing:,})" if missing else ""
        out.append(f"  {symbol:<7} {parts}{tail}")

    out.append("")
    out.append("READ IT LIKE THIS:")
    out.append("  A symbol whose |dni| sits an order of magnitude below the others is not")
    out.append("  quiet — it is being measured against a floor sized for a different")
    out.append("  instrument. Index options carry far more notional per contract, so their")
    out.append("  open interest, and any share-equivalent delta derived from it, is")
    out.append("  structurally smaller. The fix is a per-symbol normalizer, which is what")
    out.append("  net_gex_sign, put_call_ratio and local_gamma already do via")
    out.append("  ctx.extra['normalizers']; dealer_delta_pressure is the one that does not.")
    return "\n".join(out)


def main(symbols: str, days: int) -> int:
    try:
        from src.database import db_connection
    except Exception as exc:  # pragma: no cover
        raise SystemExit(f"cannot import the database layer ({exc}).")
    keys = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    per_symbol: dict[str, tuple[list[float], dict[str, int], int]] = {}
    with db_connection() as conn:
        for key in keys:
            per_symbol[key] = _load(conn, key, days)
    print(render_dni(per_symbol))
    return 0
