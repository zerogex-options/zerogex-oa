"""Where do the composite's components abstain, and does it matter?

A row "reconstructs" when rebuilding the composite from its persisted component
scores reproduces the persisted ``composite_score``. It fails when components
**abstained** -- the engine built the composite from a partial set and
renormalized onto the full 100-point scale (``ScoringEngine.score``).

The first study run found reconstruction splitting the instruments cleanly in
two: SPX and ES near 98%, SPY / QQQ / NDX / NQ near 55%. Those are exactly the
four whose correlation with forward excursion fails to survive multiplicity
correction, so the two facts are probably one fact.

This breaks the rate down by hour of the ET trading day, which is the cheapest
way to test the leading hypothesis: that the degraded readings are the ones
scored outside the cash session, where the options tape is too thin to feed the
components. Reads the extract's JSONL. No database, no bootstrap, seconds to run.

    python -m research.msi_regime_excursion.cli abstention research_output/msi_excursion.jsonl
"""

from __future__ import annotations

import json
from datetime import datetime

from research.msi_regime_excursion.excursion import ET
from research.msi_regime_excursion.study import RECONSTRUCTION_TOLERANCE

__all__ = ["render_abstention"]

#: The cash session, ET. Rows outside it are the hypothesis under test.
RTH_START_HOUR = 9
RTH_END_HOUR = 16


def _load(path: str) -> dict[str, dict[int, list[int]]]:
    """``{instrument: {et_hour: [n_ok, n_total]}}``."""
    out: dict[str, dict[int, list[int]]] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            inst = d.get("instrument")
            ts = d.get("timestamp")
            if not inst or not ts:
                continue
            err = d.get("reconstruction_error")
            try:
                hour = datetime.fromisoformat(ts).astimezone(ET).hour
            except (TypeError, ValueError):
                continue
            cell = out.setdefault(inst, {}).setdefault(hour, [0, 0])
            cell[1] += 1
            if err is not None and err <= RECONSTRUCTION_TOLERANCE:
                cell[0] += 1
    return out


def render_abstention(path: str) -> str:
    data = _load(path)
    if not data:
        return f"{path}: no rows."

    instruments = sorted(data)
    hours = sorted({h for inst in data.values() for h in inst})
    out: list[str] = []
    out.append("=" * 78)
    out.append("COMPONENT ABSTENTION — % of readings that reconstruct, by ET hour")
    out.append("=" * 78)
    out.append("")
    out.append("A low rate means the composite was built from a PARTIAL component set")
    out.append("and renormalized. Those readings still get a regime label, still reach")
    out.append("customers, and still gate the playbook patterns.")
    out.append("")
    out.append(f"{'ET hour':>8} " + " ".join(f"{i:>8}" for i in instruments) + "   session")
    out.append("-" * (9 + 9 * len(instruments) + 10))
    for h in hours:
        cells = []
        for inst in instruments:
            ok, total = data[inst].get(h, [0, 0])
            cells.append(f"{100.0 * ok / total:7.1f}%" if total else f"{'—':>8}")
        rth = "RTH" if RTH_START_HOUR <= h < RTH_END_HOUR else "extended"
        out.append(f"{h:>6}:00 " + " ".join(cells) + f"   {rth}")

    # Totals, split by session.
    out.append("")
    out.append(f"{'':>8} " + " ".join(f"{i:>8}" for i in instruments))
    for label, keep in (
        ("ALL", lambda h: True),
        ("RTH only", lambda h: RTH_START_HOUR <= h < RTH_END_HOUR),
        ("extended", lambda h: not (RTH_START_HOUR <= h < RTH_END_HOUR)),
    ):
        cells = []
        for inst in instruments:
            ok = sum(v[0] for h, v in data[inst].items() if keep(h))
            total = sum(v[1] for h, v in data[inst].items() if keep(h))
            cells.append(f"{100.0 * ok / total:7.1f}%" if total else f"{'—':>8}")
        out.append(f"{label:>8} " + " ".join(cells))

    out.append("")
    out.append(f"{'':>8} " + " ".join(f"{i:>8}" for i in instruments))
    cells = []
    for inst in instruments:
        ext = sum(v[1] for h, v in data[inst].items()
                  if not (RTH_START_HOUR <= h < RTH_END_HOUR))
        total = sum(v[1] for v in data[inst].values())
        cells.append(f"{100.0 * ext / total:7.1f}%" if total else f"{'—':>8}")
    out.append("% rows  " + " ".join(cells) + "   <- share of readings outside RTH")

    out.append("")
    out.append("READ IT LIKE THIS:")
    out.append("  If the low rates sit in the extended-hours rows, the fix is a session")
    out.append("  gate on scoring, not a model change — and the four weak instruments")
    out.append("  may simply be diluted by readings taken on a tape too thin to score.")
    out.append("  If the rate is low INSIDE RTH too, something else is starving the")
    out.append("  components and that is the bug to chase.")
    return "\n".join(out)


def main(path: str) -> int:
    print(render_abstention(path))
    return 0
