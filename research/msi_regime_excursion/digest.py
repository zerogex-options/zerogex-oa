"""A one-screen summary of a study run.

The full markdown report is thousands of rows — every instrument, horizon,
measure and bucket — which is the right level of detail to audit a finding and
the wrong level to read one. This prints the part that decides the question,
small enough to paste into an email.

Reads the JSON findings written by ``analyze --json-out``.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from research.msi_regime_excursion.bands import BAND_KEYS
from research.msi_regime_excursion.excursion import REST_OF_SESSION

__all__ = ["render_digest"]

#: The measure the headline is read from, and the one the band copy is about.
HEADLINE = "range_bps"
#: Order variants are shown in, shipped score first, with short column labels.
VARIANT_ORDER: tuple[tuple[str, str], ...] = (
    ("msi", "msi"),
    ("msi_folded", "folded"),
    ("msi_magnitude", "magnitude"),
    ("msi_magnitude_pcr", "mag+pcr"),
    ("msi_direction", "DIRECTION"),
)


def _h(h: Any) -> str:
    return "session" if h == REST_OF_SESSION else f"{h}m"


def _horizons(inst: dict) -> list[Any]:
    seen: list[Any] = []
    for c in inst.get("correlations", []):
        if c["horizon"] not in seen:
            seen.append(c["horizon"])
    return sorted(seen, key=lambda x: (x == REST_OF_SESSION, x if isinstance(x, int) else 0))


def _f(v: Optional[float], places: int = 2, width: int = 0) -> str:
    txt = "—" if v is None else f"{v:.{places}f}"
    return txt.rjust(width) if width else txt


def render_digest(payload: dict) -> str:
    meta = payload.get("meta", {})
    instruments = payload.get("instruments", [])
    out: list[str] = []

    window = ""
    if meta.get("start") and meta.get("end"):
        window = f"{meta['start'][:10]} to {meta['end'][:10]}"
    out.append("=" * 78)
    out.append("MSI EXCURSION STUDY — DIGEST")
    if window:
        out.append(f"window: {window}")
    out.append("=" * 78)

    # ---- sample ----------------------------------------------------------
    out.append("")
    out.append("SAMPLE  (sessions is what significance actually rests on, not rows)")
    out.append(f"{'inst':<5} {'rows':>8} {'sessions':>9} {'reconstructible':>16}")
    for inst in instruments:
        total = inst.get("reconstruction_total") or 0
        ok = inst.get("reconstruction_ok") or 0
        recon = f"{100.0 * ok / total:.1f}%" if total else "—"
        out.append(
            f"{inst['instrument']:<5} {inst['n_rows']:>8,} {inst['n_sessions']:>9} {recon:>16}"
        )

    out.append("")
    out.append("READINGS PER BAND")
    out.append(f"{'inst':<5} {'reversal':>9} {'chop':>9} {'controlled':>11} {'trend':>9}")
    for inst in instruments:
        bc = inst.get("band_counts", {})
        out.append(
            f"{inst['instrument']:<5} " + " ".join(
                f"{bc.get(k, 0):>{w},}" for k, w in zip(BAND_KEYS, (9, 9, 11, 9))
            )
        )

    # ---- headline: does the score order excursion at all? ----------------
    out.append("")
    out.append(f"HEADLINE — Spearman rho, shipped MSI vs {HEADLINE}")
    out.append("  (|rho| < 0.05 means it does not order excursion; * = survives BH)")
    hz_all: list[Any] = []
    for inst in instruments:
        for h in _horizons(inst):
            if h not in hz_all:
                hz_all.append(h)
    hz_all = sorted(hz_all, key=lambda x: (x == REST_OF_SESSION, x if isinstance(x, int) else 0))
    out.append(f"{'inst':<5} " + " ".join(f"{_h(h):>10}" for h in hz_all))
    for inst in instruments:
        cells = []
        for h in hz_all:
            c = next(
                (x for x in inst["correlations"]
                 if x["horizon"] == h and x["measure"] == HEADLINE and x["score"] == "msi"),
                None,
            )
            if c is None or c.get("rho") is None:
                cells.append(f"{'—':>10}")
            else:
                cells.append(f"{c['rho']:>+9.3f}{'*' if c.get('survives_bh') else ' '}")
        out.append(f"{inst['instrument']:<5} " + " ".join(cells))

    # ---- bands vs the base rate -----------------------------------------
    out.append("")
    out.append(f"BANDS vs UNCONDITIONAL BASE RATE — {HEADLINE}, by horizon")
    out.append("  ratio = band mean / base mean.  1.00 means the band tells you nothing.")
    out.append("  d = Cliff's delta; |d| < 0.147 is negligible.  * = survives BH")
    out.append(
        f"{'inst':<5} {'horizon':>8} {'band':<19} {'n':>7} {'ratio':>7} {'d':>7}  "
        f"{'p(block)':>9} {'p(naive)':>9}"
    )
    for inst in instruments:
        for b in inst.get("buckets", []):
            if b["bucket_kind"] != "band" or b["measure"] != HEADLINE:
                continue
            star = "*" if b.get("survives_bh") else " "
            pb = b.get("p_block")
            pn = b.get("p_naive")
            out.append(
                f"{inst['instrument']:<5} {_h(b['horizon']):>8} {b['bucket']:<19} "
                f"{b['n']:>7,} {_f(b.get('ratio'), 3, 7)} {_f(b.get('cliffs_delta'), 3, 6)}{star} "
                f"{_f(pb, 4, 9)} {_f(pn, 4, 9)}"
            )

    # ---- alternative constructions --------------------------------------
    out.append("")
    out.append(f"ALTERNATIVE CONSTRUCTIONS — Spearman rho vs {HEADLINE}")
    out.append("  msi_direction is the NEGATIVE CONTROL: built only from the components")
    out.append("  whose own docstrings call them bullish/bearish. If it matches msi, the")
    out.append("  shipped score's regime content is direction wearing a regime label.")
    out.append(
        f"{'inst':<5} {'horizon':>8} " + " ".join(f"{label:>10}" for _, label in VARIANT_ORDER)
    )
    for inst in instruments:
        for h in _horizons(inst):
            cells = []
            for variant, _label in VARIANT_ORDER:
                c = next(
                    (x for x in inst["correlations"]
                     if x["horizon"] == h and x["measure"] == HEADLINE
                     and x["score"] == variant),
                    None,
                )
                cells.append(f"{'—':>10}" if c is None or c.get("rho") is None
                             else f"{c['rho']:>+10.3f}")
            out.append(f"{inst['instrument']:<5} {_h(h):>8} " + " ".join(cells))

    # ---- point targets ---------------------------------------------------
    targets = [(i, t) for i in instruments for t in i.get("targets", [])]
    if targets:
        out.append("")
        out.append("POINT TARGETS — P(travels at least N points), by band vs base rate")
        out.append("  the question in the units a scalper uses.  * = survives BH")
        out.append(
            f"{'inst':<5} {'horizon':>8} {'pts':>6} {'side':>5} {'band':<19} {'n':>7} "
            f"{'hit':>7} {'base':>7} {'diff':>8}"
        )
        for inst, t in targets:
            star = "*" if t.get("survives_bh") else " "
            rate = t.get("rate")
            base = t.get("rate_base")
            diff = t.get("diff")
            out.append(
                f"{inst['instrument']:<5} {_h(t['horizon']):>8} {t['target_pts']:>6g} "
                f"{t['side']:>5} {t['bucket']:<19} {t['n']:>7,} "
                f"{('—' if rate is None else f'{100*rate:.1f}%'):>7} "
                f"{('—' if base is None else f'{100*base:.1f}%'):>7} "
                f"{('—' if diff is None else f'{100*diff:+.1f}pp'):>8}{star}"
            )

    # ---- multiplicity ----------------------------------------------------
    total = survived = 0
    for inst in instruments:
        for group in ("correlations", "buckets", "targets"):
            for f in inst.get(group, []):
                total += 1
                if f.get("survives_bh"):
                    survived += 1
    out.append("")
    out.append(f"MULTIPLICITY: {survived:,} of {total:,} tests survive Benjamini-Hochberg at 5%.")
    out.append("")
    for inst in instruments:
        for note in inst.get("notes", []):
            out.append(f"NOTE ({inst['instrument']}): {note}")
    return "\n".join(out)


def main(path: str) -> int:
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    print(render_digest(payload))
    return 0
