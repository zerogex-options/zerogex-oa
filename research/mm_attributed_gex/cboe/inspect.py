"""Discover a real file's schema and *propose* a column mapping.

This exists because the experiment was built without a Cboe sample.  Rather
than guess a header and ship code that breaks (or worse, silently
mis-attributes) on first contact with real data, the ingestion layer ships a
proposer: point it at a delivered file and it prints the actual header, the
mapping it infers, and — critically — everything it could NOT map.

The output is a *proposal for human review*, never an automatic decision.
:attr:`HeaderProposal.blocking_problems` is non-empty whenever the proposal is
unusable, and the emitted profile always carries ``confirmed=False``.
"""

from __future__ import annotations

import csv
import gzip
import io
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

from research.mm_attributed_gex.cboe.profiles import (
    ALIAS_CANDIDATES,
    ColumnProfile,
    EFFECT_TOKENS,
    PARTICIPANT_TOKENS,
    SIDE_TOKENS,
    VolumeColumn,
    unmapped_columns,
)
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantType,
    PositionEffect,
    Side,
)

__all__ = ["HeaderProposal", "read_header", "propose_profile", "render_proposal"]

#: Cboe reports CUSTOMER and PROFESSIONAL_CUSTOMER volume split three ways by
#: order size: fewer than 100 contracts, 100-199, and more than 199.  Recognized
#: so a size-split column is captured WITH its bucket rather than dropped —
#: the split carries no weight in the arithmetic (all three sum into the same
#: participant) but losing it silently would hide part of the file.
_SIZE_TOKENS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("lt100", ("lt100", "under100", "less_than_100", "_1_99", "small")),
    ("100to199", ("100to199", "100_199", "mid", "medium")),
    ("gt199", ("gt199", "gte200", "over200", "200plus", "ge200", "large")),
)


@dataclass
class HeaderProposal:
    """What the inspector inferred from a real file."""

    path: str
    header: list[str]
    sample_rows: list[dict[str, Any]]
    profile: Optional[ColumnProfile]
    field_map: dict[str, str] = field(default_factory=dict)
    volume_map: list[VolumeColumn] = field(default_factory=list)
    unmapped: list[str] = field(default_factory=list)
    blocking_problems: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def usable(self) -> bool:
        return self.profile is not None and not self.blocking_problems


def read_header(path: str | Path, sample_rows: int = 3) -> tuple[list[str], list[dict[str, Any]]]:
    """Return ``(header, first N rows)`` for csv / csv.gz / zip / parquet."""
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("parquet inspection requires pyarrow") from exc
        table = pq.read_table(p)
        rows = table.slice(0, sample_rows).to_pylist()
        return list(table.column_names), rows

    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as zf:
            members = [
                n
                for n in zf.namelist()
                if n.lower().endswith((".csv", ".txt")) and not n.endswith("/")
            ]
            if not members:
                raise RuntimeError(f"{p}: zip contains no .csv/.txt member")
            with zf.open(sorted(members)[0]) as raw:
                text = io.TextIOWrapper(io.BytesIO(raw.read()), encoding="utf-8-sig", newline="")
                return _read_csv_head(text, sample_rows)

    if ".gz" in [s.lower() for s in p.suffixes]:
        with gzip.open(p, "rt", encoding="utf-8-sig", newline="") as fh:
            return _read_csv_head(io.StringIO(fh.read()), sample_rows)

    with p.open("r", encoding="utf-8-sig", newline="") as fh:
        return _read_csv_head(io.StringIO(fh.read()), sample_rows)


def _read_csv_head(stream: io.TextIOBase, n: int) -> tuple[list[str], list[dict[str, Any]]]:
    sample = stream.read(64 * 1024)
    stream.seek(0)
    try:
        dialect: Any = csv.Sniffer().sniff(sample, delimiters=",;|\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(stream, dialect=dialect)
    header = list(reader.fieldnames or ())
    rows: list[dict[str, Any]] = []
    for i, row in enumerate(reader):
        if i >= n:
            break
        rows.append(dict(row))
    return header, rows


def _norm(text: str) -> str:
    return text.strip().lower().replace("-", "_").replace(" ", "_").replace(".", "_")


def _find_field(header: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    """Exact-normalized match first, then substring; never a fuzzy guess."""
    normalized = {_norm(h): h for h in header}
    for cand in candidates:
        hit = normalized.get(_norm(cand))
        if hit:
            return hit
    for cand in candidates:
        c = _norm(cand)
        for norm_h, orig in normalized.items():
            if c == norm_h or norm_h.endswith("_" + c) or norm_h.startswith(c + "_"):
                return orig
    return None


def _classify_volume_column(name: str) -> Optional[VolumeColumn]:
    """Infer ``(participant, side, effect)`` from a wide column's name.

    Requires a participant AND a side to be present.  The position effect may
    be absent — a column that names a participant and a side but no open/close
    designation is mapped to :attr:`PositionEffect.UNKNOWN`, which the
    inventory layer handles explicitly rather than assuming OPEN.
    """
    low = _norm(name)
    participant = None
    for member, tokens in PARTICIPANT_TOKENS:
        if any(_norm(t) in low for t in tokens if len(t) > 2):
            participant = member
            break
    if participant is None:
        # Short aliases ("mm", "bd") only match as whole underscore-tokens, so
        # a column called "summary" can't be read as a market maker column.
        parts = set(low.split("_"))
        for member, tokens in PARTICIPANT_TOKENS:
            if any(_norm(t) in parts for t in tokens):
                participant = member
                break
    if participant is None:
        return None

    parts = set(low.split("_"))
    side = None
    for member, tokens in SIDE_TOKENS:
        if any(_norm(t) in parts for t in tokens):
            side = member
            break
    if side is None:
        for member, tokens in SIDE_TOKENS:
            if any(_norm(t) in low for t in tokens if len(t) > 1):
                side = member
                break
    if side is None:
        return None

    effect: PositionEffect = PositionEffect.UNKNOWN
    for member, tokens in EFFECT_TOKENS:
        if any(_norm(t) in parts for t in tokens):
            effect = member
            break
    else:
        for member, tokens in EFFECT_TOKENS:
            if any(_norm(t) in low for t in tokens if len(t) > 1):
                effect = member
                break

    size_bucket = None
    for bucket, tokens in _SIZE_TOKENS:
        if any(_norm(t) in low for t in tokens):
            size_bucket = bucket
            break

    return VolumeColumn(
        column=name,
        participant=participant,
        side=side,
        position_effect=effect,
        size_bucket=size_bucket,
    )


def propose_profile(
    path: str | Path,
    *,
    default_symbol: str = "SPX",
    interval: Interval = Interval.UNKNOWN,
    sample_rows: int = 3,
) -> HeaderProposal:
    """Read a real file and infer a :class:`ColumnProfile` for human review."""
    header, rows = read_header(path, sample_rows=sample_rows)
    proposal = HeaderProposal(path=str(path), header=header, sample_rows=rows, profile=None)
    if not header:
        proposal.blocking_problems.append("file has no header row")
        return proposal

    field_map: dict[str, str] = {}
    for concept, candidates in ALIAS_CANDIDATES.items():
        hit = _find_field(header, candidates)
        if hit:
            field_map[concept] = hit
    proposal.field_map = field_map

    # Long layout when the file names participant/side/contracts as columns.
    is_long = all(k in field_map for k in ("participant", "side", "contracts"))

    volume_map: list[VolumeColumn] = []
    if not is_long:
        structural = set(field_map.values())
        for name in header:
            if name in structural:
                continue
            vc = _classify_volume_column(name)
            if vc is not None:
                volume_map.append(vc)
    proposal.volume_map = volume_map

    for concept in ("expiration", "strike", "option_type", "trading_date"):
        if concept not in field_map:
            proposal.blocking_problems.append(
                f"could not locate a {concept!r} column "
                f"(searched {list(ALIAS_CANDIDATES[concept])})"
            )

    if is_long:
        if "position_effect" not in field_map:
            proposal.warnings.append(
                "no open/close column found — every record will carry "
                "position_effect=UNKNOWN and only the net-flow estimator will be usable"
            )
    else:
        mm_cols = [vc for vc in volume_map if vc.participant is ParticipantType.MARKET_MAKER]
        if not mm_cols:
            proposal.blocking_problems.append(
                "no Market Maker volume column recognized — the experiment "
                "cannot run without MM-classified volume"
            )
        else:
            effects = {vc.position_effect for vc in mm_cols}
            if PositionEffect.OPEN not in effects or PositionEffect.CLOSE not in effects:
                proposal.warnings.append(
                    f"Market Maker columns carry effects {sorted(e.value for e in effects)}; "
                    "open/close inventory reconstruction needs both OPEN and CLOSE — "
                    "the net-flow estimator will still work"
                )
            sides = {vc.side for vc in mm_cols}
            if sides != {Side.BUY, Side.SELL}:
                proposal.blocking_problems.append(
                    f"Market Maker columns cover sides {sorted(s.value for s in sides)}; "
                    "both BUY and SELL are required"
                )

    if proposal.blocking_problems:
        proposal.unmapped = [h for h in header if h not in set(field_map.values())]
        return proposal

    profile = ColumnProfile(
        name=f"proposed_from_{Path(path).stem}",
        layout="long" if is_long else "wide",
        exchange=Exchange.CBOE_C1,
        interval=interval,
        confirmed=False,
        symbol_col=field_map.get("symbol"),
        option_root_col=field_map.get("option_root"),
        expiration_col=field_map["expiration"],
        strike_col=field_map["strike"],
        option_type_col=field_map["option_type"],
        trading_date_col=field_map["trading_date"],
        interval_end_col=field_map.get("interval_end"),
        volume_columns=tuple(volume_map),
        participant_col=field_map.get("participant"),
        side_col=field_map.get("side"),
        effect_col=field_map.get("position_effect"),
        contracts_col=field_map.get("contracts"),
        default_symbol=default_symbol,
        notes=(
            "PROPOSED by inspect.propose_profile — review every mapping below "
            "against the vendor's field documentation, then set confirmed=true."
        ),
    )
    proposal.profile = profile
    proposal.unmapped = unmapped_columns(profile, header)
    if proposal.unmapped:
        proposal.warnings.append(
            f"{len(proposal.unmapped)} column(s) are not mapped and will be ignored: "
            + ", ".join(proposal.unmapped[:20])
            + ("…" if len(proposal.unmapped) > 20 else "")
        )

    # Strike-scale sanity: an OSI-style file publishes strike in thousandths.
    if rows and "strike" in field_map:
        raw = [r.get(field_map["strike"]) for r in rows]
        numeric = []
        for value in raw:
            try:
                numeric.append(float(str(value).replace(",", "")))
            except (TypeError, ValueError):
                continue
        if numeric and min(numeric) > 100_000:
            proposal.warnings.append(
                f"strike values look scaled (min {min(numeric):,.0f}); if the feed "
                "publishes strike in thousandths set strike_scale=0.001"
            )
    return proposal


def render_proposal(proposal: HeaderProposal) -> str:
    """Human-readable review sheet for a proposal."""
    lines: list[str] = []
    lines.append(f"File: {proposal.path}")
    lines.append(f"Columns ({len(proposal.header)}): {', '.join(proposal.header)}")
    lines.append("")
    lines.append("Inferred structural fields:")
    if proposal.field_map:
        for concept, col in sorted(proposal.field_map.items()):
            lines.append(f"  {concept:<16} <- {col}")
    else:
        lines.append("  (none)")
    lines.append("")
    if proposal.volume_map:
        lines.append(f"Inferred volume columns ({len(proposal.volume_map)}):")
        for vc in proposal.volume_map:
            lines.append(f"  {vc.column:<40} -> {vc.describe()}")
        mm = [vc for vc in proposal.volume_map if vc.participant is ParticipantType.MARKET_MAKER]
        lines.append(f"  ... of which Market Maker: {len(mm)}")
        lines.append("")
    if proposal.unmapped:
        lines.append(f"UNMAPPED columns ({len(proposal.unmapped)}) — confirm these are not needed:")
        for name in proposal.unmapped:
            lines.append(f"  {name}")
        lines.append("")
    if proposal.warnings:
        lines.append("Warnings:")
        for w in proposal.warnings:
            lines.append(f"  ! {w}")
        lines.append("")
    if proposal.blocking_problems:
        lines.append("BLOCKING problems — no profile emitted:")
        for p in proposal.blocking_problems:
            lines.append(f"  x {p}")
        lines.append("")
    if proposal.sample_rows:
        lines.append("First row:")
        first = proposal.sample_rows[0]
        for k in proposal.header[:40]:
            lines.append(f"  {k:<40} = {first.get(k)!r}")
        lines.append("")
    lines.append(
        "This is a PROPOSAL. Nothing is confirmed until a human checks it "
        "against the vendor field documentation and sets confirmed=true."
    )
    return "\n".join(lines)
