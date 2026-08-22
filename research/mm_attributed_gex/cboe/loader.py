"""Read Open-Close files into normalized :class:`ParticipantActivity` records.

Streaming by design.  A full-chain 1-minute SPX Open-Close day is tens of
millions of volume buckets; the loader never materializes a whole file, it
yields records so callers can fold them straight into an inventory
accumulator.  ``load_paths`` is a generator over any number of files.

Supported containers: ``.csv``, ``.csv.gz``/``.gz``, ``.zip`` (every ``.csv``
member), ``.txt`` with a delimiter sniff, and ``.parquet`` when ``pyarrow`` is
installed.  The container is orthogonal to the :class:`ColumnProfile`, which
only describes columns.
"""

from __future__ import annotations

import csv
import gzip
import io
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from research.mm_attributed_gex.cboe.profiles import (
    ColumnProfile,
    EFFECT_TOKENS,
    PARTICIPANT_TOKENS,
    SIDE_TOKENS,
    VolumeColumn,
)
from research.mm_attributed_gex.schema import (
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    normalize_option_type,
)

__all__ = ["LoadResult", "load_file", "load_paths", "records_from_rows", "ET"]

try:  # Python 3.9+ stdlib; the repo already depends on tzdata via pytz.
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
    _ET_IS_PYTZ = False
except Exception:  # pragma: no cover - defensive; pytz is a hard dependency
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]
    _ET_IS_PYTZ = True


def _localize_et(naive: datetime) -> datetime:
    """Attach Eastern Time to a naive wall-clock datetime.

    ``replace(tzinfo=...)`` is correct for :mod:`zoneinfo` (the offset is
    resolved from the date) but silently wrong for pytz, which hands out a
    fixed LMT offset until ``localize`` is called.  Branching here keeps the
    fallback path honest instead of shifting every DST-season bucket by
    several minutes.
    """
    if _ET_IS_PYTZ:  # pragma: no cover - fallback path
        return ET.localize(naive)  # type: ignore[attr-defined]
    return naive.replace(tzinfo=ET)


class ProfileNotConfirmed(RuntimeError):
    """Raised when an unverified column mapping is used without opting in.

    An unconfirmed profile is a *guess* at the file's schema.  Letting one run
    silently is how a research result ends up resting on mis-attributed flow,
    so the loader makes the caller say ``allow_unconfirmed=True`` out loud.
    """


class ProfileMismatch(ValueError):
    """Raised when the profile's columns are absent from the file header."""


@dataclass
class LoadResult:
    """Counters describing what a load actually did.

    Reported alongside every experiment run so "the methodology failed" can be
    told apart from "the file was half-parsed".
    """

    files: int = 0
    files_skipped: int = 0
    rows_read: int = 0
    rows_skipped: int = 0
    records_emitted: int = 0
    zero_volume_buckets: int = 0
    contracts_total: int = 0
    mm_contracts_total: int = 0
    unknown_effect_contracts: int = 0
    symbols_seen: set[str] = field(default_factory=set)
    trading_dates: set[date] = field(default_factory=set)
    errors: list[str] = field(default_factory=list)

    def merge(self, other: "LoadResult") -> "LoadResult":
        self.files += other.files
        self.files_skipped += other.files_skipped
        self.rows_read += other.rows_read
        self.rows_skipped += other.rows_skipped
        self.records_emitted += other.records_emitted
        self.zero_volume_buckets += other.zero_volume_buckets
        self.contracts_total += other.contracts_total
        self.mm_contracts_total += other.mm_contracts_total
        self.unknown_effect_contracts += other.unknown_effect_contracts
        self.symbols_seen |= other.symbols_seen
        self.trading_dates |= other.trading_dates
        self.errors.extend(other.errors)
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "files": self.files,
            "files_skipped": self.files_skipped,
            "rows_read": self.rows_read,
            "rows_skipped": self.rows_skipped,
            "records_emitted": self.records_emitted,
            "zero_volume_buckets": self.zero_volume_buckets,
            "contracts_total": self.contracts_total,
            "mm_contracts_total": self.mm_contracts_total,
            "unknown_effect_contracts": self.unknown_effect_contracts,
            "symbols_seen": sorted(self.symbols_seen),
            "trading_dates": sorted(d.isoformat() for d in self.trading_dates),
            "errors": self.errors[:50],
            "error_count": len(self.errors),
        }


# ---------------------------------------------------------------------------
# Cell coercion
# ---------------------------------------------------------------------------


def _parse_date(raw: object, formats: Sequence[str]) -> date:
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    text = str(raw).strip()
    if not text:
        raise ValueError("empty date cell")
    # ISO first: cheapest and the most common wire form.
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unparseable date {text!r} (tried {list(formats)})")


def _parse_time(raw: object, formats: Sequence[str]) -> Optional[time]:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.timetz().replace(tzinfo=None)
    if isinstance(raw, time):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    # A full timestamp in the interval column is common; take its time part.
    if len(text) > 10 and ("T" in text or " " in text):
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).time()
        except ValueError:
            pass
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _parse_int(raw: object) -> int:
    if raw is None:
        return 0
    if isinstance(raw, (int,)) and not isinstance(raw, bool):
        return int(raw)
    if isinstance(raw, float):
        return int(round(raw))
    text = str(raw).strip().replace(",", "")
    if not text or text in ("-", "NA", "N/A", "null", "NULL", "None"):
        return 0
    try:
        return int(text)
    except ValueError:
        return int(round(float(text)))


def _parse_float(raw: object) -> float:
    if raw is None:
        raise ValueError("empty numeric cell")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    text = str(raw).strip().replace(",", "").replace("$", "")
    if not text:
        raise ValueError("empty numeric cell")
    return float(text)


def _match_token(text: str, table: Sequence[tuple[Any, Sequence[str]]]) -> Optional[Any]:
    """Resolve a cell value (long layout) or column name to an enum member.

    Longest token first inside each entry so ``"professional_customer"`` beats
    ``"customer"`` when both would match.
    """
    low = text.strip().lower().replace("-", "_").replace(" ", "_")
    for member, tokens in table:
        for token in sorted(tokens, key=len, reverse=True):
            norm = token.replace("-", "_").replace(" ", "_")
            if low == norm:
                return member
    for member, tokens in table:
        for token in sorted(tokens, key=len, reverse=True):
            norm = token.replace("-", "_").replace(" ", "_")
            if norm in low:
                return member
    return None


def _bucket_timestamp(
    trading_date: date, interval_end: Optional[time], profile: ColumnProfile
) -> datetime:
    """Timestamp for a bucket, always tz-aware UTC.

    With no interval column (an end-of-day summary) the bucket is stamped at
    16:00 ET on the trading date — the session close, i.e. the instant at which
    the day's aggregate flow is fully known.  Stamping it at the session close
    rather than the open is what keeps the reconstruction causal: an inventory
    level derived from a session summary must not be used to explain price
    action inside that same session.
    """
    if interval_end is None:
        naive = datetime.combine(trading_date, time(16, 0))
    else:
        naive = datetime.combine(trading_date, interval_end)
    return _localize_et(naive).astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Row -> records
# ---------------------------------------------------------------------------


def records_from_rows(
    rows: Iterable[Mapping[str, Any]],
    profile: ColumnProfile,
    *,
    result: Optional[LoadResult] = None,
    source: str = "",
    drop_zero: bool = True,
) -> Iterator[ParticipantActivity]:
    """Convert already-read rows into records.  Pure; no file I/O.

    ``drop_zero`` discards empty buckets, which are the overwhelming majority
    of cells in a wide Open-Close file (most strikes see no Firm closing sells
    in most 30-minute windows).  They carry no information for the inventory
    recursion — adding zero changes nothing — and keeping them would multiply
    the record count by ~20 for no gain.  They are still counted in
    :attr:`LoadResult.zero_volume_buckets` so a file that is *entirely* empty
    is visibly distinguishable from one that failed to parse.
    """
    res = result if result is not None else LoadResult()
    wide = profile.layout == "wide"
    symbol_filter = profile.symbol_filter

    for row in rows:
        res.rows_read += 1
        try:
            raw_symbol = (
                str(row.get(profile.symbol_col, "")).strip()
                if profile.symbol_col
                else (profile.default_symbol or "")
            )
            symbol = raw_symbol or (profile.default_symbol or "")
            if not symbol:
                res.rows_skipped += 1
                continue
            if symbol_filter and symbol.upper() not in {s.upper() for s in symbol_filter}:
                res.rows_skipped += 1
                continue
            # Canonicalize a vendor-decorated symbol ("$SPX.X" -> "SPX") but
            # keep the listed root separately: SPX vs SPXW decides AM vs PM
            # settlement downstream.
            canonical = symbol.upper().lstrip("$")
            if canonical.endswith(".X"):
                canonical = canonical[:-2]
            option_root = (
                str(row.get(profile.option_root_col, "")).strip() or None
                if profile.option_root_col
                else (symbol.upper() if symbol.upper().startswith("SPXW") else None)
            )
            if canonical == "SPXW":
                canonical, option_root = "SPX", option_root or "SPXW"

            expiration = _parse_date(row.get(profile.expiration_col), profile.date_formats)
            trading_date = _parse_date(row.get(profile.trading_date_col), profile.date_formats)
            strike = _parse_float(row.get(profile.strike_col)) * profile.strike_scale
            option_type = normalize_option_type(row.get(profile.option_type_col))
            interval_end = (
                _parse_time(row.get(profile.interval_end_col), profile.time_formats)
                if profile.interval_end_col
                else None
            )
            timestamp = _bucket_timestamp(trading_date, interval_end, profile)
        except Exception as exc:  # noqa: BLE001 - row-level tolerance is the point
            res.rows_skipped += 1
            if len(res.errors) < 200:
                res.errors.append(f"{source}: row {res.rows_read}: {exc}")
            continue

        res.symbols_seen.add(canonical)
        res.trading_dates.add(trading_date)

        buckets: Iterable[tuple[VolumeColumn, int]]
        if wide:
            buckets = ((vc, _parse_int(row.get(vc.column))) for vc in profile.volume_columns)
        else:
            participant = _match_token(
                str(row.get(profile.participant_col, "")), PARTICIPANT_TOKENS
            )
            side = _match_token(str(row.get(profile.side_col, "")), SIDE_TOKENS)
            effect = (
                _match_token(str(row.get(profile.effect_col, "")), EFFECT_TOKENS)
                if profile.effect_col
                else None
            )
            if participant is None or side is None:
                res.rows_skipped += 1
                if len(res.errors) < 200:
                    res.errors.append(
                        f"{source}: row {res.rows_read}: unmapped participant/side "
                        f"({row.get(profile.participant_col)!r}/{row.get(profile.side_col)!r})"
                    )
                continue
            buckets = (
                (
                    VolumeColumn(
                        column=profile.contracts_col or "contracts",
                        participant=participant,
                        side=side,
                        position_effect=effect or PositionEffect.UNKNOWN,
                    ),
                    _parse_int(row.get(profile.contracts_col)),
                ),
            )

        for vc, contracts in buckets:
            if contracts == 0:
                res.zero_volume_buckets += 1
                if drop_zero:
                    continue
            if contracts < 0:
                res.rows_skipped += 1
                if len(res.errors) < 200:
                    res.errors.append(
                        f"{source}: row {res.rows_read}: negative volume {contracts} "
                        f"in {vc.column!r} — Open-Close volumes are counts, not net"
                    )
                continue
            res.records_emitted += 1
            res.contracts_total += contracts
            if vc.participant is ParticipantType.MARKET_MAKER:
                res.mm_contracts_total += contracts
            if vc.position_effect is PositionEffect.UNKNOWN:
                res.unknown_effect_contracts += contracts
            attrs: dict[str, object] = {"source": source, "column": vc.column}
            if vc.size_bucket:
                attrs["size_bucket"] = vc.size_bucket
            yield ParticipantActivity(
                exchange=profile.exchange,
                symbol=canonical,
                expiration=expiration,
                strike=strike,
                option_type=option_type,
                participant=vc.participant,
                side=vc.side,
                position_effect=vc.position_effect,
                contracts=contracts,
                timestamp=timestamp,
                trading_date=trading_date,
                interval=profile.interval,
                option_root=option_root,
                attrs=attrs,
            )


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------


def _sniff_reader(stream: io.TextIOBase, name: str) -> csv.DictReader:
    sample = stream.read(64 * 1024)
    stream.seek(0)
    try:
        dialect: Any = csv.Sniffer().sniff(sample, delimiters=",;|\t")
    except csv.Error:
        dialect = csv.excel
    return csv.DictReader(stream, dialect=dialect)


def _iter_csv_streams(path: Path) -> Iterator[tuple[str, io.TextIOBase]]:
    suffixes = [s.lower() for s in path.suffixes]
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            members = [
                n
                for n in zf.namelist()
                if n.lower().endswith((".csv", ".txt")) and not n.endswith("/")
            ]
            if not members:
                raise ProfileMismatch(f"{path}: zip contains no .csv/.txt member")
            for member in sorted(members):
                with zf.open(member) as raw:
                    yield f"{path.name}:{member}", io.TextIOWrapper(
                        io.BytesIO(raw.read()), encoding="utf-8-sig", newline=""
                    )
        return
    if ".gz" in suffixes:
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as fh:
            yield path.name, io.StringIO(fh.read())
        return
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        yield path.name, io.StringIO(fh.read())


def _iter_parquet_rows(path: Path) -> tuple[list[str], Iterator[Mapping[str, Any]]]:
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ProfileMismatch(
            f"{path}: parquet input requires pyarrow (pip install pyarrow)"
        ) from exc
    table = pq.read_table(path)
    header = list(table.column_names)

    def _gen() -> Iterator[Mapping[str, Any]]:
        for batch in table.to_batches(max_chunksize=50_000):
            yield from batch.to_pylist()

    return header, _gen()


def load_file(
    path: str | Path,
    profile: ColumnProfile,
    *,
    allow_unconfirmed: bool = False,
    result: Optional[LoadResult] = None,
    drop_zero: bool = True,
) -> Iterator[ParticipantActivity]:
    """Stream one Open-Close file as normalized records.

    Raises :class:`ProfileNotConfirmed` for an unverified mapping unless
    ``allow_unconfirmed`` is set, and :class:`ProfileMismatch` when the
    profile's columns are not in the file.
    """
    if not profile.confirmed and not allow_unconfirmed:
        raise ProfileNotConfirmed(
            f"Column profile {profile.name!r} is not confirmed against a real file. "
            "Inspect a delivered sample "
            "(`python -m research.mm_attributed_gex.cli inspect-cboe <file>`), "
            "confirm the mapping, save it with confirmed=true, and re-run — or pass "
            "allow_unconfirmed=True for a deliberate exploratory parse."
        )
    p = Path(path)
    res = result if result is not None else LoadResult()
    counted = False

    if p.suffix.lower() == ".parquet":
        header, rows = _iter_parquet_rows(p)
        problems = profile.validate(header)
        if problems:
            raise ProfileMismatch(f"{p}: " + "; ".join(problems))
        res.files += 1
        yield from records_from_rows(rows, profile, result=res, source=p.name, drop_zero=drop_zero)
        return

    for source, stream in _iter_csv_streams(p):
        reader = _sniff_reader(stream, source)
        header = list(reader.fieldnames or ())
        problems = profile.validate(header)
        if problems:
            raise ProfileMismatch(f"{source}: " + "; ".join(problems))
        if not counted:
            # Count the FILE once, after its header validates — a zip with
            # several members is still one file.
            res.files += 1
            counted = True
        yield from records_from_rows(
            reader, profile, result=res, source=source, drop_zero=drop_zero
        )


#: Filenames a data directory routinely carries that are not data.  A real
#: Open-Close delivery ships alongside a readme, a manifest and checksums; the
#: walk must not treat those as files that failed to parse.
_NON_DATA_STEMS = ("readme", "manifest", "checksum", "sha256", "md5", "license", "notice")


def _looks_like_documentation(path: Path) -> bool:
    stem = path.stem.lower()
    return any(token in stem for token in _NON_DATA_STEMS)


def load_paths(
    paths: Iterable[str | Path],
    profile: ColumnProfile,
    *,
    allow_unconfirmed: bool = False,
    result: Optional[LoadResult] = None,
    drop_zero: bool = True,
) -> Iterator[ParticipantActivity]:
    """Stream many files (or directories of files) in sorted order.

    Sorted order matters: the inventory recursion is a running sum over time,
    so files must be consumed chronologically.  Directory arguments expand to
    their supported members, also sorted.

    Explicitly-named files and directory members are treated differently on a
    header mismatch, and the asymmetry is deliberate.  Naming a file is a
    statement that it *is* data, so a mismatch there is an error worth
    stopping on.  A directory is a container that legitimately holds readmes,
    manifests and checksums next to the data, so a member that does not match
    the profile is skipped and counted rather than aborting the run.  If
    *nothing* in the directory matched, that is a real failure and it raises —
    silently yielding zero records would look exactly like a day with no
    trading.
    """
    res = result if result is not None else LoadResult()
    explicit: list[Path] = []
    from_directory: list[Path] = []
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            from_directory.extend(
                sorted(
                    q
                    for q in p.rglob("*")
                    if q.is_file()
                    and q.suffix.lower() in (".csv", ".gz", ".zip", ".txt", ".parquet")
                    and not _looks_like_documentation(q)
                )
            )
        else:
            explicit.append(p)

    mismatches: list[str] = []
    loaded_from_directory = 0
    for p in sorted(explicit):
        yield from load_file(
            p, profile, allow_unconfirmed=allow_unconfirmed, result=res, drop_zero=drop_zero
        )
    for p in sorted(from_directory):
        try:
            yield from load_file(
                p, profile, allow_unconfirmed=allow_unconfirmed, result=res, drop_zero=drop_zero
            )
            loaded_from_directory += 1
        except ProfileMismatch as exc:
            res.files_skipped += 1
            mismatches.append(str(exc).split(";")[0])
            if len(res.errors) < 200:
                res.errors.append(f"skipped (header mismatch): {p.name}")

    if from_directory and not explicit and loaded_from_directory == 0:
        raise ProfileMismatch(
            f"none of the {len(from_directory)} file(s) found matched profile "
            f"{profile.name!r}. First mismatch: " + (mismatches[0] if mismatches else "unknown")
        )
