"""Session-by-session assembly of the labelled dataset.

Order of operations per session, and the reason for each:

1. **Bars on the instrument's own price axis** — futures bars for ES/NQ, cash
   bars otherwise — de-phantomed if this is a cash index.
2. **Freeze the opening range**, then build the ladder.  Both happen before any
   gamma is read, so nothing about the ladder can depend on gamma.
3. **Extract touch events** from price alone.
4. **Build the gamma timeline**, ranking any GEX ladder against the INDEX spot
   at each frame's own timestamp.  A frame whose availability clock cannot be
   trusted raises, and the whole session is recorded as skipped — the brief's
   fail-closed requirement.
5. **Per event**: pick the newest frame that satisfies the lead time, project
   its levels onto the price axis, measure confluence, featurise, and label the
   forward excursion.

Two ordering choices are load-bearing.  Gamma is read AFTER the events exist,
so no gamma value can influence which events exist.  And the projection basis
is anchored at each frame's own availability instant, never at the session or
at "now", because a carry ratio walks toward expiry and applying today's to a
past frame offsets every level (``src/jobs/futures_projection.py:460``).

Output is JSONL plus a sibling ``.meta.json``.  Not Parquet: ``pyarrow`` is not
a project dependency, and the repo's existing research packages all write JSONL
so the analysis tooling already reads it.  A CSV export is available from the
report layer for spreadsheet work.  When ``cfg.null_replicas`` is non-zero a
third file, ``.null.jsonl``, holds the surrogate touches of :mod:`.null` -- kept
apart so nothing that reads the event rows can mistake one for a real touch.

:func:`build_datasets` builds several configs in ONE pass over the sessions.
Every config that differs only in the ladder (opening-range length, step)
reads identical rows from the database, and the gex_by_strike and basis reads
are most of a build's time; reading them once per session instead of once per
config is the difference between one build's wall-clock and three.  Each
config still runs the unchanged :func:`build_session`, so a shared build and a
solo build produce the same rows.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Sequence

from research.msi_regime_excursion.excursion import Bar
from research.or_gamma_confluence import sources
from research.or_gamma_confluence.basis import (
    BasisReader,
    BasisUnavailable,
    project_snapshot,
    resolve_basis_at,
)
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.events import extract_touch_events
from research.or_gamma_confluence.features import SessionContext, build_features
from research.or_gamma_confluence.instruments import InstrumentSpec, spec
from research.or_gamma_confluence.levels import build_timeline_tolerant, confluence_at
from research.or_gamma_confluence.null import surrogate_rows
from research.or_gamma_confluence.outcomes import measure_outcome
from research.or_gamma_confluence.ranges import (
    build_ladder,
    build_opening_range,
    dephantom_open_bar,
    session_window,
)

logger = logging.getLogger(__name__)

__all__ = [
    "SessionResult",
    "SessionFetchCache",
    "build_session",
    "build_dataset",
    "build_datasets",
    "DatasetWriter",
    "write_jsonl",
    "read_jsonl",
    "read_meta",
    "read_null",
    "null_path",
]

#: How far from an extension a level is still worth storing, as a multiple of
#: the widest reporting bucket.  A storage bound only — thresholds are applied
#: in :mod:`.cohorts`, so this must stay generously wider than any of them.
_STORE_DISTANCE_SLACK = 6.0


@dataclass
class SessionResult:
    """What one session contributed, including why it contributed nothing."""

    symbol: str
    session: date
    rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_reason: Optional[str] = None
    n_bars: int = 0
    n_frames: int = 0
    n_events: int = 0
    #: Events whose lead-time window contained no usable gamma frame.  Not an
    #: error — they are the "no pre-existing structure" observations, and the
    #: cohort split needs them.
    n_events_without_gamma: int = 0
    gex_rank_frames: int = 0
    #: Frames dropped because their availability clock could not be trusted.
    #: Counted, not silent: a run whose provenance shows many of these is
    #: measuring a degraded feed, whatever its cohort table says.
    frames_rejected: int = 0
    frame_reject_reasons: dict[str, int] = field(default_factory=dict)
    basis_source: Optional[str] = None
    or_open_bar_repaired: bool = False
    #: Surrogate touches from :func:`.null.surrogate_rows`.  Only built for a
    #: session that is USED, so the null and the real rows cover the same days.
    null_rows: list[dict[str, Any]] = field(default_factory=list)


def _spot_lookup(bars: Sequence[Bar]):
    """``ts -> close at or before ts``, from a sorted bar list."""
    import bisect

    stamps = [b.ts for b in bars]

    def _at(ts: datetime) -> Optional[float]:
        i = bisect.bisect_right(stamps, ts) - 1
        return bars[i].close if i >= 0 else None

    return _at


class SessionFetchCache:
    """The read side of :mod:`.sources`, memoised for ONE session at a time.

    :func:`build_session` only ever calls these seven readers, each keyed by
    instrument and session and by nothing in the config that differs between
    ladder variants (``session_bounds`` reads ``session_start`` /
    ``session_end`` only, and :func:`build_datasets` refuses configs that
    disagree on those).  Holding just the current session keeps memory flat:
    a session's ``gex_by_strike`` rows are the largest object in a build.
    """

    def __init__(self, base: Any = sources) -> None:
        self._base = base
        self._key: Any = None
        self._memo: dict[tuple[Any, ...], Any] = {}

    def _get(self, name: str, conn: Any, who: str, session: date, *args: Any) -> Any:
        if self._key != (who, session) and name in ("fetch_bars", "probe_bars"):
            # A new instrument-session starts with its bars.  Drop the last one.
            self._key = (who, session)
            self._memo.clear()
        key = (name, who, session)
        if key not in self._memo:
            self._memo[key] = getattr(self._base, name)(conn, *args)
        return self._memo[key]

    def fetch_bars(self, conn, inst, session, cfg):
        return self._get("fetch_bars", conn, inst.key, session, inst, session, cfg)

    def probe_bars(self, conn, inst, session):
        return self._get("probe_bars", conn, inst.key, session, inst, session)

    def fetch_summary_frames(self, conn, gamma_symbol, session, cfg):
        return self._get(
            "fetch_summary_frames", conn, gamma_symbol, session, gamma_symbol, session, cfg
        )

    def fetch_index_bars(self, conn, gamma_symbol, session, cfg):
        return self._get(
            "fetch_index_bars", conn, gamma_symbol, session, gamma_symbol, session, cfg
        )

    def fetch_strike_frames(self, conn, gamma_symbol, session, cfg):
        return self._get(
            "fetch_strike_frames", conn, gamma_symbol, session, gamma_symbol, session, cfg
        )

    def fetch_volumes(self, conn, inst, session, cfg):
        return self._get("fetch_volumes", conn, inst.key, session, inst, session, cfg)

    def fetch_trade_bias(self, conn, gamma_symbol, session, cfg):
        return self._get(
            "fetch_trade_bias", conn, gamma_symbol, session, gamma_symbol, session, cfg
        )


def build_session(
    conn: Any,
    inst: InstrumentSpec,
    session: date,
    cfg: ResearchConfig,
    *,
    basis_reader: Optional[BasisReader] = None,
    fetch: Any = None,
) -> SessionResult:
    """Label every touch event in one session, with its features and outcome.

    ``fetch`` defaults to :mod:`.sources`; :func:`build_datasets` passes a
    :class:`SessionFetchCache` so several configs share one set of reads.
    """
    src = sources if fetch is None else fetch
    result = SessionResult(symbol=inst.key, session=session)

    bars = src.fetch_bars(conn, inst, session, cfg)
    result.n_bars = len(bars)
    if not bars:
        # "no_bars" on its own cannot distinguish a coverage gap from a window
        # bug, and those need different responses. Probe only on the empty
        # path, so the normal path pays nothing.
        probe = src.probe_bars(conn, inst, session)
        detail = probe.get("probe", "unknown")
        if detail == "rows_outside_session_window":
            detail += (
                f" ({probe['rows_on_date']} rows on date, "
                f"{probe.get('first_et')}-{probe.get('last_et')} ET)"
            )
        result.skipped_reason = f"no_bars: {detail}"
        return result

    # De-phantom ONCE, here, so the repaired bars feed the opening range, the
    # session high/low and every feature alike.  Keyed on ``inst.key``, not
    # ``bar_symbol``: ES/NQ bars come from the futures feed and are real traded
    # prints, and their ``bar_symbol`` names the backing cash index — routing on
    # that would "repair" a futures bar that was never broken.
    or_start, _ = session_window(session, cfg)
    bars, repaired = dephantom_open_bar(bars, inst.key, or_start)
    result.or_open_bar_repaired = repaired

    orange, why = build_opening_range(bars, session, inst.key, cfg)
    if orange is None:
        result.skipped_reason = why
        return result

    ladder = build_ladder(orange, cfg)
    events = extract_touch_events(inst.key, orange, ladder, bars, cfg, tick=inst.tick)
    result.n_events = len(events)

    frames = src.fetch_summary_frames(conn, inst.gamma_symbol, session, cfg)
    result.n_frames = len(frames)
    if len(frames) < cfg.min_session_frames:
        result.skipped_reason = f"frames_{len(frames)}_below_min_{cfg.min_session_frames}"
        return result

    # The GEX ladder ranks INDEX strikes, so it needs the INDEX spot — never
    # the futures price, which sits a basis away and would flip which side of
    # spot every strike falls on.
    index_bars = (
        src.fetch_index_bars(conn, inst.gamma_symbol, session, cfg) if inst.is_futures else bars
    )
    index_spot_at = _spot_lookup(index_bars)
    strike_frames = (
        src.fetch_strike_frames(conn, inst.gamma_symbol, session, cfg) if cfg.use_gex_ranks else {}
    )

    built = build_timeline_tolerant(
        frames,
        cfg,
        spot_at=index_spot_at,
        strikes_at=strike_frames.get,
    )
    result.frames_rejected = built.rejected
    result.frame_reject_reasons = dict(built.reasons)
    if built.rejected_frac > cfg.max_rejected_frame_frac:
        # Fail closed. An isolated unusable frame is dropped and the step
        # function holds the previous value across it; a session where many
        # frames are unusable does not have a clock worth trusting anywhere.
        result.skipped_reason = (
            f"gamma_clock_rejected: {built.rejected}/{built.total} frames "
            f"({built.rejected_frac:.1%}) unusable {built.reasons}"
        )
        return result
    timeline = built.timeline
    if built.accepted < cfg.min_session_frames:
        result.skipped_reason = f"usable_frames_{built.accepted}_below_min_{cfg.min_session_frames}"
        return result
    result.gex_rank_frames = sum(1 for s in timeline if s.gex_ranks_available)

    volumes, proxy = src.fetch_volumes(conn, inst, session, cfg)
    bias_rows = src.fetch_trade_bias(conn, inst.gamma_symbol, session, cfg)
    ctx = SessionContext.build(
        inst.key, bars, cfg, volumes=volumes, volume_proxy=proxy, bias_rows=bias_rows
    )

    store_distance = max(cfg.confluence_buckets_pts or (10.0,)) * _STORE_DISTANCE_SLACK
    fingerprint = cfg.fingerprint()
    rows: list[dict[str, Any]] = []

    for i, event in enumerate(events):
        snapshot = timeline.as_of(event.touched_at, cfg.gamma_min_lead_seconds)
        confluence = None
        basis = None
        if snapshot is not None:
            if inst.needs_basis:
                if basis_reader is None:
                    raise BasisUnavailable(
                        f"{inst.key} needs a futures basis; no BasisReader supplied"
                    )
                # Anchored at the frame's OWN availability instant.
                basis = resolve_basis_at(basis_reader, inst, snapshot.available_at)
                result.basis_source = basis.source if basis else None
                snapshot = project_snapshot(snapshot, basis, inst.tick)
            confluence = confluence_at(snapshot, event.level_price, max_distance=store_distance)
        else:
            result.n_events_without_gamma += 1

        row: dict[str, Any] = {
            **event.to_dict(),
            **build_features(
                event,
                ctx,
                orange,
                ladder,
                cfg,
                snapshot=snapshot,
                confluence=confluence,
                prior=events[:i],
            ),
            **measure_outcome(event, ctx.series, cfg),
            "config_fingerprint": fingerprint,
            "instrument": inst.to_dict(),
            "gamma_symbol": inst.gamma_symbol,
        }
        if basis is not None:
            row.update(
                {
                    "basis_ratio": basis.ratio,
                    "basis_source": basis.source,
                    "basis_observed_at": (
                        basis.observed_at.isoformat() if basis.observed_at else None
                    ),
                    "basis_sample_count": basis.sample_count,
                }
            )
        else:
            row.update(
                {
                    "basis_ratio": None,
                    "basis_source": None,
                    "basis_observed_at": None,
                    "basis_sample_count": None,
                }
            )
        # Levels are kept on the row so any classification can be reconstructed
        # from the row alone — the brief's audit requirement.
        row["gamma_levels_nearby"] = (
            [{**lv.to_dict(), "distance": d} for lv, d in confluence.matches[:12]]
            if confluence is not None
            else []
        )
        rows.append(row)

    result.rows = rows
    result.null_rows = surrogate_rows(inst.key, bars, orange, ladder, cfg, tick=inst.tick)
    return result


def _guarded(
    conn: Any,
    inst: InstrumentSpec,
    session: date,
    cfg: ResearchConfig,
    reader: BasisReader,
    fetch: Any = None,
) -> SessionResult:
    """:func:`build_session`, with a failure recorded as a skip, not raised."""
    try:
        return build_session(conn, inst, session, cfg, basis_reader=reader, fetch=fetch)
    except BasisUnavailable as exc:
        return SessionResult(symbol=inst.key, session=session, skipped_reason=f"basis: {exc}")
    except Exception as exc:  # one bad session must not end the run
        logger.warning("%s %s failed: %r", inst.key, session, exc, exc_info=True)
        return SessionResult(
            symbol=inst.key,
            session=session,
            skipped_reason=f"error: {type(exc).__name__}: {exc}",
        )


def build_dataset(
    conn: Any,
    symbols: Sequence[str],
    start: date,
    end: date,
    cfg: ResearchConfig,
) -> Iterator[SessionResult]:
    """Every session for every symbol, in chronological order per symbol.

    Chronological because the trailing / prior-session state any future feature
    needs must only ever see earlier sessions, and because the out-of-sample
    split is a date cut — a shuffled build would make that cut meaningless.
    """
    reader = BasisReader(conn)
    for symbol in symbols:
        inst = spec(symbol)
        sessions = sources.fetch_sessions(conn, inst.gamma_symbol, start, end)
        logger.info("%s: %d candidate sessions", inst.key, len(sessions))
        for session in sessions:
            yield _guarded(conn, inst, session, cfg, reader)


def build_datasets(
    conn: Any,
    symbols: Sequence[str],
    start: date,
    end: date,
    cfgs: Sequence[ResearchConfig],
    writers: Sequence["DatasetWriter"],
    *,
    fetch_base: Any = None,
) -> None:
    """Build every config in ``cfgs`` in one pass, each into its own writer.

    Same order and same per-session code as :func:`build_dataset`; the only
    difference is that the database is read once per session rather than once
    per config.  The configs must agree on the session window, because that is
    the one config input the readers depend on.
    """
    if len(cfgs) != len(writers):
        raise ValueError("one writer per config")
    windows = {(c.session_start, c.session_end) for c in cfgs}
    if len(windows) > 1:
        raise ValueError(f"configs disagree on the session window: {sorted(windows)}")
    base = sources if fetch_base is None else fetch_base
    cache = SessionFetchCache(base)
    reader = BasisReader(conn)
    for symbol in symbols:
        inst = spec(symbol)
        sessions = base.fetch_sessions(conn, inst.gamma_symbol, start, end)
        logger.info("%s: %d candidate sessions x %d configs", inst.key, len(sessions), len(cfgs))
        for session in sessions:
            for cfg, writer in zip(cfgs, writers):
                writer.add(_guarded(conn, inst, session, cfg, reader, fetch=cache))


def _meta_path(path: str | Path) -> Path:
    return Path(str(path) + ".meta.json")


def null_path(path: str | Path) -> Path:
    """Where the surrogate rows for the dataset at ``path`` live."""
    return Path(str(path) + ".null.jsonl")


class DatasetWriter:
    """Streams one dataset's events, surrogate rows and provenance to disk.

    Incremental, so a multi-config build holds one session in memory rather
    than every config's whole dataset.  :meth:`close` writes the
    ``.meta.json``; until then the meta file is absent, so a half-written
    dataset cannot be analyzed as though it were complete.
    """

    def __init__(self, path: str | Path, cfg: ResearchConfig) -> None:
        self.path = Path(path)
        self.cfg = cfg
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _meta_path(self.path).unlink(missing_ok=True)
        self._fh = self.path.open("w", encoding="utf-8")
        self._null_fh = None
        if cfg.null_replicas > 0:
            self._null_fh = null_path(self.path).open("w", encoding="utf-8")
        else:
            null_path(self.path).unlink(missing_ok=True)
        self.per_symbol: dict[str, dict[str, Any]] = {}
        self.skips: dict[str, int] = {}
        self.n_rows = 0
        self.n_null_rows = 0

    def add(self, result: SessionResult) -> None:
        acc = self.per_symbol.setdefault(
            result.symbol,
            {
                "sessions_seen": 0,
                "sessions_used": 0,
                "events": 0,
                "events_without_gamma": 0,
                "gex_rank_frames": 0,
                "frames_rejected": 0,
                "frame_reject_reasons": {},
                "open_bars_repaired": 0,
                "basis_sources": {},
                "null_rows": 0,
            },
        )
        acc["sessions_seen"] += 1
        acc["frames_rejected"] += result.frames_rejected
        for reason, count in result.frame_reject_reasons.items():
            acc["frame_reject_reasons"][reason] = acc["frame_reject_reasons"].get(reason, 0) + count
        if result.or_open_bar_repaired:
            acc["open_bars_repaired"] += 1
        if result.skipped_reason:
            # Keep the diagnostic detail. Collapsing on the first colon
            # threw away exactly the part that says WHY, which made the
            # bar-coverage probe pointless in the summary.
            key = result.skipped_reason
            if key.startswith("error:"):
                key = "error"  # exception text is unbounded
            self.skips[key] = self.skips.get(key, 0) + 1
            return
        acc["sessions_used"] += 1
        acc["events"] += len(result.rows)
        acc["events_without_gamma"] += result.n_events_without_gamma
        acc["gex_rank_frames"] += result.gex_rank_frames
        if result.basis_source:
            acc["basis_sources"][result.basis_source] = (
                acc["basis_sources"].get(result.basis_source, 0) + 1
            )
        for row in result.rows:
            self._fh.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
            self.n_rows += 1
        if self._null_fh is not None:
            for row in result.null_rows:
                self._null_fh.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
                self.n_null_rows += 1
            acc["null_rows"] += len(result.null_rows)

    def close(self, *, symbols: Sequence[str], start: date, end: date) -> dict[str, Any]:
        self._fh.close()
        if self._null_fh is not None:
            self._null_fh.close()
        meta = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "config": self.cfg.to_dict(),
            "config_fingerprint": self.cfg.fingerprint(),
            "symbols": list(symbols),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "rows": self.n_rows,
            "null_rows": self.n_null_rows,
            "null_path": str(null_path(self.path)) if self._null_fh is not None else None,
            "per_symbol": self.per_symbol,
            "skip_reasons": self.skips,
        }
        _meta_path(self.path).write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        return meta


def write_jsonl(
    path: str | Path,
    results: Iterable[SessionResult],
    cfg: ResearchConfig,
    *,
    symbols: Sequence[str],
    start: date,
    end: date,
) -> dict[str, Any]:
    """Write events to ``path`` and provenance to ``<path>.meta.json``.

    The meta file carries the config fingerprint, the window, per-symbol session
    accounting and every skip reason.  A printed report reads it, so a report
    always carries its own provenance and a thin sample reads as thin coverage
    rather than as an absence of effect.  Surrogate rows, when the config asks
    for them, go to :func:`null_path`.
    """
    writer = DatasetWriter(path, cfg)
    for result in results:
        writer.add(result)
    return writer.close(symbols=symbols, start=start, end=end)


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def read_null(path: str | Path) -> list[dict[str, Any]]:
    """The surrogate rows beside the dataset at ``path``; ``[]`` if none."""
    p = null_path(path)
    return read_jsonl(p) if p.exists() else []


def read_meta(path: str | Path) -> Optional[dict[str, Any]]:
    """The sidecar provenance for a dataset, or ``None`` if absent."""
    meta = _meta_path(path)
    if not meta.exists():
        return None
    try:
        return json.loads(meta.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
