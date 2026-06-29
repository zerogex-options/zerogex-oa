"""Empirical-base feedback loop for the Playbook engine.

The live ``PatternBase.compute_confidence`` starts from a hand-set
``pattern_base`` prior. This module lets the engine *replace* that prior with
the empirical win rate the backtest harness measured for the pattern
(``playbook_pattern_stats.proposed_base``), so live Action Card confidence is
grounded in what a pattern actually did rather than a guess.

Design points (see ``docs/design/pattern-calibration.md``):

* **Off by default.** ``calibrated_base`` returns the supplied fallback prior
  unless calibration is enabled *and* a trustworthy measurement exists, so the
  feature is inert until an operator turns it on.
* **Sample-size gated.** A (pattern, underlying) window is only trusted once it
  has at least ``MIN_SAMPLES`` resolved trades; otherwise the prior is kept.
* **Freshness gated.** Windows older than ``MAX_AGE_DAYS`` are ignored — edge
  decays, so a stale number must not pin live confidence.
* **Clamped.** The measured base is clamped to [FLOOR, CEIL] (the catalog's
  design band) so one unlucky/lucky window can't push a pattern out of range.
* **Per-(pattern, underlying)** with a sample-weighted **pattern-wide**
  fallback, since live patterns evaluate per underlying.

The active store is a process-global loaded from the DB by the long-running
signals service via :func:`maybe_refresh` (a cheap no-op between reloads). The
hot-path consult (:func:`calibrated_base`) never touches the DB.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from src import config

logger = logging.getLogger(__name__)


def _band_for_source(source: Optional[str]) -> tuple[float, float]:
    """Return the (floor, ceil) clamp band for a measurement source.

    ``option_pnl`` has its own overridable band (defaulting to the global one);
    every other source uses the global [FLOOR, CEIL].
    """
    if source == "option_pnl":
        return (
            config.SIGNALS_PATTERN_CALIBRATION_FLOOR_OPTION_PNL,
            config.SIGNALS_PATTERN_CALIBRATION_CEIL_OPTION_PNL,
        )
    return (
        config.SIGNALS_PATTERN_CALIBRATION_FLOOR,
        config.SIGNALS_PATTERN_CALIBRATION_CEIL,
    )


def _clamp(value: float, source: Optional[str] = None) -> float:
    lo, hi = _band_for_source(source)
    return min(max(value, lo), hi)


@dataclass
class CalibrationStore:
    """In-memory map of calibrated bases, keyed by (pattern, underlying).

    ``by_pair`` holds the per-underlying calibrated base; ``by_pattern`` holds
    a sample-weighted pattern-wide fallback used when a specific underlying has
    no trustworthy window.
    """

    by_pair: dict[tuple[str, str], float] = field(default_factory=dict)
    by_pattern: dict[str, float] = field(default_factory=dict)
    loaded_at: float = 0.0

    def lookup(self, pattern_id: str, underlying: str) -> Optional[float]:
        if not pattern_id:
            return None
        key = (pattern_id, (underlying or "").upper())
        if key in self.by_pair:
            return self.by_pair[key]
        return self.by_pattern.get(pattern_id)


# Process-global active store + the lock guarding (re)assignment.
_active: Optional[CalibrationStore] = None
_lock = threading.Lock()


def set_active_store(store: Optional[CalibrationStore]) -> None:
    """Install (or clear) the active store. Used by the refresh path + tests."""
    global _active
    with _lock:
        _active = store


def active_store() -> Optional[CalibrationStore]:
    return _active


def calibrated_base(pattern_id: str, underlying: str, fallback: float) -> float:
    """Return the calibrated base for a pattern, or ``fallback`` (the prior).

    This is the hot-path consult used by ``PatternBase.compute_confidence``. It
    is a pure in-memory lookup: no DB, no I/O. Returns ``fallback`` unchanged
    whenever calibration is disabled or no trustworthy measurement exists, so
    enabling/disabling the feature is behavior-preserving by construction.
    """
    if not config.SIGNALS_PATTERN_CALIBRATION_ENABLED:
        return fallback
    store = _active
    if store is None:
        return fallback
    value = store.lookup(pattern_id, underlying)
    return value if value is not None else fallback


def build_store_from_rows(
    rows,
    *,
    source: Optional[str] = None,
    exclude_pairs: Optional[set[tuple[str, str]]] = None,
) -> CalibrationStore:
    """Construct a :class:`CalibrationStore` from stats rows.

    Each row is ``(pattern, underlying, window_end, n_resolved, proposed_base)``
    — the latest window per (pattern, underlying). Rows that fail the
    sample-size or freshness gates are dropped. The pattern-wide fallback is a
    resolved-count-weighted mean of the surviving per-underlying bases.

    ``source`` selects the clamp band (``option_pnl`` has its own overridable
    band; everything else uses the global one).

    ``exclude_pairs`` drops specific (pattern, underlying) rows before any
    aggregation — used by the auto-source disagreement veto to keep an inflated
    touch reading from contributing to either the per-pair store OR the
    sample-weighted pattern-wide fallback.
    """
    min_samples = config.SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES
    max_age = timedelta(days=config.SIGNALS_PATTERN_CALIBRATION_MAX_AGE_DAYS)
    today = date.today()

    by_pair: dict[tuple[str, str], float] = {}
    agg: dict[str, list[tuple[float, int]]] = {}
    for pattern, underlying, window_end, n_resolved, proposed_base in rows:
        if proposed_base is None or n_resolved is None:
            continue
        if int(n_resolved) < min_samples:
            continue
        if window_end is not None and (today - window_end) > max_age:
            continue
        key = (pattern, (underlying or "").upper())
        if exclude_pairs and key in exclude_pairs:
            continue
        base = _clamp(float(proposed_base), source)
        by_pair[key] = base
        agg.setdefault(pattern, []).append((base, int(n_resolved)))

    by_pattern: dict[str, float] = {}
    for pattern, samples in agg.items():
        total_n = sum(n for _, n in samples)
        if total_n <= 0:
            continue
        by_pattern[pattern] = _clamp(
            sum(b * n for b, n in samples) / total_n, source
        )

    return CalibrationStore(by_pair=by_pair, by_pattern=by_pattern, loaded_at=time.time())


_VALID_SOURCES = ("underlying_touch", "option_pnl")


def _load_rows(conn, source: str):
    """Latest stats window per (pattern, underlying) for one measurement source."""
    cur = conn.cursor()
    # DISTINCT ON keeps the most-recently-computed window for each pair.
    cur.execute(
        """
        SELECT DISTINCT ON (pattern, underlying)
               pattern, underlying, window_end, n_resolved, proposed_base
        FROM playbook_pattern_stats
        WHERE source = %s
        ORDER BY pattern, underlying, window_end DESC, computed_at DESC
        """,
        (source,),
    )
    return cur.fetchall()


def _merge_prefer(base: CalibrationStore, preferred: CalibrationStore) -> CalibrationStore:
    """Overlay ``preferred`` onto ``base`` per key.

    Both stores have already passed the sample-size / freshness gates in
    :func:`build_store_from_rows`, so a key only appears when trustworthy.
    Overlaying therefore yields "preferred when it has a trustworthy window,
    else the base" — the auto-source fallback.
    """
    return CalibrationStore(
        by_pair={**base.by_pair, **preferred.by_pair},
        by_pattern={**base.by_pattern, **preferred.by_pattern},
        loaded_at=max(base.loaded_at, preferred.loaded_at),
    )


def auto_vetoed_pairs(touch_rows, pnl_rows) -> set[tuple[str, str]]:
    """(pattern, underlying) pairs whose touch base is vetoed under SOURCE=auto.

    Why: a pair whose ``option_pnl`` window is below ``MIN_SAMPLES`` currently
    leaves the touch base in place, and the touch proxy can overstate edge — a
    target/stop hit is not a profitable trade. When the sub-gate pnl reading
    materially disagrees with touch, the safe move is to drop the touch base so
    the live consult falls through to the catalog prior.

    Asymmetric and one-directional: only fires when ``touch_base − pnl_base ≥
    threshold`` (touch is HIGHER than pnl by at least the threshold — the
    unsafe direction). Touch under-stating relative to pnl is the conservative
    side and is left alone. Both sides are clamped to their respective bands
    before the comparison so the check tracks what the live engine would
    actually use, not raw beta-smoothed values.

    Returns an empty set when either knob is disabled
    (``THRESHOLD == 0`` or ``SOFT_MIN_SAMPLES == 0``).
    """
    threshold = config.SIGNALS_PATTERN_CALIBRATION_AUTO_DISAGREEMENT_THRESHOLD
    soft_min = config.SIGNALS_PATTERN_CALIBRATION_AUTO_PNL_SOFT_MIN_SAMPLES
    if threshold <= 0 or soft_min <= 0:
        return set()

    max_age = timedelta(days=config.SIGNALS_PATTERN_CALIBRATION_MAX_AGE_DAYS)
    today = date.today()
    pnl_band = _band_for_source("option_pnl")
    touch_band = _band_for_source("underlying_touch")

    touch_index: dict[tuple[str, str], float] = {}
    for pattern, underlying, _we, n_resolved, proposed_base in touch_rows:
        if proposed_base is None or n_resolved is None:
            continue
        key = (pattern, (underlying or "").upper())
        clamped = min(max(float(proposed_base), touch_band[0]), touch_band[1])
        touch_index[key] = clamped

    vetoed: set[tuple[str, str]] = set()
    for pattern, underlying, window_end, n_resolved, proposed_base in pnl_rows:
        if proposed_base is None or n_resolved is None:
            continue
        if int(n_resolved) < soft_min:
            continue
        if window_end is not None and (today - window_end) > max_age:
            continue
        key = (pattern, (underlying or "").upper())
        if key not in touch_index:
            continue
        pnl_clamped = min(max(float(proposed_base), pnl_band[0]), pnl_band[1])
        if touch_index[key] - pnl_clamped >= threshold:
            vetoed.add(key)
    return vetoed


def load_store(conn) -> CalibrationStore:
    """Load the calibration store, honoring ``SIGNALS_PATTERN_CALIBRATION_SOURCE``.

    * a concrete source ('underlying_touch' | 'option_pnl') loads only that
      harness's rows;
    * 'auto' loads both and prefers 'option_pnl' per (pattern, underlying),
      falling back to 'underlying_touch' where no trustworthy P&L window exists.
      Under 'auto' a sub-gate pnl reading that strongly disagrees with the
      touch base vetoes that touch pair via :func:`auto_vetoed_pairs`, so the
      live consult falls through to the catalog prior rather than inheriting an
      inflated touch overshoot.
    """
    source = config.SIGNALS_PATTERN_CALIBRATION_SOURCE
    if source == "auto":
        touch_rows = _load_rows(conn, "underlying_touch")
        pnl_rows = _load_rows(conn, "option_pnl")
        vetoed = auto_vetoed_pairs(touch_rows, pnl_rows)
        if vetoed:
            logger.info(
                "pattern calibration: auto-source vetoed %d touch pair(s) "
                "(touch − pnl ≥ %.2f, n ≥ %d): %s",
                len(vetoed),
                config.SIGNALS_PATTERN_CALIBRATION_AUTO_DISAGREEMENT_THRESHOLD,
                config.SIGNALS_PATTERN_CALIBRATION_AUTO_PNL_SOFT_MIN_SAMPLES,
                ", ".join(f"{p}/{u}" for p, u in sorted(vetoed)),
            )
        touch = build_store_from_rows(
            touch_rows, source="underlying_touch", exclude_pairs=vetoed,
        )
        pnl = build_store_from_rows(pnl_rows, source="option_pnl")
        return _merge_prefer(touch, pnl)
    if source not in _VALID_SOURCES:
        logger.warning(
            "unknown SIGNALS_PATTERN_CALIBRATION_SOURCE %r; using 'underlying_touch'",
            source,
        )
        source = "underlying_touch"
    return build_store_from_rows(_load_rows(conn, source), source=source)


def maybe_refresh(ttl_seconds: Optional[int] = None) -> None:
    """Reload the active store from the DB if the TTL has elapsed.

    Called once per cycle by the long-running signals service. Cheap no-op
    until ``ttl_seconds`` has passed since the last successful load. Fully
    best-effort: any failure leaves the previous store (or the priors) in
    place and is logged, never raised.
    """
    if not config.SIGNALS_PATTERN_CALIBRATION_ENABLED:
        return
    if ttl_seconds is None:
        ttl_seconds = config.SIGNALS_PATTERN_CALIBRATION_REFRESH_SECONDS
    ttl = ttl_seconds
    store = _active
    if store is not None and (time.time() - store.loaded_at) < ttl:
        return
    try:
        from src.database.connection import close_db_connection, get_db_connection

        conn = get_db_connection()
        try:
            new_store = load_store(conn)
        finally:
            close_db_connection(conn)
        set_active_store(new_store)
        logger.info(
            "pattern calibration: refreshed store (%d pairs, %d pattern-wide)",
            len(new_store.by_pair),
            len(new_store.by_pattern),
        )
    except Exception:  # noqa: BLE001 - calibration must never break a signal cycle
        logger.warning("pattern calibration: refresh failed; keeping prior store", exc_info=True)
