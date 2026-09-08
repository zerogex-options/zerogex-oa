"""Every research knob in one auditable, hashable place.

Two things this module is responsible for beyond holding values:

* **Nothing else may hold a constant.**  A threshold that lives in a function
  body cannot appear in ``.meta.json``, which means a result cannot be
  reproduced from its own provenance.  If it changes an answer, it belongs
  here.
* **A run's identity is its config.**  :meth:`ResearchConfig.fingerprint`
  hashes the whole dataclass, and every dataset/report carries it.  That is
  what stops a cached artefact built under one parameter set from being read
  back under another — the failure the brief asks for a test against.

Defaults are inherited from production wherever production has an opinion, so
a number here is not quietly measuring something different from what the
product draws:

* ``session_start`` / ``session_end`` — ``src/analytics/wall_breaks.py``.
* ``touch_tolerance_bp`` — ``src/jobs/level_history.TOUCH_PCT`` (5 bp) is the
  product's "did price test this level" band; we tighten to 2 bp because an
  extension is a computed price rather than a strike, so there is no
  strike-grid quantisation to absorb.  Both are swept.
* ``client_poll_lag_seconds`` — the indicator's minimum poll interval, and the
  reason a level is "about a minute behind" (source email, 2026-09-07).
* ``snapshot_cadence_seconds`` — ``ANALYTICS_INTERVAL`` (``src/config.py:1806``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from datetime import time
from typing import Any, Optional

__all__ = [
    "CLOCK_DATA",
    "CLOCK_PUBLISHED",
    "CLOCK_VISIBLE",
    "AVAILABILITY_CLOCKS",
    "MODE_BOUNDARY",
    "MODE_OPEN",
    "EXTENSION_MODES",
    "DEFAULT_LEAD_SECONDS",
    "DEFAULT_OUTCOME_HORIZONS",
    "ResearchConfig",
]

# ── The availability clock ───────────────────────────────────────────
#
# Which instant counts as "the gamma level existed".  This is the single most
# consequential setting in the package, because the whole study is a claim
# about what was knowable before price arrived.

#: ``gex_summary.timestamp`` — the option-chain instant the levels were
#: computed FROM.  Optimistic: the level did not exist yet at this instant,
#: the engine had not run.  Kept for sensitivity reporting only.
CLOCK_DATA = "data"

#: ``gex_summary.created_at`` — when the row hit the database.  "The platform
#: knew it."
CLOCK_PUBLISHED = "published"

#: ``created_at + client_poll_lag_seconds`` — when a trader's chart could have
#: drawn it.  The honest default: it is the only clock under which a signal is
#: something a human or a bot could actually have acted on.
CLOCK_VISIBLE = "visible"

AVAILABILITY_CLOCKS: tuple[str, ...] = (CLOCK_DATA, CLOCK_PUBLISHED, CLOCK_VISIBLE)

# ── Extension ladder construction ────────────────────────────────────

#: Mode A — extensions project from the opening-range BOUNDARIES.
#: Up: ``ORH + k*R``.  Down: ``ORL - k*R``.
MODE_BOUNDARY = "boundary"

#: Mode B — extensions project from the session OPENING PRICE.
#: Up: ``open + k*R``.  Down: ``open - k*R``.
MODE_OPEN = "open"

EXTENSION_MODES: tuple[str, ...] = (MODE_BOUNDARY, MODE_OPEN)

#: Lead times swept by default, in seconds.  30 is deliberately included even
#: though it is BELOW the 60 s snapshot cadence: it should behave almost
#: identically to 0, and a sweep in which it does not is evidence of a bug in
#: the clock handling rather than a finding about gamma.
DEFAULT_LEAD_SECONDS: tuple[int, ...] = (0, 30, 60, 120, 180)

#: Forward horizons in minutes for excursion measurement.  ``rest_of_session``
#: is added separately by :mod:`.outcomes` and covers the brief's "EOD".
DEFAULT_OUTCOME_HORIZONS: tuple[int, ...] = (1, 3, 5, 10, 15, 30)

#: Confluence distance buckets, in POINTS, for the NQ-scale report.  These are
#: REPORTING buckets only — the dataset stores raw distances and the analysis
#: layer applies these edges, so changing them never requires a rebuild and no
#: single value is baked in as "the" threshold.
DEFAULT_CONFLUENCE_BUCKETS_PTS: tuple[float, ...] = (2.0, 5.0, 10.0, 15.0, 20.0)

#: The same buckets scale-free, so SPY (~$600) and NDX (~$29,000) are asking
#: one question rather than six.  10 points on NQ ~= 3.4 bp.
DEFAULT_CONFLUENCE_BUCKETS_BP: tuple[float, ...] = (1.0, 2.0, 3.5, 5.0, 7.0)


def _round(value: float, places: int = 10) -> float:
    """Kill float noise so two configs that mean the same thing hash the same."""
    return round(float(value), places)


@dataclass(frozen=True)
class ResearchConfig:
    """One fully-specified run.

    Immutable on purpose: a run's parameters cannot drift while it executes,
    and :meth:`fingerprint` is therefore stable for the run's whole life.
    Derive a variant with :meth:`variant` rather than mutating.
    """

    # ── Session ──────────────────────────────────────────────────────
    #: Regular cash session, ET.  Everything is normalised to this window;
    #: futures bars outside it are discarded rather than folded in, so ES/NQ
    #: and SPX/NDX answer the same question.
    session_start: time = time(9, 30)
    session_end: time = time(16, 0)

    # ── Opening range ────────────────────────────────────────────────
    #: Length of the opening range in minutes, measured from ``session_start``.
    #: Swept over 5 / 15 / 30.
    opening_range_minutes: int = 5
    #: Minimum OR width, in basis points of the opening price, for a session to
    #: be usable.  A degenerate range makes every extension collapse onto the
    #: same price and the ladder meaningless.
    min_or_width_bp: float = 2.0
    #: Minimum bars required inside the OR window.  A 5-minute OR built from
    #: one bar is not an opening range.
    min_or_bars: int = 2

    # ── Extension ladder ─────────────────────────────────────────────
    extension_mode: str = MODE_BOUNDARY
    #: Ladder increment in units of R (the OR width).  0.5 == the brief's 50%.
    extension_step: float = 0.5
    #: Furthest ladder rung in units of R.  10.0 == the brief's +/-1000%.
    max_extension: float = 10.0

    # ── Touch detection ──────────────────────────────────────────────
    #: How close price must come to count as reaching an extension, in basis
    #: points of the level.
    #:
    #: This must stay TIGHTER than the tightest confluence bucket, or the
    #: touch band swallows the measurement: at 2 bp on NQ (~5.9 points) the
    #: statement "price reached the extension" would be fuzzier than "a gamma
    #: level sits within 2 points of it", and every confluence distance would
    #: inherit the touch slop.  1 bp is ~2.9 points on NQ, ~$0.06 on SPY, and
    #: is swept over 0.5 / 1 / 2 / 5.
    #:
    #: Deliberately tighter than production's 5 bp
    #: (``src/jobs/level_history.TOUCH_PCT``): that band is sized for STRIKES,
    #: where "testing the wall" is inherently fuzzy.  An extension is an exact
    #: computed price with no grid to absorb.
    touch_tolerance_bp: float = 1.0
    #: Absolute floor under the touch band, in the instrument's price units, so
    #: a low-priced underlying keeps a band wider than one tick.  ``None``
    #: resolves per-instrument to one tick.
    touch_tolerance_min: Optional[float] = None
    #: Minutes a spent extension must wait before it can fire again.  ``None``
    #: (the default) means FIRST TOUCH ONLY — a rung fires at most once per
    #: session.  That is the cleanest reading of the brief's dependent
    #: variable; the re-arm path exists so the choice can be tested rather
    #: than assumed.
    rearm_minutes: Optional[int] = None
    #: When re-arming is enabled, price must also travel this far from the
    #: level (in units of R) before the rung is eligible again.  Without it a
    #: slow grind re-fires on the cooldown clock alone.
    rearm_distance_r: float = 0.25

    # ── Gamma availability (the anti-look-ahead core) ────────────────
    availability_clock: str = CLOCK_VISIBLE
    #: Added to ``created_at`` under ``CLOCK_VISIBLE``.  30 s is the
    #: indicator's own minimum poll interval.
    client_poll_lag_seconds: int = 30
    #: A gamma level counts as pre-existing only if it was available at least
    #: this many seconds before the touch.
    gamma_min_lead_seconds: int = 120
    #: Published cadence of the analytics engine, seconds.  Used to sanity-
    #: check a session's frame density, never to interpolate.
    snapshot_cadence_seconds: int = 60
    #: A snapshot whose publish lag exceeds this is treated as a backfilled or
    #: pathological row and the session FAILS CLOSED.  Twenty minutes is far
    #: beyond any healthy compute + write, and far below the hours-to-days gap
    #: a backfill leaves.
    max_publish_lag_seconds: int = 1200
    #: Sessions with fewer usable gamma frames than this are dropped whole.
    #: A handful of scattered frames cannot distinguish "the level sat there
    #: all morning" from "we sampled it twice".
    min_session_frames: int = 60
    #: Depth of the recomputed GEX rank ladder per side (C1..Cn / P1..Pn).
    gex_ladder_depth: int = 10
    #: Include the ranked-GEX level source at all.  Requires ``gex_by_strike``,
    #: which is retention-pruned; a run without it still produces every
    #: wall / flip / max-pain / pin cohort.
    use_gex_ranks: bool = True

    # ── Confluence reporting ─────────────────────────────────────────
    confluence_buckets_pts: tuple[float, ...] = DEFAULT_CONFLUENCE_BUCKETS_PTS
    confluence_buckets_bp: tuple[float, ...] = DEFAULT_CONFLUENCE_BUCKETS_BP

    # ── Trend / regime filters (research dimensions, not truths) ─────
    #: ``none`` | ``ema_slope`` | ``hma`` | ``vwap_slope`` | ``trade_bias``.
    #: Recorded on every event regardless; this selects which one a COHORT
    #: splits on.
    trend_filter: str = "none"
    trend_lookback_minutes: int = 30
    #: A trend move smaller than this reads as flat.  Matches
    #: ``impliedDirection.ts`` FLAT_PCT (0.05%).
    trend_flat_bp: float = 5.0
    hma_period: int = 21
    ema_period: int = 21

    # ── Outcome measurement ──────────────────────────────────────────
    outcome_horizons: tuple[int, ...] = DEFAULT_OUTCOME_HORIZONS
    #: Bars used to estimate ATR for distance normalisation.
    atr_period: int = 14

    # ── Cohort thresholds ────────────────────────────────────────────
    #: Rungs at or beyond this |k| count as "extreme" for the reversion arm.
    min_extension_for_reversion: float = 2.0
    #: Consecutive rungs broken without a meaningful reaction before the
    #: continuation arm considers the ladder "not respected".
    min_failed_extensions_for_continuation: int = 2
    #: A rung counts as RESPECTED if, after its touch, price retraced at least
    #: this fraction of a step before continuing.
    respect_retrace_frac: float = 0.5

    # ── Out-of-sample split (chronological, never shuffled) ──────────
    discovery_frac: float = 0.6
    validation_frac: float = 0.2
    # The remainder is the untouched final test set.

    # ── Transaction costs (Phase 4) ──────────────────────────────────
    #: Slippage in TICKS per side.  Points/dollars are derived per instrument
    #: so one number means the same thing on ES and NQ.
    slippage_ticks: float = 1.0
    #: Round-turn commission + exchange/NFA fees, per contract, in dollars.
    commission_round_turn: float = 4.50
    #: Bars of delay between the confirmation bar and the entry fill.  Zero
    #: would fill at the signal bar's close, which nothing can do.
    entry_delay_bars: int = 1

    # ── Provenance ───────────────────────────────────────────────────
    #: Free-text label carried into ``.meta.json``.  Not part of the
    #: fingerprint — relabelling a run must not invalidate its cache.
    label: str = ""

    def __post_init__(self) -> None:
        if self.availability_clock not in AVAILABILITY_CLOCKS:
            raise ValueError(
                f"availability_clock={self.availability_clock!r}; "
                f"expected one of {AVAILABILITY_CLOCKS}"
            )
        if self.extension_mode not in EXTENSION_MODES:
            raise ValueError(
                f"extension_mode={self.extension_mode!r}; expected one of {EXTENSION_MODES}"
            )
        if self.opening_range_minutes <= 0:
            raise ValueError("opening_range_minutes must be positive")
        if self.extension_step <= 0:
            raise ValueError("extension_step must be positive")
        if self.max_extension < self.extension_step:
            raise ValueError("max_extension must be at least one step")
        if self.gamma_min_lead_seconds < 0:
            raise ValueError("gamma_min_lead_seconds must not be negative")
        if self.client_poll_lag_seconds < 0:
            raise ValueError("client_poll_lag_seconds must not be negative")
        if self.session_start >= self.session_end:
            raise ValueError("session_start must precede session_end")
        if not 0.0 < self.discovery_frac < 1.0:
            raise ValueError("discovery_frac must be in (0, 1)")
        if not 0.0 <= self.validation_frac < 1.0:
            raise ValueError("validation_frac must be in [0, 1)")
        if self.discovery_frac + self.validation_frac >= 1.0:
            raise ValueError("discovery_frac + validation_frac must leave a non-empty holdout")
        if self.rearm_minutes is not None and self.rearm_minutes < 0:
            raise ValueError("rearm_minutes must not be negative")

    # ── Derived ──────────────────────────────────────────────────────

    @property
    def first_touch_only(self) -> bool:
        """True when a ladder rung fires at most once per session."""
        return self.rearm_minutes is None

    @property
    def test_frac(self) -> float:
        """The untouched final holdout fraction."""
        return _round(1.0 - self.discovery_frac - self.validation_frac)

    def rungs(self) -> list[float]:
        """Ladder multiples of R, ascending, excluding 0.

        ``0`` is the ladder ANCHOR (the OR boundary, or the opening price in
        Mode B) rather than a rung: it is never a touch event, but it is the
        "previous extension" for the first rung, so it has to exist in the
        price ladder.  :mod:`.ranges` adds it.
        """
        out: list[float] = []
        # Integer stepping, so 0.1 + 0.2 style drift cannot shorten the ladder.
        i = 1
        while True:
            k = _round(self.extension_step * i)
            if k > self.max_extension + 1e-9:
                break
            out.append(k)
            i += 1
        return out

    def tolerance(self, level: float, tick: Optional[float] = None) -> float:
        """Touch band around ``level`` in the instrument's price units."""
        floor = self.touch_tolerance_min
        if floor is None:
            floor = tick if tick and tick > 0 else 0.0
        return max(abs(level) * self.touch_tolerance_bp / 10_000.0, float(floor))

    def variant(self, **changes: Any) -> "ResearchConfig":
        """A copy with ``changes`` applied — the only supported way to vary."""
        return replace(self, **changes)

    # ── Provenance ───────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe view.  ``time`` and tuples are normalised for stability."""
        raw = asdict(self)
        out: dict[str, Any] = {}
        for key, value in raw.items():
            if isinstance(value, time):
                out[key] = value.isoformat()
            elif isinstance(value, tuple):
                out[key] = list(value)
            elif isinstance(value, float):
                out[key] = _round(value)
            else:
                out[key] = value
        return out

    def fingerprint(self) -> str:
        """Stable 16-hex digest of everything that can change an answer.

        ``label`` is excluded — it is documentation, not a parameter — so a
        run can be renamed without orphaning its cached dataset.  Everything
        else is in, including the fields a caller might think are cosmetic:
        ``confluence_buckets_*`` shape the report, and a report is an answer.
        """
        payload = self.to_dict()
        payload.pop("label", None)
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]

    def describe(self) -> str:
        """One line for a log or a report header."""
        lead = f"lead>={self.gamma_min_lead_seconds}s@{self.availability_clock}"
        return (
            f"OR={self.opening_range_minutes}m mode={self.extension_mode} "
            f"step={self.extension_step}R max={self.max_extension}R "
            f"touch={self.touch_tolerance_bp}bp {lead} "
            f"[{self.fingerprint()}]"
        )
