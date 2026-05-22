"""Regression tests for stale-state handoff across the 09:30 ET vendor reset.

Background
----------
TradeStation's per-contract cumulative ``Volume`` resets to 0 at 09:30 ET
(cash open).  Two pieces of in-process state span that boundary on a
continuously-running engine:

  1. :class:`src.ingestion.stream_manager.OptionStreamAccumulator._state`
     — a per-symbol dict of merged quote fields.  The merger has a
     load-bearing patch at ``stream_manager.py:421-430`` that *only*
     overwrites the stored ``Volume`` when the incoming value is > 0.
     Without that patch, frequent ``Volume=0`` stream updates between
     trades would erase the running cumulative.
  2. The per-contract :class:`src.ingestion.main_engine._FlowAccumulator`
     watermark.  ``_ingest_snapshot_into_accumulator`` has a load-bearing
     re-anchor at ``main_engine.py:966-967`` that resets the watermark
     when ``curr_vol < acc.last_volume_cum`` — the only known cause is a
     vendor reset.  Existing regression coverage at
     ``test_ingestion_volume_classification.py::
     test_accumulator_reanchors_watermark_on_vendor_cumulative_reset``.

These tests pin the layer-1 (stream merger) behavior that the layer-2
(_FlowAccumulator) recovery depends on, and document the residual
hand-off between them at the 09:30 ET boundary.  See
``docs/architecture/volume-tracking-review.md`` section 3.2 row 5 for
the broader analysis of why this hand-off matters for the
``flow_contract_facts`` LAG-delta consumer.
"""

from __future__ import annotations

import threading
from datetime import date, datetime
from typing import Any, Dict

import pytz

from src.ingestion.main_engine import IngestionEngine, _FlowAccumulator
from src.ingestion.stream_manager import OptionStreamAccumulator

ET = pytz.timezone("US/Eastern")

SYM = "SPY260515P00739000"


# ---------------------------------------------------------------------------
# Helpers — bypass __init__ for both the merger and the engine so the tests
# stay free of network / DB / config wiring (same pattern as
# tests/test_ingestion_volume_baseline.py and
# tests/test_ingestion_volume_classification.py).
# ---------------------------------------------------------------------------


def _merger() -> OptionStreamAccumulator:
    acc = OptionStreamAccumulator.__new__(OptionStreamAccumulator)
    acc._state = {}
    acc._lock = threading.Lock()
    acc._dirty = set()
    acc._updates_received = 0
    acc._wakeup = None
    return acc


def _engine() -> IngestionEngine:
    e = IngestionEngine.__new__(IngestionEngine)
    e.options_buffer = {}
    e._option_flow = {}
    e._option_flow_lock = threading.Lock()
    e._option_bucket_last_write = {}
    e._classify_fallback_count = 0
    e.errors_count = 0
    return e


def _snap(
    volume: int, *, last: float = 5.70, bid: float = 5.66, ask: float = 5.70
) -> Dict[str, Any]:
    return {
        "option_symbol": SYM,
        "underlying": "SPY",
        "strike": 739.0,
        "expiration": "2026-05-15",
        "option_type": "P",
        "last": last,
        "bid": bid,
        "ask": ask,
        "mid": (bid + ask) / 2.0,
        "volume": volume,
        "open_interest": 100,
        "implied_volatility": 0.3,
        "delta": -0.5,
        "gamma": 0.01,
        "theta": -0.1,
        "vega": 0.2,
    }


# ---------------------------------------------------------------------------
# Layer 1: OptionStreamAccumulator merge semantics
# ---------------------------------------------------------------------------


def test_stream_merger_zero_volume_does_not_clobber_prior_cumulative():
    """``Volume=0`` quote updates are common between trades (NBBO changes
    without a print).  The merger MUST keep the prior accumulated Volume,
    otherwise the running cumulative would be erased on every such update.

    Pins the patch at ``stream_manager.py:421-430``."""
    m = _merger()
    # Prior state: yesterday's last quote left cumulative at 1000.
    m._state[SYM] = {"Symbol": SYM, "Volume": 1000, "Last": 5.70, "Bid": 5.66, "Ask": 5.70}

    # Incoming stream message has Volume=0 (no trade since last update).
    m._merge_single_quote({"Symbol": SYM, "Volume": 0, "Last": 5.70, "Bid": 5.66, "Ask": 5.70})

    assert m._state[SYM]["Volume"] == 1000, (
        "merger must preserve prior Volume when incoming Volume=0; "
        "otherwise the running cumulative is erased between trades"
    )


def test_stream_merger_positive_volume_overwrites_prior_cumulative():
    """Once a real trade arrives (Volume > 0), the merger overwrites.
    Inverse of the zero-clobber test — this is the path that lets the
    post-reset cumulative replace the stale prior-session value once a
    new-session trade prints."""
    m = _merger()
    m._state[SYM] = {"Symbol": SYM, "Volume": 1000, "Last": 5.70, "Bid": 5.66, "Ask": 5.70}

    # Post-reset first trade: TS publishes Volume=5 (5 contracts since
    # 09:30 ET reset, which is now strictly less than the stale prior
    # value).  Merger does NOT compare to prior; it just requires > 0.
    m._merge_single_quote({"Symbol": SYM, "Volume": 5, "Last": 5.70, "Bid": 5.66, "Ask": 5.70})

    assert m._state[SYM]["Volume"] == 5, (
        "merger must overwrite stored Volume with any incoming Volume > 0, "
        "including a smaller value (vendor reset is detected downstream by "
        "the _FlowAccumulator watermark re-anchor, not here)"
    )


def test_stream_merger_dirty_set_includes_symbol_even_on_zero_volume_update():
    """The merger marks the symbol dirty on EVERY update, including
    ``Volume=0``.  That means a quote-only update (no trade) still
    triggers a drain → engine → ``_ingest_snapshot_into_accumulator``
    cycle that observes the *preserved* prior Volume value.  Pins the
    behavior at ``stream_manager.py:452-453``."""
    m = _merger()
    m._state[SYM] = {"Symbol": SYM, "Volume": 1000, "Last": 5.70, "Bid": 5.66, "Ask": 5.70}
    m._merge_single_quote({"Symbol": SYM, "Volume": 0, "Last": 5.71, "Bid": 5.66, "Ask": 5.71})

    drained = m.drain()
    assert SYM in drained, "Volume=0 update must still mark dirty so price-only changes propagate"
    # Confirm the drained payload carries the preserved prior Volume.
    assert drained[SYM]["Volume"] == 1000


# ---------------------------------------------------------------------------
# Layer 2: _FlowAccumulator behavior under the stale-handoff scenario
# ---------------------------------------------------------------------------


def test_accumulator_treats_first_session_snapshot_as_full_delta_relative_to_watermark():
    """Cold-start (hydrate returned zeros) accumulator sees a snapshot with
    Volume=N.  ``vol_delta = max(N - 0, 0) = N``.  The full N is classified
    and added to the cumulative classified totals.

    If the merger handed off a stale prior-session value (e.g. N=1000 at
    01:00 ET because the engine ran continuously across midnight and the
    merger has not yet seen the 09:30 vendor reset), this absorbs the
    prior session's residual into today's first-bucket classified flow.
    Layer-3 mitigation (re-fetching via REST seed at session open, or
    daily process restart) is what defends against this in production.

    This test does NOT claim the resulting classification is *correct* —
    it pins what the accumulator currently does so any future fix that
    changes the behavior is caught explicitly.  See
    docs/architecture/volume-tracking-review.md §3.2 row 5.
    """
    e = _engine()
    bucket = ET.localize(datetime(2026, 4, 28, 10, 15))

    # Fresh accumulator from cold-start hydrate (no rows yet today).
    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
        # No prior NBBO in the accumulator -- classifier will fall through
        # to the snapshot's own bid/ask, same as cold-start at any session.
    )

    # Stale snapshot handed off by the merger: Volume=1000, last at ask.
    e._ingest_snapshot_into_accumulator(acc, _snap(volume=1000), bucket)

    # vol_delta = 1000 - 0 = 1000; classifier sees last=ask → ask_volume.
    assert acc.last_volume_cum == 1000
    assert acc.ask_cum + acc.mid_cum + acc.bid_cum == 1000, (
        "cold-start accumulator absorbs the full snapshot Volume as a "
        "delta -- ALL of it gets classified.  If the snapshot value was a "
        "stale prior-session cumulative carried by the merger across "
        "00:00 ET, this attributes the prior day's residual to today's "
        "first bucket.  Documented in "
        "docs/architecture/volume-tracking-review.md §3.2 row 5."
    )


def test_accumulator_reanchors_then_classifies_post_reset_trade_after_stale_handoff():
    """End-to-end of the stale-handoff scenario from layer 1 → layer 2:

    1. Merger holds Volume=1000 (prior-session residual).
    2. Accumulator (cold-started today) ingests it -> last_volume_cum=1000,
       full 1000 classified.
    3. First post-reset trade arrives with Volume=5.
    4. Accumulator detects curr_vol(5) < watermark(1000), re-anchors
       watermark to 0, vol_delta=5, classifies the 5-contract trade.

    The accumulator's in-memory state at the end is correct for the
    *post-reset* portion (vol_delta=5 attributed to the trade bucket),
    but its classified totals still carry the 1000 absorbed in step 2.
    Pins both halves so any future fix has a clean delta to assert
    against."""
    e = _engine()
    pre_open_bucket = ET.localize(datetime(2026, 4, 28, 9, 0))
    post_open_bucket = ET.localize(datetime(2026, 4, 28, 9, 35))

    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )

    # Step 1+2: stale handoff absorbed.
    e._ingest_snapshot_into_accumulator(acc, _snap(volume=1000), pre_open_bucket)
    assert acc.last_volume_cum == 1000
    classified_after_stale = acc.ask_cum + acc.mid_cum + acc.bid_cum
    assert classified_after_stale == 1000

    # Step 3+4: post-reset trade.
    e._ingest_snapshot_into_accumulator(acc, _snap(volume=5), post_open_bucket)

    # Watermark re-anchored to the post-reset cumulative, NOT to the
    # stale 1000.  vol_delta computed against the re-anchored watermark
    # (5 - 0 = 5), classified, and accumulated.
    assert acc.last_volume_cum == 5, "watermark must re-anchor on vendor reset"
    assert acc.ask_cum + acc.mid_cum + acc.bid_cum == 1005, (
        "classified totals stay monotonic across the reset (existing "
        "contract from "
        "test_accumulator_reanchors_watermark_on_vendor_cumulative_reset). "
        "The stale 1000 absorbed in the pre-open bucket is still in the "
        "cumulative -- only the watermark is re-anchored, not the "
        "classified columns."
    )


# ---------------------------------------------------------------------------
# UPSERT contract: the option_chains.volume column under stale-handoff
# ---------------------------------------------------------------------------


def test_option_chains_volume_upsert_uses_greatest_and_can_lock_in_stale_value():
    """The ``option_chains`` UPSERT at ``main_engine.py:1110`` uses
    ``GREATEST(option_chains.volume, EXCLUDED.volume)`` for idempotency
    under retry / replay.  That same GREATEST makes the column NEVER
    decrease, so if a stale-handoff row got persisted with Volume=1000
    at, say, 09:00 ET (pre-open), and then a post-reset bucket at
    09:35 ET tries to UPSERT with Volume=5 (the correct post-reset
    cumulative from the re-anchored accumulator), the stored value
    stays at 1000.

    This test models the UPSERT semantics in Python (no DB).  It pins
    the implication: any fix to the stale-handoff scenario must address
    BOTH the in-memory state (already correct -- see
    test_accumulator_reanchors_*) AND the already-persisted row.  See
    docs/architecture/volume-tracking-review.md §3.3 (load-bearing
    patch: GREATEST on every cumulative column).
    """

    def upsert_volume(stored: int, incoming: int) -> int:
        # Mirror of: volume = GREATEST(option_chains.volume, EXCLUDED.volume)
        return max(stored, incoming)

    # 09:00 ET pre-open bucket: stale-handoff snapshot persisted.
    stored = upsert_volume(0, 1000)
    assert stored == 1000

    # 09:35 ET first-trade bucket: accumulator has re-anchored, the row
    # the engine submits carries Volume=5 (the correct post-reset cum).
    stored = upsert_volume(stored, 5)
    assert stored == 1000, (
        "GREATEST upsert blocks the post-reset cumulative from "
        "displacing the stale pre-open value.  This is by design "
        "for idempotency under replay; the cost is that a stale-handoff "
        "row cannot be self-corrected by a subsequent smaller-cumulative "
        "write.  Documented in "
        "docs/architecture/volume-tracking-review.md §3.2 row 5."
    )


# ---------------------------------------------------------------------------
# Sanity check: when the merger is properly seeded post-reset, the whole
# chain works correctly.  Demonstrates the "clean" path that a daily
# restart / explicit REST re-seed at session open would produce.
# ---------------------------------------------------------------------------


def test_clean_post_reset_seed_path_produces_correct_first_bucket_delta():
    """When the merger is freshly seeded post-09:30 (e.g. via REST seed
    on engine start, or by a daily process restart), it carries no
    prior-session residual.  The first-bucket snapshot reflects the true
    post-reset cumulative, and the LAG-delta reader sees the correct
    per-bucket value.  This is the production path that the daily
    restart / engine-run-window scheduling is designed to keep us on."""
    e = _engine()
    bucket = ET.localize(datetime(2026, 4, 28, 9, 35))

    acc = _FlowAccumulator(
        session_date=date(2026, 4, 28),
        last_volume_cum=0,
        ask_cum=0,
        mid_cum=0,
        bid_cum=0,
    )

    # First snapshot of the session reflects true post-reset cumulative.
    e._ingest_snapshot_into_accumulator(acc, _snap(volume=5), bucket)

    assert acc.last_volume_cum == 5
    assert acc.ask_cum + acc.mid_cum + acc.bid_cum == 5, (
        "with no stale handoff, the first new-session bucket carries "
        "only the actual post-reset volume"
    )
