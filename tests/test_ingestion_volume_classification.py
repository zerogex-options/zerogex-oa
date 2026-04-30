"""Unit tests for volume classification (Lee-Ready prior-tick + mid-band).

Covers ``IngestionEngine._classify_volume_chunk`` and the opening-auction
helper. These pin the classification semantics that drive ask/bid/mid
volume in ``option_chains``.
"""

import threading
from datetime import datetime

import pytz

from src.ingestion.main_engine import IngestionEngine

ET = pytz.timezone("US/Eastern")


def _engine() -> IngestionEngine:
    return IngestionEngine.__new__(IngestionEngine)


def test_classify_at_ask_is_ask_volume():
    av, mv, bv = _engine()._classify_volume_chunk(100, last=5.58, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (100, 0, 0)


def test_classify_at_bid_is_bid_volume():
    av, mv, bv = _engine()._classify_volume_chunk(100, last=5.53, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (0, 0, 100)


def test_classify_user_5_57_case_routes_to_mid_with_default_band():
    # The reported case: user sold 250 contracts at 5.57 with bid 5.53,
    # ask 5.58, mid 5.555. Half-spread = 0.025; default band 0.70 puts
    # the ask threshold at mid + 0.70*0.025 = 5.5725, so a 5.57 fill is
    # below the threshold and is routed to mid_volume rather than getting
    # full ask credit.
    av, mv, bv = _engine()._classify_volume_chunk(250, last=5.57, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (0, 250, 0)


def test_classify_clear_ask_print_outside_band():
    # 5.578 is well above the 5.5700 threshold -> still ask_volume.
    av, mv, bv = _engine()._classify_volume_chunk(100, last=5.578, bid=5.53, ask=5.58, mid=5.555)
    assert (av, mv, bv) == (100, 0, 0)


def test_classify_band_zero_is_pure_lee_ready():
    # band_pct=0 => any print strictly above mid is ask_volume, below is bid.
    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.557, bid=5.53, ask=5.58, mid=5.555, band_pct=0.0
    )
    assert (av, mv, bv) == (100, 0, 0)

    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.553, bid=5.53, ask=5.58, mid=5.555, band_pct=0.0
    )
    assert (av, mv, bv) == (0, 0, 100)


def test_classify_band_one_only_counts_at_quote_as_ask_bid():
    # band_pct=1 => mid zone covers the full inside spread; only prints at
    # or beyond the quote count as ask/bid.
    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.575, bid=5.53, ask=5.58, mid=5.555, band_pct=1.0
    )
    assert (av, mv, bv) == (0, 100, 0)

    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.585, bid=5.53, ask=5.58, mid=5.555, band_pct=1.0
    )
    assert (av, mv, bv) == (100, 0, 0)


def test_classify_band_clamped_to_unit_interval():
    # Out-of-range band shouldn't invert the zones; >1 behaves like 1.
    av, mv, bv = _engine()._classify_volume_chunk(
        100, last=5.575, bid=5.53, ask=5.58, mid=5.555, band_pct=5.0
    )
    assert (av, mv, bv) == (0, 100, 0)


def test_classify_zero_volume_returns_zeros():
    assert _engine()._classify_volume_chunk(0, 5.0, 4.95, 5.05, 5.0) == (0, 0, 0)


def test_classify_missing_last_defaults_to_mid():
    av, mv, bv = _engine()._classify_volume_chunk(50, last=None, bid=4.95, ask=5.05, mid=5.0)
    assert (av, mv, bv) == (0, 50, 0)


def test_classify_missing_bid_and_ask_defaults_to_mid():
    av, mv, bv = _engine()._classify_volume_chunk(50, last=5.0, bid=None, ask=None, mid=None)
    assert (av, mv, bv) == (0, 50, 0)


def test_classify_falls_back_to_nearest_neighbor_when_only_one_side_known():
    # Only ask known, last very close to it -> still classifies as ask.
    av, mv, bv = _engine()._classify_volume_chunk(50, last=5.05, bid=None, ask=5.05, mid=5.0)
    assert (av, mv, bv) == (50, 0, 0)


def test_opening_auction_bucket_detected_at_0930_et():
    bucket_open = ET.localize(datetime(2026, 4, 28, 9, 30))
    bucket_post_open = ET.localize(datetime(2026, 4, 28, 9, 31))
    bucket_pre_open = ET.localize(datetime(2026, 4, 28, 9, 29))
    assert IngestionEngine._is_opening_auction_bucket(bucket_open) is True
    assert IngestionEngine._is_opening_auction_bucket(bucket_post_open) is False
    assert IngestionEngine._is_opening_auction_bucket(bucket_pre_open) is False


def test_opening_auction_helper_handles_utc_input():
    # 13:30 UTC == 09:30 ET during US daylight time.
    bucket_utc = pytz.UTC.localize(datetime(2026, 4, 28, 13, 30))
    assert IngestionEngine._is_opening_auction_bucket(bucket_utc) is True


def test_opening_auction_helper_naive_datetime_treated_as_et():
    naive = datetime(2026, 4, 28, 9, 30)
    assert IngestionEngine._is_opening_auction_bucket(naive) is True


def test_cached_last_quote_roundtrip():
    from collections import OrderedDict

    engine = _engine()
    engine._option_last_quote = OrderedDict()
    engine._option_last_quote_lock = threading.Lock()
    engine._option_last_quote_max = 10000

    assert engine._get_cached_last_quote("XYZ") is None

    engine._update_cached_last_quote("XYZ", bid=1.0, ask=1.1, mid=1.05)
    cached = engine._get_cached_last_quote("XYZ")
    assert cached == {"bid": 1.0, "ask": 1.1, "mid": 1.05}

    # All-None update is a no-op (don't wipe a known quote with empty data).
    engine._update_cached_last_quote("XYZ", bid=None, ask=None, mid=None)
    assert engine._get_cached_last_quote("XYZ") == {"bid": 1.0, "ask": 1.1, "mid": 1.05}


def test_cached_last_quote_lru_eviction():
    from collections import OrderedDict

    engine = _engine()
    engine._option_last_quote = OrderedDict()
    engine._option_last_quote_lock = threading.Lock()
    engine._option_last_quote_max = 3

    for sym in ("A", "B", "C"):
        engine._update_cached_last_quote(sym, bid=1.0, ask=1.1, mid=1.05)
    assert list(engine._option_last_quote) == ["A", "B", "C"]

    # Reading A promotes it to most-recently-used; inserting D should evict B.
    engine._get_cached_last_quote("A")
    engine._update_cached_last_quote("D", bid=2.0, ask=2.1, mid=2.05)
    assert list(engine._option_last_quote) == ["C", "A", "D"]
    assert engine._get_cached_last_quote("B") is None


def test_invalidate_baseline_also_drops_last_quote():
    from collections import OrderedDict

    engine = _engine()
    engine._option_volume_baseline = {"XYZ": (100, 0.0)}
    engine._option_volume_baseline_lock = threading.Lock()
    engine._option_last_quote = OrderedDict()
    engine._option_last_quote_lock = threading.Lock()
    engine._option_last_quote_max = 10000

    engine._update_cached_last_quote("XYZ", bid=1.0, ask=1.1, mid=1.05)
    engine._invalidate_option_volume_baseline("XYZ")
    assert engine._get_cached_last_quote("XYZ") is None
