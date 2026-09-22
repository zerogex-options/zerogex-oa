"""Total volume is its own fact, not the sum of a tick-test classification.

``up_volume`` / ``down_volume`` are a CLASSIFICATION -- TradeStation's own
tick test, as ``schema.sql`` says where it defines
``underlying_buying_pressure``. Until now their sum was also the only record
of how much traded at all, and the two facts were conflated in the schema,
in the ingestion payload, and in three views.

The vendor migration is what made that expensive: ThetaData cannot report a
signed split, so every volume-derived view -- VWAP, opening range, volume
spikes -- would have gone dark along with the classification, for want of a
number the feed does supply. These tests pin the separation.

No live Postgres in CI, so the SQL contract is asserted through the real
code path, the same convention as test_idempotent_writes_and_ohlc_merge.py.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import src.ingestion.main_engine as me
from src.ingestion.main_engine import IngestionEngine

SCHEMA = Path("setup/database/schema.sql").read_text()

ET = timezone(timedelta(hours=-4))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def test_underlying_quotes_has_its_own_volume_column():
    assert "ALTER TABLE underlying_quotes ADD COLUMN IF NOT EXISTS volume BIGINT;" in SCHEMA


def test_the_volume_column_is_nullable_with_no_default():
    """NULL means unknown, 0 means nothing traded, and the views tell them
    apart. A ``DEFAULT 0`` would erase that distinction on every row a feed
    could not answer for -- which is the whole failure being fixed."""
    line = next(ln for ln in SCHEMA.splitlines() if "ADD COLUMN IF NOT EXISTS volume BIGINT" in ln)
    assert "DEFAULT" not in line.upper()
    assert "NOT NULL" not in line.upper()


@pytest.mark.parametrize(
    "view",
    ["underlying_vwap_deviation", "opening_range_breakout", "unusual_volume_spikes"],
)
def test_total_volume_views_prefer_the_column_and_fall_back_to_the_split(view):
    """Both spellings are the same quantity and only one vendor supplies
    both, so history written before the column existed must keep reading."""
    body = _view_body(view)
    assert "COALESCE(volume, up_volume + down_volume)" in body or (
        "COALESCE(q.volume, q.up_volume + q.down_volume)" in body
    ), body

    # And no TOTAL-volume expression may still read the bare sum: one
    # surviving `up_volume + down_volume` is a NULL waiting to blank the
    # view after cutover. Scoped to the aggregates and the projected column
    # on purpose -- unusual_volume_spikes rightly keeps the bare sum as the
    # DENOMINATOR of its buy-pressure ratio, which is about the split
    # itself and has no meaning in terms of the total.
    for shape in (
        "SUM(up_volume + down_volume)",
        "AVG(up_volume + down_volume)",
        "STDDEV_SAMP(up_volume + down_volume)",
        "SUM(close * (up_volume + down_volume))",
        "(up_volume + down_volume) AS",
        "(q.up_volume + q.down_volume) AS",
    ):
        assert shape not in body, f"{view} still totals volume as {shape}"


def test_buy_pressure_abstains_instead_of_reporting_fifty_percent():
    """The defect this project keeps finding: a COALESCE default that turns
    "we cannot know" into a confident, permanent, plausible number.

    ``COALESCE(up/(up+down)*100, 50)`` answers 50% both when nothing traded
    (fair) and when the feed cannot classify at all (not fair -- that reads
    as a measured neutral tape, forever, on a column nobody re-checks).
    """
    for view in ("underlying_buying_pressure", "unusual_volume_spikes"):
        body = _view_body(view)
        assert "up_volume IS NULL OR " in body and "down_volume IS NULL THEN" in body, view

    pressure = _view_body("underlying_buying_pressure")
    # The label has to say so too, not fall through to Neutral.
    assert "No Tick Data" in pressure
    assert pressure.count("No Tick Data") == 2, "canonical column and back-compat alias"


def test_the_fifty_percent_default_survives_for_a_bar_that_simply_did_not_trade():
    """Not a blanket removal: with a real split and zero total volume, no
    ticks either way IS balanced, and 50 is the right answer."""
    assert _view_body("underlying_buying_pressure").count("100, 50)") == 2


def _view_body(name: str) -> str:
    """The whole CREATE VIEW statement, comments included.

    Not "up to the first semicolon": these view bodies carry prose comments
    that contain semicolons, and cutting there silently returns a fragment
    that passes some assertions and fails others for the wrong reason.
    """
    start = SCHEMA.index(f"CREATE VIEW {name} AS")
    ends = [
        SCHEMA.find(marker, start + 1)
        for marker in ("\nDROP VIEW ", "\nCREATE VIEW ", "\nCREATE OR REPLACE ", "\n-- ===")
    ]
    stops = [e for e in ends if e != -1]
    return SCHEMA[start : min(stops)] if stops else SCHEMA[start:]


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
class _RecCursor:
    def __init__(self, sink):
        self._sink = sink

    def execute(self, sql, params=None):
        self._sink.append((sql, params))

    def fetchone(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _RecConn:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _RecCursor(self._sink)

    def commit(self):
        pass

    def rollback(self):
        pass


def _engine(monkeypatch, sink):
    @contextlib.contextmanager
    def _conn():
        yield _RecConn(sink)

    monkeypatch.setattr(me, "db_connection", _conn)
    e = IngestionEngine.__new__(IngestionEngine)
    e._db_backoff_until = 0.0
    e._db_consecutive_failures = 0
    e.errors_count = 0
    e.underlying_bars_stored = 0
    e.last_flush_time = None
    return e


def test_the_accumulators_volume_reaches_the_database(monkeypatch):
    """stream_manager has carried "volume" in its bar dict since it was
    written; _store_underlying built a payload that dropped it. Harmless
    while one vendor supplied both numbers, load-bearing the moment a
    vendor supplies only the total."""
    sink: list = []
    e = _engine(monkeypatch, sink)
    e.db_symbol = "SPY"
    e._last_underlying_signature = None
    e._log_parity_signature = lambda *a, **k: None
    e.latest_underlying_price = None
    e.latest_underlying_price_ts = None

    e._store_underlying(
        {
            "symbol": "SPY",
            "timestamp": datetime(2026, 9, 22, 14, 31, tzinfo=timezone.utc),
            "open": 660.0,
            "high": 661.0,
            "low": 659.0,
            "close": 660.5,
            "up_volume": 700,
            "down_volume": 300,
            "volume": 1000,
        }
    )

    inserts = [(s, p) for (s, p) in sink if "INSERT INTO underlying_quotes" in s]
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert params[-1] == 1000, "the total must be persisted, not recomputed from the split"
    assert "volume = EXCLUDED.volume" in " ".join(sql.split())


def test_a_feed_without_a_total_writes_null_not_zero(monkeypatch):
    """`data.get("volume", 0)` would book "unknown" as "nothing traded" and
    drag every rolling mean that reads the column."""
    sink: list = []
    e = _engine(monkeypatch, sink)
    e.db_symbol = "SPY"
    e._last_underlying_signature = None
    e._log_parity_signature = lambda *a, **k: None
    e.latest_underlying_price = None
    e.latest_underlying_price_ts = None

    e._store_underlying(
        {
            "symbol": "SPY",
            "timestamp": datetime(2026, 9, 22, 14, 31, tzinfo=timezone.utc),
            "open": 660.0,
            "high": 661.0,
            "low": 659.0,
            "close": 660.5,
            "up_volume": None,
            "down_volume": None,
        }
    )

    inserts = [(s, p) for (s, p) in sink if "INSERT INTO underlying_quotes" in s]
    assert inserts[0][1][-1] is None


def test_the_websocket_tick_carries_volume(monkeypatch):
    """quote_broadcaster already forwards a "volume" key to subscribers; the
    ingestion NOTIFY never populated it, so every tick the frontend received
    said volume: null."""
    sink: list = []
    e = _engine(monkeypatch, sink)
    e._lookup_asset_type = lambda cursor, symbol: "etf"

    e._publish_quote_notify(
        _RecCursor(sink),
        {
            "symbol": "SPY",
            "timestamp": datetime(2026, 9, 22, 14, 31, tzinfo=timezone.utc),
            "open": 660.0,
            "high": 661.0,
            "low": 659.0,
            "close": 660.5,
            "up_volume": 700,
            "down_volume": 300,
            "volume": 1000,
        },
    )

    notifies = [p for (s, p) in sink if p and any("zgx_quote_updates" == x for x in _flat(p))]
    assert notifies, [s for s, _ in sink]
    body = next(x for x in _flat(notifies[0]) if isinstance(x, str) and x.startswith("{"))
    assert json.loads(body)["volume"] == 1000


def _flat(params):
    if isinstance(params, (list, tuple)):
        return list(params)
    return [params]
