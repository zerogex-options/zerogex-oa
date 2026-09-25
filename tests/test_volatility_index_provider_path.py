"""VIX and VXN must survive the move off TradeStation, and stay 5-minute bars.

``vix_bars`` and ``vxn_bars`` hold one row per 5-minute bar, stamped at the
bar's CLOSE, with a real OHLC candle in it. TradeStation delivers exactly
that -- a finished bar, already aggregated -- so the original reader could
write what it was handed.

The provider seam does not. ``stream_index_bars`` returns a MARK: the index
level right now, re-read every poll second, with no period of its own. Three
different things go wrong if that is written straight through, and none of
them fails loudly:

* one row per POLL instead of per bar -- ~23,000 rows a session against 78,
  turning the gauge's two-session read into a 46,000-row scan and its
  "momentum over the last ten bars" into momentum over the last ten seconds;
* the row stamped at the mark's own second, so it never lines up with the
  TradeStation history sitting beside it in the same table;
* every candle flattened to open = high = low = close, because the existing
  conflict clause overwrites all four with the last mark to arrive.

These tests pin the bucketing, the stamping, and the accumulation, and the
rule that decides which reader runs at all.
"""

from __future__ import annotations

import ast
import io
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.providers.base import Bar
from src.ingestion import volatility_index_ingester as vix_mod
from src.ingestion.volatility_index_ingester import (
    VolatilityIndexIngester,
    _bar_close_timestamp,
    _mark_row,
    run_ingester,
)

INGESTER_SRC = (
    Path(__file__).resolve().parents[1] / "src" / "ingestion" / "volatility_index_ingester.py"
)

ET_OFFSET = timezone(timedelta(hours=-4))


def _ingester(**kw: Any) -> VolatilityIndexIngester:
    """An ingester with signal handlers stubbed out.

    ``__init__`` installs SIGINT/SIGTERM handlers, which pytest's main
    thread would otherwise keep for the rest of the session.
    """
    defaults: Dict[str, Any] = dict(
        ticker="VIX",
        symbol="$VIX.X",
        table_name="vix_bars",
        initial_barsback=160,
        poll_barsback=3,
        retention_days=7,
    )
    defaults.update(kw)
    client = defaults.pop("client", None)
    with patch("src.ingestion.volatility_index_ingester.signal.signal"):
        return VolatilityIndexIngester(client, **defaults)


# ---------------------------------------------------------------------------
# 1. Which reader runs
# ---------------------------------------------------------------------------


def test_run_ingester_builds_a_tradestation_client_only_for_tradestation(monkeypatch):
    """No TradeStation client under any other feed.

    Its three credentials stop existing when TradeStation is decommissioned.
    Building one unconditionally -- which this entry point used to do -- makes
    every other feed depend on the old vendor's secrets just to start.
    """
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "thetadata")
    built: List[str] = []

    class _Boom:
        def __init__(self, *a, **kw):
            built.append("tradestation")

    with (
        patch.object(vix_mod, "TradeStationClient", _Boom),
        patch.object(vix_mod, "VolatilityIndexIngester") as ingester_cls,
        patch("dotenv.load_dotenv"),
        patch("src.ingestion.providers.get_provider") as get_provider,
    ):
        get_provider.return_value = MagicMock(name="provider")
        run_ingester(
            ticker="VIX",
            symbol="$VIX.X",
            table_name="vix_bars",
            initial_barsback=160,
            poll_barsback=3,
            retention_days=7,
        )

    assert built == [], "built a TradeStationClient for a non-TradeStation feed"
    assert get_provider.called, "never asked the provider registry for a feed"
    args, kwargs = ingester_cls.call_args
    assert args[0] is None, "passed a client through on the provider path"
    assert kwargs["provider"] is get_provider.return_value


def test_run_ingester_still_builds_a_client_for_tradestation(monkeypatch):
    """The TradeStation path is untouched -- client built, provider absent."""
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "tradestation")

    with (
        patch.object(vix_mod, "TradeStationClient") as client_cls,
        patch.object(vix_mod, "VolatilityIndexIngester") as ingester_cls,
        patch("dotenv.load_dotenv"),
        patch("src.ingestion.providers.get_provider") as get_provider,
    ):
        run_ingester(
            ticker="VIX",
            symbol="$VIX.X",
            table_name="vix_bars",
            initial_barsback=160,
            poll_barsback=3,
            retention_days=7,
        )

    assert client_cls.called, "TradeStation feed did not build a TradeStation client"
    assert not get_provider.called, "reached for a provider on the TradeStation path"
    args, kwargs = ingester_cls.call_args
    assert args[0] is client_cls.return_value
    assert kwargs["provider"] is None


def test_unset_provider_env_still_means_tradestation(monkeypatch):
    """An operator who has never heard of MARKET_DATA_PROVIDER keeps today's feed."""
    monkeypatch.delenv("MARKET_DATA_PROVIDER", raising=False)

    with (
        patch.object(vix_mod, "TradeStationClient") as client_cls,
        patch.object(vix_mod, "VolatilityIndexIngester"),
        patch("dotenv.load_dotenv"),
        patch("src.ingestion.providers.get_provider") as get_provider,
    ):
        run_ingester(
            ticker="VXN",
            symbol="$VXN.X",
            table_name="vxn_bars",
            initial_barsback=160,
            poll_barsback=3,
            retention_days=7,
        )

    assert client_cls.called
    assert not get_provider.called


@pytest.mark.parametrize(
    "client, expected",
    [(MagicMock(name="ts_client"), "_read_stream"), (None, "_read_provider_stream")],
)
def test_run_picks_its_reader_from_the_client(client, expected):
    """One branch, and it keys off the client actually present.

    Not off the env var: an ingester handed a client must use it even if
    MARKET_DATA_PROVIDER says otherwise, or a half-configured process would
    hold an authenticated TradeStation session open and never read from it.
    """
    ing = _ingester(client=client, provider=MagicMock(name="provider"))
    calls: List[str] = []

    def _stop(name: str):
        def _inner():
            calls.append(name)
            ing.running = False

        return _inner

    with (
        patch.object(ing, "_read_stream", _stop("_read_stream")),
        patch.object(ing, "_read_provider_stream", _stop("_read_provider_stream")),
        patch.object(vix_mod, "is_engine_run_window", return_value=True),
        patch.object(vix_mod, "close_connection_pool"),
    ):
        ing.run()

    assert calls == [expected]


# ---------------------------------------------------------------------------
# 2. Bucketing -- the row has to land on the same grid as the history
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "observed, expected",
    [
        # Mid-bucket: belongs to the bar that closes at 09:35.
        ("09:31:07", "09:35:00"),
        ("09:34:59", "09:35:00"),
        # Exactly on the boundary CLOSES that bar, it does not open the next.
        # TradeStation's interval is (09:30, 09:35], not [09:30, 09:35).
        ("09:35:00", "09:35:00"),
        # One microsecond past it is the next bar.
        ("09:35:00.000001", "09:40:00"),
        # Hour rollover, which a naive minute+5 would write as 09:60.
        ("09:56:12", "10:00:00"),
        ("15:59:59", "16:00:00"),
    ],
)
def test_marks_land_on_the_five_minute_close(observed, expected):
    """Stamped at the bar's CLOSE, matching what TradeStation wrote.

    ``ingestion_freshness_healthcheck`` documents the convention these
    tables already follow: the 09:30-09:35 bar arrives stamped 09:35.
    Stamping at the bucket's START instead would offset every provider row
    one bar earlier than the TradeStation rows beside it -- invisible in the
    gauge, and quietly wrong in get_vix_z_score_20d, which reads both.
    """
    day = "2026-09-25 "
    ts = datetime.fromisoformat(day + observed).replace(tzinfo=ET_OFFSET)
    want = datetime.fromisoformat(day + expected).replace(tzinfo=ET_OFFSET)
    assert _bar_close_timestamp(ts) == want


def test_bucketing_keeps_the_timezone_it_was_given():
    """A dropped tzinfo would shift every row by the UTC offset."""
    ts = datetime(2026, 9, 25, 9, 31, 7, tzinfo=timezone.utc)
    assert _bar_close_timestamp(ts).tzinfo is timezone.utc


def test_many_marks_in_one_bucket_produce_one_timestamp():
    """The whole point: poll rate stops driving row count."""
    base = datetime(2026, 9, 25, 9, 30, 1, tzinfo=timezone.utc)
    stamps = {_bar_close_timestamp(base + timedelta(seconds=s)) for s in range(0, 299)}
    assert len(stamps) == 1, f"one 5-minute bucket produced {len(stamps)} distinct rows"


# ---------------------------------------------------------------------------
# 3. _mark_row -- close only, and skip what cannot be written
# ---------------------------------------------------------------------------


def test_mark_row_ignores_a_running_daily_bar_and_uses_the_close():
    """open/high/low on the Bar are NOT this interval's, so they are dropped.

    The Market Value index endpoint answers a mark alone. The realtime one
    answers a running DAILY bar -- and passing its high and low through
    would stamp the whole session's range onto every 5-minute row, a
    high/low that is right on its face and wrong by a factor of a day.
    """
    bar = Bar(
        symbol="VIX",
        timestamp=datetime(2026, 9, 25, 10, 2, 30, tzinfo=timezone.utc),
        open=18.0,
        high=31.0,  # the DAY's high
        low=12.0,  # the DAY's low
        close=20.4,
    )
    row = _mark_row(bar)
    assert row is not None
    assert (row["open"], row["high"], row["low"], row["close"]) == (20.4, 20.4, 20.4, 20.4)


def test_mark_row_drops_a_bar_with_no_close():
    """``close`` is NOT NULL in both tables -- a row without one cannot land."""
    bar = Bar(symbol="VIX", timestamp=datetime.now(timezone.utc), close=None)
    assert _mark_row(bar) is None


def test_mark_row_drops_a_bar_with_no_usable_timestamp():
    """``timestamp`` is the primary key; a missing one takes the write down."""

    class _NoTime:
        close = 20.0
        timestamp = None

    assert _mark_row(_NoTime()) is None


def test_mark_row_carries_no_volume_key():
    """A cash index has no volume of its own, and these tables have no column."""
    bar = Bar(symbol="VIX", timestamp=datetime.now(timezone.utc), close=20.0, volume=1234)
    row = _mark_row(bar)
    assert row is not None and "volume" not in row


# ---------------------------------------------------------------------------
# 4. The conflict clause -- a candle, not four copies of the last mark
# ---------------------------------------------------------------------------


def _captured_sql(ing: VolatilityIndexIngester, row: Dict[str, Any]) -> str:
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = False
    with patch.object(vix_mod, "db_connection", return_value=ctx):
        assert ing._accumulate_mark(row) == 1
    return " ".join(cursor.execute.call_args[0][0].split())


def test_accumulate_mark_takes_the_period_correct_aggregate():
    """First-seen open, running high and low, last close.

    The same conflict clause ``IngestionEngine._upsert_underlying_quote``
    uses. ``_upsert_bars``' plain overwrite is right for TradeStation, which
    sends a finished bar, and wrong here: four marks of 20.1, 21.5, 19.2,
    20.8 would store 20.8/20.8/20.8/20.8 instead of 20.1/21.5/19.2/20.8.
    """
    ing = _ingester()
    sql = _captured_sql(ing, _mark_row(Bar("VIX", datetime.now(timezone.utc), close=20.0)))

    # The bucket is the primary key, and it is what marks collide on. Conflict
    # on anything else and every mark opens a new row -- the exact per-poll
    # explosion the bucketing exists to prevent.
    assert "ON CONFLICT (timestamp) DO UPDATE SET" in sql
    assert "open = COALESCE(vix_bars.open, EXCLUDED.open)" in sql
    assert "high = GREATEST(vix_bars.high, EXCLUDED.high)" in sql
    assert "low = LEAST(vix_bars.low, EXCLUDED.low)" in sql
    assert "close = EXCLUDED.close" in sql
    assert "open = EXCLUDED.open" not in sql, "overwrites the bucket's open with the last mark"
    assert "high = EXCLUDED.high" not in sql, "overwrites the bucket's high with the last mark"
    assert "low = EXCLUDED.low" not in sql, "overwrites the bucket's low with the last mark"


def test_accumulate_mark_writes_to_the_ingester_s_own_table():
    """VXN marks must not land in vix_bars."""
    ing = _ingester(ticker="VXN", symbol="$VXN.X", table_name="vxn_bars")
    sql = _captured_sql(ing, _mark_row(Bar("VXN", datetime.now(timezone.utc), close=24.0)))
    assert "INSERT INTO vxn_bars" in sql
    assert "vix_bars" not in sql


def test_accumulate_mark_reports_zero_when_the_write_fails():
    """A failed write must not be counted as a bar, or the seed marker lies."""
    ing = _ingester()
    with patch.object(vix_mod, "db_connection", side_effect=RuntimeError("db down")):
        assert (
            ing._accumulate_mark(_mark_row(Bar("VIX", datetime.now(timezone.utc), close=20.0))) == 0
        )


def test_only_allowlisted_tables_reach_the_sql():
    """The interpolation in _accumulate_mark is safe only because of this."""
    with patch("src.ingestion.volatility_index_ingester.signal.signal"):
        with pytest.raises(ValueError):
            VolatilityIndexIngester(
                None,
                ticker="EVIL",
                symbol="$EVIL.X",
                table_name="vix_bars; DROP TABLE users",
                initial_barsback=1,
                poll_barsback=1,
                retention_days=1,
            )


# ---------------------------------------------------------------------------
# 5. The provider reader
# ---------------------------------------------------------------------------


class _FakeStream:
    """A BarStream that hands out a scripted sequence, then goes quiet."""

    def __init__(self, bars: List[Optional[Bar]], alive: bool = True):
        self._bars = list(bars)
        self._alive = alive
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def is_alive(self) -> bool:
        return self._alive

    def drain(self) -> Optional[Bar]:
        return self._bars.pop(0) if self._bars else None

    @property
    def updates_received(self) -> int:
        return 0


def _run_provider_reader(ing: VolatilityIndexIngester, stream: _FakeStream, ticks: int):
    """Drive _read_provider_stream for ``ticks`` iterations, then stop it."""
    ing.running = True
    remaining = {"n": ticks}

    def _sleep(_seconds):
        remaining["n"] -= 1
        if remaining["n"] <= 0:
            ing.running = False

    ing.provider = MagicMock()
    ing.provider.stream_index_bars.return_value = stream
    with (
        patch.object(vix_mod.time, "sleep", _sleep),
        patch.object(vix_mod, "is_engine_run_window", return_value=True),
    ):
        ing._read_provider_stream()


def test_provider_reader_folds_each_mark_into_its_bucket():
    """Three marks in one bucket: three writes, all to the same timestamp."""
    ing = _ingester()
    base = datetime(2026, 9, 25, 13, 31, tzinfo=timezone.utc)
    stream = _FakeStream(
        [
            Bar("VIX", base, close=20.1),
            Bar("VIX", base + timedelta(seconds=30), close=21.5),
            Bar("VIX", base + timedelta(seconds=60), close=20.8),
        ]
    )
    written: List[Dict[str, Any]] = []
    with patch.object(ing, "_accumulate_mark", lambda row: written.append(row) or 1):
        _run_provider_reader(ing, stream, ticks=3)

    assert [r["close"] for r in written] == [20.1, 21.5, 20.8]
    assert {r["timestamp"] for r in written} == {datetime(2026, 9, 25, 13, 35, tzinfo=timezone.utc)}
    assert stream.started and stream.stopped


def test_provider_reader_counts_bars_not_marks():
    """_PRUNE_EVERY_N_UPSERTS is a BAR budget.

    At a mark a second, counting marks would fire the retention DELETE every
    two minutes instead of roughly every ten hours -- and would log a "cache
    seeded" line's worth of bookkeeping against something that is not a bar.
    """
    ing = _ingester()
    base = datetime(2026, 9, 25, 13, 31, tzinfo=timezone.utc)
    stream = _FakeStream(
        [
            Bar("VIX", base, close=20.1),  # bucket 13:35
            Bar("VIX", base + timedelta(seconds=60), close=20.2),  # bucket 13:35
            Bar("VIX", base + timedelta(seconds=300), close=20.3),  # bucket 13:40
        ]
    )
    recorded: List[int] = []
    with (
        patch.object(ing, "_accumulate_mark", return_value=1),
        patch.object(ing, "_record_upsert", lambda n: recorded.append(n)),
    ):
        _run_provider_reader(ing, stream, ticks=3)

    assert recorded == [1, 1], f"three marks over two bars recorded {recorded}"


def test_provider_reader_skips_a_mark_it_cannot_write():
    """A bar with no close is dropped, not handed to the DB as a NULL."""
    ing = _ingester()
    stream = _FakeStream([Bar("VIX", datetime.now(timezone.utc), close=None)])
    with patch.object(ing, "_accumulate_mark") as accum:
        _run_provider_reader(ing, stream, ticks=2)
    assert not accum.called


def test_provider_reader_raises_when_the_stream_dies():
    """run()'s reconnect backoff is what handles this -- it needs the raise."""
    ing = _ingester()
    stream = _FakeStream([], alive=False)
    with pytest.raises(RuntimeError, match="stream stopped"):
        _run_provider_reader(ing, stream, ticks=5)
    assert stream.stopped, "a dead stream was left unstopped"


def test_provider_reader_stops_the_stream_even_when_the_body_raises():
    """Otherwise a reconnect loop leaks a poller thread per attempt."""
    ing = _ingester()
    stream = _FakeStream([Bar("VIX", datetime.now(timezone.utc), close=20.0)])
    with patch.object(ing, "_accumulate_mark", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="boom"):
            _run_provider_reader(ing, stream, ticks=3)
    assert stream.stopped


def test_provider_reader_asks_for_the_configured_bar_interval():
    """5-minute bars, and the DB symbol rather than the vendor's."""
    ing = _ingester()
    stream = _FakeStream([])
    _run_provider_reader(ing, stream, ticks=1)
    _, kwargs = ing.provider.stream_index_bars.call_args
    args, _ = ing.provider.stream_index_bars.call_args
    assert args[0] == "$VIX.X"
    assert kwargs["db_symbol"] == "VIX"
    assert kwargs["interval"] == vix_mod.VOLATILITY_BAR_INTERVAL == 5
    assert kwargs["unit"] == vix_mod.VOLATILITY_BAR_UNIT == "Minute"


# ---------------------------------------------------------------------------
# 6. One implementation of the retention rule
# ---------------------------------------------------------------------------


def _methods_calling(attr: str) -> set:
    """Names of the methods whose body calls ``self.<attr>(...)``."""
    tree = ast.parse(io.open(INGESTER_SRC, encoding="utf-8").read())
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for inner in ast.walk(node):
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr == attr
                and isinstance(inner.func.value, ast.Name)
                and inner.func.value.id == "self"
            ):
                found.add(node.name)
    return found


def test_both_readers_share_one_retention_implementation():
    """Two copies of "prune every N upserts" drift, and nobody watches the copy.

    Asserted on the parse tree rather than by counting the constant's name,
    so a comment that merely mentions it does not move the number.
    """
    source = io.open(INGESTER_SRC, encoding="utf-8").read()
    assert source.count("_upserts_since_prune >= _PRUNE_EVERY_N_UPSERTS") == 1, (
        "the prune threshold is compared in more than one place; both readers "
        "must go through _record_upsert"
    )
    assert _methods_calling("_prune_old_bars") == {
        "_record_upsert"
    }, "retention is triggered from somewhere other than _record_upsert"
    assert _methods_calling("_record_upsert") == {
        "_handle_payload",
        "_read_provider_stream",
    }, "a reader stopped reporting its writes, so retention never sees them"


# ---------------------------------------------------------------------------
# 7. Against a real PostgreSQL, when one is offered
# ---------------------------------------------------------------------------

_DSN = os.getenv("VOLATILITY_BARS_DSN")


@pytest.mark.skipif(
    _DSN is None,
    reason=(
        "VOLATILITY_BARS_DSN not set -- integration check skipped. Point it at "
        "a throwaway database to run the conflict clause for real."
    ),
)
def test_conflict_clause_builds_a_real_candle_in_postgres():
    """The clause above, executed -- four marks in, one candle out.

    The string assertions in section 4 pin what we wrote; this pins what
    PostgreSQL does with it. GREATEST and LEAST ignore NULLs, which is what
    makes an existing TradeStation row with a NULL high safe to extend.
    """
    import psycopg2

    conn = psycopg2.connect(_DSN)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS vix_bars_conflict_check")
        cur.execute("""
            CREATE TABLE vix_bars_conflict_check (
                timestamp TIMESTAMPTZ PRIMARY KEY,
                open NUMERIC(10, 4), high NUMERIC(10, 4), low NUMERIC(10, 4),
                close NUMERIC(10, 4) NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """)
        ing = _ingester(table_name="vix_bars")
        sql = _captured_sql(
            ing, _mark_row(Bar("VIX", datetime.now(timezone.utc), close=1.0))
        ).replace("vix_bars", "vix_bars_conflict_check")

        bucket = datetime(2026, 9, 25, 13, 35, tzinfo=timezone.utc)
        for mark in (20.1, 21.5, 19.2, 20.8):
            cur.execute(sql, (bucket, mark, mark, mark, mark))

        cur.execute("SELECT open, high, low, close FROM vix_bars_conflict_check")
        rows = cur.fetchall()
        assert len(rows) == 1, "one bucket produced more than one row"
        assert [float(v) for v in rows[0]] == [20.1, 21.5, 19.2, 20.8]
        cur.execute("DROP TABLE vix_bars_conflict_check")
    finally:
        conn.close()
