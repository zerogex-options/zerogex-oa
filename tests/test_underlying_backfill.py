"""Tests for the historical underlying-bar backfill tool.

The HTTP + DB paths need live TradeStation credentials, but the tricky pure
logic — date-range chunking (so a request never blows the bar cap), bar parsing
/ validation, dedup, and the upsert row shape — is exercised here with fakes.
"""

from __future__ import annotations

from datetime import date

from src.tools.underlying_backfill import (
    _bar_to_row,
    _chunk_ranges,
    _safe_bigint,
    fetch_symbol,
    upsert_bars,
)

# ----------------------------------------------------------------------
# Range chunking
# ----------------------------------------------------------------------


def test_chunk_ranges_covers_window_without_overlap():
    chunks = _chunk_ranges(date(2022, 1, 1), date(2022, 3, 1), days_per_chunk=25)
    assert chunks[0][0] == "2022-01-01T00:00:00Z"
    # First chunk spans 25 days: Jan 1 .. Jan 25 (inclusive).
    assert chunks[0][1] == "2022-01-25T23:59:59Z"
    # Next chunk starts the day after, no gap and no overlap.
    assert chunks[1][0] == "2022-01-26T00:00:00Z"
    # Last chunk ends exactly on the window end.
    assert chunks[-1][1] == "2022-03-01T23:59:59Z"


def test_chunk_ranges_single_day():
    chunks = _chunk_ranges(date(2022, 5, 2), date(2022, 5, 2), days_per_chunk=25)
    assert chunks == [("2022-05-02T00:00:00Z", "2022-05-02T23:59:59Z")]


def test_chunk_ranges_exact_multiple():
    chunks = _chunk_ranges(date(2022, 1, 1), date(2022, 1, 10), days_per_chunk=5)
    assert chunks == [
        ("2022-01-01T00:00:00Z", "2022-01-05T23:59:59Z"),
        ("2022-01-06T00:00:00Z", "2022-01-10T23:59:59Z"),
    ]


def test_chunk_ranges_reversed_or_bad_is_empty():
    assert _chunk_ranges(date(2022, 5, 10), date(2022, 5, 1)) == []
    assert _chunk_ranges(date(2022, 5, 1), date(2022, 5, 10), days_per_chunk=0) == []


# ----------------------------------------------------------------------
# Bar parsing / validation
# ----------------------------------------------------------------------


def _bar(**over):
    raw = {
        "TimeStamp": "2022-01-03T14:31:00Z",
        "Open": "470.10",
        "High": "470.90",
        "Low": "469.80",
        "Close": "470.55",
        "UpVolume": "1200",
        "DownVolume": "800",
    }
    raw.update(over)
    return raw


def test_bar_to_row_parses_valid():
    row = _bar_to_row(_bar())
    assert row is not None
    assert row["open"] == 470.10 and row["close"] == 470.55
    assert row["up_volume"] == 1200 and row["down_volume"] == 800
    assert row["timestamp"] is not None


def test_bar_to_row_rejects_partial_and_degenerate():
    assert _bar_to_row(_bar(Close=None)) is None  # missing leg
    assert _bar_to_row(_bar(Low="471.00")) is None  # high < low
    assert _bar_to_row(_bar(Open="-1")) is None  # non-positive
    assert _bar_to_row({"Open": "1", "High": "1", "Low": "1", "Close": "1"}) is None  # no TimeStamp


def test_safe_bigint():
    assert _safe_bigint("1200") == 1200
    assert _safe_bigint(None) == 0
    assert _safe_bigint(-5) == 0
    assert _safe_bigint("junk") == 0


# ----------------------------------------------------------------------
# Upsert row shape + fetch dedup
# ----------------------------------------------------------------------


class _FakeCursor:
    def __init__(self):
        self.rows = None

    def executemany(self, sql, seq):
        self.sql = sql
        self.rows = list(seq)


class _FakeConn:
    def __init__(self):
        self._cur = _FakeCursor()

    def cursor(self):
        return self._cur


def test_upsert_bars_shapes_rows():
    conn = _FakeConn()
    rows = [r for r in (_bar_to_row(_bar()),) if r]
    n = upsert_bars(conn, "SPY", rows)
    assert n == 1
    (params,) = conn._cur.rows
    # (symbol, ts, open, high, low, close, up, down)
    assert params[0] == "SPY"
    assert len(params) == 8
    assert params[2] == 470.10


def test_upsert_empty_is_noop():
    conn = _FakeConn()
    assert upsert_bars(conn, "SPY", []) == 0


class _FakeClient:
    """Returns two bars per chunk; the second chunk repeats a timestamp."""

    def __init__(self):
        self.calls = []

    def get_stream_bars(self, symbol, **kw):
        self.calls.append((symbol, kw["firstdate"], kw["lastdate"]))
        n = len(self.calls)
        if n == 1:
            return {
                "Bars": [
                    _bar(TimeStamp="2022-01-03T14:31:00Z"),
                    _bar(TimeStamp="2022-01-03T14:32:00Z"),
                ]
            }
        # Second chunk: one new bar + a duplicate of a prior timestamp.
        return {
            "Bars": [_bar(TimeStamp="2022-02-01T15:00:00Z"), _bar(TimeStamp="2022-01-03T14:31:00Z")]
        }


def test_fetch_symbol_iterates_chunks_and_dedups():
    client = _FakeClient()
    rows = fetch_symbol(
        client, "SPY", date(2022, 1, 1), date(2022, 2, 5), days_per_chunk=25, sleep_seconds=0
    )
    # Two chunks requested.
    assert len(client.calls) == 2
    # 3 unique timestamps (the duplicate 14:31 is dropped).
    ts = {r["timestamp"] for r in rows}
    assert len(ts) == 3


# ----------------------------------------------------------------------
# Alias resolution: fetch via SYMBOL_ALIASES, write under the canonical symbol
# ----------------------------------------------------------------------


def test_backfill_resolves_alias_for_fetch_but_writes_canonical(monkeypatch):
    """``backfill(["NDX"])`` fetches the TS chain symbol ($NDXP.X) but writes
    rows under the canonical DB symbol ('NDX') so they land where the charts
    read. Aliasless equities (SPY) fetch and write under the same string."""
    import contextlib

    import src.tools.underlying_backfill as ub

    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X,NDX=$NDXP.X")
    # backfill() calls fetch_symbol without sleep_seconds -> default 0.3s pause.
    monkeypatch.setattr(ub.time, "sleep", lambda *_a, **_k: None)

    fetched: list = []
    written: list = []

    class _Client:
        def get_stream_bars(self, symbol, **kw):
            fetched.append(symbol)
            return {"Bars": [_bar(TimeStamp="2022-01-03T14:31:00Z")]}

    class _Cur:
        def executemany(self, sql, seq):
            self.seq = list(seq)

    class _Conn:
        def __init__(self):
            self._cur = _Cur()

        def cursor(self):
            return self._cur

    @contextlib.contextmanager
    def _fake_db():
        conn = _Conn()
        yield conn
        # Capture the symbol each row was written under (params[0]).
        written.extend(params[0] for params in conn._cur.seq)

    monkeypatch.setattr(
        "src.ingestion.tradestation_client.TradeStationClient",
        lambda *a, **k: _Client(),
    )
    monkeypatch.setattr("src.database.db_connection", _fake_db)

    result = ub.backfill(["NDX", "SPY"], date(2022, 1, 3), date(2022, 1, 3))

    # NDX is fetched via its alias; SPY (no alias) is fetched as itself.
    assert "$NDXP.X" in fetched
    assert "SPY" in fetched
    assert "NDX" not in fetched  # never fetch the canonical for an aliased index
    # Rows are written under the CANONICAL symbols, not the TS fetch symbols.
    assert sorted(written) == ["NDX", "SPY"]
    assert result == {"NDX": 1, "SPY": 1}
