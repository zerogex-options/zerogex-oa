"""Tests for the historical underlying-bar backfill tool.

The HTTP + DB paths need live TradeStation credentials, but the tricky pure
logic — date-range chunking (so a request never blows the bar cap), bar parsing
/ validation, dedup, and the upsert row shape — is exercised here with fakes.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from src.tools.underlying_backfill import (
    _DEFAULT_SESSION_TEMPLATE,
    _bar_to_row,
    _chunk_ranges,
    _coverage_gaps,
    _et_span,
    _safe_bigint,
    fetch_symbol,
    upsert_bars,
)

ET = ZoneInfo("America/New_York")

# ----------------------------------------------------------------------
# Range chunking
# ----------------------------------------------------------------------


def test_chunk_ranges_covers_window_without_overlap():
    # Boundaries are ET midnights rendered in UTC (Jan = EST, UTC-5).
    chunks = _chunk_ranges(date(2022, 1, 1), date(2022, 3, 1), days_per_chunk=25)
    assert chunks[0][0] == "2022-01-01T05:00:00Z"
    # First chunk spans 25 days: Jan 1 .. Jan 25 (inclusive).
    assert chunks[0][1] == "2022-01-26T04:59:59Z"
    # Next chunk starts one second later, no gap and no overlap.
    assert chunks[1][0] == "2022-01-26T05:00:00Z"
    # Last chunk ends exactly on the window end.
    assert chunks[-1][1] == "2022-03-02T04:59:59Z"


def test_chunk_ranges_single_day():
    chunks = _chunk_ranges(date(2022, 5, 2), date(2022, 5, 2), days_per_chunk=25)
    assert chunks == [("2022-05-02T04:00:00Z", "2022-05-03T03:59:59Z")]


def test_chunk_ranges_exact_multiple():
    chunks = _chunk_ranges(date(2022, 1, 1), date(2022, 1, 10), days_per_chunk=5)
    assert chunks == [
        ("2022-01-01T05:00:00Z", "2022-01-06T04:59:59Z"),
        ("2022-01-06T05:00:00Z", "2022-01-11T04:59:59Z"),
    ]


def test_chunk_ranges_single_day_contains_the_whole_extended_session():
    """Regression: the requested ET day must contain 04:00-20:00 ET.

    UTC-anchored days (the old behaviour) read 2026-08-18 as
    2026-08-17 20:00 ET .. 2026-08-18 19:59 ET — dragging in the previous
    evening's post-market and clipping the requested day's 20:00 ET close.
    """
    first, last = _chunk_ranges(date(2026, 8, 18), date(2026, 8, 18))[0]
    assert first == "2026-08-18T04:00:00Z"  # 00:00 ET, before the 04:00 ET open
    assert last == "2026-08-19T03:59:59Z"  # 23:59 ET, after the 20:00 ET close


def test_chunk_ranges_stay_aligned_across_a_dst_boundary():
    """Chunks are built in ET, so the offset follows the DST change."""
    chunks = _chunk_ranges(date(2022, 3, 11), date(2022, 3, 15), days_per_chunk=1)
    assert chunks[0] == ("2022-03-11T05:00:00Z", "2022-03-12T04:59:59Z")  # EST
    assert chunks[-1] == ("2022-03-15T04:00:00Z", "2022-03-16T03:59:59Z")  # EDT


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

    def get_bars(self, symbol, **kw):
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
    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "cid")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "crt")
    # backfill() calls fetch_symbol without sleep_seconds -> default 0.3s pause.
    monkeypatch.setattr(ub.time, "sleep", lambda *_a, **_k: None)

    fetched: list = []
    written: list = []

    class _Client:
        def get_bars(self, symbol, **kw):
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

    client_ctor: dict = {}

    def _make_client(*a, **k):
        client_ctor["args"] = a
        return _Client()

    monkeypatch.setattr("src.ingestion.tradestation_client.TradeStationClient", _make_client)
    monkeypatch.setattr("src.database.db_connection", _fake_db)

    result = ub.backfill(["NDX", "SPY"], date(2022, 1, 3), date(2022, 1, 3))

    # NDX is fetched via its alias; SPY (no alias) is fetched as itself.
    assert "$NDXP.X" in fetched
    assert "SPY" in fetched
    assert "NDX" not in fetched  # never fetch the canonical for an aliased index
    # Rows are written under the CANONICAL symbols, not the TS fetch symbols.
    assert sorted(written) == ["NDX", "SPY"]
    assert result == {"NDX": 1, "SPY": 1}
    # Regression guard: the client is built from env credentials, not the
    # invalid no-arg TradeStationClient() the tool used to call.
    assert client_ctor["args"] == ("cid", "csecret", "crt")


# ----------------------------------------------------------------------
# Session template: the fetch must ask for the extended-hours session
# ----------------------------------------------------------------------


class _TemplateRecordingClient:
    def __init__(self):
        self.templates = []

    def get_bars(self, symbol, **kw):
        self.templates.append(kw["sessiontemplate"])
        return {"Bars": []}


def test_fetch_symbol_requests_the_extended_hours_session_by_default():
    """Regression: the tool used to request ``sessiontemplate=Default``.

    TradeStation serves nothing outside the template's window, so a backfill
    run under "Default" (core session only) comes back starting hours into
    the day and silently leaves the pre-market gap it was run to repair.
    04:00-20:00 ET is the window the live ingester streams in production, and
    the window a backfill has to reproduce.
    """
    client = _TemplateRecordingClient()
    fetch_symbol(client, "QQQ", date(2026, 8, 18), date(2026, 8, 18), sleep_seconds=0)
    assert client.templates == ["USEQ24Hour"]
    assert _DEFAULT_SESSION_TEMPLATE == "USEQ24Hour"


def test_fetch_symbol_session_template_is_overridable():
    client = _TemplateRecordingClient()
    fetch_symbol(
        client,
        "QQQ",
        date(2026, 8, 18),
        date(2026, 8, 18),
        session_template="USEQPre",
        sleep_seconds=0,
    )
    assert client.templates == ["USEQPre"]


# ----------------------------------------------------------------------
# Coverage check: a short day must not pass silently
# ----------------------------------------------------------------------

# 2026-08-18 is a Tuesday.
_DAY = date(2026, 8, 18)
_NOON = datetime(2026, 8, 18, 12, 0, tzinfo=ET)


def _rows_from(first_et: datetime, count: int = 3):
    """``count`` consecutive 1-min rows starting at ``first_et``."""
    return [{"timestamp": first_et + timedelta(minutes=i)} for i in range(count)]


def test_coverage_gap_flags_a_day_that_starts_after_the_premarket_open():
    """The exact failure this tool shipped with: bars from 07:33, not 04:00."""
    rows = _rows_from(datetime(2026, 8, 18, 7, 33, tzinfo=ET))
    gaps = _coverage_gaps(rows, _DAY, _DAY, symbol="QQQ", now_et=_NOON)
    assert len(gaps) == 1
    day, expected, got = gaps[0]
    assert day == _DAY
    assert expected.hour == 4 and expected.minute == 0
    assert got.hour == 7 and got.minute == 33


def test_coverage_clean_when_the_day_starts_at_the_premarket_open():
    rows = _rows_from(datetime(2026, 8, 18, 4, 0, tzinfo=ET))
    assert _coverage_gaps(rows, _DAY, _DAY, symbol="QQQ", now_et=_NOON) == []


def test_coverage_tolerates_a_quiet_first_few_minutes():
    """Pre-market can open quiet; a handful of missing minutes is not a fault."""
    rows = _rows_from(datetime(2026, 8, 18, 4, 8, tzinfo=ET))
    assert _coverage_gaps(rows, _DAY, _DAY, symbol="QQQ", now_et=_NOON) == []


def test_coverage_is_symbol_aware_for_cash_indices():
    """NDX has no pre-market print — 09:30 is full coverage, not a gap."""
    rows = _rows_from(datetime(2026, 8, 18, 9, 30, tzinfo=ET))
    assert _coverage_gaps(rows, _DAY, _DAY, symbol="NDX", now_et=_NOON) == []
    # The same bars for an ETF are a gap: QQQ should have printed from 04:00.
    assert _coverage_gaps(rows, _DAY, _DAY, symbol="QQQ", now_et=_NOON) != []


def test_coverage_is_scored_against_the_chart_session_not_the_template_used():
    """A core-session fetch must not be able to mark its own day complete.

    Bars from 09:30 are exactly what ``--session-template Default`` returns.
    Scoring that run against its own template's 09:30 open would call it
    clean while the pre-market hole it was run to fill is still there.
    """
    rows = _rows_from(datetime(2026, 8, 18, 9, 30, tzinfo=ET))
    gaps = _coverage_gaps(rows, _DAY, _DAY, symbol="QQQ", now_et=_NOON)
    assert len(gaps) == 1
    assert gaps[0][1].hour == 4  # measured against the 04:00 ET pre-market open


def test_coverage_flags_a_trading_day_with_no_bars_at_all():
    gaps = _coverage_gaps([], _DAY, _DAY, symbol="QQQ", now_et=_NOON)
    assert len(gaps) == 1 and gaps[0][2] is None


def test_coverage_skips_weekends_and_sessions_that_have_not_opened_yet():
    # Sat 2026-08-15 / Sun 2026-08-16 carry no session at all.
    assert (
        _coverage_gaps([], date(2026, 8, 15), date(2026, 8, 16), symbol="QQQ", now_et=_NOON) == []
    )
    # Backfilling "today" before the 04:00 ET open is not a gap either.
    before_open = datetime(2026, 8, 18, 3, 0, tzinfo=ET)
    assert _coverage_gaps([], _DAY, _DAY, symbol="QQQ", now_et=before_open) == []


def test_coverage_skips_nyse_holidays(monkeypatch):
    monkeypatch.setenv("NYSE_HOLIDAYS", "2026-08-18")
    assert _coverage_gaps([], _DAY, _DAY, symbol="QQQ", now_et=_NOON) == []


def test_et_span_reports_the_window_actually_fetched():
    assert _et_span([]) == "no bars"
    rows = _rows_from(datetime(2026, 8, 18, 4, 0, tzinfo=ET), count=2)
    assert _et_span(rows) == "2026-08-18 04:00-04:01 ET"


# ----------------------------------------------------------------------
# An incomplete backfill must not read as a successful one
# ----------------------------------------------------------------------


def _install_fakes(monkeypatch, bars):
    """Wire ``backfill``'s TradeStation + DB dependencies to fakes."""
    import contextlib

    import src.tools.underlying_backfill as ub

    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "cid")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "csecret")
    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "crt")
    monkeypatch.setenv("NYSE_HOLIDAYS", "")
    monkeypatch.setattr(ub.time, "sleep", lambda *_a, **_k: None)

    class _Client:
        def get_bars(self, symbol, **kw):
            return {"Bars": bars}

    class _Cur:
        def executemany(self, sql, seq):
            self.seq = list(seq)

    class _Conn:
        def cursor(self):
            self._cur = getattr(self, "_cur", _Cur())
            return self._cur

    @contextlib.contextmanager
    def _fake_db():
        yield _Conn()

    monkeypatch.setattr(
        "src.ingestion.tradestation_client.TradeStationClient", lambda *a, **k: _Client()
    )
    monkeypatch.setattr("src.database.db_connection", _fake_db)
    return ub


def _premarket_bar(hh, mm):
    """A QQQ bar at ``hh:mm`` ET on 2026-08-18, stamped in UTC (EDT = UTC-4)."""
    return _bar(TimeStamp=f"2026-08-18T{hh + 4:02d}:{mm:02d}:00Z")


def test_backfill_reports_gaps_when_the_day_starts_after_the_open(monkeypatch):
    """The incident shape: bars arrive, but not from the 04:00 ET open."""
    ub = _install_fakes(monkeypatch, [_premarket_bar(7, 33)])
    gaps: dict = {}
    written = ub.backfill(["QQQ"], _DAY, _DAY, gaps=gaps, dry_run=True)
    assert written == {"QQQ": 0}
    assert gaps == {"QQQ": 1}


def test_backfill_reports_no_gaps_when_coverage_reaches_the_open(monkeypatch):
    ub = _install_fakes(monkeypatch, [_premarket_bar(4, 0), _premarket_bar(9, 30)])
    gaps: dict = {}
    ub.backfill(["QQQ"], _DAY, _DAY, gaps=gaps, dry_run=True)
    assert gaps == {"QQQ": 0}


def test_main_exits_non_zero_on_incomplete_coverage(monkeypatch):
    ub = _install_fakes(monkeypatch, [_premarket_bar(7, 33)])
    rc = ub.main(["--symbols", "QQQ", "--start", "2026-08-18", "--end", "2026-08-18", "--dry-run"])
    assert rc == 1


def test_main_exits_zero_when_the_premarket_open_is_covered(monkeypatch):
    ub = _install_fakes(monkeypatch, [_premarket_bar(4, 0)])
    rc = ub.main(["--symbols", "QQQ", "--start", "2026-08-18", "--end", "2026-08-18", "--dry-run"])
    assert rc == 0
