"""Coverage for the ES/NQ historical backfill.

The live ingester only holds a rolling window, so everything ES/NQ wants
beyond intraday — the daily and hourly candlestick timeframes, a basis history
spanning a quarterly roll, any replay or backtest over futures price action —
depends on this tool.

Two properties matter more than the fetch loop:

* a backfilled bar must be **indistinguishable from a streamed one**, which
  means the same bar-start stamping. Getting this wrong puts backfilled bars a
  minute ahead of live ones and silently mis-pairs the index/future basis join;
* a range outside ``FUTURES_BARS_RETENTION_DAYS`` must **warn**, because the
  ingester's prune cannot tell a backfilled row from a streamed one and will
  delete the work without saying anything.
"""

import logging
from datetime import date, datetime, timedelta, timezone

import pytest

from src.tools import futures_backfill as fb


def _bar(ts: str, **over):
    raw = {"TimeStamp": ts, "Open": 6600, "High": 6610, "Low": 6590, "Close": 6605}
    raw.update(over)
    return raw


# --- stamping: the property the basis join depends on ----------------------


def test_bars_are_stamped_on_their_own_minute_like_the_live_ingester():
    """TradeStation stamps a bar at its CLOSE. Storing that raw would put
    futures_quotes a minute ahead of underlying_quotes and pair each futures
    bar with the NEXT minute's index print."""
    from src.ingestion.futures_underlying_ingester import _parse_bar as live_parse

    raw = _bar("2026-08-19T14:31:00Z")
    assert fb._bar_to_row(raw)["timestamp"] == live_parse(raw)["timestamp"]
    assert fb._bar_to_row(raw)["timestamp"] == datetime(2026, 8, 19, 14, 30, tzinfo=timezone.utc)


def test_degenerate_bars_are_skipped_not_written():
    """futures_quotes enforces positive prices and high >= low."""
    assert fb._bar_to_row(_bar("2026-08-19T14:31:00Z", Open=0)) is None
    assert fb._bar_to_row(_bar("2026-08-19T14:31:00Z", High=1, Low=2)) is None
    assert fb._bar_to_row(_bar("2026-08-19T14:31:00Z", Close=None)) is None
    assert fb._bar_to_row({"Open": 1, "High": 1, "Low": 1, "Close": 1}) is None


def test_streaming_only_volume_fields_default_to_zero():
    """The historical endpoint carries no Up/Down split; OHLC is exact."""
    row = fb._bar_to_row(_bar("2026-08-19T14:31:00Z"))
    assert row["up_volume"] == 0 and row["down_volume"] == 0
    assert (row["open"], row["high"], row["low"], row["close"]) == (6600, 6610, 6590, 6605)


# --- retention: the silent way a backfill gets thrown away -----------------


def test_a_range_outside_retention_warns_loudly(caplog, monkeypatch):
    monkeypatch.setenv("FUTURES_BARS_RETENTION_DAYS", "7")
    with caplog.at_level(logging.WARNING):
        fb._warn_if_retention_will_delete(date.today() - timedelta(days=90))
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "WILL BE DELETED" in text
    assert "FUTURES_BARS_RETENTION_DAYS" in text


def test_a_range_inside_retention_is_silent(caplog, monkeypatch):
    monkeypatch.setenv("FUTURES_BARS_RETENTION_DAYS", "90")
    with caplog.at_level(logging.WARNING):
        fb._warn_if_retention_will_delete(date.today() - timedelta(days=5))
    assert not [r for r in caplog.records if "WILL BE DELETED" in r.getMessage()]


# --- fetch loop ------------------------------------------------------------


class _FakeClient:
    """Returns two bars per chunk, one of them a duplicate across chunks."""

    def __init__(self):
        self.calls = []

    def get_bars(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        return {
            "Bars": [
                _bar("2026-08-19T14:31:00Z"),
                _bar("2026-08-19T14:32:00Z", Close=6606),
            ]
        }


def test_fetch_uses_the_historical_endpoint_with_a_bounded_range():
    client = _FakeClient()
    rows = fb.fetch_future(client, "@ES", date(2026, 8, 1), date(2026, 8, 21), sleep_seconds=0)
    assert client.calls, "no request issued"
    symbol, kwargs = client.calls[0]
    assert symbol == "@ES"
    # firstdate/lastdate are what make this a backfill rather than a snapshot.
    assert kwargs["firstdate"] and kwargs["lastdate"]
    assert kwargs["interval"] == 1 and kwargs["unit"] == "Minute"
    assert rows, "no bars parsed"


def test_duplicate_timestamps_across_chunks_are_deduplicated():
    client = _FakeClient()
    rows = fb.fetch_future(
        client, "@ES", date(2026, 6, 1), date(2026, 8, 21), days_per_chunk=25, sleep_seconds=0
    )
    assert len(client.calls) > 1, "range should have been chunked"
    stamps = [r["timestamp"] for r in rows]
    assert len(stamps) == len(set(stamps))


def test_an_index_with_no_mapped_future_is_skipped_not_crashed(monkeypatch, caplog):
    monkeypatch.setenv("INDEX_FUTURES_MAP", "SPX=@ES")  # NDX deliberately absent
    monkeypatch.setattr(fb, "resolve_index_future", lambda s: "@ES" if s == "SPX" else None)

    class _NoClient:
        def get_bars(self, *a, **k):
            return {"Bars": []}

    monkeypatch.setattr(
        "src.ingestion.tradestation_client.TradeStationClient", lambda *a, **k: _NoClient()
    )
    with caplog.at_level(logging.ERROR):
        written = fb.backfill(["NDX"], date(2026, 8, 1), date(2026, 8, 2), dry_run=True)
    assert written == 0
    assert any("INDEX_FUTURES_MAP" in r.getMessage() for r in caplog.records)


def test_end_before_start_is_rejected_by_the_cli():
    assert fb.main(["--symbols", "SPX", "--start", "2026-08-21", "--end", "2026-08-01"]) == 2


@pytest.mark.parametrize("bad", ["not-a-date", "2026-13-01"])
def test_malformed_dates_are_rejected_by_the_cli(bad):
    assert fb.main(["--symbols", "SPX", "--start", bad, "--end", "2026-08-21"]) == 2
