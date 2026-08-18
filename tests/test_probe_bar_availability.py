"""Tests for the bar-availability probe.

The HTTP path needs live credentials; the pure logic — ET day bounds, the
probe matrix, earliest-bar extraction, and the failure-tolerant loop — is
exercised here with fakes (mirroring test_underlying_backfill.py).
"""

from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

from src.tools.probe_bar_availability import (
    _earliest_et,
    _et_day_bounds,
    _probes,
    probe,
)

ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 18)


def _bar(hh: int, mm: int, day: date = DAY):
    """A bar stamped in UTC for ``hh:mm`` ET (EDT = UTC-4 in August)."""
    return {"TimeStamp": f"{day.isoformat()}T{hh + 4:02d}:{mm:02d}:00Z"}


def test_et_day_bounds_span_the_whole_et_day():
    first, last = _et_day_bounds(DAY)
    assert first == "2026-08-18T04:00:00Z"  # 00:00 ET
    assert last == "2026-08-19T03:59:59Z"  # 23:59 ET


def test_earliest_et_picks_the_first_bar_and_converts_to_et():
    earliest = _earliest_et([_bar(9, 30), _bar(7, 35), _bar(11, 0)])
    assert earliest is not None
    assert (earliest.hour, earliest.minute) == (7, 35)


def test_earliest_et_ignores_other_days_and_junk():
    bars = [_bar(4, 0, date(2026, 8, 17)), {"TimeStamp": "nonsense"}, {}, _bar(7, 35)]
    earliest = _earliest_et(bars, DAY)
    assert earliest is not None and (earliest.hour, earliest.minute) == (7, 35)
    assert _earliest_et([], DAY) is None


def test_probe_matrix_varies_one_axis_at_a_time():
    rows = _probes(DAY)
    # Three rows differ only by template, over the identical full-day range.
    day_rows = [r for r in rows if r[0] == "full ET day"]
    assert [r[1] for r in day_rows] == ["Default", "USEQ24Hour", "USEQPre"]
    assert len({str(r[2]) for r in day_rows}) == 1
    # And one row swaps the query path entirely (barsback, not firstdate).
    assert any("barsback" in kwargs for _l, _t, kwargs in rows)
    assert any("firstdate" in kwargs for _l, _t, kwargs in rows)


class _FakeClient:
    """Serves bars from ``start_et`` onward, whatever is asked for."""

    def __init__(self, start_hh=7, start_mm=35):
        self.calls = []
        self._start = (start_hh, start_mm)

    def get_bars(self, symbol, **kw):
        self.calls.append(kw)
        hh, mm = self._start
        return {"Bars": [_bar(hh, mm), _bar(hh + 1, mm)]}


def test_probe_runs_every_row_and_reports_the_earliest_bar():
    client = _FakeClient()
    results = probe(client, "QQQ", DAY)
    assert len(results) == len(_probes(DAY))
    assert len(client.calls) == len(_probes(DAY))
    # Every row agrees on 07:35 — the signature of an upstream gap rather
    # than a request the tool got wrong.
    for _label, earliest, count in results:
        assert earliest is not None
        assert (earliest.hour, earliest.minute) == (7, 35)
        assert count == 2


def test_probe_labels_name_the_template_and_the_window():
    labels = [label for label, _e, _c in probe(_FakeClient(), "QQQ", DAY)]
    assert any(lbl.startswith("Default") and "full ET day" in lbl for lbl in labels)
    assert any("barsback" in lbl for lbl in labels)


class _FailingClient:
    def get_bars(self, symbol, **kw):
        raise RuntimeError("session template rejected")


def test_probe_reports_a_rejected_request_instead_of_raising():
    """A rejected template is an answer, not a crash — the run must finish."""
    results = probe(_FailingClient(), "QQQ", DAY)
    assert len(results) == len(_probes(DAY))
    assert all(count == -1 and earliest is None for _l, earliest, count in results)
