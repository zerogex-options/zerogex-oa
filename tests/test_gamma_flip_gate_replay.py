"""Naming a gate is only useful if the naming cannot be wrong by construction.

The replay answers "which gate rejected this crossing" by relaxing one knob at
a time and seeing which relaxation publishes. Three things would quietly make
that answer a lie, and they are what these tests hold:

* a relaxation that LEAKS -- if a patched knob is not restored, every later
  relaxation runs under the previous one and the first gate tried gets blamed
  for all of them;
* the CONTROL counted as a culprit -- "all gates off" publishes by
  construction, so including it would name a gate in every single run;
* SAMPLING only the head of a session -- a blackout that starts at the open
  and one that starts at noon can have different causes, and replaying the
  first three rows would never show the second kind.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import pytz

from src.tools import gamma_flip_gate_replay as tool

ET = pytz.timezone("America/New_York")
OPEN = ET.localize(datetime(2026, 9, 17, 9, 30))


def _result(**overrides):
    base = dict(
        symbol="NDX",
        timestamp=OPEN,
        spot=23_000.0,
        contracts=900,
        baseline_flip=None,
        baseline_span=0.25,
        stored_flip=None,
        stored_raw=22_800.0,
        relaxed=(
            ("interior margin off", None),
            ("structural floor off", 22_750.0),
            ("distance ceiling off", None),
            ("active-strike filter loosened", None),
            ("DTE ramp off", None),
            ("all gates off (control)", 22_750.0),
        ),
        diagnostics={},
    )
    base.update(overrides)
    return tool.ReplayResult(**base)


# --- the patch must not leak -----------------------------------------------


def test_patched_restores_every_knob_it_touched():
    module = SimpleNamespace(A=1.0, B=False)
    with tool._patched(module, {"A": 0.0, "B": True}):
        assert (module.A, module.B) == (0.0, True)
    assert (module.A, module.B) == (1.0, False)


def test_patched_restores_even_when_the_replay_raises():
    """A resolver that throws on a degraded chain must not poison the next gate."""
    module = SimpleNamespace(A=1.0)
    with pytest.raises(RuntimeError):
        with tool._patched(module, {"A": 0.0}):
            raise RuntimeError("resolver blew up")
    assert module.A == 1.0


def test_every_relaxation_names_knobs_the_engine_actually_reads():
    """A typo'd knob name would patch nothing and silently never publish."""
    from src.analytics import main_engine

    for _label, overrides in tool.RELAXATIONS:
        for name in overrides:
            assert hasattr(main_engine, name), name


# --- the control is not a culprit ------------------------------------------


def test_the_control_is_never_reported_as_a_gate():
    result = _result(
        relaxed=(
            ("interior margin off", None),
            ("all gates off (control)", 22_750.0),
        )
    )
    assert result.culprits == []


def test_a_single_relaxation_that_publishes_is_the_culprit():
    assert _result().culprits == ["structural floor off"]


def test_several_publishing_relaxations_are_all_reported():
    result = _result(
        relaxed=(
            ("structural floor off", 22_750.0),
            ("DTE ramp off", 22_810.0),
            ("all gates off (control)", 22_750.0),
        )
    )
    assert result.culprits == ["structural floor off", "DTE ramp off"]


# --- sampling spreads across the blackout ----------------------------------


class _Cursor:
    """Returns one blank row per minute for a whole session."""

    def __init__(self, count):
        self._rows = [(OPEN + timedelta(minutes=i), 22_800.0 + i) for i in range(count)]
        self.count = count

    def execute(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        return list(self._rows)


def test_samples_are_spread_across_the_blackout_not_taken_from_its_head():
    picked = tool.blank_timestamps(_Cursor(300), "NDX", OPEN.date(), samples=3)
    assert len(picked) == 3
    minutes = [(ts - OPEN).total_seconds() / 60 for ts, _raw in picked]
    assert minutes == [0, 100, 200]


def test_a_short_blackout_is_replayed_whole():
    picked = tool.blank_timestamps(_Cursor(2), "NDX", OPEN.date(), samples=5)
    assert len(picked) == 2


def test_a_session_with_no_blank_rows_has_nothing_to_replay():
    assert tool.blank_timestamps(_Cursor(0), "NDX", OPEN.date(), samples=3) == []


def test_zero_samples_asks_for_nothing_and_gets_nothing():
    assert tool.blank_timestamps(_Cursor(300), "NDX", OPEN.date(), samples=0) == []


# --- the report states only what the cycles support ------------------------


def test_a_gate_shared_by_every_blank_cycle_is_stated_as_the_answer():
    body = "\n".join(tool.format_report([_result(), _result()]))
    assert "structural floor off" in body
    assert "no single relaxation" not in body


def test_cycles_with_different_causes_are_not_collapsed_into_one_answer():
    other = _result(
        relaxed=(
            ("distance ceiling off", 21_000.0),
            ("all gates off (control)", 21_000.0),
        )
    )
    body = "\n".join(tool.format_report([_result(), other]))
    assert "no single relaxation" in body


def test_a_replay_that_publishes_where_production_did_not_says_so():
    """The chain moved under us; that cycle explains nothing and must admit it."""
    body = "\n".join(tool.format_report([_result(baseline_flip=22_900.0)]))
    assert "PUBLISHED where production did not" in body
    assert "cannot be reproduced" in body


def test_nothing_to_replay_is_a_sentence_not_an_empty_report():
    assert tool.format_report([]) == [
        "nothing to replay: no blank cycles found in the requested window"
    ]


def test_diagnostics_render_when_fields_are_missing_or_none():
    """These are printed for exactly the degraded cycles that lack the fields."""
    result = _result(diagnostics={"usable_total": 12, "profile_peak": None})
    body = "\n".join(tool.format_report([result]))
    assert "usable=12" in body
    assert "peak=-" in body
