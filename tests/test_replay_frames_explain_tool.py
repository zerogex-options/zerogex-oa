"""The replay-frames EXPLAIN diagnostic emits a script that actually checks.

``make replay-frames-explain`` exists to answer one question against a real
database: is the frames read fenced to the session, or does it scale with how
much history ``gex_by_strike`` holds?  Two properties make it able to answer
that, and both are easy to break by accident:

1. It EXPLAINs the LIVE query, extracted from ``src/api/database.py``. A copy
   pasted into the tool would keep passing while the API ran something else.

2. It runs the read a second time with ``enable_nestloop = off``. The failure
   is plan-dependent, so a single run under whatever plan the planner happens
   to pick today proves nothing -- the forced-fallback run is the whole point.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.tools.replay_frames_explain import build_script, main

SESSION = date(2026, 7, 24)


def _script(symbol: str = "NDX", session: date = SESSION) -> str:
    return build_script(symbol, session, 0.04, 120000)


def test_script_explains_the_live_query_not_a_copy():
    """The SQL under EXPLAIN must come from database.py.

    Pinned by a fragment that only exists in the real query — if the tool ever
    grows its own hardcoded copy, this fails rather than silently EXPLAINing
    something the API does not run.
    """
    sql = _script()
    assert "session_summary AS MATERIALIZED (" in sql
    assert "LEFT JOIN LATERAL (" in sql
    assert "AVG(gbs.call_gamma * 100 * s.spot * s.spot * 0.01)" in sql


def test_script_forces_the_fallback_plan():
    """The decisive run: without this, the diagnostic only reports today's luck."""
    sql = _script()
    assert "SET enable_nestloop = off;" in sql
    assert "RESET enable_nestloop;" in sql
    # Forced-fallback EXPLAIN comes after the free-planner one, and both run.
    assert sql.count("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)") == 2
    assert sql.index("SET enable_nestloop = off;") < sql.rindex(
        "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)"
    )


def test_script_is_bounded_and_read_only():
    """EXPLAIN ANALYZE executes the query, so it must not be able to camp on a
    production connection — and it must never write."""
    sql = _script()
    assert "SET statement_timeout = 120000;" in sql
    assert "RESET statement_timeout;" in sql
    for forbidden in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE "):
        assert forbidden not in sql.upper(), f"diagnostic must be read-only: {forbidden}"


def test_window_is_the_et_cash_session_converted_to_utc():
    """09:30-16:01 ET, DST-aware — the same window the method itself builds.

    July is EDT (UTC-4), so the session opens 13:30Z. A naive UTC treatment
    would probe the wrong 6.5 hours and quietly EXPLAIN an empty session.
    """
    sql = _script()
    assert "'2026-07-24T13:30:00+00:00'::timestamptz" in sql
    assert "'2026-07-24T20:01:00+00:00'::timestamptz" in sql


def test_winter_session_shifts_with_est():
    """The same date in January is EST (UTC-5): 14:30Z, not 13:30Z."""
    sql = _script(session=date(2026, 1, 14))
    assert "'2026-01-14T14:30:00+00:00'::timestamptz" in sql
    assert "'2026-01-14T21:01:00+00:00'::timestamptz" in sql


def test_symbol_is_bound_everywhere_the_query_takes_it():
    sql = _script(symbol="SPX")
    assert "'SPX'" in sql
    assert "$1" not in sql and "$2" not in sql and "$3" not in sql and "$4" not in sql


def test_amplification_query_compares_session_rows_to_total_rows():
    """Step [1] is what turns the plan into a number an operator can act on."""
    sql = _script()
    assert "AS total_rows" in sql
    assert "AS session_rows" in sql


def test_bad_date_is_rejected_rather_than_silently_probing_nothing():
    with pytest.raises(SystemExit):
        main(["--symbol", "NDX", "--date", "07/24/2026"])


def test_no_echo_line_can_be_mislexed_by_psql():
    """Regression: an apostrophe in \\echo prose truncates the legend.

    psql parses meta-command arguments, so ``session's`` opens a quoted string
    that never closes. The first production run printed ``unterminated quoted
    string`` and swallowed the rest of that line plus the next -- two lines of
    the PASS/FAIL legend, i.e. exactly the text an operator needs in order to
    read the plan. The output still looked broadly fine, which is what makes it
    worth a test rather than a careful eye.
    """
    for line in _script().splitlines():
        if line.startswith("\\echo"):
            assert "'" not in line, f"psql will mis-lex this \\echo line: {line!r}"


def test_echo_helper_rejects_an_apostrophe_at_the_source():
    """The guard lives at the point of authorship, not just in this test."""
    from src.tools.replay_frames_explain import _echo

    assert _echo("no quotes here") == "\\echo no quotes here"
    with pytest.raises(ValueError, match="apostrophe"):
        _echo("the session's minute count")
