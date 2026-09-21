"""The daily replay's expiration scope: what a 0DTE frame is made of.

``/api/replay/range?expirations=0dte`` is not just a narrower ladder. The
levels drawn over that ladder -- call wall, put wall, gamma flip, max pain --
are persisted WHOLE-CHAIN on ``gex_summary``, so shipping them unchanged beside
0DTE bars would draw a line from a book that isn't on screen: a chart that says
"0DTE" over next week's call wall. These tests pin the re-derivation, and pin
the two levels that deliberately do NOT move with the scope.

The read is exercised through the real method with ``conn.fetch`` scripted,
same harness as ``test_replay_frames_read`` -- no Postgres needed, and the
query text that actually gets sent is captured for the predicate assertions.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import pytest

from src.api.database import DatabaseManager

SESSION = date(2026, 8, 20)
BAR = datetime(2026, 8, 20, 14, 0, tzinfo=timezone.utc)

# One minute of ladder rows, as the lateral hands them back. Hand-pickable:
#   call wall  -> 605 (largest call_gamma at-or-above spot 600)
#   put wall   -> 595 (largest put_gamma at-or-below spot)
#   flip       -> cumulative call-put ascending crosses between 600 and 605:
#                 595: -800, 600: -700, 605: +50  =>  600 + 5*700/750 = 604.666…
_LADDER = [
    (595.0, 100.0, 900.0),
    (600.0, 500.0, 400.0),
    (605.0, 800.0, 50.0),
]

MAX_PAIN_BY_EXP = {"2026-08-20": 601.0, "2026-08-21": 610.0}


def _dollars(gamma, spot):
    """Raw gamma -> dollar GEX per 1% move, NULL when the minute has no spot."""
    return None if spot is None else gamma * 100 * spot * spot * 0.01


def _rows(*, spot=600.0, max_pain_by_expiration=None, ladder=None):
    """Scripted result rows for one minute, in the query's column shape."""
    out = []
    for strike, call_gamma, put_gamma in ladder if ladder is not None else _LADDER:
        out.append(
            {
                "timestamp": BAR,
                # Stored whole-chain levels: every one of these must be
                # replaced under a scope, which is what the tests below check.
                "gamma_flip": 601.0,
                "call_wall": 650.0,
                "put_wall": 550.0,
                "max_pain": 599.0,
                "max_pain_by_expiration": max_pain_by_expiration,
                # Whole-chain by definition; these two must survive untouched.
                "pin_strike": 600.0,
                "pin_confidence": 0.4,
                "max_gamma_strike": 511.0,
                "spot": spot,
                "strike": strike,
                # The dollar columns are scaled by spot in SQL, so a minute
                # with no tape hands them back NULL alongside the raw gamma.
                "net_gex": _dollars(call_gamma - put_gamma, spot),
                "call_gex": _dollars(call_gamma, spot),
                "put_gex": _dollars(-put_gamma, spot),
                "call_gamma": call_gamma,
                "put_gamma": put_gamma,
            }
        )
    return out


def _run(rows, **kwargs):
    """Run the real read against scripted rows; returns (frames, query, args)."""
    db = DatabaseManager()
    captured: dict = {}

    class _Conn:
        async def fetch(self, query, *args):
            captured["query"] = query
            captured["args"] = args
            return rows

    @asynccontextmanager
    async def _acquire():
        yield _Conn()

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    frames = asyncio.run(db.get_gex_frames_for_session("SPY", SESSION, **kwargs))
    return frames, captured["query"], captured["args"]


# --------------------------------------------------------------------------
# The scope reaches the database
# --------------------------------------------------------------------------


def test_whole_chain_read_carries_no_expiration_predicate():
    """The default read must stay the exact statement it has always been.

    A NULL-guarded ``OR`` would have been fewer lines and would have made every
    whole-chain call carry a parameter that is NULL every time -- and this read
    has already cost one 31.9s timeout to a plan the session bound could not
    reach. The predicate is substituted in, so "All" is not a new plan.
    """
    _frames, query, args = _run(_rows())
    assert "gbs.expiration" not in query
    assert "{exp_predicate}" not in query, "the placeholder must be substituted"
    assert len(args) == 4


def test_scoped_read_binds_the_dates_as_an_array_parameter():
    _frames, query, args = _run(_rows(), expirations=[SESSION])
    assert "AND gbs.expiration = ANY($5::date[])" in query
    assert len(args) == 5
    assert args[4] == [SESSION]


def test_scope_is_normalised_before_it_is_bound():
    """Sorted + de-duped, so two spellings of one scope hit one plan."""
    dupes = [date(2026, 8, 21), SESSION, date(2026, 8, 21)]
    _frames, _query, args = _run(_rows(), expirations=dupes)
    assert args[4] == [SESSION, date(2026, 8, 21)]


def test_an_empty_scope_list_reads_the_whole_chain():
    """``expirations=[]`` is "no filter", never "no expirations"."""
    _frames, query, args = _run(_rows(), expirations=[])
    assert "gbs.expiration" not in query
    assert len(args) == 4


# --------------------------------------------------------------------------
# The levels follow the scope
# --------------------------------------------------------------------------


def test_whole_chain_frame_keeps_the_stored_levels():
    frames, _q, _a = _run(_rows())
    frame = frames[0]
    assert frame["call_wall"] == 650.0
    assert frame["put_wall"] == 550.0
    assert frame["gamma_flip"] == 601.0
    assert frame["max_pain"] == 599.0


def test_scoped_frame_rederives_walls_and_flip_from_the_scoped_ladder():
    """The stored walls describe the whole chain; the bars no longer do.

    Both walls and the flip come back from the canonical helpers in
    src.analytics.walls, run over exactly the rows the chart draws -- the same
    treatment /api/gex/strike-profile-timeseries gives its own filter, so a
    0DTE replay and a 0DTE rewind cannot disagree about where the wall sat.
    """
    frames, _q, _a = _run(_rows(max_pain_by_expiration=MAX_PAIN_BY_EXP), expirations=[SESSION])
    frame = frames[0]
    assert frame["call_wall"] == pytest.approx(605.0)
    assert frame["put_wall"] == pytest.approx(595.0)
    assert frame["gamma_flip"] == pytest.approx(604.6666667)


def test_scoped_frame_quotes_the_scopes_own_max_pain():
    """Not the front-month scalar: this expiration's own settlement."""
    frames, _q, _a = _run(_rows(max_pain_by_expiration=MAX_PAIN_BY_EXP), expirations=[SESSION])
    assert frames[0]["max_pain"] == pytest.approx(601.0)


def test_max_pain_survives_a_jsonb_column_handed_back_as_text():
    """asyncpg returns JSONB as str unless a codec is registered."""
    frames, _q, _a = _run(
        _rows(max_pain_by_expiration=json.dumps(MAX_PAIN_BY_EXP)),
        expirations=[SESSION],
    )
    assert frames[0]["max_pain"] == pytest.approx(601.0)


@pytest.mark.parametrize(
    "scope, stored",
    [
        # Two settlements have no single max pain.
        ([SESSION, date(2026, 8, 21)], MAX_PAIN_BY_EXP),
        # Column never backfilled on this row.
        ([SESSION], None),
        # The scope's date isn't in the breakdown.
        ([date(2026, 8, 24)], MAX_PAIN_BY_EXP),
        # Unparseable payload.
        ([SESSION], "{not json"),
    ],
)
def test_max_pain_is_null_rather_than_another_expirations_level(scope, stored):
    """Null draws no line. The stored 599 would draw the wrong one."""
    frames, _q, _a = _run(_rows(max_pain_by_expiration=stored), expirations=scope)
    assert frames[0]["max_pain"] is None


def test_pin_and_gex_king_do_not_move_with_the_scope():
    """Both are whole-chain by definition, and stay stored under any scope.

    The pin is 0DTE-by-construction (it models into-expiration hedging) and the
    GEX King is the whole-chain heaviest node. Recomputing either from a scoped
    ladder would make the replay disagree with the live charts, where neither
    follows the Expiry selector.
    """
    frames, _q, _a = _run(_rows(max_pain_by_expiration=MAX_PAIN_BY_EXP), expirations=[SESSION])
    frame = frames[0]
    assert frame["pin_strike"] == 600.0
    assert frame["pin_confidence"] == 0.4
    assert frame["max_gamma_strike"] == 511.0


def test_a_minute_with_no_tape_gets_null_levels_not_stale_ones():
    """No spot means nothing to rank walls against.

    The stored whole-chain levels are not a fallback -- they are the answer to
    a different question, and the scrubber already omits a level it has no
    value for.
    """
    frames, _q, _a = _run(
        _rows(spot=None, max_pain_by_expiration=MAX_PAIN_BY_EXP), expirations=[SESSION]
    )
    frame = frames[0]
    assert frame["call_wall"] is None
    assert frame["put_wall"] is None
    assert frame["gamma_flip"] is None


def test_a_session_with_no_contracts_in_scope_keeps_its_frames():
    """0DTE on a day whose chain had no same-day expiry.

    The lateral is a LEFT JOIN, so the minute still arrives -- with a NULL
    strike. It must keep its frame (empty ladder, null levels) so the client
    can say "no 0DTE contracts that session" instead of the page silently
    rendering the whole chain.
    """
    row = _rows(max_pain_by_expiration=MAX_PAIN_BY_EXP)[0]
    row.update(
        {
            "strike": None,
            "net_gex": None,
            "call_gex": None,
            "put_gex": None,
            "call_gamma": None,
            "put_gamma": None,
        }
    )
    frames, _q, _a = _run([row], expirations=[SESSION])
    assert len(frames) == 1
    assert frames[0]["strikes"] == []
    assert frames[0]["call_wall"] is None
    assert frames[0]["gamma_flip"] is None


def test_scoped_frames_do_not_leak_scratch_keys():
    """The rescope inputs are internal; the payload shape is unchanged."""
    frames, _q, _a = _run(_rows(max_pain_by_expiration=MAX_PAIN_BY_EXP), expirations=[SESSION])
    assert not [k for k in frames[0] if k.startswith("_")]
    assert set(frames[0]) == {
        "timestamp",
        "gamma_flip",
        "call_wall",
        "put_wall",
        "max_pain",
        "pin_strike",
        "pin_confidence",
        "max_gamma_strike",
        "strikes",
    }
