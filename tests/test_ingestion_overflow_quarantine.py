"""One unstorable value must not stop the table from writing.

On 2026-10-14 an NDX batch hit Postgres's "numeric field overflow" at 15:51
ET. ``execute_values`` sends a batch as one statement, and the ``option_chains``
and ``option_chains_latest`` upserts share one transaction, so the single bad
row rolled back every good row beside it. ``_retain_failed_option_rows`` then
put the bad row in the pending buffer, the next flush re-sent it, and it failed
the same way. A poison pill, not a retry: NDX stopped persisting for the last
nine minutes of the session, and the only thing that could ever have evicted
the row was the buffer's 20,000-row cap, which it never reached (it got to
14,281).

The fix does not need to know which column overflows -- which matters, because
two further rehearsal sessions (10-15, and its own re-run) never reproduced it.
It drops the rows that cannot be stored and writes the rest, so the loss is one
contract for one minute rather than every contract until restart.

These tests pin that, and pin just as hard the cases where it must NOT salvage:
too much of the batch, no identifiable offender, nothing left to keep, a
different error, or a second overflow inside its own retry.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime

import pytz

import src.ingestion.main_engine as me
from src.ingestion.main_engine import IngestionEngine

ET = pytz.timezone("US/Eastern")
BUCKET = ET.localize(datetime(2026, 10, 14, 15, 51, 0))

#: Index of ``theta`` in the values tuple the writer builds, counted off the
#: upsert's column list. The fake uses it to behave like Postgres: the
#: STATEMENT fails if any row in it is out of range, not just that row.
_THETA = 18

#: NUMERIC(10, 6): values must stay under 10^(10-6).
_THETA_BOUND = 1e4


class _FakeCursor:
    pass


class _FakeConn:
    def cursor(self):
        return _FakeCursor()

    def commit(self):
        pass


class _OverflowDB:
    """Postgres's behavior for an out-of-range NUMERIC, not a flag.

    Raising on "the first call" would prove only that something was retried.
    Failing *while a bad row is in the batch* is what makes the retry's success
    evidence that the offending row is gone.
    """

    def __init__(self, bound: float = _THETA_BOUND, always: bool = False):
        self.bound = bound
        self.always = always
        self.batches: list[list[tuple]] = []
        self.attempts = 0

    @contextlib.contextmanager
    def connection(self):
        yield _FakeConn()

    def execute_values(self, cursor, sql, values, page_size=None):
        values = list(values)
        self.attempts += 1
        if self.always or any(abs(float(v[_THETA])) >= self.bound for v in values):
            # psycopg2 surfaces this as DataError; the writer keys off the
            # message, and matching the text is what the production path sees.
            raise RuntimeError("numeric field overflow")
        if "INSERT INTO option_chains_latest" not in sql:
            self.batches.append(values)

    @property
    def persisted_symbols(self) -> set[str]:
        return {row[0] for batch in self.batches for row in batch}


def _install(monkeypatch, db: _OverflowDB) -> _OverflowDB:
    monkeypatch.setattr(me, "db_connection", db.connection)
    monkeypatch.setattr(me, "execute_values", db.execute_values)
    return db


def _engine() -> IngestionEngine:
    """Minimal stub driving the real _write_option_rows.

    Deliberately does NOT seed ``_in_overflow_quarantine`` or
    ``_overflow_quarantined_rows``: the quarantine reads both through
    ``getattr``, and a stub built by ``__new__`` is how the rest of this
    suite builds engines.
    """
    e = IngestionEngine.__new__(IngestionEngine)
    e._pending_failed_option_rows = []
    e._pending_failed_option_rows_max = 20000
    e._db_backoff_until = 0.0
    e._db_consecutive_failures = 0
    e.errors_count = 0
    e.option_quotes_stored = 0
    e.last_flush_time = None
    e._obs_batches_written = 0
    e._obs_rows_written = 0
    e._obs_write_time_ms = 0.0
    e._obs_last_log = 0.0
    return e


def _agg(option_symbol: str, theta: float = -0.1) -> dict:
    return {
        "option_symbol": option_symbol,
        "timestamp": BUCKET,
        "underlying": "NDX",
        "strike": 30000.0,
        "expiration": "2026-10-14",
        "option_type": "C",
        "last": 5.0,
        "bid": 4.9,
        "ask": 5.1,
        "mid": 5.0,
        "volume": 10,
        "open_interest": 100,
        "implied_volatility": 0.3,
        "ask_volume": 5,
        "mid_volume": 0,
        "bid_volume": 5,
        "delta": -0.5,
        "gamma": 0.01,
        "theta": theta,
        "vega": 0.2,
        "charm": 0.03,
        "vanna": -0.02,
    }


def test_theta_is_where_the_fake_thinks_it_is(monkeypatch):
    """If the values tuple is reordered, every test here would pass while
    testing nothing. Pin the index against a row built for the purpose."""
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    engine._write_option_rows([_agg("NDX_A", theta=-1234.5)])

    assert db.batches, "the clean write did not reach the fake"
    assert db.batches[0][0][_THETA] == -1234.5


# ── it salvages ─────────────────────────────────────────────────────────────
def test_one_absurd_greek_no_longer_stops_the_whole_batch(monkeypatch):
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    engine._write_option_rows(
        [_agg("NDX_GOOD_1"), _agg("NDX_BAD", theta=-48213.7), _agg("NDX_GOOD_2")]
    )

    assert db.persisted_symbols == {"NDX_GOOD_1", "NDX_GOOD_2"}
    assert engine.option_quotes_stored == 2


def test_the_quarantined_row_is_not_retained_to_poison_the_next_flush(monkeypatch):
    """The whole mechanism of the 2026-10-14 outage. If the bad row goes back
    into the pending buffer, the next flush re-sends it and nothing improved."""
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    engine._write_option_rows([_agg("NDX_GOOD"), _agg("NDX_BAD", theta=-48213.7)])

    assert engine._pending_failed_option_rows == []


def test_a_salvaged_write_does_not_trip_the_breaker(monkeypatch):
    """A write that succeeded is not an outage. Counting it would back off and
    skip the NEXT flush too, losing good rows to a row already discarded."""
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    engine._write_option_rows([_agg("NDX_GOOD"), _agg("NDX_BAD", theta=-48213.7)])

    assert engine._db_consecutive_failures == 0
    assert engine._db_backoff_until == 0.0
    assert engine.errors_count == 0


def test_the_log_names_the_contract_and_the_column(monkeypatch, caplog):
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    with caplog.at_level(logging.ERROR, logger=me.logger.name):
        engine._write_option_rows([_agg("NDX_GOOD"), _agg("NDX_BAD", theta=-48213.7)])

    text = caplog.text
    assert "NDX_BAD" in text
    assert "theta" in text
    assert "NDX_GOOD" not in text, "the good contract is not the offender"


def test_the_running_count_of_quarantined_rows_is_reported(monkeypatch):
    """A silent drop that recurs every day is a data-loss bug nobody sees."""
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    engine._write_option_rows([_agg("NDX_GOOD"), _agg("NDX_BAD", theta=-48213.7)])
    engine._write_option_rows([_agg("NDX_GOOD_2"), _agg("NDX_BAD_2", theta=-99999.0)])

    assert engine._overflow_quarantined_rows == 2


# ── it refuses to salvage ───────────────────────────────────────────────────
def test_too_many_offenders_fails_loudly_instead_of_discarding(monkeypatch, caplog):
    """30 bad rows in 100 is not a stray expiry-day greek. Quarantining that
    would destroy real data and hide whatever caused it."""
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()
    rows = [_agg(f"NDX_OK_{i}") for i in range(70)]
    rows += [_agg(f"NDX_BAD_{i}", theta=-50000.0) for i in range(30)]

    with caplog.at_level(logging.ERROR, logger=me.logger.name):
        engine._write_option_rows(rows)

    assert "NOT quarantining" in caplog.text
    assert len(engine._pending_failed_option_rows) == 100
    assert engine._db_consecutive_failures == 1
    assert db.batches == []


def test_an_overflow_with_no_identifiable_offender_falls_through(monkeypatch):
    """The bounds table is maintained by hand against schema.sql. If it has
    fallen behind, there is no row to drop and guessing would be worse."""
    db = _install(monkeypatch, _OverflowDB(always=True))
    engine = _engine()
    rows = [_agg("NDX_A"), _agg("NDX_B")]

    engine._write_option_rows(rows)

    assert len(engine._pending_failed_option_rows) == 2
    assert engine._db_consecutive_failures == 1


def test_a_single_bad_row_has_nothing_to_salvage_and_fails_normally(monkeypatch):
    db = _install(monkeypatch, _OverflowDB())
    engine = _engine()

    engine._write_option_rows([_agg("NDX_BAD", theta=-48213.7)])

    assert len(engine._pending_failed_option_rows) == 1
    assert engine._db_consecutive_failures == 1


def test_a_failure_that_is_not_an_overflow_is_untouched(monkeypatch):
    class _DownDB(_OverflowDB):
        def execute_values(self, cursor, sql, values, page_size=None):
            self.attempts += 1
            raise RuntimeError("simulated DB down (execute)")

    db = _install(monkeypatch, _DownDB())
    engine = _engine()
    rows = [_agg("NDX_GOOD"), _agg("NDX_BAD", theta=-48213.7)]

    engine._write_option_rows(rows)

    assert len(engine._pending_failed_option_rows) == 2, "a DB outage still retains"
    assert engine._db_consecutive_failures == 1
    assert db.attempts == 1, "no salvage retry on an error it does not understand"


def test_it_does_not_recurse_when_the_retry_also_overflows(monkeypatch):
    """A bounds table that covers SOME of the bad values would otherwise
    quarantine, retry, overflow, quarantine... until the stack gave out."""
    db = _install(monkeypatch, _OverflowDB(always=True))
    engine = _engine()
    rows = [_agg("NDX_GOOD"), _agg("NDX_BAD", theta=-48213.7)]

    engine._write_option_rows(rows)

    assert db.attempts <= 2, f"retried {db.attempts} times; one salvage only"
    assert engine._db_consecutive_failures == 1
    # The kept row is retained by the inner write, so it is tried again later.
    assert [r["option_symbol"] for r in engine._pending_failed_option_rows] == ["NDX_GOOD"]
