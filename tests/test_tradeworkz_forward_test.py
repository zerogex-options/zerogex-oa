"""Forward tests: the catalog's ``forward_test`` flag puts a bot on paper.

The lane exists so a strategy that history cannot settle (too few trades in
the chain archive) earns its record live. What it must guarantee:

* provisioning funds and enables exactly the flagged bots, refunds a sleeve
  that a disable zeroed, and still disables everything that shipped and is no
  longer live;
* the auto-disable circuit breaker never stops a forward test (the flag is
  the switch);
* the API tells the website which bots are tests.
"""

from __future__ import annotations

from src.tradeworkz import engine
from src.tradeworkz.ml import apply_auto_disable_guard
from src.tradeworkz.registry import (
    DISABLED_BOT_IDS,
    FORWARD_TEST_SPECS,
    is_forward_test_bot,
)


class _RecCursor:
    def __init__(self):
        self.rowcount = 1
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _RecConn:
    def __init__(self):
        self._cur = _RecCursor()

    def cursor(self):
        return self._cur


def _provision(monkeypatch, fleet: float = 1_000_000.0):
    monkeypatch.setattr(engine, "DEFAULT_ROSTER", ())
    conn = _RecConn()
    engine.provision_defaults(conn, fleet_capital=fleet)
    return conn._cur.calls


def _index(calls, needle):
    return next(i for i, (sql, _p) in enumerate(calls) if needle in sql)


def test_provision_inserts_every_forward_test_enabled_with_an_even_slice(monkeypatch):
    calls = _provision(monkeypatch)
    ids = {s.id for s in FORWARD_TEST_SPECS}
    assert len(ids) == 10
    bot_inserts = [p for sql, p in calls if sql.startswith("INSERT INTO tw_bots ")]
    assert {p[0] for p in bot_inserts} == ids
    for p in bot_inserts:
        assert p[9] is True, f"{p[0]} must be inserted enabled"
    cap_inserts = [p for sql, p in calls if sql.startswith("INSERT INTO tw_bot_capital")]
    assert {p[0] for p in cap_inserts} == ids
    for p in cap_inserts:
        assert p[1] == p[2] == p[3] == 100_000.0  # 1M sliced across 10


def test_provision_refunds_a_zeroed_sleeve_before_resyncing_starting_capital(monkeypatch):
    calls = _provision(monkeypatch)
    refund = _index(
        calls, "WHERE bot_id = ANY(%s) AND starting_capital = 0 AND current_capital = 0"
    )
    resync = _index(calls, "SET starting_capital = %s, updated_at = %s")
    # The re-sync makes a zeroed sleeve look funded (starting = slice), so the
    # refund has to see it first or current_capital stays at zero.
    assert refund < resync
    params = calls[refund][1]
    assert params[:3] == (100_000.0, 100_000.0, 100_000.0)
    assert set(params[4]) == {s.id for s in FORWARD_TEST_SPECS}


def test_provision_turns_forward_tests_back_on_and_nothing_else(monkeypatch):
    calls = _provision(monkeypatch)
    i = _index(calls, "SET enabled = TRUE")
    assert set(calls[i][1][1]) == {s.id for s in FORWARD_TEST_SPECS}


def test_provision_still_disables_and_zeroes_the_shelved_bots(monkeypatch):
    calls = _provision(monkeypatch)
    disable = _index(calls, "SET enabled = FALSE")
    zero = _index(calls, "SET starting_capital = 0, current_capital = 0")
    forward = {s.id for s in FORWARD_TEST_SPECS}
    for i in (disable, zero):
        target = set(calls[i][1][1])
        assert target == set(DISABLED_BOT_IDS)
        assert target.isdisjoint(forward)


def test_provision_does_not_resync_enabled_in_the_upsert(monkeypatch):
    """Only forward tests are switched on, by their own statement; the upsert
    must still leave ``enabled`` alone so a validated bot's auto-disable
    survives a redeploy."""
    calls = _provision(monkeypatch)
    for sql, _p in calls:
        if sql.startswith("INSERT INTO tw_bots "):
            assert "enabled" not in sql.split("DO UPDATE SET", 1)[1].lower()


class _GuardConn:
    def __init__(self):
        self.rowcount = 1
        self.executed = []

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.executed.append(sql)


def test_breaker_never_disables_a_forward_test():
    bot_id = FORWARD_TEST_SPECS[0].id
    assert is_forward_test_bot(bot_id)
    conn = _GuardConn()
    assert apply_auto_disable_guard(conn, bot_id, 40, 0.10) is False
    assert conn.executed == []


def test_breaker_still_disables_a_bleeder_that_is_not_a_test():
    assert not is_forward_test_bot("eod_pin_drifter")
    conn = _GuardConn()
    assert apply_auto_disable_guard(conn, "eod_pin_drifter", 40, 0.10) is True


def test_forward_test_lookup_accepts_either_id():
    # gex_gradient_trend has no legacy bot id; charm_close_magnet's catalog
    # and bot ids are the same. Unknown ids are never tests.
    assert is_forward_test_bot("gex_gradient_trend")
    assert is_forward_test_bot("charm_close_magnet")
    assert not is_forward_test_bot("not_a_bot")
    assert not is_forward_test_bot("vwap_reversion_scalper")


def test_api_marks_forward_tests_for_the_website():
    from src.api.routers.tradeworkz import _catalog_fields, _is_forward_test

    fields = _catalog_fields("charm_close_magnet", "CharmCloseMagnet")
    assert fields["forward_test"] is True
    assert fields["provisionable"] is False
    shelved = _catalog_fields("vwap_reversion_scalper", "VwapReversionScalper")
    assert shelved["forward_test"] is False
    user_bot = _catalog_fields("user_123_spec", "spec_strategy")
    assert user_bot["forward_test"] is False
    assert _is_forward_test("gex_gradient_trend") is True
