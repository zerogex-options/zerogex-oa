"""One ThetaData login per ACCOUNT, not one per worker process.

ThetaData permits a single connection per account to a given MDDS server and
kicks the previous one when a second arrives ("You are unable to connect to
the same MDDS or FPSS server, as doing so will kick any existing connection"
-- Performance-And-Tuning/Multiple-Terminals). ``main_engine`` runs one
``multiprocessing.Process`` per underlying and each built its own provider,
so a ThetaData run meant four logins racing to invalidate each other. On
2026-09-24 ingestion never got past ``initialize()`` for any symbol; every
call came back "Invalid session ID".

The cache is now keyed by PID as well, and a process that inherits another
process's client adopts its session via ``existing_authorized_client``
instead of authenticating.

These tests fork for real. A mock of ``os.getpid`` would prove nothing about
whether the inherited dict actually arrives in the child.
"""

from __future__ import annotations

import multiprocessing
import os
from typing import Any, List

import pytest

from src.ingestion.providers import thetadata as td


class _FakeClient:
    """Stands in for ThetaClient, recording how it came to exist."""

    def __init__(self, origin: str, parent: Any = None) -> None:
        self.origin = origin  # "login" or "adopted"
        self.session = getattr(parent, "session", None) or f"session-{os.getpid()}"
        self.built_in_pid = os.getpid()


@pytest.fixture(autouse=True)
def _clean_cache():
    td.reset_shared_clients()
    yield
    td.reset_shared_clients()


KEY = ("mdds-01.thetadata.us", "443")


def _login() -> _FakeClient:
    return _FakeClient("login")


def _adopt(parent: Any) -> _FakeClient:
    return _FakeClient("adopted", parent=parent)


# ---------------------------------------------------------------------------
# Same process
# ---------------------------------------------------------------------------


def test_one_client_per_process():
    a = td.shared_client(_login, key=KEY, adopt=_adopt)
    b = td.shared_client(_login, key=KEY, adopt=_adopt)
    assert a is b, "a second provider in the same process must reuse the client"
    assert a.origin == "login"


def test_a_different_connection_gets_its_own_client():
    a = td.shared_client(_login, key=KEY, adopt=_adopt)
    b = td.shared_client(_login, key=("other-host", "443"), adopt=_adopt)
    assert a is not b


# ---------------------------------------------------------------------------
# Across a real fork
# ---------------------------------------------------------------------------


def _child_report(queue: Any) -> None:
    """Runs in the forked child: ask for the client, report what happened."""
    client = td.shared_client(_login, key=KEY, adopt=_adopt)
    queue.put(
        {
            "origin": client.origin,
            "session": client.session,
            "child_pid": os.getpid(),
            "built_in_pid": client.built_in_pid,
        }
    )


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork inheritance is the mechanism under test",
)
def test_a_forked_worker_adopts_the_parents_session_instead_of_logging_in():
    """The regression. A second login is what kicks the first off."""
    ctx = multiprocessing.get_context("fork")
    parent_client = td.shared_client(_login, key=KEY, adopt=_adopt)
    assert parent_client.origin == "login"

    queue = ctx.Queue()
    proc = ctx.Process(target=_child_report, args=(queue,))
    proc.start()
    report = queue.get(timeout=30)
    proc.join(timeout=30)

    assert report["origin"] == "adopted", (
        "the worker authenticated on its own; that login invalidates the "
        "supervisor's session and every subsequent call fails"
    )
    assert report["session"] == parent_client.session, "the session must carry over"
    assert report["built_in_pid"] == report["child_pid"], (
        "the child must build its OWN client object -- a gRPC channel does "
        "not survive fork, so reusing the inherited one is not an option"
    )
    assert report["child_pid"] != os.getpid(), "the test must actually have forked"


def _child_report_no_adopt(queue: Any) -> None:
    client = td.shared_client(_login, key=KEY)  # adopt omitted
    queue.put((client.origin, client.built_in_pid, os.getpid()))


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork inheritance is the mechanism under test",
)
def test_without_adopt_a_forked_worker_falls_back_to_logging_in():
    """Pins the cost of omitting adopt, so the parameter is not dropped as dead."""
    ctx = multiprocessing.get_context("fork")
    td.shared_client(_login, key=KEY, adopt=_adopt)

    queue = ctx.Queue()
    proc = ctx.Process(target=_child_report_no_adopt, args=(queue,))
    proc.start()
    origin, built_in_pid, child_pid = queue.get(timeout=30)
    proc.join(timeout=30)

    assert origin == "login", (
        "without adopt the inherited entry must be rebuilt from credentials "
        "-- which is the behaviour that broke the 2026-09-24 rehearsal"
    )
    # Distinguishes "rebuilt by the factory here" from "handed the parent's
    # object", which the pre-fix cache did and which also reports "login".
    assert built_in_pid == child_pid


def _child_reports_pid_ownership(queue: Any) -> None:
    td.shared_client(_login, key=KEY, adopt=_adopt)
    with td._CLIENTS_LOCK:
        owner_pid, _ = td._CLIENTS[KEY]
    queue.put((owner_pid, os.getpid()))


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork inheritance is the mechanism under test",
)
def test_the_child_takes_ownership_so_it_adopts_only_once():
    """A second call in the same worker must not build another client."""
    ctx = multiprocessing.get_context("fork")
    td.shared_client(_login, key=KEY, adopt=_adopt)

    queue = ctx.Queue()
    proc = ctx.Process(target=_child_reports_pid_ownership, args=(queue,))
    proc.start()
    owner_pid, child_pid = queue.get(timeout=30)
    proc.join(timeout=30)

    assert owner_pid == child_pid


# ---------------------------------------------------------------------------
# The supervisor half: one login, before any fork
# ---------------------------------------------------------------------------


def _calls_to_get_provider(monkeypatch, provider: str, start_method: str = "fork"):
    from src.ingestion import main_engine as me
    from src.ingestion import providers as providers_mod

    calls: List[int] = []
    monkeypatch.setenv("MARKET_DATA_PROVIDER", provider)
    monkeypatch.setattr(me.multiprocessing, "get_start_method", lambda **kw: start_method)
    monkeypatch.setattr(providers_mod, "get_provider", lambda **kw: calls.append(1))
    me._authenticate_shared_feed_session()
    return calls


def test_the_supervisor_logs_in_for_thetadata(monkeypatch):
    assert _calls_to_get_provider(monkeypatch, "thetadata_mv") == [1]


def test_the_supervisor_does_not_log_in_for_tradestation(monkeypatch):
    """TradeStation has no such constraint, and its client is built per worker
    on purpose so the new feed cannot inherit a dependency on its secrets."""
    assert _calls_to_get_provider(monkeypatch, "tradestation") == []


def test_the_supervisor_does_not_log_in_when_the_provider_is_unset(monkeypatch):
    from src.ingestion import main_engine as me
    from src.ingestion import providers as providers_mod

    calls: List[int] = []
    monkeypatch.delenv("MARKET_DATA_PROVIDER", raising=False)
    monkeypatch.setattr(providers_mod, "get_provider", lambda **kw: calls.append(1))
    me._authenticate_shared_feed_session()
    assert calls == [], "unset means TradeStation, which needs no shared session"


@pytest.mark.parametrize("start_method", ["spawn", "forkserver"])
def test_a_non_fork_start_method_is_reported_not_silently_broken(monkeypatch, caplog, start_method):
    """Adoption rides on fork. Under spawn the child starts empty and logs in."""
    with caplog.at_level("ERROR"):
        calls = _calls_to_get_provider(monkeypatch, "thetadata_mv", start_method)
    assert calls == [], "pre-authenticating is pointless if it cannot be inherited"
    assert start_method in caplog.text
    assert "fork" in caplog.text


def test_a_failed_login_does_not_stop_the_supervisor(monkeypatch, caplog):
    """A feed outage at boot must not take the unit down; the workers' own
    initialize() backoff bounds the retries."""
    from src.ingestion import main_engine as me
    from src.ingestion import providers as providers_mod

    def _boom(**kw):
        raise RuntimeError("terminal not up yet")

    monkeypatch.setenv("MARKET_DATA_PROVIDER", "thetadata")
    monkeypatch.setattr(me.multiprocessing, "get_start_method", lambda **kw: "fork")
    monkeypatch.setattr(providers_mod, "get_provider", _boom)

    with caplog.at_level("ERROR"):
        me._authenticate_shared_feed_session()  # must not raise

    assert "terminal not up yet" in caplog.text


def test_the_supervisor_logs_in_before_it_spawns_any_worker():
    """Order is the whole point: after the first fork it is already too late."""
    import io as _io
    from pathlib import Path

    src = _io.open(
        Path(__file__).resolve().parents[1] / "src" / "ingestion" / "main_engine.py",
        encoding="utf-8",
    ).read()
    login = src.index("    _authenticate_shared_feed_session()\n")
    spawn = src.index("        spawn_worker(name, target, worker_args)")
    assert login < spawn, (
        "the shared session must be established before the first fork, or the "
        "workers have nothing to adopt and each logs in"
    )


# ---------------------------------------------------------------------------
# The production shape: four underlyings, one account
# ---------------------------------------------------------------------------


def _worker_like_production(queue: Any, symbol: str) -> None:
    client = td.shared_client(_login, key=KEY, adopt=_adopt)
    queue.put((symbol, client.origin, client.session))


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork inheritance is the mechanism under test",
)
def test_four_concurrent_workers_produce_exactly_one_login():
    """SPY, QQQ, SPX and NDX -- the live underlying set.

    Four logins is what the 2026-09-24 rehearsal did, and each one kicked
    the last: "Invalid session ID. This can occur if more than one terminal
    is running." Started together rather than in sequence, so a race in the
    adoption path shows up here rather than in production.
    """
    ctx = multiprocessing.get_context("fork")
    parent = td.shared_client(_login, key=KEY, adopt=_adopt)

    queue = ctx.Queue()
    symbols = ["SPY", "QQQ", "$SPXW.X", "$NDXP.X"]
    procs = [ctx.Process(target=_worker_like_production, args=(queue, sym)) for sym in symbols]
    for proc in procs:
        proc.start()
    reports = [queue.get(timeout=30) for _ in symbols]
    for proc in procs:
        proc.join(timeout=30)
        assert proc.exitcode == 0, f"worker exited {proc.exitcode}"

    assert sorted(r[0] for r in reports) == sorted(symbols)
    origins = [r[1] for r in reports]
    assert origins == ["adopted"] * 4, f"expected four adoptions, got {origins}"
    assert {r[2] for r in reports} == {
        parent.session
    }, "every worker must be on the supervisor's single session"
