"""A dead per-symbol ingestion worker must be restarted, not silently dropped.

``main_engine.main`` runs one child process per symbol (plus the VIX / VXN /
futures side feeds). Every one of them is meant to stay up for the life of the
unit, so a child that is no longer running is always a fault.

Before supervision the parent simply sat in ``join()`` until *all* children
exited, so one child dying was completely invisible: the survivors kept
streaming, the unit stayed ``active``, the ``systemctl is-active`` liveness
watchdog stayed green, systemd's ``Restart=always`` never fired because the
parent had not exited, and ``OnFailure`` never dispatched. The dead symbol's
data just stopped -- the "nothing but SPY is updating" shape -- until a human
noticed by eye and restarted the unit.

These tests pin the supervisor: a worker that exits gets respawned; a worker
that crash-loops past its budget escalates to a nonzero exit (so systemd
restarts the unit and the alert fires) instead of respawning forever; and
workers killed by our own SIGTERM never count against the budget.
"""

import threading

import pytest

from src.ingestion import main_engine


class _FakeProcess:
    """Stand-in for multiprocessing.Process with scriptable liveness."""

    def __init__(self, name, alive=True, exitcode=None):
        self.name = name
        self._alive = alive
        self.exitcode = exitcode
        self.joined = False
        self.closed = False
        self.terminated = False
        # Optional callback fired on each is_alive() poll, so a test can end
        # the supervisor loop after a set number of passes.
        self.on_is_alive = None

    def is_alive(self):
        if self.closed:
            raise ValueError("process object is closed")
        if self.on_is_alive is not None:
            self.on_is_alive()
        return self._alive

    def join(self, timeout=None):
        if self.closed:
            raise ValueError("process object is closed")
        self.joined = True

    def close(self):
        self.closed = True

    def terminate(self):
        self.terminated = True
        self._alive = False

    def die(self, exitcode=1):
        self._alive = False
        self.exitcode = exitcode


def _harness(names, monkeypatch, poll=0.0, max_restarts=5, window=900, backoff=0.0):
    """Wire up worker_specs/processes/spawn against fake processes.

    Respawn backoff defaults to 0 so tests that are not about backoff run at
    full speed; the ones that are set it explicitly.
    """
    monkeypatch.setattr(main_engine, "WORKER_SUPERVISE_POLL_SECONDS", poll)
    monkeypatch.setattr(main_engine, "WORKER_MAX_RESTARTS_PER_WINDOW", max_restarts)
    monkeypatch.setattr(main_engine, "WORKER_RESTART_WINDOW_SECONDS", window)
    monkeypatch.setattr(main_engine, "WORKER_RESTART_BACKOFF_BASE_SECONDS", backoff)
    monkeypatch.setattr(main_engine, "WORKER_RESTART_BACKOFF_MAX_SECONDS", backoff)

    worker_specs = [(name, lambda *a: None, ()) for name in names]
    processes = {}
    restart_history = {name: [] for name in names}
    spawns = []

    def spawn_worker(name, target, args):
        proc = _FakeProcess(name)
        processes[name] = proc
        spawns.append(name)
        return proc

    for name, target, args in worker_specs:
        spawn_worker(name, target, args)
    spawns.clear()  # only count RE-spawns

    return worker_specs, processes, restart_history, spawns


def test_dead_worker_is_restarted_and_survivors_untouched(monkeypatch):
    """QQQ dies, SPY keeps streaming: QQQ is respawned, SPY is left alone."""
    specs, processes, history, spawns = _harness(["ingest-SPY", "ingest-QQQ"], monkeypatch)
    shutting_down = threading.Event()

    spy_before = processes["ingest-SPY"]
    dead_qqq = processes["ingest-QQQ"]
    dead_qqq.die(exitcode=1)

    # Stop the loop after it has had one pass to notice the death.
    def stop_after_one_pass(name, target, args):
        proc = _FakeProcess(name)
        processes[name] = proc
        spawns.append(name)
        shutting_down.set()
        return proc

    exit_code = main_engine.supervise_workers(
        specs, processes, history, shutting_down, stop_after_one_pass
    )

    assert exit_code == 0
    assert spawns == ["ingest-QQQ"], "only the dead worker should be respawned"
    assert processes["ingest-QQQ"] is not dead_qqq, "QQQ replaced with a live handle"
    assert processes["ingest-QQQ"].is_alive()
    # The healthy worker must not be disturbed -- restarting it would gap SPY
    # and re-race the TradeStation stream slots for no reason.
    assert processes["ingest-SPY"] is spy_before
    assert not spy_before.terminated

    # The dead handle was reaped and dropped, never left for the caller's
    # final join() (which raises ValueError on a closed Process).
    assert dead_qqq.joined and dead_qqq.closed


def test_crash_looping_worker_is_abandoned_and_survivors_keep_running(monkeypatch):
    """One bad worker must not take the healthy ones down with it.

    Escalating to a unit-wide exit here would kill three healthy symbols to
    punish one, and the unit's StartLimitBurst would turn a single
    crash-looping symbol into a permanent TOTAL outage -- strictly worse
    than the single-symbol gap this supervisor exists to fix. So the bad
    worker is abandoned and everyone else keeps streaming.
    """
    specs, processes, history, spawns = _harness(
        ["ingest-SPY", "ingest-QQQ"], monkeypatch, max_restarts=3
    )
    shutting_down = threading.Event()

    # Every respawn of QQQ comes back dead -- a crash loop a restart can't fix.
    def spawn_already_dead(name, target, args):
        proc = _FakeProcess(name)
        if name == "ingest-QQQ":
            proc.die(exitcode=1)
        processes[name] = proc
        spawns.append(name)
        return proc

    # End the loop a few passes after QQQ is abandoned, so we can observe
    # that the supervisor kept going rather than exiting.
    polls = {"n": 0}

    def count_polls():
        polls["n"] += 1
        if polls["n"] > 12:
            shutting_down.set()

    processes["ingest-SPY"].on_is_alive = count_polls
    processes["ingest-QQQ"].die(exitcode=1)

    exit_code = main_engine.supervise_workers(
        specs, processes, history, shutting_down, spawn_already_dead
    )

    # Budget is 3, so 3 respawns then abandonment on the 4th death -- and no
    # further respawns after that, however long the loop runs.
    assert spawns.count("ingest-QQQ") == 3
    # The healthy worker is never touched: not terminated, not restarted.
    assert not processes["ingest-SPY"].terminated
    assert spawns.count("ingest-SPY") == 0
    assert processes["ingest-SPY"].is_alive()
    # Nonzero so the unit reports failure when it eventually stops, but the
    # supervisor stayed up rather than tearing everything down.
    assert exit_code == 1


def test_all_workers_abandoned_exits_nonzero(monkeypatch):
    """Nothing left to supervise is a real total failure -- let systemd retry."""
    specs, processes, history, spawns = _harness(
        ["ingest-SPY", "ingest-QQQ"], monkeypatch, max_restarts=2
    )
    shutting_down = threading.Event()

    def spawn_already_dead(name, target, args):
        proc = _FakeProcess(name)
        proc.die(exitcode=1)
        processes[name] = proc
        spawns.append(name)
        return proc

    for proc in processes.values():
        proc.die(exitcode=1)

    exit_code = main_engine.supervise_workers(
        specs, processes, history, shutting_down, spawn_already_dead
    )

    assert exit_code == 1
    assert shutting_down.is_set(), "loop must end once every worker is abandoned"


def test_shutdown_does_not_count_as_worker_failure(monkeypatch):
    """SIGTERM kills every worker; that must not trip the restart budget."""
    specs, processes, history, spawns = _harness(["ingest-SPY", "ingest-QQQ"], monkeypatch)
    shutting_down = threading.Event()
    shutting_down.set()  # handler ran before the supervisor's first poll

    for proc in processes.values():
        proc.die(exitcode=-15)

    exit_code = main_engine.supervise_workers(
        specs, processes, history, shutting_down, spawn_worker=None
    )

    assert exit_code == 0
    assert spawns == [], "no respawns on the way down"
    assert all(not h for h in history.values()), "shutdown must not burn budget"


def test_restart_budget_is_a_sliding_window(monkeypatch):
    """Deaths older than the window are pruned, so occasional blips recover."""
    specs, processes, history, spawns = _harness(
        ["ingest-QQQ"], monkeypatch, max_restarts=2, window=900
    )
    shutting_down = threading.Event()

    # Two deaths already on the books, but both long outside the window.
    now = main_engine.time.monotonic()
    history["ingest-QQQ"] = [now - 5000.0, now - 4000.0]
    processes["ingest-QQQ"].die(exitcode=1)

    def stop_after_one_pass(name, target, args):
        proc = _FakeProcess(name)
        processes[name] = proc
        spawns.append(name)
        shutting_down.set()
        return proc

    exit_code = main_engine.supervise_workers(
        specs, processes, history, shutting_down, stop_after_one_pass
    )

    assert exit_code == 0, "stale deaths must not count toward the budget"
    assert spawns == ["ingest-QQQ"]
    assert len(history["ingest-QQQ"]) == 1, "the two stale entries were pruned"
    assert history["ingest-QQQ"][0] >= now, "only this pass's death is on the books"


def test_restart_delay_backs_off_and_caps(monkeypatch):
    """Doubling per consecutive death, bounded so retries never stop entirely."""
    monkeypatch.setattr(main_engine, "WORKER_RESTART_BACKOFF_BASE_SECONDS", 5.0)
    monkeypatch.setattr(main_engine, "WORKER_RESTART_BACKOFF_MAX_SECONDS", 120.0)

    assert main_engine._worker_restart_delay(1) == 5.0
    assert main_engine._worker_restart_delay(2) == 10.0
    assert main_engine._worker_restart_delay(3) == 20.0
    assert main_engine._worker_restart_delay(4) == 40.0
    # Capped, not unbounded — a worker must keep getting retried.
    assert main_engine._worker_restart_delay(99) == 120.0


def test_respawn_waits_for_backoff_and_death_counts_once(monkeypatch):
    """A fast-dying worker must not burn its whole budget in a few seconds.

    Without backoff the supervisor respawns on every poll, so a worker that
    dies instantly exhausts a 5-restart budget in ~30s and gets abandoned
    before a transient cause (stream slots from the previous process not yet
    released, a brief API/DB blip) has any chance to clear.
    """
    specs, processes, history, spawns = _harness(["ingest-SPY", "ingest-QQQ"], monkeypatch)
    monkeypatch.setattr(main_engine, "WORKER_RESTART_BACKOFF_BASE_SECONDS", 3600.0)
    shutting_down = threading.Event()

    polls = {"n": 0}

    def count_polls():
        polls["n"] += 1
        if polls["n"] > 8:
            shutting_down.set()

    processes["ingest-SPY"].on_is_alive = count_polls

    def spawn(name, target, args):
        proc = _FakeProcess(name)
        processes[name] = proc
        spawns.append(name)
        return proc

    processes["ingest-QQQ"].die(exitcode=1)

    exit_code = main_engine.supervise_workers(specs, processes, history, shutting_down, spawn)

    assert exit_code == 0
    assert spawns == [], "must not respawn until the backoff has elapsed"
    # And the single death is counted ONCE, not re-counted on every poll while
    # the worker sits in backoff — otherwise backoff would itself burn budget.
    assert len(history["ingest-QQQ"]) == 1


def test_spawn_failure_does_not_kill_the_supervisor(monkeypatch):
    """A failed respawn must not take the healthy workers down with it."""
    specs, processes, history, spawns = _harness(["ingest-SPY", "ingest-QQQ"], monkeypatch)
    monkeypatch.setattr(main_engine, "WORKER_RESTART_BACKOFF_BASE_SECONDS", 0.0)
    shutting_down = threading.Event()

    polls = {"n": 0}

    def count_polls():
        polls["n"] += 1
        if polls["n"] > 6:
            shutting_down.set()

    processes["ingest-SPY"].on_is_alive = count_polls

    def spawn_raises(name, target, args):
        spawns.append(name)
        raise RuntimeError("fork failed")

    processes["ingest-QQQ"].die(exitcode=1)

    exit_code = main_engine.supervise_workers(
        specs, processes, history, shutting_down, spawn_raises
    )

    assert spawns, "it did try to respawn"
    assert exit_code == 0
    # SPY was never disturbed by QQQ's failure to come back.
    assert processes["ingest-SPY"].is_alive()
    assert not processes["ingest-SPY"].terminated


def test_spawn_loop_does_not_shadow_the_argparse_namespace(monkeypatch):
    """main()'s spawn loop must not bind a loop variable named ``args``.

    run_for_symbol (and the VIX/VXN/futures targets) are closures over
    main()'s locals, where ``args`` is the argparse Namespace they read their
    config from. A loop variable of that name silently rebinds it to a tuple
    for EVERY worker, so each child dies instantly on ``args.expirations``.

    This is a total ingestion outage that is nearly invisible from the parent:
    the traceback only ever appears in the child, so the parent just reports
    workers dying with exitcode 1 for no stated reason. It took down
    production once; this test runs the worker target inline so the closure
    breaks loudly and locally instead.
    """
    import sys

    monkeypatch.setenv("INGEST_UNDERLYINGS", "SPY,QQQ")
    monkeypatch.setenv("INGEST_VIX_ENABLED", "false")
    monkeypatch.setenv("INGEST_VXN_ENABLED", "false")
    monkeypatch.setenv("INGEST_FUTURES_ENABLED", "false")
    monkeypatch.setenv("INGEST_EXPIRATIONS", "4")
    monkeypatch.setattr(sys, "argv", ["main_engine"])

    built = []

    class _RecordingEngine:
        def __init__(self, **kwargs):
            built.append(kwargs)

        def run(self):
            return None

    class _SyncProcess:
        """Runs the target INLINE on start(), so a bad closure raises here."""

        def __init__(self, target=None, args=(), name=None):
            self._target, self._args, self.name = target, args, name
            self.exitcode = 0

        def start(self):
            self._target(*self._args)

        def is_alive(self):
            return False

        def join(self, timeout=None):
            return None

        def terminate(self):
            return None

    import src.ingestion.api_call_tracker as api_call_tracker

    monkeypatch.setattr(api_call_tracker, "attach_db_writer", lambda *a, **k: None)
    monkeypatch.setattr(main_engine, "TradeStationClient", lambda *a, **k: object())
    monkeypatch.setattr(main_engine, "IngestionEngine", _RecordingEngine)
    monkeypatch.setattr(main_engine, "Process", _SyncProcess)
    monkeypatch.setattr(main_engine, "supervise_workers", lambda *a, **k: 0)

    with pytest.raises(SystemExit) as exit_info:
        main_engine.main()

    assert exit_info.value.code == 0
    # Both workers were constructed, each reading config off the Namespace --
    # which is only possible if `args` still refers to it at spawn time.
    assert [kwargs["underlying"] for kwargs in built] == ["SPY", "QQQ"]
    assert all(kwargs["num_expirations"] == 4 for kwargs in built)
