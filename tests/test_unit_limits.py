"""``make service-limits-install`` (src/tools/unit_limits.py).

On 2026-11-06 the box was still running ingestion at CPUQuota=150%, the 300%
the repo has said since 2026-10-28 never installed. These pin how the tool
finds that, what it changes, and what it refuses.
"""

import subprocess
from pathlib import Path

import pytest

from src.tools import unit_limits as ul

G = 1024**3
M = 1024**2

OLD = """[Service]
ExecStart=/home/ubuntu/zerogex-oa/venv/bin/python -m src.ingestion.main_engine
# Resource limits
MemoryMax=2G
CPUQuota=150%
CPUWeight=100
"""
NEW = """[Service]
ExecStart=/home/ubuntu/zerogex-oa/venv/bin/python -m src.ingestion.main_engine
# Resource limits
MemoryMax=2G
# Up to 3 of the box's 8 cores.
CPUQuota=300%
CPUWeight=100
"""


def test_sizes_and_quotas_read_as_systemd_reads_them():
    assert ul.parse_size("2G") == 2 * G
    assert ul.parse_size("256M") == 256 * M
    assert ul.parse_size("1024") == 1024
    assert ul.parse_size("infinity") is None
    assert ul.parse_size("50%") is None
    assert ul.parse_quota("300%") == 3.0
    assert ul.parse_quota("150%") == 1.5
    assert ul.parse_quota("") is None


def test_the_service_sections_last_assignment_wins():
    text = """[Unit]
MemoryMax=9G
[Service]
; a comment
MemoryMax=1G
CPUQuota=100%
MemoryMax=2G
# CPUQuota=400%
"""
    assert ul.unit_file_limits(text) == {"MemoryMax": "2G", "CPUQuota": "100%"}
    assert ul.unit_file_limits("[Service]\nMemoryMax=2G\nMemoryMax=\n") == {
        "MemoryMax": None,
        "CPUQuota": None,
    }


def test_the_repos_own_unit_files_parse():
    expect = {
        "zerogex-oa-ingestion": ("2G", "300%"),
        "zerogex-oa-analytics": ("1G", "100%"),
        "zerogex-oa-signals": ("1G", "100%"),
        "zerogex-oa-api": ("8G", "400%"),
        "zerogex-oa-thetaterminal": ("1G", "100%"),
    }
    for unit, (mem, cpu) in expect.items():
        limits = ul.unit_file_limits((ul.REPO_UNITS / f"{unit}.service").read_text())
        assert (limits["MemoryMax"], limits["CPUQuota"]) == (mem, cpu), unit


def _box(tmp_path, installed=OLD, cpu_max="150000 100000", mem_current=760 * M, mem_max=2 * G):
    repo = tmp_path / "repo"
    etc = tmp_path / "etc"
    cg = tmp_path / "cgroup" / "zerogex-oa-ingestion.service"
    for d in (repo, etc, cg):
        d.mkdir(parents=True)
    (repo / "zerogex-oa-ingestion.service").write_text(NEW)
    if installed is not None:
        (etc / "zerogex-oa-ingestion.service").write_text(installed)
    (cg / "cpu.max").write_text(cpu_max + "\n")
    (cg / "memory.max").write_text(f"{mem_max}\n")
    (cg / "memory.current").write_text(f"{mem_current}\n")
    return repo, etc, cg


def test_the_plan_finds_the_uninstalled_quota(tmp_path):
    repo, etc, cg = _box(tmp_path)
    plan = ul.make_plan("zerogex-oa-ingestion", repo, etc, cg.parent)
    assert plan.file_differs
    assert plan.live_differs == ["CPUQuota is 150%, the file says 300%"]
    assert plan.set_property_args == ["MemoryMax=2G", "CPUQuota=300%"]
    assert plan.refusals == []
    # Only a comment and the limit changed, so nothing waits for a restart.
    assert ul.other_changes(plan.diff) == []
    text = ul.describe(plan)
    assert "-CPUQuota=150%" in text and "+CPUQuota=300%" in text
    assert "CPUQuota" in text and "150%" in text and "300%" in text


def test_other_changed_lines_are_named_because_they_wait_for_a_restart(tmp_path):
    repo, etc, cg = _box(tmp_path, installed=OLD.replace("main_engine", "old_engine"))
    plan = ul.make_plan("zerogex-oa-ingestion", repo, etc, cg.parent)
    changed = ul.other_changes(plan.diff)
    assert any(line.startswith("-ExecStart") and "old_engine" in line for line in changed)
    assert any(line.startswith("+ExecStart") for line in changed)


def test_a_memory_cap_below_what_the_service_holds_is_refused(tmp_path):
    repo, etc, cg = _box(tmp_path, mem_current=int(2.5 * G), mem_max=3 * G)
    plan = ul.make_plan("zerogex-oa-ingestion", repo, etc, cg.parent)
    assert plan.refusals and "reclaim or kill" in plan.refusals[0]


@pytest.fixture
def box(tmp_path, monkeypatch):
    repo, etc, cg = _box(tmp_path)
    monkeypatch.setattr(ul, "REPO_UNITS", repo)
    monkeypatch.setattr(ul, "INSTALLED_UNITS", etc)
    monkeypatch.setattr(ul, "CGROUP_ROOT", cg.parent)
    calls = []

    def fake_run(cmd, check=True):
        calls.append(cmd)
        # What systemd would do: the copy lands, and set-property changes the cgroup.
        if cmd[:2] == ["sudo", "cp"]:
            Path(cmd[3]).write_text(Path(cmd[2]).read_text())
        if "set-property" in cmd:
            (cg / "cpu.max").write_text("300000 100000\n")

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls, etc, cg


def test_without_apply_nothing_runs(box, capsys):
    calls, etc, _ = box
    assert ul.main(["--unit", "ingestion"]) == 0
    assert calls == []
    assert "CPUQuota=150%" in (etc / "zerogex-oa-ingestion.service").read_text()
    assert "Nothing changed. CONFIRM=yes" in capsys.readouterr().out


def test_apply_installs_reloads_sets_the_limits_live_and_checks_them(box, capsys):
    calls, etc, _ = box
    assert ul.main(["--unit", "ingestion", "--apply"]) == 0
    assert [c[:3] for c in calls] == [
        ["sudo", "cp", str(ul.REPO_UNITS / "zerogex-oa-ingestion.service")],
        ["sudo", "systemctl", "daemon-reload"],
        ["sudo", "systemctl", "set-property"],
    ]
    assert calls[2][3:] == ["--runtime", "zerogex-oa-ingestion", "MemoryMax=2G", "CPUQuota=300%"]
    assert "CPUQuota=300%" in (etc / "zerogex-oa-ingestion.service").read_text()
    assert (
        "now runs with MemoryMax=2G and CPUQuota=300%, without a restart" in capsys.readouterr().out
    )
    # Run again: nothing left to do.
    calls.clear()
    assert ul.main(["--unit", "ingestion", "--apply"]) == 0
    assert calls == []


def test_a_limit_that_did_not_take_is_reported(box, monkeypatch, capsys):
    calls, _, _ = box
    monkeypatch.setattr(subprocess, "run", lambda cmd, check=True: calls.append(cmd))
    assert ul.main(["--unit", "ingestion", "--apply"]) == 1
    assert "does not have the file's limits yet" in capsys.readouterr().out


def test_a_failed_step_stops_there(box, monkeypatch, capsys):
    def failing(cmd, check=True):
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(subprocess, "run", failing)
    assert ul.main(["--unit", "ingestion", "--apply"]) == 1
    assert "Stopped: `sudo cp" in capsys.readouterr().out


def test_only_known_services(capsys):
    assert ul.unit_name("ingestion") == "zerogex-oa-ingestion"
    assert ul.unit_name("zerogex-oa-api.service") == "zerogex-oa-api"
    assert ul.main(["--unit", "postgres"]) == 2
