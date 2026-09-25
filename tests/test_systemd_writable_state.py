"""Every sandboxed unit can write the state directory its job depends on.

This exists because of a failure that left no trace.  ``forecast_tweet`` keeps
a posted-marker file per (mode, symbol, date) so a re-run cannot tweet the same
thing twice, and its timers carry ``Persistent=true`` — so after downtime
systemd runs the missed job, which is precisely when the guard matters.  The
markers live under ``/var/lib/zerogex-oa``, the unit declared
``ProtectSystem=strict``, and nothing granted write access to that path.  The
write therefore failed, the guard is documented to fail OPEN, the failure
logged at DEBUG under a default level of INFO, and ``_already_posted`` returned
None forever.  Nothing was broken enough to notice: the tweets went out, the
duplicate protection simply was not there.

That is a config-drift bug, not a code bug, so a code test would never have
caught it.  This one reads the shipped unit files and the job modules they
launch, and asserts mechanically that a job which names a ``/var/lib`` path has
a unit that can write it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SYSTEMD_DIR = Path(__file__).resolve().parent.parent / "setup" / "systemd"
JOBS_DIR = Path(__file__).resolve().parent.parent / "src" / "jobs"

STATE_ROOT = "/var/lib/zerogex-oa"


def _directives(unit: str, key: str) -> list[str]:
    """All values for ``key``, ignoring commented-out lines."""
    out = []
    for line in unit.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        name, _, value = stripped.partition("=")
        if name.strip() == key:
            out.append(value.strip())
    return out


def _launched_modules(unit: str) -> list[str]:
    """The ``src.jobs.X`` modules an ExecStart line runs."""
    mods = []
    for exec_line in _directives(unit, "ExecStart"):
        mods.extend(re.findall(r"-m\s+(src\.jobs\.[\w.]+)", exec_line))
    return mods


def _can_write_state_root(unit: str) -> bool:
    """Does this unit grant write access to ``/var/lib/zerogex-oa``?

    Two directives qualify.  ``StateDirectory=zerogex-oa`` is the preferred
    one: it creates the directory with the service's ownership as well as
    making it writable.  ``ReadWritePaths`` also works but requires the path
    to already exist — systemd refuses to start the unit otherwise.
    """
    if any(v.split()[0] == "zerogex-oa" for v in _directives(unit, "StateDirectory") if v):
        return True
    for value in _directives(unit, "ReadWritePaths"):
        for path in value.split():
            if path.lstrip("-+!") in (STATE_ROOT, "/var/lib"):
                return True
    return False


def _service_units() -> list[Path]:
    return sorted(SYSTEMD_DIR.glob("*.service"))


def test_there_are_units_to_check():
    """Guard against the glob silently matching nothing and the suite passing."""
    assert _service_units(), f"no unit files found under {SYSTEMD_DIR}"


@pytest.mark.parametrize(
    "unit_path", _service_units(), ids=lambda p: p.name
)
def test_a_unit_can_write_the_state_dir_its_job_uses(unit_path: Path):
    unit = unit_path.read_text(encoding="utf-8")

    if not _directives(unit, "ProtectSystem"):
        return  # unsandboxed: the whole filesystem is writable anyway
    if "strict" not in " ".join(_directives(unit, "ProtectSystem")):
        return

    needs_state = []
    for module in _launched_modules(unit):
        source = JOBS_DIR / (module.rsplit(".", 1)[-1] + ".py")
        if source.is_file() and STATE_ROOT in source.read_text(encoding="utf-8"):
            needs_state.append(module)

    if not needs_state:
        return

    assert _can_write_state_root(unit), (
        f"{unit_path.name} runs {', '.join(needs_state)}, which writes under "
        f"{STATE_ROOT}, but the unit declares ProtectSystem=strict without "
        f"StateDirectory=zerogex-oa or a ReadWritePaths covering it. The write "
        f"will fail silently and any guard that depends on it is a no-op."
    )
