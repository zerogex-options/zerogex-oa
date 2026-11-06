"""Install one service's unit file from setup/systemd, and put its CPU and
memory limits into effect on the running service, without a restart.

The unit files in setup/systemd are the source of truth, but the only things
that copy one to /etc/systemd/system are a full deploy (deploy/steps/100.systemd,
which stops and starts every service) and a per-service install target, which
only the API has. So a limit changed in the repo can sit uninstalled. On
2026-11-06 the box was still running ingestion at CPUQuota=150%: the 300% the
repo said from 2026-10-28 never got there. (The session showed 150% is enough,
so the repo says 150% again.)

Without --apply this only shows: how the installed file differs from the repo's,
and the memory cap and CPU quota each file sets beside what the running service
has. With --apply it copies the repo's file over, reloads systemd, and sets the
file's MemoryMax and CPUQuota on the running service (systemctl set-property
--runtime). That takes effect at once, and from then on the installed file says
the same, so a restart or a reboot keeps it. Nothing restarts.

It refuses to set a memory cap below what the service holds now, which would
make the kernel reclaim or kill on the spot: lower one with a restart instead.

Usage::

    python -m src.tools.unit_limits --unit ingestion            # show
    python -m src.tools.unit_limits --unit ingestion --apply    # install, apply, verify

Exit codes: 0 done or nothing to do, 1 refused or not verified, 2 usage.
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

REPO_UNITS = Path(__file__).resolve().parents[2] / "setup" / "systemd"
INSTALLED_UNITS = Path("/etc/systemd/system")
CGROUP_ROOT = Path("/sys/fs/cgroup/system.slice")

#: The long-running services whose limits matter to capacity.
SERVICES = (
    "zerogex-oa-ingestion",
    "zerogex-oa-analytics",
    "zerogex-oa-signals",
    "zerogex-oa-api",
    "zerogex-oa-thetaterminal",
    "zerogex-oa-wall-alerts",
)
LIMIT_KEYS = ("MemoryMax", "CPUQuota")

_SIZE_RE = re.compile(r"^(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[KMGTPE]?)$", re.IGNORECASE)
_POWERS = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}


def unit_name(name: str) -> str:
    """``ingestion`` or ``zerogex-oa-ingestion`` (with or without .service)."""
    short = name.strip().removesuffix(".service")
    return short if short.startswith("zerogex-oa-") else f"zerogex-oa-{short}"


def parse_size(value: Optional[str]) -> Optional[int]:
    """A systemd size (``2G``, ``256M``, bytes) in bytes, base 1024.

    None for ``infinity``, an empty value (the default, no cap) and anything
    this does not read, such as a percentage of RAM.
    """
    text = (value or "").strip()
    if not text or text.lower() == "infinity":
        return None
    m = _SIZE_RE.match(text)
    if not m:
        return None
    return int(float(m["num"]) * 1024 ** _POWERS[m["unit"].upper()])


def parse_quota(value: Optional[str]) -> Optional[float]:
    """A systemd CPUQuota (``300%``) in cores, or None."""
    text = (value or "").strip()
    if not text.endswith("%"):
        return None
    try:
        return float(text[:-1]) / 100.0
    except ValueError:
        return None


def unit_file_limits(text: str) -> Dict[str, Optional[str]]:
    """MemoryMax and CPUQuota as a unit file sets them, in the [Service] section.

    As systemd reads them: the last assignment wins, and an empty assignment
    resets the setting.
    """
    out: Dict[str, Optional[str]] = {key: None for key in LIMIT_KEYS}
    section = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line[0] in "#;":
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line
            continue
        if section != "[Service]" or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip() in out:
            out[key.strip()] = value.strip() or None
    return out


def _read(path: Path) -> Optional[str]:
    try:
        return path.read_text()
    except OSError:
        return None


def live_limits(unit: str, root: Path = CGROUP_ROOT) -> Optional[Dict[str, Optional[float]]]:
    """The running service's memory cap and CPU quota from its cgroup, and what
    it holds now. None when it is not running."""
    base = root / f"{unit}.service"
    if not base.is_dir():
        return None

    def number(name: str) -> Optional[int]:
        text = (_read(base / name) or "").strip()
        return int(text) if text.isdigit() else None

    quota = None
    parts = (_read(base / "cpu.max") or "").split()
    if len(parts) == 2 and parts[0] != "max":
        try:
            quota = int(parts[0]) / int(parts[1])
        except (ValueError, ZeroDivisionError):
            quota = None
    return {
        "MemoryMax": number("memory.max"),
        "CPUQuota": quota,
        "memory_current": number("memory.current"),
    }


def limits_match(
    file_limits: Dict[str, Optional[str]], live: Dict[str, Optional[float]]
) -> List[str]:
    """Each limit the running service does not have as the file sets it, as
    a sentence. Empty when they agree."""
    out = []
    want_mem = parse_size(file_limits.get("MemoryMax"))
    if file_limits.get("MemoryMax") is not None and want_mem != live.get("MemoryMax"):
        out.append(
            f"MemoryMax is {fmt_size(live.get('MemoryMax'))},"
            f" the file says {file_limits['MemoryMax']}"
        )
    want_cpu = parse_quota(file_limits.get("CPUQuota"))
    have_cpu = live.get("CPUQuota")
    if file_limits.get("CPUQuota") is not None and (
        have_cpu is None or want_cpu is None or abs(have_cpu - want_cpu) > 0.005
    ):
        out.append(f"CPUQuota is {fmt_quota(have_cpu)}, the file says {file_limits['CPUQuota']}")
    return out


def fmt_size(value: Optional[float]) -> str:
    if value is None:
        return "no cap"
    if value >= 1024**3 and value % 1024**3 == 0:
        return f"{int(value // 1024**3)}G"
    return f"{value / 1024**2:,.0f}M"


def fmt_quota(value: Optional[float]) -> str:
    return "no quota" if value is None else f"{value * 100:.0f}%"


@dataclass
class Plan:
    unit: str
    repo_file: Path
    installed_file: Path
    file_differs: bool
    diff: List[str]
    repo_limits: Dict[str, Optional[str]]
    installed_limits: Dict[str, Optional[str]]
    live: Optional[Dict[str, Optional[float]]]
    refusals: List[str] = field(default_factory=list)

    @property
    def set_property_args(self) -> List[str]:
        return [f"{key}={value}" for key, value in self.repo_limits.items() if value is not None]

    @property
    def live_differs(self) -> List[str]:
        return limits_match(self.repo_limits, self.live) if self.live is not None else []


def make_plan(
    unit: str,
    repo_dir: Path = REPO_UNITS,
    installed_dir: Path = INSTALLED_UNITS,
    cgroup_root: Path = CGROUP_ROOT,
) -> Plan:
    repo_file = repo_dir / f"{unit}.service"
    installed_file = installed_dir / f"{unit}.service"
    repo_text = _read(repo_file)
    if repo_text is None:
        raise SystemExit(f"{repo_file} not found")
    installed_text = _read(installed_file)
    diff = list(
        difflib.unified_diff(
            (installed_text or "").splitlines(),
            repo_text.splitlines(),
            fromfile=str(installed_file)
            + ("" if installed_text is not None else " (not installed)"),
            tofile=str(repo_file),
            lineterm="",
        )
    )
    plan = Plan(
        unit=unit,
        repo_file=repo_file,
        installed_file=installed_file,
        file_differs=bool(diff),
        diff=diff,
        repo_limits=unit_file_limits(repo_text),
        installed_limits=unit_file_limits(installed_text or ""),
        live=live_limits(unit, cgroup_root),
    )
    want = parse_size(plan.repo_limits.get("MemoryMax"))
    held = (plan.live or {}).get("memory_current")
    if want is not None and held is not None and want < held:
        plan.refusals.append(
            f"the file's MemoryMax={plan.repo_limits['MemoryMax']} is less than the"
            f" {fmt_size(held)} "
            f"{unit} holds now; applying it live would make the kernel reclaim or kill. Lower it "
            "with a restart instead"
        )
    for key, value in plan.repo_limits.items():
        if value is None or value.lower() == "infinity":
            continue
        if (parse_size(value) if key == "MemoryMax" else parse_quota(value)) is None:
            plan.refusals.append(
                f"{key}={value} is not a value this tool reads; install it by hand"
            )
    return plan


def other_changes(diff: Sequence[str]) -> List[str]:
    """Changed lines that are neither a comment nor one of the limits: they
    wait for the next restart, so the reader should know they are there."""
    out = []
    for line in diff:
        if line.startswith(("+++", "---")) or not line[:1] in ("+", "-"):
            continue
        body = line[1:].strip()
        if not body or body[0] in "#;" or body.split("=", 1)[0] in LIMIT_KEYS:
            continue
        out.append(line)
    return out


def describe(plan: Plan) -> str:
    lines = [f"=== {plan.unit} ==="]
    lines += plan.diff if plan.diff else ["installed file: the same as the repo's"]
    lines.append("")
    lines.append(f"  {'':<10}{'repo file':>12}{'installed':>12}{'running':>12}")
    for key in LIMIT_KEYS:
        if plan.live is None:
            running = "not running"
        elif key == "MemoryMax":
            running = fmt_size(plan.live.get("MemoryMax"))
        else:
            running = fmt_quota(plan.live.get("CPUQuota"))
        lines.append(
            f"  {key:<10}{plan.repo_limits.get(key) or '-':>12}"
            f"{plan.installed_limits.get(key) or '-':>12}{running:>12}"
        )
    return "\n".join(lines)


def apply(plan: Plan, run=subprocess.run) -> List[str]:
    """Install the file, reload, set the limits live. Returns the commands run."""
    commands: List[List[str]] = [
        ["sudo", "cp", str(plan.repo_file), str(plan.installed_file)],
        ["sudo", "systemctl", "daemon-reload"],
    ]
    if plan.live is not None and plan.set_property_args:
        commands.append(
            ["sudo", "systemctl", "set-property", "--runtime", plan.unit, *plan.set_property_args]
        )
    done = []
    for cmd in commands:
        run(cmd, check=True)
        done.append(" ".join(cmd))
    return done


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--unit", required=True, help="ingestion, analytics, signals, api, ...")
    parser.add_argument(
        "--apply", action="store_true", help="install the file and apply its limits now"
    )
    args = parser.parse_args(argv)

    unit = unit_name(args.unit)
    if unit not in SERVICES:
        print(f"{unit} is not one of: {', '.join(s.removeprefix('zerogex-oa-') for s in SERVICES)}")
        return 2
    plan = make_plan(unit, REPO_UNITS, INSTALLED_UNITS, CGROUP_ROOT)
    print(describe(plan))
    if plan.refusals:
        print("\nRefused, nothing changed:\n  " + "\n  ".join(plan.refusals))
        return 1
    if not plan.file_differs and not plan.live_differs:
        print("\nNothing to do: the installed file and the running limits match the repo.")
        return 0
    others = other_changes(plan.diff)
    if others:
        print(
            "\nThe file also changes lines other than the limits. Those take effect at the "
            "service's next restart, not now:\n  " + "\n  ".join(others)
        )
    if not args.apply:
        print(
            "\nNothing changed. CONFIRM=yes installs the repo's file and applies its limits"
            " now, without a restart."
        )
        return 0

    try:
        for command in apply(plan, run=subprocess.run):
            print(f"ran: {command}")
    except subprocess.CalledProcessError as e:
        print(f"\nStopped: `{' '.join(e.cmd)}` exited {e.returncode}. Check what it printed above.")
        return 1
    after = live_limits(unit, CGROUP_ROOT)
    if after is None:
        print(f"\nInstalled. {unit} is not running; its limits apply when it starts.")
        return 0
    still = limits_match(plan.repo_limits, after)
    if still:
        print(
            "\nInstalled, but the running service does not have the file's limits yet:\n  "
            + "\n  ".join(still)
        )
        return 1
    print(
        f"\nDone. {unit} now runs with MemoryMax={fmt_size(after['MemoryMax'])} and "
        f"CPUQuota={fmt_quota(after['CPUQuota'])}, without a restart."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
