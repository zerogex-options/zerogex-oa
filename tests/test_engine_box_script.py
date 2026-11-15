"""deploy/engine_box.sh moves the engines between boxes without ever running
them on two (docs/runbooks/engine_box_split.md).

These run the real script in a sandbox: a fake ``systemctl`` that keeps unit
state in files and refuses to start an engine while the engines.disabled
marker exists (as the drop-in guard does), a fake ``psql`` backed by files,
and temporary directories for every path the script writes. Nothing touches
the host's systemd, /etc or the database.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "deploy" / "engine_box.sh"

ENGINES = [
    "zerogex-oa-thetaterminal",
    "zerogex-oa-ingestion",
    "zerogex-oa-analytics",
    "zerogex-oa-signals",
    "zerogex-oa-wall-alerts",
    "zerogex-oa-backtest-worker",
]

FAKE_SYSTEMCTL = r"""#!/usr/bin/env bash
# Unit state lives in $FAKE_STATE/<unit>.{active,enabled}; every call is logged.
echo "$*" >> "$FAKE_LOG"
quiet=0; args=()
for a in "$@"; do
  case "$a" in
    --quiet) quiet=1 ;;
    --no-legend|--now) ;;
    --state=*) ;;
    *) args+=("$a") ;;
  esac
done
verb="${args[0]:-}"; unit="${args[1]:-}"; unit="${unit%.service}"
now=0; case " $* " in *" --now "*) now=1;; esac
case "$verb" in
  is-active)
    s="$(cat "$FAKE_STATE/$unit.active" 2>/dev/null || echo inactive)"
    [ "$quiet" = 1 ] || echo "$s"; [ "$s" = active ] ;;
  is-enabled)
    s="$(cat "$FAKE_STATE/$unit.enabled" 2>/dev/null || echo disabled)"
    [ "$quiet" = 1 ] || echo "$s"; [ "$s" = enabled ] ;;
  start)
    case "$unit" in
      zerogex-oa-thetaterminal|zerogex-oa-ingestion|zerogex-oa-analytics|zerogex-oa-signals|zerogex-oa-wall-alerts|zerogex-oa-backtest-worker)
        if [ -e "$FAKE_MARKER_DIR/engines.disabled" ]; then exit 1; fi ;;
    esac
    echo active > "$FAKE_STATE/$unit.active" ;;
  stop)
    [ -e "$FAKE_MARKER_DIR/engines.disabled" ] && echo "marker-at-stop $unit" >> "$FAKE_LOG"
    echo inactive > "$FAKE_STATE/$unit.active" ;;
  enable)
    echo enabled > "$FAKE_STATE/$unit.enabled"
    [ "$now" = 1 ] && echo active > "$FAKE_STATE/$unit.active"; true ;;
  disable) echo disabled > "$FAKE_STATE/$unit.enabled" ;;
  list-unit-files)
    for f in "$FAKE_STATE"/*.timer.enabled; do
      [ -e "$f" ] || continue
      [ "$(cat "$f")" = enabled ] && printf '%s enabled enabled\n' "$(basename "$f" .enabled)"
    done; true ;;
  *) true ;;
esac
"""

# SQL in, rows out, from $FAKE_DB/{table_exists,handoffs,sessions}.
FAKE_PSQL = r"""#!/usr/bin/env python3
import os, re, sys
db = os.environ["FAKE_DB"]
sql = sys.argv[1]
def read(name):
    try:
        with open(os.path.join(db, name)) as f:
            return [ln.rstrip("\n") for ln in f if ln.strip()]
    except FileNotFoundError:
        return []
if "to_regclass" in sql:
    print("t" if read("table_exists") == ["t"] else "f")
elif sql.lstrip().startswith("INSERT INTO engine_box_handoffs"):
    vals = re.findall(r"'([^']*)'", sql.split("VALUES", 1)[1])
    host, addr, action, units = vals
    with open(os.path.join(db, "handoffs"), "a") as f:
        f.write(f"{action}|{host}|{addr}|{units}|2026-11-21 10:00 ET\n")
elif "FROM engine_box_handoffs" in sql:
    rows = read("handoffs")
    if rows:
        print(rows[-1])
elif "FROM pg_stat_activity" in sql:
    for row in read("sessions"):
        print(row)
"""


class Box:
    def __init__(self, root: Path, name: str, addr: str):
        self.root = root
        self.name = name
        self.addr = addr
        self.bin = root / "bin"
        self.state = root / "state"
        self.target = root / "systemd"
        self.markers = root / "etc-zerogex"
        self.theta = root / "thetaterminal"
        self.db = root / "db"
        self.log = root / "systemctl.log"
        for d in (self.bin, self.state, self.target, self.theta, self.db):
            d.mkdir(parents=True, exist_ok=True)
        _exe(self.bin / "systemctl", FAKE_SYSTEMCTL)
        _exe(self.bin / "psql-fake", FAKE_PSQL)
        _exe(self.bin / "java", "#!/usr/bin/env bash\necho 'openjdk version \"21\"' >&2\n")
        (self.theta / "ThetaTerminalv3.jar").write_text("jar")
        (self.theta / "creds.txt").write_text("me@example.com\nsecret\n")
        (root / ".env").write_text(
            "DB_HOST=db.example.us-east-2.rds.amazonaws.com\n"
            "INGEST_UNDERLYINGS=SPY,QQQ,SPX,NDX,NVDA,MSFT\n"
            "ANALYTICS_UNDERLYINGS=SPY,QQQ,SPX,NDX,NVDA,MSFT\n"
        )
        (self.db / "table_exists").write_text("t\n")

    def install(self, unit: str, *, enabled: bool = False, active: bool = False) -> None:
        (self.target / f"{unit}.service").write_text("[Service]\n")
        (self.state / f"{unit}.enabled").write_text("enabled" if enabled else "disabled")
        (self.state / f"{unit}.active").write_text("active" if active else "inactive")

    def is_active(self, unit: str) -> bool:
        path = self.state / f"{unit}.active"
        return path.exists() and path.read_text().strip() == "active"

    def is_enabled(self, unit: str) -> bool:
        path = self.state / f"{unit}.enabled"
        return path.exists() and path.read_text().strip() == "enabled"

    def calls(self) -> list[str]:
        return self.log.read_text().splitlines() if self.log.exists() else []

    def run(self, *args: str, db: Path | None = None) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env.update(
            PATH=f"{self.bin}:{env['PATH']}",
            FAKE_STATE=str(self.state),
            FAKE_LOG=str(self.log),
            FAKE_MARKER_DIR=str(self.markers),
            FAKE_DB=str(db or self.db),
            ZGX_ENV_FILE=str(self.root / ".env"),
            ZGX_SYSTEMD_SOURCE=str(REPO / "setup" / "systemd"),
            ZGX_SYSTEMD_TARGET=str(self.target),
            ZGX_JOURNALD_DROPIN_DIR=str(self.root / "journald.conf.d"),
            ZGX_MARKER_DIR=str(self.markers),
            ZGX_THETA_DIR=str(self.theta),
            ZGX_MONITORING_DIR=str(self.root / "monitoring"),
            ZGX_SUDO="",
            ZGX_SYSTEMCTL=str(self.bin / "systemctl"),
            ZGX_PSQL=str(self.bin / "psql-fake"),
            ZGX_DEPLOY_SCRIPT="/bin/false",
            ZGX_SETTLE_SECONDS="0",
            ZGX_THETA_SETTLE_SECONDS="0",
            ZGX_HOSTNAME=self.name,
            ZGX_ADDRS=self.addr,
            ZGX_ALLOW_ROOT="1",
        )
        return subprocess.run(
            ["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60
        )


def _exe(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(0o755)


@pytest.fixture
def web(tmp_path):
    """Today's one box: every engine installed, enabled and running, plus the API."""
    box = Box(tmp_path / "web", "zerogex-web", "10.0.1.10")
    for unit in ENGINES:
        box.install(unit, enabled=True, active=True)
    box.install("zerogex-oa-api", enabled=True, active=True)
    return box


@pytest.fixture
def engine(tmp_path, web):
    """A new engine box, sharing the web box's fake database."""
    box = Box(tmp_path / "engine", "zerogex-engine", "10.0.1.20")
    box.db = web.db
    return box


def test_status_on_the_one_box_finds_nothing_unsafe(web):
    result = web.run("status")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "ONE BOX" in result.stdout
    assert "fingerprint" in result.stdout


def test_a_dry_run_changes_nothing(web):
    for cmd in ("release", "takeover", "prepare"):
        web.run(cmd)
    assert not web.markers.exists()
    assert not any(re.match(r"(start|stop|enable|disable) ", c) for c in web.calls())
    assert all(web.is_active(u) for u in ENGINES)


def test_release_marks_first_stops_last_first_and_records_the_units(web):
    result = web.run("release", "--confirm")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (web.markers / "engines.disabled").exists()
    for unit in ENGINES:
        assert not web.is_active(unit) and not web.is_enabled(unit), unit
        dropin = web.target / f"{unit}.service.d" / "50-engine-box.conf"
        assert "/etc/zerogex/engines.disabled" in dropin.read_text()
        assert "PGAPPNAME=%N" in dropin.read_text()
    stops = [c.split()[1] for c in web.calls() if c.startswith("stop ")]
    assert stops == list(reversed(ENGINES))
    # The marker was down before the first stop, so nothing could restart them.
    marked = [c.split()[1] for c in web.calls() if c.startswith("marker-at-stop ")]
    assert marked == stops
    # The API and the website are not touched.
    assert web.is_active("zerogex-oa-api")
    assert not any("zerogex-oa-api" in c for c in web.calls() if c.startswith(("stop", "disable")))
    action, host, addr, units, _ = (
        web.db.joinpath("handoffs").read_text().splitlines()[-1].split("|")
    )
    assert (action, host, addr) == ("released", "zerogex-web", "10.0.1.10")
    assert units.split(",") == ENGINES


def test_an_engine_cannot_be_restarted_on_the_released_box(web):
    web.run("release", "--confirm")
    blocked = subprocess.run(
        [str(web.bin / "systemctl"), "start", "zerogex-oa-ingestion"],
        env={
            **os.environ,
            "FAKE_STATE": str(web.state),
            "FAKE_LOG": str(web.log),
            "FAKE_MARKER_DIR": str(web.markers),
        },
    )
    assert blocked.returncode != 0


def test_takeover_is_refused_until_the_other_box_releases(web, engine):
    for unit in ENGINES:
        engine.install(unit)
    (engine.markers).mkdir(parents=True)
    (engine.markers / "engines.disabled").write_text("x")
    engine.db.joinpath("handoffs").write_text(
        "took_over|zerogex-web|10.0.1.10|" + ",".join(ENGINES) + "|t\n"
    )
    result = engine.run("takeover", "--confirm")
    assert result.returncode == 1
    assert "has not released them" in result.stdout
    assert (engine.markers / "engines.disabled").exists()
    assert not any(engine.is_active(u) for u in ENGINES)


def test_takeover_with_no_record_at_all_is_refused(web, engine):
    for unit in ENGINES:
        engine.install(unit)
    result = engine.run("takeover", "--confirm")
    assert result.returncode == 1
    assert "No release is recorded" in result.stdout
    assert not any(engine.is_active(u) for u in ENGINES)


def test_takeover_starts_exactly_what_was_released_in_data_order(web, engine):
    web.install("zerogex-oa-backtest-worker")  # was not running on the old box
    for unit in ENGINES:
        engine.install(unit)
    engine.markers.mkdir(parents=True)
    (engine.markers / "engines.disabled").write_text("x")
    assert web.run("release", "--confirm").returncode == 0

    result = engine.run("takeover", "--confirm")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (engine.markers / "engines.disabled").exists()
    started = [c.split()[1] for c in engine.calls() if c.startswith("start ")]
    assert started == ENGINES[:-1]  # the terminal first, and no backtest worker
    assert not engine.is_active("zerogex-oa-backtest-worker")
    assert all(engine.is_enabled(u) for u in ENGINES[:-1])
    last = engine.db.joinpath("handoffs").read_text().splitlines()[-1]
    assert last.startswith("took_over|zerogex-engine|10.0.1.20|")


def test_takeover_refuses_engine_sessions_connected_from_another_box(web, engine):
    for unit in ENGINES:
        engine.install(unit)
    engine.db.joinpath("handoffs").write_text(
        "released|zerogex-web|10.0.1.10|" + ",".join(ENGINES) + "|t\n"
    )
    engine.db.joinpath("sessions").write_text("zerogex-oa-ingestion|10.0.1.10|4\n")
    result = engine.run("takeover", "--confirm")
    assert result.returncode == 1
    assert "zerogex-oa-ingestion@10.0.1.10" in result.stdout
    assert not any(engine.is_active(u) for u in ENGINES)


def test_a_failed_start_stops_the_takeover_and_says_how_to_hand_back(web, engine):
    for unit in ENGINES:
        engine.install(unit)
    engine.db.joinpath("handoffs").write_text(
        "released|zerogex-web|10.0.1.10|" + ",".join(ENGINES) + "|t\n"
    )
    # The terminal's start is accepted, but it never stays up.
    started_line = '    echo active > "$FAKE_STATE/$unit.active" ;;'
    assert FAKE_SYSTEMCTL.count(started_line) == 1
    _exe(
        engine.bin / "systemctl",
        FAKE_SYSTEMCTL.replace(
            started_line,
            '    [ "$unit" = zerogex-oa-thetaterminal ]'
            ' || echo active > "$FAKE_STATE/$unit.active" ;;',
        ),
    )
    result = engine.run("takeover", "--confirm")
    assert result.returncode == 1
    assert "zerogex-oa-thetaterminal did not come up" in result.stdout
    assert "make engine-release CONFIRM=yes" in result.stdout
    assert not any(c.startswith("start zerogex-oa-ingestion") for c in engine.calls())


def test_prepare_refuses_the_web_box(web):
    result = web.run("prepare", "--confirm", "--skip-deploy")
    assert result.returncode == 1
    assert "this is the web box" in result.stdout
    assert not web.markers.exists()


def test_prepare_marks_installs_and_starts_nothing_that_touches_the_feed(engine):
    result = engine.run("prepare", "--confirm", "--skip-deploy")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (engine.markers / "engine-box").exists()
    assert (engine.markers / "engines.disabled").exists()
    for unit in ENGINES:
        assert (engine.target / f"{unit}.service").exists(), unit
        assert (engine.target / f"{unit}.service.d" / "50-engine-box.conf").exists(), unit
        assert not engine.is_active(unit) and not engine.is_enabled(unit), unit
    api_guard = (engine.target / "zerogex-oa-api.service.d" / "50-engine-box.conf").read_text()
    assert "/etc/zerogex/engine-box" in api_guard
    enabled = [c for c in engine.calls() if c.startswith("enable ")]
    assert enabled and all(".timer" in c for c in enabled)
    assert {c.split()[-1] for c in enabled} == {
        "zerogex-oa-system-monitor.timer",
        "zerogex-oa-liveness-watch.timer",
        "zerogex-oa-logs-clear.timer",
    }


def test_status_flags_scheduled_jobs_enabled_on_the_engine_box(engine):
    engine.run("prepare", "--confirm", "--skip-deploy")
    (engine.state / "zerogex-oa-market-tide-refresh.timer.enabled").write_text("enabled")
    result = engine.run("status")
    assert result.returncode == 1
    assert "zerogex-oa-market-tide-refresh" in result.stdout
    assert "run twice" in result.stdout


def test_status_flags_an_engine_running_on_a_box_marked_off(web):
    web.markers.mkdir(parents=True)
    (web.markers / "engines.disabled").write_text("x")
    result = web.run("status")
    assert result.returncode == 1
    assert "running on a box marked engines.disabled" in result.stdout


def test_the_script_refuses_root_without_the_test_override():
    if os.geteuid() != 0:
        pytest.skip("only meaningful when the suite runs as root")
    env = {k: v for k, v in os.environ.items() if k != "ZGX_ALLOW_ROOT"}
    result = subprocess.run(
        ["bash", str(SCRIPT), "status"], env=env, capture_output=True, text=True
    )
    assert result.returncode == 1
    assert "not root" in result.stdout


def test_a_renamed_box_is_still_recognized_by_its_address(web, engine):
    """A rerun of takeover on the box that holds the engines (after a partial
    start, say) is allowed even if its hostname changed since."""
    for unit in ENGINES:
        engine.install(unit)
    engine.db.joinpath("handoffs").write_text(
        "took_over|ip-10-0-1-20|10.0.1.20|" + ",".join(ENGINES) + "|t\n"
    )
    result = engine.run("takeover", "--confirm")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "this box already holds the engines" in result.stdout
    assert all(engine.is_active(u) for u in ENGINES)
