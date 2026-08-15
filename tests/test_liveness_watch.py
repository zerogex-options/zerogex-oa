"""The liveness watchdog must alert when a critical service is DOWN — the gap
``OnFailure=`` misses (clean stop / left inactive / active-but-hung).

These drive ``setup/systemd/zerogex-liveness-watch.sh`` with a fake
``systemctl`` + ``curl`` on PATH and a fake alert dispatcher, and assert the
core contract: alert once on the DOWN edge, do NOT re-alert while it stays
down, alert on recovery, and treat an active-but-not-serving API as down.

Fakes read their config from env vars (not string interpolation) so the shell
snippets stay literal.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
WATCH = REPO_ROOT / "setup" / "systemd" / "zerogex-liveness-watch.sh"
MUTE_SCRIPT = REPO_ROOT / "setup" / "systemd" / "zerogex-liveness-mute.sh"

# Fake systemctl: `is-active [--quiet] <unit>` reads active/inactive from
# $FAKE_UNIT_STATE_DIR/<unit>; exit 0 when active, 3 otherwise (as real
# systemctl does for inactive).
_FAKE_SYSTEMCTL = r"""#!/usr/bin/env bash
quiet=0; unit=""
for a in "$@"; do
  case "$a" in
    is-active) ;;
    --quiet) quiet=1 ;;
    *) unit="$a" ;;
  esac
done
state="$(cat "$FAKE_UNIT_STATE_DIR/$unit" 2>/dev/null || echo inactive)"
[ "$quiet" = "0" ] && echo "$state"
[ "$state" = "active" ] && exit 0 || exit 3
"""

# Fake curl: succeed iff FAKE_API_OK=1 (ignores the URL/args).
_FAKE_CURL = r"""#!/usr/bin/env bash
[ "${FAKE_API_OK:-0}" = "1" ] && exit 0 || exit 7
"""

# Fake alert dispatcher: append "<unit>\t<reason>" to $ALERT_LOG.
_FAKE_ALERT = r"""#!/usr/bin/env bash
printf '%s\t%s\n' "$1" "$2" >> "$ALERT_LOG"
"""


def _write_exec(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o755)


class _Harness:
    def __init__(self, tmp_path: Path):
        self.bindir = tmp_path / "bin"
        self.bindir.mkdir()
        self.unit_state = tmp_path / "units"
        self.unit_state.mkdir()
        self.state_dir = tmp_path / "state"
        self.alert_log = tmp_path / "alerts.log"
        self.alert_script = tmp_path / "fake-alert.sh"

        _write_exec(self.bindir / "systemctl", _FAKE_SYSTEMCTL)
        _write_exec(self.bindir / "curl", _FAKE_CURL)
        _write_exec(self.alert_script, _FAKE_ALERT)

    def set_unit(self, unit: str, *, active: bool) -> None:
        (self.unit_state / unit).write_text("active" if active else "inactive")

    def set_mute(self, unit: str, *, offset: int) -> None:
        """Write a maintenance mute expiring ``offset`` seconds from now
        (negative = already expired)."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / f"{unit}.mute").write_text(str(int(time.time()) + offset))

    def mute_file(self, unit: str) -> Path:
        return self.state_dir / f"{unit}.mute"

    def run(self, *, api_ok: bool) -> None:
        env = dict(os.environ)
        env["PATH"] = f"{self.bindir}:{env['PATH']}"
        env["FAKE_UNIT_STATE_DIR"] = str(self.unit_state)
        env["FAKE_API_OK"] = "1" if api_ok else "0"
        env["ALERT_LOG"] = str(self.alert_log)
        env["ALERT_SCRIPT"] = str(self.alert_script)
        env["LIVENESS_SERVICES"] = "zerogex-oa-api"
        env["LIVENESS_API_UNIT"] = "zerogex-oa-api"
        env["LIVENESS_API_URL"] = "http://127.0.0.1:8000/api/health/live"
        env["LIVENESS_STATE_DIR"] = str(self.state_dir)
        subprocess.run(["bash", str(WATCH)], env=env, check=True)

    def alerts(self) -> list[str]:
        if not self.alert_log.exists():
            return []
        return [ln for ln in self.alert_log.read_text().splitlines() if ln.strip()]


@pytest.fixture
def harness(tmp_path):
    return _Harness(tmp_path)


def test_no_alert_when_api_healthy(harness):
    harness.set_unit("zerogex-oa-api", active=True)
    harness.run(api_ok=True)
    assert harness.alerts() == []


def test_alerts_once_on_down_edge_then_dedups(harness):
    # API stopped (inactive) — the exact incident shape.
    harness.set_unit("zerogex-oa-api", active=False)
    harness.run(api_ok=False)
    harness.run(api_ok=False)  # still down — must NOT re-alert every minute
    fired = harness.alerts()
    assert len(fired) == 1, fired
    assert fired[0].startswith("zerogex-oa-api\t")
    assert "not active" in fired[0]


def test_recovery_alerts_after_down(harness):
    harness.set_unit("zerogex-oa-api", active=False)
    harness.run(api_ok=False)  # down edge
    harness.set_unit("zerogex-oa-api", active=True)
    harness.run(api_ok=True)  # recovery edge
    fired = harness.alerts()
    assert len(fired) == 2, fired
    assert "recovered" in fired[1]


def test_active_but_not_serving_is_down(harness):
    # Process active but the liveness endpoint doesn't answer (hung worker) —
    # OnFailure would never fire here; the watchdog must.
    harness.set_unit("zerogex-oa-api", active=True)
    harness.run(api_ok=False)
    fired = harness.alerts()
    assert len(fired) == 1, fired
    assert "not serving" in fired[0]


def test_muted_down_does_not_alert(harness):
    """A planned stop/restart writes a mute; the watchdog must stay silent."""
    harness.set_unit("zerogex-oa-api", active=False)
    harness.set_mute("zerogex-oa-api", offset=3600)
    harness.run(api_ok=False)
    assert harness.alerts() == []


def test_expired_mute_still_alerts_when_down(harness):
    """A lapsed mute must NOT mask a still-down unit — a forgotten stop is
    exactly the incident we must still catch."""
    harness.set_unit("zerogex-oa-api", active=False)
    harness.set_mute("zerogex-oa-api", offset=-10)  # already expired
    harness.run(api_ok=False)
    fired = harness.alerts()
    assert len(fired) == 1 and "not active" in fired[0], fired
    assert not harness.mute_file("zerogex-oa-api").exists()  # stale mute cleared


def test_mute_dropped_once_service_back_up(harness):
    """Restart case: the unit is back up within the window → the mute is
    dropped so monitoring resumes at once, and no recovery page fires."""
    harness.set_unit("zerogex-oa-api", active=True)
    harness.set_mute("zerogex-oa-api", offset=3600)
    harness.run(api_ok=True)
    assert harness.alerts() == []
    assert not harness.mute_file("zerogex-oa-api").exists()


def test_mute_script_writes_future_expiry(tmp_path):
    """The mute writer drops a future-epoch file the watchdog will honor."""
    state = tmp_path / "liveness"
    env = dict(os.environ, LIVENESS_STATE_DIR=str(state))
    subprocess.run(["bash", str(MUTE_SCRIPT), "zerogex-oa-api", "600"], env=env, check=True)
    mute = state / "zerogex-oa-api.mute"
    assert mute.exists()
    assert int(mute.read_text().strip()) > int(time.time()) + 300
