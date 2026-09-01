"""The dispatcher must fold a repeating failure into one alert — without ever
folding away a *different* one.

Before this, a failing oneshot alerted on every firing of its own timer: the
freshness check re-fires every 10 minutes, so one stale symbol sent ~12 emails
a day and the operator learned to filter the channel. Only the pagerduty
backend deduplicated.

The hazard in fixing that is worse than the noise: a per-unit timer would also
swallow a SECOND, unrelated outage arriving mid-cooldown. So suppression is
keyed on a signature of the failing journal lines, and these tests pin both
halves — the repeat is held, the new failure is not.

Drives setup/systemd/zerogex-alert.sh with a fake curl and a scriptable fake
journalctl, the same harness shape as test_alert_resend.py.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ALERT = REPO_ROOT / "setup" / "systemd" / "zerogex-alert.sh"

pytestmark = pytest.mark.skipif(shutil.which("jq") is None, reason="jq not installed")

_FAKE_CURL = r"""#!/usr/bin/env bash
prev=""
for a in "$@"; do
  [ "$prev" = "-d" ] && printf '%s' "$a" > "$CURL_BODY_FILE"
  prev="$a"
done
# One line per send, so a test can count them.
echo "sent" >> "$CURL_SEND_LOG"
exit 0
"""

# Emits whatever the test put in JOURNAL_FIXTURE — that text becomes the
# dispatcher's context, and therefore its failure signature.
_FAKE_JOURNALCTL = r"""#!/usr/bin/env bash
cat "$JOURNAL_FIXTURE"
exit 0
"""

STALE_QQQ = (
    "Aug 31 19:40:21 host zerogex[1]: ERROR option chains QQQ: STALE — "
    "nothing written for 18.4 min (threshold 15.0)."
)
STALE_QQQ_LATER = (
    "Aug 31 19:50:15 host zerogex[1]: ERROR option chains QQQ: STALE — "
    "nothing written for 28.3 min (threshold 15.0)."
)
STALE_SPY = (
    "Aug 31 19:50:15 host zerogex[1]: ERROR underlying bars SPY: STALE — "
    "nothing written for 21.7 min (threshold 15.0)."
)


class _Harness:
    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.bindir = tmp_path / "bin"
        self.bindir.mkdir()
        for name, body in (("curl", _FAKE_CURL), ("journalctl", _FAKE_JOURNALCTL)):
            p = self.bindir / name
            p.write_text(body)
            p.chmod(0o755)
        self.send_log = tmp_path / "sends"
        self.body_file = tmp_path / "body"
        self.fixture = tmp_path / "journal.txt"
        self.state_dir = tmp_path / "state"

    def run(
        self,
        journal_text,
        *,
        unit="zerogex-oa-ingestion-freshness.service",
        reason="systemd unit failed",
        cooldown="3600",
    ):
        self.fixture.write_text(journal_text)
        env = dict(os.environ)
        env.update(
            PATH=f"{self.bindir}:{os.environ['PATH']}",
            ALERT_BACKEND="resend",
            ALERT_MIN_INTERVAL_SEC=cooldown,
            ALERT_STATE_DIR=str(self.state_dir),
            ALERT_APP_ENV=str(self.tmp / "nonexistent.env"),
            RESEND_API_KEY="re_test",
            RESEND_FROM_EMAIL="ZeroGEX <ops@example.com>",
            ALERT_EMAIL_TO="ops@example.com",
            CURL_BODY_FILE=str(self.body_file),
            CURL_SEND_LOG=str(self.send_log),
            JOURNAL_FIXTURE=str(self.fixture),
        )
        subprocess.run(["bash", str(ALERT), unit, reason], env=env, check=True)

    @property
    def sends(self) -> int:
        return len(self.send_log.read_text().splitlines()) if self.send_log.exists() else 0

    @property
    def body(self) -> dict:
        return json.loads(self.body_file.read_text())


@pytest.fixture
def h(tmp_path):
    return _Harness(tmp_path)


# --- the noise this exists to stop ------------------------------------------


def test_the_same_failure_repeating_sends_once(h):
    """Six firings of one stale symbol — an hour of the old behaviour."""
    for _ in range(6):
        h.run(STALE_QQQ)
    assert h.sends == 1


def test_the_same_failure_with_a_grown_counter_still_folds(h):
    """18.4 min and 28.3 min are the same outage. Numbers are flattened out of
    the signature precisely so a ticking staleness counter cannot defeat it."""
    h.run(STALE_QQQ)
    h.run(STALE_QQQ_LATER)
    assert h.sends == 1


# --- the hazard the fix must not introduce ----------------------------------


def test_a_different_failure_pages_immediately(h):
    """A second stream dying mid-cooldown is a NEW incident, not a duplicate.
    This is the case a plain per-unit timer would have swallowed."""
    h.run(STALE_QQQ)
    h.run(f"{STALE_QQQ}\n{STALE_SPY}")
    assert h.sends == 2


def test_a_different_unit_is_never_folded_into_another(h):
    h.run(STALE_QQQ, unit="zerogex-oa-ingestion-freshness.service")
    h.run(STALE_QQQ, unit="zerogex-oa-api.service")
    assert h.sends == 2


# --- escape hatches ---------------------------------------------------------


def test_cooldown_zero_restores_alert_every_time(h):
    for _ in range(3):
        h.run(STALE_QQQ, cooldown="0")
    assert h.sends == 3


def test_expiry_resends_and_reports_what_was_held(h):
    """After the window the same failure alerts again, and says how many were
    held — otherwise the quiet hour is indistinguishable from a fixed system.

    The window is aged by rewriting the recorded timestamp rather than by
    shortening the cooldown: three runs inside one wall-clock second leave
    age=0, which is below any positive cooldown, so the test would pass or
    fail on how fast the machine is."""
    h.run(STALE_QQQ)
    h.run(STALE_QQQ)  # held
    assert h.sends == 1

    state = next(h.state_dir.iterdir())
    epoch, sig, count = state.read_text().split()
    assert count == "1", "the held alert must be counted"
    state.write_text(f"{int(epoch) - 7200} {sig} {count}\n")

    h.run(STALE_QQQ)
    assert h.sends == 2
    assert "1 identical alert(s) were suppressed" in h.body["text"]


def test_recovery_is_never_suppressed_and_rearms(h):
    """A recovery notice must land, and must leave the next failure free to
    page at once rather than inheriting the outage's cooldown."""
    h.run(STALE_QQQ)
    h.run("Aug 31 20:00:00 host systemd[1]: Started.", reason="recovered — now active")
    h.run(STALE_QQQ)
    assert h.sends == 3


# --- it must fail open ------------------------------------------------------


def test_an_unusable_state_dir_still_alerts(h, capfd):
    """A cooldown that loses an alert is worse than one that repeats itself.

    Uses a path under a non-directory (ENOTDIR) rather than a chmod: CI and
    the deploy box both run this as root, where a mode-500 directory is still
    writable and the test would prove nothing.

    It must also fail QUIETLY. `printf > "$f" 2>/dev/null` does not: the SHELL
    performs the redirection before printf runs, so the open failure is
    reported on the shell's own stderr and the redirect never covers it.
    Production logged one raw bash error per alert this way on 2026-09-01,
    before StateDirectory= was installed.
    """
    h.state_dir = Path("/dev/null/cannot-exist")
    h.run(STALE_QQQ)
    h.run(STALE_QQQ)
    assert h.sends == 2
    err = capfd.readouterr().err
    assert "No such file or directory" not in err, err
    assert "zerogex-alert.sh: line" not in err, err


def test_a_corrupt_state_file_still_alerts(h):
    h.run(STALE_QQQ)
    state = next(h.state_dir.iterdir())
    state.write_text("garbage not a timestamp\n")
    h.run(STALE_QQQ)
    assert h.sends == 2
