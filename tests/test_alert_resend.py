"""The ``resend`` backend of zerogex-alert.sh must build a correct Resend API
request — and, crucially, source the API key from the app .env when it isn't
duplicated into alert.env, and default the recipient to the From address.

Drives setup/systemd/zerogex-alert.sh with a fake ``curl`` (captures the URL,
headers, and JSON body instead of sending) and a fake ``journalctl`` on PATH.
Requires ``jq`` (the dispatcher uses it to build the payload); skipped if
absent.
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
: > "$CURL_HEADERS_FILE"
prev=""
for a in "$@"; do
  case "$a" in
    https://*) printf '%s' "$a" > "$CURL_URL_FILE" ;;
  esac
  [ "$prev" = "-H" ] && printf '%s\n' "$a" >> "$CURL_HEADERS_FILE"
  [ "$prev" = "-d" ] && printf '%s' "$a" > "$CURL_BODY_FILE"
  prev="$a"
done
exit 0
"""

_FAKE_JOURNALCTL = r"""#!/usr/bin/env bash
echo "fake journal line for context"
exit 0
"""


def _exec(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o755)


class _Harness:
    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.bindir = tmp_path / "bin"
        self.bindir.mkdir()
        _exec(self.bindir / "curl", _FAKE_CURL)
        _exec(self.bindir / "journalctl", _FAKE_JOURNALCTL)
        self.url_file = tmp_path / "url"
        self.headers_file = tmp_path / "headers"
        self.body_file = tmp_path / "body"

    def run(self, *, unit="zerogex-oa-api", reason="not active (inactive)", extra_env=None):
        env = dict(os.environ)
        env["PATH"] = f"{self.bindir}:{env['PATH']}"
        env["ALERT_BACKEND"] = "resend"
        env["CURL_URL_FILE"] = str(self.url_file)
        env["CURL_HEADERS_FILE"] = str(self.headers_file)
        env["CURL_BODY_FILE"] = str(self.body_file)
        # Ensure a stray /etc/zerogex/alert.env on a dev box can't interfere.
        env["ALERT_APP_ENV"] = env.get("ALERT_APP_ENV", str(self.tmp / "nonexistent.env"))
        if extra_env:
            env.update(extra_env)
        subprocess.run(["bash", str(ALERT), unit, reason], env=env, check=True)

    @property
    def url(self) -> str:
        return self.url_file.read_text() if self.url_file.exists() else ""

    @property
    def headers(self) -> list[str]:
        return self.headers_file.read_text().splitlines() if self.headers_file.exists() else []

    @property
    def body(self) -> dict:
        return json.loads(self.body_file.read_text())


@pytest.fixture
def h(tmp_path):
    return _Harness(tmp_path)


def test_resend_builds_correct_request_from_env(h):
    h.run(extra_env={
        "RESEND_API_KEY": "re_testkey_abc123",
        "RESEND_FROM_EMAIL": "Michael at ZeroGEX <michael@zerogex.io>",
        "ALERT_EMAIL_TO": "ops@zerogex.io",
    })
    assert h.url == "https://api.resend.com/emails"
    assert "Authorization: Bearer re_testkey_abc123" in h.headers
    body = h.body
    assert body["from"] == "Michael at ZeroGEX <michael@zerogex.io>"
    assert body["to"] == ["ops@zerogex.io"]
    # Subject is the one-line summary; body carries the reason + journal tail.
    assert "not active (inactive)" in body["subject"]
    assert "zerogex-oa-api" in body["subject"]
    assert "fake journal line for context" in body["text"]


def test_resend_reads_key_from_app_env_when_not_in_alert_env(h):
    # The key lives ONLY in the app .env (as on the box) — the backend must
    # pull it rather than require duplication into alert.env.
    app_env = h.tmp / "app.env"
    app_env.write_text(
        "SOME_OTHER=1\n"
        "RESEND_API_KEY=re_from_dotenv_xyz\n"
        "RESEND_FROM_EMAIL=Michael at ZeroGEX <michael@zerogex.io>\n"
    )
    h.run(extra_env={"ALERT_APP_ENV": str(app_env), "ALERT_EMAIL_TO": "ops@zerogex.io"})
    assert "Authorization: Bearer re_from_dotenv_xyz" in h.headers
    assert h.body["from"] == "Michael at ZeroGEX <michael@zerogex.io>"


def test_resend_defaults_recipient_to_from_address(h):
    # With no ALERT_EMAIL_TO, the recipient is the address inside the From.
    h.run(extra_env={
        "RESEND_API_KEY": "re_testkey",
        "RESEND_FROM_EMAIL": "Michael at ZeroGEX <michael@zerogex.io>",
    })
    assert h.body["to"] == ["michael@zerogex.io"]


def test_resend_supports_multiple_recipients(h):
    h.run(extra_env={
        "RESEND_API_KEY": "re_testkey",
        "RESEND_FROM_EMAIL": "michael@zerogex.io",
        "ALERT_EMAIL_TO": "a@zerogex.io, b@zerogex.io",
    })
    assert h.body["to"] == ["a@zerogex.io", "b@zerogex.io"]
