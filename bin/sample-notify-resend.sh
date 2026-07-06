#!/usr/bin/env bash
#
# Sample $BULLETIN_TWEET_NOTIFY_HOOK — sends the draft via Resend API.
# --------------------------------------------------------------------
# Uses the same Resend account the zerogex-web frontend uses for
# transactional email, so there's no SMTP relay to configure and no
# Gmail App Password to babysit.  Just an API key + a verified sender
# address — you have both already.
#
# To install:
#   1. Reuse the RESEND_API_KEY from the frontend .env.local, or
#      create a new one at https://resend.com/api-keys.
#   2. Add to ~/zerogex-oa/.env (NO surrounding quotes on the values —
#      systemd's EnvironmentFile keeps quotes as part of the string):
#        RESEND_API_KEY=re_xxxxxxxxxxxxxx
#        RESEND_FROM_EMAIL=Michael at ZeroGEX <michael@zerogex.io>
#        BULLETIN_TWEET_EMAIL_TO=you@example.com
#        BULLETIN_TWEET_NOTIFY_HOOK=/home/ubuntu/zerogex-oa/bin/sample-notify-resend.sh
#   3. Next --stage fire emails the draft to $BULLETIN_TWEET_EMAIL_TO
#      with the PNG attached.
#
# Uses only curl + python3 (both stock).  No pip install, no npm.

set -euo pipefail

MODE="${1:-unknown}"
ARTIFACT_DIR="${2:-unknown}"

# Strip surrounding quotes that a user might have put in .env — a common
# mistake since dotenv accepts them but our shell/systemd load path does
# not.  Only strips a *pair* of matching quotes; embedded quotes are
# preserved.
strip_quotes() {
    local v="$1"
    if [[ "$v" == \"*\" && ${#v} -ge 2 ]]; then echo "${v:1:-1}"; return; fi
    if [[ "$v" == \'*\' && ${#v} -ge 2 ]]; then echo "${v:1:-1}"; return; fi
    echo "$v"
}

RESEND_API_KEY="$(strip_quotes "${RESEND_API_KEY:-}")"
RESEND_FROM_EMAIL="$(strip_quotes "${RESEND_FROM_EMAIL:-}")"
BULLETIN_TWEET_EMAIL_TO="$(strip_quotes "${BULLETIN_TWEET_EMAIL_TO:-}")"

if [[ -z "$RESEND_API_KEY" || -z "$RESEND_FROM_EMAIL" || -z "$BULLETIN_TWEET_EMAIL_TO" ]]; then
    echo "sample-notify-resend: RESEND_API_KEY / RESEND_FROM_EMAIL / BULLETIN_TWEET_EMAIL_TO not all set — skipping" >&2
    exit 0
fi

TEXT_PATH="$ARTIFACT_DIR/tweet_text.md"
if [[ ! -f "$TEXT_PATH" ]]; then
    echo "sample-notify-resend: no tweet_text.md at $TEXT_PATH — skipping" >&2
    exit 0
fi

LEAD_SYMBOL="${BULLETIN_TWEET_LEAD_SYMBOL:-SPX}"
PNG_PATH="$ARTIFACT_DIR/bulletin-${LEAD_SYMBOL,,}.png"

# Export the sanitized values so the Python script can read them via
# os.environ — safer than interpolating shell variables into a
# heredoc, which broke on any value containing quotes / newlines / $.
export RESEND_API_KEY RESEND_FROM_EMAIL BULLETIN_TWEET_EMAIL_TO
export BT_MODE="$MODE"
export BT_LEAD_SYMBOL="$LEAD_SYMBOL"
export BT_ARTIFACT_DIR="$ARTIFACT_DIR"
export BT_TEXT_PATH="$TEXT_PATH"
export BT_PNG_PATH="$PNG_PATH"
export BT_TEXT_LEN="${BULLETIN_TWEET_TEXT_LEN:-?}"
export BT_HAS_PNG="${BULLETIN_TWEET_HAS_PNG:-0}"
export BT_HAS_CLIP="${BULLETIN_TWEET_HAS_CLIP:-0}"

RESPONSE_PATH="/tmp/bulletin-resend-response.json"

python3 - <<'PY' > /tmp/bulletin-resend-payload.json
import base64
import json
import os

mode = os.environ["BT_MODE"]
lead = os.environ["BT_LEAD_SYMBOL"]
artifact_dir = os.environ["BT_ARTIFACT_DIR"]
text_len = os.environ.get("BT_TEXT_LEN", "?")
has_png = os.environ.get("BT_HAS_PNG", "0") == "1"
has_clip = os.environ.get("BT_HAS_CLIP", "0") == "1"

text = open(os.environ["BT_TEXT_PATH"]).read()

# HTML body: instructions at top, then the draft.  We use direct
# string replacement (not %-formatting or f-strings) so a stray "%"
# or "{" in the tweet body never breaks the render.
approve_line = f"ssh &lt;host&gt; 'cd zerogex-oa &amp;&amp; bin/bulletin-approve.sh {mode}'"
discard_line = f"ssh &lt;host&gt; 'cd zerogex-oa &amp;&amp; bin/bulletin-approve.sh {mode} --discard'"
escaped_text = (
    text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
)

html = (
    f"<h2>ZeroGEX bulletin: {mode} &mdash; approval needed</h2>"
    f"<p><b>Text length:</b> {text_len} chars &middot; "
    f"<b>PNG attached:</b> {'yes' if has_png else 'no'} &middot; "
    f"<b>Clip present:</b> {'yes' if has_clip else 'no'}</p>"
    f"<p><b>To approve (POST to X):</b><br><code>{approve_line}</code></p>"
    f"<p><b>To discard:</b><br><code>{discard_line}</code></p>"
    f"<p><b>Artifacts:</b> <code>{artifact_dir}</code></p>"
    f"<hr>"
    f"<pre style=\"white-space: pre-wrap; "
    f"font-family: ui-monospace, Menlo, monospace\">{escaped_text}</pre>"
)

payload = {
    "from": os.environ["RESEND_FROM_EMAIL"],
    "to": [os.environ["BULLETIN_TWEET_EMAIL_TO"]],
    "subject": f"ZeroGEX bulletin {mode} — approval needed ({lead})",
    "html": html,
    "text": text,
}

png_path = os.environ["BT_PNG_PATH"]
if has_png and os.path.exists(png_path):
    with open(png_path, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    payload["attachments"] = [{
        "filename": os.path.basename(png_path),
        "content": b64,
    }]

print(json.dumps(payload))
PY

curl -sS -X POST https://api.resend.com/emails \
    -H "Authorization: Bearer $RESEND_API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@/tmp/bulletin-resend-payload.json" \
    -o "$RESPONSE_PATH" \
    -w "sample-notify-resend: HTTP %{http_code} — see $RESPONSE_PATH\n"
