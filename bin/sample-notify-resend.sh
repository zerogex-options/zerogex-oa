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
#   2. Add to ~/zerogex-oa/.env:
#        RESEND_API_KEY=re_xxxxxxxxxxxxxx
#        RESEND_FROM_EMAIL=alerts@your-verified-domain.com
#        BULLETIN_TWEET_EMAIL_TO=you@example.com
#        BULLETIN_TWEET_NOTIFY_HOOK=/home/ubuntu/zerogex-oa/bin/sample-notify-resend.sh
#   3. Next --stage fire emails the draft to $BULLETIN_TWEET_EMAIL_TO
#      with the PNG attached.
#
# Uses only curl + base64 (both stock).  No pip install, no npm.

set -euo pipefail

MODE="${1:-unknown}"
ARTIFACT_DIR="${2:-unknown}"

API_KEY="${RESEND_API_KEY:-}"
FROM="${RESEND_FROM_EMAIL:-}"
TO="${BULLETIN_TWEET_EMAIL_TO:-}"

if [[ -z "$API_KEY" || -z "$FROM" || -z "$TO" ]]; then
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

# Compose the JSON payload.  We use a Python one-liner to safely
# json-encode the body text (which contains newlines, quotes, and
# the occasional em-dash) and to base64-encode the PNG for the
# ``attachments`` array Resend expects.
PY_PAYLOAD=$(python3 - <<PY
import base64
import json
import os

text = open("$TEXT_PATH").read()
mode = "$MODE"
lead = "$LEAD_SYMBOL"
artifact_dir = "$ARTIFACT_DIR"
text_len = os.environ.get("BULLETIN_TWEET_TEXT_LEN", "?")
has_png = os.environ.get("BULLETIN_TWEET_HAS_PNG", "0") == "1"
has_clip = os.environ.get("BULLETIN_TWEET_HAS_CLIP", "0") == "1"

# HTML body: instructions at top, then the draft, then metadata.
approve_line = f"ssh &lt;host&gt; 'cd zerogex-oa &amp;&amp; bin/bulletin-approve.sh {mode}'"
discard_line = f"ssh &lt;host&gt; 'cd zerogex-oa &amp;&amp; bin/bulletin-approve.sh {mode} --discard'"
escaped_text = (
    text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
)
html = f"""<h2>ZeroGEX bulletin: {mode} — approval needed</h2>
<p><b>Text length:</b> {text_len} chars &middot;
   <b>PNG attached:</b> {"yes" if has_png else "no"} &middot;
   <b>Clip present:</b> {"yes" if has_clip else "no"}</p>
<p><b>To approve (POST to X):</b><br><code>{approve_line}</code></p>
<p><b>To discard:</b><br><code>{discard_line}</code></p>
<p><b>Artifacts:</b> <code>{artifact_dir}</code></p>
<hr>
<pre style="white-space: pre-wrap; font-family: ui-monospace, Menlo, monospace">{escaped_text}</pre>
"""

payload = {
    "from": "$FROM",
    "to": ["$TO"],
    "subject": f"ZeroGEX bulletin {mode} — approval needed ({lead})",
    "html": html,
    "text": text,
}

png_path = "$PNG_PATH"
if has_png and os.path.exists(png_path):
    with open(png_path, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    payload["attachments"] = [{
        "filename": os.path.basename(png_path),
        "content": b64,
    }]

print(json.dumps(payload))
PY
)

curl -sS -X POST https://api.resend.com/emails \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d "$PY_PAYLOAD" \
    -o /tmp/bulletin-resend-response.json \
    -w "HTTP %{http_code}\n" \
    || echo "sample-notify-resend: POST to Resend failed" >&2
