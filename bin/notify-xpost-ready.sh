#!/usr/bin/env bash
#
# $BULLETIN_TWEET_NOTIFY_HOOK — "‹Timing› X-Post Ready" email with a link.
# ---------------------------------------------------------------------------
# Point BULLETIN_TWEET_NOTIFY_HOOK at this script to get a short email each
# time a bulletin X-post is auto-generated (09:15 / 12:30 / 16:05 ET).  The
# email says e.g. "Midday X-Post Ready" and carries ONE link to the admin
# review page (/admin/x-post) where you view, edit, regenerate, and copy the
# post to X.  The page is admin-only, so the link only opens for your Admin
# account.
#
# Unlike bin/sample-notify-resend.sh (which pastes the full draft + SSH
# approve/discard commands into the email), this hook is intentionally
# minimal: the review page is where the draft lives now.
#
# Install — add to ~/zerogex-oa/.env (NO surrounding quotes on the values —
# systemd's EnvironmentFile keeps quotes as part of the string):
#     RESEND_API_KEY=re_xxxxxxxxxxxxxx
#     RESEND_FROM_EMAIL=Michael at ZeroGEX <michael@zerogex.io>
#     BULLETIN_TWEET_EMAIL_TO=you@example.com
#     BULLETIN_TWEET_NOTIFY_HOOK=/home/ubuntu/zerogex-oa/bin/notify-xpost-ready.sh
#     # Optional — the review-page URL.  Defaults to
#     # ${ZEROGEX_SITE_URL:-https://zerogex.io}/admin/x-post
#     BULLETIN_TWEET_ADMIN_URL=https://zerogex.io/admin/x-post
#
# Uses only curl + python3 (both stock).  No pip install, no npm.  Any error
# logs to stderr and exits 0 so a mail hiccup never fails the tweet job.

set -euo pipefail

MODE="${1:-unknown}"

strip_quotes() {
    local v="$1"
    if [[ "$v" == \"*\" && ${#v} -ge 2 ]]; then echo "${v:1:-1}"; return; fi
    if [[ "$v" == \'*\' && ${#v} -ge 2 ]]; then echo "${v:1:-1}"; return; fi
    echo "$v"
}

RESEND_API_KEY="$(strip_quotes "${RESEND_API_KEY:-}")"
RESEND_FROM_EMAIL="$(strip_quotes "${RESEND_FROM_EMAIL:-}")"
BULLETIN_TWEET_EMAIL_TO="$(strip_quotes "${BULLETIN_TWEET_EMAIL_TO:-}")"
ZEROGEX_SITE_URL="$(strip_quotes "${ZEROGEX_SITE_URL:-https://zerogex.io}")"
ADMIN_URL="$(strip_quotes "${BULLETIN_TWEET_ADMIN_URL:-${ZEROGEX_SITE_URL%/}/admin/x-post}")"

if [[ -z "$RESEND_API_KEY" || -z "$RESEND_FROM_EMAIL" || -z "$BULLETIN_TWEET_EMAIL_TO" ]]; then
    echo "notify-xpost-ready: RESEND_API_KEY / RESEND_FROM_EMAIL / BULLETIN_TWEET_EMAIL_TO not all set — skipping" >&2
    exit 0
fi

# Friendly timing word from the mode — matches "market open / midday /
# market close".
case "$MODE" in
    premarket) TIMING="Market Open" ;;
    midday)    TIMING="Midday" ;;
    close)     TIMING="Market Close" ;;
    *)         TIMING="$MODE" ;;
esac

export RESEND_API_KEY RESEND_FROM_EMAIL BULLETIN_TWEET_EMAIL_TO
export BT_TIMING="$TIMING"
export BT_ADMIN_URL="$ADMIN_URL"

RESPONSE_PATH="/tmp/xpost-ready-resend-response.json"

python3 - <<'PY' > /tmp/xpost-ready-resend-payload.json
import json
import os

timing = os.environ["BT_TIMING"]
url = os.environ["BT_ADMIN_URL"]
subject = f"{timing} X-Post Ready"

html = (
    f"<h2>{timing} X-Post Ready</h2>"
    f"<p>The {timing.lower()} X-post has been generated.</p>"
    f'<p><a href="{url}" '
    f'style="display:inline-block;padding:10px 18px;background:#111;color:#fff;'
    f'border-radius:8px;text-decoration:none;font-weight:600">'
    f"Review &amp; copy the post &rarr;</a></p>"
    f'<p style="color:#666;font-size:13px">Or open: <a href="{url}">{url}</a><br>'
    f"You'll need to be signed in to your Admin account to view it.</p>"
)
text = f"{timing} X-Post Ready\n\nReview & copy the post: {url}\n(Admin sign-in required.)"

payload = {
    "from": os.environ["RESEND_FROM_EMAIL"],
    "to": [os.environ["BULLETIN_TWEET_EMAIL_TO"]],
    "subject": subject,
    "html": html,
    "text": text,
}
print(json.dumps(payload))
PY

curl -sS -X POST https://api.resend.com/emails \
    -H "Authorization: Bearer $RESEND_API_KEY" \
    -H "Content-Type: application/json" \
    --data-binary "@/tmp/xpost-ready-resend-payload.json" \
    -o "$RESPONSE_PATH" \
    -w "notify-xpost-ready: HTTP %{http_code} — see $RESPONSE_PATH\n" \
    || echo "notify-xpost-ready: curl failed — skipping (non-fatal)" >&2

exit 0
