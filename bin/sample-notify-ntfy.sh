#!/usr/bin/env bash
#
# Sample $BULLETIN_TWEET_NOTIFY_HOOK — sends a push notification via ntfy.sh.
# ------------------------------------------------------------------
# ntfy.sh is a free push service — no account, no OAuth.  Pick a
# random topic name known only to you and subscribe to it in the
# ntfy iOS or Android app.  Every POST to
# https://ntfy.sh/<your-topic> pings your phone within seconds.
#
# To install:
#   1. Pick a hard-to-guess topic name, e.g. ``zerogex-drafts-abc123xyz``.
#      Anyone who knows the topic name receives your notifications, so
#      keep it long enough to be unguessable.
#   2. Install ntfy from the App Store / Play Store, subscribe to the
#      topic.
#   3. Set BULLETIN_TWEET_NTFY_TOPIC=<your-topic> in ~/zerogex-oa/.env.
#   4. Set BULLETIN_TWEET_NOTIFY_HOOK=/home/ubuntu/zerogex-oa/bin/sample-notify-ntfy.sh
#      in ~/zerogex-oa/.env.
#   5. Next --stage fire pings your phone.
#
# The staging job passes ``$1=mode`` and ``$2=artifact_dir`` and
# additional context via BULLETIN_TWEET_* env vars.

set -euo pipefail

MODE="${1:-unknown}"
ARTIFACT_DIR="${2:-unknown}"

TOPIC="${BULLETIN_TWEET_NTFY_TOPIC:-}"
if [[ -z "$TOPIC" ]]; then
    echo "sample-notify-ntfy: BULLETIN_TWEET_NTFY_TOPIC not set — skipping" >&2
    exit 0
fi

# Compose the notification body: mode + text length + attach hint.
BODY="ZeroGEX bulletin draft ready.
Mode: $MODE
Text: ${BULLETIN_TWEET_TEXT_LEN:-?} chars"

if [[ "${BULLETIN_TWEET_HAS_PNG:-0}" == "1" ]]; then
    BODY+=$'\nPNG: yes'
fi
if [[ "${BULLETIN_TWEET_HAS_CLIP:-0}" == "1" ]]; then
    BODY+=$'\nClip: yes'
fi
BODY+="

Approve: bin/bulletin-approve.sh $MODE
Discard: bin/bulletin-approve.sh $MODE --discard
Artifacts: $ARTIFACT_DIR"

curl -sS \
    -H "Title: ZeroGEX bulletin $MODE — approval needed" \
    -H "Priority: high" \
    -H "Tags: chart_with_upwards_trend" \
    -d "$BODY" \
    "https://ntfy.sh/$TOPIC" >/dev/null \
    || echo "sample-notify-ntfy: POST to ntfy.sh failed" >&2
