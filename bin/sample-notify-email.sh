#!/usr/bin/env bash
#
# Sample $BULLETIN_TWEET_NOTIFY_HOOK — sends the draft via email.
# ---------------------------------------------------------------
# Uses the local ``mail`` command (from bsd-mailx / mailutils) so it
# just works on a stock Ubuntu host that has an outgoing SMTP relay.
# For an EC2 host without local mail: install msmtp or sSMTP and
# point it at Gmail / SES / whatever.
#
# To install:
#   1. sudo apt-get install -y mailutils  (or ssmtp, msmtp, ...)
#   2. Verify: echo test | mail -s "test" you@example.com — check inbox.
#   3. Set BULLETIN_TWEET_EMAIL_TO=you@example.com in ~/zerogex-oa/.env
#   4. Set BULLETIN_TWEET_NOTIFY_HOOK=/home/ubuntu/zerogex-oa/bin/sample-notify-email.sh
#      in ~/zerogex-oa/.env.
#
# The staging job passes ``$1=mode`` and ``$2=artifact_dir`` and
# additional context via BULLETIN_TWEET_* env vars.  This script pulls
# the tweet text out of the artifact directory and attaches the PNG
# preview so the operator can review from their inbox.

set -euo pipefail

MODE="${1:-unknown}"
ARTIFACT_DIR="${2:-unknown}"

TO="${BULLETIN_TWEET_EMAIL_TO:-}"
if [[ -z "$TO" ]]; then
    echo "sample-notify-email: BULLETIN_TWEET_EMAIL_TO not set — skipping" >&2
    exit 0
fi

TEXT_PATH="$ARTIFACT_DIR/tweet_text.md"
if [[ ! -f "$TEXT_PATH" ]]; then
    echo "sample-notify-email: no tweet_text.md at $TEXT_PATH — skipping" >&2
    exit 0
fi

SUBJECT="ZeroGEX bulletin $MODE — approval needed (${BULLETIN_TWEET_LEAD_SYMBOL:-?})"

# Compose the message with instructions at the top and the draft below.
BODY_TMP=$(mktemp)
trap 'rm -f "$BODY_TMP"' EXIT
{
    echo "New $MODE draft staged and waiting for approval."
    echo
    echo "Text length:   ${BULLETIN_TWEET_TEXT_LEN:-?} chars"
    echo "PNG attached:  ${BULLETIN_TWEET_HAS_PNG:-0}"
    echo "Clip present:  ${BULLETIN_TWEET_HAS_CLIP:-0}"
    echo "Artifacts:     $ARTIFACT_DIR"
    echo
    echo "To approve (POST to X):"
    echo "    ssh <host> 'cd zerogex-oa && bin/bulletin-approve.sh $MODE'"
    echo
    echo "To discard:"
    echo "    ssh <host> 'cd zerogex-oa && bin/bulletin-approve.sh $MODE --discard'"
    echo
    echo "----- DRAFT -----"
    cat "$TEXT_PATH"
    echo "-----------------"
} > "$BODY_TMP"

PNG_PATH=""
if [[ "${BULLETIN_TWEET_HAS_PNG:-0}" == "1" ]]; then
    PNG_PATH="$ARTIFACT_DIR/bulletin-${BULLETIN_TWEET_LEAD_SYMBOL,,}.png"
    if [[ ! -f "$PNG_PATH" ]]; then
        PNG_PATH=""  # not fatal — send text-only
    fi
fi

if [[ -n "$PNG_PATH" ]] && command -v mail >/dev/null; then
    # ``mail -A`` for attachments works with mailutils; other mail
    # binaries may need mutt or mpack — falls back to no-attachment
    # below when unavailable.
    mail -s "$SUBJECT" -A "$PNG_PATH" "$TO" < "$BODY_TMP" \
        || mail -s "$SUBJECT" "$TO" < "$BODY_TMP"
else
    mail -s "$SUBJECT" "$TO" < "$BODY_TMP"
fi
