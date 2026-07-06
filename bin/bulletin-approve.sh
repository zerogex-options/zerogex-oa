#!/usr/bin/env bash
#
# Approve (or discard) a staged bulletin tweet draft.
# ---------------------------------------------------
# Companion to bin/bulletin-tweet.sh.  When the systemd cron fires
# with ``--stage``, it writes a full draft (text + PNG + clip +
# manifest with state=pending) to
# $BULLETIN_TWEET_ARTIFACT_DIR/<mode>/<date>/ instead of posting.  The
# operator reviews the draft (via SSH, email, ntfy, etc.) and then
# runs this script to post it — or to reject it.
#
# Usage:
#   bin/bulletin-approve.sh <premarket|midday|close> [flags]
#
# Common invocations:
#   bin/bulletin-approve.sh close                    # POST today's close draft
#   bin/bulletin-approve.sh close --discard          # reject it
#   bin/bulletin-approve.sh close --print            # dump text + media paths
#                                                    # for manual paste-into-X
#   bin/bulletin-approve.sh close --print --mark-posted
#                                                    # print + mark posted
#                                                    # (use after pasting manually)
#   bin/bulletin-approve.sh close --date 2026-07-06  # yesterday's draft
#   bin/bulletin-approve.sh close --short            # force 280-char fallback
#
# Extra flags are passed through to bulletin_approve.py.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    cat <<EOF >&2
Usage: bin/bulletin-approve.sh <premarket|midday|close> [--discard]
                               [--print [--mark-posted]] [--short]
                               [--date YYYY-MM-DD] [--artifact-dir /path]

Reads the latest pending draft for <mode> and posts it (or discards it,
or prints it for manual paste).
EOF
    exit 2
fi

MODE="$1"
shift

case "$MODE" in
    premarket|midday|close) ;;
    -h|--help)
        exec "$0" 2>&1 || true ;;
    *)
        echo "bulletin-approve.sh: unknown mode '$MODE' (want premarket|midday|close)" >&2
        exit 2 ;;
esac

# Prefer the deployed venv when it exists (systemd path); fall back to
# the shell's python3 for dev laptops.  Same lookup as bulletin-tweet.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "$SCRIPT_DIR/venv/bin/python" ]]; then
    PYTHON="$SCRIPT_DIR/venv/bin/python"
else
    PYTHON="${PYTHON:-python3}"
fi

cd "$SCRIPT_DIR"

echo "=== bulletin-approve: $MODE ==="
echo "Python:    $PYTHON"
echo "Repo:      $SCRIPT_DIR"
echo "Artifact:  ${BULLETIN_TWEET_ARTIFACT_DIR:-/var/lib/zerogex-oa/bulletin-tweets (default)}"
echo

exec "$PYTHON" -m src.jobs.bulletin_approve --mode "$MODE" "$@"
