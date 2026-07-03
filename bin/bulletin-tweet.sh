#!/usr/bin/env bash
#
# Manual runner for the Live Bulletin X-post job.
# ------------------------------------------------
# Convenience wrapper around ``python -m src.jobs.bulletin_tweet`` so the
# operator can preview any of the three daily fires without having to
# remember the flag grammar.  Every invocation is dry-run by default: it
# renders the tweet body, fetches the PNG from the frontend, and (if the
# host has Playwright installed) the Replay video, then writes all three
# to a per-mode/per-date directory under
# $BULLETIN_TWEET_ARTIFACT_DIR (default /var/lib/zerogex-oa/bulletin-tweets)
# so the operator can inspect exactly what would have gone out.
#
# Add ``--post`` (or ``-p``) to actually fire the tweet — requires the
# X_BOT_BEARER_TOKEN plus the four OAuth1 secrets configured in
# ~/zerogex-oa/.env.  Missing any of them silently degrades the run
# back to dry-run, so a half-configured rollout can never accidentally
# post.
#
# Usage:
#   bin/bulletin-tweet.sh <premarket|midday|close> [flags]
#
# Convenience aliases (default to dry-run):
#   bin/bulletin-tweet.sh premarket           # 09:15 slot
#   bin/bulletin-tweet.sh midday              # 12:30 slot
#   bin/bulletin-tweet.sh close               # 16:05 slot
#
#   bin/bulletin-tweet.sh close --post        # live post the close read
#   bin/bulletin-tweet.sh midday --date 2026-07-03 --allow-non-trading-day
#   bin/bulletin-tweet.sh close --no-media    # skip PNG + video render
#   bin/bulletin-tweet.sh close --short       # force 280-char fallback
#   bin/bulletin-tweet.sh close --artifact-dir /tmp/preview
#
# All extra flags are passed straight through to bulletin_tweet.py.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    cat <<EOF >&2
Usage: bin/bulletin-tweet.sh <premarket|midday|close> [--post] [--date YYYY-MM-DD]
                             [--symbols SPY,SPX,QQQ] [--lead-symbol SPX]
                             [--artifact-dir /path] [--no-media] [--short]
                             [--allow-non-trading-day]

Dry-run by default. Add --post to actually publish. Runs
src.jobs.bulletin_tweet under the venv at ``$$HOME/zerogex-oa/venv`` if
present, otherwise the system python3.
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
        echo "bulletin-tweet.sh: unknown mode '$MODE' (want premarket|midday|close)" >&2
        exit 2 ;;
esac

# Prefer the deployed venv when it exists (systemd path); fall back to
# the shell's python3 for dev laptops.  This mirrors what forecast_tweet
# and scorecard_tweet do — see setup/systemd/*.service.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "$SCRIPT_DIR/venv/bin/python" ]]; then
    PYTHON="$SCRIPT_DIR/venv/bin/python"
else
    PYTHON="${PYTHON:-python3}"
fi

# Load ~/.env if present so the operator doesn't have to source it
# manually before every invocation. Silently no-ops if the file is
# missing (dev laptop path).
#
# We disable ``set -u`` around the source: real-world ``.env`` files
# often contain values like ``SYMBOL_ALIASES=SPX=$SPXW.X`` that are
# meant to be read literally by python-dotenv but that bash tries to
# expand when sourcing.  Under nounset that expansion crashes the
# wrapper before Python ever runs.  Python's own dotenv parser reads
# the file as text and doesn't have this problem, so all we need out
# of the shell source is to seed the env vars whose values ARE plain
# strings — anything with a ``$`` in the value will get re-read
# correctly by the Python process anyway.
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set +u
    set -a
    # shellcheck disable=SC1090,SC1091
    source "$SCRIPT_DIR/.env"
    set +a
    set -u
fi

cd "$SCRIPT_DIR"

echo "=== bulletin-tweet: $MODE ==="
echo "Python:    $PYTHON"
echo "Repo:      $SCRIPT_DIR"
echo "Artifact:  ${BULLETIN_TWEET_ARTIFACT_DIR:-/var/lib/zerogex-oa/bulletin-tweets (default)}"
echo "Site URL:  ${ZEROGEX_SITE_URL:-https://zerogex.io (default)}"
echo

exec "$PYTHON" -m src.jobs.bulletin_tweet --mode "$MODE" "$@"
