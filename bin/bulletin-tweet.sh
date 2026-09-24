#!/usr/bin/env bash
#
# Manual runner for the Live Bulletin X-post job.
# ------------------------------------------------
# Convenience wrapper around ``python -m src.jobs.bulletin_tweet`` so the
# operator can preview any of the three daily fires without having to
# remember the flag grammar.  Every invocation is dry-run by default: it
# screenshots the live bulletin card, writes the post from its numbers
# and the latest CNBC headlines, runs the review, and writes the post,
# reply, PNG and manifest to a per-mode/per-date directory under
# $BULLETIN_TWEET_ARTIFACT_DIR (default /var/lib/zerogex-oa/bulletin-tweets)
# so the operator can inspect exactly what would have gone out.  A run the
# review holds back exits 1 and lists the reasons.
#
# Add ``--post`` to actually post (only once the review passes) — requires
# the four X OAuth1 keys in ~/zerogex-oa/.env.  Without them nothing is
# posted.
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
#   bin/bulletin-tweet.sh close --no-media    # skip the screenshot (can't post)
#   bin/bulletin-tweet.sh close --short       # force 280-char fallback
#   bin/bulletin-tweet.sh close --artifact-dir /tmp/preview
#
# All extra flags are passed straight through to bulletin_tweet.py.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    cat <<EOF >&2
Usage: bin/bulletin-tweet.sh <premarket|midday|close> [--post] [--date YYYY-MM-DD]
                             [--symbols SPY,SPX,QQQ] [--lead-symbol SPY]
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

# Note: we deliberately do NOT ``source`` the .env file here.  Real
# .env files at zerogex-oa contain values that are legal Python-
# dotenv syntax but not legal bash — unquoted lists like
# ``KEY=0.010,foo,bar`` (bash parses ``foo,bar`` as a command name)
# and $-refs like ``SYMBOL_ALIASES=SPX=$SPXW.X`` (bash tries to
# expand ``$SPXW``).  Sourcing that file in bash crashes on the
# first offending line long before Python runs.
#
# We don't need those vars in the shell env anyway: the Python job
# imports ``src.config`` which calls ``load_dotenv()`` at startup,
# so the .env is read correctly by the actual consumer.  This
# wrapper only exists to pick the venv Python and print a banner.
#
# Systemd invocations of the job go through ``EnvironmentFile=`` in
# the .service unit, which uses systemd's own parser (also not
# bash) so the production path was never affected.

cd "$SCRIPT_DIR"

echo "=== bulletin-tweet: $MODE ==="
echo "Python:    $PYTHON"
echo "Repo:      $SCRIPT_DIR"
echo "Artifact:  ${BULLETIN_TWEET_ARTIFACT_DIR:-/var/lib/zerogex-oa/bulletin-tweets (default)}"
echo "Site URL:  ${ZEROGEX_SITE_URL:-https://zerogex.io (default)}"
echo

exec "$PYTHON" -m src.jobs.bulletin_tweet --mode "$MODE" "$@"
