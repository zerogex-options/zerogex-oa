#!/usr/bin/env bash
#
# Pull the futures ingester's journal around a reported chart delay.
# ------------------------------------------------------------------
# Companion to setup/database/diagnostics/futures_feed_forensics.sql.
# The SQL says WHETHER and WHEN the feed was late; this says WHY, by
# showing the reconnects, auth refreshes and worker respawns that the
# ingester logged inside the same window.
#
# Run the SQL FIRST. It reads a table that survives for days; this reads
# the journal, which is capped (see `make journal-volume`) and routinely
# holds only hours. A report that arrives after lunch can easily outlive
# its own logs — so this script leads with a coverage check and tells you
# plainly when the window is already gone rather than printing an empty
# result that reads like "nothing happened".
#
# Usage:
#   bin/futures-feed-logs.sh [flags]
#
#   --date YYYY-MM-DD   session date, in --tz            (default: today)
#   --open HH:MM        local cash open to anchor on     (default: 08:00)
#   --tz ZONE           zone --open is quoted in         (default: Europe/London)
#   --pre MINUTES       window starts this far before    (default: 45)
#   --post MINUTES      window ends this far after       (default: 120)
#   --symbol INDEX      NDX / SPX, or 'all'              (default: all)
#   --raw               every futures line, unfiltered
#   --service UNIT      override the systemd unit
#
# Examples:
#   # The London-open default: today, 07:15-10:00 London.
#   bin/futures-feed-logs.sh
#
#   # A specific morning, NQ only, widened to three hours.
#   bin/futures-feed-logs.sh --date 2026-09-14 --symbol NDX --post 180
#
#   # New York cash open instead.
#   bin/futures-feed-logs.sh --open 09:30 --tz America/New_York
#
set -uo pipefail

SERVICE="zerogex-oa-ingestion"
DATE_ARG="today"
OPEN_LOCAL="08:00"
TZ_LOCAL="Europe/London"
PRE_MIN=45
POST_MIN=120
SYMBOL="all"
RAW=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)    DATE_ARG="$2";   shift 2 ;;
        --open)    OPEN_LOCAL="$2"; shift 2 ;;
        --tz)      TZ_LOCAL="$2";   shift 2 ;;
        --pre)     PRE_MIN="$2";    shift 2 ;;
        --post)    POST_MIN="$2";   shift 2 ;;
        --symbol)  SYMBOL="$2";     shift 2 ;;
        --service) SERVICE="$2";    shift 2 ;;
        --raw)     RAW=1;           shift   ;;
        -h|--help) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown flag: $1 (try --help)" >&2; exit 2 ;;
    esac
done

# Resolve the anchor through GNU date's TZ prefix so DST is handled for us:
# 08:00 Europe/London is 03:00 ET in BST and 02:00 ET in GMT, and getting
# that wrong points the whole search an hour off the incident.
if ! ANCHOR_EPOCH=$(date -d "TZ=\"${TZ_LOCAL}\" ${DATE_ARG} ${OPEN_LOCAL}" +%s 2>/dev/null); then
    echo "Could not parse --date '${DATE_ARG}' --open '${OPEN_LOCAL}' --tz '${TZ_LOCAL}'" >&2
    exit 2
fi

START_EPOCH=$(( ANCHOR_EPOCH - PRE_MIN  * 60 ))
END_EPOCH=$((   ANCHOR_EPOCH + POST_MIN * 60 ))

# journalctl parses --since/--until in the BOX's local time, so hand it
# box-local strings rather than assuming it shares the operator's zone.
SINCE=$(date -d "@${START_EPOCH}" '+%Y-%m-%d %H:%M:%S')
UNTIL=$(date -d "@${END_EPOCH}"   '+%Y-%m-%d %H:%M:%S')

echo "=== Futures feed logs ==="
echo "Unit:      ${SERVICE}"
echo "Anchor:    ${OPEN_LOCAL} ${TZ_LOCAL} on ${DATE_ARG}"
printf 'Window:    %s  ->  %s   (box local)\n' "$SINCE" "$UNTIL"
printf '           %s  ->  %s   (UTC)\n' \
    "$(date -u -d "@${START_EPOCH}" '+%Y-%m-%d %H:%M:%S')" \
    "$(date -u -d "@${END_EPOCH}"   '+%Y-%m-%d %H:%M:%S')"
printf '           %s  ->  %s   (%s)\n' \
    "$(TZ="$TZ_LOCAL" date -d "@${START_EPOCH}" '+%Y-%m-%d %H:%M:%S')" \
    "$(TZ="$TZ_LOCAL" date -d "@${END_EPOCH}"   '+%Y-%m-%d %H:%M:%S')" "$TZ_LOCAL"
echo "Symbol:    ${SYMBOL}"
echo ""

# --- Does the journal still reach back that far? -------------------------
# Asked BEFORE anything is grepped. An empty grep over a window the journal
# no longer covers looks identical to a clean feed, and that misread is the
# whole reason this check leads.
OLDEST_RAW=$(sudo journalctl -u "$SERVICE" -o short-unix --no-pager 2>/dev/null | head -1 | cut -d' ' -f1)
OLDEST_EPOCH=${OLDEST_RAW%%.*}
if [[ -z "$OLDEST_EPOCH" || ! "$OLDEST_EPOCH" =~ ^[0-9]+$ ]]; then
    echo "!! Could not read the journal for ${SERVICE}."
    echo "   Check the unit name (systemctl list-units | grep zerogex) and that"
    echo "   this user can sudo journalctl."
    exit 1
fi
echo "Journal for ${SERVICE} starts: $(date -d "@${OLDEST_EPOCH}" '+%Y-%m-%d %H:%M:%S') (box local)"
if (( OLDEST_EPOCH > START_EPOCH )); then
    echo ""
    echo "!! THE WINDOW IS OUTSIDE THE JOURNAL — it has already been rotated away."
    echo "   Anything printed below is a PARTIAL view and silence is NOT evidence"
    echo "   the feed was healthy. Use the SQL instead:"
    echo "     make futures-forensics SYMBOL=NDX DATE=${DATE_ARG}"
    echo "   and see 'make journal-volume' for why retention is this short."
    echo ""
fi
echo ""

case "$SYMBOL" in
    all|ALL|"") SYM_RE="" ;;
    *)          SYM_RE="$(printf '%s' "$SYMBOL" | tr '[:lower:]' '[:upper:]')" ;;
esac

LOG=$(sudo journalctl -u "$SERVICE" --since "$SINCE" --until "$UNTIL" --no-pager 2>/dev/null)

if [[ -z "$LOG" ]]; then
    echo "(no journal entries at all in this window)"
    exit 0
fi

# Narrow to the futures children. Their log lines are all prefixed with the
# INDEX symbol (see FuturesUnderlyingIngester's "%s futures ..." format), and
# the supervisor names the worker ingest-futures-<INDEX>.
if [[ -n "$SYM_RE" ]]; then
    FUT=$(printf '%s\n' "$LOG" | grep -E "(^|[^A-Z])${SYM_RE} futures|ingest-futures-${SYM_RE}" || true)
else
    FUT=$(printf '%s\n' "$LOG" | grep -E "futures|ingest-futures-" || true)
fi

if (( RAW )); then
    echo "--- all futures lines in window ---"
    printf '%s\n' "${FUT:-(none)}"
    exit 0
fi

section() {
    local title="$1" pattern="$2" hit
    hit=$(printf '%s\n' "$FUT" | grep -E "$pattern" || true)
    echo "--- ${title} ---"
    if [[ -n "$hit" ]]; then
        printf '%s\n' "$hit"
    else
        echo "(none)"
    fi
    echo ""
}

# Ordered by what actually explains a delay. Patterns are the ingester's own
# format strings (src/ingestion/futures_underlying_ingester.py) and the
# supervisor's (src/ingestion/main_engine.py) — grep them there before
# editing, because a reworded log line silently empties a section.
section "Stream connects / reconnects (each one replays barsback and heals a gap)" \
        "futures stream: connected"
section "Disconnects and backoffs" \
        "futures (stream disconnected|stream ended without an error).*reconnecting in"
section "Auth — 401s and token refreshes (a lapsed CME entitlement lands here)" \
        "futures stream: 401 auth failure|futures stream reported auth error"
section "Seeding (a re-seed means the child restarted or the window reopened)" \
        "futures cache seeded with"
section "Worker deaths and respawns" \
        "Ingestion worker ingest-futures|past the restart budget|Could not respawn ingestion worker"
section "Shutdown signals" \
        "futures ingester received signal|futures ingester stopped"
section "Write and prune failures" \
        "futures bar upsert failed|futures bar prune failed"
section "Malformed payloads" \
        "futures stream: JSON decode failed"
section "Fatal" \
        "Fatal error in .* futures ingester"

echo "--- counts ---"
printf '%s\n' "$FUT" | grep -cE "futures stream: connected"        | sed 's/^/  stream connects:  /'
printf '%s\n' "$FUT" | grep -cE "reconnecting in"                  | sed 's/^/  backoffs:         /'
printf '%s\n' "$FUT" | grep -cE " - ERROR - "                      | sed 's/^/  ERROR lines:      /'
printf '%s\n' "$FUT" | grep -cE " - WARNING - "                    | sed 's/^/  WARNING lines:    /'
echo ""
echo "A healthy window has ONE connect (or none, if the stream was already up)"
echo "and no backoffs. Repeated connects are the delay: every gap between one"
echo "disconnect and the next connect is time the chart was not advancing."
