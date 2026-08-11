#!/usr/bin/env bash
# zerogex-liveness-watch — actively detect a critical service that is DOWN and
# alert, covering the gap that OnFailure= cannot.
#
# OnFailure=zerogex-alert@%n only fires when systemd marks a unit *failed*. A
# clean `systemctl stop` (e.g. a deploy that stops a service and errors before
# it starts again), a unit simply left `inactive`, or a process that is
# `active` but hung and not serving all leave OnFailure silent. That is exactly
# how the API went down — 503s to users, `make services-health` red — with no
# failure in the journal and no alert, until a human happened to notice. This
# watchdog closes that hole.
#
# Run once a minute by zerogex-oa-liveness-watch.timer. For each critical unit
# it checks `systemctl is-active`; for the API unit it *also* probes the
# DB-free liveness endpoint, so a hung-but-active worker is caught too. It
# alerts on the transition INTO a bad state and again on RECOVERY, deduping via
# a per-unit state file so a sustained outage pages once, not every minute.
#
# Dispatch reuses setup/systemd/zerogex-alert.sh (same backends + the same
# /etc/zerogex/alert.env config as the OnFailure path); with no alert.env it
# falls back to stderr, which lands in this unit's own journal.
#
# Dependencies: bash, systemctl, curl. No Python/venv — so it still runs when
# the application environment itself is the thing that is broken.
#
# Exit status is 0 on any normal run, INCLUDING when it finds and alerts on a
# down service: the alert is the signal, and a non-zero exit would (a) mark
# this watchdog unit itself failed and (b) misrepresent "watchdog worked, found
# a problem" as "watchdog broke". Non-zero is reserved for the watchdog's own
# internal errors.

set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Config (env-overridable; defaults match the production box) -----------
SERVICES="${LIVENESS_SERVICES:-zerogex-oa-ingestion zerogex-oa-analytics zerogex-oa-signals zerogex-oa-api}"
API_UNIT="${LIVENESS_API_UNIT:-zerogex-oa-api}"
API_URL="${LIVENESS_API_URL:-http://127.0.0.1:8000/api/health/live}"
STATE_DIR="${LIVENESS_STATE_DIR:-${HOME}/monitoring/liveness}"
ALERT_SCRIPT="${ALERT_SCRIPT:-${SELF_DIR}/zerogex-alert.sh}"
CURL_MAX_TIME="${LIVENESS_CURL_MAX_TIME:-5}"

mkdir -p "$STATE_DIR" 2>/dev/null || true

# --- Helpers ---------------------------------------------------------------
dispatch() {
    # dispatch <unit> <reason>
    local unit="$1" reason="$2"
    if [ -x "$ALERT_SCRIPT" ]; then
        "$ALERT_SCRIPT" "$unit" "$reason" \
            || echo "zerogex-liveness-watch: alert dispatch failed for $unit ($reason)" >&2
    else
        echo "zerogex-liveness-watch: $reason: $unit (no alert script at $ALERT_SCRIPT)" >&2
    fi
}

# Print the current verdict for a unit: "up", or "down:<reason>".
verdict() {
    local svc="$1"
    if ! systemctl is-active --quiet "$svc"; then
        local st
        st="$(systemctl is-active "$svc" 2>/dev/null || true)"
        printf 'down:not active (%s)' "${st:-unknown}"
        return
    fi
    # Active — for the API also confirm it is actually serving, so a worker
    # that is technically active but hung (not answering) is still caught.
    if [ "$svc" = "$API_UNIT" ]; then
        if ! curl -fsS --max-time "$CURL_MAX_TIME" "$API_URL" >/dev/null 2>&1; then
            printf 'down:active but not serving %s' "$API_URL"
            return
        fi
    fi
    printf 'up'
}

# --- Main ------------------------------------------------------------------
for svc in $SERVICES; do
    now="$(verdict "$svc")"
    state_file="${STATE_DIR}/${svc}.state"
    prev="up"
    [ -f "$state_file" ] && prev="$(cat "$state_file" 2>/dev/null || echo up)"

    case "$now" in
        up)
            # Only announce a recovery if we previously saw it down, so a
            # steady-state healthy box stays silent.
            [ "$prev" != "up" ] && dispatch "$svc" "recovered — now active"
            ;;
        down:*)
            # Alert only on the DOWN edge (prev was up); a still-down service
            # was already paged and must not re-page every minute.
            [ "$prev" = "up" ] && dispatch "$svc" "${now#down:}"
            ;;
    esac

    printf '%s\n' "$now" >"$state_file" 2>/dev/null || true
done

exit 0
