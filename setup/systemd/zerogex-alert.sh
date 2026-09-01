#!/usr/bin/env bash
# zerogex-alert — dispatch a unit-failure notification to a configured backend.
#
# Invoked by the systemd template unit at setup/systemd/zerogex-alert@.service
# whenever a peer unit fires OnFailure=zerogex-alert@%n.service.  The peer's
# unit name (e.g. ``zerogex-oa-normalizer-healthcheck.service``) is passed as
# the first positional argument and used to pull recent journal context.
#
# Configuration lives in /etc/zerogex/alert.env — see alert.env.example for
# the available backends (slack | sns | pagerduty | webhook | resend | stderr).
# The default backend is ``stderr`` so a fresh install logs visibly without
# depending on infra the operator may not have wired up yet.
#
# Dependencies: bash, curl, journalctl.  Slack / generic-webhook / pagerduty /
# resend additionally need jq.  SNS additionally needs the AWS CLI configured
# for the host's IAM role.  ``resend`` sends email via https://resend.com and
# reads RESEND_API_KEY / RESEND_FROM_EMAIL from alert.env or, failing that,
# from the app .env (ALERT_APP_ENV) so the key need not be duplicated.
#
# Exit codes:
#   0 — alert sent (or stderr fallback printed)
#   1 — required env var missing for the selected backend
#   2 — backend command failed (curl/aws non-zero)

set -euo pipefail

unit_name="${1:-unknown}"
# Optional human reason for the alert. Defaults to the OnFailure= meaning so
# the single-arg invocation from zerogex-alert@.service is unchanged; the
# liveness watchdog passes a precise reason ("not active (inactive)",
# "active but not serving …", "recovered — now active").
reason="${2:-systemd unit failed}"

# Optional config — keep failures soft so the template install works on a
# fresh box where the operator hasn't deployed alert.env yet.
if [ -f /etc/zerogex/alert.env ]; then
    # shellcheck disable=SC1091
    set -a
    . /etc/zerogex/alert.env
    set +a
fi

backend="${ALERT_BACKEND:-stderr}"
host="$(hostname -f 2>/dev/null || hostname)"
ts="$(date --iso-8601=seconds)"

# Pull a journal tail without exploding if the user lacks permissions or the
# unit name is bogus — empty context is better than aborting the alert.
context="$(journalctl -u "$unit_name" -n 20 --no-pager 2>/dev/null || true)"
if [ -z "$context" ]; then
    context="(no journal lines for ${unit_name})"
fi

human_summary="🚨 ZeroGEX: ${reason} — ${unit_name} on ${host} at ${ts}"
human_message=$(printf '%s\n\nLast 20 journal lines:\n%s' "$human_summary" "$context")

# ---------------------------------------------------------------------------
# Cooldown
#
# A failing oneshot re-fires on its own timer -- the freshness check every ten
# minutes -- and every firing dispatched. One stale symbol therefore sent ~12
# emails a day (measured 2026-08-31). Only the pagerduty backend deduplicated,
# through its dedup_key; every other backend resent blind, which is how a known
# outage becomes an inbox the operator learns to filter.
#
# Suppress a repeat of the SAME failure inside ALERT_MIN_INTERVAL_SEC. "Same"
# is a signature over the failure-bearing journal lines with numbers flattened,
# so "QQQ chains stale for 18 min" and the same line ten minutes later at 28
# min collapse into one incident -- while a SECOND symbol going dark changes
# the signature and pages immediately. That distinction is the entire point: a
# plain per-unit timer would swallow the new outage as a duplicate, which is
# the one failure mode a cooldown must not introduce.
#
# Recovery notices never wait, and clear the state so the next failure pages at
# once.
#
# Fails OPEN. Any trouble reading or writing state -- missing directory, no
# permission, a corrupt line -- sends the alert. A cooldown that loses an alert
# is worse than one that repeats itself.
# ---------------------------------------------------------------------------
cooldown_secs="${ALERT_MIN_INTERVAL_SEC:-3600}"
case "$cooldown_secs" in ''|*[!0-9]*) cooldown_secs=3600 ;; esac
state_dir="${ALERT_STATE_DIR:-/var/lib/zerogex-alert}"
state_file="${state_dir}/$(printf '%s' "$unit_name" | tr -c '[:alnum:]._@-' '_').state"

# Only the failure-bearing lines, numbers flattened, order-independent. An INFO
# line flipping from "fresh" to "outside its delivery window" as a session
# closes must not register as a new failure and restart the paging.
signature="$(printf '%s\n' "$context" \
    | grep -aiE 'error|critical|fatal|traceback|stale|fail' \
    | sed -E 's/[0-9]+/N/g' \
    | sort -u \
    | sha256sum 2>/dev/null | cut -c1-16 || true)"
[ -z "$signature" ] && signature="nosig"

now_epoch="$(date +%s)"
is_recovery=no
case "$reason" in *recover*) is_recovery=yes ;; esac

if [ "$is_recovery" = "yes" ]; then
    rm -f "$state_file" 2>/dev/null || true
elif [ "$cooldown_secs" -gt 0 ]; then
    prev_epoch=0; prev_sig=""; prev_count=0
    if [ -r "$state_file" ]; then
        read -r prev_epoch prev_sig prev_count _ < "$state_file" 2>/dev/null || true
    fi
    case "${prev_epoch:-}" in ''|*[!0-9]*) prev_epoch=0 ;; esac
    case "${prev_count:-}" in ''|*[!0-9]*) prev_count=0 ;; esac
    age=$(( now_epoch - prev_epoch ))
    if [ "${prev_sig:-}" = "$signature" ] && [ "$age" -lt "$cooldown_secs" ]; then
        held=$(( prev_count + 1 ))
        mkdir -p "$state_dir" 2>/dev/null || true
        printf '%s %s %s\n' "$prev_epoch" "$signature" "$held" \
            > "$state_file" 2>/dev/null || true
        # Logged, never silent: a suppressed alert must still be discoverable
        # in `journalctl -t zerogex-alert` or the cooldown becomes a blindfold.
        echo "zerogex-alert: suppressed — same failure ${age}s into a ${cooldown_secs}s cooldown for ${unit_name} (${held} held)" >&2
        exit 0
    fi
    if [ "${prev_sig:-}" = "$signature" ] && [ "$prev_count" -gt 0 ]; then
        human_message="$(printf '%s\n\n%s identical alert(s) were suppressed during the %ss cooldown.' \
            "$human_message" "$prev_count" "$cooldown_secs")"
    fi
fi

require() {
    local var_name="$1"
    if [ -z "${!var_name:-}" ]; then
        echo "zerogex-alert: backend=${backend} requires ${var_name} in /etc/zerogex/alert.env" >&2
        exit 1
    fi
}

# Build a JSON-safe version of an arbitrary string using jq.  Caller supplies
# the value via stdin so we don't have to escape shell-quote it ourselves.
jq_string() {
    jq -Rs .
}

case "$backend" in
    stderr)
        # Default — write to systemd journal via stderr.  Useful as a
        # zero-config sanity check (`make alert-template-test`) and as a
        # fallback while the operator is still wiring real alerting.
        printf '%s\n' "$human_message" >&2
        ;;

    slack)
        require SLACK_WEBHOOK_URL
        payload="$(printf '%s' "$human_message" | jq -Rs '{text: .}')"
        curl --fail --silent --show-error \
            -X POST -H 'Content-Type: application/json' \
            -d "$payload" \
            "$SLACK_WEBHOOK_URL" >/dev/null \
            || { echo "zerogex-alert: slack POST failed" >&2; exit 2; }
        ;;

    sns)
        require SNS_TOPIC_ARN
        aws sns publish \
            --topic-arn "$SNS_TOPIC_ARN" \
            --subject "ZeroGEX unit failure: ${unit_name}" \
            --message "$human_message" >/dev/null \
            || { echo "zerogex-alert: aws sns publish failed" >&2; exit 2; }
        ;;

    pagerduty)
        require PAGERDUTY_ROUTING_KEY
        payload="$(jq -n \
            --arg routing_key "$PAGERDUTY_ROUTING_KEY" \
            --arg summary "ZeroGEX: ${reason} — ${unit_name} on ${host}" \
            --arg source "$host" \
            --arg dedup "${unit_name}@${host}" \
            --arg ctx "$context" \
            '{
                routing_key: $routing_key,
                event_action: "trigger",
                dedup_key: $dedup,
                payload: {
                    summary: $summary,
                    source: $source,
                    severity: "error",
                    custom_details: {journal_tail: $ctx}
                }
            }')"
        curl --fail --silent --show-error \
            -X POST -H 'Content-Type: application/json' \
            -d "$payload" \
            https://events.pagerduty.com/v2/enqueue >/dev/null \
            || { echo "zerogex-alert: pagerduty enqueue failed" >&2; exit 2; }
        ;;

    webhook)
        # Generic POST of structured JSON for any custom receiver.
        require WEBHOOK_URL
        payload="$(jq -n \
            --arg unit "$unit_name" \
            --arg reason "$reason" \
            --arg host "$host" \
            --arg ts "$ts" \
            --arg ctx "$context" \
            '{unit: $unit, reason: $reason, host: $host, timestamp: $ts, journal_tail: $ctx}')"
        curl --fail --silent --show-error \
            -X POST -H 'Content-Type: application/json' \
            -d "$payload" \
            "$WEBHOOK_URL" >/dev/null \
            || { echo "zerogex-alert: webhook POST failed" >&2; exit 2; }
        ;;

    resend)
        # Email via Resend (https://resend.com). The API key + From address are
        # taken from the environment (alert.env) when set, otherwise pulled
        # straight from the app .env (ALERT_APP_ENV) so the secret already
        # configured for the app is not duplicated into a second file.
        # Recipient: ALERT_EMAIL_TO if set, else the address in RESEND_FROM_EMAIL.
        app_env="${ALERT_APP_ENV:-/home/ubuntu/zerogex-oa/.env}"
        # Parse ONLY the two RESEND_* lines (never source the whole .env), and
        # only for values not already supplied via alert.env. Guarded with
        # `set +e` so a missing key/file cannot abort the dispatcher.
        set +e
        if [ -z "${RESEND_API_KEY:-}" ] && [ -r "$app_env" ]; then
            RESEND_API_KEY="$(grep -E '^RESEND_API_KEY=' "$app_env" | head -1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//')"
        fi
        if [ -z "${RESEND_FROM_EMAIL:-}" ] && [ -r "$app_env" ]; then
            RESEND_FROM_EMAIL="$(grep -E '^RESEND_FROM_EMAIL=' "$app_env" | head -1 | cut -d= -f2- | sed -e 's/^"//' -e 's/"$//')"
        fi
        set -e
        if [ -z "${ALERT_EMAIL_TO:-}" ] && [ -n "${RESEND_FROM_EMAIL:-}" ]; then
            # Default the recipient to the From address: "Name <addr>" -> addr,
            # or the whole value if it carries no angle-bracketed address.
            ALERT_EMAIL_TO="$(printf '%s' "$RESEND_FROM_EMAIL" | sed -n 's/.*<\(.*\)>.*/\1/p')"
            [ -z "$ALERT_EMAIL_TO" ] && ALERT_EMAIL_TO="$RESEND_FROM_EMAIL"
        fi
        require RESEND_API_KEY
        require RESEND_FROM_EMAIL
        require ALERT_EMAIL_TO
        # jq builds the JSON (and escapes the body); ALERT_EMAIL_TO may be a
        # comma-separated list -> a JSON array of trimmed, non-empty addresses.
        payload="$(jq -n \
            --arg from "$RESEND_FROM_EMAIL" \
            --arg to "$ALERT_EMAIL_TO" \
            --arg subject "$human_summary" \
            --arg text "$human_message" \
            '{from: $from,
              to: ($to | split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0))),
              subject: $subject,
              text: $text}')"
        # The API key travels only in the Authorization header (never printed).
        curl --fail --silent --show-error \
            -X POST \
            -H "Authorization: Bearer $RESEND_API_KEY" \
            -H 'Content-Type: application/json' \
            -d "$payload" \
            https://api.resend.com/emails >/dev/null \
            || { echo "zerogex-alert: resend email send failed" >&2; exit 2; }
        ;;

    *)
        echo "zerogex-alert: unknown backend '${backend}' (set ALERT_BACKEND in /etc/zerogex/alert.env to one of: stderr, slack, sns, pagerduty, webhook, resend)" >&2
        exit 1
        ;;
esac

# Reached only on a successful dispatch -- every backend exits non-zero above
# on failure. Recording the send here, rather than before it, is what keeps a
# failed send from opening a cooldown that swallows the retry.
if [ "$is_recovery" = "no" ] && [ "$cooldown_secs" -gt 0 ]; then
    mkdir -p "$state_dir" 2>/dev/null || true
    printf '%s %s 0\n' "$now_epoch" "$signature" > "$state_file" 2>/dev/null || true
fi
