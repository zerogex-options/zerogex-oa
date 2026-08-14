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
