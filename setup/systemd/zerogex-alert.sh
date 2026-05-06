#!/usr/bin/env bash
# zerogex-alert — dispatch a unit-failure notification to a configured backend.
#
# Invoked by the systemd template unit at setup/systemd/zerogex-alert@.service
# whenever a peer unit fires OnFailure=zerogex-alert@%n.service.  The peer's
# unit name (e.g. ``zerogex-oa-normalizer-healthcheck.service``) is passed as
# the first positional argument and used to pull recent journal context.
#
# Configuration lives in /etc/zerogex/alert.env — see alert.env.example for
# the available backends (slack | sns | pagerduty | webhook | stderr).  The
# default backend is ``stderr`` so a fresh install logs visibly without
# depending on infra the operator may not have wired up yet.
#
# Dependencies: bash, curl, journalctl.  Slack / generic-webhook / pagerduty
# additionally need jq.  SNS additionally needs the AWS CLI configured for
# the host's IAM role.
#
# Exit codes:
#   0 — alert sent (or stderr fallback printed)
#   1 — required env var missing for the selected backend
#   2 — backend command failed (curl/aws non-zero)

set -euo pipefail

unit_name="${1:-unknown}"

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

human_summary="🚨 ZeroGEX systemd unit failed: ${unit_name} on ${host} at ${ts}"
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
            --arg summary "ZeroGEX unit failure: ${unit_name} on ${host}" \
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
            --arg host "$host" \
            --arg ts "$ts" \
            --arg ctx "$context" \
            '{unit: $unit, host: $host, timestamp: $ts, journal_tail: $ctx}')"
        curl --fail --silent --show-error \
            -X POST -H 'Content-Type: application/json' \
            -d "$payload" \
            "$WEBHOOK_URL" >/dev/null \
            || { echo "zerogex-alert: webhook POST failed" >&2; exit 2; }
        ;;

    *)
        echo "zerogex-alert: unknown backend '${backend}' (set ALERT_BACKEND in /etc/zerogex/alert.env to one of: stderr, slack, sns, pagerduty, webhook)" >&2
        exit 1
        ;;
esac
