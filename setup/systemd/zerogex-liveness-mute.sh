#!/usr/bin/env bash
# zerogex-liveness-mute — silence the liveness watchdog for a service the
# operator is intentionally stopping or restarting, so PLANNED downtime does
# not page.
#
# Usage: zerogex-liveness-mute.sh <unit> [seconds]
#
# Writes $LIVENESS_STATE_DIR/<unit>.mute containing an expiry epoch
# (now + seconds, default 900 = 15 min). The watchdog stays quiet for <unit>
# until that time — and clears the mute the moment the unit is back up, so a
# restart resumes monitoring immediately — while a unit left down PAST the
# window still alerts (a forgotten stop is exactly what we want to catch).
#
# Dependency-free (bash + coreutils date) and best-effort: it must never block
# the stop it precedes, so callers invoke it as `… || true`.
set -uo pipefail

unit="${1:?usage: zerogex-liveness-mute.sh <unit> [seconds]}"
ttl="${2:-${LIVENESS_MUTE_TTL:-900}}"
# Fall back to the default for an empty or non-numeric window.
case "$ttl" in '' | *[!0-9]*) ttl="${LIVENESS_MUTE_TTL:-900}" ;; esac

state_dir="${LIVENESS_STATE_DIR:-${HOME}/monitoring/liveness}"
mkdir -p "$state_dir" 2>/dev/null || true

expiry="$(( $(date +%s) + ttl ))"
printf '%s\n' "$expiry" > "${state_dir}/${unit}.mute"
echo "liveness: muted ${unit} for ${ttl}s (until $(date -d "@${expiry}" '+%H:%M:%S' 2>/dev/null || echo "+${ttl}s"))" >&2
