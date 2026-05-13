#!/bin/bash

# ==============================================
# ZeroGEX-OA Platform Deployment Script
# ==============================================

set -e  # Exit on any error

# Refuse to run as root.  Several files (.env, ~/.pgpass, ~/.bashrc,
# the venv) are created during deploy with the running user's ownership;
# if that user is root, systemd's User=ubuntu services later fail to
# read them with no obvious error.  Steps that need privilege call
# `sudo` themselves.
if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    echo "✗ Do not run deploy.sh as root or with sudo."
    echo "  Run as the 'ubuntu' user; individual steps will sudo when needed."
    exit 1
fi

# Export HOME
[ -z "$HOME" ] && export HOME="/home/ubuntu"

# Variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$APP_DIR/.env"
STEPS_DIR="$SCRIPT_DIR/steps"
LOG_DIR="${HOME}/logs"
LOG_FILE="${LOG_DIR}/deployment_$(date +%Y%m%d_%H%M%S).log"

# Help text
show_help() {
    cat << EOF
ZeroGEX-OA Platform Deployment Script

Usage: ./deploy.sh [OPTIONS]

Options:
  --start-from STEP    Start deployment from a specific step
                       STEP can be a step number (e.g., 030) or name (e.g., database)
  -h, --help          Show this help message

Examples:
  ./deploy.sh                        # Run full deployment (all steps)
  ./deploy.sh --start-from 030       # Start from step 030
  ./deploy.sh --start-from database  # Start from database step
  ./deploy.sh --start-from security  # Start from security step

Available Steps:
EOF

    for step in $(ls -1 $STEPS_DIR/*.* 2>/dev/null); do
        desc=$(grep "# Step" "$step" | head -1 | awk -F: '{print $2}')
        printf "%s\t%s\n" "$(basename $step)" "- $desc"
    done

    echo
    echo "Logs are saved to: ${LOG_DIR}/oa_deployment_YYYYMMDD_HHMMSS.log"
    echo

    exit 0
}

# Parse command line arguments
START_FROM=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --start-from)
            START_FROM="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Create logs directory if it doesn't exist
[ ! -d "$LOG_DIR" ] && mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Export log function for sub-steps
export -f log
export LOG_FILE

log "=========================================="
log "🚀 Deploying ZeroGEX-OA Platform..."
if [ -n "$START_FROM" ]; then
    log "Starting from step: $START_FROM"
fi
log "=========================================="

# Source .env so every step inherits configuration without prompting.
# `set -a` auto-exports every variable defined while sourcing.
if [ ! -f "$ENV_FILE" ]; then
    log "✗ .env not found at $ENV_FILE"
    log ""
    log "  Copy the template and fill in your values before re-running:"
    log "    cp $APP_DIR/.env.example $ENV_FILE"
    log "    \$EDITOR $ENV_FILE"
    log ""
    exit 1
fi
log "Sourcing configuration from $ENV_FILE"
# Syntactic check first — a malformed line otherwise aborts deploy.sh
# under `set -e` with no clear log line written.
if ! bash -n "$ENV_FILE" 2>/tmp/deploy-env-syntax.log; then
    log "✗ .env has shell syntax errors:"
    while IFS= read -r line; do log "    $line"; done < /tmp/deploy-env-syntax.log
    log ""
    log "    Fix the file and re-run."
    exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
export APP_DIR ENV_FILE

# Always run 005.preflight, regardless of --start-from. It only validates
# .env (require_var on OA_API_DOMAIN, LETSENCRYPT_EMAIL, DB_HOST, etc.)
# and makes no system changes, so re-running it is free. Doing it here —
# before the start-from logic — means a `make deploy-from STEP=125`
# invocation gets the same config-correctness guarantees as a full
# deploy. Without this, a missing var surfaces several steps later as
# a cryptic error (e.g. an empty OA_API_DOMAIN crashing 130.ssl's DNS
# preflight with `dig: '' is not a legal name`).
PREFLIGHT_SCRIPT="$STEPS_DIR/005.preflight"
if [ -x "$PREFLIGHT_SCRIPT" ]; then
    log "=========================================="
    log "Executing: 005.preflight ..."
    if bash "$PREFLIGHT_SCRIPT"; then
        log "✓ 005.preflight completed successfully"
    else
        log "✗ 005.preflight failed — fix .env and re-run."
        exit 1
    fi
    log ""
fi

# Flag to track if we should start executing
SHOULD_EXECUTE=false
if [ -z "$START_FROM" ]; then
    SHOULD_EXECUTE=true
fi

# Execute each step in order
for step_script in "$STEPS_DIR"/*.* ; do
    if [ -x "$step_script" ]; then
        step_name=$(basename "$step_script")

        # Check if this is the start-from step
        if [ -n "$START_FROM" ] && [[ "$step_name" == *"$START_FROM"* ]]; then
            SHOULD_EXECUTE=true
            log "Found start step: $step_name"
        fi

        # 005.preflight was already run unconditionally above; skip it
        # here so a full deploy doesn't validate twice. The start-from
        # check above still fires for --start-from 005 so subsequent
        # steps run normally.
        if [[ "$step_name" == 005.* ]]; then
            continue
        fi

        # Skip steps before the start-from step
        if [ "$SHOULD_EXECUTE" = false ]; then
            log "Skipping: $step_name"
            continue
        fi

        log "=========================================="
        log "Executing: $step_name ..."

        if bash "$step_script"; then
            log "✓ $step_name completed successfully"
        else
            log "✗ $step_name failed"
            exit 1
        fi
        log ""
    fi
done

log ""
log "=========================================="
log "✅ Deployment Complete!"
log "=========================================="
log "Log file: $LOG_FILE"
