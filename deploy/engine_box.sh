#!/usr/bin/env bash
# =============================================================================
# deploy/engine_box.sh: run the engines on exactly one box, and move them
# between boxes without ever running them on two.
#
# The engines are the always-on units that read the market feed and turn it
# into numbers: the ThetaData terminal, ingestion, analytics, signals, wall
# alerts and the backtest worker. Everything customers touch (the website and
# the API) and every scheduled job stay on the web box.
#
#   status              read-only: what this box runs, and anything unsafe
#   prepare  --confirm  on a NEW engine box: mark it, install the base system
#                       and the engine units WITHOUT starting them
#   release  --confirm  on the box giving the engines up: switch them off here
#   takeover --confirm  on the box taking them: switch them on here
#
# Without --confirm, prepare / release / takeover print what they would do and
# change nothing. The make targets wrap this script:
#   make engine-status
#   make engine-prepare  CONFIRM=yes
#   make engine-release  CONFIRM=yes
#   make engine-takeover CONFIRM=yes
#
# Runbook: docs/runbooks/engine_box_split.md
#
# Two marker files in /etc/zerogex say what a box runs:
#   engines.disabled  the engines must not run here (they run on another box)
#   engine-box        this is the engine box: no API and no scheduled jobs
# With neither (one box, as before the split) nothing anywhere changes.
#
# Every engine unit gets a drop-in, <unit>.service.d/50-engine-box.conf, that
# refuses to start while engines.disabled exists and names the unit's database
# sessions (PGAPPNAME). ThetaData keeps ONE session per account, so engines on
# two boxes knock each other off the feed, and the drop-in is what stops a
# habitual `make ingestion-restart` on the wrong box from doing that. The API
# gets one that refuses to start on the engine box, where the TradeWorkz
# scheduler inside it would run a second time.
#
# The handoff is also written to the database (engine_box_handoffs): release
# appends 'released', and takeover refuses unless the newest row is a
# 'released' (or this box already holds the engines). Taking the engines over
# without releasing them first is refused, not discovered at the open.
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNBOOK="docs/runbooks/engine_box_split.md"

# Overridable for tests only. Production uses the defaults.
ENV_FILE="${ZGX_ENV_FILE:-$APP_DIR/.env}"
SYSTEMD_SOURCE="${ZGX_SYSTEMD_SOURCE:-$APP_DIR/setup/systemd}"
SYSTEMD_TARGET="${ZGX_SYSTEMD_TARGET:-/etc/systemd/system}"
JOURNALD_DROPIN_DIR="${ZGX_JOURNALD_DROPIN_DIR:-/etc/systemd/journald.conf.d}"
MARKER_DIR="${ZGX_MARKER_DIR:-/etc/zerogex}"
THETA_DIR="${ZGX_THETA_DIR:-$HOME/thetaterminal}"
MONITORING_DIR="${ZGX_MONITORING_DIR:-$HOME/monitoring}"
SUDO="${ZGX_SUDO-sudo}"
SYSTEMCTL="${ZGX_SYSTEMCTL:-systemctl}"
DEPLOY_SCRIPT="${ZGX_DEPLOY_SCRIPT:-$APP_DIR/deploy/deploy.sh}"
SETTLE_SECONDS="${ZGX_SETTLE_SECONDS:-5}"
THETA_SETTLE_SECONDS="${ZGX_THETA_SETTLE_SECONDS:-20}"
BOX_NAME="${ZGX_HOSTNAME:-$(hostname 2>/dev/null || echo unknown)}"
BOX_ADDRS="${ZGX_ADDRS-$(hostname -I 2>/dev/null || true)}"

ENGINES_DISABLED="$MARKER_DIR/engines.disabled"
ENGINE_BOX="$MARKER_DIR/engine-box"
DROPIN_NAME="50-engine-box.conf"

# Start order follows the data: the terminal first, since ingestion reads it.
ENGINE_UNITS=(
    zerogex-oa-thetaterminal
    zerogex-oa-ingestion
    zerogex-oa-analytics
    zerogex-oa-signals
    zerogex-oa-wall-alerts
    zerogex-oa-backtest-worker
)
# Without these four the product has no numbers, so a box cannot take the
# engines over without them installed.
REQUIRED_UNITS=(zerogex-oa-thetaterminal zerogex-oa-ingestion zerogex-oa-analytics zerogex-oa-signals)
API_UNIT=zerogex-oa-api
# What the engine box runs for itself. These are not the scheduled jobs (those
# stay on the web box): a per-minute resource sampler, the per-minute liveness
# watchdog, and the nightly log and cache cleanup.
ENGINE_BOX_TIMERS=(zerogex-oa-system-monitor zerogex-oa-liveness-watch zerogex-oa-logs-clear)

if [ -t 1 ]; then
    GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'; RED=$'\033[0;31m'; BLUE=$'\033[0;34m'; NC=$'\033[0m'
else
    GREEN=""; YELLOW=""; RED=""; BLUE=""; NC=""
fi

say()  { printf '%s\n' "$*"; }
ok()   { printf '%s✔ %s%s\n' "$GREEN" "$*" "$NC"; }
warn() { printf '%s⚠ %s%s\n' "$YELLOW" "$*" "$NC"; }
bad()  { printf '%s✖ %s%s\n' "$RED" "$*" "$NC"; }
head_line() { printf '\n%s=== %s ===%s\n' "$BLUE" "$*" "$NC"; }
die()  { bad "$*"; exit 1; }

usage() {
    sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
}

# --- .env ---------------------------------------------------------------------

# Value of KEY in .env, read the way systemd's EnvironmentFile= and deploy.sh
# read it: the last assignment wins, everything after the first '=' is
# literal, and one pair of surrounding quotes is stripped.
env_get() {
    local key="$1" line val=""
    [ -f "$ENV_FILE" ] || return 0
    while IFS= read -r line || [ -n "$line" ]; do
        line="${line#export }"
        case "$line" in
            "$key="*) val="${line#*=}" ;;
        esac
    done < "$ENV_FILE"
    case "$val" in
        \"*\") val="${val#\"}"; val="${val%\"}" ;;
        \'*\') val="${val#\'}"; val="${val%\'}" ;;
    esac
    printf '%s' "$val"
}

# Every line that sets a symbol list. These must read the same on both boxes:
# the engine box ingests and models from them, and the web box's API and
# scheduled jobs read the same lists.
symbol_lines() {
    [ -f "$ENV_FILE" ] || return 0
    grep -E '^(export )?[A-Z0-9_]*(UNDERLYING|UNDERLYINGS|SYMBOLS|BY_SYMBOL|UNIVERSE)[A-Z0-9_]*=' "$ENV_FILE" \
        | sed 's/^export //' | LC_ALL=C sort -u
}

# --- database -------------------------------------------------------------------

# psql_q SQL: rows on stdout, unaligned and tuples-only. Non-zero on any error.
psql_q() {
    if [ -n "${ZGX_PSQL:-}" ]; then
        "$ZGX_PSQL" "$1"
        return
    fi
    local host port name user
    host="$(env_get DB_HOST)"
    port="$(env_get DB_PORT)"
    name="$(env_get DB_NAME)"
    user="$(env_get DB_USER)"
    [ -n "$host" ] || return 1
    command -v psql >/dev/null 2>&1 || return 1
    PGPASSFILE="$HOME/.pgpass" PGAPPNAME="engine-box-script" PGCONNECT_TIMEOUT=10 psql -X -q -tA \
        -v ON_ERROR_STOP=1 \
        "sslmode=require host=$host port=${port:-5432} user=${user:-postgres} dbname=${name:-zerogex}" \
        -c "$1" 2>/dev/null
}

# Only letters, digits and . _ : , - survive into SQL literals.
sql_safe() { printf '%s' "$1" | tr -cd 'A-Za-z0-9._:,-'; }

handoff_table_exists() {
    [ "$(psql_q "SELECT to_regclass('public.engine_box_handoffs') IS NOT NULL" | tr -d '[:space:]')" = "t" ]
}

# The newest handoff row as action|host|address|units|when, or nothing.
newest_handoff() {
    psql_q "SELECT action || '|' || host || '|' || coalesce(address, '') || '|' || coalesce(units, '')
                   || '|' || to_char(at AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') || ' ET'
            FROM engine_box_handoffs ORDER BY id DESC LIMIT 1"
}

record_handoff() {  # record_handoff ACTION UNITS(comma list)
    local action="$1" units="$2"
    psql_q "INSERT INTO engine_box_handoffs (host, address, action, units)
            VALUES ('$(sql_safe "$BOX_NAME")', '$(sql_safe "$(first_addr)")', '$action', '$(sql_safe "$units")')" \
        >/dev/null
}

# shellcheck disable=SC2086  # split the address list on purpose
first_addr() { set -- $BOX_ADDRS; printf '%s' "${1:-}"; }

is_my_addr() {
    local a
    for a in $BOX_ADDRS; do
        [ "$a" = "$1" ] && return 0
    done
    return 1
}

# Whether a handoff row (host, address) is this box. The private address is
# the tie-breaker, since it outlives a hostname change.
is_this_box() { [ "$1" = "$BOX_NAME" ] || { [ -n "$2" ] && is_my_addr "$2"; }; }

# Engine sessions in the database as app|address|count, from PGAPPNAME.
# Engines started before their drop-in existed connect unnamed and do not show.
engine_sessions() {
    local names="" u
    for u in "${ENGINE_UNITS[@]}"; do names="$names,'$u'"; done
    psql_q "SELECT application_name || '|' || coalesce(host(client_addr), 'local') || '|' || count(*)
            FROM pg_stat_activity
            WHERE datname = current_database() AND application_name IN (${names#,})
            GROUP BY application_name, client_addr ORDER BY 1"
}

# --- units ----------------------------------------------------------------------

installed()  { [ -f "$SYSTEMD_TARGET/$1.service" ]; }
enabled()    { "$SYSTEMCTL" is-enabled --quiet "$1" 2>/dev/null; }
active()     { "$SYSTEMCTL" is-active --quiet "$1" 2>/dev/null; }
has_dropin() { [ -f "$SYSTEMD_TARGET/$1.service.d/$DROPIN_NAME" ]; }

any_engine_active() {
    local u
    for u in "${ENGINE_UNITS[@]}"; do
        active "$u" && return 0
    done
    return 1
}

# Engine units that run on this box: enabled to start at boot, or running now.
engines_running_here() {
    local u out=()
    for u in "${ENGINE_UNITS[@]}"; do
        if installed "$u" && { enabled "$u" || active "$u"; }; then
            out+=("$u")
        fi
    done
    local IFS=,
    printf '%s' "${out[*]}"
}

join_comma() { local IFS=,; printf '%s' "$*"; }

write_root_file() {  # write_root_file PATH MODE  (content on stdin)
    local path="$1" mode="$2" tmp
    tmp="$(mktemp)"
    cat > "$tmp"
    $SUDO install -m "$mode" "$tmp" "$path"
    local rc=$?
    rm -f "$tmp"
    return $rc
}

# /etc/zerogex is root:ubuntu 0750 where make alert-template-install made it.
# The units, make and the liveness watchdog all run as this user and must be
# able to see the markers, or the guard would silently let the engines start.
ensure_marker_dir() {
    if [ ! -d "$MARKER_DIR" ]; then
        $SUDO install -d -o root -g "$(id -gn)" -m 0750 "$MARKER_DIR" 2>/dev/null \
            || $SUDO install -d -m 0755 "$MARKER_DIR"
    fi
    if [ ! -x "$MARKER_DIR" ]; then
        $SUDO chmod a+x "$MARKER_DIR"
    fi
}

set_marker() {  # set_marker PATH WHY
    ensure_marker_dir || return 1
    printf '%s\nSet by deploy/engine_box.sh on %s at %s. See %s\n' "$2" "$BOX_NAME" \
        "$(TZ=America/New_York date '+%Y-%m-%d %H:%M %Z')" "$RUNBOOK" \
        | write_root_file "$1" 0644 || return 1
    # Seen by this user, which is what the guard, make and the watchdog run as.
    [ -e "$1" ]
}

engine_dropin() {
    cat <<'EOF'
# Installed by deploy/engine_box.sh. See docs/runbooks/engine_box_split.md.
[Service]
# Refuses to start while /etc/zerogex/engines.disabled exists: the engines run
# on another box, and ThetaData keeps one session per account, so running them
# here too would knock both boxes off the feed. Inert without the marker.
ExecStartPre=/bin/sh -c 'if [ -e /etc/zerogex/engines.disabled ]; then echo "Not starting: /etc/zerogex/engines.disabled says the engines run on another box. See docs/runbooks/engine_box_split.md" >&2; exit 1; fi'
# Names this unit's database sessions in pg_stat_activity and in RDS
# Performance Insights, so database load can be read per service.
Environment=PGAPPNAME=%N
EOF
}

api_dropin() {
    cat <<'EOF'
# Installed by deploy/engine_box.sh. See docs/runbooks/engine_box_split.md.
[Service]
# Refuses to start on the engine box: no customer traffic reaches it, and the
# TradeWorkz scheduler inside the API would run a second time beside the web
# box's (its runner lock is per host). Inert without the marker.
ExecStartPre=/bin/sh -c 'if [ -e /etc/zerogex/engine-box ]; then echo "Not starting: /etc/zerogex/engine-box marks this as the engine box, and the API runs on the web box. See docs/runbooks/engine_box_split.md" >&2; exit 1; fi'
EOF
}

install_dropin() {  # install_dropin UNIT GENERATOR
    local gen="$2" dir="$SYSTEMD_TARGET/$1.service.d"
    $SUDO install -d -m 0755 "$dir" || return 1
    "$gen" | write_root_file "$dir/$DROPIN_NAME" 0644
}

install_engine_dropins() {
    local u
    for u in "${ENGINE_UNITS[@]}"; do
        install_dropin "$u" engine_dropin || return 1
    done
}

daemon_reload() { $SUDO "$SYSTEMCTL" daemon-reload; }

# --- status -----------------------------------------------------------------------

role_line() {
    if [ -e "$ENGINE_BOX" ] && [ -e "$ENGINES_DISABLED" ]; then
        say "Role: ENGINE BOX, engines switched OFF (prepared, or released)"
    elif [ -e "$ENGINE_BOX" ]; then
        say "Role: ENGINE BOX, engines switched ON"
    elif [ -e "$ENGINES_DISABLED" ]; then
        say "Role: WEB BOX, engines switched OFF here (they run on the engine box)"
    else
        say "Role: ONE BOX (no markers): runs the engines, the API and the scheduled jobs"
    fi
}

cmd_status() {
    local problems=0 u en ac di
    head_line "Engine box status: $BOX_NAME (${BOX_ADDRS:-no address})"
    role_line

    say ""
    printf '  %-28s %-10s %-10s %-9s %s\n' unit installed enabled active guard
    for u in "${ENGINE_UNITS[@]}" "$API_UNIT"; do
        if installed "$u"; then
            en="$("$SYSTEMCTL" is-enabled "$u" 2>/dev/null)"; en="${en:-disabled}"
            ac="$("$SYSTEMCTL" is-active "$u" 2>/dev/null)"; ac="${ac:-inactive}"
            if has_dropin "$u"; then di=yes; else di=no; fi
            printf '  %-28s %-10s %-10s %-9s %s\n' "$u" yes "$en" "$ac" "$di"
        else
            printf '  %-28s %-10s\n' "$u" no
        fi
    done

    # Unsafe states.
    if [ -e "$ENGINES_DISABLED" ]; then
        for u in "${ENGINE_UNITS[@]}"; do
            if active "$u"; then
                bad "$u is running on a box marked engines.disabled"
                problems=$((problems + 1))
            fi
        done
    fi
    if [ -e "$ENGINE_BOX" ]; then
        if active "$API_UNIT"; then
            bad "$API_UNIT is running on the engine box"
            problems=$((problems + 1))
        fi
        local jobs=() t name
        while IFS= read -r t; do
            [ -n "$t" ] || continue
            name="${t%.timer}"
            case " ${ENGINE_BOX_TIMERS[*]} " in
                *" $name "*) ;;
                *) jobs+=("$name") ;;
            esac
        done < <("$SYSTEMCTL" list-unit-files --state=enabled --no-legend 'zerogex-oa-*.timer' 2>/dev/null | awk '{print $1}')
        if [ "${#jobs[@]}" -gt 0 ]; then
            bad "scheduled jobs enabled on the engine box, so they run twice: ${jobs[*]}"
            say "    They belong on the web box. Disable each here: sudo systemctl disable --now <name>.timer"
            problems=$((problems + 1))
        fi
    fi

    say ""
    # The scheduled jobs belong on the web box; the box's own monitors run on
    # both, so they are counted apart.
    local n_jobs=0 monitors=() t
    while IFS= read -r t; do
        [ -n "$t" ] || continue
        case " ${ENGINE_BOX_TIMERS[*]} " in
            *" ${t%.timer} "*) monitors+=("${t%.timer}") ;;
            *) n_jobs=$((n_jobs + 1)) ;;
        esac
    done < <("$SYSTEMCTL" list-unit-files --state=enabled --no-legend 'zerogex-oa-*.timer' 2>/dev/null | awk '{print $1}')
    say "Scheduled jobs (zerogex-oa-*.timer) enabled here: $n_jobs"
    say "This box's own monitors on: ${monitors[*]:-none}"

    say ""
    say "Database handoff record:"
    if handoff_table_exists; then
        local row
        row="$(newest_handoff)"
        if [ -z "$row" ]; then
            say "  none yet (no handoff has happened)"
        else
            IFS='|' read -r h_action h_host h_addr h_units h_when <<<"$row"
            say "  newest: $h_action by $h_host ($h_addr) at $h_when"
            [ -n "$h_units" ] && say "  units:  $h_units"
            if [ "$h_action" = "took_over" ] && ! is_this_box "$h_host" "$h_addr" && any_engine_active; then
                bad "engines are running here, but the database says $h_host took them over"
                problems=$((problems + 1))
            fi
        fi
        local app addr cnt
        while IFS='|' read -r app addr cnt; do
            [ -n "$app" ] || continue
            say "  connected now: $app from $addr ($cnt sessions)"
        done < <(engine_sessions)
    else
        say "  unavailable: the database is unreachable from here, or the"
        say "  engine_box_handoffs table is missing (run make schema-apply on the web box)"
    fi

    say ""
    say "Symbol lists in .env (must read the same on both boxes):"
    local fp
    fp="$(symbol_lines | sha256sum 2>/dev/null | cut -c1-12)"
    say "  INGEST_UNDERLYINGS=$(env_get INGEST_UNDERLYINGS)"
    say "  ANALYTICS_UNDERLYINGS=$(env_get ANALYTICS_UNDERLYINGS)"
    say "  fingerprint of every symbol-list line: ${fp:-none}"
    say "  (compare: run make engine-status on the other box; the fingerprints must match)"

    if [ -e "$ENGINE_BOX" ] || any_engine_active; then
        say ""
        say "Theta Terminal on this box:"
        if [ -f "$THETA_DIR/ThetaTerminalv3.jar" ]; then
            ok "jar: $THETA_DIR/ThetaTerminalv3.jar"
        else
            bad "jar missing: $THETA_DIR/ThetaTerminalv3.jar"
            problems=$((problems + 1))
        fi
        if [ -f "$THETA_DIR/creds.txt" ]; then
            ok "creds.txt present"
        else
            bad "creds missing: $THETA_DIR/creds.txt"
            problems=$((problems + 1))
        fi
        if command -v java >/dev/null 2>&1; then
            ok "java: $(java -version 2>&1 | head -1)"
        else
            bad "java is not installed"
            problems=$((problems + 1))
        fi
    fi

    say ""
    if [ "$problems" -gt 0 ]; then
        bad "$problems problem(s) above. See $RUNBOOK."
        return 1
    fi
    ok "No unsafe state found."
    return 0
}

# --- prepare ------------------------------------------------------------------------

cmd_prepare() {
    local confirm="$1" skip_deploy="$2" u

    head_line "Prepare an engine box: $BOX_NAME"
    # Refuse anything that looks like the web box or a live engine box. Marking
    # the web box as the engine box would stop its API at the next restart.
    if installed "$API_UNIT" || active "$API_UNIT"; then
        die "$API_UNIT is installed here, so this is the web box (or the one box), not a new engine box. Nothing changed."
    fi
    if any_engine_active; then
        die "An engine is running on this box. prepare is for a NEW box; to move engines here use release on the other box, then takeover here."
    fi
    [ -f "$ENV_FILE" ] || die ".env not found at $ENV_FILE. Copy it from the web box first ($RUNBOOK, step 3)."

    say "This will:"
    say "  1. mark this box: $ENGINE_BOX and $ENGINES_DISABLED"
    say "     (the engines cannot start here until takeover; the API never can)"
    if [ "$skip_deploy" = "1" ]; then
        say "  2. skip the base system install (SKIP_DEPLOY)"
    else
        say "  2. install the base system: deploy/deploy.sh, which on an engine box runs"
        say "     only steps 010 015 020 030 040 050 (packages, Postgres client, venv, firewall)"
    fi
    say "  3. install the engine units without enabling or starting them:"
    say "     ${ENGINE_UNITS[*]}"
    say "  4. install the guard drop-ins, the failure-alert template and the journald cap"
    say "  5. switch on this box's own monitoring: ${ENGINE_BOX_TIMERS[*]}"
    say "Nothing here logs in to ThetaData or writes to the database."

    if [ "$confirm" != "1" ]; then
        say ""
        warn "Dry run. To do it: make engine-prepare CONFIRM=yes"
        return 0
    fi

    say ""
    set_marker "$ENGINE_BOX" "This is the engine box: the API and the scheduled jobs run on the web box." \
        || die "could not write $ENGINE_BOX"
    set_marker "$ENGINES_DISABLED" "Engines switched off here until make engine-takeover CONFIRM=yes." \
        || die "could not write $ENGINES_DISABLED"
    ok "marked: $ENGINE_BOX, $ENGINES_DISABLED"

    if [ "$skip_deploy" != "1" ]; then
        say ""
        say "Running the base system install (this takes several minutes)..."
        if ! bash "$DEPLOY_SCRIPT"; then
            die "deploy/deploy.sh failed. Fix what it printed, then rerun: make engine-prepare CONFIRM=yes"
        fi
        ok "base system installed"
    fi

    say ""
    for u in "${ENGINE_UNITS[@]}"; do
        [ -f "$SYSTEMD_SOURCE/$u.service" ] || die "missing $SYSTEMD_SOURCE/$u.service"
        $SUDO cp "$SYSTEMD_SOURCE/$u.service" "$SYSTEMD_TARGET/" || die "could not install $u.service"
    done
    ok "engine units installed (not enabled, not started)"
    install_engine_dropins || die "could not install the engine drop-ins"
    install_dropin "$API_UNIT" api_dropin || die "could not install the API drop-in"
    ok "guard drop-ins installed"
    if [ -f "$SYSTEMD_SOURCE/zerogex-alert@.service" ]; then
        $SUDO cp "$SYSTEMD_SOURCE/zerogex-alert@.service" "$SYSTEMD_TARGET/" \
            && ok "failure-alert template installed (zerogex-alert@.service)"
    fi
    if [ -f "$SYSTEMD_SOURCE/zerogex-oa-journald.conf" ]; then
        $SUDO install -d -m 0755 "$JOURNALD_DROPIN_DIR" \
            && $SUDO install -m 0644 "$SYSTEMD_SOURCE/zerogex-oa-journald.conf" "$JOURNALD_DROPIN_DIR/10-zerogex-oa.conf" \
            && { $SUDO "$SYSTEMCTL" restart systemd-journald 2>/dev/null || true; } \
            && ok "journald cap installed"
    fi

    install -d -m 0755 "$MONITORING_DIR" 2>/dev/null || $SUDO install -d -o "$(id -un)" -m 0755 "$MONITORING_DIR"
    for u in "${ENGINE_BOX_TIMERS[@]}"; do
        if [ -f "$SYSTEMD_SOURCE/$u.service" ] && [ -f "$SYSTEMD_SOURCE/$u.timer" ]; then
            $SUDO cp "$SYSTEMD_SOURCE/$u.service" "$SYSTEMD_SOURCE/$u.timer" "$SYSTEMD_TARGET/" \
                || die "could not install $u"
        fi
    done
    daemon_reload || die "systemctl daemon-reload failed"
    for u in "${ENGINE_BOX_TIMERS[@]}"; do
        [ -f "$SYSTEMD_TARGET/$u.timer" ] || continue
        if $SUDO "$SYSTEMCTL" enable --now "$u.timer" >/dev/null 2>&1; then
            ok "$u.timer on"
        else
            warn "could not switch on $u.timer"
        fi
    done

    if [ ! -f "$MARKER_DIR/alert.env" ]; then
        warn "No $MARKER_DIR/alert.env: failure and liveness alerts from this box will only reach its journal."
        warn "Copy it from the web box ($RUNBOOK, step 3)."
    fi

    say ""
    cmd_status || true
    say ""
    ok "Prepared. The engines stay off until the cutover: $RUNBOOK, step 6."
}

# --- release --------------------------------------------------------------------------

cmd_release() {
    local confirm="$1" running u stopped=() failed=0
    head_line "Release the engines on: $BOX_NAME"
    running="$(engines_running_here)"
    if [ -z "$running" ]; then
        say "No engine unit is enabled or running here."
    else
        say "Engine units that run here now: ${running//,/ }"
    fi
    say ""
    say "This will:"
    say "  1. mark this box $ENGINES_DISABLED, so no engine can start here"
    say "     (not a restart, not a reboot, not a habitual make ingestion-restart)"
    say "  2. install the guard drop-ins next to the engine units"
    say "  3. stop the engines, last first, and switch them off at boot"
    say "  4. record the release in the database, so the other box may take over"
    say "The API, the website and the scheduled jobs are not touched."

    if [ "$confirm" != "1" ]; then
        say ""
        warn "Dry run. To do it: make engine-release CONFIRM=yes"
        return 0
    fi

    say ""
    set_marker "$ENGINES_DISABLED" "Engines switched off here: they run on another box." \
        || die "could not write $ENGINES_DISABLED; nothing else changed"
    ok "marked: $ENGINES_DISABLED"
    install_engine_dropins || warn "could not install every guard drop-in"
    daemon_reload || warn "systemctl daemon-reload failed"

    local i
    for (( i=${#ENGINE_UNITS[@]}-1; i>=0; i-- )); do
        u="${ENGINE_UNITS[$i]}"
        installed "$u" || continue
        if active "$u"; then
            say "→ stopping $u"
            $SUDO "$SYSTEMCTL" stop "$u" || warn "stop returned an error for $u"
        fi
        if enabled "$u"; then
            $SUDO "$SYSTEMCTL" disable "$u" >/dev/null 2>&1 || warn "could not disable $u"
        fi
        stopped+=("$u")
    done

    for u in "${ENGINE_UNITS[@]}"; do
        if active "$u"; then
            bad "$u is still running"
            failed=1
        fi
    done
    [ "$failed" = "0" ] || die "Not every engine stopped. Do not take over on the other box until this says released."
    ok "every engine is stopped and off at boot"

    # Record the units that ran here, so takeover starts the same set there.
    local units="$running"
    if [ -z "$units" ]; then
        units="$(join_comma "${REQUIRED_UNITS[@]}")"
        warn "nothing was running here, so the record names the required set: ${units//,/ }"
    fi
    if handoff_table_exists && record_handoff released "$units"; then
        ok "release recorded in the database (engine_box_handoffs)"
    else
        bad "Could not record the release in the database."
        say "  The engines are stopped here. If the other box refuses to take over, check"
        say "  that make schema-apply has run (it creates engine_box_handoffs), then rerun"
        say "  this release, or take over there with FORCE=yes."
        exit 1
    fi
    say ""
    ok "Released. Next, on the box taking the engines: make engine-takeover CONFIRM=yes"
}

# --- takeover -----------------------------------------------------------------------------

cmd_takeover() {
    local confirm="$1" force="$2" u units="" row h_action="" h_host="" h_addr="" h_units="" h_when=""
    head_line "Take the engines over on: $BOX_NAME"

    # Preconditions that do not depend on the other box.
    for u in "${REQUIRED_UNITS[@]}"; do
        installed "$u" || die "$u is not installed here. On a new engine box run make engine-prepare CONFIRM=yes first."
    done
    [ -f "$THETA_DIR/ThetaTerminalv3.jar" ] || die "Theta Terminal jar missing: $THETA_DIR/ThetaTerminalv3.jar ($RUNBOOK, step 3)"
    [ -f "$THETA_DIR/creds.txt" ] || die "Theta Terminal credentials missing: $THETA_DIR/creds.txt ($RUNBOOK, step 3)"
    command -v java >/dev/null 2>&1 || die "java is not installed ($RUNBOOK, step 4)"
    [ -f "$ENV_FILE" ] || die ".env not found at $ENV_FILE"

    # The other box must have released the engines.
    if ! handoff_table_exists; then
        if [ "$force" = "1" ]; then
            warn "FORCE: the database handoff record is unavailable; going ahead without it"
        else
            die "Cannot read the handoff record (database unreachable, or make schema-apply has not created engine_box_handoffs). Nothing changed."
        fi
    else
        row="$(newest_handoff)"
        IFS='|' read -r h_action h_host h_addr h_units h_when <<<"$row"
        if [ -z "$row" ]; then
            if [ "$force" = "1" ]; then
                warn "FORCE: no handoff recorded yet"
            else
                die "No release is recorded. Run make engine-release CONFIRM=yes on the box running the engines first. Nothing changed."
            fi
        elif [ "$h_action" = "released" ]; then
            ok "the engines were released by $h_host at $h_when"
            units="$h_units"
        elif [ "$h_action" = "took_over" ] && is_this_box "$h_host" "$h_addr"; then
            ok "this box already holds the engines (since $h_when); starting any that are down"
            units="$h_units"
        elif [ "$force" = "1" ]; then
            warn "FORCE: the database says $h_host took the engines over at $h_when and never released them."
            warn "Only right when that box is dead or stopped."
        else
            die "The database says $h_host took the engines over at $h_when and has not released them. Run make engine-release CONFIRM=yes there first (or FORCE=yes if that box is dead). Nothing changed."
        fi
    fi

    # Belt and braces: named engine sessions from another address.
    local app addr cnt others=""
    while IFS='|' read -r app addr cnt; do
        [ -n "$app" ] || continue
        if any_engine_active && is_my_addr "$addr"; then
            continue
        fi
        others="$others $app@$addr"
    done < <(engine_sessions)
    if [ -n "$others" ]; then
        if [ "$force" = "1" ]; then
            warn "FORCE: engine sessions are connected from elsewhere:$others"
        else
            die "Engine sessions are connected to the database from elsewhere:$others. Release that box first. Nothing changed."
        fi
    fi

    [ -n "$units" ] || units="$(join_comma "${ENGINE_UNITS[@]}")"
    local start=()
    for u in "${ENGINE_UNITS[@]}"; do
        case ",$units," in
            *",$u,"*)
                if installed "$u"; then
                    start+=("$u")
                else
                    die "$u ran on the releasing box but is not installed here. Run make engine-prepare CONFIRM=yes SKIP_DEPLOY=1."
                fi
                ;;
        esac
    done
    [ "${#start[@]}" -gt 0 ] || die "No engine units to start."

    say ""
    say "It is $(TZ=America/New_York date '+%a %Y-%m-%d %H:%M %Z')."
    case "$(TZ=America/New_York date '+%u')" in
        6) say "Saturday: no feed is running anywhere, so the move is invisible to customers." ;;
        7) say "Sunday: futures reopen at 18:00 ET; take over before then." ;;
        *) warn "A weekday: the live feed gaps for a minute or two while the terminal logs in and the engines start." ;;
    esac
    say ""
    say "This will:"
    say "  1. remove $ENGINES_DISABLED here"
    say "  2. switch on and start, in order, checking each stays up:"
    say "     ${start[*]}"
    say "  3. record the takeover in the database"

    if [ "$confirm" != "1" ]; then
        say ""
        warn "Dry run. To do it: make engine-takeover CONFIRM=yes"
        return 0
    fi

    say ""
    install_engine_dropins || warn "could not install every guard drop-in"
    $SUDO rm -f "$ENGINES_DISABLED" || die "could not remove $ENGINES_DISABLED"
    daemon_reload || warn "systemctl daemon-reload failed"
    ok "removed $ENGINES_DISABLED"

    local settle
    for u in "${start[@]}"; do
        say "→ starting $u"
        $SUDO "$SYSTEMCTL" enable "$u" >/dev/null 2>&1 || warn "could not enable $u at boot"
        if ! $SUDO "$SYSTEMCTL" start "$u"; then
            takeover_failed "$u"
        fi
        settle="$SETTLE_SECONDS"
        [ "$u" = "zerogex-oa-thetaterminal" ] && settle="$THETA_SETTLE_SECONDS"
        sleep "$settle"
        active "$u" || takeover_failed "$u"
        ok "$u running"
    done

    if handoff_table_exists && record_handoff took_over "$(join_comma "${start[@]}")"; then
        ok "takeover recorded in the database (engine_box_handoffs)"
    else
        warn "The engines run here, but the takeover could not be recorded in the database."
        warn "Run make engine-status on both boxes before doing anything else."
    fi
    say ""
    ok "Took over. Check it: $RUNBOOK, step 7."
}

takeover_failed() {
    bad "$1 did not come up."
    say "  Its log:  sudo journalctl -u $1 -n 50 --no-pager"
    say "  Engines started so far stay up. To hand the engines back to the other box:"
    say "    here:           make engine-release CONFIRM=yes"
    say "    the other box:  make engine-takeover CONFIRM=yes"
    exit 1
}

# --- main ---------------------------------------------------------------------------------

if [ "${EUID:-$(id -u)}" -eq 0 ] && [ -z "${ZGX_ALLOW_ROOT:-}" ]; then
    die "Run as the 'ubuntu' user, not root; the script calls sudo where it must."
fi

cmd="${1:-}"
[ -n "$cmd" ] || usage
shift
CONFIRM=0
FORCE=0
SKIP_DEPLOY=0
for arg in "$@"; do
    case "$arg" in
        --confirm) CONFIRM=1 ;;
        --force) FORCE=1 ;;
        --skip-deploy) SKIP_DEPLOY=1 ;;
        -h|--help) usage ;;
        *) die "Unknown option: $arg" ;;
    esac
done

case "$cmd" in
    status)   cmd_status ;;
    prepare)  cmd_prepare "$CONFIRM" "$SKIP_DEPLOY" ;;
    release)  cmd_release "$CONFIRM" ;;
    takeover) cmd_takeover "$CONFIRM" "$FORCE" ;;
    -h|--help|help) usage ;;
    *) die "Unknown command: $cmd (status | prepare | release | takeover)" ;;
esac
