#!/usr/bin/env bash
# uninstall.sh — remove besreceiver service, binary, and configuration.
#
# Idempotent: safe to re-run. Missing components are skipped with an informational message.
#
# Requires --yes to actually perform destructive operations. Without --yes, the script
# prints what it would do and exits.

set -euo pipefail

SERVICE_USER="${SERVICE_USER:-besreceiver}"
SERVICE_GROUP="${SERVICE_GROUP:-besreceiver}"
LAUNCHD_LABEL="com.github.chagui.besreceiver"

CONFIRM="no"
PURGE_USER="no"
KEEP_CONFIG="no"

usage() {
    cat <<EOF
Usage: $0 --yes [options]

Without --yes the script runs in dry-run mode and prints what it would remove.

Options:
  --yes           Actually perform destructive operations
  --keep-config   Do not remove configuration files or config directory
  --purge-user    Also remove the dedicated service user/group on Linux
  -h, --help      Show this help

Environment:
  SERVICE_USER    Service user on Linux (default: besreceiver)
  SERVICE_GROUP   Service group on Linux (default: besreceiver)
EOF
}

log()    { printf '[uninstall] %s\n' "$*"; }
warn()   { printf '[uninstall] WARN: %s\n' "$*" >&2; }
die()    { printf '[uninstall] ERROR: %s\n' "$*" >&2; exit 1; }
action() { # action "description" cmd args...
    local desc="$1"; shift
    if [[ "${CONFIRM}" == "yes" ]]; then
        log "${desc}"
        "$@" || warn "command failed (continuing): $*"
    else
        log "DRY-RUN would: ${desc}"
    fi
}

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        die "must run as root (try: sudo $0 $*)"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --yes)         CONFIRM="yes"; shift;;
            --keep-config) KEEP_CONFIG="yes"; shift;;
            --purge-user)  PURGE_USER="yes"; shift;;
            -h|--help)     usage; exit 0;;
            *) die "unknown argument: $1";;
        esac
    done
}

detect_os() {
    case "$(uname -s)" in
        Linux)  echo "linux";;
        Darwin) echo "darwin";;
        *)      die "unsupported OS: $(uname -s)";;
    esac
}

rm_if_exists() {
    local path="$1"
    if [[ -e "${path}" || -L "${path}" ]]; then
        action "remove ${path}" rm -rf "${path}"
    else
        log "skip (not present): ${path}"
    fi
}

# -----------------------------------------------------------------------------
# Linux (systemd)
# -----------------------------------------------------------------------------

linux_uninstall() {
    require_root "$@"
    command -v systemctl >/dev/null 2>&1 || die "systemctl not found"

    local unit="besreceiver.service"
    local unit_path="/etc/systemd/system/${unit}"
    local bin_path="/usr/local/bin/besreceiver"
    local config_dir="/etc/besreceiver"
    local log_dir="/var/log/besreceiver"
    local data_dir="/var/lib/besreceiver"

    if systemctl list-unit-files "${unit}" 2>/dev/null | grep -q "^${unit}"; then
        if systemctl is-active --quiet "${unit}"; then
            action "stop ${unit}" systemctl stop "${unit}"
        else
            log "service ${unit} not active"
        fi
        if systemctl is-enabled --quiet "${unit}" 2>/dev/null; then
            action "disable ${unit}" systemctl disable "${unit}"
        else
            log "service ${unit} not enabled"
        fi
    else
        log "service ${unit} not installed"
    fi

    rm_if_exists "${unit_path}"
    action "reload systemd" systemctl daemon-reload
    action "reset failed state" systemctl reset-failed "${unit}" 2>/dev/null || true

    rm_if_exists "${bin_path}"
    rm_if_exists "${log_dir}"
    rm_if_exists "${data_dir}"

    if [[ "${KEEP_CONFIG}" == "yes" ]]; then
        log "keeping config directory: ${config_dir}"
    else
        rm_if_exists "${config_dir}"
    fi

    if [[ "${PURGE_USER}" == "yes" ]]; then
        if id "${SERVICE_USER}" >/dev/null 2>&1; then
            action "remove user ${SERVICE_USER}" userdel "${SERVICE_USER}"
        else
            log "user ${SERVICE_USER} not present"
        fi
        if getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
            action "remove group ${SERVICE_GROUP}" groupdel "${SERVICE_GROUP}"
        else
            log "group ${SERVICE_GROUP} not present"
        fi
    else
        log "keeping service user/group (pass --purge-user to remove)"
    fi
}

# -----------------------------------------------------------------------------
# macOS (launchd)
# -----------------------------------------------------------------------------

darwin_uninstall() {
    require_root "$@"
    command -v launchctl >/dev/null 2>&1 || die "launchctl not found"

    local plist_path="/Library/LaunchDaemons/${LAUNCHD_LABEL}.plist"
    local bin_path="/usr/local/bin/besreceiver"
    local config_dir="/usr/local/etc/besreceiver"
    local log_dir="/usr/local/var/log/besreceiver"
    local data_dir="/usr/local/var/lib/besreceiver"

    if launchctl print "system/${LAUNCHD_LABEL}" >/dev/null 2>&1; then
        action "bootout launchd job" launchctl bootout "system/${LAUNCHD_LABEL}"
    else
        log "launchd job ${LAUNCHD_LABEL} not loaded"
    fi

    rm_if_exists "${plist_path}"
    rm_if_exists "${bin_path}"
    rm_if_exists "${log_dir}"
    rm_if_exists "${data_dir}"

    if [[ "${KEEP_CONFIG}" == "yes" ]]; then
        log "keeping config directory: ${config_dir}"
    else
        rm_if_exists "${config_dir}"
    fi
}

main() {
    parse_args "$@"
    if [[ "${CONFIRM}" != "yes" ]]; then
        warn "running in DRY-RUN mode; re-run with --yes to actually remove files"
    fi

    local os
    os="$(detect_os)"
    log "detected OS: ${os}"

    case "${os}" in
        linux)  linux_uninstall "$@";;
        darwin) darwin_uninstall "$@";;
    esac

    log "done."
}

main "$@"
