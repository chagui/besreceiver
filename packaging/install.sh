#!/usr/bin/env bash
# install.sh — install besreceiver as a managed service on Linux (systemd) or macOS (launchd).
#
# Idempotent: safe to re-run. Existing files are overwritten; existing users are reused;
# an already-loaded launchd job or an enabled systemd unit is re-enabled without error.
#
# Does NOT build the binary. Pass --binary to point at a pre-built binary, or let the
# script use the first of: $BESRECEIVER_BINARY, ./build/besreceiver, ./besreceiver.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SERVICE_USER="${SERVICE_USER:-besreceiver}"
SERVICE_GROUP="${SERVICE_GROUP:-besreceiver}"
LAUNCHD_LABEL="com.github.chagui.besreceiver"

BINARY_SRC=""
CONFIG_SRC=""

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --binary PATH    Path to pre-built besreceiver binary (required if not discoverable)
  --config PATH    Path to a collector config.yaml to install (optional; default sample if omitted)
  -h, --help       Show this help

Environment:
  BESRECEIVER_BINARY  Alternative to --binary
  BESRECEIVER_CONFIG  Installation path for the config file
                      (default: /etc/besreceiver/config.yaml on Linux,
                       /usr/local/etc/besreceiver/config.yaml on macOS)
  SERVICE_USER        Service user on Linux (default: besreceiver)
  SERVICE_GROUP       Service group on Linux (default: besreceiver)
EOF
}

log()  { printf '[install] %s\n' "$*"; }
warn() { printf '[install] WARN: %s\n' "$*" >&2; }
die()  { printf '[install] ERROR: %s\n' "$*" >&2; exit 1; }

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        die "must run as root (try: sudo $0 $*)"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --binary) BINARY_SRC="$2"; shift 2;;
            --config) CONFIG_SRC="$2"; shift 2;;
            -h|--help) usage; exit 0;;
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

resolve_binary() {
    if [[ -n "${BINARY_SRC}" ]]; then
        [[ -x "${BINARY_SRC}" ]] || die "binary not executable: ${BINARY_SRC}"
        return
    fi
    if [[ -n "${BESRECEIVER_BINARY:-}" ]]; then
        BINARY_SRC="${BESRECEIVER_BINARY}"
    elif [[ -x "${REPO_ROOT}/build/besreceiver" ]]; then
        BINARY_SRC="${REPO_ROOT}/build/besreceiver"
    elif [[ -x "${REPO_ROOT}/besreceiver" ]]; then
        BINARY_SRC="${REPO_ROOT}/besreceiver"
    else
        die "no binary found; pass --binary PATH or set BESRECEIVER_BINARY (installer does not build)"
    fi
    [[ -x "${BINARY_SRC}" ]] || die "binary not executable: ${BINARY_SRC}"
}

install_file() {
    # install_file SRC DST MODE [OWNER:GROUP]
    local src="$1" dst="$2" mode="$3" owner="${4:-}"
    local dst_dir
    dst_dir="$(dirname "${dst}")"
    mkdir -p "${dst_dir}"
    cp "${src}" "${dst}"
    chmod "${mode}" "${dst}"
    if [[ -n "${owner}" ]]; then
        chown "${owner}" "${dst}"
    fi
}

# -----------------------------------------------------------------------------
# Linux (systemd)
# -----------------------------------------------------------------------------

linux_paths() {
    BIN_DIR="/usr/local/bin"
    CONFIG_DIR="/etc/besreceiver"
    CONFIG_PATH="${BESRECEIVER_CONFIG:-${CONFIG_DIR}/config.yaml}"
    LOG_DIR="/var/log/besreceiver"
    DATA_DIR="/var/lib/besreceiver"
    UNIT_PATH="/etc/systemd/system/besreceiver.service"
    UNIT_SRC="${SCRIPT_DIR}/systemd/besreceiver.service"
}

linux_ensure_user() {
    if getent group "${SERVICE_GROUP}" >/dev/null 2>&1; then
        log "group ${SERVICE_GROUP} already exists"
    else
        log "creating group ${SERVICE_GROUP}"
        groupadd --system "${SERVICE_GROUP}"
    fi
    if id "${SERVICE_USER}" >/dev/null 2>&1; then
        log "user ${SERVICE_USER} already exists"
    else
        log "creating system user ${SERVICE_USER}"
        useradd --system --no-create-home --shell /usr/sbin/nologin \
            --gid "${SERVICE_GROUP}" "${SERVICE_USER}"
    fi
}

linux_install() {
    require_root "$@"
    command -v systemctl >/dev/null 2>&1 || die "systemctl not found; this installer requires systemd"

    linux_paths
    linux_ensure_user

    log "installing binary to ${BIN_DIR}/besreceiver"
    install_file "${BINARY_SRC}" "${BIN_DIR}/besreceiver" 0755 "root:root"

    log "creating directories"
    mkdir -p "${CONFIG_DIR}" "${LOG_DIR}" "${DATA_DIR}"
    chown "${SERVICE_USER}:${SERVICE_GROUP}" "${LOG_DIR}" "${DATA_DIR}"
    chmod 0750 "${LOG_DIR}" "${DATA_DIR}"

    if [[ -n "${CONFIG_SRC}" ]]; then
        log "installing config from ${CONFIG_SRC} to ${CONFIG_PATH}"
        install_file "${CONFIG_SRC}" "${CONFIG_PATH}" 0640 "root:${SERVICE_GROUP}"
    elif [[ ! -f "${CONFIG_PATH}" ]]; then
        if [[ -f "${REPO_ROOT}/collector-config.yaml" ]]; then
            log "installing sample config from collector-config.yaml to ${CONFIG_PATH}"
            install_file "${REPO_ROOT}/collector-config.yaml" "${CONFIG_PATH}" 0640 "root:${SERVICE_GROUP}"
        else
            warn "no config provided and ${CONFIG_PATH} does not exist; service will fail to start until one is created"
        fi
    else
        log "config already present at ${CONFIG_PATH} (keeping existing)"
    fi

    log "installing systemd unit at ${UNIT_PATH}"
    # Substitute BESRECEIVER_CONFIG default to match install location.
    sed "s|Environment=BESRECEIVER_CONFIG=/etc/besreceiver/config.yaml|Environment=BESRECEIVER_CONFIG=${CONFIG_PATH}|" \
        "${UNIT_SRC}" > "${UNIT_PATH}.tmp"
    mv "${UNIT_PATH}.tmp" "${UNIT_PATH}"
    chmod 0644 "${UNIT_PATH}"

    log "reloading systemd"
    systemctl daemon-reload

    log "enabling and (re)starting besreceiver.service"
    systemctl enable besreceiver.service
    # restart is idempotent: starts if stopped, restarts if running.
    systemctl restart besreceiver.service

    log "done. Check status with: systemctl status besreceiver"
}

# -----------------------------------------------------------------------------
# macOS (launchd)
# -----------------------------------------------------------------------------

darwin_paths() {
    BIN_DIR="/usr/local/bin"
    CONFIG_DIR="/usr/local/etc/besreceiver"
    CONFIG_PATH="${BESRECEIVER_CONFIG:-${CONFIG_DIR}/config.yaml}"
    LOG_DIR="/usr/local/var/log/besreceiver"
    DATA_DIR="/usr/local/var/lib/besreceiver"
    PLIST_PATH="/Library/LaunchDaemons/${LAUNCHD_LABEL}.plist"
    PLIST_SRC="${SCRIPT_DIR}/launchd/${LAUNCHD_LABEL}.plist"
}

darwin_install() {
    require_root "$@"
    command -v launchctl >/dev/null 2>&1 || die "launchctl not found"

    darwin_paths

    log "installing binary to ${BIN_DIR}/besreceiver"
    install_file "${BINARY_SRC}" "${BIN_DIR}/besreceiver" 0755

    log "creating directories"
    mkdir -p "${CONFIG_DIR}" "${LOG_DIR}" "${DATA_DIR}"

    if [[ -n "${CONFIG_SRC}" ]]; then
        log "installing config from ${CONFIG_SRC} to ${CONFIG_PATH}"
        install_file "${CONFIG_SRC}" "${CONFIG_PATH}" 0644
    elif [[ ! -f "${CONFIG_PATH}" ]]; then
        if [[ -f "${REPO_ROOT}/collector-config.yaml" ]]; then
            log "installing sample config from collector-config.yaml to ${CONFIG_PATH}"
            install_file "${REPO_ROOT}/collector-config.yaml" "${CONFIG_PATH}" 0644
        else
            warn "no config provided and ${CONFIG_PATH} does not exist; service will fail to start until one is created"
        fi
    else
        log "config already present at ${CONFIG_PATH} (keeping existing)"
    fi

    log "installing launchd plist at ${PLIST_PATH}"
    # Substitute config path to match install location.
    sed "s|/usr/local/etc/besreceiver/config.yaml|${CONFIG_PATH}|g" \
        "${PLIST_SRC}" > "${PLIST_PATH}.tmp"
    mv "${PLIST_PATH}.tmp" "${PLIST_PATH}"
    chown root:wheel "${PLIST_PATH}"
    chmod 0644 "${PLIST_PATH}"

    if launchctl print "system/${LAUNCHD_LABEL}" >/dev/null 2>&1; then
        log "unloading existing launchd job (for idempotent reload)"
        launchctl bootout "system/${LAUNCHD_LABEL}" || true
    fi

    log "bootstrapping launchd job"
    launchctl bootstrap system "${PLIST_PATH}"
    launchctl enable "system/${LAUNCHD_LABEL}"
    launchctl kickstart -k "system/${LAUNCHD_LABEL}"

    log "done. Check status with: launchctl print system/${LAUNCHD_LABEL}"
}

main() {
    parse_args "$@"
    resolve_binary

    local os
    os="$(detect_os)"
    log "detected OS: ${os}; binary source: ${BINARY_SRC}"

    case "${os}" in
        linux)  linux_install "$@";;
        darwin) darwin_install "$@";;
    esac
}

main "$@"
