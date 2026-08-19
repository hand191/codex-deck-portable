#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
APP_ROOT="/opt/codex-deck"
STATE_ROOT="/var/lib/codex-deck"
WORKSPACE_ROOT="/"
ENV_FILE="/etc/codex-deck.env"
UNIT_FILE="/etc/systemd/system/codex-deck.service"
SERVICE_NAME="codex-deck.service"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

assert_service_idle() {
  local health_json
  health_json="$(curl --fail --silent --show-error --max-time 5 "http://127.0.0.1:${LOCAL_PORT}/api/health")" \
    || die "running service health check failed; inspect it before upgrading"
  if [[ "${FORCE:-0}" != "1" ]]; then
    python3 -c 'import json,sys; h=json.loads(sys.argv[1]); busy=any(int(h.get(k,0) or 0) for k in ("active_jobs","queued_jobs","database_queued_jobs")); raise SystemExit(1 if busy else 0)' "${health_json}" \
      || die "tasks are active or queued; wait for them or rerun explicitly with FORCE=1"
  fi
}

[[ "${EUID}" -eq 0 ]] || die "run with sudo or as root"
for command_name in python3 systemctl curl openssl install cp ln mv readlink date sed grep; do
  command -v "${command_name}" >/dev/null 2>&1 || die "missing command: ${command_name}"
done

python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' \
  || die "Python 3.10 or newer is required"

APP_VERSION="$(python3 -c 'import pathlib,re,sys; text=pathlib.Path(sys.argv[1]).read_text(); match=re.search(r"^APP_VERSION = \"([^\"]+)\"", text, re.M); print(match.group(1) if match else "")' "${SOURCE_ROOT}/codex_web.py" 2>/dev/null)"
[[ "${APP_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([._-][A-Za-z0-9]+)*$ ]] \
  || die "unable to read a safe APP_VERSION"
RELEASE_NAME="v${APP_VERSION}-$(date -u +%Y%m%dT%H%M%SZ)"
RELEASE_DIR="${APP_ROOT}/releases/${RELEASE_NAME}"
[[ ! -e "${RELEASE_DIR}" ]] || die "release already exists: ${RELEASE_DIR}"

LOCAL_PORT="8788"
[[ ! -L "${ENV_FILE}" ]] || die "${ENV_FILE} must not be a symlink"
[[ ! -e "${ENV_FILE}" || -f "${ENV_FILE}" ]] \
  || die "${ENV_FILE} must be a regular file"
[[ ! -L "${UNIT_FILE}" ]] || die "${UNIT_FILE} must not be a symlink"
if [[ -f "${ENV_FILE}" ]]; then
  CONFIGURED_PORT="$(sed -n 's/^CODEX_WEB_PORT=//p' "${ENV_FILE}" | tail -n 1)"
  if [[ "${CONFIGURED_PORT}" =~ ^[0-9]+$ ]]; then
    LOCAL_PORT="${CONFIGURED_PORT}"
  fi
  CONFIGURED_HOST="$(sed -n 's/^CODEX_WEB_HOST=//p' "${ENV_FILE}" | tail -n 1)"
  [[ "${CONFIGURED_HOST}" == "127.0.0.1" ]] \
    || die "CODEX_WEB_HOST must remain 127.0.0.1 in the portable installer"
fi
((10#${LOCAL_PORT} >= 1 && 10#${LOCAL_PORT} <= 65535)) \
  || die "CODEX_WEB_PORT is invalid"

if systemctl is-active --quiet "${SERVICE_NAME}"; then
  assert_service_idle
elif python3 -c 'import socket,sys; sock=socket.socket(); sock.settimeout(0.3); occupied=sock.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0; sock.close(); raise SystemExit(0 if occupied else 1)' "${LOCAL_PORT}"; then
  die "127.0.0.1:${LOCAL_PORT} is already in use by another service"
fi

install -d -m 0755 "${APP_ROOT}/releases"
install -d -m 0700 "${STATE_ROOT}" "${STATE_ROOT}/uploads" "${STATE_ROOT}/backups"
if [[ "${WORKSPACE_ROOT}" != "/" ]]; then
  install -d -m 0700 "${WORKSPACE_ROOT}"
fi

if [[ -L "${STATE_ROOT}/api-token" ]]; then
  die "${STATE_ROOT}/api-token must not be a symlink"
elif [[ ! -f "${STATE_ROOT}/api-token" ]]; then
  umask 077
  openssl rand -hex 32 > "${STATE_ROOT}/api-token"
fi
chmod 0600 "${STATE_ROOT}/api-token"

if [[ -e "${APP_ROOT}/current" && ! -L "${APP_ROOT}/current" ]]; then
  die "${APP_ROOT}/current exists but is not a symlink"
fi

install -d -m 0755 "${RELEASE_DIR}"
for source_file in README.md AGENTS.md CHANGELOG.md requirements.txt codex_web.py codex_runtime.py job_stream.py test_codex_sso.py; do
  install -m 0644 "${SOURCE_ROOT}/${source_file}" "${RELEASE_DIR}/${source_file}"
done
cp -R "${SOURCE_ROOT}/docs" "${SOURCE_ROOT}/deploy" "${RELEASE_DIR}/"

python3 -m venv "${RELEASE_DIR}/.venv"
"${RELEASE_DIR}/.venv/bin/python" -m pip install --disable-pip-version-check \
  -r "${RELEASE_DIR}/requirements.txt"
CODEX_BIN_REAL="$("${RELEASE_DIR}/.venv/bin/python" -c 'from codex_cli_bin import bundled_codex_path; print(bundled_codex_path())')"
[[ -n "${CODEX_BIN_REAL}" && -x "${CODEX_BIN_REAL}" ]] \
  || die "the pinned Codex CLI binary is unavailable"
install -d -m 0755 "${RELEASE_DIR}/bin"
ln -s "${CODEX_BIN_REAL}" "${RELEASE_DIR}/bin/codex"
"${RELEASE_DIR}/bin/codex" --version

if [[ ! -f "${ENV_FILE}" ]]; then
  install -m 0600 "${SOURCE_ROOT}/deploy/codex-deck.env.example" "${ENV_FILE}"
fi
chmod 0600 "${ENV_FILE}"
if grep -q '^CODEX_BIN=' "${ENV_FILE}"; then
  sed -i "s#^CODEX_BIN=.*#CODEX_BIN=${APP_ROOT}/current/bin/codex#" "${ENV_FILE}"
else
  printf 'CODEX_BIN=%s/current/bin/codex\n' "${APP_ROOT}" >> "${ENV_FILE}"
fi
BIND_HOST="$(sed -n 's/^CODEX_WEB_HOST=//p' "${ENV_FILE}" | tail -n 1)"
[[ "${BIND_HOST}" == "127.0.0.1" ]] \
  || die "CODEX_WEB_HOST must remain 127.0.0.1 in the portable installer"

(cd "${RELEASE_DIR}" && "${RELEASE_DIR}/.venv/bin/python" -m py_compile \
  codex_web.py codex_runtime.py job_stream.py)
(cd "${RELEASE_DIR}" && "${RELEASE_DIR}/.venv/bin/python" -m unittest -q \
  test_codex_sso.py)

DATABASE_PATH="$(sed -n 's/^CODEX_WEB_DB_PATH=//p' "${ENV_FILE}" | tail -n 1)"
DATABASE_PATH="${DATABASE_PATH:-${STATE_ROOT}/codex.sqlite3}"
[[ "${DATABASE_PATH}" == /* ]] || die "CODEX_WEB_DB_PATH must be absolute"
PREVIOUS_RELEASE=""
if [[ -L "${APP_ROOT}/current" ]]; then
  PREVIOUS_RELEASE="$(readlink -f "${APP_ROOT}/current")"
fi
SERVICE_WAS_ACTIVE=0
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  SERVICE_WAS_ACTIVE=1
  assert_service_idle
fi
ACTIVATION_STARTED=0
recover_activation_error() {
  local exit_status=$?
  trap - ERR
  if [[ "${ACTIVATION_STARTED}" == "1" ]]; then
    if [[ -n "${PREVIOUS_RELEASE}" && -d "${PREVIOUS_RELEASE}" ]]; then
      local recovery_link="${APP_ROOT}/.current-recovery-$$"
      ln -s "${PREVIOUS_RELEASE}" "${recovery_link}" || true
      mv -Tf "${recovery_link}" "${APP_ROOT}/current" || true
      if [[ -f "${PREVIOUS_RELEASE}/deploy/codex-deck.service" ]]; then
        install -m 0644 "${PREVIOUS_RELEASE}/deploy/codex-deck.service" \
          "${UNIT_FILE}" || true
      fi
    fi
    systemctl daemon-reload || true
    if [[ "${SERVICE_WAS_ACTIVE}" == "1" && -n "${PREVIOUS_RELEASE}" ]]; then
      systemctl restart "${SERVICE_NAME}" || true
    else
      systemctl stop "${SERVICE_NAME}" || true
    fi
  fi
  exit "${exit_status}"
}
trap recover_activation_error ERR

ACTIVATION_STARTED=1
if [[ "${SERVICE_WAS_ACTIVE}" == "1" ]]; then
  systemctl stop "${SERVICE_NAME}"
fi
if [[ -f "${DATABASE_PATH}" && "${FORCE:-0}" != "1" ]]; then
  if ! python3 -c 'import sqlite3,sys; db=sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True); count=db.execute("SELECT count(*) FROM jobs WHERE status IN (?, ?)", ("queued", "running")).fetchone()[0]; db.close(); raise SystemExit(1 if count else 0)' "${DATABASE_PATH}"; then
    echo "ERROR: a task entered the queue during activation; restoring the previous release" >&2
    false
  fi
fi
if [[ -f "${DATABASE_PATH}" ]]; then
  "${RELEASE_DIR}/.venv/bin/python" "${RELEASE_DIR}/deploy/backup_sqlite.py" \
    "${DATABASE_PATH}" "${STATE_ROOT}/backups"
fi
install -m 0644 "${SOURCE_ROOT}/deploy/codex-deck.service" "${UNIT_FILE}"
NEXT_LINK="${APP_ROOT}/.current-next-$$"
ln -s "${RELEASE_DIR}" "${NEXT_LINK}"
mv -Tf "${NEXT_LINK}" "${APP_ROOT}/current"

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}" >/dev/null
systemctl restart "${SERVICE_NAME}"

HEALTH_OK=0
for ((_attempt = 1; _attempt <= 30; _attempt++)); do
  if HEALTH_JSON="$(curl --fail --silent --show-error --max-time 2 "http://127.0.0.1:${LOCAL_PORT}/api/health" 2>/dev/null)"; then
    if python3 -c 'import json,sys; h=json.loads(sys.argv[1]); raise SystemExit(0 if h.get("status")=="ok" and h.get("version")==sys.argv[2] else 1)' "${HEALTH_JSON}" "${APP_VERSION}"; then
      HEALTH_OK=1
      break
    fi
  fi
  sleep 1
done

if [[ "${HEALTH_OK}" != "1" ]]; then
  echo "New release failed its health check." >&2
  if [[ -n "${PREVIOUS_RELEASE}" && -d "${PREVIOUS_RELEASE}" ]]; then
    NEXT_LINK="${APP_ROOT}/.current-rollback-$$"
    ln -s "${PREVIOUS_RELEASE}" "${NEXT_LINK}"
    mv -Tf "${NEXT_LINK}" "${APP_ROOT}/current"
    if [[ -f "${PREVIOUS_RELEASE}/deploy/codex-deck.service" ]]; then
      install -m 0644 "${PREVIOUS_RELEASE}/deploy/codex-deck.service" \
        "${UNIT_FILE}"
      systemctl daemon-reload
    fi
    systemctl restart "${SERVICE_NAME}" || true
    echo "Previous source release restored: ${PREVIOUS_RELEASE}" >&2
  else
    systemctl stop "${SERVICE_NAME}" || true
  fi
  ACTIVATION_STARTED=0
  trap - ERR
  exit 1
fi

ACTIVATION_STARTED=0
trap - ERR

echo "Codex Deck ${APP_VERSION} is healthy on 127.0.0.1:${LOCAL_PORT}"
echo "Release: ${RELEASE_DIR}"
echo "Owner token stays in ${STATE_ROOT}/api-token (not printed)."
echo "Next: open docs/DEPLOYMENT.md and create an SSH tunnel or private Tailscale Serve endpoint."
