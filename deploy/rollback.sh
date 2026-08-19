#!/usr/bin/env bash
set -Eeuo pipefail

APP_ROOT="/opt/codex-deck"
SERVICE_NAME="codex-deck.service"
ENV_FILE="/etc/codex-deck.env"
UNIT_FILE="/etc/systemd/system/codex-deck.service"
TARGET="${1:-}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "ERROR: run with sudo or as root" >&2
  exit 1
fi
[[ -f "${ENV_FILE}" && ! -L "${ENV_FILE}" ]] || {
  echo "ERROR: ${ENV_FILE} must be a regular non-symlink file" >&2
  exit 1
}
[[ ! -L "${UNIT_FILE}" ]] || {
  echo "ERROR: ${UNIT_FILE} must not be a symlink" >&2
  exit 1
}
if [[ -z "${TARGET}" ]]; then
  echo "Usage: $0 <release-directory-name>" >&2
  echo "Available releases:" >&2
  find "${APP_ROOT}/releases" -mindepth 1 -maxdepth 1 -type d -printf '  %f\n' 2>/dev/null | sort -r >&2
  exit 2
fi
if [[ ! "${TARGET}" =~ ^[A-Za-z0-9._-]+$ || "${TARGET}" == "." || "${TARGET}" == ".." ]]; then
  echo "ERROR: pass a release directory name, not a path" >&2
  exit 2
fi

TARGET_DIR="${APP_ROOT}/releases/${TARGET}"
[[ -d "${TARGET_DIR}" && -f "${TARGET_DIR}/codex_web.py" \
  && -x "${TARGET_DIR}/.venv/bin/python" \
  && -x "${TARGET_DIR}/bin/codex" \
  && -f "${TARGET_DIR}/deploy/codex-deck.service" ]] || {
  echo "ERROR: release not found: ${TARGET}" >&2
  exit 1
}
TARGET_VERSION="$(python3 -c 'import pathlib,re,sys; text=pathlib.Path(sys.argv[1]).read_text(); match=re.search(r"^APP_VERSION = \"([^\"]+)\"", text, re.M); print(match.group(1) if match else "")' "${TARGET_DIR}/codex_web.py")"
[[ "${TARGET_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([._-][A-Za-z0-9]+)*$ ]] || {
  echo "ERROR: target APP_VERSION is invalid" >&2
  exit 1
}
[[ -L "${APP_ROOT}/current" ]] || {
  echo "ERROR: ${APP_ROOT}/current is not a release symlink" >&2
  exit 1
}
ORIGINAL_DIR="$(readlink -f "${APP_ROOT}/current")"
[[ -d "${ORIGINAL_DIR}" && -f "${ORIGINAL_DIR}/codex_web.py" ]] || {
  echo "ERROR: current release target is invalid" >&2
  exit 1
}

LOCAL_PORT="$(sed -n 's/^CODEX_WEB_PORT=//p' "${ENV_FILE}" 2>/dev/null | tail -n 1)"
[[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] || LOCAL_PORT="8788"
((10#${LOCAL_PORT} >= 1 && 10#${LOCAL_PORT} <= 65535)) || {
  echo "ERROR: CODEX_WEB_PORT is invalid" >&2
  exit 1
}
BIND_HOST="$(sed -n 's/^CODEX_WEB_HOST=//p' "${ENV_FILE}" 2>/dev/null | tail -n 1)"
[[ "${BIND_HOST}" == "127.0.0.1" ]] || {
  echo "ERROR: CODEX_WEB_HOST must remain 127.0.0.1" >&2
  exit 1
}
DATABASE_PATH="$(sed -n 's/^CODEX_WEB_DB_PATH=//p' "${ENV_FILE}" 2>/dev/null | tail -n 1)"
DATABASE_PATH="${DATABASE_PATH:-/var/lib/codex-deck/codex.sqlite3}"
[[ "${DATABASE_PATH}" == /* ]] || {
  echo "ERROR: CODEX_WEB_DB_PATH must be absolute" >&2
  exit 1
}
SERVICE_WAS_ACTIVE=0
if systemctl is-active --quiet "${SERVICE_NAME}"; then
  SERVICE_WAS_ACTIVE=1
  if HEALTH_JSON="$(curl --fail --silent --show-error --max-time 5 "http://127.0.0.1:${LOCAL_PORT}/api/health")"; then
    python3 -c 'import json,sys; h=json.loads(sys.argv[1]); busy=any(int(h.get(k,0) or 0) for k in ("active_jobs","queued_jobs","database_queued_jobs")); raise SystemExit(1 if busy else 0)' "${HEALTH_JSON}" || {
      echo "ERROR: tasks are active or queued; wait before rollback" >&2
      exit 1
    }
  elif [[ "${FORCE:-0}" != "1" ]]; then
    echo "ERROR: current health is unavailable; inspect it or use FORCE=1 for an emergency source rollback" >&2
    exit 1
  fi
fi

if [[ "${SERVICE_WAS_ACTIVE}" == "1" ]]; then
  systemctl stop "${SERVICE_NAME}"
fi
if [[ -f "${DATABASE_PATH}" && "${FORCE:-0}" != "1" ]]; then
  if ! python3 -c 'import sqlite3,sys; db=sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True); count=db.execute("SELECT count(*) FROM jobs WHERE status IN (?, ?)", ("queued", "running")).fetchone()[0]; db.close(); raise SystemExit(1 if count else 0)' "${DATABASE_PATH}"; then
    echo "ERROR: a task entered the queue; rollback was not switched" >&2
    if [[ "${SERVICE_WAS_ACTIVE}" == "1" ]]; then
      systemctl restart "${SERVICE_NAME}" || true
    fi
    exit 1
  fi
fi

SWITCHED=0
restore_original() {
  local restore_link="${APP_ROOT}/.current-restore-$$"
  ln -s "${ORIGINAL_DIR}" "${restore_link}" || true
  mv -Tf "${restore_link}" "${APP_ROOT}/current" || true
  if [[ -f "${ORIGINAL_DIR}/deploy/codex-deck.service" ]]; then
    install -m 0644 "${ORIGINAL_DIR}/deploy/codex-deck.service" \
      "${UNIT_FILE}" || true
  fi
  systemctl daemon-reload || true
  if [[ "${SERVICE_WAS_ACTIVE}" == "1" ]]; then
    systemctl restart "${SERVICE_NAME}" || true
  else
    systemctl stop "${SERVICE_NAME}" || true
  fi
  echo "Original release restored: ${ORIGINAL_DIR}" >&2
}
recover_switch_error() {
  local exit_status=$?
  trap - ERR
  if [[ "${SWITCHED}" == "1" ]]; then
    restore_original
  elif [[ "${SERVICE_WAS_ACTIVE}" == "1" ]]; then
    systemctl restart "${SERVICE_NAME}" || true
  fi
  exit "${exit_status}"
}
trap recover_switch_error ERR

NEXT_LINK="${APP_ROOT}/.current-rollback-$$"
ln -s "${TARGET_DIR}" "${NEXT_LINK}"
mv -Tf "${NEXT_LINK}" "${APP_ROOT}/current"
SWITCHED=1
install -m 0644 "${TARGET_DIR}/deploy/codex-deck.service" "${UNIT_FILE}"
systemctl daemon-reload
systemctl restart "${SERVICE_NAME}"
TARGET_OK=0
for ((_attempt = 1; _attempt <= 20; _attempt++)); do
  if HEALTH_JSON="$(curl --fail --silent --show-error --max-time 2 "http://127.0.0.1:${LOCAL_PORT}/api/health" 2>/dev/null)"; then
    if python3 -c 'import json,sys; h=json.loads(sys.argv[1]); raise SystemExit(0 if h.get("status")=="ok" and h.get("version")==sys.argv[2] else 1)' "${HEALTH_JSON}" "${TARGET_VERSION}"; then
      TARGET_OK=1
      break
    fi
  fi
  sleep 1
done
if [[ "${TARGET_OK}" != "1" ]]; then
  echo "ERROR: target release failed health/version validation" >&2
  trap - ERR
  restore_original
  exit 1
fi
SWITCHED=0
trap - ERR
echo "${HEALTH_JSON}"
echo
echo "Source rollback complete: ${TARGET_DIR}"
echo "Runtime data was not replaced. Restore a database backup only after separate inspection."
