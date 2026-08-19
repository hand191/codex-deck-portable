#!/usr/bin/env bash
set -u

SERVICE_NAME="codex-deck.service"
ENV_FILE="/etc/codex-deck.env"
LOCAL_PORT="$(sed -n 's/^CODEX_WEB_PORT=//p' "${ENV_FILE}" 2>/dev/null | tail -n 1)"
[[ "${LOCAL_PORT}" =~ ^[0-9]+$ ]] || LOCAL_PORT="8788"

echo "== service =="
systemctl status "${SERVICE_NAME}" --no-pager --lines=12 || true
echo
echo "== local health =="
curl --silent --show-error --max-time 5 "http://127.0.0.1:${LOCAL_PORT}/api/health" || true
echo
echo
echo "== versions =="
if [[ -x /opt/codex-deck/current/.venv/bin/python && -f /opt/codex-deck/current/codex_web.py ]]; then
  /opt/codex-deck/current/.venv/bin/python -c 'import pathlib,re; text=pathlib.Path("/opt/codex-deck/current/codex_web.py").read_text(); print("Deck", re.search(r"^APP_VERSION = \"([^\"]+)\"", text, re.M).group(1))'
fi
CODEX_BIN_PATH="$(sed -n 's/^CODEX_BIN=//p' "${ENV_FILE}" 2>/dev/null | tail -n 1)"
CODEX_HOME_PATH="$(sed -n 's/^CODEX_HOME=//p' "${ENV_FILE}" 2>/dev/null | tail -n 1)"
if [[ -n "${CODEX_BIN_PATH}" && -x "${CODEX_BIN_PATH}" ]]; then
  "${CODEX_BIN_PATH}" --version || true
  CODEX_HOME="${CODEX_HOME_PATH:-/root/.codex}" "${CODEX_BIN_PATH}" login status || true
fi
echo
echo "== recent logs =="
journalctl -u "${SERVICE_NAME}" --no-pager -n 40 || true
