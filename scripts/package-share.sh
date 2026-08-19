#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${1:-${REPO_ROOT}/dist}"

cd "${REPO_ROOT}"
git diff --quiet && git diff --cached --quiet \
  || { echo "ERROR: commit or discard local changes before packaging" >&2; exit 1; }

for forbidden in \
  '/Users/' \
  'BEGIN OPENSSH PRIVATE KEY' \
  'BEGIN PRIVATE KEY' \
  '"access_token"'; do
  if git grep -I -n -F "${forbidden}" HEAD -- . \
    ':(exclude)scripts/package-share.sh' >/dev/null 2>&1; then
    echo "ERROR: tracked source still contains forbidden share marker: ${forbidden}" >&2
    exit 1
  fi
done
if git grep -I -n -i -E \
  '01[0-9a-f]{6}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}' \
  HEAD -- . ':(exclude)scripts/package-share.sh' >/dev/null 2>&1; then
  echo "ERROR: tracked source contains a UUIDv7-like session identifier" >&2
  exit 1
fi

VERSION="$(git show HEAD:codex_web.py | sed -n 's/^APP_VERSION = "\([^"]*\)"/\1/p')"
[[ -n "${VERSION}" ]] || { echo "ERROR: APP_VERSION not found" >&2; exit 1; }

mkdir -p "${OUTPUT_DIR}"
ARCHIVE="${OUTPUT_DIR}/Codex-Deck-v${VERSION}-source.zip"
CHECKSUM="${ARCHIVE}.sha256"
[[ ! -e "${ARCHIVE}" && ! -e "${CHECKSUM}" ]] \
  || { echo "ERROR: output already exists: ${ARCHIVE}" >&2; exit 1; }

git archive --format=zip --prefix="codex-deck-v${VERSION}/" \
  --output="${ARCHIVE}" HEAD
if command -v sha256sum >/dev/null 2>&1; then
  (cd "${OUTPUT_DIR}" && sha256sum "$(basename "${ARCHIVE}")") > "${CHECKSUM}"
else
  (cd "${OUTPUT_DIR}" && shasum -a 256 "$(basename "${ARCHIVE}")") > "${CHECKSUM}"
fi

echo "${ARCHIVE}"
echo "${CHECKSUM}"
