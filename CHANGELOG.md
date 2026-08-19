# Changelog

## 2.21.1 — 2026-08-19

- Keep the index response Content-Security-Policy compatible with the documented
  Python 3.10+ runtime instead of relying on Python 3.12's relaxed f-string
  grammar.

## 2.21.0 — 2026-08-01

- Add a neutral `standalone` instance profile for portable single-VPS installs.
- Make the header portal optional through `CODEX_WEB_PORTAL_URL`; hide it when
  unconfigured.
- Remove personal repository, domain, Tailnet, and identity examples from the
  shareable source tree.
- Add a pinned, release-based Ubuntu/systemd deployment with queue gating,
  per-release venv and bundled CLI, SQLite online backup, health verification,
  and automatic source rollback.
- Add human deployment, AI deployment, operations, architecture, and private
  sharing guides.
- Add a clean-tree source archive builder with SHA-256 output and common
  credential/path marker checks.
