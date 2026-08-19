# Codex Deck contributor guide

This repository is a personal exploration project that wraps the official
Codex CLI/app-server in a persistent mobile-friendly web UI. Prefer the
shortest understandable implementation over production infrastructure.

## Supported deployment

- Linux VPS with systemd; the documented path targets Ubuntu 24.04.
- Python 3.10 or newer.
- The official `codex` CLI, authenticated by the VPS owner.
- The server stays on loopback unless the owner explicitly chooses a private
  Tailscale Serve endpoint or approves public exposure.

## Before changing code

1. Read `README.md`, `docs/DEPLOYMENT.md`, and the relevant source/tests.
2. Inspect `git status`; preserve unrelated user changes.
3. Never copy or commit `.env`, API tokens, `.codex/auth.json`, SQLite files,
   uploads, workspaces, logs, or backups.
4. Do not deploy, publish a domain, change firewall rules, or replace runtime
   data unless the user explicitly asks.

## Verification

Run the smallest relevant checks while iterating, then before a shareable
release run:

```bash
python3 -m py_compile codex_web.py codex_runtime.py job_stream.py
python3 -m unittest -q test_codex_sso.py
bash -n deploy/*.sh
```

## Release rules

- Keep `APP_VERSION`, README version text, and version assertions synchronized.
- A source release and a VPS deployment are separate facts; never claim a VPS
  was upgraded only because source was committed.
- The portable default is `CODEX_WEB_INSTANCE_ID=standalone`.
- Personal portal and peer-instance URLs belong in environment configuration,
  never in source defaults.
- Prefer a clean `git archive` or fresh repository when sharing; do not include
  the private repository's historical deployment domains.
