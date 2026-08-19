# Codex Deck

Codex Deck 把官方 Codex CLI/app-server 包装成一个适合电脑、手机和 iPad 的
持久化 Web 界面。任务在 VPS 上继续运行，浏览器刷新或临时断网不会丢失对话。

当前源码版本：`v2.21.1`。

## 给朋友部署

- 想直接交给 AI：阅读 [`docs/AI_DEPLOYMENT.md`](docs/AI_DEPLOYMENT.md)。
- 想自己操作：阅读 [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md)。
- 想了解更新、回滚和排错：阅读 [`docs/OPERATIONS.md`](docs/OPERATIONS.md)。
- 想发送源码压缩包或私库：阅读 [`docs/SHARING.md`](docs/SHARING.md)。
- 想快速理解代码与数据流：阅读 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。

最短路径以 Ubuntu 24.04、systemd、root 和一台专用测试 VPS 为基准。默认：

- Web 服务只监听 `127.0.0.1:8788`；
- 通过 SSH 隧道访问，手机/iPad 可选 Tailscale Serve；
- 写任务拥有整台 VPS 的完全权限，工作区根目录为 `/`；
- 同时运行两个互不冲突的任务；
- 数据、上传、Owner Token 和 Codex 登录态全部留在朋友自己的 VPS。

> 这是个人探索版的高权限配置。不要把 `127.0.0.1` 改成 `0.0.0.0`，也不要
> 在没有额外确认的情况下公开到互联网。朋友必须使用自己的 OpenAI/Codex
> 账户登录；你的 `.codex/auth.json`、额度、Token 和历史对话都不在分享包中。

## Technical reference

## Trusted proxy SSO

The browser UI can use identity headers injected by a local reverse proxy while
the existing bearer-token and device-session API paths remain available.
Trusted SSO is disabled by default and requires all of these settings:

```ini
CODEX_WEB_TRUSTED_SSO_ENABLED=true
CODEX_WEB_TRUSTED_SSO_HOST=codex.example.com
CODEX_WEB_TRUSTED_SSO_ORIGINS=https://codex.example.com
CODEX_WEB_TRUSTED_SSO_MAP_PATH=/etc/codex-web-sso-map.json
```

The JSON map binds identities to actors already present in the Codex Web
database:

```json
{
  "users": {"alice": "owner"},
  "emails": {"alice@example.test": "owner"}
}
```

Headers are accepted only from a loopback peer and only for the configured
host. SSO-authenticated writes also require an exact matching `Origin`.

Mobile-friendly Web UI and persistent API for the official Codex CLI.
The bind address and port are runtime settings. The portable deployment uses
`127.0.0.1:8788` by default.

## What is persisted

- Every Web chat is stored in SQLite.
- A chat keeps the exact Codex thread ID returned by the official Codex
  app-server.
- The first turn starts a persistent app-server thread; later turns resume the
  same thread. A legacy `codex exec` adapter remains available as a
  pre-turn startup fallback.
- User messages, assistant replies, task states, project, and permission mode
  survive a browser refresh and a service restart.
- Uploaded attachments are stored outside SQLite under the configured upload
  root; messages keep their ordered attachment metadata
  in SQLite, so image previews and file downloads survive refreshes and
  restarts.
- The composer accepts normal files from the file picker. Pasting a clipboard
  screenshot into the task box uploads it immediately and shows a removable
  preview before the task is sent. PNG, JPEG, and WebP files are also passed to
  Codex as image inputs; other files are copied into a private, temporary
  directory inside the selected workspace so Codex can read them by path.
- User messages, chats, and quick-feedback entries retain the authenticated
  actor identity that created them.
- The floating feedback control saves ideas to one VPS-side inbox without
  changing the current chat, URL, scroll position, or Codex job.
- Open chats fetch only messages newer than the last visible message. This
  keeps multiple authorized devices in sync without replacing the page, composer draft, or scroll
  position.
- Chat and feedback removal is reversible: archive and recently-deleted states
  keep the underlying records in SQLite until a future retention policy is
  introduced.
- Chat titles, pin state, and custom categories are stored in SQLite. The
  desktop conversation sidebar stays docked by default and remembers whether
  the current browser has collapsed it; mobile keeps the overlay drawer.
- Opening a saved chat waits for the page layout to settle and then positions
  the browser at the newest messages instead of the beginning of the history.
- Conversations show a compact message rail at the far-left edge of the active
  app area. With the desktop sidebar docked it stays attached to the sidebar
  edge instead of following the centered transcript. The whole rail is an
  independent touch target: finger-down gives immediate visual feedback, a
  7-pixel threshold separates taps from drags, and dragging continuously maps
  the finger position to the transcript through one animation-frame writer,
  light input smoothing, reversal hysteresis, and a per-frame travel cap before
  a short release settle capped below one viewport. This prevents a noisy touch
  coordinate from becoming a multi-screen jump that mobile Safari cannot paint
  in time.
  Visual Viewport height changes and delayed auto-follow work are deferred for
  the duration of the gesture, so mobile browser chrome and streaming updates
  cannot fight the user's scroll. Its
  compact preview shows the message number, speaker, and one-line summary while
  the actual conversation title remains stable. The outline and background stay
  transparent until scrubbing, the
  former center line is removed, and narrow layouts no longer reserve a wide
  content gutter. Hovering, focusing, and clicking remain available on
  desktop. The active marker grows once and remains stable; child transition
  events cannot trigger a rail rebuild loop.
- On touch devices, side-chat and annotation inputs retain a 16-pixel font to
  avoid focus zoom. New and reopened side chats focus the prompt directly from
  the initiating tap, while a Visual Viewport adapter keeps the side panel and
  composer inside the keyboard-visible area and restores the page position when
  the overlay closes.
- Queued and running tasks display a live `mm:ss` / `h:mm:ss` elapsed timer.
  While the current task is active, the send arrow becomes a stop control.
  Cancelling removes a queued job or terminates the live Codex process group,
  preserves any partial output, records an explicit stopped result, and
  releases the scheduler's workspace lock.
- Running assistant replies use a same-origin SSE channel. The official
  app-server's `item/agentMessage/delta` notifications are normalized by a
  separate runtime adapter, so main chats and side chats can display text
  before the turn completes. Reconnects receive a full in-memory snapshot,
  normal job polling remains the terminal fallback, and only the final
  reconciled message is written to SQLite and rendered as Markdown.
- The composer uses a Codex-style `Model` / `Effort` / `Speed` menu. A main
  chat can switch all three values per task; the exact combination is
  atomically snapshotted on the queued job and becomes that chat's next
  default. Resume, retry, and missing-session recovery keep the original
  combination. Side chats inherit it from their parent.
- Text selected from a normal rendered Codex reply can be saved as one of up to
  12 numbered sentence-level annotations. The reply, exact UTF-16 text anchor,
  annotation, and eventual user response remain stored in SQLite and are
  restored after refresh. Draft annotations can be edited or removed one at a
  time, and desktop pointer selection plus mobile `selectionchange` are both
  supported.
- `More details` sends an immediate one-click read of the selected sentence.
  `Ask in side chat` creates a separate, always-read-only Codex conversation.
  Side chats are stored beneath their parent chat, indented, and collapsed by
  default. Their jobs, request retries, polling state, pagination, and long
  reply loading are independent from the main chat.
- Feedback records support status, priority, text search, identity filtering,
  and newest/priority sorting.
- Long assistant replies are stored once in SQLite. List/chat responses include
  only a preview; the browser fetches the full content in bounded chunks.
- The durable scheduler runs up to two Codex jobs concurrently by default.
  Different workspaces can run in parallel. Read-only jobs may share a
  workspace, while any workspace-write job remains exclusive for that
  workspace to prevent concurrent file changes.

The legacy synchronous endpoint remains available for existing Shortcuts. It
does not share a Web chat unless the caller uses the chat endpoints.

## Dedicated unrestricted VPS mode

For a dedicated, externally isolated VPS, writable chats can deliberately run
without Codex approvals or sandbox boundaries:

```ini
CODEX_WEB_UNRESTRICTED_WRITE=true
```

When enabled, every new and resumed writable app-server turn uses the
`dangerFullAccess` sandbox with approvals disabled. The legacy exec fallback
continues to use `--dangerously-bypass-approvals-and-sandbox` together with
`--ignore-rules`. Read-only chats and side chats remain read-only. The browser
labels writable chats as `完全权限`, and the health response exposes
`unrestricted_write: true` for deployment verification.

This mode does not elevate the Unix account that runs the service. If the
service account needs host-administrator authority, grant that separately and
keep the web entry point private. Do not publish an unrestricted instance to
the public internet.

Legacy mode can load its owner token from a private file:

```ini
CODEX_WEB_API_TOKEN_FILE=/var/lib/codex-deck/api-token
```

## Version

The current source version is `v2.21.1`. The version is shown beside the Codex
Deck title, in the HTTP `Server` response header, and in the health-check JSON.
The sticky header can include a link back to another portal when
`CODEX_WEB_PORTAL_URL` is configured; otherwise that button is hidden.

`CODEX_WEB_INSTANCE_ID` defaults to `standalone`. Legacy multi-instance
deployments may select `hostinger` or `ubuntu-vps`, and
`CODEX_WEB_INSTANCE_SWITCH_URL` supplies the HTTPS peer origin opened by the
server mark.
Each authenticated `/api/instance` response exposes the Deck version and a
release fingerprint calculated from the three runtime source files. The page
checks the configured peer origin with the browser's existing credentials and
shows a warning when versions differ, when the version matches but the source
bundle does not, or when the other instance reports a degraded local state.
An unreachable peer is shown only as an unknown check and never degrades the
local `/health` endpoint.

The default runtime is the official app-server adapter:

```ini
CODEX_WEB_RUNTIME=app-server
```

Install the pinned SDK into the Python environment used by the service:

```bash
python3 -m pip install -r requirements.txt
```

Set `CODEX_WEB_RUNTIME=exec` only for rollback or diagnosis.

Set `CODEX_WEB_MAX_CONCURRENT_JOBS` to tune the scheduler between 1 and 8
workers. The production default is 2, matching the current VPS CPU count.

The model picker defaults to the current Codex GPT-5.6 family:

```ini
CODEX_WEB_ALLOWED_MODELS=gpt-5.6-sol,gpt-5.6-terra,gpt-5.6-luna
CODEX_WEB_DEFAULT_MODEL=gpt-5.6-sol
```

The default must be present in the allowlist. The server passes the selected
value to `codex exec --model` / `codex exec resume --model`. Reasoning effort
is passed through `model_reasoning_effort`; Standard explicitly uses the
default service tier and Fast enables Codex fast mode with the priority service
tier.

An instance can also force one supported reasoning effort and speed. When set,
the model API publishes only those choices and every write endpoint rejects a
conflicting override; unset variables preserve the normal picker behavior:

```ini
CODEX_WEB_FORCED_REASONING_EFFORT=max
CODEX_WEB_FORCED_SPEED=fast
```

Attachment defaults can be tuned without changing source:

```ini
CODEX_WEB_MAX_ATTACHMENTS_PER_MESSAGE=8
CODEX_WEB_MAX_ATTACHMENT_BYTES=20971520
CODEX_WEB_MAX_ATTACHMENT_TOTAL_BYTES=52428800
CODEX_WEB_ATTACHMENT_DRAFT_TTL_SECONDS=86400
CODEX_WEB_ATTACHMENT_CLEANUP_INTERVAL_SECONDS=3600
```

Unsent uploads expire after 24 hours by default. Every execution uses a
server-created staging directory and verifies the stored size and SHA-256
before Codex receives the file. The staging copy is removed when the job
finishes; a conservative periodic cleanup removes abandoned staging
directories after their TTL.

## Optional LifeOS turn envelope

The LifeOS bridge is disabled by default. To expose the source turn to a
LifeOS-aware Codex/MCP process, set an absolute private runtime directory:

```ini
CODEX_WEB_LIFEOS_TURN_ENVELOPE_ROOT=/var/lib/codex-root/lifeos-turn-envelopes
```

For each asynchronous chat job, Codex Deck writes one schema-v1 JSON envelope,
passes its absolute path only to that Codex child as
`LIFEOS_TURN_ENVELOPE_PATH`, reuses it for missing-session recovery, and
removes it after completion, failure, or cancellation. Startup also removes
stale per-job envelopes.

The root's real parent must already exist and must not be writable by group or
other users. If the root already exists, it must be a real directory owned by
the service user. Deck enforces private permissions and a 256 KiB envelope
limit. The directory must be dedicated to one Deck process; use a separate root
for each instance. Leave the setting unset to keep the bridge disabled. The
legacy synchronous `/api/run` endpoint never receives an envelope.

## Authentication

Authentication mode defaults to `legacy`. A dedicated VPS that is reachable
only through Tailscale Serve can instead make the Tailnet its sole browser
access boundary:

```ini
CODEX_WEB_HOST=127.0.0.1
CODEX_WEB_AUTH_MODE=tailnet-owner
CODEX_WEB_PUBLIC_URL=https://ubuntu-vps.example.ts.net
CODEX_WEB_TAILNET_OWNER_HOST=ubuntu-vps.example.ts.net
CODEX_WEB_TAILNET_OWNER_ORIGINS=https://ubuntu-vps.example.ts.net,http://127.0.0.1:18787
```

In `tailnet-owner` mode, the backend accepts Owner identity only when the peer
is loopback and either:

- Tailscale Serve preserved the exact configured host and injected a
  non-empty `Tailscale-User-Login` identity header; or
- the browser is using an explicit localhost SSH-tunnel fallback.

Every Owner write also requires an exact allowed `Origin`. The login, pairing,
logout, and device-management endpoints return `404`; Bearer headers and old
device cookies are ignored; the browser hides all login UI and starts at
`已连接 · 我`. Existing token/session rows may remain on disk only as rollback
material and are inert while this mode is enabled.

Keep the listener on loopback and use Tailscale Serve rather than Funnel. A
public reverse proxy must never be placed in front of an unrestricted
`tailnet-owner` deployment.

In the default `legacy` mode, API clients may authenticate with:

```text
Authorization: Bearer <token>
```

The preferred browser flow uses a 10-minute, one-time pairing code. An already
authenticated device calls `POST /api/pairings`, sends the returned fragment
link to the new phone, and the phone exchanges the code at
`POST /api/auth/pair`. Pairing codes use 100 bits of randomness, are stored only
as keyed HMAC-SHA256 values, and are consumed atomically. The fragment is
removed from the browser address bar before any API request.

Each browser or installed PWA receives an independent random device session and
a host-only `HttpOnly`, `Secure`, `SameSite=Lax` cookie. SQLite stores only the
session hash. Sessions last 365 days and renew in place during their final
30 days, without rotating the browser token. Up to 8 active devices are
allowed by default; reaching the limit returns `409` and never silently evicts
another phone. Device sessions can be listed, renamed, individually revoked,
or logged out.

The original Bearer Token exchange at `POST /api/auth/session` remains as an
emergency bootstrap and compatibility path. The browser never keeps the raw
Bearer Token in Web Storage. All `/api/*` routes except `/api/health`,
`/api/auth/session`, and `/api/auth/pair` require either a valid device cookie
or Bearer Token. The token from `CODEX_WEB_API_TOKEN` remains the owner
identity. Extra tokens are stored as SHA-256 hashes in SQLite and map to
separate display identities.

Mobile/device defaults can be tuned without source changes:

```ini
CODEX_WEB_PUBLIC_URL=https://ubuntu-vps.example.ts.net
CODEX_WEB_DEVICE_SESSION_TTL_DAYS=365
CODEX_WEB_DEVICE_SESSION_RENEW_WINDOW_DAYS=30
CODEX_WEB_DEVICE_SESSION_TOUCH_SECONDS=21600
CODEX_WEB_MAX_DEVICE_SESSIONS=8
CODEX_WEB_PAIRING_CODE_TTL_SECONDS=600
```

`CODEX_WEB_PUBLIC_URL` is required when the management browser uses a local
SSH tunnel, because pairing links must still point to the phone-reachable
Tailnet HTTPS URL.

Create one additional token for a partner identity:

```sh
/opt/codex-deck/current/.venv/bin/python \
  /opt/codex-deck/current/codex_web.py --create-api-key partner Friend
```

The raw token is printed once. The command refuses to create another active key
for the same identity. Use `GET /api/me` to confirm which identity a token maps
to.

## Chat API

Create a chat:

```http
POST /api/chats
Content-Type: application/json

{
  "title": "检查项目",
  "project": ".",
  "mode": "write",
  "model": "gpt-5.6-sol",
  "reasoning_effort": "high",
  "speed": "standard"
}
```

Upload a file before sending the message:

```http
POST /api/attachments
Authorization: Bearer <token>
Content-Type: text/plain
X-File-Name: notes.txt

<raw file bytes>
```

The response contains an opaque attachment ID and safe display metadata, never
an absolute server path.

Send a message:

```http
POST /api/chats/<chat-id>/messages
Content-Type: application/json

{
  "prompt": "检查项目并运行测试",
  "model": "gpt-5.6-terra",
  "reasoning_effort": "ultra",
  "speed": "fast",
  "attachments": [
    "<attachment-id>"
  ],
  "client_request_id": "<stable-id-for-retry>"
}
```

Send sentence-level annotations with an optional main request:

```http
POST /api/chats/<chat-id>/messages
Content-Type: application/json

{
  "prompt": "请一起说明这些问题",
  "annotations": [
    {
      "source_message_id": 42,
      "quote": "数据库完整性：正常",
      "comment": "具体检查了哪些项目？",
      "start_offset": 0,
      "end_offset": 9,
      "action": "annotation"
    }
  ],
  "client_request_id": "<stable-id-for-retry>"
}
```

Offsets use JavaScript UTF-16 code units and bind to the server's normal
rendered-text projection. The server verifies the source message, exact quoted
range, and source projection before enqueueing the task. Selection inside the
optional raw/full-text view is intentionally disabled because its Markdown
offsets do not match the normal rendered transcript.

Create an independent read-only side chat:

```http
POST /api/chats/<parent-chat-id>/side-chats
Content-Type: application/json

{
  "source_message_id": 42,
  "quote": "数据库完整性：正常",
  "start_offset": 0,
  "end_offset": 9,
  "question": "这里的正常判定边界是什么？",
  "client_request_id": "<required-stable-id>"
}
```

The server snapshots the verified source context on the first request and
compares a full request fingerprint on retries. A side chat is permanently
`read` even if its parent is writable. It cannot be independently pinned,
categorized, archived, or deleted; archive/delete/restore follows the parent.

The server returns `202 Accepted` immediately. Poll
`GET /api/jobs/<job-id>` until the state becomes `completed` or `failed`.

Other routes:

- `GET /api/me`
- `POST /api/auth/session` for emergency Bearer-to-device bootstrap
- `POST /api/auth/pair` to redeem a one-time pairing code
- `POST /api/auth/logout`
- `POST /api/pairings` to create a one-time pairing code
- `GET /api/devices`
- `POST /api/devices/<device-id>/rename`
- `POST /api/devices/<device-id>/revoke`
- `POST /api/devices/revoke-others`
- `GET /api/models`
- `POST /api/attachments` with raw request bytes, `Content-Type`, and
  `X-File-Name`
- `GET /api/attachments/<attachment-id>/content`
- `POST /api/attachments/<attachment-id>/discard` for an unsent upload
- `GET /api/chats`
- `GET /api/chats/<chat-id>?limit=24&before=<message-id>`
- `GET /api/chats/<chat-id>/updates?after=<message-id>&limit=100`
- `POST /api/chats/<chat-id>/update` with one or more of `title`, `category`,
  and `pinned`
- `POST /api/chats/<chat-id>/archive`
- `POST /api/chats/<chat-id>/delete`
- `POST /api/chats/<chat-id>/restore`
- `POST /api/chats/<parent-chat-id>/side-chats`
- `GET /api/messages/<message-id>/content?offset=0&limit=32768`
- `GET /api/messages/<message-id>/download` for an authenticated plain-text download
- `GET /api/feedback?view=inbox&actor=owner&q=<text>&sort=priority`
- `GET /api/projects`
- `GET /api/health`

Both `/health` and `/api/health` are unauthenticated health-check routes. They
return the application version together with database and worker status.

`mode` is `write` (`workspace-write`) or `read` (`read-only`). Project and mode
are fixed once a chat has started. A main chat may select a different allowed
`model`, `reasoning_effort`, and `speed` for each new task; the job stores the
complete configuration and ordered attachment fingerprint independently so a
later picker change or retry cannot alter an already queued task.

## Feedback API

Create a quick-feedback entry:

```http
POST /api/feedback
Content-Type: application/json

{
  "content": "希望增加一个更快的筛选入口",
  "chat_id": "<optional-current-chat-id>",
  "page_path": "/codex/",
  "client_request_id": "<stable-id-for-retry>"
}
```

The server records the authenticated identity, application version, timestamp,
status, and optional current chat. Repeating the same `client_request_id` for
the same identity returns the existing entry instead of creating a duplicate.

Feedback management routes:

- `POST /api/feedback/<id>/update` with `status` (`pending`, `planned`, or
  `completed`) and/or `priority` (`normal`, `important`, or `urgent`)
- `POST /api/feedback/<id>/archive`
- `POST /api/feedback/<id>/delete`
- `POST /api/feedback/<id>/restore`

Feedback views are `inbox`, `planned`, `completed`, `archived`, and `deleted`.

## Legacy Shortcuts API

```http
POST /api/run
Content-Type: application/json

{
  "prompt": "检查项目并运行测试",
  "project": ".",
  "mode": "write",
  "model": "gpt-5.6-sol",
  "reasoning_effort": "medium",
  "speed": "standard"
}
```

This endpoint waits synchronously and creates an ephemeral Codex task. It is
kept for compatibility; the Web UI uses the asynchronous chat API.

## Runtime data

Portable deployment defaults:

- Application: `/opt/codex-deck/current/codex_web.py`
- Environment: `/etc/codex-deck.env`
- SQLite: `/var/lib/codex-deck/codex.sqlite3`
- Uploaded attachments: `/var/lib/codex-deck/uploads`
- Codex home/session data: `/root/.codex`
- Workspace root: `/`

Back up the SQLite database, uploaded attachments, and Codex home together if
chat/session continuity matters. SQLite runs in WAL mode, so use SQLite's
backup mechanism or stop the service before copying database files.
