import importlib.util
import io
import json
import os
import queue
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from unittest import mock
from email.message import Message
from pathlib import Path


class FakeHandler:
    def __init__(self, headers, peer="127.0.0.1"):
        self.headers = Message()
        for name, value in headers.items():
            self.headers[name] = value
        self.client_address = (peer, 12345)


class SinkStdin:
    def write(self, _value):
        return None

    def close(self):
        return None


class FinishedProcessStub:
    pid = 4242

    def __init__(self):
        self.stdin = SinkStdin()
        self.stdout = ()

    def wait(self, timeout=None):
        return 0

    def poll(self):
        return 0


class TrustedSsoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        root = Path(cls.tempdir.name)
        cls.root = root
        mapping = root / "sso-map.json"
        mapping.write_text(
            json.dumps(
                {
                    "users": {"alice": "owner"},
                    "emails": {"alice@example.test": "owner"},
                }
            ),
            encoding="utf-8",
        )
        os.environ.update(
            {
                "CODEX_WEB_API_TOKEN": "test-api-token-long-enough",
                "CODEX_WEB_AUTH_MODE": "legacy",
                "CODEX_WEB_DB_PATH": str(root / "codex.sqlite3"),
                "CODEX_WEB_UPLOAD_ROOT": str(root / "uploads"),
                "CODEX_WORKSPACE_ROOT": str(root / "workspaces"),
                "CODEX_WEB_TRUSTED_SSO_ENABLED": "true",
                "CODEX_WEB_TRUSTED_SSO_HOST": "codex.example.test",
                "CODEX_WEB_TRUSTED_SSO_ORIGINS": "https://codex.example.test",
                "CODEX_WEB_TRUSTED_SSO_MAP_PATH": str(mapping),
                "CODEX_WEB_PUBLIC_URL": "https://deck-vps.example.ts.net",
                "CODEX_WEB_COOKIE_PATH": "/",
                "CODEX_WEB_COOKIE_SECURE": "1",
                "CODEX_WEB_DEVICE_SESSION_TTL_DAYS": "365",
                "CODEX_WEB_DEVICE_SESSION_RENEW_WINDOW_DAYS": "30",
                "CODEX_WEB_DEVICE_SESSION_TOUCH_SECONDS": "21600",
                "CODEX_WEB_MAX_DEVICE_SESSIONS": "8",
                "CODEX_WEB_PAIRING_CODE_TTL_SECONDS": "600",
                "CODEX_WEB_INSTANCE_ID": "standalone",
                "CODEX_WEB_RUNTIME": "exec",
                "CODEX_WEB_LIFEOS_TURN_ENVELOPE_ROOT": "",
            }
        )
        spec = importlib.util.spec_from_file_location(
            "codex_web_under_test",
            Path(__file__).with_name("codex_web.py"),
        )
        cls.app = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.app)
        cls.app.initialize_database(recover_jobs=False)

    @classmethod
    def tearDownClass(cls):
        cls.tempdir.cleanup()

    def setUp(self):
        with self.app.db_connect() as connection:
            connection.execute("DELETE FROM pairing_codes")
            connection.execute("DELETE FROM device_sessions")

    def lifeos_turn_root(self, label):
        root = self.root / f"lifeos-turns-{label}"
        self.app.shutil.rmtree(root, ignore_errors=True)
        self.addCleanup(
            self.app.shutil.rmtree,
            root,
            ignore_errors=True,
        )
        return root

    @contextmanager
    def configured_lifeos_root(self, label):
        root = self.lifeos_turn_root(label)
        with mock.patch.object(
            self.app,
            "LIFEOS_TURN_ENVELOPE_ROOT",
            root,
        ):
            yield root

    def enqueue_test_job(
        self,
        label,
        *,
        mode="write",
        prompt=None,
        message_content=None,
        message_meta=None,
        attachment_ids=None,
    ):
        chat = self.app.create_chat(
            f"LifeOS {label}",
            ".",
            mode,
            "owner",
        )
        self.addCleanup(self.cleanup_lifeos_test_chat, chat["id"])
        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            queued = self.app.enqueue_message(
                chat["id"],
                prompt or f"LifeOS execution prompt {label}",
                "owner",
                f"lifeos-{label}-request",
                message_content=message_content,
                message_meta=message_meta,
                attachment_ids=attachment_ids,
            )
        return chat, queued

    def cleanup_lifeos_test_chat(self, chat_id):
        attachment_paths = []
        job_ids = []
        with self.app.db_connect() as connection:
            rows = connection.execute(
                """
                SELECT a.*
                FROM attachments a
                JOIN messages m ON m.id = a.message_id
                WHERE m.chat_id = ?
                """,
                (chat_id,),
            ).fetchall()
            attachment_paths = [
                self.app.attachment_storage_path(row)
                for row in rows
            ]
            job_ids = [
                row["id"]
                for row in connection.execute(
                    "SELECT id FROM jobs WHERE chat_id = ?",
                    (chat_id,),
                ).fetchall()
            ]
            connection.execute(
                "DELETE FROM chats WHERE id = ?",
                (chat_id,),
            )
        for path in attachment_paths:
            path.unlink(missing_ok=True)
        for job_id in job_ids:
            self.app.release_job_cancel_event(job_id)

    def codex_result(self, **overrides):
        result = {
            "ok": True,
            "cancelled": False,
            "returncode": 0,
            "output": "LifeOS 测试完成",
            "thread_id": None,
            "error": "",
            "duration_seconds": 0.1,
        }
        result.update(overrides)
        return result

    def test_maps_trusted_proxy_identity_to_existing_actor(self):
        handler = FakeHandler(
            {
                "Host": "codex.example.test",
                "X-Forwarded-Host": "codex.example.test",
                "Remote-User": "alice",
                "Remote-Email": "alice@example.test",
            }
        )
        actor = self.app.authenticate_trusted_sso(handler)
        self.assertEqual(actor["id"], "owner")
        self.assertEqual(actor["auth_type"], "sso")

    def test_api_token_can_be_loaded_from_a_private_file(self):
        token_path = self.root / "api-token"
        token_path.write_text("private-file-token\n", encoding="utf-8")
        with mock.patch.dict(
            os.environ,
            {
                "CODEX_WEB_API_TOKEN": "",
                "CODEX_WEB_API_TOKEN_FILE": str(token_path),
            },
        ):
            self.assertEqual(
                self.app.load_api_token(),
                "private-file-token",
            )

    def test_tailnet_owner_mode_can_start_without_api_token(self):
        with (
            mock.patch.object(self.app, "TAILNET_OWNER_MODE", True),
            mock.patch.dict(
                os.environ,
                {
                    "CODEX_WEB_API_TOKEN": "",
                    "CODEX_WEB_API_TOKEN_FILE": "",
                },
            ),
        ):
            self.assertEqual(self.app.load_api_token(), "")

    def test_rejects_wrong_host_or_non_proxy_peer(self):
        wrong_host = FakeHandler(
            {
                "Host": "api.example.test",
                "Remote-User": "alice",
                "Remote-Email": "alice@example.test",
            }
        )
        self.assertIsNone(self.app.authenticate_trusted_sso(wrong_host))
        external_peer = FakeHandler(
            {
                "Host": "codex.example.test",
                "Remote-User": "alice",
                "Remote-Email": "alice@example.test",
            },
            peer="203.0.113.5",
        )
        self.assertIsNone(self.app.authenticate_trusted_sso(external_peer))

    def test_sso_writes_require_exact_origin(self):
        actor = {"auth_type": "sso"}
        accepted = FakeHandler({"Origin": "https://codex.example.test"})
        missing = FakeHandler({})
        foreign = FakeHandler({"Origin": "https://evil.example"})
        self.assertTrue(self.app.sso_origin_allowed(accepted, actor))
        self.assertFalse(self.app.sso_origin_allowed(missing, actor))
        self.assertFalse(self.app.sso_origin_allowed(foreign, actor))
        self.assertTrue(
            self.app.sso_origin_allowed(missing, {"auth_type": "api_token"})
        )

    def test_tailnet_owner_requires_serve_identity_or_local_tunnel(self):
        serve = FakeHandler(
            {
                "Host": "deck-vps.example.ts.net",
                "Tailscale-User-Login": "owner@example.test",
            }
        )
        tunnel = FakeHandler({"Host": "127.0.0.1:18787"})
        missing_identity = FakeHandler(
            {
                "Host": "deck-vps.example.ts.net",
                "Authorization": "Bearer test-api-token-long-enough",
            }
        )
        external = FakeHandler(
            {
                "Host": "deck-vps.example.ts.net",
                "Tailscale-User-Login": "owner@example.test",
            },
            peer="203.0.113.7",
        )
        with (
            mock.patch.object(self.app, "TAILNET_OWNER_MODE", True),
            mock.patch.object(
                self.app,
                "TAILNET_OWNER_HOST",
                "deck-vps.example.ts.net",
            ),
        ):
            for handler in (serve, tunnel):
                actor = self.app.authenticate(handler)
                self.assertEqual(actor["id"], "owner")
                self.assertEqual(actor["auth_type"], "tailnet_owner")
                self.assertEqual(actor["key_id"], "tailnet")
            self.assertIsNone(self.app.authenticate(missing_identity))
            self.assertIsNone(self.app.authenticate(external))

    def test_tailnet_owner_writes_require_an_exact_allowed_origin(self):
        actor = {"auth_type": "tailnet_owner"}
        allowed = FakeHandler(
            {"Origin": "https://deck-vps.example.ts.net"}
        )
        tunnel = FakeHandler({"Origin": "http://127.0.0.1:18787"})
        missing = FakeHandler({})
        foreign = FakeHandler({"Origin": "https://evil.example"})
        with mock.patch.object(
            self.app,
            "TAILNET_OWNER_ORIGINS",
            {
                "https://deck-vps.example.ts.net",
                "http://127.0.0.1:18787",
            },
        ):
            self.assertTrue(self.app.sso_origin_allowed(allowed, actor))
            self.assertTrue(self.app.sso_origin_allowed(tunnel, actor))
            self.assertFalse(self.app.sso_origin_allowed(missing, actor))
            self.assertFalse(self.app.sso_origin_allowed(foreign, actor))

    def test_tailnet_owner_disables_legacy_auth_management_routes(self):
        paths = (
            "/api/auth/session",
            "/api/auth/pair",
            "/api/auth/logout",
            "/api/pairings",
            "/api/devices",
            "/api/devices/example/revoke",
        )
        with mock.patch.object(self.app, "TAILNET_OWNER_MODE", True):
            for path in paths:
                self.assertTrue(
                    self.app.auth_management_disabled(path),
                    path,
                )
            self.assertFalse(
                self.app.auth_management_disabled("/api/me")
            )
            self.assertFalse(
                self.app.auth_management_disabled("/api/chats")
            )
        self.assertFalse(
            self.app.auth_management_disabled("/api/auth/session")
        )

    def test_tailnet_owner_dashboard_is_connected_and_hides_login_ui(self):
        html = self.app.render_index_html(
            tailnet_owner_mode=True,
            unrestricted_write=True,
        )
        self.assertIn('<body class="tailnet-owner-mode">', html)
        self.assertIn('id="connectionText">已连接 · 我</span>', html)
        self.assertIn('id="settings" aria-label="连接设置" hidden', html)
        self.assertIn('id="modalBackdrop" hidden', html)
        self.assertIn('const tailnetOwnerMode = true;', html)
        self.assertIn('<option value="write">完全权限</option>', html)
        self.assertIn("if (tailnetOwnerMode) return;", html)
        self.assertIn(
            "const initialPairing = tailnetOwnerMode ? null : pairingFragment;",
            html,
        )
        self.assertNotIn("__TAILNET_OWNER_MODE__", html)

    def test_instance_switch_renders_both_server_variants(self):
        hostinger = self.app.render_index_html(
            instance_id="hostinger",
            instance_switch_url="https://deck-b.example.test",
        )
        self.assertIn(
            'class="mark instance-hostinger" '
            'href="https://deck-b.example.test"',
            hostinger,
        )
        self.assertIn('aria-label="切换到 Ubuntu VPS"', hostinger)

        ubuntu = self.app.render_index_html(
            tailnet_owner_mode=True,
            instance_id="ubuntu-vps",
            instance_switch_url="https://deck-a.example.test",
        )
        self.assertIn(
            'class="mark instance-ubuntu-vps" '
            'href="https://deck-a.example.test"',
            ubuntu,
        )
        self.assertIn('aria-label="切回 Hostinger VPS"', ubuntu)
        self.assertIn(".mark.instance-ubuntu-vps {", ubuntu)
        self.assertNotIn(".mark { display: none; }", ubuntu)

        standalone = self.app.render_index_html(
            instance_id="standalone",
            instance_switch_url="",
        )
        self.assertIn(
            'class="mark instance-standalone" href="/"',
            standalone,
        )
        self.assertIn('aria-label="Codex Deck"', standalone)

    def test_instance_switch_rejects_unsafe_configuration(self):
        for instance_id, switch_url in (
            ("other", "https://deck-a.example.test"),
            ("ubuntu-vps", "http://deck-a.example.test"),
            ("ubuntu-vps", "https://deck-a.example.test/path"),
        ):
            with self.subTest(
                instance_id=instance_id,
                switch_url=switch_url,
            ):
                with self.assertRaises(RuntimeError):
                    self.app.render_index_html(
                        instance_id=instance_id,
                        instance_switch_url=switch_url,
                    )

    def test_instance_payload_and_exact_peer_cors(self):
        switch = {
            "id": "hostinger",
            "class": "instance-hostinger",
            "label": "切换到 Ubuntu VPS",
            "url": "https://deck-b.example.test",
        }
        with mock.patch.object(self.app, "INSTANCE_SWITCH", switch):
            payload = self.app.instance_payload()
            allowed = self.app.instance_cors_headers(
                FakeHandler(
                    {
                        "Origin": "https://deck-b.example.test",
                    }
                )
            )
            denied = self.app.instance_cors_headers(
                FakeHandler({"Origin": "https://evil.example"})
            )
        self.assertEqual(payload["instance_id"], "hostinger")
        self.assertEqual(payload["deck_version"], "2.21.1")
        self.assertEqual(payload["release_id"], self.app.RELEASE_ID)
        self.assertIn(
            (
                "Access-Control-Allow-Origin",
                "https://deck-b.example.test",
            ),
            allowed,
        )
        self.assertEqual(denied, ())

    def test_job_stream_hub_isolates_jobs_and_replays_snapshot(self):
        hub = self.app.JobStreamHub(1000)
        hub.ensure("job-a", "queued")
        hub.ensure("job-b", "queued")
        hub.set_status("job-a", "running")
        hub.append("job-a", "第一句")
        hub.append("job-a", "第二句")
        snapshot = hub.snapshot("job-a")
        other = hub.snapshot("job-b")
        self.assertEqual(snapshot.text, "第一句第二句")
        self.assertGreaterEqual(snapshot.revision, 3)
        self.assertEqual(other.text, "")
        self.assertEqual(other.status, "queued")

    def test_job_sse_sends_snapshot_before_terminal(self):
        class SseHandler:
            def __init__(self):
                self.wfile = io.BytesIO()
                self.response_status = None
                self.response_headers = []
                self.close_connection = False

            def send_response(self, status):
                self.response_status = status

            def send_header(self, name, value):
                self.response_headers.append((name, value))

            def end_headers(self):
                return None

        hub = self.app.JobStreamHub(1000)
        hub.ensure("stream-job", "running")
        hub.append("stream-job", "提前看到的内容")
        hub.finish("stream-job", "completed")
        handler = SseHandler()
        with (
            mock.patch.object(self.app, "JOB_STREAMS", hub),
            mock.patch.object(
                self.app,
                "get_job",
                return_value={
                    "id": "stream-job",
                    "status": "completed",
                },
            ),
        ):
            self.app.serve_job_events(handler, "stream-job")
        body = handler.wfile.getvalue().decode("utf-8")
        self.assertEqual(handler.response_status, 200)
        self.assertIn(
            ("Content-Type", "text/event-stream; charset=utf-8"),
            handler.response_headers,
        )
        self.assertIn(("X-Accel-Buffering", "no"), handler.response_headers)
        self.assertLess(
            body.index("event: snapshot"),
            body.index("event: terminal"),
        )
        self.assertIn("提前看到的内容", body)

    def test_health_reports_the_auth_boundary_without_secrets(self):
        with (
            mock.patch.object(self.app, "AUTH_MODE", "tailnet-owner"),
            mock.patch.object(self.app, "TAILNET_OWNER_MODE", True),
        ):
            payload = self.app.health_payload()
        self.assertEqual(payload["auth_mode"], "tailnet-owner")
        self.assertFalse(payload["auth_required"])
        self.assertNotIn("token", payload)
        self.assertNotIn("tailnet_login", payload)

    def test_dashboard_hides_unconfigured_portal_and_renders_configured_one(self):
        self.assertIn(
            'class="icon-button portal-button" href="/" '
            'aria-label="返回统一主界面" title="返回统一主界面" hidden',
            self.app.INDEX_HTML,
        )
        configured = self.app.render_index_html(
            portal_url="https://portal.example.test/home/",
        )
        self.assertIn('href="https://portal.example.test/home"', configured)
        self.assertNotIn(
            'title="返回统一主界面" hidden',
            configured,
        )

    def test_dashboard_rejects_unsafe_portal_url(self):
        for portal_url in (
            "javascript:alert(1)",
            "https://user:secret@portal.example.test",
            "https://portal.example.test/?token=secret",
            "https://portal.example.test/#private",
        ):
            with self.subTest(portal_url=portal_url):
                with self.assertRaises(RuntimeError):
                    self.app.render_index_html(portal_url=portal_url)

    def test_chat_navigation_metadata_is_persisted_and_sorted(self):
        first = self.app.create_chat("普通对话", ".", "write", "owner")
        pinned = self.app.create_chat("原始标题", ".", "read", "owner")
        updated = self.app.update_chat_metadata(
            pinned["id"],
            "owner",
            {
                "title": "VPS 发布计划",
                "category": "运维",
                "pinned": True,
            },
        )
        self.assertEqual(updated["title"], "VPS 发布计划")
        self.assertEqual(updated["category"], "运维")
        self.assertIsNotNone(updated["pinned_at"])

        listing = self.app.list_chats("active")
        ids = [chat["id"] for chat in listing["chats"]]
        self.assertLess(ids.index(pinned["id"]), ids.index(first["id"]))
        self.assertIn("运维", listing["categories"])

    def test_chat_navigation_metadata_validation(self):
        chat = self.app.create_chat("待整理", ".", "write", "owner")
        with self.assertRaises(ValueError):
            self.app.update_chat_metadata(
                chat["id"], "owner", {"title": " " * 4}
            )
        with self.assertRaises(ValueError):
            self.app.update_chat_metadata(
                chat["id"], "owner", {"pinned": "yes"}
            )

    def test_parent_state_change_rejects_active_child_job_and_cascades(self):
        parent = self.app.create_chat("父对话状态", ".", "write", "owner")
        child_id = "state-child-" + parent["id"]
        timestamp = self.app.now_iso()
        with self.app.db_connect() as connection:
            connection.execute(
                """
                INSERT INTO chats(
                    id, title, project, mode, model, creator_actor_id,
                    parent_chat_id, created_at, updated_at
                )
                VALUES (?, '状态侧聊', '.', 'read', ?, 'owner', ?, ?, ?)
                """,
                (
                    child_id,
                    self.app.DEFAULT_MODEL,
                    parent["id"],
                    timestamp,
                    timestamp,
                ),
            )
            message_id = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, actor_id, created_at
                )
                VALUES (?, 'user', '正在处理', 'completed', '{}', 'owner', ?)
                """,
                (child_id, timestamp),
            ).lastrowid
            connection.execute(
                """
                INSERT INTO jobs(
                    id, chat_id, user_message_id, model,
                    prompt, status, created_at
                )
                VALUES (?, ?, ?, ?, '正在处理', 'queued', ?)
                """,
                (
                    "active-child-job-" + parent["id"],
                    child_id,
                    message_id,
                    self.app.DEFAULT_MODEL,
                    timestamp,
                ),
            )
        with self.assertRaises(RuntimeError):
            self.app.change_chat_state(parent["id"], "delete", "owner")
        self.assertIsNone(self.app.get_chat(parent["id"])["deleted_at"])
        self.assertIsNone(self.app.get_chat(child_id)["deleted_at"])
        with self.app.db_connect() as connection:
            connection.execute(
                """
                UPDATE jobs
                SET status = 'completed', completed_at = ?
                WHERE id = ?
                """,
                (self.app.now_iso(), "active-child-job-" + parent["id"]),
            )
        self.app.change_chat_state(parent["id"], "delete", "owner")
        self.assertIsNotNone(self.app.get_chat(parent["id"])["deleted_at"])
        self.assertIsNotNone(self.app.get_chat(child_id)["deleted_at"])
        self.app.change_chat_state(parent["id"], "restore", "owner")
        restored = next(
            chat
            for chat in self.app.list_chats("active")["chats"]
            if chat["id"] == parent["id"]
        )
        self.assertEqual(restored["side_chats"][0]["id"], child_id)

    def test_dashboard_exposes_conversation_organization_controls(self):
        self.assertIn('id="chatSearch"', self.app.INDEX_HTML)
        self.assertIn('id="chatCategoryFilter"', self.app.INDEX_HTML)
        self.assertIn('id="chatEditBackdrop"', self.app.INDEX_HTML)
        self.assertIn('id="chatPinnedInput"', self.app.INDEX_HTML)

    def test_chat_navigation_exposes_codex_session_id_for_copying(self):
        chat = self.app.create_chat(
            "复制 Session ID",
            ".",
            "write",
            "owner",
        )
        session_id = "00000000-0000-4000-8000-000000000001"
        with self.app.db_connect() as connection:
            connection.execute(
                "UPDATE chats SET codex_thread_id = ? WHERE id = ?",
                (session_id, chat["id"]),
            )
        listing = self.app.list_chats("active")
        listed = next(
            item for item in listing["chats"] if item["id"] == chat["id"]
        )
        self.assertEqual(listed["codex_thread_id"], session_id)
        self.assertEqual(
            self.app.get_chat(chat["id"])["codex_thread_id"],
            session_id,
        )

    def test_dashboard_uses_id_copy_instead_of_chat_delete(self):
        html = self.app.INDEX_HTML
        chat_builder = html.split(
            "const buildChatItem =",
            1,
        )[1].split("function renderChatTree", 1)[0]
        self.assertEqual(
            chat_builder.count('recordActionButton("ID", "copy-id")'),
            2,
        )
        self.assertIn("copySessionId(chat)", chat_builder)
        self.assertNotIn('recordActionButton("删除", "delete"', chat_builder)
        self.assertIn(".record-action.session-id", html)

    def test_codex_rate_limit_payload_reports_used_and_remaining(self):
        payload = self.app.normalize_codex_rate_limits(
            {
                "rateLimits": {
                    "limitId": "codex",
                    "planType": "plus",
                    "primary": {
                        "usedPercent": 13,
                        "windowDurationMins": 10080,
                        "resetsAt": 1785905290,
                    },
                    "secondary": {
                        "usedPercent": 42.5,
                        "windowDurationMins": 300,
                        "resetsAt": 1785305290,
                    },
                    "credits": {
                        "hasCredits": False,
                        "unlimited": False,
                        "balance": "0",
                    },
                },
                "rateLimitResetCredits": {"availableCount": 1},
            }
        )
        self.assertTrue(payload["available"])
        self.assertEqual(payload["plan_type"], "plus")
        self.assertEqual(
            [
                (
                    window["used_percent"],
                    window["remaining_percent"],
                )
                for window in payload["windows"]
            ],
            [(13, 87), (42.5, 57.5)],
        )
        self.assertEqual(payload["reset_credits_available"], 1)

    def test_dashboard_exposes_live_codex_usage_controls(self):
        html = self.app.INDEX_HTML
        for marker in (
            'id="usageButton"',
            'id="usagePopover"',
            'id="usageRefresh"',
            'api(`/usage${force ? "?refresh=1" : ""}`',
            "已用 ${used}% · 剩 ${remaining}%",
            'if path == "/api/usage":',
            '"method": "account/rateLimits/read"',
        ):
            self.assertIn(
                marker,
                html
                if not marker.startswith('if path ==')
                and not marker.startswith('"method"')
                else Path(self.app.__file__).read_text(encoding="utf-8"),
            )

    def test_v27_schema_supports_nested_side_chats_and_idempotency(self):
        with self.app.db_connect() as connection:
            chat_columns = self.app.table_columns(connection, "chats")
            job_columns = self.app.table_columns(connection, "jobs")
            user_version = connection.execute("PRAGMA user_version").fetchone()[0]
        for name in (
            "parent_chat_id",
            "source_message_id",
            "source_quote",
            "source_start_offset",
            "source_end_offset",
            "source_text_sha256",
            "source_offset_encoding",
            "source_projection",
            "side_request_id",
            "side_request_fingerprint",
            "side_context_snapshot",
        ):
            self.assertIn(name, chat_columns)
        self.assertIn("request_fingerprint", job_columns)
        self.assertEqual(user_version, 9)

    def test_v26_database_migrates_before_new_indexes_are_created(self):
        legacy_path = self.root / "v26-upgrade.sqlite3"
        with sqlite3.connect(legacy_path) as connection:
            connection.executescript(
                """
                PRAGMA user_version = 5;
                CREATE TABLE actors (
                    id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE chats (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    project TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    codex_thread_id TEXT,
                    creator_actor_id TEXT,
                    category TEXT NOT NULL DEFAULT '',
                    pinned_at TEXT,
                    archived_at TEXT,
                    deleted_at TEXT,
                    state_changed_by_actor_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'completed',
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    actor_id TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    user_message_id INTEGER NOT NULL,
                    assistant_message_id INTEGER,
                    client_request_id TEXT,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    duration_seconds REAL
                );
                CREATE TABLE api_keys (
                    id TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    token_prefix TEXT NOT NULL UNIQUE,
                    token_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    revoked_at TEXT
                );
                CREATE TABLE device_sessions (
                    id TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    session_prefix TEXT NOT NULL UNIQUE,
                    session_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE feedback_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_id TEXT NOT NULL,
                    content TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    priority TEXT NOT NULL DEFAULT 'normal',
                    app_version TEXT NOT NULL,
                    page_path TEXT NOT NULL DEFAULT '',
                    chat_id TEXT,
                    client_request_id TEXT,
                    updated_at TEXT,
                    archived_at TEXT,
                    deleted_at TEXT,
                    state_changed_by_actor_id TEXT,
                    created_at TEXT NOT NULL
                );
                INSERT INTO actors
                    (id, display_name, role, created_at)
                VALUES
                    ('owner', 'Owner', 'owner', '2026-07-27T00:00:00+00:00');
                INSERT INTO device_sessions
                    (id, actor_id, session_prefix, session_hash, created_at)
                VALUES
                    ('legacy-device', 'owner', 'cds_legacy',
                     'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                     '2026-07-27T00:00:00+00:00');
                INSERT INTO chats
                    (id, title, project, mode, creator_actor_id, created_at, updated_at)
                VALUES
                    ('legacy-chat', '旧对话', '.', 'read', 'owner',
                     '2026-07-27T00:00:00+00:00',
                     '2026-07-27T00:00:00+00:00');
                INSERT INTO messages
                    (id, chat_id, role, content, meta_json, actor_id, created_at)
                VALUES
                    (1, 'legacy-chat', 'user', '旧任务', '{}', 'owner',
                     '2026-07-27T00:00:00+00:00');
                INSERT INTO jobs
                    (id, chat_id, user_message_id, client_request_id,
                     prompt, status, created_at)
                VALUES
                    ('legacy-job', 'legacy-chat', 1, 'legacy-request',
                     '旧任务', 'completed', '2026-07-27T00:00:00+00:00');
                """
            )
        original_path = self.app.DB_PATH
        try:
            self.app.DB_PATH = legacy_path
            self.app.initialize_database(recover_jobs=False)
            self.app.initialize_database(recover_jobs=False)
            with self.app.db_connect() as connection:
                chat_columns = self.app.table_columns(connection, "chats")
                indexes = {
                    row["name"]
                    for row in connection.execute("PRAGMA index_list(chats)")
                }
                fingerprint = connection.execute(
                    "SELECT request_fingerprint FROM jobs WHERE id = 'legacy-job'"
                ).fetchone()[0]
                migrated_chat_model = connection.execute(
                    "SELECT model FROM chats WHERE id = 'legacy-chat'"
                ).fetchone()[0]
                migrated_job_model = connection.execute(
                    "SELECT model FROM jobs WHERE id = 'legacy-job'"
                ).fetchone()[0]
                user_version = connection.execute(
                    "PRAGMA user_version"
                ).fetchone()[0]
                legacy_device = connection.execute(
                    """
                    SELECT device_name, last_seen_at, expires_at, revoked_at
                    FROM device_sessions
                    WHERE id = 'legacy-device'
                    """
                ).fetchone()
                pairing_columns = self.app.table_columns(
                    connection, "pairing_codes"
                )
            self.assertIn("parent_chat_id", chat_columns)
            self.assertIn("side_request_fingerprint", chat_columns)
            self.assertIn("side_context_snapshot", chat_columns)
            self.assertIn("source_offset_encoding", chat_columns)
            self.assertIn("source_projection", chat_columns)
            self.assertIn("chats_parent_idx", indexes)
            self.assertIn("chats_side_request_idx", indexes)
            self.assertEqual(len(fingerprint), 64)
            self.assertEqual(migrated_chat_model, self.app.DEFAULT_MODEL)
            self.assertEqual(migrated_job_model, self.app.DEFAULT_MODEL)
            self.assertEqual(user_version, 9)
            self.assertTrue(legacy_device["device_name"])
            self.assertEqual(
                legacy_device["last_seen_at"],
                "2026-07-27T00:00:00+00:00",
            )
            self.assertTrue(legacy_device["expires_at"])
            self.assertIsNone(legacy_device["revoked_at"])
            self.assertIn("code_hash", pairing_columns)
        finally:
            self.app.DB_PATH = original_path

    def test_v28_model_selection_is_whitelisted_persisted_and_snapshotted(self):
        allowed = tuple(self.app.ALLOWED_MODELS)
        self.assertGreaterEqual(len(allowed), 2)
        self.assertIn(self.app.DEFAULT_MODEL, allowed)
        initial_model = allowed[0]
        selected = next(model for model in allowed if model != initial_model)
        self.assertEqual(self.app.validate_model(None), self.app.DEFAULT_MODEL)
        self.assertEqual(
            self.app.validate_model(f"  {selected}  "),
            selected,
        )
        with self.assertRaises(ValueError):
            self.app.validate_model("not-an-allowed-codex-model")
        with self.app.db_connect() as connection:
            self.assertIn(
                "model",
                self.app.table_columns(connection, "chats"),
            )
            self.assertIn(
                "model",
                self.app.table_columns(connection, "jobs"),
            )

        chat = self.app.create_chat(
            "模型持久化",
            ".",
            "read",
            "owner",
            model=initial_model,
        )
        self.assertEqual(chat["model"], initial_model)
        self.assertEqual(
            self.app.get_chat(chat["id"])["model"],
            initial_model,
        )
        listed = next(
            item
            for item in self.app.list_chats("active")["chats"]
            if item["id"] == chat["id"]
        )
        self.assertEqual(listed["model"], initial_model)

        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            queued = self.app.enqueue_message(
                chat["id"],
                "使用选定模型执行",
                "owner",
                "model-snapshot-request",
                model=selected,
            )
            with self.assertRaises(ValueError):
                self.app.enqueue_message(
                    chat["id"],
                    "使用选定模型执行",
                    "owner",
                    "model-snapshot-request",
                    model=initial_model,
                )
        with self.app.db_connect() as connection:
            stored = connection.execute(
                "SELECT model FROM jobs WHERE id = ?",
                (queued["job"]["id"],),
            ).fetchone()
        self.assertEqual(stored["model"], selected)
        self.assertEqual(
            self.app.get_chat(chat["id"])["model"],
            selected,
        )

    def test_v28_model_options_payload_matches_the_exact_allowlist(self):
        payload = self.app.model_options_payload()
        self.assertEqual(payload["default"], self.app.DEFAULT_MODEL)
        self.assertFalse(payload["unrestricted_write"])
        self.assertEqual(
            [item["id"] for item in payload["models"]],
            list(self.app.ALLOWED_MODELS),
        )
        self.assertEqual(
            len({item["id"] for item in payload["models"]}),
            len(payload["models"]),
        )
        for item in payload["models"]:
            self.assertEqual(self.app.validate_model(item["id"]), item["id"])
            self.assertTrue(item["label"].strip())
            self.assertTrue(item["description"].strip())
            self.assertIn(
                item["default_reasoning_effort"],
                [effort["id"] for effort in item["reasoning_efforts"]],
            )
            self.assertIn(
                "standard",
                [speed["id"] for speed in item["speed_tiers"]],
            )
        self.assertEqual(
            payload["defaults"]["reasoning_effort"],
            self.app.default_reasoning_effort(self.app.DEFAULT_MODEL),
        )
        self.assertGreater(payload["attachments"]["max_file_bytes"], 0)
        self.assertIn(
            'if path == "/api/models":',
            Path(self.app.__file__).read_text(encoding="utf-8"),
        )

    def test_v220_model_policy_can_force_luna_max_fast(self):
        policy = {
            "ALLOWED_MODELS": ("gpt-5.6-luna",),
            "DEFAULT_MODEL": "gpt-5.6-luna",
            "FORCED_REASONING_EFFORT": "max",
            "FORCED_SPEED": "fast",
        }
        with mock.patch.multiple(self.app, **policy):
            self.app.validate_model_policy_configuration()
            payload = self.app.model_options_payload()
            self.assertEqual(
                payload["defaults"],
                {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                    "speed": "fast",
                },
            )
            self.assertEqual(
                [item["id"] for item in payload["models"]],
                ["gpt-5.6-luna"],
            )
            self.assertEqual(
                [
                    item["id"]
                    for item in payload["models"][0]["reasoning_efforts"]
                ],
                ["max"],
            )
            self.assertEqual(
                [
                    item["id"]
                    for item in payload["models"][0]["speed_tiers"]
                ],
                ["fast"],
            )
            with self.assertRaises(ValueError):
                self.app.validate_model("gpt-5.6-sol")
            with self.assertRaises(ValueError):
                self.app.validate_reasoning_effort(
                    "gpt-5.6-luna",
                    "medium",
                )
            with self.assertRaises(ValueError):
                self.app.validate_speed("gpt-5.6-luna", "standard")

            chat = self.app.create_chat(
                "固定 Luna Max Fast",
                ".",
                "read",
                "owner",
            )
            self.assertEqual(
                (chat["model"], chat["reasoning_effort"], chat["speed"]),
                ("gpt-5.6-luna", "max", "fast"),
            )
            with mock.patch.object(
                self.app.JOB_QUEUE,
                "put_nowait",
                return_value=None,
            ):
                queued = self.app.enqueue_message(
                    chat["id"],
                    "强制策略验收",
                    "owner",
                    "forced-policy-request",
                )
                with self.assertRaises(ValueError):
                    self.app.enqueue_message(
                        chat["id"],
                        "尝试绕过推理限制",
                        "owner",
                        "forced-policy-effort-override",
                        reasoning_effort="medium",
                    )
                with self.assertRaises(ValueError):
                    self.app.enqueue_message(
                        chat["id"],
                        "尝试绕过速度限制",
                        "owner",
                        "forced-policy-speed-override",
                        speed="standard",
                    )
            self.assertEqual(
                (
                    queued["job"]["model"],
                    queued["job"]["reasoning_effort"],
                    queued["job"]["speed"],
                ),
                ("gpt-5.6-luna", "max", "fast"),
            )
            with self.app.db_connect() as connection:
                stored = connection.execute(
                    "SELECT model, reasoning_effort, speed FROM jobs "
                    "WHERE id = ?",
                    (queued["job"]["id"],),
                ).fetchone()
                connection.execute(
                    "UPDATE jobs SET status = 'failed' WHERE id = ?",
                    (queued["job"]["id"],),
                )
            self.assertEqual(
                tuple(stored),
                ("gpt-5.6-luna", "max", "fast"),
            )

        with mock.patch.multiple(
            self.app,
            ALLOWED_MODELS=("gpt-5.6-luna",),
            DEFAULT_MODEL="gpt-5.6-luna",
            FORCED_REASONING_EFFORT="ultra",
            FORCED_SPEED="fast",
        ):
            with self.assertRaises(RuntimeError):
                self.app.validate_model_policy_configuration()

    def test_v220_forced_policy_normalizes_chats_not_job_history(self):
        with tempfile.TemporaryDirectory() as tempdir:
            database_path = Path(tempdir) / "policy-migration.sqlite3"
            with mock.patch.object(self.app, "DB_PATH", database_path):
                self.app.initialize_database(recover_jobs=False)
                chat = self.app.create_chat(
                    "策略迁移",
                    ".",
                    "read",
                    "owner",
                    model="gpt-5.6-sol",
                    reasoning_effort="low",
                    speed="standard",
                )
                with mock.patch.object(
                    self.app.JOB_QUEUE,
                    "put_nowait",
                    return_value=None,
                ):
                    queued = self.app.enqueue_message(
                        chat["id"],
                        "保留历史任务配置",
                        "owner",
                        "forced-policy-history",
                    )
                with self.app.db_connect() as connection:
                    connection.execute(
                        "UPDATE jobs SET status = 'completed' WHERE id = ?",
                        (queued["job"]["id"],),
                    )

                with mock.patch.multiple(
                    self.app,
                    ALLOWED_MODELS=("gpt-5.6-luna",),
                    DEFAULT_MODEL="gpt-5.6-luna",
                    FORCED_REASONING_EFFORT="max",
                    FORCED_SPEED="fast",
                ):
                    self.app.initialize_database(recover_jobs=False)
                    migrated_chat = self.app.get_chat(chat["id"])
                    with self.app.db_connect() as connection:
                        historical_job = connection.execute(
                            "SELECT model, reasoning_effort, speed "
                            "FROM jobs WHERE id = ?",
                            (queued["job"]["id"],),
                        ).fetchone()

                self.assertEqual(
                    (
                        migrated_chat["model"],
                        migrated_chat["reasoning_effort"],
                        migrated_chat["speed"],
                    ),
                    ("gpt-5.6-luna", "max", "fast"),
                )
                self.assertEqual(
                    tuple(historical_job),
                    ("gpt-5.6-sol", "low", "standard"),
                )

    def test_v220_model_catalog_blocks_submit_and_refreshes_on_restore(self):
        html = self.app.INDEX_HTML
        for marker in (
            "let modelCatalogReady = false;",
            "|| !modelCatalogReady",
            'toast("正在同步模型策略，请稍候")',
            "const initialization = [loadModels()];",
        ):
            self.assertIn(marker, html)

    def test_v28_execute_codex_passes_model_before_resume_thread_id(self):
        commands = []

        def fake_popen(command, **_kwargs):
            commands.append(list(command))
            return FinishedProcessStub()

        selected = self.app.ALLOWED_MODELS[-1]
        with mock.patch.object(
            self.app.subprocess,
            "Popen",
            side_effect=fake_popen,
        ):
            fresh = self.app.execute_codex(
                self.root,
                "read",
                "新会话",
                model=selected,
            )
            resumed = self.app.execute_codex(
                self.root,
                "read",
                "恢复会话",
                model=selected,
                thread_id="thread-v28",
            )
        self.assertTrue(fresh["ok"])
        self.assertTrue(resumed["ok"])
        self.assertEqual(len(commands), 2)
        for command in commands:
            model_index = command.index("--model")
            self.assertEqual(command[model_index + 1], selected)
        self.assertLess(
            commands[1].index("--model"),
            commands[1].index("thread-v28"),
        )

    def test_execute_codex_honors_a_preexisting_cancel_request(self):
        cancel_event = self.app.threading.Event()
        cancel_event.set()
        with mock.patch.object(self.app.subprocess, "Popen") as popen:
            result = self.app.execute_codex(
                self.root,
                "read",
                "无需实际启动",
                cancel_event=cancel_event,
            )
        popen.assert_not_called()
        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        self.assertEqual(result["error"], "用户已停止")

    def test_execute_codex_scopes_lifeos_envelope_to_each_child(self):
        invocations = []

        def fake_popen(command, **kwargs):
            invocations.append((list(command), dict(kwargs["env"])))
            return FinishedProcessStub()

        first_path = self.root / "lifeos-first.json"
        second_path = self.root / "lifeos-second.json"
        stale_path = str(self.root / "stale-parent-value.json")
        with (
            mock.patch.dict(
                os.environ,
                {
                    self.app.LIFEOS_TURN_ENVELOPE_ENV: stale_path,
                    "CODEX_WEB_API_TOKEN": "must-not-enter-child-env",
                },
            ),
            mock.patch.object(
                self.app.subprocess,
                "Popen",
                side_effect=fake_popen,
            ) as popen,
        ):
            self.app.execute_codex(
                self.root,
                "read",
                "第一轮",
                turn_envelope_path=first_path,
            )
            self.app.execute_codex(
                self.root,
                "read",
                "恢复轮",
                thread_id="thread-lifeos",
                turn_envelope_path=second_path,
            )
            self.app.execute_codex(
                self.root,
                "read",
                "无 LifeOS 来源",
            )
            with self.assertRaises(RuntimeError):
                self.app.execute_codex(
                    self.root,
                    "read",
                    "相对路径",
                    turn_envelope_path=Path("relative-turn.json"),
                )
            self.assertEqual(
                os.environ[self.app.LIFEOS_TURN_ENVELOPE_ENV],
                stale_path,
            )
            self.assertEqual(popen.call_count, 3)

        self.assertEqual(
            invocations[0][1][self.app.LIFEOS_TURN_ENVELOPE_ENV],
            str(first_path),
        )
        self.assertEqual(
            invocations[1][1][self.app.LIFEOS_TURN_ENVELOPE_ENV],
            str(second_path),
        )
        self.assertNotIn(
            self.app.LIFEOS_TURN_ENVELOPE_ENV,
            invocations[2][1],
        )
        for command, environment in invocations:
            self.assertNotIn("CODEX_WEB_API_TOKEN", environment)
            self.assertNotIn(str(first_path), command)
            self.assertNotIn(str(second_path), command)

    def test_execute_codex_stops_a_live_process_group(self):
        fake_codex = self.root / "fake-codex-cancel.sh"
        fake_codex.write_text(
            "#!/bin/sh\ncat >/dev/null\nsleep 30\n",
            encoding="utf-8",
        )
        fake_codex.chmod(0o755)
        cancel_event = self.app.threading.Event()
        with (
            mock.patch.object(self.app, "CODEX_BIN", str(fake_codex)),
            mock.patch.object(self.app, "TIMEOUT_SECONDS", 10),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(
                self.app.execute_codex,
                self.root,
                "read",
                "启动后停止",
                cancel_event=cancel_event,
            )
            self.app.time.sleep(0.2)
            cancel_event.set()
            result = future.result(timeout=5)
        self.assertFalse(result["ok"])
        self.assertTrue(result["cancelled"])
        self.assertEqual(result["error"], "用户已停止")
        self.assertLess(result["duration_seconds"], 5)

    def test_unrestricted_write_bypasses_sandbox_for_new_and_resumed_turns(self):
        commands = []

        def fake_popen(command, **_kwargs):
            commands.append(list(command))
            return FinishedProcessStub()

        with (
            mock.patch.object(self.app, "UNRESTRICTED_WRITE", True),
            mock.patch.object(
                self.app.subprocess,
                "Popen",
                side_effect=fake_popen,
            ),
        ):
            self.assertTrue(
                self.app.model_options_payload()["unrestricted_write"]
            )
            self.app.execute_codex(self.root, "write", "新完全权限会话")
            self.app.execute_codex(
                self.root,
                "write",
                "恢复完全权限会话",
                thread_id="thread-unrestricted",
            )
            self.app.execute_codex(self.root, "read", "只读会话")

        fresh, resumed, read_only = commands
        for command in (fresh, resumed):
            self.assertIn(
                "--dangerously-bypass-approvals-and-sandbox",
                command,
            )
            self.assertIn("--ignore-rules", command)
            self.assertNotIn("--sandbox", command)
        self.assertIn("--sandbox", read_only)
        self.assertEqual(
            read_only[read_only.index("--sandbox") + 1],
            "read-only",
        )
        self.assertNotIn(
            "--dangerously-bypass-approvals-and-sandbox",
            read_only,
        )

    def test_v29_execute_codex_snapshots_effort_speed_and_images(self):
        commands = []

        def fake_popen(command, **_kwargs):
            commands.append(list(command))
            return FinishedProcessStub()

        image_path = self.root / "cli-image.png"
        image_path.write_bytes(b"\x89PNG\r\n\x1a\n")
        attachment = {"kind": "image", "path": image_path}
        with mock.patch.object(
            self.app.subprocess,
            "Popen",
            side_effect=fake_popen,
        ):
            self.app.execute_codex(
                self.root,
                "read",
                "标准速度",
                model="gpt-5.6-sol",
                reasoning_effort="high",
                speed="standard",
                attachments=[attachment],
            )
            self.app.execute_codex(
                self.root,
                "read",
                "快速恢复",
                model="gpt-5.6-sol",
                reasoning_effort="ultra",
                speed="fast",
                attachments=[attachment],
                thread_id="thread-v29",
            )
        standard, fast = commands
        self.assertIn('model_reasoning_effort="high"', standard)
        self.assertIn('service_tier="default"', standard)
        self.assertIn("--disable", standard)
        self.assertIn("fast_mode", standard)
        self.assertIn('model_reasoning_effort="ultra"', fast)
        self.assertIn('service_tier="priority"', fast)
        self.assertIn("--enable", fast)
        self.assertLess(fast.index("--image"), fast.index("thread-v29"))
        self.assertEqual(fast[fast.index("--image") + 1], str(image_path))

    def test_v29_attachments_are_claimed_ordered_and_persisted(self):
        png = b"\x89PNG\r\n\x1a\n" + b"test-image"
        image = self.app.create_attachment(
            "owner",
            "截图.tmp",
            "image/png",
            png,
        )
        document = self.app.create_attachment(
            "owner",
            "../说明.txt",
            "text/plain",
            b"attachment text",
        )
        self.assertEqual(image["kind"], "image")
        self.assertEqual(image["mime_type"], "image/png")
        self.assertEqual(document["name"], "说明.txt")
        with self.assertRaises(ValueError):
            self.app.create_attachment(
                "owner",
                "伪图片.png",
                "image/png",
                b"not an image",
            )

        chat = self.app.create_chat(
            "附件任务",
            ".",
            "read",
            "owner",
            model="gpt-5.6-sol",
            reasoning_effort="high",
            speed="fast",
        )
        attachment_ids = [document["id"], image["id"]]
        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            queued = self.app.enqueue_message(
                chat["id"],
                "读取这些附件",
                "owner",
                "attachment-request",
                model="gpt-5.6-sol",
                reasoning_effort="high",
                speed="fast",
                attachment_ids=attachment_ids,
            )
            repeated = self.app.enqueue_message(
                chat["id"],
                "读取这些附件",
                "owner",
                "attachment-request",
                model="gpt-5.6-sol",
                reasoning_effort="high",
                speed="fast",
                attachment_ids=attachment_ids,
            )
        self.assertEqual(repeated["job"]["id"], queued["job"]["id"])
        self.assertEqual(
            [item["id"] for item in queued["message"]["attachments"]],
            attachment_ids,
        )
        persisted = self.app.get_chat(chat["id"])
        user_message = next(
            message
            for message in persisted["messages"]
            if message["id"] == queued["message"]["id"]
        )
        self.assertEqual(
            [item["id"] for item in user_message["attachments"]],
            attachment_ids,
        )
        with self.app.db_connect() as connection:
            job = connection.execute(
                """
                SELECT model, reasoning_effort, speed
                FROM jobs WHERE id = ?
                """,
                (queued["job"]["id"],),
            ).fetchone()
        self.assertEqual(
            (job["model"], job["reasoning_effort"], job["speed"]),
            ("gpt-5.6-sol", "high", "fast"),
        )

        rows = self.app.attachment_rows_for_job(queued["job"]["id"])
        staging_root, staged = self.app.stage_job_attachments(
            self.root,
            queued["job"]["id"],
            rows,
        )
        try:
            self.assertEqual(len(staged), 2)
            self.assertEqual(staged[1]["path"].suffix, ".png")
            self.assertEqual(staged[1]["path"].read_bytes(), png)
            execution_prompt = self.app.attachment_execution_prompt(
                "读取这些附件",
                staged,
            )
            self.assertIn("以下文件由用户上传", execution_prompt)
            self.assertIn(str(staged[0]["path"]), execution_prompt)
        finally:
            self.app.shutil.rmtree(staging_root, ignore_errors=True)

    def test_lifeos_envelope_preserves_source_atomically_and_privately(self):
        document = self.app.create_attachment(
            "owner",
            "生活记录.txt",
            "text/plain",
            "早餐：燕麦".encode("utf-8"),
        )
        image = self.app.create_attachment(
            "owner",
            "早餐.png",
            "image/png",
            b"\x89PNG\r\n\x1a\nlifeos-image",
        )
        visible_text = "请记录我今天 07:30 起床 😀"
        message_meta = {
            "annotations": [
                {
                    "source_message_id": 42,
                    "quote": "07:30 起床",
                    "comment": "这是实际起床，不是计划",
                    "start_offset": 6,
                    "end_offset": 14,
                    "offset_encoding": "utf-16",
                    "action": "annotation",
                }
            ],
            "nested": {"保留": ["中文", "emoji-🌅"]},
        }
        chat, queued = self.enqueue_test_job(
            "exact-source",
            mode="write",
            prompt=(
                "EXPANDED-PROMPT-MUST-NOT-LEAK\n"
                "这里可能含服务器扩展历史"
            ),
            message_content=visible_text,
            message_meta=message_meta,
            attachment_ids=[document["id"], image["id"]],
        )
        job_id = queued["job"]["id"]
        job_row = self.app.queued_job_execution_row(job_id)
        attachment_rows = self.app.attachment_rows_for_job(job_id)
        staging_root, staged = self.app.stage_job_attachments(
            self.root,
            job_id,
            attachment_rows,
        )
        self.addCleanup(
            self.app.shutil.rmtree,
            staging_root,
            ignore_errors=True,
        )
        replace_calls = []
        original_replace = self.app.os.replace

        def observing_replace(source, target):
            source = Path(source)
            target = Path(target)
            replace_calls.append(
                {
                    "source": source,
                    "target": target,
                    "target_absent": not target.exists(),
                    "source_bytes": source.read_bytes(),
                    "source_mode": self.app.stat.S_IMODE(
                        source.stat().st_mode
                    ),
                }
            )
            return original_replace(source, target)

        with self.configured_lifeos_root(
            "exact-source"
        ) as envelope_root:
            previous_umask = self.app.os.umask(0o777)
            try:
                with mock.patch.object(
                    self.app.os,
                    "replace",
                    side_effect=observing_replace,
                ):
                    path = self.app.create_lifeos_turn_envelope(
                        job_row,
                        staged,
                    )
            finally:
                self.app.os.umask(previous_umask)
            try:
                payload_bytes = path.read_bytes()
                payload = json.loads(payload_bytes)
                expected_attachments = [
                    {
                        "id": item["id"],
                        "ordinal": ordinal,
                        "name": item["name"],
                        "mime_type": item["mime_type"],
                        "kind": item["kind"],
                        "size_bytes": item["size_bytes"],
                        "sha256": item["sha256"],
                        "temp_path": str(item["path"]),
                    }
                    for ordinal, item in enumerate(staged)
                ]
                expected_payload = {
                    "schema_version": 1,
                    "source": {
                        "system": "codex-deck",
                        "instance_id": self.app.INSTANCE_SWITCH["id"],
                    },
                    "turn": {
                        "job_id": job_id,
                        "chat_id": chat["id"],
                        "user_message_id": queued["message"]["id"],
                        "client_request_id": (
                            "lifeos-exact-source-request"
                        ),
                        "request_fingerprint": (
                            job_row["request_fingerprint"]
                        ),
                        "submitted_at": job_row["created_at"],
                        "effective_mode": "read-or-write",
                    },
                    "message": {
                        "text": visible_text,
                        "meta": message_meta,
                    },
                    "attachments": expected_attachments,
                    "idempotency_root": (
                        f"codex-deck:{self.app.INSTANCE_SWITCH['id']}:"
                        f"{job_id}"
                    ),
                }
                self.assertEqual(payload, expected_payload)
                self.assertEqual(
                    self.app.stat.S_IMODE(envelope_root.stat().st_mode),
                    0o700,
                )
                self.assertEqual(
                    self.app.stat.S_IMODE(path.parent.stat().st_mode),
                    0o700,
                )
                self.assertEqual(
                    self.app.stat.S_IMODE(path.stat().st_mode),
                    0o600,
                )
                self.assertEqual(path.stat().st_nlink, 1)
                self.assertEqual(len(replace_calls), 1)
                replacement = replace_calls[0]
                self.assertEqual(replacement["target"], path)
                self.assertEqual(
                    replacement["source"].parent,
                    replacement["target"].parent,
                )
                self.assertTrue(replacement["target_absent"])
                self.assertEqual(replacement["source_mode"], 0o600)
                self.assertEqual(
                    replacement["source_bytes"],
                    payload_bytes,
                )
                self.assertNotIn(
                    "EXPANDED-PROMPT-MUST-NOT-LEAK",
                    payload_bytes.decode("utf-8"),
                )
            finally:
                self.assertTrue(
                    self.app.cleanup_lifeos_turn_envelope(path)
                )
                self.assertFalse(path.parent.exists())

    def test_lifeos_envelope_read_mode_and_utf8_byte_limit(self):
        _, queued = self.enqueue_test_job(
            "read-limit",
            mode="read",
            message_content="只读取今天的记录",
            message_meta={"说明": "汉字按 UTF-8 字节计数"},
        )
        job_row = self.app.queued_job_execution_row(
            queued["job"]["id"]
        )
        payload = self.app.build_lifeos_turn_envelope(job_row, [])
        self.assertEqual(payload["turn"]["effective_mode"], "read")
        utf16_boundary = dict(job_row)
        boundary_text = (
            "😀" * (self.app.MAX_LIFEOS_TURN_MESSAGE_CHARS // 2)
        )
        utf16_boundary["user_message_content"] = boundary_text
        self.assertEqual(
            self.app.build_lifeos_turn_envelope(
                utf16_boundary,
                [],
            )["message"]["text"],
            boundary_text,
        )
        too_long = dict(utf16_boundary)
        too_long["user_message_content"] += "😀"
        with self.assertRaises(RuntimeError):
            self.app.build_lifeos_turn_envelope(too_long, [])
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        envelope_root = self.lifeos_turn_root("read-limit")
        with (
            mock.patch.object(
                self.app,
                "LIFEOS_TURN_ENVELOPE_ROOT",
                envelope_root,
            ),
            mock.patch.object(
                self.app,
                "MAX_LIFEOS_TURN_ENVELOPE_BYTES",
                len(encoded) - 1,
            ),
        ):
            with self.assertRaises(RuntimeError):
                self.app.create_lifeos_turn_envelope(job_row, [])
            self.assertFalse(envelope_root.exists())
        with (
            mock.patch.object(
                self.app,
                "LIFEOS_TURN_ENVELOPE_ROOT",
                envelope_root,
            ),
            mock.patch.object(
                self.app,
                "MAX_LIFEOS_TURN_ENVELOPE_BYTES",
                len(encoded),
            ),
        ):
            path = self.app.create_lifeos_turn_envelope(job_row, [])
            self.assertEqual(path.read_bytes(), encoded)
            self.assertTrue(
                self.app.cleanup_lifeos_turn_envelope(path)
            )

    def test_lifeos_envelope_rejects_unsafe_source_and_symlink_root(self):
        _, queued = self.enqueue_test_job(
            "unsafe-source",
            mode="write",
        )
        job_row = self.app.queued_job_execution_row(
            queued["job"]["id"]
        )
        for unsafe in ("../escape", "/absolute"):
            with self.subTest(unsafe=repr(unsafe)):
                changed = dict(job_row)
                changed["id"] = unsafe
                with self.assertRaises(RuntimeError):
                    self.app.build_lifeos_turn_envelope(changed, [])
        for invalid_time in (
            "2026-01-01 00:00:00+00:00",
            "2026-01-01T00:00:00+0000",
        ):
            with self.subTest(submitted_at=invalid_time):
                changed = dict(job_row)
                changed["created_at"] = invalid_time
                with self.assertRaises(RuntimeError):
                    self.app.build_lifeos_turn_envelope(changed, [])
        for corrupt_meta in (
            '{"value":NaN}',
            '["not","an","object"]',
        ):
            with self.subTest(meta=corrupt_meta):
                changed = dict(job_row)
                changed["user_message_meta_json"] = corrupt_meta
                with self.assertRaises(RuntimeError):
                    self.app.build_lifeos_turn_envelope(changed, [])
        with self.assertRaises(ValueError):
            self.enqueue_test_job(
                "nonfinite-entry",
                message_meta={"value": float("nan")},
            )

        outside = self.root / "lifeos-symlink-outside"
        self.app.shutil.rmtree(outside, ignore_errors=True)
        outside.mkdir(mode=0o700)
        marker = outside / "marker.txt"
        marker.write_text("outside-must-survive", encoding="utf-8")
        symlink_root = self.root / "lifeos-symlink-root"
        symlink_root.unlink(missing_ok=True)
        symlink_root.symlink_to(outside, target_is_directory=True)
        self.addCleanup(symlink_root.unlink, missing_ok=True)
        self.addCleanup(
            self.app.shutil.rmtree,
            outside,
            ignore_errors=True,
        )
        with mock.patch.object(
            self.app,
            "LIFEOS_TURN_ENVELOPE_ROOT",
            symlink_root,
        ):
            with self.assertRaises(RuntimeError):
                self.app.create_lifeos_turn_envelope(job_row, [])
        self.assertEqual(
            marker.read_text(encoding="utf-8"),
            "outside-must-survive",
        )

    def test_lifeos_attachment_query_rejects_cross_message_binding(self):
        attachment = self.app.create_attachment(
            "owner",
            "private-source.txt",
            "text/plain",
            b"belongs only to the original message",
        )
        _, original = self.enqueue_test_job(
            "attachment-owner",
            mode="read",
            attachment_ids=[attachment["id"]],
        )
        _, foreign = self.enqueue_test_job(
            "attachment-foreign",
            mode="read",
        )
        with self.app.db_connect() as connection:
            connection.execute(
                """
                UPDATE attachments
                SET message_id = ?
                WHERE id = ?
                """,
                (
                    foreign["message"]["id"],
                    attachment["id"],
                ),
            )
        self.assertEqual(
            self.app.attachment_rows_for_job(
                original["job"]["id"]
            ),
            [],
        )
        self.assertEqual(
            self.app.attachment_rows_for_job(
                foreign["job"]["id"]
            ),
            [],
        )

    def test_lifeos_cleanup_does_not_follow_job_directory_symlinks(self):
        envelope_root = self.lifeos_turn_root("stale-cleanup")
        outside = self.root / "lifeos-stale-outside"
        self.app.shutil.rmtree(outside, ignore_errors=True)
        outside.mkdir(mode=0o700)
        outside_turn = outside / "turn.json"
        outside_turn.write_text("outside-must-survive", encoding="utf-8")
        self.addCleanup(
            self.app.shutil.rmtree,
            outside,
            ignore_errors=True,
        )
        job_token = "a" * 32 + "-" + "b" * 32
        stale_token = "c" * 32 + "-" + "d" * 32
        unrelated = envelope_root / "keep-this-directory"
        with mock.patch.object(
            self.app,
            "LIFEOS_TURN_ENVELOPE_ROOT",
            envelope_root,
        ):
            self.app.ensure_lifeos_turn_envelope_root()
            symlink = envelope_root / job_token
            symlink.symlink_to(outside, target_is_directory=True)
            stale = envelope_root / stale_token
            stale.mkdir(mode=0o700)
            (stale / "turn.json").write_text("stale", encoding="utf-8")
            (stale / (".turn-" + "e" * 32 + ".tmp")).write_text(
                "temporary",
                encoding="utf-8",
            )
            unrelated.mkdir()
            (unrelated / "turn.json").write_text(
                "unrelated",
                encoding="utf-8",
            )

            self.assertFalse(
                self.app.cleanup_lifeos_turn_envelope(
                    symlink / "turn.json"
                )
            )
            self.assertEqual(
                outside_turn.read_text(encoding="utf-8"),
                "outside-must-survive",
            )
            self.assertEqual(
                self.app.cleanup_stale_lifeos_turn_envelopes(),
                2,
            )
            self.assertFalse(symlink.exists())
            self.assertFalse(stale.exists())
            self.assertTrue(unrelated.is_dir())
            self.assertEqual(
                outside_turn.read_text(encoding="utf-8"),
                "outside-must-survive",
            )

    def test_queued_job_can_be_cancelled_and_remains_in_history(self):
        chat = self.app.create_chat("排队取消", ".", "read", "owner")
        scheduler = self.app.JobScheduler(queue_capacity=4, max_running=1)
        with mock.patch.object(self.app, "JOB_QUEUE", scheduler):
            queued = self.app.enqueue_message(
                chat["id"],
                "稍后执行",
                "owner",
                "queued-cancel-request",
            )
            cancelled = self.app.cancel_job(
                queued["job"]["id"],
                {"id": "owner", "role": "owner"},
            )
        self.assertEqual(cancelled["status"], "failed")
        self.assertEqual(scheduler.snapshot()["queued_jobs"], 0)
        self.assertTrue(cancelled["message"]["meta"]["cancelled"])
        self.assertEqual(cancelled["message"]["status"], "partial")
        self.assertIn("任务已由你停止", cancelled["message"]["content"])
        self.assertNotIn(
            queued["job"]["id"],
            self.app.JOB_CANCEL_EVENTS,
        )
        active = self.app.get_chat(chat["id"])["active_jobs"]
        self.assertEqual(active, [])

    def test_claimed_queued_cancel_never_creates_lifeos_envelope(self):
        chat = self.app.create_chat(
            "LifeOS 调度后取消",
            ".",
            "read",
            "owner",
        )
        self.addCleanup(self.cleanup_lifeos_test_chat, chat["id"])
        scheduler = self.app.JobScheduler(
            queue_capacity=4,
            max_running=1,
        )
        claimed = None
        with mock.patch.object(self.app, "JOB_QUEUE", scheduler):
            queued = self.app.enqueue_message(
                chat["id"],
                "调度器已经取走，但还没有开始运行",
                "owner",
                "lifeos-claimed-cancel-request",
            )
            job_id = queued["job"]["id"]
            claimed = scheduler.claim(timeout=0)
            requested = self.app.cancel_job(
                job_id,
                {"id": "owner", "role": "owner"},
            )
            self.assertEqual(requested["status"], "queued")
            with (
                mock.patch.object(
                    self.app,
                    "stage_job_attachments",
                ) as stage_attachments,
                mock.patch.object(
                    self.app,
                    "create_lifeos_turn_envelope",
                ) as create_envelope,
                mock.patch.object(
                    self.app,
                    "execute_codex",
                ) as execute,
            ):
                self.assertTrue(self.app.process_job(job_id))
            scheduler.complete(claimed)

        stage_attachments.assert_not_called()
        create_envelope.assert_not_called()
        execute.assert_not_called()
        cancelled = self.app.get_job(job_id)
        self.assertEqual(cancelled["status"], "failed")
        self.assertEqual(cancelled["error"], "用户已停止")
        self.assertTrue(cancelled["message"]["meta"]["cancelled"])
        self.assertNotIn(job_id, self.app.JOB_CANCEL_EVENTS)

    def test_running_job_cancel_request_sets_the_worker_event(self):
        chat = self.app.create_chat("运行取消", ".", "read", "owner")
        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            queued = self.app.enqueue_message(
                chat["id"],
                "正在执行",
                "owner",
                "running-cancel-request",
            )
        job_id = queued["job"]["id"]
        with self.app.db_connect() as connection:
            connection.execute(
                """
                UPDATE jobs SET status = 'running', started_at = ?
                WHERE id = ?
                """,
                (self.app.now_iso(), job_id),
            )
        requested = self.app.cancel_job(
            job_id,
            {"id": "owner", "role": "owner"},
        )
        self.assertEqual(requested["status"], "running")
        self.assertTrue(self.app.job_cancel_event(job_id).is_set())
        cancelled = self.app.persist_job_cancellation(job_id)
        self.app.release_job_cancel_event(job_id)
        self.assertEqual(cancelled["status"], "failed")

    def test_process_job_envelope_failure_never_starts_codex(self):
        _, queued = self.enqueue_test_job(
            "producer-failure",
            mode="write",
        )
        job_id = queued["job"]["id"]
        with (
            self.configured_lifeos_root(
                "producer-failure"
            ) as envelope_root,
            mock.patch.object(
                self.app.os,
                "replace",
                side_effect=OSError("synthetic envelope disk failure"),
            ),
            mock.patch.object(
                self.app,
                "execute_codex",
            ) as execute,
        ):
            self.assertTrue(self.app.process_job(job_id))
        execute.assert_not_called()
        failed = self.app.get_job(job_id)
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(
            failed["error"],
            "LifeOS 回合来源准备失败，本次任务未启动",
        )
        self.assertNotIn("synthetic", failed["error"])
        self.assertTrue(envelope_root.is_dir())
        self.assertEqual(list(envelope_root.iterdir()), [])
        self.assertNotIn(job_id, self.app.JOB_CANCEL_EVENTS)

    def test_process_job_cleans_envelope_on_failure_cancel_and_exception(self):
        cases = (
            (
                "failure",
                self.codex_result(
                    ok=False,
                    returncode=1,
                    output="",
                    error="synthetic Codex failure",
                ),
                "failed",
            ),
            (
                "cancel",
                self.codex_result(
                    ok=False,
                    cancelled=True,
                    returncode=None,
                    output="已完成一部分",
                    error="用户已停止",
                ),
                "failed",
            ),
            (
                "exception",
                RuntimeError("synthetic execute exception"),
                "failed",
            ),
        )
        for label, behavior, expected_status in cases:
            with self.subTest(label=label):
                _, queued = self.enqueue_test_job(
                    f"terminal-{label}",
                    mode="write",
                )
                job_id = queued["job"]["id"]
                observations = []

                def fake_execute(*_args, **kwargs):
                    path = kwargs["turn_envelope_path"]
                    observations.append(
                        {
                            "exists": path.is_file(),
                            "job_id": json.loads(
                                path.read_text(encoding="utf-8")
                            )["turn"]["job_id"],
                        }
                    )
                    if isinstance(behavior, Exception):
                        raise behavior
                    return dict(behavior)

                with (
                    self.configured_lifeos_root(
                        f"terminal-{label}"
                    ) as envelope_root,
                    mock.patch.object(
                        self.app,
                        "execute_codex",
                        side_effect=fake_execute,
                    ),
                ):
                    self.assertTrue(self.app.process_job(job_id))

                self.assertEqual(
                    observations,
                    [{"exists": True, "job_id": job_id}],
                )
                self.assertEqual(
                    self.app.get_job(job_id)["status"],
                    expected_status,
                )
                self.assertEqual(list(envelope_root.iterdir()), [])
                self.assertNotIn(job_id, self.app.JOB_CANCEL_EVENTS)

    def test_v29_attachment_discard_and_ttl_cleanup(self):
        discarded = self.app.create_attachment(
            "owner",
            "discard.txt",
            "text/plain",
            b"discard me",
        )
        with self.app.db_connect() as connection:
            discarded_row = connection.execute(
                "SELECT * FROM attachments WHERE id = ?",
                (discarded["id"],),
            ).fetchone()
            discarded_path = self.app.attachment_storage_path(discarded_row)
        self.assertTrue(discarded_path.is_file())
        self.app.discard_attachment(discarded["id"], "owner")
        self.assertFalse(discarded_path.exists())

        expired = self.app.create_attachment(
            "owner",
            "expired.txt",
            "text/plain",
            b"expired",
        )
        with self.app.db_connect() as connection:
            row = connection.execute(
                "SELECT * FROM attachments WHERE id = ?",
                (expired["id"],),
            ).fetchone()
            expired_path = self.app.attachment_storage_path(row)
            connection.execute(
                """
                UPDATE attachments
                SET created_at = '2000-01-01T00:00:00+00:00'
                WHERE id = ?
                """,
                (expired["id"],),
            )
        self.app.cleanup_attachment_storage()
        self.assertFalse(expired_path.exists())
        with self.app.db_connect() as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM attachments WHERE id = ?",
                    (expired["id"],),
                ).fetchone()
            )

    def test_v29_recovery_attachment_context_uses_recovery_limit(self):
        long_prompt = "长" * (self.app.MAX_EXECUTION_PROMPT_CHARS + 1)
        with self.assertRaises(ValueError):
            self.app.attachment_execution_prompt(long_prompt, [])
        accepted = self.app.attachment_execution_prompt(
            long_prompt,
            [],
            max_chars=self.app.MAX_RECOVERY_PROMPT_CHARS,
        )
        self.assertEqual(accepted, long_prompt)

    def test_v28_recovery_keeps_the_job_model_snapshot(self):
        selected = self.app.ALLOWED_MODELS[-1]
        chat = self.app.create_chat(
            "模型恢复",
            ".",
            "read",
            "owner",
            model=selected,
        )
        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            queued = self.app.enqueue_message(
                chat["id"],
                "恢复时继续使用原模型",
                "owner",
                "model-recovery-request",
                model=selected,
            )
        with self.app.db_connect() as connection:
            connection.execute(
                "UPDATE chats SET codex_thread_id = ?, model = ? WHERE id = ?",
                ("missing-thread", self.app.DEFAULT_MODEL, chat["id"]),
            )

        missing = {
            "ok": False,
            "returncode": 1,
            "output": "",
            "thread_id": "missing-thread",
            "error": "session not found",
            "duration_seconds": 0.1,
        }
        recovered = {
            "ok": True,
            "returncode": 0,
            "output": "已恢复",
            "thread_id": "replacement-thread",
            "error": "",
            "duration_seconds": 0.2,
        }
        with mock.patch.object(
            self.app,
            "execute_codex",
            side_effect=(missing, recovered),
        ) as execute:
            self.assertTrue(self.app.process_job(queued["job"]["id"]))
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(execute.call_args_list[0].kwargs["model"], selected)
        self.assertEqual(execute.call_args_list[1].kwargs["model"], selected)
        self.assertEqual(
            execute.call_args_list[0].kwargs["thread_id"],
            "missing-thread",
        )
        self.assertIsNone(execute.call_args_list[1].kwargs["thread_id"])

    def test_corrupt_lifeos_message_meta_fails_closed(self):
        _, queued = self.enqueue_test_job(
            "corrupt-meta",
            mode="write",
        )
        job_id = queued["job"]["id"]
        with self.app.db_connect() as connection:
            connection.execute(
                "UPDATE messages SET meta_json = ? WHERE id = ?",
                (
                    '{"duplicate":1,"duplicate":2}',
                    queued["message"]["id"],
                ),
            )
        with (
            self.configured_lifeos_root(
                "corrupt-meta"
            ) as envelope_root,
            mock.patch.object(
                self.app,
                "execute_codex",
            ) as execute,
        ):
            self.assertTrue(self.app.process_job(job_id))
        execute.assert_not_called()
        failed = self.app.get_job(job_id)
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(
            failed["error"],
            "LifeOS 回合来源准备失败，本次任务未启动",
        )
        self.assertNotIn("duplicate", failed["error"])
        self.assertFalse(
            envelope_root.exists() and any(envelope_root.iterdir())
        )

    def test_session_missing_retry_reuses_exact_lifeos_envelope(self):
        chat, queued = self.enqueue_test_job(
            "session-recovery",
            mode="read",
            message_content="恢复后仍应绑定同一次用户回合",
        )
        job_id = queued["job"]["id"]
        with self.app.db_connect() as connection:
            connection.execute(
                """
                UPDATE chats
                SET codex_thread_id = ?
                WHERE id = ?
                """,
                ("missing-lifeos-thread", chat["id"]),
            )
        calls = []

        def fake_execute(*_args, **kwargs):
            path = kwargs["turn_envelope_path"]
            payload_bytes = path.read_bytes()
            calls.append(
                {
                    "path": path,
                    "bytes": payload_bytes,
                    "inode": path.stat().st_ino,
                    "thread_id": kwargs["thread_id"],
                }
            )
            if len(calls) == 1:
                return self.codex_result(
                    ok=False,
                    returncode=1,
                    output="",
                    thread_id="missing-lifeos-thread",
                    error="session not found",
                )
            return self.codex_result(
                output="已从保存记录恢复",
                thread_id="replacement-lifeos-thread",
            )

        with (
            self.configured_lifeos_root(
                "session-recovery"
            ) as envelope_root,
            mock.patch.object(
                self.app,
                "execute_codex",
                new=fake_execute,
            ),
        ):
            self.assertTrue(self.app.process_job(job_id))

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["path"], calls[1]["path"])
        self.assertEqual(calls[0]["bytes"], calls[1]["bytes"])
        self.assertEqual(calls[0]["inode"], calls[1]["inode"])
        self.assertEqual(
            calls[0]["thread_id"],
            "missing-lifeos-thread",
        )
        self.assertIsNone(calls[1]["thread_id"])
        self.assertFalse(calls[0]["path"].exists())
        self.assertFalse(calls[0]["path"].parent.exists())
        self.assertEqual(list(envelope_root.iterdir()), [])
        self.assertEqual(self.app.get_job(job_id)["status"], "completed")

    def test_parallel_jobs_receive_isolated_lifeos_envelopes(self):
        _, first = self.enqueue_test_job(
            "parallel-first",
            mode="read",
            message_content="并发来源甲",
        )
        _, second = self.enqueue_test_job(
            "parallel-second",
            mode="read",
            message_content="并发来源乙",
        )
        job_ids = {
            first["job"]["id"],
            second["job"]["id"],
        }
        envelope_root = self.lifeos_turn_root("parallel")
        observations = []
        overlap_paths = []
        observation_lock = self.app.threading.Lock()

        def capture_overlap():
            overlap_paths.extend(
                sorted(envelope_root.glob("*/turn.json"))
            )

        barrier = self.app.threading.Barrier(
            2,
            action=capture_overlap,
        )

        def fake_execute(*_args, **kwargs):
            path = kwargs["turn_envelope_path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            with observation_lock:
                observations.append(
                    {
                        "job_id": payload["turn"]["job_id"],
                        "message": payload["message"]["text"],
                        "path": path,
                    }
                )
            barrier.wait(timeout=5)
            return self.codex_result(
                output=f"完成 {payload['turn']['job_id']}",
                thread_id=f"thread-{payload['turn']['job_id']}",
            )

        with (
            mock.patch.object(
                self.app,
                "LIFEOS_TURN_ENVELOPE_ROOT",
                envelope_root,
            ),
            mock.patch.object(
                self.app,
                "execute_codex",
                new=fake_execute,
            ),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            futures = [
                executor.submit(self.app.process_job, job_id)
                for job_id in job_ids
            ]
            results = [
                future.result(timeout=10)
                for future in futures
            ]

        self.assertEqual(results, [True, True])
        self.assertEqual(
            {item["job_id"] for item in observations},
            job_ids,
        )
        self.assertEqual(
            {item["message"] for item in observations},
            {"并发来源甲", "并发来源乙"},
        )
        self.assertEqual(
            len({item["path"] for item in observations}),
            2,
        )
        self.assertEqual(len(overlap_paths), 2)
        self.assertEqual(set(overlap_paths), {
            item["path"] for item in observations
        })
        for item in observations:
            self.assertFalse(item["path"].exists())
        self.assertEqual(list(envelope_root.iterdir()), [])
        for job_id in job_ids:
            self.assertEqual(
                self.app.get_job(job_id)["status"],
                "completed",
            )

    def test_response_annotations_are_validated_and_round_trip(self):
        chat = self.app.create_chat("批注测试", ".", "read", "owner")
        with self.app.db_connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', ?, 'completed', '{}', ?)
                """,
                (
                    chat["id"],
                    "数据库完整性：正常。这里还有一个示例。",
                    self.app.now_iso(),
                ),
            )
            source_id = cursor.lastrowid
        prepared = self.app.prepare_chat_message(
            chat["id"],
            "",
            [
                {
                    "source_message_id": source_id,
                    "quote": "数据库完整性：正常",
                    "comment": "具体检查了哪些项目？",
                    "start_offset": 0,
                    "end_offset": 9,
                    "action": "annotation",
                }
            ],
        )
        self.assertIn("具体检查了哪些项目", prepared["execution_prompt"])
        self.assertEqual(
            prepared["message_meta"]["annotations"][0]["source_message_id"],
            source_id,
        )
        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            queued = self.app.enqueue_message(
                chat["id"],
                prepared["execution_prompt"],
                "owner",
                "annotation-round-trip",
                message_content=prepared["message_content"],
                message_meta=prepared["message_meta"],
            )
        self.assertEqual(
            queued["message"]["meta"]["annotations"][0]["comment"],
            "具体检查了哪些项目？",
        )

    def test_response_annotations_reject_cross_chat_and_forged_quote(self):
        first = self.app.create_chat("来源", ".", "read", "owner")
        second = self.app.create_chat("目标", ".", "read", "owner")
        with self.app.db_connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', '真实原文', 'completed', '{}', ?)
                """,
                (first["id"], self.app.now_iso()),
            )
            source_id = cursor.lastrowid
        payload = [
            {
                "source_message_id": source_id,
                "quote": "伪造引用",
                "comment": "为什么？",
                "start_offset": 0,
                "end_offset": 4,
            }
        ]
        with self.assertRaises(ValueError):
            self.app.prepare_chat_message(first["id"], "", payload)
        payload[0]["quote"] = "真实原文"
        with self.assertRaises(ValueError):
            self.app.prepare_chat_message(second["id"], "", payload)

    def test_response_annotation_offsets_bind_the_exact_rendered_text(self):
        chat = self.app.create_chat("精确锚点", ".", "read", "owner")
        source_text = "### 检查结果\n- **数据库完整性：正常**\n- `队列：正常`"
        rendered = self.app.rendered_message_text(source_text)
        quote = "数据库完整性：正常"
        start = rendered.index(quote)
        with self.app.db_connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', ?, 'completed', '{}', ?)
                """,
                (chat["id"], source_text, self.app.now_iso()),
            )
            source_id = cursor.lastrowid
        prepared = self.app.prepare_chat_message(
            chat["id"],
            "",
            [
                {
                    "source_message_id": source_id,
                    "quote": quote,
                    "comment": "检查范围是什么？",
                    "start_offset": start,
                    "end_offset": start + len(quote),
                }
            ],
        )
        annotation = prepared["message_meta"]["annotations"][0]
        self.assertEqual(annotation["start_offset"], start)
        self.assertEqual(len(annotation["source_text_sha256"]), 64)
        with self.assertRaises(ValueError):
            self.app.prepare_chat_message(
                chat["id"],
                "",
                [
                    {
                        "source_message_id": source_id,
                        "quote": quote,
                        "comment": "不能锚到另一处",
                        "start_offset": start + 1,
                        "end_offset": start + 1 + len(quote),
                    }
                ],
            )
        with self.app.db_connect() as connection:
            emoji_source_id = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', '😀abc', 'completed', '{}', ?)
                """,
                (chat["id"], self.app.now_iso()),
            ).lastrowid
            raw_source_id = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', '**abc**', 'completed', '{}', ?)
                """,
                (chat["id"], self.app.now_iso()),
            ).lastrowid
        emoji = self.app.prepare_chat_message(
            chat["id"],
            "",
            [
                {
                    "source_message_id": emoji_source_id,
                    "quote": "abc",
                    "comment": "UTF-16 偏移应正确",
                    "start_offset": 2,
                    "end_offset": 5,
                }
            ],
        )["message_meta"]["annotations"][0]
        self.assertEqual(emoji["offset_encoding"], "utf-16")
        self.assertEqual(emoji["source_projection"], "rendered")
        with self.assertRaises(ValueError):
            self.app.prepare_chat_message(
                chat["id"],
                "",
                [
                    {
                        "source_message_id": raw_source_id,
                        "quote": "abc",
                        "comment": "原始 Markdown 偏移不能冒充渲染偏移",
                        "start_offset": 2,
                        "end_offset": 5,
                    }
                ],
            )

    def test_side_chat_is_readonly_nested_and_collapsed_in_navigation(self):
        selected_model = self.app.ALLOWED_MODELS[-1]
        parent = self.app.create_chat(
            "主对话",
            ".",
            "write",
            "owner",
            model=selected_model,
        )
        with self.app.db_connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', ?, 'completed', '{}', ?)
                """,
                (
                    parent["id"],
                    "数据库完整性：正常。",
                    self.app.now_iso(),
                ),
            )
            source_id = cursor.lastrowid
        fake_queue = {
            "job": {
                "id": "side-job",
                "chat_id": "placeholder",
                "status": "queued",
                "created_at": self.app.now_iso(),
            },
            "message": {
                "id": 999,
                "role": "user",
                "content": "具体依据是什么？",
                "meta": {},
            },
        }
        with mock.patch.object(
            self.app,
            "enqueue_message",
            return_value=fake_queue,
        ):
            created = self.app.create_side_chat(
                parent["id"],
                source_id,
                "数据库完整性：正常",
                0,
                9,
                "具体依据是什么？",
                "owner",
                "side-create-1",
            )
            repeated = self.app.create_side_chat(
                parent["id"],
                source_id,
                "数据库完整性：正常",
                0,
                9,
                "具体依据是什么？",
                "owner",
                "side-create-1",
            )
            with self.assertRaises(ValueError):
                self.app.create_side_chat(
                    parent["id"],
                    source_id,
                    "数据库完整性：正常",
                    0,
                    9,
                    "换成另一个问题",
                    "owner",
                    "side-create-1",
                )
        self.assertEqual(created["chat"]["id"], repeated["chat"]["id"])
        self.assertEqual(created["chat"]["mode"], "read")
        self.assertEqual(created["chat"]["model"], selected_model)
        self.assertEqual(created["chat"]["parent_chat_id"], parent["id"])
        if len(self.app.ALLOWED_MODELS) > 1:
            other_model = next(
                model
                for model in self.app.ALLOWED_MODELS
                if model != selected_model
            )
            with self.assertRaises(ValueError):
                self.app.enqueue_message(
                    created["chat"]["id"],
                    "侧聊不能改模型",
                    "owner",
                    "side-model-override",
                    model=other_model,
                )
        listing = self.app.list_chats("active")
        root_ids = [item["id"] for item in listing["chats"]]
        self.assertNotIn(created["chat"]["id"], root_ids)
        listed_parent = next(
            item for item in listing["chats"] if item["id"] == parent["id"]
        )
        self.assertEqual(listed_parent["side_chat_count"], 1)
        self.assertEqual(
            listed_parent["side_chats"][0]["id"],
            created["chat"]["id"],
        )
        with self.assertRaises(RuntimeError):
            self.app.change_chat_state(
                created["chat"]["id"],
                "archive",
                "owner",
            )
        with self.assertRaises(RuntimeError):
            self.app.update_chat_metadata(
                created["chat"]["id"],
                "owner",
                {"pinned": True},
            )
        self.app.change_chat_state(parent["id"], "archive", "owner")
        archived_child = self.app.get_chat(created["chat"]["id"])
        self.assertIsNotNone(archived_child["archived_at"])
        self.app.change_chat_state(parent["id"], "restore", "owner")
        restored_child = self.app.get_chat(created["chat"]["id"])
        self.assertIsNone(restored_child["archived_at"])

    def test_concurrent_identical_side_requests_create_one_child_and_job(self):
        parent = self.app.create_chat("并发侧聊", ".", "write", "owner")
        with self.app.db_connect() as connection:
            source_id = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, created_at
                )
                VALUES (?, 'assistant', '😀并发检查正常。', 'completed', '{}', ?)
                """,
                (parent["id"], self.app.now_iso()),
            ).lastrowid

        def create():
            return self.app.create_side_chat(
                parent["id"],
                source_id,
                "并发检查正常",
                2,
                8,
                "请解释并发检查。",
                "owner",
                "concurrent-side-request",
            )

        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: create(), range(2)))
        child_ids = {result["chat"]["id"] for result in results}
        self.assertEqual(len(child_ids), 1)
        child_id = next(iter(child_ids))
        with self.app.db_connect() as connection:
            child_count = connection.execute(
                "SELECT count(*) FROM chats WHERE parent_chat_id = ?",
                (parent["id"],),
            ).fetchone()[0]
            message_count = connection.execute(
                "SELECT count(*) FROM messages WHERE chat_id = ?",
                (child_id,),
            ).fetchone()[0]
            job_count = connection.execute(
                "SELECT count(*) FROM jobs WHERE chat_id = ?",
                (child_id,),
            ).fetchone()[0]
            connection.execute(
                """
                UPDATE jobs
                SET status = 'completed', completed_at = ?
                WHERE chat_id = ?
                """,
                (self.app.now_iso(), child_id),
            )
        self.assertEqual((child_count, message_count, job_count), (1, 1, 1))

    def test_recovery_uses_saved_execution_prompt_for_annotation_history(self):
        chat = self.app.create_chat("恢复批注", ".", "read", "owner")
        timestamp = self.app.now_iso()
        with self.app.db_connect() as connection:
            previous = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, actor_id, created_at
                )
                VALUES (
                    ?, 'user', '提交了 1 条批注', 'completed',
                    '{"annotations":[]}', 'owner', ?
                )
                """,
                (chat["id"], timestamp),
            ).lastrowid
            connection.execute(
                """
                INSERT INTO jobs(
                    id, chat_id, user_message_id, model,
                    prompt, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, 'completed', ?)
                """,
                (
                    "annotation-history-job",
                    chat["id"],
                    previous,
                    self.app.DEFAULT_MODEL,
                    "【批注 1】用户批注：请解释真实检查范围",
                    timestamp,
                ),
            )
            current = connection.execute(
                """
                INSERT INTO messages(
                    chat_id, role, content, status, meta_json, actor_id, created_at
                )
                VALUES (?, 'user', '继续', 'completed', '{}', 'owner', ?)
                """,
                (chat["id"], timestamp),
            ).lastrowid
        recovered = self.app.build_recovery_prompt(
            chat["id"],
            current,
            "继续",
        )
        self.assertIn("请解释真实检查范围", recovered)

    def test_request_id_reuse_with_different_content_is_rejected(self):
        chat = self.app.create_chat("幂等测试", ".", "read", "owner")
        with mock.patch.object(
            self.app.JOB_QUEUE,
            "put_nowait",
            return_value=None,
        ):
            self.app.enqueue_message(
                chat["id"],
                "第一条请求",
                "owner",
                "same-request-id",
            )
            with self.assertRaises(ValueError):
                self.app.enqueue_message(
                    chat["id"],
                    "不同请求",
                    "owner",
                    "same-request-id",
                )

    def test_dashboard_exposes_annotation_side_chat_and_message_rail(self):
        for marker in (
            'id="selectionToolbar"',
            'id="annotationEditor"',
            'id="annotationInput"',
            'id="sideChatPanel"',
            'id="sideMessages"',
            'id="messageRail"',
            'id="messageRailPreview"',
            "captureMessageSelection",
            "openAnnotationEditor",
            "renderMessageAnnotations",
            "composeAnnotatedPrompt",
            "sendMoreDetails",
            "openSideChat",
            "runSideTask",
            "stableSideRequestId",
            "sideViewGeneration",
            "loadOlderSideMessages",
            "loadFullSideMessage",
            "renderChatTree",
            "rebuildMessageRail",
            'await submitMainTask("", {',
            "完整原文模式暂不支持批注",
            'document.addEventListener("selectionchange"',
            "一次最多添加 12 条批注",
            'id="sideLoadOlder"',
            'aria-expanded", String(expanded)',
        ):
            self.assertIn(marker, self.app.INDEX_HTML)

    def test_android_fold_shell_bridge_and_message_download_are_exposed(self):
        for marker in (
            'id="wideLayoutToggle"',
            "window.codexDeckApplyAndroidWindowInfo = info =>",
            "window.codexDeckHandleAndroidBack = closeTopLayer;",
            'type: "openInstances"',
            'type: "jobStarted"',
            "notifyAndroidJobStarted(",
            'data-android-pane="history"',
            'data-android-pane="side"',
            "fold-separating.fold-vertical",
            "/download`",
        ):
            self.assertIn(marker, self.app.INDEX_HTML)

        chat, queued = self.enqueue_test_job(
            "android-download",
            message_content="Android 原生下载内容",
        )
        message = self.app.get_message_download(queued["message"]["id"])
        self.assertEqual(message["chat_id"], chat["id"])
        self.assertEqual(message["content"], "Android 原生下载内容")

    def test_v28_dashboard_exposes_model_selector_and_message_rail_positioning(self):
        for marker in (
            'id="model"',
            'id="modelPickerButton"',
            'id="reasoningEffort"',
            'id="speed"',
            'id="modelMenuLayer"',
            'aria-label="选择模型、推理强度和速度"',
            'api("/models"',
            "applyModelSelection(",
            "syncModelPickerSummary",
            'model: $("model").value',
            "layoutMessageRail",
            "showRailPreview",
            'const appRect = document.querySelector(".app").getBoundingClientRect();',
            "appRect.left +",
            'document.body.classList.add("rail-visible")',
            'document.body.classList.remove("rail-visible")',
            'marker.style.top = `${ratio * 100}%`',
            'marker.addEventListener("mouseenter"',
            'marker.addEventListener("focus"',
            '"aria-current"',
            "message.scrollIntoView({",
        ):
            self.assertIn(marker, self.app.INDEX_HTML)
        self.assertNotIn(
            "window.innerWidth <= 700 || messageNodes.length < 2",
            self.app.INDEX_HTML,
        )
        self.assertNotIn(
            ".message-rail { display: none !important; }",
            self.app.INDEX_HTML,
        )

    def test_message_rail_transition_listener_ignores_bubbled_marker_transitions(self):
        html = self.app.INDEX_HTML
        listener = (
            'document.querySelector(".app").addEventListener('
            '"transitionend", event => {'
        )
        start = html.index(listener)
        end = html.index("\n  });", start)
        handler = html[start:end]

        self.assertIn(
            'event.propertyName === "margin-left"',
            handler,
        )
        self.assertTrue(
            (
                "event.target === event.currentTarget" in handler
                or "event.target !== event.currentTarget" in handler
            ),
            "The .app transitionend handler must filter bubbled child events",
        )
        self.assertEqual(handler.count("rebuildMessageRail()"), 0)
        self.assertEqual(handler.count("layoutMessageRail()"), 1)

        # Rail markers animate only transforms, so the hot path does not
        # repeatedly lay out width and margin changes while the page scrolls.
        self.assertIn(".rail-marker::before", html)
        marker_css = html.split("    .rail-marker::before {", 1)[1].split(
            "    .rail-marker.user::before", 1
        )[0]
        active_css = html.split(
            "    .rail-marker.active::before {", 1
        )[1].split("    .rail-marker.scrub-target::before", 1)[0]
        self.assertIn("transition: transform", marker_css)
        self.assertIn("transform: scale", active_css)
        self.assertNotIn("transition: width", marker_css)
        self.assertNotIn("width:", active_css)
        self.assertNotIn("margin-left:", active_css)

    def test_message_rail_rebuild_preserves_active_marker_state(self):
        html = self.app.INDEX_HTML

        self.assertIn(
            'const previousActiveId = track.querySelector(".rail-marker.active")',
            html,
        )
        self.assertIn(
            "marker.dataset.messageId === previousActiveId",
            html,
        )
        self.assertIn(
            'marker.setAttribute("aria-current", "true")',
            html,
        )
        self.assertIn(
            "scheduleMessageRailLayout();",
            html,
        )
        self.assertIn("let railLayoutFrame = null", html)

    def test_message_rail_coalesces_and_smooths_pointer_scrubbing(self):
        html = self.app.INDEX_HTML
        for marker in (
            "touch-action: none",
            ".message-rail.scrubbing",
            ".rail-marker.scrub-target::before",
            "const RAIL_DRAG_THRESHOLD = 7",
            "const RAIL_SETTLE_DURATION = 120",
            "const RAIL_SCRUB_TIME_CONSTANT = 46",
            "const RAIL_MAX_RENDERED_SPEED = 1100",
            "const RAIL_MAX_SCROLL_VIEWPORTS_PER_SECOND = 36",
            "const RAIL_SETTLE_MAX_VIEWPORTS = .75",
            "const RAIL_REVERSE_DEADZONE = 1.5",
            "const captureRailScrubGeometry = () =>",
            "const railViewportCenter = () =>",
            "const railScrollTopAt = clientY =>",
            "const railSampleAtScrollTop = scrollTop =>",
            "const scrubRailAt = (clientY, elapsed = 1000 / 60) =>",
            "const updateRailScrubInput = (clientY, force = false) =>",
            "const runRailScrubFrame = timestamp =>",
            "const scheduleRailScrub = (clientY, force = false) =>",
            "const settleRailTo = (sample, requestedTop) =>",
            "const finishRailScrub = (holdPreview = false) =>",
            "if (delay <= 0)",
            "railLayoutDirty = true",
            "railRebuildDirty = true",
            "railViewportSyncDirty = true",
            'window.scrollTo({top, behavior: "auto"})',
            "railSuppressClickUntil = Date.now() + 600;",
            'addEventListener("pointerdown"',
            'addEventListener("pointermove"',
            'addEventListener("pointerup"',
            'addEventListener("pointercancel"',
            'addEventListener("lostpointercapture"',
            "setPointerCapture(event.pointerId)",
            "Math.abs(event.clientY - railPointerStartY)",
            "if (distance < RAIL_DRAG_THRESHOLD) return;",
            "settleRailTo(sample, requestedTop);",
            "marker.dataset.railIndex = String(index + 1)",
            "const handleVisualViewportChange = () =>",
            "visualViewportGeometry() !== railViewportGeometry",
        ):
            self.assertIn(marker, html)
        pointer_down = html.split(
            '$("messageRail").addEventListener("pointerdown"', 1
        )[1].split(
            '$("messageRail").addEventListener("pointermove"', 1
        )[0]
        self.assertIn("captureRailScrubGeometry()", pointer_down)
        self.assertIn("railScrubRenderedY = event.clientY", pointer_down)
        self.assertIn("scrollRequestGeneration += 1", pointer_down)
        self.assertNotIn("scrubRailAt(event.clientY)", pointer_down)
        self.assertNotIn("scrollTo(", pointer_down)
        self.assertNotIn("pointerup", pointer_down)

        scrub = html.split(
            "const scrubRailAt = (clientY, elapsed = 1000 / 60) =>", 1
        )[1].split(
            "const updateRailScrubInput = (clientY, force = false) =>", 1
        )[0]
        self.assertIn("railScrollTopAt(clientY)", scrub)
        self.assertIn("const maxScrollStep = Math.max(", scrub)
        self.assertIn("return Math.abs(requestedTop - top) >= .5", scrub)
        self.assertNotIn("getBoundingClientRect", scrub)
        self.assertNotIn("querySelectorAll", scrub)

        scheduler = html.split(
            "const scheduleRailScrub = (clientY, force = false) =>", 1
        )[1].split("const markRailActiveSample", 1)[0]
        self.assertIn("updateRailScrubInput(clientY, force)", scheduler)
        self.assertEqual(
            scheduler.count("requestAnimationFrame(runRailScrubFrame)"),
            1,
        )
        self.assertNotIn("scrollTo(", scheduler)

        pointer_up = html.split(
            '$("messageRail").addEventListener("pointerup"', 1
        )[1].split(
            '$("messageRail").addEventListener("pointercancel"', 1
        )[0]
        self.assertIn(
            "if (railScrubStarted) updateRailScrubInput(event.clientY)",
            pointer_up,
        )
        self.assertIn("(railScrubClientY ?? event.clientY)", pointer_up)
        self.assertIn("railScrollTopAt(releaseY)", pointer_up)
        self.assertIn("window.innerHeight * RAIL_SETTLE_MAX_VIEWPORTS", pointer_up)
        self.assertIn("railSampleAtScrollTop(requestedTop) || sample", pointer_up)
        self.assertNotIn("updateRailScrubInput(event.clientY, true)", pointer_up)
        self.assertNotIn("scrubRailAt(event.clientY)", pointer_up)

        scrub_frame = html.split(
            "const runRailScrubFrame = timestamp =>", 1
        )[1].split("const scheduleRailScrub", 1)[0]
        self.assertIn("const maxRenderedStep = Math.max(", scrub_frame)
        self.assertIn("Math.min(Math.abs(blendedStep), maxRenderedStep)", scrub_frame)
        self.assertIn("|| scrollCatchingUp", scrub_frame)

        settle = html.split(
            "const settleRailTo = (sample, requestedTop) =>", 1
        )[1].split("const finishRailScrub", 1)[0]
        step = settle.split("const step = timestamp =>", 1)[1]
        self.assertEqual(step.count("window.scrollTo({"), 1)
        self.assertIn("const maxScrollStep = Math.max(", step)
        self.assertIn("Math.min(", step)
        self.assertNotIn("currentRailSnapTop", settle)

        pointer_cancel = html.split(
            '$("messageRail").addEventListener("pointercancel"', 1
        )[1].split(
            '$("messageRail").addEventListener("lostpointercapture"', 1
        )[0]
        self.assertIn("finishRailScrub(false)", pointer_cancel)
        self.assertNotIn("snapRailTo", pointer_cancel)

        pending_stream = html.split("const renderPendingStream", 1)[1].split(
            "const startJobEventStream", 1
        )[0]
        self.assertIn(
            "const follow = !railInteractionActive() && isNearPageBottom()",
            pending_stream,
        )
        self.assertIn("if (follow && !railInteractionActive())", pending_stream)

    def test_message_rail_blocks_deferred_auto_follow_and_viewport_feedback(self):
        html = self.app.INDEX_HTML
        bottom_scroll = html.split(
            "const scrollConversationToBottom", 1
        )[1].split("const abortError", 1)[0]
        incremental = html.split(
            "const appendIncrementalMessages", 1
        )[1].split("const stopChatSync", 1)[0]
        poll_job = html.split("async function pollJob", 1)[1].split(
            "async function cancelCurrentJob", 1
        )[0]
        viewport_change = html.split(
            "const handleVisualViewportChange = () =>", 1
        )[1].split("if (window.visualViewport)", 1)[0]

        self.assertIn("|| railInteractionActive()", bottom_scroll)
        self.assertIn("if (!railInteractionActive())", incremental)
        self.assertIn("if (!railInteractionActive())", poll_job)
        self.assertIn("railViewportSyncDirty = true", viewport_change)
        self.assertIn("return;", viewport_change)

    def test_mobile_keyboard_tracks_visual_viewport_and_focuses_side_prompt_once(self):
        html = self.app.INDEX_HTML
        for marker in (
            "--vv-top: 0px",
            "--vv-height: 100dvh",
            "#sidePrompt, #annotationInput { font-size: 16px; }",
            "(any-pointer: coarse)",
            "body.keyboard-open .composer-wrap",
            "body.keyboard-open .side-chat-panel",
            "const syncViewportGeometry = () =>",
            "const scheduleViewportSync = () =>",
            'window.visualViewport.addEventListener("resize", handleVisualViewportChange',
            'window.visualViewport.addEventListener("scroll", handleVisualViewportChange',
            'document.body.classList.toggle("keyboard-open", keyboardOpen)',
            "viewportBaselineHeight - height",
            "const focusSidePrompt = () =>",
            "prompt.focus({preventScroll: true})",
            "let bodyLockScrollY = null",
            'window.scrollTo({top: restoreY, behavior: "auto"})',
        ):
            self.assertIn(marker, html)

        new_side_chat = html.split("function openSideChat", 1)[1].split(
            "async function pollSideJob", 1
        )[0]
        self.assertIn("openSidePanel();", new_side_chat)
        self.assertIn("focusSidePrompt();", new_side_chat)
        self.assertNotIn("setTimeout", new_side_chat)

        stored_side_chat = html.split("async function openStoredSideChat", 1)[1].split(
            "async function reopenSavedSideChat", 1
        )[0]
        self.assertLess(
            stored_side_chat.index("focusSidePrompt();"),
            stored_side_chat.index("await loadSideChat"),
        )
        self.assertNotIn("setTimeout", stored_side_chat)

    def test_job_ui_exposes_elapsed_time_and_manual_stop(self):
        html = self.app.INDEX_HTML
        for marker in (
            'elapsed.id = "pendingElapsed"',
            'elapsed.className = "pending-elapsed"',
            "const formatElapsed = value =>",
            "setInterval(renderPendingState, 1000)",
            'runButton.textContent = submitInFlight ? "…" : (running ? "■" : "↑")',
            'running ? "停止当前任务" : "运行 Codex"',
            "async function cancelCurrentJob()",
            "if (running) {",
            "cancelCurrentJob();",
            "body: JSON.stringify({})",
            "job.status === \"cancelled\"",
        ):
            self.assertIn(marker, html)
        self.assertIn(
            "await api(`/jobs/${jobId}/cancel`",
            html,
        )

    def test_submit_button_guards_double_click_without_pointer_business_action(self):
        html = self.app.INDEX_HTML
        for marker in (
            "const STOP_GUARD_MS = 800",
            "let submitInFlight = false",
            "let stopGuardUntil = 0",
            "const armStopGuard = () =>",
            "stopGuardUntil = Date.now() + STOP_GUARD_MS",
            "if (submitInFlight) return false",
            "submitInFlight = true",
            "armStopGuard();\n      startPolling(queued.job.id);",
            "submitInFlight = false;\n      refreshRunState();",
        ):
            self.assertIn(marker, html)

        cancel_handler = html.split(
            "async function cancelCurrentJob() {", 1
        )[1].split(
            "async function finishConnection", 1
        )[0]
        self.assertIn("submitInFlight", cancel_handler)
        self.assertIn("Date.now() < stopGuardUntil", cancel_handler)

        run_pointer_handler = html.split(
            '$("run").addEventListener("pointerdown"', 1
        )[1].split(
            '$("run").addEventListener("pointerup"', 1
        )[0]
        self.assertIn('classList.add("pressed")', run_pointer_handler)
        self.assertNotIn("runTask()", run_pointer_handler)
        self.assertNotIn("cancelCurrentJob()", run_pointer_handler)

        run_click_handler = html.split(
            '$("run").addEventListener("click"', 1
        )[1].split(
            'document.querySelectorAll(".quick")', 1
        )[0]
        self.assertIn("if (submitInFlight) return", run_click_handler)
        self.assertIn("Date.now() < stopGuardUntil", run_click_handler)
        self.assertIn("cancelCurrentJob();", run_click_handler)
        self.assertIn("runTask();", run_click_handler)

    def test_mobile_quick_dock_reopens_saved_side_chat_with_press_feedback(self):
        html = self.app.INDEX_HTML
        for marker in (
            'id="mobileQuickDock"',
            'id="reopenSideChat"',
            'id="sideChatCount"',
            'id="recentChatToggle"',
            "async function reopenSavedSideChat()",
            "savedSideChatsForActiveChat()",
            "lastSideChatStorageKey(parent.id)",
            "sideChats.find(chat => chat.id === remembered) || sideChats[0]",
            "await openStoredSideChat(target, parent)",
            "rememberOpenedSideChat(loaded)",
            '$("reopenSideChat").addEventListener("click", reopenSavedSideChat)',
            '@media (hover: none), (pointer: coarse)',
            "@media (max-width: 959px)",
            "width: 44px; height: 44px",
            "touch-action: manipulation",
        ):
            self.assertIn(marker, html)

        dock_pointer_handler = html.split(
            'button.addEventListener("pointerdown"', 1
        )[1].split(
            'button.addEventListener("pointerup"', 1
        )[0]
        self.assertIn('classList.add("pressed")', dock_pointer_handler)
        self.assertNotIn("reopenSavedSideChat", dock_pointer_handler)
        self.assertNotIn("toggleRecentChat", dock_pointer_handler)

    def test_recent_chat_stack_blocks_drafts_and_skips_invalid_targets(self):
        html = self.app.INDEX_HTML
        for marker in (
            "const RECENT_MAIN_CHAT_LIMIT = 12",
            "let recentMainChatIds = []",
            "const recentMainChatStorageKey = () =>",
            "const recordMainChatVisit = chatId =>",
            "const forgetRecentMainChat = chatId =>",
            "const hasUnsentDraft = () => Boolean(",
            '$("prompt").value.trim()',
            "|| stagedAttachments.length",
            '|| (sideParentChatId && $("sidePrompt").value.trim())',
            "recentButton.disabled = recentChatSwitchInFlight || submitInFlight",
            "async function toggleRecentChat()",
            "if (recentChatSwitchInFlight || submitInFlight) return;",
            'toast("请先发送或清空当前草稿")',
            "const candidates = recentMainChatIds.filter(chatId => chatId !== originId)",
            "for (const chatId of candidates)",
            "requireActive: true",
            "skipMissing: true",
            'result === "missing" || result === "inactive"',
            "forgetRecentMainChat(chatId);\n          continue;",
            'return options.skipMissing ? "superseded" : undefined',
            'if (result === "superseded")',
            "if (!switched && !superseded && originId)",
            "await loadChat(originId);",
            '$("recentChatToggle").addEventListener("click", toggleRecentChat)',
        ):
            self.assertIn(marker, html)

    def test_archiving_active_chat_advances_in_visible_list_order(self):
        html = self.app.INDEX_HTML
        helper = html.split(
            "const visibleMainChatIds = () =>", 1
        )[1].split(
            "async function changeChatState", 1
        )[0]
        handler = html.split(
            "async function changeChatState", 1
        )[1].split(
            "const refreshCategoryControls", 1
        )[0]

        for marker in (
            '.querySelectorAll(".chat-item:not(.side-chat)")',
            "const index = ids.indexOf(chatId)",
            "...ids.slice(index + 1)",
            "...ids.slice(0, index).reverse()",
        ):
            self.assertIn(marker, helper)

        for marker in (
            'action === "archive"',
            'chatView === "active"',
            "activeChat.id === chatId",
            "if (archivingActiveChat && hasUnsentDraft())",
            'toast("请先发送或清空当前草稿")',
            "const navigationCandidates = archivingActiveChat",
            "await loadChats()",
            "for (const candidateId of navigationCandidates)",
            "requireActive: true",
            "skipMissing: true",
            'result === true || result === "superseded"',
            "closeSideChat()",
            "resetToNewChat()",
        ):
            self.assertIn(marker, handler)
        self.assertIn(
            'sideParentChatId && $("sidePrompt").value.trim()',
            html,
        )

        self.assertLess(
            handler.index("const navigationCandidates = archivingActiveChat"),
            handler.index("await api("),
        )
        self.assertLess(handler.index("await api("), handler.index("await loadChats()"))
        self.assertLess(
            handler.index("await loadChats()"),
            handler.index("for (const candidateId of navigationCandidates)"),
        )
        catch_body = handler.split("} catch (error) {", 1)[1]
        self.assertNotIn("loadChat(", catch_body)
        self.assertNotIn("resetToNewChat(", catch_body)
        self.assertNotIn("setChatControls(", catch_body)

    def test_dashboard_exposes_main_side_streaming_and_fleet_warning(self):
        html = self.app.render_index_html(
            instance_id="hostinger",
            instance_switch_url="https://deck-b.example.test",
        )
        for marker in (
            'stream.id = "pendingStream"',
            "const startJobEventStream = (jobId, generation) =>",
            'source.addEventListener("delta"',
            "const startSideJobEventStream = (",
            'stream.id = "sidePendingStream"',
            "closeJobEventStream();",
            "closeSideJobEventStream();",
            "两台版本不同",
            "发布包不同",
            "另一台异常",
            "credentials: \"include\"",
            'id="fleetAlert"',
            ".mark.fleet-warning::after",
        ):
            self.assertIn(marker, html)
        self.assertNotIn("__INSTANCE_SWITCH_ORIGIN__", html)

    def test_process_job_streams_deltas_but_persists_one_final_message(self):
        chat, queued = self.enqueue_test_job("streamed-final")
        job_id = queued["job"]["id"]

        def streamed_result(*_args, **kwargs):
            kwargs["on_delta"]("第一句")
            kwargs["on_delta"]("第二句")
            return self.codex_result(output="第一句第二句")

        with mock.patch.object(
            self.app,
            "execute_codex",
            side_effect=streamed_result,
        ):
            self.assertTrue(self.app.process_job(job_id))
        snapshot = self.app.JOB_STREAMS.snapshot(job_id)
        self.assertEqual(snapshot.text, "第一句第二句")
        self.assertTrue(snapshot.terminal)
        with self.app.db_connect() as connection:
            rows = connection.execute(
                """
                SELECT role, content FROM messages
                WHERE chat_id = ? AND role = 'assistant'
                """,
                (chat["id"],),
            ).fetchall()
        self.assertEqual(
            [(row["role"], row["content"]) for row in rows],
            [("assistant", "第一句第二句")],
        )

    def test_message_rail_is_visually_quiet_until_scrubbing(self):
        html = self.app.INDEX_HTML
        rail_css = html.split("    .message-rail {", 1)[1].split(
            "    .message-rail[hidden]", 1
        )[0]
        track_css = html.split(
            "    .message-rail-track::before {", 1
        )[1].split("    .rail-marker {", 1)[0]
        scrub_css = html.split("    .message-rail.scrubbing {", 1)[1].split(
            "    .message-rail-track {", 1
        )[0]
        self.assertIn("border: 1px solid transparent", rail_css)
        self.assertIn("background: transparent", rail_css)
        self.assertIn("box-shadow: none", rail_css)
        self.assertIn("display: none", track_css)
        self.assertIn("border-color: rgba(161,161,170,.2)", scrub_css)
        self.assertNotIn("body.rail-visible main { padding-left:", html)

    def test_main_submission_switch_only_clears_the_submitted_chat_drafts(self):
        expected = """
      if (clearSubmittedAnnotations) {
        localStorage.removeItem(annotationDraftKey(targetChatId));
        if (activeChat && activeChat.id === targetChatId) {
          annotationDrafts = [];
          renderAnnotationDraftSummary();
          renderMessageAnnotations();
        }
      }
      if (generation !== viewGeneration || !activeChat || activeChat.id !== targetChatId) return;
"""
        self.assertIn(expected, self.app.INDEX_HTML)

    def test_side_older_page_ignores_stale_view_and_load_generations(self):
        for marker in (
            "sideViewGeneration += 1;",
            "sideOlderLoadGeneration += 1;",
            "const generation = sideViewGeneration;",
            "const loadGeneration = ++sideOlderLoadGeneration;",
            "generation !== sideViewGeneration",
            "loadGeneration !== sideOlderLoadGeneration",
            "sideChat.id !== chatId",
            "sideOldestMessageId !== beforeId",
        ):
            self.assertIn(marker, self.app.INDEX_HTML)

    def test_opened_chat_scrolls_to_latest_messages_after_layout_settles(self):
        self.assertIn(
            "const scrollConversationToBottom = async",
            self.app.INDEX_HTML,
        )
        self.assertIn(
            "await scrollConversationToBottom(generation)",
            self.app.INDEX_HTML,
        )
        self.assertIn(
            'history.scrollRestoration = "manual"',
            self.app.INDEX_HTML,
        )

    def test_pairing_code_is_one_time_and_never_stored_in_plaintext(self):
        pairing = self.app.create_pairing_code(
            "owner",
            requested_device_name="主力 iPhone",
        )
        compact_code = self.app.normalize_pairing_code(pairing["code"])
        self.assertEqual(len(compact_code), self.app.PAIRING_CODE_LENGTH)
        self.assertEqual(
            pairing["pair_url"],
            (
                "https://deck-vps.example.ts.net/"
                f"#pair={pairing['code']}"
            ),
        )
        with self.app.db_connect() as connection:
            columns = self.app.table_columns(connection, "pairing_codes")
            stored = connection.execute(
                "SELECT * FROM pairing_codes"
            ).fetchone()
        self.assertNotIn("code", columns)
        self.assertEqual(
            stored["code_hash"],
            self.app.pairing_code_hash(compact_code),
        )
        self.assertNotIn(compact_code, json.dumps(dict(stored)))

        actor, session_token = self.app.redeem_pairing_code(
            pairing["code"],
            "",
        )
        self.assertEqual(actor["device_name"], "主力 iPhone")
        self.assertEqual(
            self.app.authenticate_device_session(session_token)["id"],
            "owner",
        )
        with self.assertRaises(self.app.PairingCodeError):
            self.app.redeem_pairing_code(pairing["code"], "重放设备")
        with self.app.db_connect() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM device_sessions"
                ).fetchone()[0],
                1,
            )
            self.assertIsNotNone(
                connection.execute(
                    "SELECT consumed_at FROM pairing_codes"
                ).fetchone()[0]
            )

    def test_concurrent_pairing_redemption_has_exactly_one_winner(self):
        pairing = self.app.create_pairing_code(
            "owner",
            requested_device_name="并发手机",
        )
        barrier = self.app.threading.Barrier(4)

        def redeem_once(index):
            barrier.wait()
            try:
                self.app.redeem_pairing_code(
                    pairing["code"],
                    f"并发手机 {index}",
                )
                return "success"
            except self.app.PairingCodeError:
                return "invalid"

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(redeem_once, range(4)))
        self.assertEqual(results.count("success"), 1)
        self.assertEqual(results.count("invalid"), 3)
        with self.app.db_connect() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM device_sessions"
                ).fetchone()[0],
                1,
            )

    def test_three_devices_are_independent_and_one_can_be_revoked(self):
        sessions = []
        for name in ("主力 iPhone", "备用 iPhone", "折叠屏 Android"):
            pairing = self.app.create_pairing_code(
                "owner",
                requested_device_name=name,
            )
            actor, token = self.app.redeem_pairing_code(
                pairing["code"],
                "",
            )
            sessions.append((actor, token))

        self.assertEqual(
            len({actor["device_id"] for actor, _ in sessions}),
            3,
        )
        self.assertEqual(len({token for _, token in sessions}), 3)
        devices = self.app.list_device_sessions(
            "owner",
            sessions[0][0]["device_id"],
        )
        self.assertEqual(len(devices), 3)
        self.assertTrue(devices[0].keys() >= {
            "id",
            "name",
            "created_at",
            "last_seen_at",
            "expires_at",
            "current",
        })
        self.assertNotIn("session_hash", devices[0])
        self.assertNotIn("token", devices[0])

        self.app.revoke_device_session(
            "owner",
            sessions[1][0]["device_id"],
        )
        self.assertIsNotNone(
            self.app.authenticate_device_session(sessions[0][1])
        )
        self.assertIsNone(
            self.app.authenticate_device_session(sessions[1][1])
        )
        self.assertIsNotNone(
            self.app.authenticate_device_session(sessions[2][1])
        )
        self.assertEqual(
            len(self.app.list_device_sessions("owner")),
            2,
        )

    def test_device_limit_rejects_ninth_device_without_evicting_others(self):
        sessions = [
            self.app.create_device_session(
                "owner",
                f"设备 {index + 1}",
                return_details=True,
            )
            for index in range(self.app.MAX_DEVICE_SESSIONS)
        ]
        with self.assertRaises(self.app.DeviceSessionLimitError):
            self.app.create_device_session(
                "owner",
                "第九台设备",
                return_details=True,
            )
        for session in sessions:
            self.assertIsNotNone(
                self.app.authenticate_device_session(session["token"])
            )
        self.app.revoke_device_session("owner", sessions[0]["id"])
        replacement = self.app.create_device_session(
            "owner",
            "替换设备",
            return_details=True,
        )
        self.assertIsNotNone(
            self.app.authenticate_device_session(replacement["token"])
        )

    def test_device_session_renews_in_place_and_cookie_flags_are_stable(self):
        created = self.app.create_device_session(
            "owner",
            "续期手机",
            return_details=True,
        )
        fixed_now = self.app.datetime(
            2027,
            1,
            2,
            3,
            4,
            5,
            tzinfo=self.app.timezone.utc,
        )
        with self.app.db_connect() as connection:
            original_hash = connection.execute(
                "SELECT session_hash FROM device_sessions WHERE id = ?",
                (created["id"],),
            ).fetchone()[0]
            connection.execute(
                """
                UPDATE device_sessions
                SET last_seen_at = ?, expires_at = ?
                WHERE id = ?
                """,
                (
                    self.app.timestamp_iso(
                        fixed_now - self.app.timedelta(hours=7)
                    ),
                    self.app.timestamp_iso(
                        fixed_now + self.app.timedelta(days=1)
                    ),
                    created["id"],
                ),
            )
        handler = FakeHandler(
            {
                "Cookie": (
                    f"{self.app.DEVICE_SESSION_COOKIE_NAME}="
                    f"{created['token']}"
                )
            }
        )
        with mock.patch.object(
            self.app,
            "utc_now",
            return_value=fixed_now,
        ):
            actor = self.app.authenticate_device_session(
                created["token"],
                handler=handler,
            )
        self.assertEqual(actor["device_id"], created["id"])
        self.assertEqual(
            handler.device_session_refresh_token,
            created["token"],
        )
        with self.app.db_connect() as connection:
            renewed = connection.execute(
                """
                SELECT session_hash, expires_at
                FROM device_sessions
                WHERE id = ?
                """,
                (created["id"],),
            ).fetchone()
        self.assertEqual(renewed["session_hash"], original_hash)
        self.assertEqual(
            renewed["expires_at"],
            self.app.timestamp_iso(
                fixed_now
                + self.app.timedelta(
                    days=self.app.DEVICE_SESSION_TTL_DAYS
                )
            ),
        )

        cookie = self.app.device_session_cookie_header(created["token"])
        for attribute in (
            "Path=/",
            "Max-Age=31536000",
            "HttpOnly",
            "SameSite=Lax",
            "Secure",
        ):
            self.assertIn(attribute, cookie)
        self.assertNotIn("Domain=", cookie)
        cleared = self.app.clear_device_session_cookie_header()
        self.assertIn("Max-Age=0", cleared)
        self.assertIn("Expires=Thu, 01 Jan 1970 00:00:00 GMT", cleared)
        self.assertIn("Path=/", cleared)

    def test_expired_pairing_and_device_sessions_are_rejected(self):
        pairing = self.app.create_pairing_code(
            "owner",
            requested_device_name="过期手机",
        )
        session = self.app.create_device_session(
            "owner",
            "过期会话",
            return_details=True,
        )
        expired_at = self.app.timestamp_iso(
            self.app.utc_now() - self.app.timedelta(seconds=1)
        )
        with self.app.db_connect() as connection:
            connection.execute(
                "UPDATE pairing_codes SET expires_at = ?",
                (expired_at,),
            )
            connection.execute(
                "UPDATE device_sessions SET expires_at = ?",
                (expired_at,),
            )
        with self.assertRaises(self.app.PairingCodeError):
            self.app.redeem_pairing_code(pairing["code"], "")
        self.assertIsNone(
            self.app.authenticate_device_session(session["token"])
        )

    def test_mobile_login_ui_distinguishes_network_loss_from_auth_loss(self):
        html = self.app.INDEX_HTML
        for marker in (
            'id="pairingCodeInput"',
            'id="deviceList"',
            'id="revokeOtherDevices"',
            'location.hash.startsWith("#")',
            "history.replaceState(",
            "error.network = true",
            "error.network = true;\n      setReconnecting();\n      scheduleDeviceRestore();",
            "if (error.status === 401)",
            "scheduleDeviceRestore();",
            "startDeviceHeartbeat();",
            "deviceHeartbeatTimer = setInterval(",
            'window.addEventListener("offline", () => {',
            'rel="manifest"',
        ):
            self.assertIn(marker, html)
        self.assertEqual(self.app.APP_VERSION, "2.21.1")

    def test_scheduler_parallelizes_independent_workspaces(self):
        scheduler = self.app.JobScheduler(queue_capacity=10, max_running=2)
        scheduler.put_nowait("a", "/workspace/a", "write")
        scheduler.put_nowait("b", "/workspace/b", "write")
        first = scheduler.claim(timeout=0)
        second = scheduler.claim(timeout=0)
        self.assertEqual({first[0], second[0]}, {"a", "b"})
        self.assertEqual(scheduler.snapshot()["active_jobs"], 2)
        scheduler.complete(first)
        scheduler.complete(second)

    def test_scheduler_can_remove_a_pending_job(self):
        scheduler = self.app.JobScheduler(queue_capacity=3, max_running=1)
        scheduler.put_nowait("cancel-me", "/workspace/a", "write")
        removed = scheduler.cancel_pending("cancel-me")
        self.assertEqual(removed[0], "cancel-me")
        self.assertEqual(scheduler.snapshot()["queued_jobs"], 0)
        with self.assertRaises(queue.Empty):
            scheduler.claim(timeout=0)

    def test_scheduler_serializes_conflicting_workspace_writes(self):
        scheduler = self.app.JobScheduler(queue_capacity=10, max_running=2)
        scheduler.put_nowait("write-a", "/workspace/a", "write")
        scheduler.put_nowait("read-a", "/workspace/a", "read")
        scheduler.put_nowait("write-b", "/workspace/b", "write")
        first = scheduler.claim(timeout=0)
        second = scheduler.claim(timeout=0)
        self.assertEqual(first[0], "write-a")
        self.assertEqual(second[0], "write-b")
        with self.assertRaises(queue.Empty):
            scheduler.claim(timeout=0)
        scheduler.complete(first)
        third = scheduler.claim(timeout=0)
        self.assertEqual(third[0], "read-a")
        scheduler.complete(second)
        scheduler.complete(third)

    def test_scheduler_allows_parallel_reads_in_one_workspace(self):
        scheduler = self.app.JobScheduler(queue_capacity=10, max_running=2)
        scheduler.put_nowait("read-a-1", "/workspace/a", "read")
        scheduler.put_nowait("read-a-2", "/workspace/a", "read")
        first = scheduler.claim(timeout=0)
        second = scheduler.claim(timeout=0)
        self.assertEqual({first[0], second[0]}, {"read-a-1", "read-a-2"})
        scheduler.complete(first)
        scheduler.complete(second)


if __name__ == "__main__":
    unittest.main()
