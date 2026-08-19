# 更新、回滚和排错

## 日常检查

```bash
sudo bash /opt/codex-deck/current/deploy/diagnose.sh
```

该脚本显示 systemd、无凭据的健康 JSON、Deck/CLI 版本、`codex login status`
和最近日志。它不会打印 Owner Token 或读取 `auth.json`。

单独查看：

```bash
curl -fsS http://127.0.0.1:8788/api/health
sudo systemctl status codex-deck.service --no-pager
sudo journalctl -u codex-deck.service -n 80 --no-pager
```

## 更新

1. 下载并验证新的源码包，进入新源码目录。
2. 确认没有正在运行或排队的任务。
3. 执行：

```bash
sudo bash deploy/install.sh
```

安装器不修改旧 release，而是创建新目录和独立 venv。切换前会再次确认队列，
停服后检查 SQLite 中没有刚进入的任务，再创建一致性备份；
新版本健康检查失败会自动恢复上一份源码、venv 和 bundled CLI。

不要在 `/opt/codex-deck/current` 中执行 `git pull` 或 `pip install -U`。不要在
运行状态下只复制 WAL 模式 SQLite 的主文件。

个人版目前没有单独的 maintenance/drain API。空闲检查与停服之间仍有一个
极短窗口：若此时刚好提交新任务，安装器会在停服后发现数据库不空、放弃切版
并恢复旧服务，但这个竞态任务可能已被短暂中断。因此开始升级后不要再从网页
提交任务；只有长期多人使用时才值得增加应用侧原子维护模式。

## 回滚源码

先列出版本：

```bash
sudo bash /opt/codex-deck/current/deploy/rollback.sh
```

然后传入一个 release 目录名：

```bash
sudo bash /opt/codex-deck/current/deploy/rollback.sh \
  v2.21.2-20260819T120000Z
```

回滚只切换源码、venv、CLI 和对应 systemd unit，默认保留当前 SQLite、上传、工作区和 Codex
登录态。这样不会因为回退代码而自动丢失刚产生的对话。

目标版本必须同时通过 HTTP、`status=ok` 和目标版本号校验；失败时脚本会把
原来的 `current` 和 unit 切回并重启，避免停在半回滚状态。

如果数据库结构真的不兼容，应先停服务、再次备份并人工评估。恢复旧 SQLite
会丢失备份时间之后的对话，因此没有放进一键回滚脚本。

## 数据备份

聊天、上传和 Codex 会话都开始有长期价值时，一起备份：

- `/var/lib/codex-deck/codex.sqlite3`（使用 SQLite backup API）；
- `/var/lib/codex-deck/uploads/`；
- `/root/.codex/`；
- 需要保留的工作目录。

`deploy/install.sh` 每次更新只自动备份 SQLite，位置是
`/var/lib/codex-deck/backups/`。它不上传备份，也不替代完整灾难恢复方案。

## 常见问题

### 页面能开，任务失败

```bash
sudo -i
source /etc/codex-deck.env
"$CODEX_BIN" login status
exit
sudo journalctl -u codex-deck.service -n 80 --no-pager
```

通常是 Codex 未登录、朋友账户没有配置模型、OpenAI 出站网络失败，或选中的
工作区不存在。

### SDK 与 CLI 版本

`requirements.txt` 固定 `openai-codex`。每个 release 的 venv 同时安装对应
`openai-codex-cli-bin`，服务和登录均通过
`/opt/codex-deck/current/bin/codex` 使用这份 bundled CLI。`/api/health` 的
`runtime_details` 会同时显示 SDK 和 CLI 版本。

系统里另一个全局 `codex` 不参与 Deck 运行，即使它版本不同也不代表当前
Deck 漂移。升级源码时按新 `requirements.txt` 创建新 release，二者会一起
切换或一起回滚。

### 手机打不开

检查手机是否在同一 Tailnet、`tailscale serve status` 是否指向
`http://127.0.0.1:8788`、`CODEX_WEB_PUBLIC_URL` 是否是输出的精确 HTTPS
Origin，以及 `CODEX_WEB_COOKIE_SECURE=1`。不要改用 Funnel。

### 端口冲突

优先查清已有进程。确需改端口时，同时修改 `/etc/codex-deck.env`、SSH 隧道
或 Tailscale Serve 目标，然后重启。不要直接杀掉未知进程。
