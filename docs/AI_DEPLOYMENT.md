# 把部署交给 AI

把整个源码目录或分享压缩包交给能操作朋友 VPS 的 AI，然后粘贴下面的提示词。
默认先只读核查，AI 必须汇报并停下；朋友回复“继续安装”后，AI 才能写入
系统。这样既保持低摩擦，也避免覆盖 VPS 上已有服务。

先替换三个输入：

```text
SOURCE_PATH=<VPS 上源码目录或压缩包的绝对路径>
EXPECTED_VERSION=2.21.2
ACCESS_MODE=ssh
```

需要手机/iPad 访问时，把 `ACCESS_MODE` 改为 `tailscale`。

## 可直接复制的部署提示词

```text
你要在这台 VPS 上部署 Codex Deck。它只用于个人探索、学习和测试，优先最短
流程，不引入 Docker、Kubernetes、Nginx、Traefik、面板或额外认证系统。

输入：
SOURCE_PATH=<填写绝对路径>
EXPECTED_VERSION=2.21.2
ACCESS_MODE=<ssh 或 tailscale>

目标配置：Ubuntu 24.04、systemd、root 运行、CODEX_WORKSPACE_ROOT=/、
CODEX_WEB_UNRESTRICTED_WRITE=true、两个并行任务、只监听 127.0.0.1:8788。
这是一台专用测试 VPS。不得监听 0.0.0.0，不得开放 8788 公网端口，不得使用
Tailscale Funnel。若要求公网域名，停止并另行征得明确同意。

必须分两阶段。

第一阶段只能只读核查，不安装、不写文件、不启动/停止服务：

1. 检查 hostname、whoami、pwd、Ubuntu 版本、架构、CPU、内存、swap、磁盘。
2. 检查 systemd、python3、python3-venv、curl、git、openssl、Tailscale。
3. 检查 127.0.0.1:8788、codex-deck.service 以及 /opt/codex-deck、
   /var/lib/codex-deck、/etc/codex-deck.env 是否已存在；不要读取秘密内容。
4. 检查 SOURCE_PATH 是否包含 README.md、AGENTS.md、requirements.txt、
   codex_web.py、codex_runtime.py、job_stream.py、test_codex_sso.py、deploy/ 和 docs/。
5. 读取 APP_VERSION 和 requirements.txt，确认 EXPECTED_VERSION 完全一致。
6. 扫描分享目录是否意外包含 .env、auth.json、.codex、Token、SQLite、上传、
   日志或备份；只报告文件名，不输出秘密内容。
7. 如果已有 Codex，只允许执行其 --version 和 login status，不读取 auth.json。
8. 报告网络是否能访问 Ubuntu 软件源、PyPI 和官方 OpenAI 登录/服务。

按以下格式汇报：

READ_ONLY_AUDIT
- 主机和资源：
- 必要命令：
- 端口/服务冲突：
- 目标路径冲突：
- 源码与预期版本：
- 分享包隐私检查：
- Codex 登录状态：
- 网络：
- 将要修改的路径：
- 回滚方式：
- 结论：READY 或 BLOCKED

如果 READY，只问“只读核查完成，是否继续安装？”，然后停止。

收到“继续安装”后：

1. 先安装缺少的最小系统依赖：ca-certificates、curl、git、openssl、python3、
   python3-venv、unzip。不要改变 SSH、防火墙或其他服务。
2. 阅读 AGENTS.md、docs/DEPLOYMENT.md 和 deploy/install.sh。
3. 执行 `sudo bash deploy/install.sh`。不要自行改写它的 release、测试、队列
   检查、SQLite backup、原子切换或失败回滚流程。
   从开始执行安装器到最终 health 通过期间，不要从网页提交新任务。
4. 安装器失败时，先解释实际错误；禁止用 FORCE=1 绕过活动任务，禁止删除
   旧 release、数据库、上传、工作区、Token 或 .codex。
5. 安装后进入 root 环境，从 /etc/codex-deck.env 读取 CODEX_BIN 和
   CODEX_HOME，只运行该 pinned CLI 的 `login --device-auth`。让我在官方
   OpenAI 页面完成朋友自己的登录；不得显示、读取、复制或上传 auth.json。
   设备码不可用时，按 docs/DEPLOYMENT.md 使用 1455 SSH 回调隧道。
6. 执行同一 CLI 的 `login status`，不输出凭据。
7. ACCESS_MODE=ssh 时，只给出电脑端命令：
   `ssh -N -L 18788:127.0.0.1:8788 root@VPS_IP`，不改服务器网络。
8. ACCESS_MODE=tailscale 时，先确认朋友的设备和 VPS 已进入同一 Tailnet，
   用当前 `tailscale serve --help` 核实语法，再执行：
   `tailscale serve --bg http://127.0.0.1:8788`。禁止 Funnel。把 Serve 输出的
   精确 HTTPS Origin 写入 CODEX_WEB_PUBLIC_URL，并设
   CODEX_WEB_COOKIE_SECURE=1，随后重启服务。
9. 运行 deploy/diagnose.sh，并另外确认监听地址只有 127.0.0.1:8788。
10. 核对 /api/health：status=ok、version=EXPECTED_VERSION、
    runtime=app-server、runtime_details.available=true、worker_alive_count=2、
    active_jobs=queued_jobs=database_queued_jobs=0、unrestricted_write=true、
    instance_id=standalone，且 release_id 非空。
11. 不要自动运行真实模型任务。真实验收会消耗朋友额度，先问我是否允许；
    允许后只在临时目录做一个最小只读请求。

最终报告只写：版本、release_id、SDK/bundled CLI 版本、服务状态、访问地址、
数据和备份路径、登录是否成功、测试结果、更新与回滚命令、已知限制。不得包含
Owner Token、Cookie、OpenAI 凭据或 auth.json 内容。
```

## 以后升级时给 AI 的提示词

```text
升级 Codex Deck。先只读检查当前 health、版本、release_id、磁盘、源码版本和
任务队列，然后停下来等待我回复“继续升级”。三个队列计数任一不为 0 时禁止
升级。得到确认后使用新源码中的 deploy/install.sh；不要在 current 中 git
pull 或 pip install，不要恢复旧数据库，不要删除旧 release。验证失败时让
安装器自动回到旧 release。完成后报告新旧版本、SQLite 在线备份路径和完整
health，不运行真实模型任务。
```
