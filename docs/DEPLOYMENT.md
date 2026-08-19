# 在一台 Ubuntu VPS 上部署 Codex Deck

这条路径面向个人探索 VPS：root 运行、整机工作区、完全权限、两个并行任务，
但 Web 服务始终只监听回环地址。预计首次部署需要 10–20 分钟，主要等待依赖
下载和朋友自己的 Codex 登录。

如果想让 AI 完成这些步骤，直接改用 [AI_DEPLOYMENT.md](AI_DEPLOYMENT.md)。

## 1. 前提

- Ubuntu 24.04；Debian 12 通常也能运行，但自动教程以 Ubuntu 为验收基准。
- root 或免密 sudo。
- 能访问 PyPI、OpenAI 登录页和 Codex 服务。
- 至少 2 GB 内存和 5 GB 可用磁盘；每个 release 有独立 Python venv。
- 源码压缩包或私有 Git 仓库访问权限。

不要从原作者服务器复制 `.codex/auth.json`、SQLite、Token、Cookie、上传目录
或工作区。朋友要在自己的 VPS 上用自己的账户重新登录。

## 2. 准备系统

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git openssl python3 python3-venv unzip
```

把源码 zip 和 `.sha256` 文件上传到同一个目录，先验证：

```bash
sha256sum -c Codex-Deck-v2.21.2-source.zip.sha256
```

然后解压并进入目录：

```bash
unzip Codex-Deck-v2.21.2-source.zip
cd codex-deck-v2.21.2
```

## 3. 安装服务

```bash
sudo bash deploy/install.sh
```

安装器会：

1. 构建前和切换前检查任务队列；短暂停服后再查一次 SQLite，避免检查期间
   刚好进入的新任务被带进版本切换；
2. 把源码安装到 `/opt/codex-deck/releases/<版本>-<时间>`；
3. 为该 release 创建独立 venv，并严格安装 `requirements.txt`；
4. 使用 SDK 自带的同版本 Codex CLI；
5. 运行 Python 编译检查和单元测试；
6. 首次生成一个不打印到终端的 Owner Token；
7. 升级前使用 SQLite backup API 创建一致性备份；
8. 原子切换 `current`，启动 systemd 并验证 `/api/health`；
9. 新版本健康检查失败时自动切回上一份源码、venv、CLI 和 systemd unit。

默认位置：

| 内容 | 路径 |
|---|---|
| 当前 release | `/opt/codex-deck/current` |
| 历史 release | `/opt/codex-deck/releases/` |
| 环境配置 | `/etc/codex-deck.env` |
| SQLite / 上传 / Token / 备份 | `/var/lib/codex-deck/` |
| Codex 登录和会话 | `/root/.codex/` |
| systemd | `codex-deck.service` |

## 4. 用朋友自己的 Codex 账户登录

安装器固定使用当前 release 随附的官方 CLI。先进入 root shell，再加载同一套
运行配置：

```bash
sudo -i
set -a
source /etc/codex-deck.env
set +a
"$CODEX_BIN" login --device-auth
"$CODEX_BIN" login status
exit
```

在手机或电脑上打开命令给出的官方 OpenAI 地址并完成登录。设备码方式不可用
时，可在自己的电脑建立回调隧道：

```bash
ssh -N -L 1455:127.0.0.1:1455 root@你的_VPS_IP
```

然后在 VPS 执行 `"$CODEX_BIN" login`，并只在官方 OpenAI 页面完成授权。
不要打开、打印或传输 `/root/.codex/auth.json`。

## 5. 最短访问方式：SSH 隧道

在自己的电脑运行：

```bash
ssh -N -L 18788:127.0.0.1:8788 root@你的_VPS_IP
```

浏览器打开：

```text
http://127.0.0.1:18788
```

首次进入时，在 VPS 本人终端读取 Owner Token：

```bash
sudo cat /var/lib/codex-deck/api-token
```

只粘贴到自己的 Deck 登录框。浏览器随后使用独立设备会话，不会长期保存原始
Owner Token。

## 6. 手机和 iPad：可选 Tailscale Serve

先按 Tailscale 官方方法把 VPS、手机和 iPad 加入同一 Tailnet。确认
`tailscale status` 正常后，在 VPS 执行：

```bash
sudo tailscale serve --bg http://127.0.0.1:8788
sudo tailscale serve status
```

记录输出的 `https://<设备名>.<tailnet>.ts.net`，然后编辑：

```bash
sudoedit /etc/codex-deck.env
```

设置：

```ini
CODEX_WEB_PUBLIC_URL=https://<设备名>.<tailnet>.ts.net
CODEX_WEB_COOKIE_SECURE=1
```

重启并检查：

```bash
sudo systemctl restart codex-deck.service
curl -fsS http://127.0.0.1:8788/api/health
```

现在可从同一 Tailnet 的手机/iPad 打开 HTTPS 地址，仍使用朋友自己的 Deck
Owner Token 登录。使用 Serve，不要使用会公开到互联网的 Funnel。

## 7. 验收

```bash
sudo bash deploy/diagnose.sh
```

至少确认：

- `codex-deck.service` 为 active；
- `/api/health` 的 `status` 为 `ok`、版本为 `2.21.2`；
- `runtime_details.available` 为 `true`；
- SDK 与 bundled CLI 版本来自同一份 pinned release；
- worker 数为 2，三个任务/队列计数均为 0；
- `codex login status` 显示朋友自己的账户已登录。

最后在临时目录新建一个对话，先做只读小任务。真实请求会使用朋友自己的
Codex 额度。

## 8. 模型不可用时

默认模型列表来自当前源码版本。如果朋友的账户没有其中某个模型，编辑
`/etc/codex-deck.env` 中的：

```ini
CODEX_WEB_ALLOWED_MODELS=模型A,模型B
CODEX_WEB_DEFAULT_MODEL=模型A
```

默认模型必须同时出现在 allowlist 中。保存后重启服务。

## 官方参考

- [OpenAI Codex CLI](https://developers.openai.com/codex/cli)
- [OpenAI Codex authentication](https://developers.openai.com/codex/auth)
- [Tailscale Serve CLI](https://tailscale.com/docs/reference/tailscale-cli/serve)
