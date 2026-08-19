# 分享源码给朋友

## 推荐方式

最方便持续更新的是公开的 portable Git 仓库；只想控制访问时也可以保持私有
并邀请 collaborator。无论选择哪种可见性，都应只提交当前清理后的源码树，
不带原私人仓库的历史。一次性分享则发送源码 zip 和它的 SHA-256 文件。

在干净、已提交的源码仓库运行：

```bash
bash scripts/package-share.sh /你要保存分享包的目录
```

脚本使用 `git archive`，因此不会包含 `.git` 历史、未跟踪文件或运行数据；
它还会拒绝常见的个人绝对路径和私钥/访问令牌标记。发送前仍应按本页清单
人工确认一次。把生成的两个文件一起
发送给朋友：

```text
Codex-Deck-v2.21.2-source.zip
Codex-Deck-v2.21.2-source.zip.sha256
```

朋友应先执行：

```bash
sha256sum -c Codex-Deck-v2.21.2-source.zip.sha256
```

## 绝对不要分享

- `.codex/` 或任何 `auth.json`；
- `/etc/codex-deck.env`；
- Owner Token、API Key、Cookie、SSH 私钥；
- SQLite、WAL/SHM、上传、工作区、日志和备份；
- 现有服务器的域名、IP、反向代理、SSO 或 Tailscale 私人配置。

朋友的 OpenAI/Codex 登录、额度、会话、Deck Token 和数据都应在他自己的 VPS
重新生成。

## GitHub

不要直接把带私人部署历史的旧仓库改成 public。推荐做法：

1. 新建空的 portable 仓库，根据分享范围选择 public 或 private；
2. 解压分享包；
3. `git init` 后创建一次全新的初始提交；
4. 以后以不可变 tag 分享升级。

## 许可状态

当前源码以 MIT License 开源，允许使用、修改和再分发，但必须保留版权与
许可声明。项目是非官方社区封装，与 OpenAI 无隶属、维护或背书关系。
