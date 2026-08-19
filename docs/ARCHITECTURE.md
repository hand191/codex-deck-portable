# 架构速览

```text
浏览器 / PWA
    │  HTTP + SSE（默认经 SSH 隧道或 Tailscale Serve）
    ▼
codex_web.py
    ├─ SQLite：对话、消息、任务、设备会话、反馈
    ├─ uploads：附件原文件
    ├─ JobScheduler：默认两个并行任务、工作区冲突锁
    └─ codex_runtime.py
          └─ pinned openai-codex SDK + bundled Codex app-server CLI
                 └─ 朋友自己的 CODEX_HOME 登录和线程
```

`codex_web.py` 负责 HTTP、认证、持久化、任务调度和内嵌前端；
`codex_runtime.py` 把官方 app-server 的增量事件解耦成稳定文本流；
`job_stream.py` 管理同源 SSE 快照、重连与终态。

默认单机版：

- `CODEX_WEB_INSTANCE_ID=standalone`；
- 未配置 `CODEX_WEB_PORTAL_URL` 时不显示门户按钮；
- 未配置 peer URL 时不进行双实例版本比较；
- LifeOS、trusted SSO 和 tailnet-owner 均为可选扩展，不是部署前提。

运行数据与源码 release 分离。切换 `/opt/codex-deck/current` 不会覆盖数据库、
附件、工作区或 Codex 登录态。
