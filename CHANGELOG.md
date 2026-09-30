# 更新日志

> AstrBot 会在插件详情页读取插件目录下的 `CHANGELOG.md`（依次尝试
> `CHANGELOG.md` → `changelog.md` → `CHANGELOG` → `changelog`），按 Markdown 渲染。
> 约定：发版时让 `metadata.yaml` 的 `version` 与本文件最新一节的标题一致。

## v1.0.0

首个版本：按 [OneBot 12 标准](https://12.onebot.dev/) 实现 AstrBot 平台适配器。

### 新增

- **四种通信方式**（`onebot12_client.py`）：反向 WebSocket、正向 WebSocket、
  HTTP Webhook、HTTP 轮询。三者共用同一套「动作请求 / 动作响应 / 事件」处理，
  只把连接方式抽象成 `OneBot12Connection`。
- **鉴权**：`Authorization: Bearer <access_token>`，接收侧额外接受 `access_token`
  查询参数（规范允许实现端在无法设置请求头时这样做）；令牌两侧空白不裁剪。
- **消息段双向转换**（`onebot12_message.py`）：收到支持 `text` / `mention` /
  `mention_all` / `image` / `voice` / `audio` / `video` / `file` / `location` / `reply`；
  发出支持 `text` / `mention` / `mention_all` / `reply` / `location` /
  `image` / `voice` / `video` / `file`。
- **媒体处理**：收到的 `file_id` 先记成 `onebot12://` 惰性引用，再按配置调
  `get_file`（`url` → `path` → `data` 逐级降级）落地成本地文件并登记为事件临时文件；
  发出时调 `upload_file`（`url` / `path`，不支持则降级为按字节 `data`）换回 `file_id`。
- **事件**：消息事件（`private` / `group` / `channel`）与通知事件（`notice.*`）转成
  AstrBot 事件；元事件 `meta.connect` 记录实现端版本、`meta.status_update` 记录机器人账号。
- **多账号路由**：动作请求优先回到产生该事件的那条连接，并带上规范的 `self` 字段。
- **发送能力**：`AstrMessageEvent.send` / `send_streaming`（合并后一次性发出）/
  `get_group` / `react`，以及不依赖 event 的 `Platform.send_by_session`。
- **插件侧**：`/onebot12 status` 指令查看实例状态、连接方式与实现端版本；
  WebUI 平台表单的 `config_metadata` 与中英文案 `i18n_resources`；适配器 logo。

### 说明

- 只要插件被加载，WebUI「新增机器人 → 消息平台类别」里就会出现 **OneBot 12**；
  注册发生在 `import` 期，没有配置开关能关掉它，插件重载/卸载时随模块一并注销。
- 新建的平台配置默认 `enable: False`，在 WebUI 里还要手动打开「启用」才会 `run()`。
- `astrbot_version` 下限取 `>=4.16.0`（平台表单的 `config_metadata` /
  `i18n_resources` 需要该版本；`register_platform_adapter` 自 3.4.0 起就有）。

### 审查发现并修复的缺陷

首版完成后做过一轮对抗式代码审查（含单测/集成测试与真实实现端模拟），以下是查出来
并已修复的问题，连同对应的回归测试一起记录，避免以后改回去：

- **收到图片永远超时**（`onebot12_client.py`）：事件处理原来直接 `await` 在读循环里，
  而落地上游事件会发起 `get_file`——它的响应同样要走读循环，于是读循环被自己占住。
  实测实现端秒回，图片依然每次 3 秒超时后被丢弃。改为「每条连接一个事件队列 + worker
  协程」，读循环只负责读与派发，事件仍按到达顺序串行处理。
- **`meta.status_update` 被丢弃**（`onebot12_client.py`）：分类逻辑「有 `status` 就当
  动作响应」把这条标准元事件吃掉了，适配器里的处理分支成了死代码。改为按
  `action` / `status+retcode` / `type` 三者组合判别。
- **`self` 字段位置错误**（`onebot12_event.py`）：原来放在 `params` 里，规范
  （`connect/data-protocol/action-request.md`）要求它是 `action`/`params`/`echo` 的
  同级顶层字段。位置错了多账号会拿到 `10101 Who Am I`。同时从「只加在
  `send_message` 上」改为所有非元动作都带。
- **`aclose()` 在队列有积压时抛 `ValueError`**（`onebot12_client.py`）：
  `while not queue.empty(): queue.task_done()` 里 `task_done()` 并不出队，循环减到 0
  后抛错，把 `runner.cleanup()` / `session.close()` 一起带没，监听端口泄漏。
- **`File` 组件发不出去**（`onebot12_message.py`）：读的是 `File.file`，而它是 property
  ——路径不在磁盘上时返回空串（真身在 `file_`），于是文件被静默跳过。
- **动作响应按到达顺序错位认领**（`onebot12_client.py`）：实现端漏回 `echo` 时原来会
  猜一个等待者，两个请求同时在飞就会互换数据。改为明确失败并提示实现端未守规范。
- **每条连接的 worker/队列不回收**（`onebot12_client.py`）：连接 ID 每次都新，旧 worker
  永远停在 `await queue.get()` 上，反复重连会稳定泄漏任务与队列。改为注销时投退休
  哨兵，worker 处理完存量事件后自行清理。
- **正向 WebSocket 退避不增长**（`onebot12_client.py`）：连上就把退避重置，遇到「接受
  握手后立刻断开」的实现端会变成每秒一次的连接风暴。改为连接存活够久才重置。
- **HTTP Webhook 事件的来源连接 ID 是假的**（`onebot12_client.py`）：回复时按这个假
  ID 找连接必然失败。事件本来就没有「连接」概念，改为传 `None` 走 HTTP 动作通道。
- **`media_resolve_mode=all` 漏掉 `File`**、**`session_id` 非字符串时崩**、
  **扩展 `detail_type` 用错参数名**、**反向 WebSocket 没有心跳**、
  **`get_file`/`upload_file` 不分错误类型地重试或过早放弃**、
  **`path` 引用拼 URI 不合法** 等问题一并修复。
