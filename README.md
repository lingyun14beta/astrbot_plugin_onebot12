# astrbot_plugin_onebot12

为 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 接入 [OneBot 12 标准](https://12.onebot.dev/)的**平台适配器插件**。

装上之后，WebUI「配置 → 消息平台 → 新增」的下拉框里会多出一项 **OneBot 12**。
任何符合 OneBot 12 标准的实现端（QQ、Telegram、Discord……）都能接进来收发消息。

> 与内置的 `aiocqhttp` 适配器的区别：`aiocqhttp` 走的是 **OneBot 11**（CQ 码那一套），
> 本插件走的是 **OneBot 12**（消息段数组 + `file_id` 媒体）。两者互不冲突，可以同时启用。

## 特性

| 能力 | 说明 |
| --- | --- |
| 四种通信方式 | 反向 WebSocket（推荐）、正向 WebSocket、HTTP Webhook、HTTP 轮询 |
| 数据协议 | 事件 / 动作请求 / 动作响应三种数据按规范实现，动作请求带唯一 `echo` |
| 鉴权 | `Authorization: Bearer <access_token>`；接收侧同时接受 `access_token` 查询参数 |
| 消息段 | 收到：`text`/`mention`/`mention_all`/`image`/`voice`/`audio`/`video`/`file`/`location`/`reply`；发出：`text`/`mention`/`mention_all`/`reply`/`location`/`image`/`voice`/`video`/`file` |
| 媒体 | 收到时 `get_file` 落地（`url` → `path` → `data` 逐级降级），发出时 `upload_file`（`url`/`path`/`data`，同样降级） |
| 事件 | 消息事件（`private`/`group`/`channel`）与通知事件（`notice.*`）都会转成 AstrBot 事件 |
| 元事件 | `meta.connect` 记录实现端版本，`meta.status_update` 记录机器人账号 |
| 多账号 | 动作请求优先回到产生该事件的那条连接，并带上 `self` 字段 |
| 主动推送 | 实现了 `send_by_session`，插件可以按会话直接发消息 |

单测 + 集成测试共 119 项，覆盖消息段双向转换、四种通信方式的连接与鉴权、
动作请求/响应配对、媒体落地、发送选路与昵称补全。

## 安装

把整个 `astrbot_plugin_onebot12` 目录放进 AstrBot 的 `data/plugins/` 下，重启 AstrBot 即可。

```
AstrBot/
└── data/
    └── plugins/
        └── astrbot_plugin_onebot12/
            ├── main.py
            ├── onebot12_adapter.py
            ├── onebot12_client.py
            ├── onebot12_event.py
            ├── onebot12_message.py
            ├── metadata.yaml
            ├── _conf_schema.json
            └── logo.png
```

依赖：`aiohttp`（AstrBot 自带），无需额外安装。

## 快速开始：反向 WebSocket

这是最省事的方式 —— AstrBot 起一个 WebSocket 服务端，实现端连过来。

1. WebUI →「配置」→「消息平台」→「新增」→ 消息平台类别选 **OneBot 12**；
2. 填两个字段：
   - **连接方式** = `反向 WebSocket`
   - **访问令牌** = 自己定一个字符串（比如 `my-secret-token`），留空则不鉴权
3. 保存并**启用**这条平台配置（新增时 `enable` 默认是关的，要手动打开）；
4. 去实现端那边，把**反向 WebSocket 地址**填成：

   ```
   ws://<AstrBot 的 IP>:6700/onebot/v12
   ```

   访问令牌填成同一个字符串。若实现端不方便设置请求头，也可以把令牌放进 URL：

   ```
   ws://<AstrBot 的 IP>:6700/onebot/v12?access_token=my-secret-token
   ```

5. AstrBot 日志里看到 `[onebot12] 反向 WebSocket 已连接` 就成功了。给机器人发条消息试试。

端口和路径都可以在平台配置里改（`AstrBot 监听端口` / `WebSocket 路径`）。

## 四种连接方式怎么选

| 连接方式 | 谁连谁 | 适用场景 |
| --- | --- | --- |
| `反向 WebSocket`（默认） | 实现端 → AstrBot | **推荐**。一条连接同时跑事件与动作，AstrBot 需要有可被访问的地址 |
| `正向 WebSocket` | AstrBot → 实现端 | 实现端有固定地址、AstrBot 在内网时用；填「实现端 WebSocket 地址」，如 `127.0.0.1:6700` |
| `HTTP Webhook` | 实现端 → AstrBot | 只推事件；动作请求走「实现端 HTTP API 地址」。实现端需要支持 Webhook 上报 |
| `HTTP 轮询` | AstrBot → 实现端 | AstrBot 定时调 `get_latest_events` 拉事件；实现端需要开启 `event_enabled` |

> HTTP 两种方式都要额外填「实现端 HTTP API 地址」（如 `http://127.0.0.1:6700/`），
> 否则只能收事件、发不出消息。

## 配置项

平台配置页（每个 OneBot 12 机器人一份）：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| 连接方式 | `反向 WebSocket` | 见上表 |
| 访问令牌 | 空 | 对应实现端的 `access_token`，留空表示不鉴权 |
| AstrBot 监听地址 / 端口 | `0.0.0.0` / `6700` | 反向 WebSocket 与 HTTP Webhook 用 |
| WebSocket 路径 | `/onebot/v12` | 实现端反向连接时填的路径 |
| 实现端 WebSocket 地址 | `127.0.0.1:6700` | 正向 WebSocket 用，写 `host:port` 或完整 `ws://` 都行 |
| Webhook 监听地址 / 端口 / 路径 | `0.0.0.0` / `6701` / `/onebot/v12/webhook` | HTTP Webhook 用 |
| 实现端 HTTP API 地址 | `http://127.0.0.1:6700/` | HTTP 两种方式发动作请求用 |
| 轮询间隔（秒） | `5` | HTTP 轮询用 |
| 动作请求超时（秒） | `30` | 单次动作请求最长等待 |
| 补全发送者昵称 | 开 | OneBot 12 的消息事件**不带昵称**，开启后额外调一次 `get_group_member_info` / `get_user_info`（带 10 分钟缓存，最多等 3 秒） |
| 媒体落地范围 | `仅图片` | `仅图片`：图片下载成本地文件（多模态必需）；`全部媒体`：语音/视频/文件也落地；`不落地`：全部保留引用 |
| 调试日志 | 关 | 打印收发的原始 JSON，排查协议问题时打开 |

插件配置页只有一个「启用插件」开关；连接参数都在平台配置里。

## 媒体是怎么处理的

OneBot 12 的 `image` / `voice` / `audio` / `video` / `file` 消息段**只有一个 `file_id`**，
没有 URL 也没有路径。本插件的处理是：

- **收到时**：先把 `file_id` 记成内部引用 `onebot12://<file_id>`，再按「媒体落地范围」
  决定是否调 `get_file` 换回来。`get_file` 依次尝试 `url`、`path`、`data` 三种方式
  （实现端对不支持的方式会返回 `10004 Unsupported Param`，据此降级）。
  图片会下载成本地文件并登记到事件上，事件结束后由 AstrBot 统一清理。
  取不到内容时保留 `onebot12://` 引用并打一条 warning，消息本身不会丢。
- **发出时**：远端 http(s) 链接用 `upload_file(type=url)`，本地文件用 `type=path`；
  实现端不支持时自动降级为 `type=data`（按字节 base64 上传），拿到 `file_id` 再发
  `image` / `voice` / `video` / `file` 消息段。

## 能力边界

这些不是 bug，是「本来就没做」，列出来省得你踩坑：

- **流式消息不支持**。OneBot 12 没有流式语义，`send_streaming` 会合并成一条发出，
  平台元数据里也标了 `support_streaming_message=False`。
- **实现端必须回显 `echo`**。规范（`connect/data-protocol/action-response.md`）要求
  请求带了非空 `echo` 时响应原样回显。适配器**不按到达顺序猜**响应属于谁——猜错会把
  A 的数据交给 B，而且此后每一个响应都跟着错位。所以实现端漏回 `echo` 时动作请求会
  明确失败并提示原因。单条请求在飞时通常不受影响，HTTP 并发场景下会直接报错。
- **请求类事件（`request.*`）不处理**，只记一条 debug 日志。通知事件（`notice.*`）会
  转成 AstrBot 事件、`raw_message` 里带完整原始 JSON，交给插件自行解读。
- **`platform_adapter_type` 过滤器认不出 `onebot12`**。AstrBot 的
  `ADAPTER_NAME_2_TYPE` 是内置适配器的白名单（`astrbot/core/star/filter/platform_adapter_type.py`），
  插件注册的适配器不在里面，因此 `@filter.platform_adapter_type(...)` 对本适配器的事件
  一律返回 False。需要按平台过滤时请用 `event.get_platform_name() == "onebot12"`。
- **群主 / 群管理员拿不到**。OneBot 12 的 `get_group_member_info` 没有角色字段，
  `get_group()` 返回的 `group_owner` / `group_admins` 保持为空。
- **MessagePack 不支持**。规范里 MessagePack 是可选实现；收到二进制帧会打一条
  warning 并忽略，不会静默丢消息。
- **表情回应（`react`）退化成发一条消息**。OneBot 12 标准没有表情回应动作。
- 实现端**多次重复推送同一事件**时不会去重（规范里的 `id` 只用于日志排查）。

## 排障

先在平台配置里打开「调试日志」，日志里会出现 `[onebot12] -> ws-reverse#1 {...}`（发出）
和 `[onebot12] raw <- {...}`（收到）两种行。

| 现象 | 排查方向 |
| --- | --- |
| 日志里没有「反向 WebSocket 服务已启动」 | 平台配置没**启用**，或端口被占用 |
| 实现端连不上，AstrBot 日志有「鉴权失败」 | 两边 `access_token` 不一致；实现端可能没发 `Authorization` 头，改用 `?access_token=` |
| 能连上但收不到消息 | 实现端是否真的在推事件；`meta.connect` 事件有没有到（到了会打「实现端已连接」） |
| 能收消息但回复发不出去 | 日志里看有没有 `send_message` 动作请求；有请求但超时说明实现端没回响应 |
| 图片显示不出来 | 打开「全部媒体」或「仅图片」，并在日志里看 `get_file` 是否成功；失败会打「无法获取媒体内容」 |
| HTTP 模式只能收不能发 | 「实现端 HTTP API 地址」没填或填错 |

## 开发与测试

```bash
# 在 AstrBot 仓库同级目录下
AstrBot/.venv/Scripts/python.exe -m pytest astrbot_plugin_onebot12/tests -q
AstrBot/.venv/Scripts/python.exe -m ruff check astrbot_plugin_onebot12
AstrBot/.venv/Scripts/python.exe -m ruff format astrbot_plugin_onebot12
```

测试不依赖网络：`tests/conftest.py` 会把插件目录与 AstrBot 源码目录加进 `sys.path`，
`tests/test_client.py` 里的端到端用例起的是本机 aiohttp 服务，
`tests/test_media_integration.py` 用的是真实 transport + 脚本化连接。

还有一个端到端冒烟自检，会真的把插件按 AstrBot 的方式加载起来：

```bash
cd AstrBot && .venv/Scripts/python.exe selftest_onebot12.py
```

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `main.py` | 插件入口：导入适配器完成注册，提供 `/onebot12 status` 指令 |
| `onebot12_adapter.py` | 平台适配器：配置解析、事件转换、媒体落地、发送选路 |
| `onebot12_client.py` | 通信层：四种连接方式、鉴权、动作请求/响应配对 |
| `onebot12_event.py` | 事件对象：把 AstrBot 的发送/查询翻译成 OneBot 12 动作 |
| `onebot12_message.py` | 消息段 ↔ AstrBot 消息组件的双向转换 |

## 许可

AGPL-3.0，与 AstrBot 保持一致。
