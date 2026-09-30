"""OneBot 12 平台适配器。

实现依据 https://12.onebot.dev/ ：

- 通信方式：支持正向 WebSocket、反向 WebSocket、HTTP Webhook、HTTP 轮询四种
  （见 ``onebot12_client.py``）；
- 数据协议：事件 / 动作请求 / 动作响应三种数据在四种通信方式上完全一致；
- 接口：消息段的收发见 ``onebot12_message.py``，事件对象见 ``onebot12_event.py``。

与内置的 ``aiocqhttp``（OneBot 11）适配器不同，OneBot 12 的媒体消息段只有一个
``file_id``，需要 ``get_file`` / ``upload_file`` 动作才能拿到或送出真正的字节，
因此这里多了一层媒体落地逻辑（见 :meth:`OneBot12Adapter._resolve_media_component`）。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import File, Image, Record, Video
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.platform.astr_message_event import MessageSession
from astrbot.core.utils.media_utils import MediaResolver

from .onebot12_client import (
    OneBot12Endpoint,
    OneBot12Transport,
    parse_endpoint,
)
from .onebot12_event import (
    CONNECTION_ID_KEY,
    UNSUPPORTED_PARAM,
    OneBot12Event,
    OneBot12Outgoing,
)
from .onebot12_message import (
    OneBot12ActionError,
    convert_incoming_message,
    dump_compact,
    parse_media_reference,
)

ADAPTER_NAME = "onebot12"
DISPLAY_NAME = "OneBot 12"
DESCRIPTION = "适用于 OneBot 12 标准的消息平台适配器，支持正向/反向 WebSocket 与 HTTP。"

ALLOWED_MODES = ("ws_reverse", "ws_forward", "http_post", "http_poll")
ALLOWED_MEDIA_MODES = ("auto", "all", "none")

NICKNAME_FETCH_TIMEOUT = 3.0
"""补全昵称的最长等待时间（秒）。

昵称只是锦上添花，而它发生在消息进管线之前：实现端一旦不响应
``get_group_member_info``，事件就会一直卡在这里。因此这里给一个远小于
``api_timeout`` 的上限，超时就放弃昵称、照常把消息放行。
"""

MEDIA_FETCH_TIMEOUT = 60.0
"""``get_file`` 三类尝试合计的等待上限（秒），避免媒体把事件卡死。"""

# WebUI「创建机器人 → 消息平台」表单用的元数据；不提供的话表单会退化成裸键值对。
CONFIG_METADATA: dict[str, dict[str, Any]] = {
    "connection_mode": {
        "description": "连接方式",
        "type": "string",
        "options": list(ALLOWED_MODES),
        "labels": [
            "反向 WebSocket（推荐）",
            "正向 WebSocket",
            "HTTP Webhook",
            "HTTP 轮询",
        ],
        "hint": "反向 WebSocket：AstrBot 监听端口，OneBot 12 实现端反向连入；正向 WebSocket：AstrBot 主动连实现端；HTTP Webhook：实现端把事件 POST 给 AstrBot；HTTP 轮询：AstrBot 调 get_latest_events 拉取事件。",
    },
    "access_token": {
        "description": "访问令牌",
        "type": "string",
        "hint": "对应实现端的 access_token。留空表示不鉴权。接收侧同时接受 Authorization: Bearer 与 access_token 查询参数。",
        "secret": True,
        "show_key": True,
    },
    "ws_host": {
        "description": "AstrBot 监听地址",
        "type": "string",
        "hint": "反向 WebSocket / HTTP Webhook 模式下 AstrBot 的监听 IP，0.0.0.0 表示监听全部网卡。",
    },
    "ws_port": {
        "description": "AstrBot 监听端口",
        "type": "int",
        "hint": "反向 WebSocket / HTTP Webhook 模式下 AstrBot 的监听端口。",
    },
    "ws_path": {
        "description": "WebSocket 路径",
        "type": "string",
        "hint": "实现端反向连接时填写的路径，例如 /onebot/v12。",
    },
    "ws_url": {
        "description": "实现端 WebSocket 地址",
        "type": "string",
        "hint": "正向 WebSocket 专用。写 127.0.0.1:6700 或完整 ws:// 地址均可。",
    },
    "http_host": {
        "description": "Webhook 监听地址",
        "type": "string",
        "hint": "HTTP Webhook 模式下 AstrBot 的监听 IP。",
    },
    "http_port": {
        "description": "Webhook 监听端口",
        "type": "int",
        "hint": "HTTP Webhook 模式下 AstrBot 的监听端口。",
    },
    "http_path": {
        "description": "Webhook 路径",
        "type": "string",
        "hint": "实现端上报事件的地址路径，例如 /onebot/v12/webhook。",
    },
    "api_base_url": {
        "description": "实现端 HTTP API 地址",
        "type": "string",
        "hint": "HTTP Webhook / HTTP 轮询专用。动作请求会 POST 到该地址，例如 http://127.0.0.1:6700/。",
    },
    "poll_interval": {
        "description": "轮询间隔（秒）",
        "type": "int",
        "slider": {"min": 1, "max": 60, "step": 1},
        "hint": "HTTP 轮询模式下调用 get_latest_events 的间隔。",
    },
    "api_timeout": {
        "description": "动作请求超时（秒）",
        "type": "int",
        "slider": {"min": 5, "max": 300, "step": 5},
        "hint": "单次动作请求的最长等待时间。",
    },
    "fetch_nickname": {
        "description": "补全发送者昵称",
        "type": "bool",
        "hint": "OneBot 12 的消息事件不带昵称，开启后会额外调用 get_group_member_info / get_user_info（结果带 10 分钟缓存）。",
    },
    "media_resolve_mode": {
        "description": "媒体落地范围",
        "type": "string",
        "options": list(ALLOWED_MEDIA_MODES),
        "labels": ["仅图片", "全部媒体", "不落地"],
        "hint": "OneBot 12 的媒体消息段只带 file_id，需要 get_file 才能取到内容。仅图片：把图片换成本地文件（多模态必需）；全部媒体：语音/视频/文件也一并落地；不落地：全部保留原样。",
    },
    "debug": {
        "description": "调试日志",
        "type": "bool",
        "hint": "打印收发的原始 JSON 与连接明细，排查协议问题时开启。",
    },
}

DEFAULT_CONFIG: dict[str, Any] = {
    "connection_mode": "ws_reverse",
    "access_token": "",
    "ws_host": "0.0.0.0",
    "ws_port": 6700,
    "ws_path": "/onebot/v12",
    "ws_url": "127.0.0.1:6700",
    "http_host": "0.0.0.0",
    "http_port": 6701,
    "http_path": "/onebot/v12/webhook",
    "api_base_url": "http://127.0.0.1:6700/",
    "poll_interval": 5,
    "api_timeout": 30,
    "fetch_nickname": True,
    "media_resolve_mode": "auto",
    "debug": False,
}

I18N_RESOURCES: dict[str, dict[str, dict[str, str]]] = {
    "zh-CN": {
        key: {
            "description": meta["description"],
            "hint": meta["hint"],
        }
        for key, meta in CONFIG_METADATA.items()
    },
    "en-US": {
        "connection_mode": {
            "description": "Connection mode",
            "hint": "ws_reverse: AstrBot listens and the OneBot 12 implementation connects in (recommended). ws_forward: AstrBot connects out. http_post: the implementation POSTs events to AstrBot. http_poll: AstrBot polls get_latest_events.",
        },
        "access_token": {
            "description": "Access token",
            "hint": "Must match the implementation's access_token. Empty means no authentication.",
        },
        "ws_host": {
            "description": "Listen address",
            "hint": "Address AstrBot binds for reverse WebSocket / HTTP Webhook. Use 0.0.0.0 to listen on all interfaces.",
        },
        "ws_port": {
            "description": "Listen port",
            "hint": "Port AstrBot binds for reverse WebSocket / HTTP Webhook.",
        },
        "ws_path": {
            "description": "WebSocket path",
            "hint": "Path the implementation should use for the reverse connection, e.g. /onebot/v12.",
        },
        "ws_url": {
            "description": "Implementation WebSocket URL",
            "hint": "Forward WebSocket only. Either host:port or a full ws:// URL.",
        },
        "http_host": {
            "description": "Webhook listen address",
            "hint": "Address AstrBot binds for the HTTP Webhook mode.",
        },
        "http_port": {
            "description": "Webhook listen port",
            "hint": "Port AstrBot binds for the HTTP Webhook mode.",
        },
        "http_path": {
            "description": "Webhook path",
            "hint": "Path the implementation posts events to, e.g. /onebot/v12/webhook.",
        },
        "api_base_url": {
            "description": "Implementation HTTP API URL",
            "hint": "HTTP Webhook / HTTP poll only. Action requests are POSTed here, e.g. http://127.0.0.1:6700/.",
        },
        "poll_interval": {
            "description": "Poll interval (seconds)",
            "hint": "Interval between get_latest_events calls in HTTP poll mode.",
        },
        "api_timeout": {
            "description": "Action timeout (seconds)",
            "hint": "Maximum wait time for a single action request.",
        },
        "fetch_nickname": {
            "description": "Fetch sender nickname",
            "hint": "OneBot 12 message events carry no nickname. Enable to call get_group_member_info / get_user_info (cached for 10 minutes).",
        },
        "media_resolve_mode": {
            "description": "Media materialization",
            "hint": "OneBot 12 media segments only carry a file_id, so get_file is required to read the content. auto: images only (needed for multimodal models); all: audio/video/file too; none: keep references untouched.",
        },
        "debug": {
            "description": "Debug logging",
            "hint": "Log raw inbound/outbound JSON to troubleshoot protocol issues.",
        },
    },
}


def _coerce_int(value: Any, default: int, minimum: int) -> int:
    """把配置项转成正整数。

    Args:
        value: 原始配置值。
        default: 转换失败时的默认值。
        minimum: 允许的最小值。

    Returns:
        不小于 ``minimum`` 的整数。
    """
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, minimum)


def _coerce_str(value: Any, default: str) -> str:
    """把配置项转成字符串。

    Args:
        value: 原始配置值。
        default: 空值时使用的默认值。

    Returns:
        字符串。
    """
    if value is None:
        return default
    text = str(value).strip()
    return text or default


@register_platform_adapter(
    ADAPTER_NAME,
    DESCRIPTION,
    default_config_tmpl=DEFAULT_CONFIG,
    adapter_display_name=DISPLAY_NAME,
    logo_path="logo.png",
    support_streaming_message=False,
    config_metadata=CONFIG_METADATA,
    i18n_resources=I18N_RESOURCES,
)
class OneBot12Adapter(Platform):
    """OneBot 12 平台适配器。

    Args:
        platform_config: 该平台实例的配置（对应 DEFAULT_CONFIG 里的字段）。
        platform_settings: AstrBot 全局平台设置。
        event_queue: AstrBot 事件队列。
    """

    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.settings = platform_settings

        self.mode = _coerce_str(
            platform_config.get("connection_mode"),
            DEFAULT_CONFIG["connection_mode"],
        )
        if self.mode not in ALLOWED_MODES:
            logger.warning(
                "[onebot12] 未知的连接方式 %r，回退为 %s",
                self.mode,
                DEFAULT_CONFIG["connection_mode"],
            )
            self.mode = DEFAULT_CONFIG["connection_mode"]

        self.access_token = _coerce_str(platform_config.get("access_token"), "")
        self.ws_host = _coerce_str(platform_config.get("ws_host"), "0.0.0.0")
        self.ws_port = _coerce_int(platform_config.get("ws_port"), 6700, 1)
        self.ws_path = _coerce_str(platform_config.get("ws_path"), "/onebot/v12")
        if not self.ws_path.startswith("/"):
            self.ws_path = "/" + self.ws_path
        self.ws_url = _coerce_str(platform_config.get("ws_url"), "127.0.0.1:6700")
        self.http_host = _coerce_str(platform_config.get("http_host"), "0.0.0.0")
        self.http_port = _coerce_int(platform_config.get("http_port"), 6701, 1)
        self.http_path = _coerce_str(
            platform_config.get("http_path"),
            "/onebot/v12/webhook",
        )
        if not self.http_path.startswith("/"):
            self.http_path = "/" + self.http_path
        self.api_base_url = _coerce_str(
            platform_config.get("api_base_url"),
            DEFAULT_CONFIG["api_base_url"],
        )
        self.poll_interval = _coerce_int(platform_config.get("poll_interval"), 5, 1)
        self.api_timeout = _coerce_int(platform_config.get("api_timeout"), 30, 1)
        self.fetch_nickname = bool(platform_config.get("fetch_nickname", True))
        self.media_resolve_mode = _coerce_str(
            platform_config.get("media_resolve_mode"),
            "auto",
        )
        if self.media_resolve_mode not in ALLOWED_MEDIA_MODES:
            self.media_resolve_mode = "auto"
        self.debug = bool(platform_config.get("debug", False))

        # 只有 HTTP 轮询把「实现端 HTTP API 地址」当必填：正向 WebSocket 用的是
        # ws_url。HTTP Webhook 少了它仍能收事件，但发不出消息，所以只告警。
        if self.mode == "http_poll":
            self._require_api_endpoint()
        elif self.mode == "http_post" and self._media_api_endpoint() is None:
            logger.error(
                "[onebot12] HTTP Webhook 模式没有可用的「实现端 HTTP API 地址」，"
                "只能接收事件、无法发送消息；请在平台配置里补上该地址",
            )

        self.transport = OneBot12Transport(
            self.mode,
            access_token=self.access_token,
            api_timeout=float(self.api_timeout),
            debug=self.debug,
        )
        self.transport.on_event(self._on_raw_event)

        self._self_platform = ""
        self._self_user_id = ""
        self.impl_version: dict[str, str] = {}
        """实现端版本信息，来自 ``meta.connect`` 事件的 ``version`` 字段。"""
        self._metadata = PlatformMetadata(
            name=ADAPTER_NAME,
            description=DESCRIPTION,
            id=str(self.config.get("id") or ADAPTER_NAME),
            adapter_display_name=DISPLAY_NAME,
            support_streaming_message=False,
        )

    # -- Platform 接口 ----------------------------------------------------

    def meta(self) -> PlatformMetadata:
        """返回平台元数据。

        Returns:
            平台元数据，``id`` 为配置里该实例的唯一 ID。
        """
        return self._metadata

    def get_client(self) -> OneBot12Transport:
        """返回通信层对象，便于插件直接调用 OneBot 12 动作。

        Returns:
            :class:`OneBot12Transport` 实例。
        """
        return self.transport

    async def run(self) -> None:
        """平台主循环：按连接方式启动通信层并常驻。"""
        logger.info(
            "[onebot12] 适配器 %s 启动，连接方式=%s",
            self.meta().id,
            self.mode,
        )
        try:
            if self.mode == "ws_reverse":
                await self.transport.run_reverse_websocket(
                    host=self.ws_host,
                    port=self.ws_port,
                    path=self.ws_path,
                )
            elif self.mode == "ws_forward":
                endpoint = parse_endpoint(
                    self.ws_url,
                    default_scheme="ws",
                    default_path="/",
                )
                await self.transport.run_forward_websocket(endpoint)
            elif self.mode == "http_post":
                await self.transport.run_http_webhook(
                    host=self.http_host,
                    port=self.http_port,
                    path=self.http_path,
                    api_endpoint=self._media_api_endpoint(),
                )
            else:
                endpoint = parse_endpoint(
                    self.api_base_url,
                    default_scheme="http",
                    default_path="/",
                )
                await self.transport.run_http_poll(
                    endpoint,
                    interval=float(self.poll_interval),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("[onebot12] 适配器运行失败: %s", exc, exc_info=True)
            self.record_error(str(exc))
            raise

    async def terminate(self) -> None:
        """停止适配器并释放所有连接。"""
        await self.transport.aclose()
        logger.info("[onebot12] 适配器 %s 已停止", self.meta().id)

    async def send_by_session(
        self,
        session: MessageSession,
        message_chain: MessageChain,
    ) -> None:
        """通过会话数据发送消息，不依赖 event 对象。

        Args:
            session: 会话对象，``session_id`` 为群号 / 用户号 / ``guild:channel``。
            message_chain: 待发送的消息链。

        Raises:
            OneBot12ActionError: 没有可用连接或实现端返回失败。
        """
        # session_id 是未校验的 dataclass 字段，别的插件可能塞进 int，这里统一成字符串
        session_id = str(session.session_id or "")
        detail_type = "private"
        if session.message_type == MessageType.GROUP_MESSAGE:
            detail_type = "channel" if ":" in session_id else "group"
        params = build_send_params(detail_type, session_id)
        sent = await OneBot12Outgoing(
            self.transport,
            self_info=self._self_info(),
        ).send(message_chain, params)
        if sent:
            await super().send_by_session(session, message_chain)

    def _self_info(self) -> dict[str, str]:
        """取本适配器当前已知的机器人自身标识。

        Returns:
            ``{"platform": ..., "user_id": ...}``；信息不全时返回空字典。
        """
        if self._self_platform and self._self_user_id:
            return {
                "platform": self._self_platform,
                "user_id": self._self_user_id,
            }
        return {}

    # -- 事件转换 ---------------------------------------------------------

    async def _on_raw_event(
        self,
        event: dict[str, Any],
        connection_id: str | None,
    ) -> None:
        """通信层的事件回调入口。

        Args:
            event: OneBot 12 事件 JSON。
            connection_id: 事件来源连接 ID。
        """
        if self.debug:
            logger.debug("[onebot12] 事件 %s", dump_compact(event))

        event_type = str(event.get("type") or "")
        if event_type == "meta":
            self._handle_meta_event(event)
            return

        if connection_id and isinstance(event.get("self"), dict):
            self._self_platform = str(
                event["self"].get("platform") or self._self_platform
            )
            self._self_user_id = str(
                event["self"].get("user_id") or self._self_user_id,
            )

        if event_type == "message":
            try:
                abm = self.convert_message(event, connection_id)
            except Exception as exc:
                logger.error("[onebot12] 转换消息事件失败: %s", exc, exc_info=True)
                return
            await self.handle_msg(abm)
        elif event_type == "notice":
            try:
                abm = self.convert_notice(event, connection_id)
            except Exception as exc:
                logger.error("[onebot12] 转换通知事件失败: %s", exc, exc_info=True)
                return
            await self.handle_msg(abm)
        elif event_type == "request":
            logger.debug("[onebot12] 暂不处理请求类事件: %s", dump_compact(event))

    def _handle_meta_event(self, event: dict[str, Any]) -> None:
        """记录元事件（连接、心跳、状态更新）。

        Args:
            event: 元事件 JSON。
        """
        detail_type = str(event.get("detail_type") or "")
        if detail_type == "connect":
            version = event.get("version") or {}
            if isinstance(version, dict):
                self.impl_version = {
                    "impl": str(version.get("impl") or ""),
                    "version": str(version.get("version") or ""),
                    "onebot_version": str(version.get("onebot_version") or ""),
                }
            logger.info(
                "[onebot12] 实现端已连接: impl=%s version=%s onebot_version=%s",
                self.impl_version.get("impl") or "unknown",
                self.impl_version.get("version") or "unknown",
                self.impl_version.get("onebot_version") or "unknown",
            )
        elif detail_type == "status_update":
            status = event.get("status") or {}
            bots = status.get("bots") or []
            for bot in bots:
                if not isinstance(bot, dict):
                    continue
                self_info = bot.get("self") or {}
                self._self_platform = str(
                    self_info.get("platform") or self._self_platform,
                )
                self._self_user_id = str(
                    self_info.get("user_id") or self._self_user_id,
                )
            if self.debug:
                logger.debug("[onebot12] 状态更新: %s", dump_compact(status))
        elif detail_type == "heartbeat" and self.debug:
            logger.debug("[onebot12] 心跳: %s", dump_compact(event))

    def convert_message(
        self,
        event: dict[str, Any],
        connection_id: str | None = None,
    ) -> AstrBotMessage:
        """把 OneBot 12 消息事件转成 AstrBotMessage。

        Args:
            event: OneBot 12 消息事件。
            connection_id: 事件来源连接 ID，用于回复时选路。

        Returns:
            转换后的 AstrBotMessage；来源是机器人自己时仍会返回，由上层的
            :meth:`handle_msg` 判断是否提交。

        Raises:
            ValueError: 事件缺少基本的消息字段时抛出。
        """
        abm, detail_type = convert_incoming_message(event)
        abm.raw_message = dict(event)
        abm.raw_message[CONNECTION_ID_KEY] = connection_id
        abm.raw_message["_detail_type"] = detail_type
        if not self._self_user_id and abm.self_id:
            self._self_user_id = abm.self_id
        return abm

    def convert_notice(
        self,
        event: dict[str, Any],
        connection_id: str | None = None,
    ) -> AstrBotMessage:
        """把 OneBot 12 通知事件转成 AstrBotMessage。

        通知事件没有消息内容，这里只带上事件类型与操作者，交给插件按
        ``raw_message`` 自行解读。

        Args:
            event: OneBot 12 通知事件。
            connection_id: 事件来源连接 ID。

        Returns:
            转换后的 AstrBotMessage。
        """
        abm = AstrBotMessage()
        self_info = event.get("self") or {}
        abm.self_id = str(self_info.get("user_id") or "")
        detail_type = str(event.get("detail_type") or "notice")
        user_id = str(event.get("user_id") or "")
        group_id = str(event.get("group_id") or "")
        operator_id = str(event.get("operator_id") or "")

        abm.message_id = str(event.get("message_id") or event.get("id") or "")
        abm.sender = MessageMember(user_id=user_id, nickname="")
        abm.message = []
        abm.message_str = ""
        timestamp = event.get("time")
        abm.timestamp = int(float(timestamp)) if timestamp else int(time.time())

        if group_id:
            abm.type = MessageType.GROUP_MESSAGE
            abm.group_id = group_id
            abm.session_id = group_id
        else:
            abm.type = MessageType.FRIEND_MESSAGE
            abm.session_id = user_id or operator_id

        abm.raw_message = dict(event)
        abm.raw_message[CONNECTION_ID_KEY] = connection_id
        abm.raw_message["_detail_type"] = detail_type
        return abm

    async def handle_msg(self, message: AstrBotMessage) -> None:
        """落地媒体、补全昵称，然后把事件交给 AstrBot。

        Args:
            message: 待处理的 AstrBotMessage。
        """
        raw = message.raw_message if isinstance(message.raw_message, dict) else {}
        detail_type = str(raw.get("_detail_type") or "")
        connection_id = raw.get(CONNECTION_ID_KEY)
        connection_id = connection_id if isinstance(connection_id, str) else None

        if (
            raw.get("type") == "message"
            and self._self_user_id
            and message.sender
            and message.sender.user_id == self._self_user_id
        ):
            # 自己发的消息（多账号 / 回声）不进入管线
            logger.debug("[onebot12] 忽略机器人自己发送的消息")
            return

        event = OneBot12Event(
            message_str=message.message_str,
            message_obj=message,
            platform_meta=self.meta(),
            session_id=message.session_id,
            transport=self.transport,
            detail_type=detail_type,
            guild_id=str(raw.get("guild_id") or ""),
        )

        if self.media_resolve_mode != "none":
            await self._resolve_media(message, event, connection_id)

        if self.fetch_nickname and message.sender and not message.sender.nickname:
            nickname = await event.fetch_nickname()
            if nickname:
                message.sender.nickname = nickname

        self.commit_event(event)

    # -- 媒体 -------------------------------------------------------------

    def _media_api_endpoint(self) -> OneBot12Endpoint | None:
        """解析实现端的 HTTP API 地址。

        Returns:
            解析结果；地址非法时返回 ``None`` 并记录告警。
        """
        try:
            return parse_endpoint(
                self.api_base_url,
                default_scheme="http",
                default_path="/",
            )
        except ValueError as exc:
            logger.warning("[onebot12] api_base_url 不可用: %s", exc)
            return None

    def _require_api_endpoint(self) -> OneBot12Endpoint:
        """解析实现端 HTTP API 地址，地址非法时直接报错。

        HTTP 轮询离开它就跑不起来（没有地址可取事件）。与其让 ``run()`` 在
        ``parse_endpoint`` 抛错后反复重启，不如在实例化阶段就失败，让日志明确指向
        要改哪个配置项。

        Returns:
            解析后的地址。

        Raises:
            ValueError: 地址为空或无法解析时抛出。
        """
        try:
            return parse_endpoint(
                self.api_base_url,
                default_scheme="http",
                default_path="/",
            )
        except ValueError as exc:
            raise ValueError(
                f"连接方式 {self.mode} 需要合法的「实现端 HTTP API 地址」"
                f"（当前值 {self.api_base_url!r}）: {exc}",
            ) from exc

    def _should_resolve(self, component: Any) -> bool:
        """判断某个媒体组件是否需要在事件入队前落地。

        Args:
            component: AstrBot 消息组件。

        Returns:
            需要落地返回 ``True``。
        """
        if isinstance(component, Image):
            return True
        # 「全部媒体」要把语音/视频/文件一并落地：File 不是 Image 的子类，
        # 漏掉它会让 file 消息段永远停在 onebot12:// 引用上（核心不认这个引用）。
        if self.media_resolve_mode == "all":
            return isinstance(component, (Record, Video, File))
        return False

    async def _fetch_file_id(
        self,
        file_id: str,
        connection_id: str | None,
    ) -> tuple[str, str]:
        """用 ``get_file`` 把 ``file_id`` 换成可访问的引用。

        依次尝试 ``url`` / ``path`` / ``data`` 三种方式。只有实现端明确回
        ``10004 Unsupported Param`` 时才换下一种——那才是「这个方式我不支持」的
        信号；其它错误（网络、文件系统）换个方式重发只会把同样的内容再传一遍。
        超时同样不视为「不支持」，继续试下一种。

        Args:
            file_id: OneBot 12 的文件 ID。
            connection_id: 事件来源连接 ID。

        Returns:
            ``(kind, value)``，``kind`` 为 ``url`` / ``path`` / ``data``；
            全部失败时返回 ``("", "")``。
        """
        for kind in ("url", "path", "data"):
            try:
                data = await asyncio.wait_for(
                    self.transport.request(
                        "get_file",
                        {"file_id": file_id, "type": kind},
                        prefer=connection_id,
                        self_info=self._self_info(),
                    ),
                    timeout=MEDIA_FETCH_TIMEOUT,
                )
            except TimeoutError:
                logger.warning(
                    "[onebot12] get_file(type=%s) 超过 %.0fs 未响应，改试下一种方式: %s",
                    kind,
                    MEDIA_FETCH_TIMEOUT,
                    file_id,
                )
                continue
            except OneBot12ActionError as exc:
                logger.debug("[onebot12] get_file(type=%s) 失败: %s", kind, exc)
                if exc.retcode != UNSUPPORTED_PARAM:
                    # 不是「不支持这个方式」，再换方式没有意义
                    logger.warning(
                        "[onebot12] get_file(type=%s) 失败(retcode=%s)，放弃获取媒体 %s: %s",
                        kind,
                        exc.retcode,
                        file_id,
                        exc.message,
                    )
                    return "", ""
                continue
            if isinstance(data, dict):
                value = data.get(kind)
                if isinstance(value, str) and value:
                    return kind, value
        return "", ""

    async def _resolve_media(
        self,
        message: AstrBotMessage,
        event: OneBot12Event,
        connection_id: str | None,
    ) -> None:
        """把消息链里带 ``onebot12://`` 引用的媒体换成本地文件或 URL。

        图片（以及 ``media_resolve_mode=all`` 时的其它媒体）会被 :class:`MediaResolver`
        落地成本地文件，并把路径登记到事件上，事件结束后由 AstrBot 统一清理。必须落地
        的原因有两个：一是 ``onebot12://`` 只是本适配器内部的引用，核心的媒体预处理
        不认；二是接到 ``file_id`` 后并不存在可直接下载的 URL。

        Args:
            message: AstrBotMessage，其 ``message`` 会被原地修改。
            event: 对应的事件对象，用于登记临时文件。
            connection_id: 事件来源连接 ID。
        """
        for component in message.message:
            if not isinstance(component, (Image, Record, Video, File)):
                continue
            if isinstance(component, File):
                # File 组件把本地路径放在 file_（file 是被 property 包过的读取入口）
                reference = component.file_ or component.url
            else:
                reference = component.file or getattr(component, "url", "")
            file_id = parse_media_reference(
                reference if isinstance(reference, str) else None,
            )
            if not file_id or not self._should_resolve(component):
                continue

            kind, value = await self._fetch_file_id(file_id, connection_id)
            if not kind:
                logger.warning(
                    "[onebot12] 无法获取媒体内容，保留 file_id 引用: %s",
                    file_id,
                )
                continue

            if kind == "data":
                source = f"base64://{value}"
            elif kind == "path":
                # 用 Path.as_uri() 而不是拼字符串：后者遇到 Windows 盘符、# 或
                # 非 ASCII 字符会拼出不合法的 URI。
                try:
                    source = Path(value).as_uri()
                except ValueError:
                    source = value if value.startswith("file:") else f"file://{value}"
            else:
                source = value

            if isinstance(component, File):
                media_type, suffix = "file", None
            elif isinstance(component, Image):
                media_type, suffix = "image", None
            elif isinstance(component, Record):
                media_type, suffix = "audio", ".wav"
            else:
                media_type, suffix = "video", ".mp4"

            try:
                local_path = await MediaResolver(
                    source,
                    media_type=media_type,
                    default_suffix=suffix,
                ).to_path()
            except Exception as exc:
                logger.warning(
                    "[onebot12] 媒体落地失败，改用原始引用 %s: %s",
                    source,
                    exc,
                )
                if isinstance(component, File):
                    component.file_ = ""
                    component.url = source
                else:
                    component.url = source
                continue

            if isinstance(component, File):
                component.file_ = local_path
                component.url = ""
            else:
                component.file = local_path
                component.path = local_path
                component.url = ""
            event.track_temporary_local_file(local_path)

    def create_event(self, message: AstrBotMessage) -> OneBot12Event:
        """构造 OneBot 12 事件对象。

        Args:
            message: AstrBotMessage。

        Returns:
            :class:`OneBot12Event` 实例。
        """
        raw = message.raw_message if isinstance(message.raw_message, dict) else {}
        return OneBot12Event(
            message_str=message.message_str,
            message_obj=message,
            platform_meta=self.meta(),
            session_id=message.session_id,
            transport=self.transport,
            detail_type=str(raw.get("_detail_type") or ""),
            guild_id=str(raw.get("guild_id") or ""),
        )


def build_send_params(
    detail_type: str,
    session_id: str,
) -> dict[str, Any]:
    """按 ``detail_type`` 与会话 ID 拼出 ``send_message`` 的目标参数。

    规范里这些 ID 都是 ``string``，而 ``MessageSession.session_id`` 是未校验的
    dataclass 字段（别的插件可能塞 int），所以这里统一转成字符串。

    Args:
        detail_type: ``private`` / ``group`` / ``channel``。
        session_id: 会话 ID，频道会话形如 ``guild_id:channel_id``。

    Returns:
        ``send_message`` 的参数（不含 ``message``）。
    """
    resolved = str(session_id or "")
    params: dict[str, Any] = {"detail_type": detail_type}
    if detail_type == "group":
        params["group_id"] = resolved
    elif detail_type == "channel":
        guild_id, _, channel_id = resolved.partition(":")
        params["guild_id"] = guild_id
        params["channel_id"] = channel_id
    elif detail_type == "private":
        params["user_id"] = resolved
    elif "." in detail_type:
        # 实现端扩展的类型（如 qq.discuss）按规范用 <detail_type>_id
        params[f"{detail_type}_id"] = resolved
    else:
        params["user_id"] = resolved
    return params


def is_registered() -> bool:
    """适配器是否已注册到 ``platform_registry``（插件加载后应为 True）。

    Returns:
        已注册返回 ``True``。
    """
    from astrbot.core.platform.register import platform_registry

    return any(meta.name == ADAPTER_NAME for meta in platform_registry)
