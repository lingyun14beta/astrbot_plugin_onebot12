"""OneBot 12 事件对象：把 AstrBot 的发送/查询动作翻译成 OneBot 12 动作请求。

一个容易踩的坑：OneBot 12 里同一个实现端可以同时挂多条连接，动作请求应当尽量
回到「产生这条事件的那条连接」上，否则多账号场景会串号。事件来源连接 ID 由适配器
写在 :attr:`AstrBotMessage.raw_message` 的同级字典里（键 ``_connection_id``），
这里读出来作为 ``prefer`` 参数传给通信层。
"""

from __future__ import annotations

import asyncio
import base64
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.platform import AstrBotMessage, Group, MessageMember, PlatformMetadata

from .onebot12_client import OneBot12Transport
from .onebot12_message import (
    OneBot12ActionError,
    convert_outgoing_message,
)

CONNECTION_ID_KEY = "_connection_id"
"""存放在 ``AstrBotMessage`` 上的事件来源连接 ID 字段名。"""

NICKNAME_CACHE_SIZE = 2048
NICKNAME_CACHE_TTL = 600.0
NICKNAME_FETCH_TIMEOUT = 3.0
"""补全昵称的最长等待时间（秒），避免实现端不响应时卡住消息管线。"""

UNSUPPORTED_PARAM = 10004
"""OneBot 12 返回码：不支持的动作参数（``10004 Unsupported Param``）。

规范（``interface/index.md``）规定：实现端对确实做不到的参数应返回它，因此这是
「换个方式重试」的最可靠信号。
"""

DOWNGRADEABLE_RETCODES = frozenset({10004, 10003, 20002})
"""收到这些返回码时才值得把 ``upload_file`` 降级成按字节上传。

- ``10004``：规范建议的「不支持该参数」（``interface/index.md``）；
- ``10003 Bad Param``：实现端把「不支持的 type」也归到这里；
- ``20002``：动作处理器内部异常，部分实现端用它表达同样的意思。
"""


def _basename(reference: str) -> str:
    """从媒体引用里取一个文件名。

    Args:
        reference: 本地路径、``file://`` URI 或 http(s) URL。

    Returns:
        文件名；取不到时返回空字符串。
    """
    tail = reference.split("?", 1)[0].replace("\\", "/").rsplit("/", 1)[-1]
    return tail or ""


class OneBot12Outgoing:
    """把 AstrBot 消息链发出去的工具（事件回复与 ``send_by_session`` 共用）。

    Args:
        transport: OneBot 12 通信层。
        connection_id: 优先使用的连接 ID，``None`` 表示由通信层自行挑选。
        self_info: 机器人自身标识 ``{"platform": ..., "user_id": ...}``，随每个
            非元动作一起发出（规范：多账号共享一条连接时实现端据此选账号）。
    """

    def __init__(
        self,
        transport: OneBot12Transport,
        connection_id: str | None = None,
        self_info: dict[str, str] | None = None,
    ) -> None:
        self.transport = transport
        self.connection_id = connection_id
        self.self_info = self_info or {}

    async def request(self, action: str, params: dict[str, Any] | None = None) -> Any:
        """发送动作请求。

        Args:
            action: 动作名。
            params: 动作参数。

        Returns:
            动作响应的 ``data``。

        Raises:
            OneBot12ActionError: 动作失败。
        """
        return await self.transport.request(
            action,
            params,
            prefer=self.connection_id,
            self_info=self.self_info,
        )

    async def upload_file(self, upload_type: str, payload: str, name: str = "") -> str:
        """调用 ``upload_file`` 把媒体换成 ``file_id``。

        实现端可能只支持 ``data`` 方式，因此 ``url`` / ``path`` 失败时降级为
        按字节上传。

        Args:
            upload_type: 规范定义的 ``url`` / ``path`` / ``data``。
            payload: 对应的取值。
            name: 文件名，实现端靠它推断媒体类型；留空时从 ``payload`` 猜。

        Returns:
            ``file_id``；失败时返回空字符串。
        """
        resolved_name = name or _basename(payload) or "file"
        params: dict[str, Any] = {"type": upload_type, "name": resolved_name}
        if upload_type == "url":
            params["url"] = payload
        elif upload_type == "path":
            params["path"] = payload
        else:
            params["data"] = payload

        try:
            data = await self.request("upload_file", params)
        except OneBot12ActionError as exc:
            # 只有「这个方式实现端做不到」才值得降级重试。除规范建议的 10004 外，
            # 实现端也可能用 10003 Bad Param / 20002 来表达同一个意思，一并放行；
            # 其它错误（网络、文件系统）换个方式重发只会把同样的内容再传一遍，
            # 还会掩盖真实原因。
            if upload_type == "data" or exc.retcode not in DOWNGRADEABLE_RETCODES:
                logger.error(
                    "[onebot12] 上传媒体失败 (type=%s, retcode=%s): %s",
                    upload_type,
                    exc.retcode,
                    exc.message,
                )
                return ""
            logger.warning(
                "[onebot12] upload_file(type=%s) 不被支持(%s)，改为按字节上传",
                upload_type,
                exc.message,
            )
            try:
                raw = await self._read_bytes(upload_type, payload)
            except Exception as fallback_exc:
                logger.error("[onebot12] 读取待上传媒体失败: %s", fallback_exc)
                return ""
            return await self.upload_file(
                "data",
                base64.b64encode(raw).decode("ascii"),
                resolved_name,
            )

        if isinstance(data, dict):
            file_id = data.get("file_id")
            if isinstance(file_id, str) and file_id:
                return file_id
        logger.warning("[onebot12] upload_file 响应缺少 file_id: %s", data)
        return ""

    async def _read_bytes(self, upload_type: str, payload: str) -> bytes:
        """读取待上传媒体的字节内容，供降级上传使用。

        Args:
            upload_type: ``url`` 或 ``path``。
            payload: 对应的取值。

        Returns:
            媒体字节内容。
        """
        if upload_type == "url":
            async with self.transport.session_for_media() as session:
                async with session.get(payload) as response:
                    return await response.read()

        from astrbot.core.utils.media_utils import file_uri_to_path, is_file_uri

        path = file_uri_to_path(payload) if is_file_uri(payload) else payload
        return await asyncio.to_thread(Path(path).read_bytes)

    async def send(
        self,
        message: MessageChain,
        params: dict[str, Any],
    ) -> bool:
        """转换并发送一个消息链。

        Args:
            message: AstrBot 消息链。
            params: ``send_message`` 的目标参数（不含 ``message``）。

        Returns:
            是否真的发出了内容。

        Raises:
            OneBot12ActionError: 实现端返回失败时抛出。
        """
        segments = await convert_outgoing_message(
            list(message.chain),
            upload=self.upload_file,
        )
        if not segments:
            return False
        payload = dict(params)
        payload["message"] = segments
        await self.request("send_message", payload)
        return True


class OneBot12Event(AstrMessageEvent):
    """OneBot 12 消息事件。

    Args:
        message_str: 纯文本消息。
        message_obj: AstrBot 消息对象。
        platform_meta: 平台元数据。
        session_id: 会话 ID。
        transport: OneBot 12 通信层。
        detail_type: OneBot 12 的 ``detail_type``，决定回复时的 ``send_message`` 目标。
        guild_id: 频道所属 Guild ID（仅 ``channel`` 消息有值）。
    """

    _nickname_cache: OrderedDict[str, tuple[float, str]] = OrderedDict()

    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        transport: OneBot12Transport,
        detail_type: str = "private",
        guild_id: str = "",
    ) -> None:
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.transport = transport
        self.detail_type = detail_type
        self.guild_id = guild_id

    # -- 内部工具 ---------------------------------------------------------

    def _connection_id(self) -> str | None:
        """取事件来源连接 ID。

        Returns:
            连接 ID；事件不是从连接上来的（例如测试注入）时为 ``None``。
        """
        raw = getattr(self.message_obj, "raw_message", None)
        if isinstance(raw, dict):
            value = raw.get(CONNECTION_ID_KEY)
            if isinstance(value, str) and value:
                return value
        return None

    async def _request(self, action: str, params: dict[str, Any] | None = None) -> Any:
        """按事件来源连接发送动作请求。

        Args:
            action: 动作名。
            params: 动作参数。

        Returns:
            动作响应的 ``data``。

        Raises:
            OneBot12ActionError: 动作失败。
        """
        return await self.transport.request(
            action,
            params,
            prefer=self._connection_id(),
            self_info=self.self_info(),
        )

    def self_info(self) -> dict[str, str]:
        """取机器人自身标识。

        规范要求多账号共享一条连接时「除元动作外的动作请求」都带 ``self``，
        否则实现端可以回 ``10101 Who Am I``。

        Returns:
            ``{"platform": ..., "user_id": ...}``；信息不全时返回空字典。
        """
        raw = getattr(self.message_obj, "raw_message", None)
        platform = ""
        if isinstance(raw, dict):
            platform = str((raw.get("self") or {}).get("platform") or "")
        user_id = self.get_self_id()
        if platform and user_id:
            return {"platform": platform, "user_id": user_id}
        return {}

    def _route(self) -> dict[str, Any]:
        """按当前会话生成 ``send_message`` 的目标参数。

        标准 ``detail_type`` 用标准参数名；实现端扩展的类型（如 ``qq.discuss``）
        按规范的扩展规则用 ``<detail_type>_id`` 作为参数名
        （``interface/message/actions.md``）。

        Returns:
            含 ``detail_type`` 与目标 ID 的字典。
        """
        detail_type = self.detail_type or "private"
        if detail_type == "group":
            return {"detail_type": "group", "group_id": self.get_group_id()}
        if detail_type == "channel":
            return {
                "detail_type": "channel",
                "guild_id": self.guild_id,
                "channel_id": self.session_id.split(":", 1)[-1],
            }
        if detail_type == "private":
            return {"detail_type": "private", "user_id": self.get_sender_id()}
        if "." in detail_type:
            # 扩展类型：qq.discuss -> qq.discuss_id
            return {
                "detail_type": detail_type,
                f"{detail_type}_id": self.session_id or self.get_sender_id(),
            }
        return {"detail_type": detail_type, "user_id": self.get_sender_id()}

    def _message_params(self) -> dict[str, Any]:
        """生成 ``send_message`` 的目标参数（``self`` 由通信层统一附加）。

        Returns:
            ``send_message`` 的目标参数。
        """
        return self._route()

    # -- AstrMessageEvent 接口 -------------------------------------------

    async def send(self, message: MessageChain) -> None:
        """把消息链发到 OneBot 12 实现端。

        Args:
            message: AstrBot 消息链。

        Raises:
            OneBot12ActionError: 实现端返回失败时抛出，交由 AstrBot 记录。
        """
        params = self._message_params()
        uploaded = await OneBot12Outgoing(
            self.transport,
            self._connection_id(),
            self.self_info(),
        ).send(
            message,
            params,
        )
        if not uploaded:
            logger.warning("[onebot12] 消息链转换后为空，未发送任何内容")
            return
        await super().send(message)

    async def send_streaming(
        self,
        generator: AsyncGenerator[MessageChain, None],
        use_fallback: bool = False,
    ) -> None:
        """OneBot 12 没有流式消息语义，这里退化成合并后一次性发送。

        Args:
            generator: 消息链异步生成器。
            use_fallback: 保留参数，与父类签名保持一致。
        """
        buffer: MessageChain | None = None
        async for chain in generator:
            if buffer is None:
                buffer = chain
            else:
                buffer.chain.extend(chain.chain)
        if buffer is not None and buffer.chain:
            buffer.squash_plain()
            await self.send(buffer)
        await super().send_streaming(generator, use_fallback)

    async def get_group(
        self, group_id: str | None = None, **kwargs: Any
    ) -> Group | None:
        """查询群信息，并尽量补齐成员列表与群主。

        OneBot 12 的 ``get_group_member_info`` 没有角色字段，因此
        ``group_owner`` / ``group_admins`` 无法可靠填充，这里保持为空。

        Args:
            group_id: 群 ID，缺省使用当前消息的群。
            **kwargs: 保留参数。

        Returns:
            群信息；私聊且未指定群号时返回 ``None``。
        """
        resolved_group_id = str(group_id or self.get_group_id())
        if not resolved_group_id:
            return None

        current = self.message_obj.group
        group = (
            current
            if current and current.group_id == resolved_group_id
            else Group(group_id=resolved_group_id)
        )

        try:
            info = await self._request(
                "get_group_info",
                {"group_id": resolved_group_id},
            )
        except OneBot12ActionError as exc:
            logger.warning("[onebot12] 获取群信息失败: %s", exc)
            info = None
        if isinstance(info, dict):
            group.group_name = info.get("group_name") or group.group_name

        try:
            members = await self._request(
                "get_group_member_list",
                {"group_id": resolved_group_id},
            )
        except OneBot12ActionError as exc:
            logger.warning("[onebot12] 获取群成员列表失败: %s", exc)
            return group
        if not isinstance(members, list):
            return group

        group.members = [
            MessageMember(
                user_id=str(member["user_id"]),
                nickname=member.get("user_displayname")
                or member.get("user_name")
                or "",
            )
            for member in members
            if isinstance(member, dict) and member.get("user_id") is not None
        ]
        group.member_count = len(group.members)
        return group

    async def react(self, emoji: str) -> None:
        """OneBot 12 标准没有表情回应动作，退化为发送一条消息。

        Args:
            emoji: 表情文本。
        """
        from astrbot.api.message_components import Plain

        await self.send(MessageChain([Plain(text=emoji)]))

    async def fetch_nickname(self, timeout: float | None = None) -> str:
        """补全发送者昵称。

        OneBot 12 的消息事件不带昵称，只能额外调一次 ``get_group_member_info``
        或 ``get_user_info``。结果带 TTL 缓存，避免每条消息都打一次 API。

        该调用发生在消息进管线之前，实现端不响应时会把整条消息卡住，所以这里用
        ``timeout`` 限制等待时间，超时就放弃昵称。

        Args:
            timeout: 最长等待时间（秒），缺省取 :data:`NICKNAME_FETCH_TIMEOUT`。

        Returns:
            昵称；查不到或超时时返回空字符串。
        """
        resolved_timeout = NICKNAME_FETCH_TIMEOUT if timeout is None else timeout
        sender_id = self.get_sender_id()
        if not sender_id:
            return ""
        group_id = self.get_group_id() if self.detail_type == "group" else ""
        cache_key = f"{group_id}:{sender_id}" if group_id else sender_id

        cached = self._nickname_cache.get(cache_key)
        now = time.time()
        if cached is not None and now - cached[0] < NICKNAME_CACHE_TTL:
            self._nickname_cache.move_to_end(cache_key)
            return cached[1]

        try:
            if group_id:
                data = await asyncio.wait_for(
                    self._request(
                        "get_group_member_info",
                        {"group_id": group_id, "user_id": sender_id},
                    ),
                    timeout=resolved_timeout,
                )
            else:
                data = await asyncio.wait_for(
                    self._request("get_user_info", {"user_id": sender_id}),
                    timeout=resolved_timeout,
                )
        except TimeoutError:
            logger.warning(
                "[onebot12] 获取昵称超时（%.1fs），本次跳过: %s",
                resolved_timeout,
                sender_id,
            )
            return ""
        except OneBot12ActionError as exc:
            logger.debug("[onebot12] 获取昵称失败: %s", exc)
            return ""

        nickname = ""
        if isinstance(data, dict):
            nickname = str(
                data.get("user_displayname") or data.get("user_name") or "",
            )
        self._nickname_cache[cache_key] = (now, nickname)
        self._nickname_cache.move_to_end(cache_key)
        while len(self._nickname_cache) > NICKNAME_CACHE_SIZE:
            self._nickname_cache.popitem(last=False)
        return nickname
