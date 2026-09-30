"""OneBot 12 消息段 <-> AstrBot 消息组件 的双向转换。

OneBot 12 的消息数据类型是「消息段数组」，每个消息段形如
``{"type": "text", "data": {...}}``（见 https://12.onebot.dev/interface/message/type/ ）。

与 OneBot 11 最大的差别在媒体文件：11 的 ``image`` 段直接带 ``file``（路径 / URL /
base64），12 的 ``image`` / ``voice`` / ``audio`` / ``video`` / ``file`` 段只带一个
``file_id``，真正的字节要靠 ``get_file`` 动作换回来（url / path / data 三种方式），
发出去时也要先用 ``upload_file`` 上传拿到 ``file_id``。

因此这里对收到的媒体段做「惰性引用」：把 ``file_id`` 记成
``onebot12://<file_id>`` 形式的引用，等真的要读字节时再通过
:func:`resolve_media_reference` 换成本地路径（见 ``onebot12_adapter.py`` 的
媒体落地逻辑），避免把每条消息的图片都同步下载一遍。
"""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from astrbot.api import logger
from astrbot.api.message_components import (
    At,
    AtAll,
    File,
    Image,
    Location,
    Plain,
    Record,
    Reply,
    Video,
)
from astrbot.api.platform import AstrBotMessage, Group, MessageMember, MessageType

ONEBOT12_MEDIA_SCHEME = "onebot12://"
"""媒体段的惰性引用前缀，后接 OneBot 12 的 ``file_id``。"""

COMPONENT_TO_SEGMENT: dict[str, str] = {
    "Plain": "text",
    "Image": "image",
    "Record": "voice",
    "Video": "video",
    "File": "file",
    "At": "mention",
    "Reply": "reply",
    "Location": "location",
}
"""AstrBot 组件类型名 -> OneBot 12 消息段类型名。"""

SEGMENT_TO_COMPONENT: dict[str, str] = {
    "image": "Image",
    "voice": "Record",
    "audio": "Record",
    "video": "Video",
    "file": "File",
}
"""OneBot 12 媒体消息段类型 -> AstrBot 媒体组件类型名。"""

SEGMENT_PLACEHOLDER: dict[str, str] = {
    "image": "[图片]",
    "voice": "[语音]",
    "audio": "[音频]",
    "video": "[视频]",
    "file": "[文件]",
    "location": "[位置]",
    "reply": "[回复]",
}
"""用于拼出纯文本消息串的占位符，风格对齐 OneBot 12 的 ``alt_message``。"""


class OneBot12ActionError(Exception):
    """OneBot 12 动作请求失败。

    Attributes:
        action: 动作名。
        retcode: OneBot 12 返回码，见 https://12.onebot.dev/connect/data-protocol/action-response/ 。
        message: 实现端给出的人类可读错误信息。
    """

    def __init__(self, action: str, retcode: int, message: str) -> None:
        super().__init__(f"OneBot 12 动作 {action} 失败 (retcode={retcode}): {message}")
        self.action = action
        self.retcode = retcode
        self.message = message


def _as_id(value: Any) -> str:
    """把 OneBot 12 的 ID 字段统一成字符串。

    OneBot 12 规定 ID 都是字符串，但实现端偶尔会给出整数，这里做一次兜底。

    Args:
        value: 原始 ID 值。

    Returns:
        ID 字符串；``None`` 与空值返回空字符串。
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _as_int(value: Any, default: int = 0) -> int:
    """把 OneBot 12 的 ID 字段转成 int，供 AstrBot 中仍是 int 的字段使用。

    Args:
        value: 原始 ID 值，可能是字符串或整数。
        default: 无法转换时的返回值。

    Returns:
        转换后的整数。
    """
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def media_reference(file_id: Any) -> str:
    """构造媒体段的惰性引用字符串。

    Args:
        file_id: OneBot 12 的 ``file_id``。

    Returns:
        ``onebot12://<file_id>`` 形式的引用；``file_id`` 为空时返回空字符串。
    """
    fid = _as_id(file_id)
    return f"{ONEBOT12_MEDIA_SCHEME}{fid}" if fid else ""


def parse_media_reference(reference: str | None) -> str | None:
    """从惰性引用中取回 ``file_id``。

    Args:
        reference: :func:`media_reference` 生成的引用字符串。

    Returns:
        ``file_id``；不是惰性引用时返回 ``None``。
    """
    if reference and reference.startswith(ONEBOT12_MEDIA_SCHEME):
        return reference[len(ONEBOT12_MEDIA_SCHEME) :]
    return None


def build_message_str(chain: list[Any], alt_message: str | None = None) -> str:
    """生成 AstrBot 侧使用的纯文本消息串。

    OneBot 12 的事件里自带 ``alt_message``（消息的纯文本替代表示），优先用它；
    缺失时退回到从消息链拼装。

    Args:
        chain: AstrBot 消息组件列表。
        alt_message: 事件里的 ``alt_message`` 字段。

    Returns:
        纯文本消息串。
    """
    if alt_message:
        return alt_message.strip()
    parts: list[str] = []
    for comp in chain:
        if isinstance(comp, Plain):
            parts.append(comp.text)
        elif isinstance(comp, AtAll):
            # AtAll 是 At 的子类，必须先判断
            parts.append(" @全体成员 ")
        elif isinstance(comp, At):
            parts.append(f" @{comp.name or comp.qq} ")
        else:
            parts.append(SEGMENT_PLACEHOLDER.get(type(comp).__name__.lower(), ""))
    return "".join(parts).strip()


def _convert_segment(seg_type: str, data: dict[str, Any]) -> Any | None:
    """把单个 OneBot 12 消息段转成 AstrBot 组件。

    Args:
        seg_type: 消息段类型名。
        data: 消息段的 ``data`` 字段。

    Returns:
        AstrBot 消息组件；无法识别时返回 ``None``。
    """
    if seg_type == "text":
        text = data.get("text") or ""
        return Plain(text=text) if text else None

    if seg_type == "mention":
        user_id = _as_id(data.get("user_id"))
        if not user_id:
            return None
        return At(qq=_as_int(user_id), name="")

    if seg_type == "mention_all":
        return AtAll()

    if seg_type in SEGMENT_TO_COMPONENT:
        reference = media_reference(data.get("file_id"))
        name = _as_id(data.get("name")) or "file"
        if seg_type in ("image", "voice", "audio", "video"):
            if not reference:
                return None
            if seg_type == "image":
                return Image(file=reference)
            if seg_type in ("voice", "audio"):
                return Record(file=reference)
            return Video(file=reference)
        # file 段没有 file_id 时必须给出文件名，否则无法向用户展示
        return File(name=name, file=reference) if reference else None

    if seg_type == "location":
        return Location(
            lat=float(data.get("latitude") or 0.0),
            lon=float(data.get("longitude") or 0.0),
            title=_as_id(data.get("title")),
            content=_as_id(data.get("content")),
        )

    if seg_type == "reply":
        message_id = _as_id(data.get("message_id"))
        if not message_id:
            return None
        user_id = _as_id(data.get("user_id"))
        return Reply(
            id=message_id,
            chain=[],
            sender_id=_as_int(user_id),
            sender_nickname="",
            time=0,
            message_str="",
            text="",
            qq=_as_int(user_id),
        )

    logger.debug("[onebot12] 暂不支持的消息段类型，已忽略: type=%s", seg_type)
    return None


def convert_incoming_message(
    payload: dict[str, Any],
) -> tuple[AstrBotMessage, str | None]:
    """把 OneBot 12 消息事件转成 AstrBot 消息对象。

    Args:
        payload: OneBot 12 消息事件的完整 JSON（``type`` 为 ``message``）。

    Returns:
        ``(AstrBotMessage, detail_type)``。``detail_type`` 为 ``private`` / ``group`` /
        ``channel`` 或实现端扩展的其它类型，发送回复时按它决定 ``send_message`` 的
        目标参数。
    """
    abm = AstrBotMessage()
    detail_type = _as_id(payload.get("detail_type")) or "private"
    self_info = payload.get("self") or {}
    abm.self_id = _as_id(self_info.get("user_id"))
    abm.message_id = _as_id(payload.get("message_id"))
    abm.raw_message = payload
    abm.sender = MessageMember(
        user_id=_as_id(payload.get("user_id")),
        nickname="",
    )

    raw_segments = payload.get("message")
    if not isinstance(raw_segments, list):
        # 规范允许消息数据类型是单个消息段或字符串，这里统一成列表
        raw_segments = [raw_segments] if raw_segments else []

    chain: list[Any] = []
    for seg in raw_segments:
        if not isinstance(seg, dict):
            continue
        seg_type = _as_id(seg.get("type"))
        data = seg.get("data")
        if not isinstance(data, dict):
            data = {}
        try:
            component = _convert_segment(seg_type, data)
        except Exception as exc:  # 单个消息段解析失败不应丢掉整条消息
            logger.warning(
                "[onebot12] 消息段解析失败，已跳过: type=%s data=%s error=%s",
                seg_type,
                data,
                exc,
            )
            continue
        if component is not None:
            chain.append(component)

    abm.message = chain
    abm.message_str = build_message_str(chain, payload.get("alt_message"))

    timestamp = payload.get("time")
    abm.timestamp = int(float(timestamp)) if timestamp else int(time.time())

    if detail_type == "group":
        group_id = _as_id(payload.get("group_id"))
        abm.type = MessageType.GROUP_MESSAGE
        abm.group = Group(group_id=group_id)
        abm.session_id = group_id
    elif detail_type == "channel":
        guild_id = _as_id(payload.get("guild_id"))
        channel_id = _as_id(payload.get("channel_id"))
        abm.type = MessageType.GROUP_MESSAGE
        # AstrBot 的 Group 只有一级群号，用 guild_id 作为群号，频道号交给 session_id
        abm.group = Group(group_id=guild_id)
        abm.session_id = f"{guild_id}:{channel_id}"
    else:
        abm.type = MessageType.FRIEND_MESSAGE
        abm.session_id = abm.sender.user_id

    return abm, detail_type


async def _media_segment(
    comp_type: str,
    source: str,
    name: str,
    upload: Callable[[str, str, str], Awaitable[str]],
) -> dict[str, Any] | None:
    """把一个 AstrBot 媒体组件转成 OneBot 12 消息段。

    远端 http(s) URL、本地路径与 ``data:`` / ``base64://`` 载荷分别对应
    ``upload_file`` 的 ``url`` / ``path`` / ``data`` 三种方式。

    Args:
        comp_type: OneBot 12 消息段类型名。
        source: 媒体引用（本地路径、``file://`` URI、http(s) URL 或 base64 载荷）。
        name: 上传时使用的文件名，实现端靠它推断媒体类型。
        upload: ``upload_file`` 调用器，接收 ``(type, payload, name)`` 并返回 ``file_id``。

    Returns:
        消息段字典；无法处理时返回 ``None``。
    """
    from astrbot.core.utils.media_utils import (
        file_uri_to_path,
        is_file_uri,
    )

    if source.startswith(("http://", "https://")):
        file_id = await upload("url", source, name)
    elif source.startswith("data:"):
        file_id = await upload("data", source.split(",", 1)[-1], name)
    elif source.startswith("base64://"):
        file_id = await upload("data", source[len("base64://") :], name)
    else:
        path = file_uri_to_path(source) if is_file_uri(source) else source
        file_id = await upload("path", path, name)
    if not file_id:
        return None
    return {"type": comp_type, "data": {"file_id": file_id}}


async def convert_outgoing_message(
    chain: list[Any],
    *,
    upload: Callable[[str, str, str], Awaitable[str]],
) -> list[dict[str, Any]]:
    """把 AstrBot 消息链转成 OneBot 12 消息段数组。

    Args:
        chain: AstrBot 消息组件列表。
        upload: ``upload_file`` 调用器，用于把本地媒体换成 ``file_id``。

    Returns:
        OneBot 12 消息段数组，可直接作为 ``send_message`` 的 ``message`` 参数。
    """
    segments: list[dict[str, Any]] = []
    for comp in chain:
        comp_name = type(comp).__name__

        if isinstance(comp, Plain):
            if comp.text:
                segments.append({"type": "text", "data": {"text": comp.text}})
            continue

        if isinstance(comp, AtAll):
            segments.append({"type": "mention_all", "data": {}})
            continue

        if isinstance(comp, At):
            user_id = _as_id(comp.qq)
            if not user_id:
                continue
            # 规范建议：回复时可能在消息内容里多带一个 mention，这里补一个空格避免粘连
            segments.append({"type": "mention", "data": {"user_id": user_id}})
            segments.append({"type": "text", "data": {"text": " "}})
            continue

        if isinstance(comp, Reply):
            message_id = _as_id(comp.id)
            if not message_id:
                continue
            data: dict[str, Any] = {"message_id": message_id}
            sender_id = _as_id(comp.sender_id)
            if sender_id:
                data["user_id"] = sender_id
            segments.append({"type": "reply", "data": data})
            continue

        if isinstance(comp, Location):
            segments.append(
                {
                    "type": "location",
                    "data": {
                        "latitude": float(comp.lat),
                        "longitude": float(comp.lon),
                        "title": comp.title or "",
                        "content": comp.content or "",
                    },
                },
            )
            continue

        if comp_name in ("Image", "Record", "Video", "File"):
            segment_type = COMPONENT_TO_SEGMENT[comp_name]
            if comp_name == "File":
                # File 的 file 是被 property 包过的读取入口：路径不在磁盘上时返回空串，
                # 本地路径的真身在 file_；先读 file 还会触发它内部的同步下载告警。
                source = comp.file_ or comp.url or ""
                name = comp.name or "file"
            else:
                source = comp.url or comp.file or ""
                name = f"{segment_type}{_guess_suffix(source)}"
            if not source:
                logger.warning(
                    "[onebot12] %s 组件没有可用的文件来源，已跳过", comp_name
                )
                continue
            try:
                segment = await _media_segment(segment_type, source, name, upload)
            except Exception as exc:
                logger.error("[onebot12] 媒体上传失败，已跳过: %s (%s)", source, exc)
                continue
            if segment is not None:
                segments.append(segment)
            continue

        logger.warning("[onebot12] 暂不支持的待发送组件，已跳过: %s", comp_name)

    return segments


def _guess_suffix(source: str) -> str:
    """从媒体引用里猜一个文件后缀，用于 ``upload_file`` 的 ``name`` 参数。

    Args:
        source: 媒体引用。

    Returns:
        形如 ``.png`` 的后缀；猜不出来时返回空字符串。
    """
    tail = source.split("?", 1)[0].rsplit("/", 1)[-1]
    if "." in tail:
        suffix = "." + tail.rsplit(".", 1)[-1]
        if len(suffix) <= 6 and suffix[1:].isalnum():
            return suffix
    return ""


def dump_compact(payload: Any) -> str:
    """把事件/响应压缩成单行 JSON 字符串，用于日志。

    Args:
        payload: 任意可 JSON 序列化的对象。

    Returns:
        单行 JSON 字符串；序列化失败时退回 ``repr``。
    """
    try:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return repr(payload)
