"""``onebot12_adapter`` 的单元测试：事件转换、媒体落地、发送选路与昵称补全。

所有测试都用假的 transport 替换真实连接层，不产生任何网络请求。
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from astrbot.api.message_components import At, Image, Plain, Record
from astrbot.api.platform import MessageType
from astrbot.api.event import MessageChain
from astrbot.core.platform.astr_message_event import MessageSession
from astrbot_plugin_onebot12.onebot12_adapter import (
    ADAPTER_NAME,
    DEFAULT_CONFIG,
    OneBot12Adapter,
    build_send_params,
    is_registered,
)
from astrbot_plugin_onebot12.onebot12_event import CONNECTION_ID_KEY, OneBot12Event
from astrbot_plugin_onebot12.onebot12_message import OneBot12ActionError


class FakeTransport:
    """替代 :class:`OneBot12Transport` 的测试替身。"""

    def __init__(self, responses: dict[str, Any] | None = None) -> None:
        self.mode = "ws_reverse"
        self.requests: list[tuple[str, dict, str | None]] = []
        self.self_infos: list[dict | None] = []
        """每次请求附带的机器人自身标识，按调用顺序记录。"""
        self.responses = responses or {}

    async def request(
        self,
        action: str,
        params: dict | None = None,
        *,
        prefer: str | None = None,
        self_info: dict | None = None,
    ) -> Any:
        """记录动作请求并按预设返回。

        Args:
            action: 动作名。
            params: 动作参数。
            prefer: 优先连接 ID。
            self_info: 机器人自身标识（规范要求非元动作携带）。

        Returns:
            预设的响应数据。

        Raises:
            OneBot12ActionError: 预设值本身是异常时抛出。
        """
        self.requests.append((action, params or {}, prefer))
        self.self_infos.append(self_info)
        if action not in self.responses:
            raise OneBot12ActionError(action, 10002, "Unsupported Action")
        result = self.responses[action]
        if isinstance(result, Exception):
            raise result
        return result


def make_config(**overrides) -> dict:
    """生成一份平台配置。

    Args:
        **overrides: 需要覆盖的字段。

    Returns:
        平台配置字典。
    """
    config = dict(DEFAULT_CONFIG)
    config.update({"id": "onebot12-test", "type": ADAPTER_NAME, "enable": True})
    config.update(overrides)
    return config


def make_adapter(
    responses: dict[str, Any] | None = None,
    queue: asyncio.Queue | None = None,
    **config_overrides,
) -> OneBot12Adapter:
    """构造一个使用假 transport 的适配器。

    Args:
        responses: 假 transport 的预设响应。
        queue: 事件队列，缺省新建。
        **config_overrides: 平台配置覆盖项。

    Returns:
        :class:`OneBot12Adapter` 实例。
    """
    adapter = OneBot12Adapter(
        make_config(**config_overrides),
        {},
        queue if queue is not None else asyncio.Queue(),
    )
    adapter.transport = FakeTransport(responses)
    return adapter


def message_event(**overrides) -> dict:
    """构造一个 OneBot 12 群消息事件。

    Args:
        **overrides: 需要覆盖的字段。

    Returns:
        群消息事件字典。
    """
    event = {
        "id": "evt-1",
        "self": {"platform": "qq", "user_id": "123234"},
        "time": 1632847927.599013,
        "type": "message",
        "detail_type": "group",
        "sub_type": "",
        "message_id": "6283",
        "message": [{"type": "text", "data": {"text": "hi"}}],
        "alt_message": "hi",
        "group_id": "12345",
        "user_id": "123456788",
    }
    event.update(overrides)
    return event


class TestRegistration:
    """适配器注册。"""

    def test_registered_after_import(self) -> None:
        assert is_registered() is True


class TestConfigParsing:
    """配置解析与兜底。"""

    def test_defaults(self) -> None:
        adapter = make_adapter()

        assert adapter.mode == "ws_reverse"
        assert adapter.ws_path == "/onebot/v12"
        assert adapter.ws_port == 6700
        assert adapter.meta().name == ADAPTER_NAME
        assert adapter.meta().id == "onebot12-test"
        assert adapter.get_client() is adapter.transport

    def test_unknown_mode_falls_back(self) -> None:
        adapter = make_adapter(connection_mode="carrier_pigeon")

        assert adapter.mode == "ws_reverse"

    def test_path_gets_leading_slash(self) -> None:
        adapter = make_adapter(ws_path="onebot/v12")

        assert adapter.ws_path == "/onebot/v12"

    def test_non_numeric_port_uses_default(self) -> None:
        adapter = make_adapter(ws_port="not-a-port", api_timeout="bad")

        assert adapter.ws_port == 6700
        assert adapter.api_timeout == 30

    def test_ports_are_clamped_to_at_least_one(self) -> None:
        adapter = make_adapter(ws_port=0, http_port=-5, poll_interval=0)

        assert adapter.ws_port == 1
        assert adapter.http_port == 1
        assert adapter.poll_interval == 1

    def test_media_mode_fallback(self) -> None:
        adapter = make_adapter(media_resolve_mode="whatever")

        assert adapter.media_resolve_mode == "auto"

    def test_http_poll_requires_api_base_url(self) -> None:
        # HTTP 轮询没有实现端 HTTP API 地址就取不到事件，配置错了要早失败
        with pytest.raises(ValueError, match="实现端 HTTP API 地址"):
            make_adapter(connection_mode="http_poll", api_base_url="http://")

    def test_http_poll_accepts_valid_api_base_url(self) -> None:
        adapter = make_adapter(connection_mode="http_poll")

        assert adapter.mode == "http_poll"

    def test_ws_forward_does_not_need_api_base_url(self) -> None:
        # 正向 WebSocket 用的是 ws_url，不该被 api_base_url 拦下
        adapter = make_adapter(connection_mode="ws_forward", api_base_url="http://")

        assert adapter.mode == "ws_forward"

    def test_http_post_without_api_base_url_still_starts(self) -> None:
        # HTTP Webhook 少了它仍能收事件，只是发不出消息：告警而非报错
        adapter = make_adapter(connection_mode="http_post", api_base_url="http://")

        assert adapter.mode == "http_post"
        assert adapter._media_api_endpoint() is None  # noqa: SLF001


class TestMessageConversion:
    """事件 -> AstrBotMessage。"""

    def test_message_event(self) -> None:
        adapter = make_adapter()
        abm = adapter.convert_message(message_event(), "ws-reverse#1")

        assert abm.type is MessageType.GROUP_MESSAGE
        assert abm.session_id == "12345"
        assert abm.raw_message[CONNECTION_ID_KEY] == "ws-reverse#1"
        assert abm.raw_message["_detail_type"] == "group"
        assert adapter._self_user_id == "123234"  # noqa: SLF001

    def test_channel_event_keeps_self_platform(self) -> None:
        adapter = make_adapter()
        abm = adapter.convert_message(
            message_event(detail_type="channel", guild_id="g", channel_id="c"),
            None,
        )

        assert abm.session_id == "g:c"
        assert abm.raw_message[CONNECTION_ID_KEY] is None

    def test_notice_event_group(self) -> None:
        adapter = make_adapter()
        abm = adapter.convert_notice(
            {
                "id": "evt-2",
                "self": {"platform": "qq", "user_id": "1"},
                "time": 1632847927.599013,
                "type": "notice",
                "detail_type": "group_member_increase",
                "sub_type": "join",
                "group_id": "12345",
                "user_id": "2",
                "operator_id": "3",
            },
            "ws-reverse#2",
        )

        assert abm.type is MessageType.GROUP_MESSAGE
        assert abm.session_id == "12345"
        assert abm.sender.user_id == "2"
        assert abm.message == []
        assert abm.raw_message["detail_type"] == "group_member_increase"

    def test_notice_event_private_falls_back_to_operator(self) -> None:
        adapter = make_adapter()
        abm = adapter.convert_notice(
            {
                "id": "evt-3",
                "type": "notice",
                "detail_type": "friend_recall",
                "user_id": "",
                "operator_id": "9",
            },
        )

        assert abm.type is MessageType.FRIEND_MESSAGE
        assert abm.session_id == "9"


class TestHandleMessage:
    """入队前的处理：媒体落地、昵称补全、自消息过滤。"""

    async def test_commits_event_and_resolves_image(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(
            responses={
                "get_file": {
                    "name": "a.png",
                    "url": "https://cdn.example.com/a.png",
                },
            },
            queue=queue,
        )
        abm = adapter.convert_message(
            message_event(
                message=[
                    {"type": "text", "data": {"text": "看图"}},
                    {"type": "image", "data": {"file_id": "img-1"}},
                ],
            ),
            "ws-reverse#1",
        )

        await adapter.handle_msg(abm)

        assert queue.qsize() == 1
        event = queue.get_nowait()
        assert isinstance(event, OneBot12Event)
        assert event.detail_type == "group"

        image = event.get_messages()[1]
        assert isinstance(image, Image)
        # 网络不可达时 url 保留远端地址，file 指向惰性引用；无论哪条路径都不抛异常
        assert image.file.startswith("onebot12://") or image.path != ""

        assert adapter.transport.requests[0][0] == "get_file"  # type: ignore[attr-defined]
        assert adapter.transport.requests[0][2] == "ws-reverse#1"  # type: ignore[attr-defined]

    async def test_media_disabled_does_not_call_get_file(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(media_resolve_mode="none", queue=queue)
        abm = adapter.convert_message(
            message_event(
                message=[{"type": "image", "data": {"file_id": "img-1"}}],
                alt_message="",
            ),
            None,
        )

        await adapter.handle_msg(abm)

        actions = [call[0] for call in adapter.transport.requests]  # type: ignore[attr-defined]
        assert "get_file" not in actions
        assert queue.qsize() == 1

    async def test_auto_mode_only_resolves_images(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(
            responses={"get_file": {"url": "https://cdn.example.com/v.wav"}},
            fetch_nickname=False,
            queue=queue,
        )
        abm = adapter.convert_message(
            message_event(
                message=[
                    {"type": "voice", "data": {"file_id": "voice-1"}},
                    {"type": "image", "data": {"file_id": "img-1"}},
                ],
                alt_message="",
            ),
            None,
        )

        await adapter.handle_msg(abm)

        actions = [call[0] for call in adapter.transport.requests]  # type: ignore[attr-defined]
        # auto 模式只对图片调 get_file
        assert actions == ["get_file"]

        messages = queue.get_nowait().get_messages()
        voice, image = messages[0], messages[1]
        assert isinstance(voice, Record)
        assert voice.file == "onebot12://voice-1"
        # 图片尽力落地，网络不可达时退化为默认 URL
        assert isinstance(image, Image)
        assert image.file.startswith("onebot12://") or image.file == ""
        assert image.url.startswith("https://") or image.file != "onebot12://img-1"

    async def test_all_mode_resolves_voice_too(self, tmp_path) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        voice_file = tmp_path / "voice.wav"
        voice_file.write_bytes(b"RIFF....WAVEfmt ")
        adapter = make_adapter(
            responses={"get_file": {"path": str(voice_file)}},
            media_resolve_mode="all",
            fetch_nickname=False,
            queue=queue,
        )
        abm = adapter.convert_message(
            message_event(
                message=[{"type": "voice", "data": {"file_id": "voice-1"}}],
                alt_message="",
            ),
            None,
        )

        await adapter.handle_msg(abm)

        event = queue.get_nowait()
        voice = event.get_messages()[0]
        assert voice.file == str(voice_file)
        assert voice.path == str(voice_file)
        # 落地出的文件必须登记到事件上，事件结束后由核心清理
        assert str(voice_file) in event._temporary_local_files  # noqa: SLF001

    async def test_get_file_failure_keeps_reference(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(queue=queue)  # 假 transport 一律返回 10002
        abm = adapter.convert_message(
            message_event(
                message=[{"type": "image", "data": {"file_id": "img-x"}}],
                alt_message="",
            ),
            None,
        )

        await adapter.handle_msg(abm)

        image = queue.get_nowait().get_messages()[0]
        assert image.file == "onebot12://img-x"

    async def test_bot_own_message_is_dropped(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(queue=queue)
        # 事件里的 self.user_id 是 123234，消息发送者也是它 -> 自己的消息
        abm = adapter.convert_message(message_event(user_id="123234"), None)
        assert abm.sender.user_id == "123234"

        await adapter.handle_msg(abm)

        assert queue.qsize() == 0

    async def test_other_user_message_is_not_dropped(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(queue=queue)
        adapter.convert_message(message_event(user_id="123234"), None)  # 先确定 self_id

        await adapter.handle_msg(
            adapter.convert_message(message_event(user_id="999"), None),
        )

        assert queue.qsize() == 1

    async def test_nickname_is_fetched_for_group_sender(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        # 用随机 ID 避免命中类级昵称缓存
        user_id = f"u-{uuid.uuid4().hex[:8]}"
        adapter = make_adapter(
            responses={
                "get_group_member_info": {
                    "user_id": user_id,
                    "user_name": "大名",
                    "user_displayname": "群名片",
                },
            },
            queue=queue,
        )
        abm = adapter.convert_message(message_event(user_id=user_id), None)

        await adapter.handle_msg(abm)

        assert abm.sender.nickname == "群名片"

    async def test_nickname_disabled(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        user_id = f"u-{uuid.uuid4().hex[:8]}"
        adapter = make_adapter(fetch_nickname=False, queue=queue)
        abm = adapter.convert_message(message_event(user_id=user_id), None)

        await adapter.handle_msg(abm)

        assert abm.sender.nickname == ""
        assert adapter.transport.requests == []  # type: ignore[attr-defined]

    async def test_nickname_cache_hits_second_time(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        user_id = f"u-{uuid.uuid4().hex[:8]}"
        adapter = make_adapter(
            responses={"get_group_member_info": {"user_name": "缓存名"}},
            queue=queue,
        )

        for _ in range(2):
            abm = adapter.convert_message(message_event(user_id=user_id), None)
            await adapter.handle_msg(abm)

        actions = [call[0] for call in adapter.transport.requests]  # type: ignore[attr-defined]
        assert actions.count("get_group_member_info") == 1

    async def test_nickname_timeout_does_not_block_event(self) -> None:
        """实现端不响应昵称查询时，消息仍必须进管线。"""

        class HangingTransport(FakeTransport):
            async def request(
                self, action, params=None, *, prefer=None, self_info=None
            ):
                self.requests.append((action, params or {}, prefer))
                await asyncio.sleep(30)
                raise AssertionError("不应走到这里")

        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(queue=queue)
        adapter.transport = HangingTransport()

        user_id = f"u-{uuid.uuid4().hex[:8]}"
        abm = adapter.convert_message(message_event(user_id=user_id), None)

        await asyncio.wait_for(adapter.handle_msg(abm), timeout=10)

        assert queue.qsize() == 1
        assert abm.sender.nickname == ""


class TestMetaEvent:
    """元事件处理。"""

    def test_connect_records_impl_version(self) -> None:
        adapter = make_adapter()

        adapter._handle_meta_event(  # noqa: SLF001
            {
                "type": "meta",
                "detail_type": "connect",
                "version": {
                    "impl": "fake-impl",
                    "version": "1.2.0",
                    "onebot_version": "12",
                },
            },
        )

        assert adapter.impl_version["impl"] == "fake-impl"
        assert adapter.impl_version["onebot_version"] == "12"

    def test_status_update_records_bot_identity(self) -> None:
        adapter = make_adapter()

        adapter._handle_meta_event(  # noqa: SLF001
            {
                "type": "meta",
                "detail_type": "status_update",
                "status": {
                    "good": True,
                    "bots": [
                        {"self": {"platform": "telegram", "user_id": "42"}},
                    ],
                },
            },
        )

        assert adapter._self_platform == "telegram"  # noqa: SLF001
        assert adapter._self_user_id == "42"  # noqa: SLF001

    async def test_raw_event_routes_by_type(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter = make_adapter(queue=queue)

        await adapter._on_raw_event(  # noqa: SLF001
            {"type": "meta", "detail_type": "heartbeat", "interval": 5000},
            None,
        )
        assert queue.qsize() == 0

        await adapter._on_raw_event(message_event(), "ws-reverse#9")  # noqa: SLF001
        assert queue.qsize() == 1

        await adapter._on_raw_event(  # noqa: SLF001
            {
                "type": "notice",
                "detail_type": "group_member_decrease",
                "group_id": "1",
                "user_id": "2",
            },
            "ws-reverse#9",
        )
        assert queue.qsize() == 2

        # request 类事件暂不处理
        await adapter._on_raw_event({"type": "request"}, None)  # noqa: SLF001
        assert queue.qsize() == 2


class TestSendBySession:
    """不依赖 event 的会话发送。"""

    async def test_group_session(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        session = MessageSession("onebot12-test", MessageType.GROUP_MESSAGE, "12345")

        await adapter.send_by_session(session, MessageChain([Plain(text="hi")]))

        action, params, prefer = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert action == "send_message"
        assert params == {
            "detail_type": "group",
            "group_id": "12345",
            "message": [{"type": "text", "data": {"text": "hi"}}],
        }
        assert prefer is None

    async def test_channel_session(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        session = MessageSession("onebot12-test", MessageType.GROUP_MESSAGE, "g:10086")

        await adapter.send_by_session(session, MessageChain([Plain(text="hi")]))

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["detail_type"] == "channel"
        assert params["guild_id"] == "g"
        assert params["channel_id"] == "10086"

    async def test_private_session(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        session = MessageSession("onebot12-test", MessageType.FRIEND_MESSAGE, "999")

        await adapter.send_by_session(session, MessageChain([Plain(text="hi")]))

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["detail_type"] == "private"
        assert params["user_id"] == "999"


class TestBuildSendParams:
    """``send_message`` 目标参数拼装。"""

    def test_group(self) -> None:
        assert build_send_params("group", "42") == {
            "detail_type": "group",
            "group_id": "42",
        }

    def test_channel(self) -> None:
        assert build_send_params("channel", "g:c") == {
            "detail_type": "channel",
            "guild_id": "g",
            "channel_id": "c",
        }

    def test_private(self) -> None:
        assert build_send_params("private", "7") == {
            "detail_type": "private",
            "user_id": "7",
        }

    def test_extended_detail_type_uses_prefixed_id(self) -> None:
        # 规范 interface/message/actions.md：扩展类型用 <detail_type>_id，
        # 例如 qq.discuss 配 qq.discuss_id（早期版本错用了 user_id）
        assert build_send_params("qq.discuss", "5") == {
            "detail_type": "qq.discuss",
            "qq.discuss_id": "5",
        }

    def test_unknown_detail_type_without_dot_falls_back_to_user_id(self) -> None:
        assert build_send_params("whatever", "5") == {
            "detail_type": "whatever",
            "user_id": "5",
        }


class TestMediaEndpoint:
    """HTTP API 地址解析。"""

    def test_valid(self) -> None:
        adapter = make_adapter()

        endpoint = adapter._media_api_endpoint()  # noqa: SLF001

        assert endpoint is not None
        assert endpoint.url() == "http://127.0.0.1:6700/"

    def test_invalid_returns_none(self) -> None:
        # 空字符串会退回默认值，这里用一个解析不出主机名的地址
        adapter = make_adapter(api_base_url="http://")

        assert adapter._media_api_endpoint() is None  # noqa: SLF001


class TestEventRouting:
    """事件对象的发送选路。"""

    async def test_send_uses_origin_connection(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        abm = adapter.convert_message(message_event(), "ws-reverse#7")
        event = adapter.create_event(abm)

        await event.send(MessageChain([Plain(text="hi")]))

        action, params, prefer = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert action == "send_message"
        assert prefer == "ws-reverse#7"
        assert params["detail_type"] == "group"
        assert params["group_id"] == "12345"
        assert params["message"] == [{"type": "text", "data": {"text": "hi"}}]
        # self 是动作请求的顶层字段，不在 params 里（规范 action-request.md）
        assert adapter.transport.self_infos[0] == {  # type: ignore[attr-defined]
            "platform": "qq",
            "user_id": "123234",
        }
        assert "self" not in params

    async def test_send_raises_on_failure(self) -> None:
        adapter = make_adapter()  # 假 transport 默认抛 10002
        abm = adapter.convert_message(message_event(), None)
        event = adapter.create_event(abm)

        with pytest.raises(OneBot12ActionError):
            await event.send(MessageChain([Plain(text="hi")]))

    async def test_send_streaming_merges_chains(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        event = adapter.create_event(adapter.convert_message(message_event(), None))

        async def gen():
            yield MessageChain([Plain(text="a")])
            yield MessageChain([Plain(text="b")])

        await event.send_streaming(gen())

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["message"] == [{"type": "text", "data": {"text": "ab"}}]

    async def test_private_route(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        event_with_group = message_event()
        event_with_group["detail_type"] = "private"
        event_with_group.pop("group_id")
        event = adapter.create_event(
            adapter.convert_message(event_with_group, None),
        )

        await event.send(MessageChain([Plain(text="hi")]))

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["detail_type"] == "private"
        assert params["user_id"] == "123456788"

    async def test_channel_route(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        event_with_channel = message_event(
            detail_type="channel",
            guild_id="guild-1",
            channel_id="chan-2",
        )
        event = adapter.create_event(
            adapter.convert_message(event_with_channel, None),
        )

        await event.send(MessageChain([Plain(text="hi")]))

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["detail_type"] == "channel"
        assert params["guild_id"] == "guild-1"
        assert params["channel_id"] == "chan-2"


class TestOutgoingUploadFallback:
    """``upload_file`` 的降级策略。"""

    async def test_url_upload_used_when_supported(self) -> None:
        adapter = make_adapter(responses={"upload_file": {"file_id": "fid-1"}})
        from astrbot_plugin_onebot12.onebot12_event import OneBot12Outgoing

        outgoing = OneBot12Outgoing(adapter.transport)  # type: ignore[arg-type]
        file_id = await outgoing.upload_file("url", "https://example.com/a.png")

        assert file_id == "fid-1"
        action, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert action == "upload_file"
        assert params["type"] == "url"
        assert params["name"] == "a.png"

    async def test_path_upload_falls_back_to_data(self, tmp_path) -> None:
        payload = b"hello-onebot12"
        target = tmp_path / "sample.bin"
        target.write_bytes(payload)

        calls: list[str] = []

        class FallbackTransport(FakeTransport):
            async def request(
                self, action, params=None, *, prefer=None, self_info=None
            ):
                calls.append(params.get("type", ""))
                if params.get("type") == "data":
                    return {"file_id": "fid-data"}
                raise OneBot12ActionError(action, 10004, "Unsupported Param")

        adapter = make_adapter()
        adapter.transport = FallbackTransport()
        from astrbot_plugin_onebot12.onebot12_event import OneBot12Outgoing

        outgoing = OneBot12Outgoing(adapter.transport)  # type: ignore[arg-type]
        file_id = await outgoing.upload_file("path", str(target))

        assert calls == ["path", "data"]
        assert file_id == "fid-data"

    async def test_data_upload_failure_returns_empty(self) -> None:
        adapter = make_adapter()
        from astrbot_plugin_onebot12.onebot12_event import OneBot12Outgoing

        outgoing = OneBot12Outgoing(adapter.transport)  # type: ignore[arg-type]
        file_id = await outgoing.upload_file("data", "QUJD")

        assert file_id == ""


class TestGetGroup:
    """群信息查询。"""

    async def test_enriches_group(self) -> None:
        adapter = make_adapter(
            responses={
                "get_group_info": {"group_id": "12345", "group_name": "一群大笨蛋"},
                "get_group_member_list": [
                    {
                        "user_id": "1",
                        "user_name": "甲",
                        "user_displayname": "小甲",
                    },
                    {"user_id": "2", "user_name": "乙"},
                ],
            },
        )
        event = adapter.create_event(adapter.convert_message(message_event(), None))

        group = await event.get_group()

        assert group is not None
        assert group.group_name == "一群大笨蛋"
        assert group.member_count == 2
        assert group.members is not None
        assert group.members[0].nickname == "小甲"
        # OneBot 12 没有角色字段，群主/管理员保持为空
        assert group.group_owner is None
        assert group.group_admins is None

    async def test_returns_none_for_private_without_group(self) -> None:
        adapter = make_adapter()
        private_event = message_event(detail_type="private")
        private_event.pop("group_id")
        event = adapter.create_event(adapter.convert_message(private_event, None))

        assert await event.get_group() is None

    async def test_group_survives_api_failure(self) -> None:
        adapter = make_adapter()
        event = adapter.create_event(adapter.convert_message(message_event(), None))

        group = await event.get_group()

        assert group is not None
        assert group.group_id == "12345"


class TestReact:
    """表情回应退化为普通消息。"""

    async def test_react_sends_text(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        event = adapter.create_event(adapter.convert_message(message_event(), None))

        await event.react("👍")

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["message"] == [{"type": "text", "data": {"text": "👍"}}]


class TestAtComponent:
    """At 组件的发送形式。"""

    async def test_at_produces_mention_segment(self) -> None:
        adapter = make_adapter(responses={"send_message": {"message_id": "1"}})
        event = adapter.create_event(adapter.convert_message(message_event(), None))

        await event.send(MessageChain([At(qq=42, name="小明")]))

        _, params, _ = adapter.transport.requests[0]  # type: ignore[attr-defined]
        assert params["message"] == [
            {"type": "mention", "data": {"user_id": "42"}},
            {"type": "text", "data": {"text": " "}},
        ]
