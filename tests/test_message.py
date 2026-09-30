"""``onebot12_message`` 的单元测试：消息段双向转换与惰性媒体引用。"""

from __future__ import annotations

import pytest
from astrbot.api.message_components import (
    At,
    AtAll,
    File,
    Image,
    Location,
    Plain,
    Record,
    Reply,
)
from astrbot.api.platform import MessageType
from astrbot_plugin_onebot12.onebot12_message import (
    ONEBOT12_MEDIA_SCHEME,
    OneBot12ActionError,
    build_message_str,
    convert_incoming_message,
    convert_outgoing_message,
    dump_compact,
    media_reference,
    parse_media_reference,
)


def group_event(**overrides) -> dict:
    """构造一个最小可用的 OneBot 12 群消息事件。

    Args:
        **overrides: 需要覆盖的顶层字段。

    Returns:
        群消息事件字典。
    """
    event = {
        "id": "b6e65187-5ac0-489c-b431-53078e9d2bbb",
        "self": {"platform": "qq", "user_id": "123234"},
        "time": 1632847927.599013,
        "type": "message",
        "detail_type": "group",
        "sub_type": "",
        "message_id": "6283",
        "message": [{"type": "text", "data": {"text": "OneBot is not a bot"}}],
        "alt_message": "OneBot is not a bot",
        "group_id": "12345",
        "user_id": "123456788",
    }
    event.update(overrides)
    return event


class TestMediaReference:
    """惰性媒体引用的编解码。"""

    def test_roundtrip(self) -> None:
        assert media_reference("abc") == f"{ONEBOT12_MEDIA_SCHEME}abc"
        assert parse_media_reference(f"{ONEBOT12_MEDIA_SCHEME}abc") == "abc"

    def test_empty_file_id(self) -> None:
        assert media_reference(None) == ""
        assert media_reference("") == ""

    def test_non_reference(self) -> None:
        assert parse_media_reference("https://example.com/a.png") is None
        assert parse_media_reference(None) is None

    def test_int_file_id_is_stringified(self) -> None:
        # 规范要求 ID 是字符串，但实现端偶尔给整数
        assert parse_media_reference(media_reference(12345)) == "12345"


class TestIncoming:
    """事件 -> AstrBotMessage。"""

    def test_group_text_message(self) -> None:
        abm, detail_type = convert_incoming_message(group_event())

        assert detail_type == "group"
        assert abm.type is MessageType.GROUP_MESSAGE
        assert abm.self_id == "123234"
        assert abm.message_id == "6283"
        assert abm.session_id == "12345"
        assert abm.group is not None and abm.group.group_id == "12345"
        assert abm.sender.user_id == "123456788"
        assert abm.message_str == "OneBot is not a bot"
        assert abm.timestamp == 1632847927
        assert [type(c).__name__ for c in abm.message] == ["Plain"]

    def test_private_message_uses_user_id_as_session(self) -> None:
        event = group_event(detail_type="private")
        event.pop("group_id")
        abm, detail_type = convert_incoming_message(event)

        assert detail_type == "private"
        assert abm.type is MessageType.FRIEND_MESSAGE
        assert abm.group is None
        assert abm.session_id == "123456788"

    def test_channel_message_keeps_guild_and_channel(self) -> None:
        event = group_event(
            detail_type="channel",
            guild_id="g-1",
            channel_id="c-2",
            group_id="12345",
        )
        abm, detail_type = convert_incoming_message(event)

        assert detail_type == "channel"
        assert abm.type is MessageType.GROUP_MESSAGE
        assert abm.group is not None and abm.group.group_id == "g-1"
        assert abm.session_id == "g-1:c-2"

    def test_all_segment_types(self) -> None:
        event = group_event(
            message=[
                {"type": "text", "data": {"text": "hi"}},
                {"type": "mention", "data": {"user_id": "42"}},
                {"type": "mention_all", "data": {}},
                {"type": "image", "data": {"file_id": "img-1"}},
                {"type": "voice", "data": {"file_id": "voice-1"}},
                {"type": "audio", "data": {"file_id": "audio-1"}},
                {"type": "video", "data": {"file_id": "video-1"}},
                {"type": "file", "data": {"file_id": "file-1", "name": "a.zip"}},
                {
                    "type": "location",
                    "data": {
                        "latitude": 31.032315,
                        "longitude": 121.447127,
                        "title": "上海交通大学闵行校区",
                        "content": "东川路800号",
                    },
                },
                {
                    "type": "reply",
                    "data": {"message_id": "6283", "user_id": "999"},
                },
            ],
            alt_message="hi",
        )
        abm, _ = convert_incoming_message(event)

        assert [type(c).__name__ for c in abm.message] == [
            "Plain",
            "At",
            "AtAll",
            "Image",
            "Record",
            "Record",
            "Video",
            "File",
            "Location",
            "Reply",
        ]
        image = abm.message[3]
        assert isinstance(image, Image)
        assert image.file == f"{ONEBOT12_MEDIA_SCHEME}img-1"

        record = abm.message[4]
        assert isinstance(record, Record)
        assert record.file == f"{ONEBOT12_MEDIA_SCHEME}voice-1"

        file_component = abm.message[7]
        assert isinstance(file_component, File)
        assert file_component.name == "a.zip"

        reply = abm.message[9]
        assert isinstance(reply, Reply)
        assert reply.id == "6283"
        assert reply.sender_id == 999
        assert reply.qq == 999

    def test_unknown_segment_is_skipped(self) -> None:
        event = group_event(
            message=[
                {"type": "text", "data": {"text": "keep"}},
                {"type": "qq.mface", "data": {"id": "1"}},
            ],
            alt_message="",
        )
        abm, _ = convert_incoming_message(event)

        assert [type(c).__name__ for c in abm.message] == ["Plain"]

    def test_broken_segment_does_not_drop_message(self) -> None:
        # location 的 latitude 传了非数字，单段解析失败不应影响其它段
        event = group_event(
            message=[
                {"type": "text", "data": {"text": "keep"}},
                {"type": "location", "data": {"latitude": "north"}},
            ],
            alt_message="",
        )
        abm, _ = convert_incoming_message(event)

        assert abm.message_str == "keep"

    def test_message_can_be_a_single_segment(self) -> None:
        # 规范允许消息数据类型是单个消息段
        event = group_event(
            message={"type": "text", "data": {"text": "single"}},
            alt_message="",
        )
        abm, _ = convert_incoming_message(event)

        assert abm.message_str == "single"

    def test_missing_time_falls_back_to_now(self) -> None:
        event = group_event()
        event.pop("time")
        abm, _ = convert_incoming_message(event)

        assert abm.timestamp > 1_600_000_000

    def test_alt_message_wins_over_chain(self) -> None:
        abm, _ = convert_incoming_message(group_event(alt_message="替代表示"))

        assert abm.message_str == "替代表示"


class TestBuildMessageStr:
    """纯文本表示的拼装。"""

    def test_from_chain(self) -> None:
        chain = [Plain(text="hi"), At(qq=42, name="小明"), AtAll()]
        text = build_message_str(chain, None)

        assert "hi" in text
        assert "小明" in text
        assert "全体成员" in text

    def test_at_all_is_not_rendered_as_plain_at(self) -> None:
        # AtAll 是 At 的子类，顺序判断错误会渲染成 "@all"
        text = build_message_str([AtAll()], None)

        assert "全体成员" in text
        assert "@all" not in text

    def test_placeholder_for_media(self) -> None:
        chain = [Plain(text="看图"), Image(file="onebot12://x")]
        text = build_message_str(chain, None)

        assert text == "看图[图片]"

    def test_alt_message_takes_priority(self) -> None:
        assert build_message_str([Plain(text="chain")], "alt") == "alt"


class TestOutgoing:
    """AstrBot 消息链 -> OneBot 12 消息段数组。"""

    async def test_text_and_mention(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise AssertionError("文本消息不应触发上传")

        segments = await convert_outgoing_message(
            [Plain(text="你好"), At(qq=42, name="小明")],
            upload=upload,
        )

        assert segments == [
            {"type": "text", "data": {"text": "你好"}},
            {"type": "mention", "data": {"user_id": "42"}},
            {"type": "text", "data": {"text": " "}},
        ]

    async def test_mention_all(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise AssertionError("不应触发上传")

        segments = await convert_outgoing_message([AtAll()], upload=upload)

        assert segments == [{"type": "mention_all", "data": {}}]

    async def test_reply_carries_message_and_user_id(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise AssertionError("不应触发上传")

        segments = await convert_outgoing_message(
            [Reply(id="6283", sender_id=999)],
            upload=upload,
        )

        assert segments == [
            {"type": "reply", "data": {"message_id": "6283", "user_id": "999"}},
        ]

    async def test_location(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise AssertionError("不应触发上传")

        segments = await convert_outgoing_message(
            [Location(lat=1.5, lon=2.5, title="t", content="c")],
            upload=upload,
        )

        assert segments == [
            {
                "type": "location",
                "data": {
                    "latitude": 1.5,
                    "longitude": 2.5,
                    "title": "t",
                    "content": "c",
                },
            },
        ]

    async def test_image_url_is_passed_through(self) -> None:
        calls: list[tuple[str, str]] = []

        async def upload(upload_type: str, payload: str, name: str = "") -> str:
            calls.append((upload_type, payload))
            return "file-1"

        segments = await convert_outgoing_message(
            [Image(file="https://example.com/a.png")],
            upload=upload,
        )

        assert calls == [("url", "https://example.com/a.png")]
        assert segments == [{"type": "image", "data": {"file_id": "file-1"}}]

    async def test_image_base64_payload(self) -> None:
        calls: list[tuple[str, str]] = []

        async def upload(upload_type: str, payload: str, name: str = "") -> str:
            calls.append((upload_type, payload))
            return "file-2"

        await convert_outgoing_message(
            [Image(file="base64://QUJD")],
            upload=upload,
        )

        assert calls == [("data", "QUJD")]

    async def test_local_image_uses_path_upload(self) -> None:
        calls: list[tuple[str, str]] = []

        async def upload(upload_type: str, payload: str, name: str = "") -> str:
            calls.append((upload_type, payload))
            return "file-3"

        await convert_outgoing_message(
            [Image(file="file:///tmp/a.png", path="/tmp/a.png")],
            upload=upload,
        )

        assert calls and calls[0][0] == "path"
        assert calls[0][1].replace("\\", "/").endswith("/tmp/a.png")

    async def test_upload_failure_skips_component(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise OneBot12ActionError("upload_file", 10002, "Unsupported Action")

        segments = await convert_outgoing_message(
            [Plain(text="文字保留"), Image(file="https://example.com/a.png")],
            upload=upload,
        )

        assert segments == [{"type": "text", "data": {"text": "文字保留"}}]

    async def test_unsupported_component_is_skipped(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise AssertionError("不应触发上传")

        from astrbot.api.message_components import Poke

        segments = await convert_outgoing_message(
            [Plain(text="a"), Poke(id="1", poke_type="126")],
            upload=upload,
        )

        assert segments == [{"type": "text", "data": {"text": "a"}}]

    async def test_empty_plain_is_dropped(self) -> None:
        async def upload(upload_type: str, payload: str, name: str = "") -> str:  # noqa: ARG001
            raise AssertionError("不应触发上传")

        assert await convert_outgoing_message([Plain(text="")], upload=upload) == []


class TestDumpCompact:
    """日志用的单行 JSON。"""

    def test_non_ascii_is_preserved(self) -> None:
        assert dump_compact({"text": "中文"}) == '{"text":"中文"}'

    def test_unserializable_falls_back_to_repr(self) -> None:
        assert dump_compact(object()).startswith("<object object at")


class TestActionError:
    """动作异常的可读信息。"""

    def test_message_and_fields(self) -> None:
        error = OneBot12ActionError("send_message", 10003, "Bad Param")

        assert error.action == "send_message"
        assert error.retcode == 10003
        assert "10003" in str(error)
        assert "Bad Param" in str(error)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
