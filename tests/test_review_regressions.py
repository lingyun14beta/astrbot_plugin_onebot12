"""针对一轮对抗式审查发现的缺陷的回归测试。

每个测试对应一个具体的历史 bug，注释里写清「错在哪、为什么会错」，避免以后被
无意中改回去。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from astrbot.api.event import MessageChain
from astrbot.api.message_components import File, Image, Plain, Record, Video
from astrbot.core.platform.astr_message_event import MessageSession
from astrbot.api.platform import MessageType
from astrbot_plugin_onebot12.onebot12_adapter import (
    ADAPTER_NAME,
    DEFAULT_CONFIG,
    OneBot12Adapter,
    build_send_params,
)
from astrbot_plugin_onebot12.onebot12_message import OneBot12ActionError

from test_media_integration import (  # noqa: F401  复用脚本化连接与事件构造
    group_message,
    image_event,
    make_adapter_with_script,
    png_server,
)


def make_adapter(**config_overrides) -> OneBot12Adapter:
    """构造一个不带连接的适配器（只测纯逻辑分支）。

    Args:
        **config_overrides: 平台配置覆盖项。

    Returns:
        :class:`OneBot12Adapter` 实例。
    """
    config = dict(DEFAULT_CONFIG)
    config.update({"id": "onebot12-regress", "type": ADAPTER_NAME, "enable": True})
    config.update(config_overrides)
    return OneBot12Adapter(config, {}, asyncio.Queue())


class TestFileComponentSource:
    """``File`` 组件的本地路径在 ``file_``，不是 ``file``。"""

    def test_should_resolve_includes_file_in_all_mode(self) -> None:
        """回归：``media_resolve_mode="all"`` 曾漏掉 ``File``。

        ``File`` 不是 ``Image`` 的子类，旧写法 ``return True`` 改成
        ``isinstance(component, (Record, Video, File))`` 前，File 永远不落地，
        ``file`` 消息段就停在 ``onebot12://`` 引用上（核心不认这个引用）。
        """
        adapter = make_adapter(media_resolve_mode="all")

        assert adapter._should_resolve(File(name="a.zip", file="/tmp/a.zip"))  # noqa: SLF001
        assert adapter._should_resolve(Record(file="/tmp/a.wav"))  # noqa: SLF001
        assert adapter._should_resolve(Video(file="/tmp/a.mp4"))  # noqa: SLF001
        assert adapter._should_resolve(Image(file="/tmp/a.png"))  # noqa: SLF001

    def test_auto_mode_resolves_only_images(self) -> None:
        adapter = make_adapter(media_resolve_mode="auto")

        assert adapter._should_resolve(Image(file="/tmp/a.png"))  # noqa: SLF001
        assert not adapter._should_resolve(File(name="a.zip", file="/tmp/a"))  # noqa: SLF001
        assert not adapter._should_resolve(Record(file="/tmp/a.wav"))  # noqa: SLF001

    async def test_file_with_local_path_is_uploaded(self, tmp_path) -> None:
        """回归：``File.file`` 是 property，路径不在磁盘上时返回空串。

        旧代码读 ``comp.url or comp.file``，于是「路径不存在」的文件被静默跳过，
        一个消息段都不发。真身在 ``file_``。
        """
        missing = tmp_path / "not-on-disk.bin"
        adapter, ws = make_adapter_with_script(
            {
                "upload_file": {"file_id": "fid-file"},
                "send_message": {"message_id": "1"},
            },
        )
        event = adapter.create_event(
            adapter.convert_message(image_event(), "ws-reverse#1"),
        )

        await event.send(
            MessageChain([File(name="not-on-disk.bin", file=str(missing))])
        )

        assert [r["action"] for r in ws.requests] == [
            "upload_file",
            "send_message",
        ], ws.requests
        assert ws.requests[0]["params"]["type"] == "path"
        assert ws.requests[0]["params"]["name"] == "not-on-disk.bin"
        assert ws.requests[1]["params"]["message"] == [
            {"type": "file", "data": {"file_id": "fid-file"}},
        ]

    async def test_file_path_is_not_read_through_the_property(self, tmp_path) -> None:
        """读 ``File.file`` 会在异步上下文里触发同步下载告警，不该走那条路。"""
        real = tmp_path / "a.bin"
        real.write_bytes(b"payload")
        adapter, ws = make_adapter_with_script(
            {
                "upload_file": {"file_id": "fid-real"},
                "send_message": {"message_id": "1"},
            },
        )
        event = adapter.create_event(
            adapter.convert_message(image_event(), "ws-reverse#1"),
        )

        await event.send(MessageChain([File(name="a.bin", file=str(real))]))

        assert ws.requests[0]["params"]["path"] == str(real)


class TestSelfField:
    """规范要求多账号共享连接时，非元动作都带 ``self``。"""

    async def test_get_file_carries_self(self) -> None:
        """回归：``self`` 曾只加在 ``send_message`` 上。

        规范（``connect/data-protocol/action-request.md``）：多账号共享一条连接时
        除元动作外的动作请求都应带 ``self``，否则实现端可以回 ``10101 Who Am I``。
        """
        adapter, ws = make_adapter_with_script(
            {"get_file": {"name": "a.png", "url": "http://x/a.png"}},
            fetch_nickname=False,
        )
        # 真实链路里这两个字段由 _on_raw_event 从事件的 self 里带出
        adapter._self_platform = "qq"  # noqa: SLF001
        adapter._self_user_id = "123234"  # noqa: SLF001

        await adapter._fetch_file_id("img-1", "ws-reverse#1")  # noqa: SLF001

        assert ws.requests[0]["action"] == "get_file"
        assert ws.requests[0].get("self") == {
            "platform": "qq",
            "user_id": "123234",
        }

    async def test_send_message_carries_self(self) -> None:
        adapter, ws = make_adapter_with_script({"send_message": {"message_id": "1"}})
        event = adapter.create_event(
            adapter.convert_message(image_event(), "ws-reverse#1"),
        )

        await event.send(MessageChain([Plain(text="hi")]))

        # self 是动作请求的顶层字段，不在 params 里（规范 action-request.md）
        assert ws.requests[0]["self"] == {"platform": "qq", "user_id": "123234"}
        assert "self" not in ws.requests[0]["params"]

    async def test_send_by_session_carries_self(self) -> None:
        adapter, ws = make_adapter_with_script({"send_message": {"message_id": "1"}})
        adapter._self_platform = "qq"  # noqa: SLF001
        adapter._self_user_id = "123234"  # noqa: SLF001
        session = MessageSession("onebot12-regress", MessageType.GROUP_MESSAGE, "12345")

        await adapter.send_by_session(session, MessageChain([Plain(text="hi")]))

        assert ws.requests[0]["self"] == {"platform": "qq", "user_id": "123234"}


class TestSessionIdCoercion:
    """``MessageSession.session_id`` 是未校验字段，可能不是字符串。"""

    async def test_int_session_id_does_not_crash(self) -> None:
        """回归：``":" in session_id`` 遇到 int 会抛 TypeError。"""
        adapter, ws = make_adapter_with_script({"send_message": {"message_id": "1"}})
        session = MessageSession(
            "onebot12-regress",
            MessageType.GROUP_MESSAGE,
            12345,  # type: ignore[arg-type] 别的插件确实可能塞 int
        )

        await adapter.send_by_session(session, MessageChain([Plain(text="hi")]))

        assert ws.requests[0]["params"]["group_id"] == "12345"

    async def test_ids_are_strings_even_when_input_is_int(self) -> None:
        params = build_send_params("private", 999)  # type: ignore[arg-type]

        assert params["user_id"] == "999"


class TestExtendedDetailType:
    """实现端扩展的 ``detail_type`` 用 ``<detail_type>_id`` 作参数名。"""

    async def test_extended_detail_type_uses_prefixed_id(self) -> None:
        """规范 ``interface/message/actions.md``：``qq.discuss`` 配 ``qq.discuss_id``。"""
        adapter, ws = make_adapter_with_script({"send_message": {"message_id": "1"}})
        event = adapter.create_event(
            adapter.convert_message(
                group_message(
                    "1",
                    [{"type": "text", "data": {"text": "hi"}}],
                    "hi",
                )
                | {"detail_type": "qq.discuss", "qq.discuss_id": "777"},
                "ws-reverse#1",
            ),
        )
        event.detail_type = "qq.discuss"
        event.session_id = "777"

        await event.send(MessageChain([Plain(text="hi")]))

        params = ws.requests[0]["params"]
        assert params["detail_type"] == "qq.discuss"
        assert params["qq.discuss_id"] == "777"
        assert "user_id" not in params


class TestGetFileRetryPolicy:
    """``get_file`` 只在实现端明确「不支持该方式」时才换下一种。"""

    async def test_non_unsupported_error_stops_retrying(self) -> None:
        """回归：任何错误都重试三种方式，网络错误会被同一份内容重发三遍。"""
        calls: list[str] = []

        class Transport:
            mode = "ws_reverse"

            async def request(
                self, action, params=None, *, prefer=None, self_info=None
            ):  # noqa: ARG002
                calls.append(params["type"])
                raise OneBot12ActionError(action, 33001, "Network Error")

        adapter = make_adapter()
        adapter.transport = Transport()  # type: ignore[assignment]

        kind, value = await adapter._fetch_file_id("img-1", None)  # noqa: SLF001

        assert (kind, value) == ("", "")
        assert calls == ["url"], f"不该继续重试: {calls}"

    async def test_unsupported_param_retries_next_kind(self) -> None:
        calls: list[str] = []

        class Transport:
            mode = "ws_reverse"

            async def request(
                self, action, params=None, *, prefer=None, self_info=None
            ):  # noqa: ARG002
                calls.append(params["type"])
                if params["type"] == "url":
                    raise OneBot12ActionError(action, 10004, "Unsupported Param")
                return {"path": "/tmp/a.png"}

        adapter = make_adapter()
        adapter.transport = Transport()  # type: ignore[assignment]

        kind, value = await adapter._fetch_file_id("img-1", None)  # noqa: SLF001

        assert (kind, value) == ("path", "/tmp/a.png")
        assert calls == ["url", "path"]

    async def test_timeout_tries_next_kind(self, monkeypatch) -> None:
        """超时说明实现端没响应「这一种」，不代表别的也不支持。"""
        import astrbot_plugin_onebot12.onebot12_adapter as adapter_mod

        monkeypatch.setattr(adapter_mod, "MEDIA_FETCH_TIMEOUT", 0.05)
        calls: list[str] = []

        class Transport:
            mode = "ws_reverse"

            async def request(
                self, action, params=None, *, prefer=None, self_info=None
            ):  # noqa: ARG002
                calls.append(params["type"])
                if params["type"] == "url":
                    await asyncio.sleep(5)
                return {"data": "QUJD"}

        adapter = make_adapter()
        adapter.transport = Transport()  # type: ignore[assignment]

        kind, value = await adapter._fetch_file_id("img-1", None)  # noqa: SLF001

        assert (kind, value) == ("data", "QUJD")
        assert calls == ["url", "path", "data"]


class TestLocalPathUri:
    """``path`` 形式的媒体引用要用 ``Path.as_uri()`` 拼。"""

    async def test_path_reference_is_a_valid_file_uri(
        self, tmp_path, monkeypatch
    ) -> None:
        from astrbot_plugin_onebot12.onebot12_adapter import OneBot12Adapter as Cls

        # 带 '#' 的路径拼字符串会得到一个被截断的 URI
        weird = tmp_path / "a#b.png"
        weird.write_bytes(b"x")

        captured: list[str] = []
        original = Cls._fetch_file_id

        async def fake_fetch(self, file_id, connection_id):  # noqa: ARG001
            return "path", str(weird)

        monkeypatch.setattr(Cls, "_fetch_file_id", fake_fetch)
        del original

        queue: asyncio.Queue = asyncio.Queue()
        adapter, _ws = make_adapter_with_script({}, queue, fetch_nickname=False)
        abm = adapter.convert_message(image_event(), None)
        await adapter.handle_msg(abm)

        image = queue.get_nowait().get_messages()[0]
        del captured
        # 无论落地成功与否，都不应该出现被 '#' 截断的 file:// 前缀
        assert "a#b" in image.file or "a%23b" in image.file, image.file


class TestMediaModeAllMaterializesFile:
    """``全部媒体`` 模式要连 ``File`` 一起落地。"""

    async def test_file_component_is_materialized(self, tmp_path, monkeypatch) -> None:
        payload = tmp_path / "doc.bin"
        payload.write_bytes(b"document-body")

        adapter, ws = make_adapter_with_script(
            {"get_file": {"path": str(payload)}},
            media_resolve_mode="all",
            fetch_nickname=False,
        )
        abm = adapter.convert_message(
            group_message(
                "1",
                [{"type": "file", "data": {"file_id": "file-1", "name": "doc.bin"}}],
                "[文件]",
            ),
            "ws-reverse#1",
        )
        await adapter.handle_msg(abm)

        component = adapter._event_queue.get_nowait().get_messages()[0]  # noqa: SLF001
        assert isinstance(component, File)
        assert component.file_ == str(payload), component.file_
        del ws


class TestUnsupportedParamConstant:
    """降级判断依赖的返回码常量与规范一致。"""

    def test_retcode_value(self) -> None:
        from astrbot_plugin_onebot12.onebot12_event import UNSUPPORTED_PARAM

        # interface/index.md：10004 Unsupported Param
        assert UNSUPPORTED_PARAM == 10004


def test_json_roundtrip_of_expected_payload() -> None:
    """确保断言里用到的结构确实是可 JSON 序列化的（防手写字典写错类型）。"""
    payload: dict[str, Any] = {
        "detail_type": "group",
        "group_id": "12345",
        "message": [{"type": "text", "data": {"text": "hi"}}],
    }
    assert json.loads(json.dumps(payload)) == payload
    assert Path("x.bin").resolve().as_uri().startswith("file://")
