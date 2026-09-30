"""媒体落地与发送的集成测试：用真实的 ``OneBot12Transport`` + 脚本化连接。

这里不碰真实网络：连接对象是 ``OneBot12WebSocketConnection``，但底层 "WebSocket"
被换成一个会把动作请求交给脚本处理的替身，因此覆盖的是
「adapter/event -> transport -> connection -> 报文」这条完整链路。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest_asyncio
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Image, Plain
from astrbot_plugin_onebot12.onebot12_adapter import (
    ADAPTER_NAME,
    DEFAULT_CONFIG,
    OneBot12Adapter,
)
from astrbot_plugin_onebot12.onebot12_client import (
    OneBot12Transport,
    OneBot12WebSocketConnection,
)
from astrbot_plugin_onebot12.onebot12_message import OneBot12ActionError
from conftest import find_free_port

PNG_BYTES = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
    "890000000d4944415478da63f8cfc0f01f00050001ffabce3689000000004945"
    "4e44ae426082",
)


class ScriptedWebSocket:
    """把动作请求交给脚本处理的最小 WebSocket 替身。"""

    def __init__(self, script: dict[str, Any]) -> None:
        self.script = script
        self.requests: list[dict] = []
        self.closed = False
        self.close_code: int | None = None

    async def send_str(self, raw: str) -> None:
        """处理一条动作请求并立刻通过连接对象回响应。

        Args:
            raw: 动作请求 JSON 字符串。
        """
        request = json.loads(raw)
        self.requests.append(request)
        handler = self.script.get(request["action"])
        if handler is None:
            connection = self.connection
            connection.resolve_response(
                {
                    "status": "failed",
                    "retcode": 10002,
                    "data": None,
                    "message": "Unsupported Action",
                    "echo": request["echo"],
                },
            )
            return
        data = handler(request["params"]) if callable(handler) else handler
        if isinstance(data, Exception):
            connection = self.connection
            connection.resolve_response(
                {
                    "status": "failed",
                    "retcode": getattr(data, "retcode", 10002),
                    "data": None,
                    "message": str(data),
                    "echo": request["echo"],
                },
            )
            return
        self.connection.resolve_response(
            {
                "status": "ok",
                "retcode": 0,
                "data": data,
                "message": "",
                "echo": request["echo"],
            },
        )

    async def close(self, code: int = 1000, message: bytes = b"") -> None:  # noqa: ARG002
        """标记连接关闭。

        Args:
            code: 关闭码。
            message: 关闭原因。
        """
        self.closed = True
        self.close_code = code


def make_adapter_with_script(
    script: dict[str, Any],
    queue: asyncio.Queue | None = None,
    **config_overrides,
) -> tuple[OneBot12Adapter, ScriptedWebSocket]:
    """构造一个使用脚本化连接的真实适配器。

    Args:
        script: 动作名到响应数据的映射，值可以是可调用对象或 Exception。
        queue: 事件队列。
        **config_overrides: 平台配置覆盖项。

    Returns:
        ``(adapter, ws)``。
    """
    config = dict(DEFAULT_CONFIG)
    config.update({"id": "onebot12-inttest", "type": ADAPTER_NAME, "enable": True})
    config.update(config_overrides)
    adapter = OneBot12Adapter(
        config,
        {},
        queue if queue is not None else asyncio.Queue(),
    )
    transport = OneBot12Transport("ws_reverse", api_timeout=5.0)
    ws = ScriptedWebSocket(script)
    connection = OneBot12WebSocketConnection(
        "ws-reverse#1",
        ws,
        api_timeout=5.0,
    )
    ws.connection = connection  # type: ignore[attr-defined]
    transport._register(connection)  # noqa: SLF001
    adapter.transport = transport
    return adapter, ws


def image_event(file_id: str = "img-1", **overrides) -> dict:
    """构造一条只带图片的 OneBot 12 群消息事件。

    Args:
        file_id: OneBot 12 文件 ID。
        **overrides: 需要覆盖的字段。

    Returns:
        事件字典。
    """
    event = {
        "id": "evt-img",
        "self": {"platform": "qq", "user_id": "123234"},
        "time": 1632847929.0,
        "type": "message",
        "detail_type": "group",
        "sub_type": "",
        "message_id": "6284",
        "message": [{"type": "image", "data": {"file_id": file_id}}],
        "alt_message": "[图片]",
        "group_id": "12345",
        "user_id": "123456788",
    }
    event.update(overrides)
    return event


def group_message(
    message_id: str,
    message: list[dict[str, Any]],
    alt: str,
) -> dict[str, Any]:
    """构造一条群消息事件（消息段由调用方给定）。

    Args:
        message_id: 消息 ID。
        message: 消息段数组。
        alt: 纯文本替代表示。

    Returns:
        事件字典。
    """
    return {
        "id": f"evt-{message_id}",
        "self": {"platform": "qq", "user_id": "123234"},
        "time": 1632847929.0,
        "type": "message",
        "detail_type": "group",
        "sub_type": "",
        "message_id": message_id,
        "message": message,
        "alt_message": alt,
        "group_id": "12345",
        "user_id": "123456788",
    }


@pytest_asyncio.fixture
async def png_server() -> AsyncIterator[str]:
    """启动一个只提供 ``/a.png`` 的本地 HTTP 服务。

    Yields:
        形如 ``http://127.0.0.1:PORT/a.png`` 的地址。
    """
    from aiohttp import web

    async def handler(request: web.Request) -> web.Response:  # noqa: ARG001
        return web.Response(body=PNG_BYTES, content_type="image/png")

    app = web.Application()
    app.router.add_get("/a.png", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = find_free_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    try:
        yield f"http://127.0.0.1:{port}/a.png"
    finally:
        await runner.cleanup()


class TestMediaResolution:
    """``file_id`` 形式的媒体落地。"""

    async def test_image_is_materialized_and_tracked(self, png_server) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter, ws = make_adapter_with_script(
            {"get_file": {"name": "a.png", "url": png_server}},
            queue,
            fetch_nickname=False,
        )

        await adapter.handle_msg(adapter.convert_message(image_event(), "ws-reverse#1"))

        assert queue.qsize() == 1
        event = queue.get_nowait()
        image = event.get_messages()[0]
        assert isinstance(image, Image)

        from pathlib import Path

        assert Path(image.file).is_file(), f"图片未落地: {image.file}"
        assert Path(image.file).read_bytes() == PNG_BYTES
        assert image.file in event._temporary_local_files  # noqa: SLF001
        assert image.url == ""

        assert [r["action"] for r in ws.requests] == ["get_file"]
        assert ws.requests[0]["params"] == {"file_id": "img-1", "type": "url"}

    async def test_url_type_unsupported_falls_back_to_path(self) -> None:
        def get_file(params: dict) -> Any:
            if params["type"] == "url":
                return OneBot12ActionError("get_file", 10004, "Unsupported Param")
            return {"name": "a.png", "path": _png_path()}

        queue: asyncio.Queue = asyncio.Queue()
        adapter, ws = make_adapter_with_script(
            {"get_file": get_file},
            queue,
            fetch_nickname=False,
        )

        await adapter.handle_msg(adapter.convert_message(image_event(), "ws-reverse#1"))

        assert [r["params"]["type"] for r in ws.requests] == ["url", "path"]
        image = queue.get_nowait().get_messages()[0]
        assert Path(image.file).is_file()
        assert Path(image.file).read_bytes() == PNG_BYTES

    async def test_all_types_unsupported_keeps_reference(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter, ws = make_adapter_with_script(
            {
                "get_file": OneBot12ActionError(
                    "get_file",
                    10004,
                    "Unsupported Param",
                ),
            },
            queue,
            fetch_nickname=False,
        )

        await adapter.handle_msg(adapter.convert_message(image_event(), "ws-reverse#1"))

        assert [r["params"]["type"] for r in ws.requests] == ["url", "path", "data"]
        image = queue.get_nowait().get_messages()[0]
        assert image.file == "onebot12://img-1"

    async def test_media_disabled_skips_fetch(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        adapter, ws = make_adapter_with_script(
            {"get_file": {"url": "http://example.invalid/a.png"}},
            queue,
            media_resolve_mode="none",
            fetch_nickname=False,
        )

        await adapter.handle_msg(adapter.convert_message(image_event(), "ws-reverse#1"))

        assert ws.requests == []
        assert queue.qsize() == 1


class TestOutgoingThroughRealTransport:
    """发送链路：event -> transport -> 报文。"""

    async def test_send_message_payload(self) -> None:
        adapter, ws = make_adapter_with_script(
            {"send_message": {"message_id": "1", "time": 1.0}},
        )
        event = adapter.create_event(
            adapter.convert_message(image_event(), "ws-reverse#1"),
        )

        await event.send(MessageChain([Plain(text="回复")]))

        assert len(ws.requests) == 1
        request = ws.requests[0]
        assert request["action"] == "send_message"
        assert request["params"]["detail_type"] == "group"
        assert request["params"]["group_id"] == "12345"
        assert request["params"]["message"] == [
            {"type": "text", "data": {"text": "回复"}},
        ]

    async def test_upload_then_send(self, png_server) -> None:
        adapter, ws = make_adapter_with_script(
            {
                "upload_file": {"file_id": "fid-9"},
                "send_message": {"message_id": "1", "time": 1.0},
            },
        )
        event = adapter.create_event(
            adapter.convert_message(image_event(), "ws-reverse#1"),
        )

        await event.send(MessageChain([Image(file=png_server)]))

        assert [r["action"] for r in ws.requests] == ["upload_file", "send_message"]
        assert ws.requests[0]["params"]["type"] == "url"
        assert ws.requests[1]["params"]["message"] == [
            {"type": "image", "data": {"file_id": "fid-9"}},
        ]


def _png_path() -> str:
    """把内置 PNG 写到临时文件并返回路径。

    Returns:
        临时 PNG 文件路径。
    """
    import tempfile

    handle = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    handle.write(PNG_BYTES)
    handle.close()
    return handle.name
