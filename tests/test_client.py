"""``onebot12_client`` 的单元测试与端到端连接测试。

端到端部分跑的是真实的 aiohttp 服务端 + 真实 WebSocket 客户端，覆盖
「握手鉴权 -> 事件推送 -> 动作请求/响应」这条完整链路。
"""

from __future__ import annotations

import asyncio
import json
import socket
from typing import Any

import aiohttp
import pytest
from astrbot_plugin_onebot12.onebot12_client import (
    OneBot12HttpConnection,
    OneBot12Transport,
    parse_endpoint,
)
from astrbot_plugin_onebot12.onebot12_message import OneBot12ActionError

from conftest import find_free_port


class TestParseEndpoint:
    """地址解析。"""

    def test_bare_host_port(self) -> None:
        endpoint = parse_endpoint("127.0.0.1:6700", default_scheme="ws")

        assert endpoint.scheme == "ws"
        assert endpoint.host == "127.0.0.1"
        assert endpoint.port == 6700
        assert endpoint.path == "/"
        assert endpoint.url() == "ws://127.0.0.1:6700/"

    def test_full_url_keeps_scheme_and_path(self) -> None:
        endpoint = parse_endpoint(
            "wss://bot.example.com:8443/onebot/v12",
            default_scheme="ws",
        )

        assert endpoint.scheme == "wss"
        assert endpoint.url() == "wss://bot.example.com:8443/onebot/v12"

    def test_path_is_added_when_missing(self) -> None:
        endpoint = parse_endpoint("127.0.0.1:6700", default_scheme="http")

        assert endpoint.path == "/"
        assert endpoint.url() == "http://127.0.0.1:6700/"

    def test_default_port_is_omitted_from_url(self) -> None:
        endpoint = parse_endpoint("http://example.com:80/x", default_scheme="http")

        assert endpoint.authority() == "example.com"
        assert endpoint.url() == "http://example.com/x"

    def test_bare_host_uses_default_scheme(self) -> None:
        endpoint = parse_endpoint("localhost", default_scheme="http")

        assert endpoint.scheme == "http"
        assert endpoint.port == 0
        assert endpoint.url() == "http://localhost/"

    def test_path_without_leading_slash(self) -> None:
        endpoint = parse_endpoint("http://h:1/x", default_scheme="http")

        assert endpoint.path == "/x"

    def test_ipv6_host_is_bracketed_in_url(self) -> None:
        # ws://::1:6700 不是合法 URL，IPv6 字面量必须加方括号
        endpoint = parse_endpoint("[::1]:6700", default_scheme="ws")

        assert endpoint.host == "::1"
        assert endpoint.url() == "ws://[::1]:6700/"

    def test_ipv6_full_url_roundtrips(self) -> None:
        endpoint = parse_endpoint("ws://[2001:db8::1]:6700/x", default_scheme="ws")

        assert endpoint.url() == "ws://[2001:db8::1]:6700/x"

    @pytest.mark.parametrize("address", ["127.0.0.1:99999", "127.0.0.1:abc"])
    def test_bad_port_reports_the_address(self, address: str) -> None:
        with pytest.raises(ValueError, match="无法解析地址"):
            parse_endpoint(address, default_scheme="ws")

    @pytest.mark.parametrize("address", ["", "   ", "http://"])
    def test_invalid_address(self, address: str) -> None:
        with pytest.raises(ValueError, match=r".+"):
            parse_endpoint(address, default_scheme="ws")


class TestTokenValidation:
    """访问令牌校验（对齐规范里的三条判定顺序）。"""

    def test_no_token_configured_allows_all(self) -> None:
        from astrbot_plugin_onebot12.onebot12_client import is_token_valid

        assert is_token_valid("", None, None) is True

    def test_bearer_header(self) -> None:
        from astrbot_plugin_onebot12.onebot12_client import is_token_valid

        assert is_token_valid("s3cret", "Bearer s3cret", None) is True
        assert is_token_valid("s3cret", "Bearer wrong", None) is False

    def test_query_param_fallback(self) -> None:
        from astrbot_plugin_onebot12.onebot12_client import is_token_valid

        assert is_token_valid("s3cret", None, "s3cret") is True
        assert is_token_valid("s3cret", None, "wrong") is False

    def test_header_takes_priority_over_query(self) -> None:
        from astrbot_plugin_onebot12.onebot12_client import is_token_valid

        # 规范：存在 Authorization 头时只看它，不再回退到 query
        assert is_token_valid("s3cret", "Bearer wrong", "s3cret") is False

    def test_missing_both_fails(self) -> None:
        from astrbot_plugin_onebot12.onebot12_client import is_token_valid

        assert is_token_valid("s3cret", None, None) is False

    def test_whitespace_is_not_trimmed(self) -> None:
        from astrbot_plugin_onebot12.onebot12_client import is_token_valid

        # 规范明确要求不裁剪两侧空白
        assert is_token_valid(" s3cret ", "Bearer  s3cret ", None) is True
        assert is_token_valid("s3cret", "Bearer  s3cret ", None) is False


class FakeWebSocket:
    """最小的 WebSocket 替身，只实现连接层用到的属性。"""

    def __init__(self, *, accepted: bool = True) -> None:
        self.sent: list[dict] = []
        self.closed = False
        self.close_code: int | None = None
        self._accept = accepted

    async def send_str(self, raw: str) -> None:
        """记录发出的报文。

        Args:
            raw: 报文字符串。

        Raises:
            ConnectionResetError: 模拟连接已断开。
        """
        if not self._accept:
            raise ConnectionResetError("broken pipe")
        self.sent.append(json.loads(raw))

    async def close(self, code: int = 1000, message: bytes = b"") -> None:  # noqa: ARG002
        """标记为已关闭。

        Args:
            code: 关闭码。
            message: 关闭原因。
        """
        self.closed = True
        self.close_code = code


def make_connection(api_timeout: float = 1.0, *, accepted: bool = True):
    """构造一个基于 :class:`FakeWebSocket` 的连接对象。

    Args:
        api_timeout: 动作超时时间。
        accepted: FakeWebSocket 是否接受发送。

    Returns:
        ``(connection, ws)``。
    """
    from astrbot_plugin_onebot12.onebot12_client import OneBot12WebSocketConnection

    ws = FakeWebSocket(accepted=accepted)
    return OneBot12WebSocketConnection(
        "ws#1",
        ws,
        api_timeout=api_timeout,
    ), ws


class TestWebSocketConnection:
    """WebSocket 连接的请求/响应配对。"""

    async def test_request_response_roundtrip(self) -> None:
        connection, ws = make_connection()

        task = asyncio.create_task(connection.request("get_version", {}))
        await asyncio.sleep(0)
        assert ws.sent[0]["action"] == "get_version"

        connection.resolve_response(
            {
                "status": "ok",
                "retcode": 0,
                "data": {"onebot_version": "12"},
                "echo": ws.sent[0]["echo"],
            },
        )
        assert await task == {"onebot_version": "12"}

    async def test_failed_response_raises_with_retcode(self) -> None:
        connection, ws = make_connection()

        task = asyncio.create_task(connection.request("send_message", {}))
        await asyncio.sleep(0)
        connection.resolve_response(
            {
                "status": "failed",
                "retcode": 10005,
                "data": None,
                "message": "Unsupported Segment",
                "echo": ws.sent[0]["echo"],
            },
        )

        with pytest.raises(OneBot12ActionError) as excinfo:
            await task
        assert excinfo.value.retcode == 10005
        assert "Unsupported Segment" in excinfo.value.message

    async def test_timeout_raises(self) -> None:
        connection, _ = make_connection(api_timeout=0.05)

        with pytest.raises(OneBot12ActionError, match="超时"):
            await connection.request("get_status", {})

    async def test_send_failure_raises(self) -> None:
        connection, _ = make_connection(accepted=False)

        with pytest.raises(OneBot12ActionError, match="WebSocket 发送失败"):
            await connection.request("get_status", {})

    async def test_echo_mismatch_is_ignored(self) -> None:
        connection, _ = make_connection()

        assert connection.resolve_response({"status": "ok", "retcode": 0}) is False
        assert (
            connection.resolve_response(
                {"status": "ok", "retcode": 0, "echo": "not-mine"},
            )
            is False
        )

    async def test_close_fails_pending(self) -> None:
        connection, _ = make_connection(api_timeout=5)

        task = asyncio.create_task(connection.request("get_status", {}))
        await asyncio.sleep(0)
        await connection.close()

        with pytest.raises(OneBot12ActionError, match="已关闭"):
            await task
        assert connection.closed() is True


class TestHttpConnection:
    """HTTP 动作通道。"""

    async def test_missing_echo_is_accepted(self) -> None:
        # 部分实现会省略 echo，此时响应应归属当前唯一的等待请求。
        # 这里把发送环节换成空操作，只验证「响应 -> 归属」这一步。
        endpoint = parse_endpoint("http://127.0.0.1:1/", default_scheme="http")
        connection = OneBot12HttpConnection(
            "http-api",
            session=None,  # type: ignore[arg-type]
            endpoint=endpoint,
            api_timeout=1.0,
        )

        async def noop(payload: dict) -> None:  # noqa: ARG001
            return None

        connection._deliver = noop  # type: ignore[method-assign]  # noqa: SLF001

        task = asyncio.create_task(connection.request("get_status", {}))
        await asyncio.sleep(0)

        # 没有 echo 的响应匹配不上，必须退回 take_pending()
        response = {"status": "ok", "retcode": 0, "data": {"good": True}}
        assert connection.resolve_response(dict(response)) is False
        future = connection.take_pending()
        assert future is not None
        future.set_result(response)

        assert await task == {"good": True}

    async def test_deliver_without_session_raises(self) -> None:
        endpoint = parse_endpoint("http://127.0.0.1:1/", default_scheme="http")
        connection = OneBot12HttpConnection(
            "http-api",
            session=None,  # type: ignore[arg-type]
            endpoint=endpoint,
            api_timeout=1.0,
        )

        with pytest.raises(OneBot12ActionError, match="未初始化客户端会话"):
            await connection.request("get_status", {})

    async def test_describe(self) -> None:
        endpoint = parse_endpoint("http://127.0.0.1:6700/", default_scheme="http")
        connection = OneBot12HttpConnection(
            "http-api",
            session=None,  # type: ignore[arg-type]
            endpoint=endpoint,
        )

        assert connection.describe() == "http-api (http://127.0.0.1:6700/)"


class TestTransportRouting:
    """动作请求的连接选择。"""

    def make_transport(self) -> OneBot12Transport:
        """构造一个未建立任何连接的通信层。

        Returns:
            :class:`OneBot12Transport` 实例。
        """
        return OneBot12Transport("ws_reverse", api_timeout=1.0)

    async def test_no_connection_raises(self) -> None:
        transport = self.make_transport()

        with pytest.raises(OneBot12ActionError, match="没有可用的 OneBot 12 连接"):
            await transport.request("get_status", {})

    async def test_prefer_is_honoured(self) -> None:
        transport = self.make_transport()
        first, _ = make_connection()
        second, second_ws = make_connection()
        transport._register(first)  # noqa: SLF001 测试内部状态
        transport._register(second)  # noqa: SLF001

        task = asyncio.create_task(
            transport.request("get_status", {}, prefer=second.connection_id),
        )
        await asyncio.sleep(0)

        assert second_ws.sent and not first.ws.sent

        second.resolve_response(
            {
                "status": "ok",
                "retcode": 0,
                "data": None,
                "echo": second_ws.sent[0]["echo"],
            },
        )
        await task

    async def test_preferred_connection_is_not_silently_swapped(self) -> None:
        """回归：来源连接没了以前会顺手挑一条别的连接。

        多条连接通常就是多个机器人账号，拿别的连接发回复等于用另一个账号说话，
        取 ``get_file`` 更是拿 A 的 file_id 去问 B（file_id 按账号隔离）。
        """
        transport = self.make_transport()
        other, other_ws = make_connection()
        transport._register(other)  # noqa: SLF001

        with pytest.raises(OneBot12ActionError, match="已断开"):
            await transport.request("get_status", {}, prefer="ws#404")

        assert other_ws.sent == [], "不该把请求改发到别的连接上"

    async def test_action_request_from_implementation_gets_unsupported(self) -> None:
        transport = self.make_transport()
        connection, ws = make_connection()
        transport._register(connection)  # noqa: SLF001

        await transport._handle_action_request(  # noqa: SLF001
            connection,
            {"action": "get_status", "params": {}, "echo": "abc"},
        )

        assert ws.sent == [
            {
                "status": "failed",
                "retcode": 10002,
                "data": None,
                "message": "Unsupported Action",
                "echo": "abc",
            },
        ]


class TestWebSocketFrameRouting:
    """同一条连接上三种数据的区分（回归测试）。

    曾经把动作响应当成事件派发下去，导致动作请求永远等不到响应而超时。
    """

    async def test_response_frame_resolves_pending_request(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            events.append(event)

        transport.on_event(on_event)
        connection, ws = make_connection()
        transport._register(connection)  # noqa: SLF001

        task = asyncio.create_task(transport.request("get_version", {}))
        await asyncio.sleep(0)
        echo = ws.sent[0]["echo"]

        await transport._handle_ws_text(  # noqa: SLF001
            connection,
            json.dumps(
                {
                    "status": "ok",
                    "retcode": 0,
                    "data": {"onebot_version": "12"},
                    "message": "",
                    "echo": echo,
                },
            ),
        )
        await transport.drain()

        assert await task == {"onebot_version": "12"}
        assert events == [], "动作响应不应被当成事件派发"

    async def test_event_frame_is_dispatched(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[tuple[dict, str | None]] = []

        async def on_event(event: dict, connection_id: str | None) -> None:
            events.append((event, connection_id))

        transport.on_event(on_event)
        connection, _ = make_connection()
        transport._register(connection)  # noqa: SLF001

        await transport._handle_ws_text(  # noqa: SLF001
            connection,
            json.dumps(
                {
                    "id": "e1",
                    "type": "message",
                    "detail_type": "private",
                    "message_id": "1",
                    "message": [{"type": "text", "data": {"text": "hi"}}],
                    "user_id": "1",
                },
            ),
        )
        await transport.drain()

        assert len(events) == 1
        assert events[0][1] == "ws#1"

    async def test_dispatch_returns_before_handler_runs(self) -> None:
        """``dispatch`` 只入队，不能等回调跑完——否则读循环会被占住。"""
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        gate = asyncio.Event()
        started = asyncio.Event()

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            started.set()
            await gate.wait()

        transport.on_event(on_event)
        connection, _ = make_connection()
        transport._register(connection)  # noqa: SLF001

        await asyncio.wait_for(
            transport.dispatch({"type": "message", "message_id": "1"}, "ws#1"),
            timeout=1.0,
        )

        # 回调已经开始了，但 dispatch 早就返回了
        await asyncio.wait_for(started.wait(), timeout=1.0)
        gate.set()
        await transport.drain()

    async def test_unmatched_response_is_ignored(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            events.append(event)

        transport.on_event(on_event)
        connection, _ = make_connection()

        await transport._handle_ws_text(  # noqa: SLF001
            connection,
            json.dumps({"status": "ok", "retcode": 0, "data": {}, "echo": "stale"}),
        )

        assert events == []

    async def test_broken_json_is_ignored(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            events.append(event)

        transport.on_event(on_event)
        connection, _ = make_connection()

        await transport._handle_ws_text(connection, "{not json")  # noqa: SLF001

        assert events == []

    async def test_status_update_meta_event_is_not_swallowed(self) -> None:
        """回归：``meta.status_update`` 自带 ``status`` 字段。

        曾经的分类逻辑是「有 status 就当动作响应」，于是这条标准元事件被当成
        匹配不上的响应丢掉（规范要求实现端必须实现所有标准元事件）。
        """
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            events.append(event)

        transport.on_event(on_event)
        connection, _ = make_connection()

        await transport._handle_ws_text(  # noqa: SLF001
            connection,
            json.dumps(
                {
                    "id": "e1",
                    "time": 1.0,
                    "type": "meta",
                    "detail_type": "status_update",
                    "sub_type": "",
                    "status": {
                        "good": True,
                        "bots": [
                            {"self": {"platform": "qq", "user_id": "1"}},
                        ],
                    },
                },
            ),
        )
        await transport.drain()

        assert len(events) == 1, "status_update 被当成动作响应丢掉了"
        assert events[0]["detail_type"] == "status_update"

    async def test_echo_less_response_is_still_a_response(self) -> None:
        """没有 echo 的响应不能被误判成事件。"""
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            events.append(event)

        transport.on_event(on_event)
        connection, ws = make_connection()
        transport._register(connection)  # noqa: SLF001

        task = asyncio.create_task(transport.request("get_status", {}))
        await asyncio.sleep(0)
        echo = ws.sent[0]["echo"]

        # 有 retcode，没有 echo：仍是动作响应，不该派发成事件
        await transport._handle_ws_text(  # noqa: SLF001
            connection,
            json.dumps({"status": "ok", "retcode": 0, "data": {"good": True}}),
        )
        await transport.drain()

        assert events == []
        # 该响应既不匹配 echo，也应被当作无法匹配处理；请求最终超时
        with pytest.raises(OneBot12ActionError, match="超时"):
            await asyncio.wait_for(task, timeout=3)
        del echo

    async def test_meta_connect_is_dispatched(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        events: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            events.append(event)

        transport.on_event(on_event)
        connection, _ = make_connection()

        await transport._handle_ws_text(  # noqa: SLF001
            connection,
            json.dumps(
                {
                    "id": "e0",
                    "time": 1.0,
                    "type": "meta",
                    "detail_type": "connect",
                    "sub_type": "",
                    "version": {"impl": "x", "version": "1", "onebot_version": "12"},
                },
            ),
        )
        await transport.drain()

        assert len(events) == 1
        assert events[0]["detail_type"] == "connect"


class TestTransportShutdown:
    """关闭路径：``drain`` 不能空等，``aclose`` 不能在队列有积压时炸掉。"""

    async def test_drain_does_not_wait_for_the_full_timeout(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        seen: list[str] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            seen.append(event["message_id"])

        transport.on_event(on_event)
        connection, _ = make_connection()
        transport._register(connection)  # noqa: SLF001

        for msg_id in ("1", "2", "3"):
            await transport.dispatch({"message_id": msg_id}, "ws#1")

        started = asyncio.get_running_loop().time()
        await transport.drain()
        elapsed = asyncio.get_running_loop().time() - started

        assert seen == ["1", "2", "3"]
        # worker 漏掉 task_done() 时 join() 会一直等到 5s 上限
        assert elapsed < 1.0, f"drain 等了 {elapsed:.2f}s，task_done 记账可能不配对"

    async def test_aclose_with_backlog_does_not_raise_and_frees_port(self) -> None:
        """回归：``aclose`` 曾在非空队列上循环 ``task_done()``。

        ``task_done()`` 不出队，``while not queue.empty()`` 永远为真，减到 0 之后
        抛 ValueError，把后面的 runner.cleanup()/session.close() 一起带没，端口泄漏。
        """
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)
        release = asyncio.Event()

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            await release.wait()

        transport.on_event(on_event)
        port = find_free_port()
        server = asyncio.create_task(
            transport.run_reverse_websocket(
                host="127.0.0.1",
                port=port,
                path="/onebot/v12",
            ),
        )

        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if transport._runner is not None:  # noqa: SLF001
                    break

            # 塞进去的事件会一直卡在 worker 里，制造积压
            for idx in range(5):
                await transport.dispatch({"message_id": str(idx)}, "__shared__")
            await asyncio.sleep(0.05)

            await asyncio.wait_for(transport.aclose(), timeout=20)
        finally:
            release.set()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

        # runner 必须真的清理掉，端口才能被重新绑定
        assert transport._runner is None  # noqa: SLF001
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
        finally:
            probe.close()


class TestEventHandlingDoesNotBlockReadLoop:
    """回归测试：处理事件期间，读循环仍必须能派发动作响应。

    曾经的实现里 ``_handle_ws_text`` 直接 ``await`` 事件回调。而回调落地上游事件时
    会发起 ``get_file`` 去换媒体——那个响应同样要走读循环，于是读循环被自己占住，
    动作请求只能等超时。实测：实现端秒回，图片依然每次 3 秒超时后被丢弃。
    """

    async def test_action_issued_from_event_handler_gets_its_response(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=3.0)
        results: list[Any] = []

        async def on_event(event: dict, connection_id: str | None) -> None:
            # 模拟适配器落地上游事件时发起的动作请求
            results.append(await transport.request("get_file", {"file_id": "x"}))

        transport.on_event(on_event)
        port = find_free_port()
        server = asyncio.create_task(
            transport.run_reverse_websocket(
                host="127.0.0.1",
                port=port,
                path="/onebot/v12",
            ),
        )
        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if transport._runner is not None:  # noqa: SLF001
                    break

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    f"http://127.0.0.1:{port}/onebot/v12",
                ) as ws:
                    await ws.send_str(
                        json.dumps(
                            {
                                "id": "e1",
                                "self": {"platform": "qq", "user_id": "1"},
                                "time": 1.0,
                                "type": "message",
                                "detail_type": "private",
                                "sub_type": "",
                                "message_id": "1",
                                "message": [{"type": "text", "data": {"text": "hi"}}],
                                "alt_message": "hi",
                                "user_id": "2",
                            },
                        ),
                    )

                    # 扮演实现端：读到 get_file 立刻回响应
                    raw = await asyncio.wait_for(ws.receive(), timeout=5)
                    request = json.loads(raw.data)
                    assert request["action"] == "get_file"
                    await ws.send_str(
                        json.dumps(
                            {
                                "status": "ok",
                                "retcode": 0,
                                "data": {"name": "a.png", "url": "http://x/a.png"},
                                "message": "",
                                "echo": request["echo"],
                            },
                        ),
                    )

                    for _ in range(100):
                        if results:
                            break
                        await asyncio.sleep(0.02)

            # 3 秒动作超时内必须拿到响应；若读循环被占住这里会是 []
            assert results == [{"name": "a.png", "url": "http://x/a.png"}]
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_events_keep_arrival_order(self) -> None:
        transport = OneBot12Transport("ws_reverse", api_timeout=3.0)
        seen: list[str] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            # 故意让先到的事件处理更慢，验证 worker 是串行的
            if event["message_id"] == "1":
                await asyncio.sleep(0.2)
            seen.append(event["message_id"])

        transport.on_event(on_event)
        port = find_free_port()
        server = asyncio.create_task(
            transport.run_reverse_websocket(
                host="127.0.0.1",
                port=port,
                path="/onebot/v12",
            ),
        )
        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if transport._runner is not None:  # noqa: SLF001
                    break

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    f"http://127.0.0.1:{port}/onebot/v12",
                ) as ws:
                    for msg_id in ("1", "2", "3"):
                        await ws.send_str(
                            json.dumps(
                                {
                                    "id": f"e{msg_id}",
                                    "self": {"platform": "qq", "user_id": "1"},
                                    "time": 1.0,
                                    "type": "message",
                                    "detail_type": "private",
                                    "sub_type": "",
                                    "message_id": msg_id,
                                    "message": [
                                        {"type": "text", "data": {"text": msg_id}},
                                    ],
                                    "alt_message": msg_id,
                                    "user_id": "2",
                                },
                            ),
                        )
                    for _ in range(150):
                        if len(seen) == 3:
                            break
                        await asyncio.sleep(0.02)

            assert seen == ["1", "2", "3"]
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)


class TestEventWorkerLifecycle:
    """每条连接的事件 worker 必须随连接注销而退休。"""

    async def test_worker_and_queue_are_retired_on_disconnect(self) -> None:
        """回归：worker/queue 只在 ``aclose`` 里清理，重连会无限堆积。

        连接 ID 每次都是新的（``ws-reverse#N``），旧 worker 会永远停在
        ``await queue.get()`` 上；正向模式反复重连就是稳定的任务泄漏。
        """
        transport = OneBot12Transport("ws_reverse", api_timeout=1.0)

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            return None

        transport.on_event(on_event)
        port = find_free_port()
        server = asyncio.create_task(
            transport.run_reverse_websocket(
                host="127.0.0.1",
                port=port,
                path="/onebot/v12",
            ),
        )
        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if transport._runner is not None:  # noqa: SLF001
                    break

            for _round in range(3):
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        f"http://127.0.0.1:{port}/onebot/v12",
                    ) as ws:
                        await ws.send_str(
                            json.dumps(
                                {
                                    "id": "e",
                                    "time": 1.0,
                                    "type": "meta",
                                    "detail_type": "heartbeat",
                                    "sub_type": "",
                                    "interval": 5000,
                                },
                            ),
                        )
                        for _ in range(100):
                            await asyncio.sleep(0.02)
                            if transport._event_queues:  # noqa: SLF001
                                break
                        assert len(transport._event_queues) == 1  # noqa: SLF001

                # 断开后 worker 应收到退休哨兵并自行清理
                for _ in range(150):
                    await asyncio.sleep(0.02)
                    if not transport._event_queues:  # noqa: SLF001
                        break
                assert not transport._event_queues, "事件队列没有被回收"  # noqa: SLF001
                assert not transport._event_workers, "worker 任务没有被回收"  # noqa: SLF001

            assert not transport.active_connections()
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_reverse_socket_has_heartbeat(self) -> None:
        """反向侧也要 ping：否则半开连接会一直留在 active_connections 里。"""
        import inspect

        from astrbot_plugin_onebot12.onebot12_client import OneBot12Transport as Cls

        source = inspect.getsource(Cls._handle_reverse_ws)  # noqa: SLF001
        assert "heartbeat=30.0" in source, "反向 WebSocket 没有开心跳"
        assert "heartbeat=None" not in source


class TestTakePendingUniqueness:
    """``take_pending`` 只在唯一等待者时可用。"""

    def test_none_when_nothing_pending(self) -> None:
        connection, _ = make_connection()
        assert connection.take_pending() is None

    async def test_none_when_multiple_pending(self) -> None:
        """回归：随便挑一个会把响应交给错误的请求。

        ``http_poll`` 有一条常驻的长轮询，漏回 echo 的实现端下会让它把别人的
        响应全部吃掉。
        """
        connection, _ = make_connection(api_timeout=5)
        tasks = [
            asyncio.create_task(connection.request("get_status", {})),
            asyncio.create_task(connection.request("get_file", {})),
        ]
        await asyncio.sleep(0)

        assert connection.take_pending() is None, "并发请求下不该猜"

        connection.fail_pending("test cleanup")
        await asyncio.gather(*tasks, return_exceptions=True)

    async def test_returns_the_only_pending(self) -> None:
        connection, _ = make_connection(api_timeout=5)
        task = asyncio.create_task(connection.request("get_status", {}))
        await asyncio.sleep(0)

        future = connection.take_pending()
        assert future is not None
        future.set_result({"status": "ok", "retcode": 0, "data": {"good": True}})
        assert await task == {"good": True}


class TestHttpConnectionNoEcho:
    """HTTP 动作通道遇到「不回显 echo」的实现端。

    规范（``connect/data-protocol/action-response.md``）要求请求带了非空 ``echo``
    时响应必须原样回显。因此「不回显」属于实现端不守规范，适配器**不做位置猜测**：
    错位认领会把 A 的数据交给 B，而且此后每一个响应都跟着错。
    """

    async def test_missing_echo_is_reported_as_protocol_problem(self) -> None:
        """缺 echo 的响应必须明确失败，而不是被随便认领。"""

        async def handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
            return aiohttp.web.json_response(
                {"status": "ok", "retcode": 0, "data": {"ok": 1}},
            )

        port = await _start_json_server(handler)
        try:
            endpoint = parse_endpoint(
                f"http://127.0.0.1:{port}/", default_scheme="http"
            )
            async with aiohttp.ClientSession() as session:
                connection = OneBot12HttpConnection(
                    "http-api",
                    session,
                    endpoint,
                    api_timeout=5.0,
                )

                with pytest.raises(OneBot12ActionError, match="没有回显 echo"):
                    await connection.request("get_status", {})
        finally:
            await _stop_json_server(port)

    async def test_echoing_response_is_matched_correctly(self) -> None:
        """守规范的实现端（回显 echo）在并发下也能正确配对。"""
        delay_by_action = {"get_file": 0.08}

        async def handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
            payload = await request.json()
            await asyncio.sleep(delay_by_action.get(payload["action"], 0.0))
            return aiohttp.web.json_response(
                {
                    "status": "ok",
                    "retcode": 0,
                    "data": {"served": payload["action"]},
                    "message": "",
                    "echo": payload["echo"],
                },
            )

        port = await _start_json_server(handler)
        try:
            endpoint = parse_endpoint(
                f"http://127.0.0.1:{port}/", default_scheme="http"
            )
            async with aiohttp.ClientSession() as session:
                connection = OneBot12HttpConnection(
                    "http-api",
                    session,
                    endpoint,
                    api_timeout=5.0,
                )
                # get_file 慢、send_message 快：有 echo 就不会互换
                slow, fast = await asyncio.gather(
                    connection.request("get_file", {}),
                    connection.request("send_message", {}),
                )

            assert slow == {"served": "get_file"}
            assert fast == {"served": "send_message"}
        finally:
            await _stop_json_server(port)

    async def test_fast_response_is_not_swapped_onto_the_pending_request(self) -> None:
        """回归：快响应曾被交给另一个动作的等待者（实测互换）。

        时间线：A(get_file) 在飞且慢 → B(send_message) 发出并立刻回来 → A 才回来。
        不回显 echo 时两者都不该被认领（旧实现会把 B 的数据交给 A）。
        """
        handlers: list[asyncio.Future[dict[str, Any]]] = []
        seen: list[str] = []

        async def handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
            payload = await request.json()
            seen.append(payload["action"])
            if payload["action"] == "get_file":
                future: asyncio.Future[dict[str, Any]] = (
                    asyncio.get_running_loop().create_future()
                )
                handlers.append(future)
                data = await future
            else:
                data = {"served": "send_message"}
            # 故意不回 echo
            return aiohttp.web.json_response(
                {"status": "ok", "retcode": 0, "data": data},
            )

        port = await _start_json_server(handler)
        try:
            endpoint = parse_endpoint(
                f"http://127.0.0.1:{port}/", default_scheme="http"
            )
            async with aiohttp.ClientSession() as session:
                connection = OneBot12HttpConnection(
                    "http-api",
                    session,
                    endpoint,
                    api_timeout=5.0,
                )

                task_a = asyncio.create_task(connection.request("get_file", {}))
                while not handlers:
                    await asyncio.sleep(0.005)

                # B 在 A 还在等的时候发出；它的响应缺 echo，不能认领
                with pytest.raises(OneBot12ActionError, match="没有回显 echo"):
                    await connection.request("send_message", {})

                handlers[0].set_result({"served": "get_file"})
                with pytest.raises(OneBot12ActionError, match="没有回显 echo"):
                    await task_a

            assert sorted(seen) == ["get_file", "send_message"]
        finally:
            await _stop_json_server(port)


_JSON_SERVERS: dict[int, tuple[aiohttp.web.AppRunner, aiohttp.web.TCPSite]] = {}


async def _start_json_server(handler) -> int:
    """起一个只回 JSON 的本地 HTTP 服务。

    Args:
        handler: 处理动作请求的协程函数。

    Returns:
        监听端口。
    """
    from aiohttp import web

    app = web.Application()
    app.router.add_post("/", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = find_free_port()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    _JSON_SERVERS[port] = (runner, site)
    return port


async def _stop_json_server(port: int) -> None:
    """关闭 :func:`_start_json_server` 起的服务。

    Args:
        port: 监听端口。
    """
    entry = _JSON_SERVERS.pop(port, None)
    if entry is not None:
        runner, _site = entry
        await runner.cleanup()


class TestReverseWebSocketEndToEnd:
    """真实 aiohttp 服务端 + 真实 WebSocket 客户端。"""

    async def _start_server(
        self,
        transport: OneBot12Transport,
        port: int,
        path: str = "/onebot/v12",
    ) -> asyncio.Task:
        """在后台启动反向 WebSocket 服务。

        Args:
            transport: 通信层实例。
            port: 监听端口。
            path: WebSocket 路径。

        Returns:
            后台任务。
        """
        task = asyncio.create_task(
            transport.run_reverse_websocket(host="127.0.0.1", port=port, path=path),
        )
        # 等待服务真正开始监听
        for _ in range(100):
            await asyncio.sleep(0.02)
            if transport._runner is not None:  # noqa: SLF001
                return task
        raise AssertionError("反向 WebSocket 服务未能启动")

    async def test_full_handshake_event_and_action(self) -> None:
        received: list[tuple[dict, str | None]] = []

        async def on_event(event: dict, connection_id: str | None) -> None:
            received.append((event, connection_id))

        transport = OneBot12Transport(
            "ws_reverse",
            access_token="tok",
            api_timeout=3,
            debug=True,
        )
        transport.on_event(on_event)
        port = find_free_port()
        server = await self._start_server(transport, port)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    f"http://127.0.0.1:{port}/onebot/v12",
                    headers={
                        "Authorization": "Bearer tok",
                        "User-Agent": "OneBot/12 (qq) FakeImpl/1.0.0",
                    },
                ) as ws:
                    # 1) 实现端在连接后立刻推 meta.connect（规范要求是第一个事件）
                    await ws.send_str(
                        json.dumps(
                            {
                                "id": "evt-1",
                                "time": 1632847927.599013,
                                "type": "meta",
                                "detail_type": "connect",
                                "sub_type": "",
                                "version": {
                                    "impl": "fake-impl",
                                    "version": "1.0.0",
                                    "onebot_version": "12",
                                },
                            },
                        ),
                    )
                    await ws.send_str(
                        json.dumps(
                            {
                                "id": "evt-2",
                                "self": {"platform": "qq", "user_id": "123234"},
                                "time": 1632847927.599013,
                                "type": "message",
                                "detail_type": "group",
                                "sub_type": "",
                                "message_id": "6283",
                                "message": [
                                    {"type": "text", "data": {"text": "hi"}},
                                ],
                                "alt_message": "hi",
                                "group_id": "12345",
                                "user_id": "123456788",
                            },
                        ),
                    )

                    for _ in range(100):
                        if len(received) >= 2:
                            break
                        await asyncio.sleep(0.02)
                    else:
                        raise AssertionError("事件未被接收")

                    assert received[0][0]["detail_type"] == "connect"
                    assert received[0][1] is not None
                    assert received[1][0]["message_id"] == "6283"
                    assert received[1][1] == received[0][1]

                    # 2) 连接层发动作请求，实现端回动作响应
                    connection = transport.active_connections()[0]
                    pending = asyncio.create_task(
                        transport.request("send_message", {"detail_type": "group"}),
                    )
                    raw = await asyncio.wait_for(ws.receive(), timeout=5)
                    request = json.loads(raw.data)
                    assert request["action"] == "send_message"
                    assert request["params"] == {"detail_type": "group"}
                    assert isinstance(request["echo"], str)

                    await ws.send_str(
                        json.dumps(
                            {
                                "status": "ok",
                                "retcode": 0,
                                "data": {
                                    "message_id": "10086",
                                    "time": 1632847928.0,
                                },
                                "message": "",
                                "echo": request["echo"],
                            },
                        ),
                    )
                    assert await asyncio.wait_for(pending, timeout=5) == {
                        "message_id": "10086",
                        "time": 1632847928.0,
                    }
                    assert connection.connection_id.startswith("ws-reverse#")
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_bad_token_is_rejected(self) -> None:
        transport = OneBot12Transport("ws_reverse", access_token="tok")
        port = find_free_port()
        server = await self._start_server(transport, port)

        try:
            async with aiohttp.ClientSession() as session:
                with pytest.raises(aiohttp.WSServerHandshakeError) as excinfo:
                    await session.ws_connect(
                        f"http://127.0.0.1:{port}/onebot/v12",
                        headers={"Authorization": "Bearer wrong"},
                    )
                assert excinfo.value.status == 401
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_token_via_query_param(self) -> None:
        transport = OneBot12Transport("ws_reverse", access_token="tok")
        port = find_free_port()
        server = await self._start_server(transport, port)

        try:
            async with (
                aiohttp.ClientSession() as session,
                session.ws_connect(
                    f"http://127.0.0.1:{port}/onebot/v12?access_token=tok",
                ) as ws,
            ):
                assert ws.closed is False
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_http_webhook_accepts_event_and_requires_token(self) -> None:
        received: list[dict] = []

        async def on_event(event: dict, connection_id: str | None) -> None:  # noqa: ARG001
            received.append(event)

        transport = OneBot12Transport("http_post", access_token="hook")
        transport.on_event(on_event)
        port = find_free_port()
        server = asyncio.create_task(
            transport.run_http_webhook(
                host="127.0.0.1",
                port=port,
                path="/onebot/v12/webhook",
                api_endpoint=None,
            ),
        )
        for _ in range(100):
            await asyncio.sleep(0.02)
            if transport._runner is not None:  # noqa: SLF001
                break

        try:
            url = f"http://127.0.0.1:{port}/onebot/v12/webhook"
            event = {
                "id": "evt-3",
                "self": {"platform": "qq", "user_id": "1"},
                "time": 1632847927.599013,
                "type": "notice",
                "detail_type": "group_member_increase",
                "sub_type": "join",
                "group_id": "12345",
                "user_id": "2",
                "operator_id": "3",
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=event,
                    headers={"Authorization": "Bearer hook"},
                ) as response:
                    assert response.status == 204
                async with session.post(
                    url,
                    json=event,
                    headers={"Authorization": "Bearer wrong"},
                ) as response:
                    assert response.status == 401

            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)
            assert received and received[0]["detail_type"] == "group_member_increase"
        finally:
            await transport.aclose()
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)
