"""HTTP 两种通信方式的端到端测试（此前只验证过 WebSocket）。

用真实的 aiohttp 服务端/客户端，覆盖：

- ``http_post``：实现端 POST 事件到 AstrBot，AstrBot 通过实现端的 HTTP API 回动作；
- ``http_poll``：AstrBot 轮询 ``get_latest_events``，事件经同一条 HTTP 通道回动作。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from astrbot.core.platform.register import platform_cls_map, platform_registry
from conftest import find_free_port

# 注意：不要在这里再 importlib 一次适配器模块——同一个模块被导入两次会让
# @register_platform_adapter 命中「适配器已注册」的 ValueError。


class FakeImplementation:
    """一个最小可用的 OneBot 12 实现端（HTTP 动作通道 + 事件来源）。"""

    def __init__(self) -> None:
        self.actions: list[dict[str, Any]] = []
        self.pending_events: list[dict[str, Any]] = []
        self.ready = asyncio.Event()

    async def handle_action(self, request: web.Request) -> web.Response:
        """处理 AstrBot 发来的动作请求。

        Args:
            request: HTTP 请求。

        Returns:
            规范格式的动作响应。
        """
        payload = await request.json()
        self.actions.append(payload)
        action = payload.get("action")
        if action == "get_latest_events":
            events, self.pending_events = self.pending_events, []
            data: Any = events
        else:
            data = {}
        return web.json_response(
            {
                "status": "ok",
                "retcode": 0,
                "data": data,
                "message": "",
                "echo": payload.get("echo"),
            },
        )

    async def handle_event_push(self, request: web.Request) -> web.Response:
        """接收 AstrBot 推送的事件（HTTP Webhook 模式下反向用不到，留作占位）。

        Args:
            request: HTTP 请求。

        Returns:
            空响应。
        """
        del request
        return web.Response(status=204)


def group_message(message_id: str, text: str) -> dict[str, Any]:
    """构造一条群消息事件。

    Args:
        message_id: 消息 ID。
        text: 文本内容。

    Returns:
        事件字典。
    """
    return {
        "id": f"evt-{message_id}",
        "self": {"platform": "qq", "user_id": "123234"},
        "time": 1632847927.599013,
        "type": "message",
        "detail_type": "group",
        "sub_type": "",
        "message_id": message_id,
        "message": [{"type": "text", "data": {"text": text}}],
        "alt_message": text,
        "group_id": "12345",
        "user_id": "123456788",
    }


@pytest_asyncio.fixture
async def impl_server() -> AsyncIterator[tuple[FakeImplementation, str]]:
    """启动假的 OneBot 12 实现端（HTTP API）。

    Yields:
        ``(impl, base_url)``。
    """
    impl = FakeImplementation()
    app = web.Application()
    app.router.add_post("/", impl.handle_action)
    app.router.add_post("/push", impl.handle_event_push)
    runner = web.AppRunner(app)
    await runner.setup()
    port = find_free_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    try:
        yield impl, f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


def build_adapter(mode: str, api_base_url: str, queue: asyncio.Queue):
    """按给定连接方式构造适配器。

    Args:
        mode: 连接方式。
        api_base_url: 实现端 HTTP API 地址。
        queue: 事件队列。

    Returns:
        ``(adapter, platform_cls)``。
    """
    meta = next(m for m in platform_registry if m.name == "onebot12")
    platform_cls = platform_cls_map["onebot12"]
    config = dict(meta.default_config_tmpl)
    config.update(
        {
            "id": f"onebot12-{mode}",
            "type": "onebot12",
            "enable": True,
            "connection_mode": mode,
            "access_token": "",
            "api_base_url": api_base_url,
            "fetch_nickname": False,
            "media_resolve_mode": "none",
            "api_timeout": 10,
            "poll_interval": 1,
        },
    )
    return platform_cls(config, {}, queue)


async def wait_event(queue: asyncio.Queue, limit: int = 300) -> Any:
    """等待事件入队。

    Args:
        queue: 事件队列。
        limit: 最长等待轮数（每轮 20ms）。

    Returns:
        事件对象。

    Raises:
        AssertionError: 超时未收到。
    """
    for _ in range(limit):
        if not queue.empty():
            return queue.get_nowait()
        await asyncio.sleep(0.02)
    raise AssertionError("事件没有进入 AstrBot 事件队列")


class TestHttpWebhookMode:
    """``http_post``：实现端推事件过来，动作走实现端 HTTP API。"""

    async def test_event_in_and_action_out(self, impl_server) -> None:
        impl, base_url = impl_server
        port = find_free_port()
        queue: asyncio.Queue = asyncio.Queue()
        adapter = build_adapter("http_post", base_url, queue)
        adapter.ws_host = "127.0.0.1"
        adapter.ws_port = port
        adapter.http_host = "127.0.0.1"
        adapter.http_port = port
        adapter.http_path = "/onebot/v12/webhook"
        runner = asyncio.create_task(adapter.run())

        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if adapter.get_client()._runner is not None:  # noqa: SLF001
                    break

            async with aiohttp.ClientSession() as session:
                # 1) 实现端把事件 POST 给 AstrBot，规范要求 204
                async with session.post(
                    f"http://127.0.0.1:{port}/onebot/v12/webhook",
                    json=group_message("6283", "webhook 消息"),
                ) as response:
                    assert response.status == 204

                event = await wait_event(queue)
                assert event.get_message_str() == "webhook 消息"
                assert event.get_group_id() == "12345"

                # 2) 事件回复走实现端的 HTTP API
                from astrbot.api.event import MessageChain
                from astrbot.api.message_components import Plain

                await asyncio.wait_for(
                    event.send(MessageChain([Plain(text="回你一句")])),
                    timeout=10,
                )

            sent = [a for a in impl.actions if a["action"] == "send_message"]
            assert len(sent) == 1, impl.actions
            assert sent[0]["params"]["detail_type"] == "group"
            assert sent[0]["params"]["group_id"] == "12345"
            assert sent[0]["params"]["message"] == [
                {"type": "text", "data": {"text": "回你一句"}},
            ]
        finally:
            await adapter.terminate()
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)

    async def test_wrong_token_is_rejected(self, impl_server) -> None:
        _impl, base_url = impl_server
        port = find_free_port()
        queue: asyncio.Queue = asyncio.Queue()
        adapter = build_adapter("http_post", base_url, queue)
        adapter.access_token = "hook-secret"
        adapter.transport.access_token = "hook-secret"
        adapter.http_host = "127.0.0.1"
        adapter.http_port = port
        adapter.http_path = "/onebot/v12/webhook"
        runner = asyncio.create_task(adapter.run())

        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if adapter.get_client()._runner is not None:  # noqa: SLF001
                    break

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://127.0.0.1:{port}/onebot/v12/webhook",
                    json=group_message("6283", "x"),
                    headers={"Authorization": "Bearer wrong"},
                ) as response:
                    assert response.status == 401
        finally:
            await adapter.terminate()
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)


class TestHttpPollMode:
    """``http_poll``：AstrBot 轮询 get_latest_events，动作走同一条 HTTP 通道。"""

    async def test_polled_event_in_and_action_out(self, impl_server) -> None:
        impl, base_url = impl_server
        queue: asyncio.Queue = asyncio.Queue()
        adapter = build_adapter("http_poll", base_url, queue)
        runner = asyncio.create_task(adapter.run())

        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if adapter.get_client()._active_poll_ready():  # type: ignore[attr-defined]
                    break
        except AttributeError:
            pass

        try:
            # 等轮询真正开始发动作请求
            for _ in range(150):
                await asyncio.sleep(0.02)
                if any(a["action"] == "get_latest_events" for a in impl.actions):
                    break
            else:
                raise AssertionError(f"没有看到 get_latest_events: {impl.actions}")

            # 把事件放进实现端的待取队列，等下一轮轮询取走
            impl.pending_events.append(group_message("777", "轮询消息"))

            event = await wait_event(queue)
            assert event.get_message_str() == "轮询消息"

            from astrbot.api.event import MessageChain
            from astrbot.api.message_components import Plain

            await asyncio.wait_for(
                event.send(MessageChain([Plain(text="poll 回复")])),
                timeout=10,
            )

            sent = [a for a in impl.actions if a["action"] == "send_message"]
            assert len(sent) == 1, impl.actions
            assert sent[0]["params"]["message"] == [
                {"type": "text", "data": {"text": "poll 回复"}},
            ]
        finally:
            await adapter.terminate()
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
