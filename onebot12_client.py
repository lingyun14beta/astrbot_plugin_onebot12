"""OneBot 12 连接层：四种通信方式的事件接收与动作请求/响应。

OneBot 12 把「通信方式」和「数据协议」拆开定义（https://12.onebot.dev/connect/ ）：
通信方式只负责搬 JSON，事件、动作请求、动作响应三种数据在四种方式上完全一致。
因此这里把连接方式抽象成统一的 :class:`OneBot12Connection` 接口，上层只关心
``request(action, params)`` 与「收到事件」回调：

- ``ws_reverse``：AstrBot 起 WebSocket 服务端，实现端反向连过来（推荐，一条连接
  同时跑事件与动作）；
- ``ws_forward``：AstrBot 主动连实现端的 WebSocket 服务端；
- ``http_post``：实现端把事件 POST 到 AstrBot 的 Webhook 地址，AstrBot 通过
  ``api_base_url`` 用 HTTP 发动作请求；
- ``http_poll``：AstrBot 定时调用 ``get_latest_events`` 拉事件，动作走 HTTP。

鉴权按规范实现：``Authorization: Bearer <access_token>``，接收侧额外接受
``access_token`` URL query 参数作为回退（规范允许实现端在无法设置请求头时这样做）。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web

from astrbot.api import logger

from .onebot12_message import OneBot12ActionError, dump_compact

RECONNECT_MAX_DELAY = 60.0
"""正向 WebSocket 重连退避的上限（秒）。"""

RECONNECT_STABLE_SECONDS = 30.0
"""连接存活超过这个时长才认为「连上了」，并把退避重置回最小值。

否则一个「接受握手后立刻断开」的实现端（没配置这个应用、角色填反、代理重置）
会让退避每次都重置成 1 秒，变成每秒一次的连接风暴。
"""

DRAIN_TIMEOUT = 5.0
"""关闭前等待积压事件处理完的上限（秒），超时就丢弃并告警。"""

_RETIRE = object()
"""事件队列的退休哨兵：连接注销后投给 worker，让它把存量事件处理完再退出。"""

EventCallback = Callable[[dict[str, Any], str | None], Awaitable[None]]
"""事件回调签名：``(event, connection_id)``，``connection_id`` 为该事件来源连接。"""


@dataclass(frozen=True)
class OneBot12Endpoint:
    """已解析的 OneBot 12 服务地址。

    Attributes:
        scheme: ``ws`` / ``wss`` / ``http`` / ``https``。
        host: 主机名或 IP。
        port: 端口号，0 表示按 scheme 取默认端口。
        path: 路径，``/`` 开头。
    """

    scheme: str
    host: str
    port: int
    path: str

    def url(self) -> str:
        """拼回完整 URL。

        Returns:
            形如 ``ws://127.0.0.1:6700/onebot/v12`` 的 URL。
        """
        return f"{self.scheme}://{self.authority()}{self.path}"

    def authority(self) -> str:
        """拼出 ``host:port``，端口为 scheme 默认值时省略端口。

        Returns:
            ``host``、``host:port``，IPv6 主机名形如 ``[::1]:6700``。
        """
        # IPv6 字面量必须加方括号，否则 ws://::1:6700 不是合法 URL
        host = f"[{self.host}]" if ":" in self.host else self.host
        default = {"ws": 80, "wss": 443, "http": 80, "https": 443}.get(self.scheme)
        if not self.port or self.port == default:
            return host
        return f"{host}:{self.port}"


def parse_endpoint(
    address: str,
    *,
    default_scheme: str,
    default_path: str = "/",
) -> OneBot12Endpoint:
    """解析用户填写的地址。

    用户可能只填 ``127.0.0.1:6700``，也可能填完整 URL，这里统一处理。

    Args:
        address: 原始地址。
        default_scheme: 没有 scheme 时使用的协议，如 ``ws`` 或 ``http``。
        default_path: 没有路径时使用的路径。

    Returns:
        解析后的 :class:`OneBot12Endpoint`。

    Raises:
        ValueError: 地址为空、解析不出主机名，或端口不合法时抛出。
    """
    raw = (address or "").strip()
    if not raw:
        raise ValueError("OneBot 12 地址为空")

    try:
        if "://" not in raw:
            # 裸地址按 scheme://host[:port][/path] 解析，避免把 host:port 误判成 scheme
            parsed = urlsplit(f"{default_scheme}://{raw}")
        else:
            parsed = urlsplit(raw)
        # 访问 .port 才会真正校验端口，urlsplit 本身不校验
        port = parsed.port or 0
        hostname = parsed.hostname
        scheme = (parsed.scheme or default_scheme).lower()
        path = parsed.path or default_path
    except ValueError as exc:
        # urlsplit 对非法端口抛的是原生 ValueError，转成能指回配置项的消息
        raise ValueError(f"无法解析地址 {address!r}: {exc}") from exc

    if not hostname:
        raise ValueError(f"无法从 {address!r} 解析出主机名")
    if not path.startswith("/"):
        path = "/" + path
    return OneBot12Endpoint(
        scheme=scheme,
        host=hostname,
        port=port,
        path=path,
    )


def is_token_valid(
    expected: str,
    authorization: str | None,
    access_token_param: str | None,
) -> bool:
    """按规范校验访问令牌。

    规范原文：先看 ``Authorization`` 头是否为 ``Bearer <access_token>``，
    没有再退回 ``access_token`` URL query 参数；两者都没有即鉴权失败。规范同时
    强调 ``<access_token>`` 两侧空白不裁剪，因此这里不做 strip。

    Args:
        expected: 配置的访问令牌，空字符串表示不鉴权。
        authorization: 请求头里的 ``Authorization`` 值。
        access_token_param: URL query 里的 ``access_token`` 值。

    Returns:
        是否通过鉴权。
    """
    if not expected:
        return True
    if authorization is not None:
        return authorization == f"Bearer {expected}"
    if access_token_param is not None:
        return access_token_param == expected
    return False


class OneBot12Connection:
    """一条 OneBot 12 连接的抽象基类。

    子类需要实现 :meth:`_deliver`、:meth:`close` 与 :meth:`closed`。所有动作请求
    用一个唯一 ``echo`` 标识，响应回来后按 ``echo`` 唤醒等待中的 Future。
    """

    def __init__(
        self,
        connection_id: str,
        *,
        api_timeout: float,
        debug: bool = False,
    ) -> None:
        self.connection_id = connection_id
        self.api_timeout = api_timeout
        self.debug = debug
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_actions: dict[str, str] = {}
        self._closed = False

    # -- 子类实现 ---------------------------------------------------------

    async def _deliver(self, payload: dict[str, Any]) -> None:
        """把动作请求或动作响应发送到对端。

        Args:
            payload: 完整的 JSON 报文。

        Raises:
            NotImplementedError: 子类未实现。
        """
        raise NotImplementedError

    async def close(self) -> None:
        """关闭连接。"""

    def closed(self) -> bool:
        """连接是否已经关闭。

        Returns:
            已关闭返回 ``True``。
        """
        return self._closed

    def describe(self) -> str:
        """返回用于日志的连接描述。

        Returns:
            连接描述字符串。
        """
        return self.connection_id

    # -- 公共实现 ---------------------------------------------------------

    def _new_echo(
        self,
        action: str,
    ) -> tuple[str, asyncio.Future[dict[str, Any]]]:
        """生成唯一 ``echo`` 并登记等待中的 Future。

        Args:
            action: 该请求的动作名，供「实现端没回 echo」时按动作名兜底匹配。

        Returns:
            ``(echo, future)``。
        """
        echo = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[echo] = future
        self._pending_actions[echo] = action
        return echo, future

    async def request(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        self_info: dict[str, str] | None = None,
    ) -> Any:
        """发送一个动作请求并等待响应。

        Args:
            action: 动作名，如 ``send_message``。
            params: 动作参数。
            self_info: 机器人自身标识，非空时作为顶层 ``self`` 字段发出。

        Returns:
            动作响应的 ``data`` 字段；实现端返回 ``data: null`` 时为 ``None``。

        Raises:
            OneBot12ActionError: 连接不可用、实现端返回 ``status != ok`` 或超时。
        """
        if self._closed:
            raise OneBot12ActionError(action, -1, "连接已关闭")

        echo, future = self._new_echo(action)
        payload: dict[str, Any] = {
            "action": action,
            "params": params or {},
            "echo": echo,
        }
        if self_info:
            payload["self"] = self_info
        if self.debug:
            logger.debug("[onebot12] -> %s %s", self.describe(), dump_compact(payload))
        try:
            await self._deliver(payload)
            response = await asyncio.wait_for(future, timeout=self.api_timeout)
        except TimeoutError as exc:
            raise OneBot12ActionError(
                action,
                -1,
                f"等待动作响应超时（{self.api_timeout:g}s）",
            ) from exc
        finally:
            self._pending.pop(echo, None)
            self._pending_actions.pop(echo, None)

        status = response.get("status")
        retcode = response.get("retcode")
        if status != "ok" or retcode != 0:
            raise OneBot12ActionError(
                action,
                int(retcode) if isinstance(retcode, int) else -1,
                str(response.get("message") or status or "unknown error"),
            )
        return response.get("data")

    def resolve_response(self, response: dict[str, Any]) -> bool:
        """把收到的动作响应交给等待中的 Future。

        Args:
            response: 动作响应 JSON。

        Returns:
            是否匹配到等待中的请求。
        """
        if self.debug:
            logger.debug(
                "[onebot12] <- %s %s",
                self.describe(),
                dump_compact(response),
            )
        echo = response.get("echo")
        if not isinstance(echo, str):
            return False
        future = self._pending.get(echo)
        if future is None or future.done():
            return False
        future.set_result(response)
        return True

    def take_pending(
        self, action: str | None = None
    ) -> asyncio.Future[dict[str, Any]] | None:
        """取出等待中的 Future，仅在**唯一**时可用（实现端漏回 ``echo`` 时兜底）。

        并发请求下不能猜：多把等待中的 Future 里随便挑一个，会把先到的响应交给
        ``http_poll`` 那条常驻的长轮询、或把 A 的响应交给 B，后续每一个响应都错位
        一格。这种情况下宁可直接失败。

        Args:
            action: 期望的动作名。传了就只在「唯一等待者且动作名相符」时返回，
                避免把 ``send_message`` 的响应交给 ``get_file``。

        Returns:
            匹配的 Future；没有、有多个、或动作名不符时返回 ``None``。
        """
        pending = [
            (echo, future)
            for echo, future in self._pending.items()
            if not future.done()
        ]
        if len(pending) != 1:
            return None
        echo, future = pending[0]
        if action is not None and self._pending_actions.get(echo) != action:
            return None
        return future

    def fail_pending(self, reason: str) -> None:
        """连接断开时让所有等待中的请求立即失败。

        Args:
            reason: 失败原因。
        """
        for echo, future in list(self._pending.items()):
            if not future.done():
                future.set_exception(OneBot12ActionError("unknown", -1, reason))
            self._pending.pop(echo, None)
            self._pending_actions.pop(echo, None)


class OneBot12WebSocketConnection(OneBot12Connection):
    """基于一条 WebSocket 连接的实现端会话（正向/反向 WebSocket 共用）。"""

    def __init__(
        self,
        connection_id: str,
        ws: Any,
        *,
        api_timeout: float,
        debug: bool = False,
        client_info: str = "",
    ) -> None:
        super().__init__(connection_id, api_timeout=api_timeout, debug=debug)
        self.ws = ws
        self.client_info = client_info

    def describe(self) -> str:
        """返回带对端信息的连接描述。

        Returns:
            形如 ``ws-reverse#1 (OneBot/12 (qq) Go-LibOneBot/1.0.0)`` 的描述。
        """
        suffix = f" ({self.client_info})" if self.client_info else ""
        return f"{self.connection_id}{suffix}"

    def closed(self) -> bool:
        """WebSocket 是否已关闭（兼容 aiohttp 的不同属性）。

        Returns:
            已关闭返回 ``True``。
        """
        if self._closed:
            return True
        ws = self.ws
        if ws is None:
            return True
        if getattr(ws, "closed", False):
            return True
        return getattr(ws, "close_code", None) is not None

    async def _deliver(self, payload: dict[str, Any]) -> None:
        """通过 WebSocket 发送报文。

        Args:
            payload: JSON 报文。

        Raises:
            OneBot12ActionError: 连接不可用或发送失败。
        """
        if self.closed():
            raise OneBot12ActionError(
                str(payload.get("action") or "response"),
                -1,
                "WebSocket 连接已关闭",
            )
        try:
            await self.ws.send_str(json.dumps(payload, ensure_ascii=False))
        except Exception as exc:
            raise OneBot12ActionError(
                str(payload.get("action") or "response"),
                -1,
                f"WebSocket 发送失败: {exc}",
            ) from exc

    async def close(self) -> None:
        """关闭 WebSocket 连接。"""
        self._closed = True
        self.fail_pending("WebSocket 连接已关闭")
        ws = self.ws
        if ws is None:
            return
        with contextlib.suppress(Exception):
            await ws.close(code=1000, message=b"adapter shutdown")


class OneBot12HttpConnection(OneBot12Connection):
    """HTTP 动作通道：动作请求以 POST 发给实现端，HTTP 响应体即动作响应。"""

    def __init__(
        self,
        connection_id: str,
        session: aiohttp.ClientSession,
        endpoint: OneBot12Endpoint,
        *,
        access_token: str = "",
        api_timeout: float = 30.0,
        debug: bool = False,
    ) -> None:
        super().__init__(connection_id, api_timeout=api_timeout, debug=debug)
        self.session = session
        self.endpoint = endpoint
        self.access_token = access_token

    def describe(self) -> str:
        """返回 HTTP 端点描述。

        Returns:
            形如 ``http-api (http://127.0.0.1:6700/)`` 的描述。
        """
        return f"{self.connection_id} ({self.endpoint.url()})"

    def _headers(self) -> dict[str, str]:
        """构造动作请求的请求头。

        Returns:
            请求头字典，配置了令牌时带 ``Authorization``。
        """
        headers = {"Content-Type": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        return headers

    async def _deliver(self, payload: dict[str, Any]) -> None:
        """通过 HTTP 发送动作请求，并把响应交给等待中的 Future。

        Args:
            payload: 动作请求 JSON。

        Raises:
            OneBot12ActionError: HTTP 层失败或响应不是合法 JSON 对象。
        """
        action = str(payload.get("action"))
        session = self.session
        if session is None:
            raise OneBot12ActionError(action, -1, "HTTP 通道未初始化客户端会话")
        timeout = aiohttp.ClientTimeout(total=self.api_timeout)
        try:
            async with session.post(
                self.endpoint.url(),
                json=payload,
                headers=self._headers(),
                timeout=timeout,
            ) as response:
                if response.status == 401:
                    raise OneBot12ActionError(action, -1, "HTTP 鉴权失败 (401)")
                if response.status != 200:
                    raise OneBot12ActionError(
                        action,
                        -1,
                        f"HTTP 状态码异常: {response.status}",
                    )
                data = await response.json(content_type=None)
        except OneBot12ActionError:
            raise
        except Exception as exc:
            raise OneBot12ActionError(action, -1, f"HTTP 请求失败: {exc}") from exc

        if not isinstance(data, dict):
            raise OneBot12ActionError(action, -1, f"动作响应不是对象: {data!r}")
        if self.resolve_response(data):
            return
        # 规范（connect/data-protocol/action-response.md）要求：请求带了非空 echo，
        # 响应就必须原样回显它。所以走到这里只可能是实现端没守规范。
        # 按到达顺序认领是**不安全**的：两个请求同时在飞时会把 A 的数据交给 B，
        # 而且从那一刻起每个响应都会跟着错位；即便「只剩一个等待者」也不能证明
        # 它就是这份响应的主人。因此这里直接失败——代价是「不回显 echo 的实现端 +
        # HTTP 并发请求」无法配对，这是实现端的问题，靠猜补不回来。
        raise OneBot12ActionError(
            action,
            -1,
            "收到无法匹配的动作响应：实现端没有回显 echo。"
            "按规范它应原样返回请求里的 echo；并发场景下无法安全配对",
        )

    async def close(self) -> None:
        """标记 HTTP 通道关闭。"""
        self._closed = True
        self.fail_pending("HTTP 通道已关闭")


class OneBot12Transport:
    """OneBot 12 通信层。

    负责按配置建立连接、接收事件、路由动作请求。事件通过 :meth:`on_event` 注册的
    回调交给上层转换成 AstrBot 事件。

    Args:
        mode: 连接方式，``ws_reverse`` / ``ws_forward`` / ``http_post`` / ``http_poll``。
        access_token: 访问令牌，空字符串表示不鉴权。
        api_timeout: 动作请求超时时间（秒）。
        debug: 是否打印收发的原始 JSON。
    """

    def __init__(
        self,
        mode: str,
        *,
        access_token: str = "",
        api_timeout: float = 30.0,
        debug: bool = False,
    ) -> None:
        self.mode = mode
        self.access_token = access_token
        self.api_timeout = max(api_timeout, 1.0)
        self.debug = debug

        self._session: aiohttp.ClientSession | None = None
        self._runner: web.AppRunner | None = None
        self._connections: dict[str, OneBot12Connection] = {}
        self._conn_seq = 0
        self._event_cb: EventCallback | None = None
        self._running = False
        self._api_connection: OneBot12Connection | None = None
        # 每条连接一份事件处理队列与工作协程，见 _enqueue_event 的说明
        self._event_queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._event_workers: dict[str, asyncio.Task[None]] = {}

    # -- 生命周期 ---------------------------------------------------------

    def on_event(self, callback: EventCallback) -> None:
        """注册事件回调。

        Args:
            callback: 收到事件时调用的异步函数。
        """
        self._event_cb = callback

    async def dispatch(self, event: dict[str, Any], connection_id: str | None) -> None:
        """把事件排进所属连接的处理队列（不阻塞调用方）。

        为什么不在读循环里直接 ``await`` 回调：回调要落地上游事件，而上游为了
        把媒体从 ``file_id`` 换回来会再发 ``get_file`` 动作请求——那个响应同样
        要经过本读循环才能派发。若读循环被回调占住，响应就永远读不到，动作请求
        只能等超时（实测：实现端秒回，图片依然每次 3 秒超时后被丢弃）。

        因此这里只入队，实际处理交给每条连接一个的 worker 协程：读循环始终可以
        继续读取并派发动作响应，同时单条连接内的事件仍按到达顺序串行处理。

        Args:
            event: OneBot 12 事件 JSON。
            connection_id: 事件来源连接 ID，未知时为 ``None``。未知来源（例如
                HTTP Webhook 首帧）会归到一条共享队列上，保证顺序。
        """
        key = connection_id or "__shared__"
        queue = self._event_queues.get(key)
        if queue is None:
            queue = asyncio.Queue()
            self._event_queues[key] = queue
            self._event_workers[key] = asyncio.create_task(
                self._run_event_worker(queue, key),
                name=f"onebot12-events-{key}",
            )
        queue.put_nowait(event)

    async def _run_event_worker(
        self,
        queue: asyncio.Queue[Any],
        key: str,
    ) -> None:
        """逐条处理队列里的事件，收到退休哨兵后自行退出。

        Args:
            queue: 该连接的事件队列。
            key: 连接 ID（仅用于日志）。
        """
        try:
            while True:
                event = await queue.get()
                try:
                    if event is _RETIRE:
                        return
                    if self._event_cb is not None:
                        await self._event_cb(
                            event,
                            None if key == "__shared__" else key,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # 单条事件处理失败不应拖垮整条连接
                    logger.error(
                        "[onebot12] 处理事件异常 (%s): %s",
                        key,
                        exc,
                        exc_info=True,
                    )
                finally:
                    # drain() 依赖 queue.join()，漏掉 task_done() 会让它一直等到超时
                    queue.task_done()
        finally:
            # 连接已经注销，队列与 worker 的登记一并收掉，避免重连时列表无限增长
            self._event_queues.pop(key, None)
            self._event_workers.pop(key, None)

    def _retire_event_worker(self, connection_id: str) -> None:
        """给某条连接的 worker 投递退休哨兵。

        不直接 ``cancel()``：注销时可能还有最后一条消息正在下载媒体，取消会把它
        拦腰截断。哨兵排在队尾，worker 把已入队的事件处理完自然退出。

        Args:
            connection_id: 已注销的连接 ID。
        """
        queue = self._event_queues.get(connection_id)
        if queue is not None:
            queue.put_nowait(_RETIRE)

    async def drain(self) -> None:
        """等待所有已入队的事件处理完毕（关闭前调用）。"""
        for key, queue in list(self._event_queues.items()):
            if queue.empty():
                continue
            try:
                await asyncio.wait_for(queue.join(), timeout=DRAIN_TIMEOUT)
            except TimeoutError:
                # 说清丢了多少条：静默丢弃最难排查
                logger.warning(
                    "[onebot12] 连接 %s 仍有 %d 条事件未处理完（等待 %.0fs），"
                    "这些事件将被丢弃",
                    key,
                    queue.qsize(),
                    DRAIN_TIMEOUT,
                )

    def _next_connection_id(self, prefix: str) -> str:
        """生成连接 ID。

        Args:
            prefix: 连接类型前缀。

        Returns:
            形如 ``ws-reverse#1`` 的 ID。
        """
        self._conn_seq += 1
        return f"{prefix}#{self._conn_seq}"

    def _register(self, connection: OneBot12Connection) -> None:
        """登记一条连接。

        Args:
            connection: 连接对象。
        """
        self._connections[connection.connection_id] = connection

    def _unregister(self, connection_id: str) -> None:
        """注销一条连接，并让它的事件 worker 自行退休。

        Args:
            connection_id: 连接 ID。
        """
        connection = self._connections.pop(connection_id, None)
        if connection is not None:
            connection.fail_pending("连接已断开")
        self._retire_event_worker(connection_id)

    def active_connections(self) -> list[OneBot12Connection]:
        """返回当前可用连接。

        Returns:
            未关闭的连接列表。
        """
        return [c for c in self._connections.values() if not c.closed()]

    async def aclose(self) -> None:
        """关闭所有连接与后台任务。

        顺序要紧：先把已入队的事件处理完（它们可能还在等动作响应），再关连接，
        最后收掉事件 worker，避免事件处理到一半连接就没了。
        """
        self._running = False
        await self.drain()

        for connection in list(self._connections.values()):
            with contextlib.suppress(Exception):
                await connection.close()
        self._connections.clear()

        for worker in list(self._event_workers.values()):
            worker.cancel()
        if self._event_workers:
            await asyncio.gather(
                *self._event_workers.values(),
                return_exceptions=True,
            )
        # 到这里还没被处理的事件已经彻底丢了（drain 已尽过力并告警），
        # 直接丢引用即可。不要试图用 task_done() 去「清账」——它不出队，
        # 在非空队列上会一直把 _unfinished_tasks 减到 0 然后抛 ValueError，
        # 那会把后面的 runner.cleanup()/session.close() 一起带没。
        self._event_workers.clear()
        self._event_queues.clear()

        if self._runner is not None:
            with contextlib.suppress(Exception):
                await self._runner.cleanup()
            self._runner = None
        if self._session is not None:
            with contextlib.suppress(Exception):
                await self._session.close()
            self._session = None
        if self._api_connection is not None:
            with contextlib.suppress(Exception):
                await self._api_connection.close()
            self._api_connection = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """惰性创建 aiohttp 客户端会话。

        Returns:
            客户端会话。
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.api_timeout),
            )
        return self._session

    @contextlib.asynccontextmanager
    async def session_for_media(self) -> AsyncIterator[aiohttp.ClientSession]:
        """借出 HTTP 会话用于下载媒体，供上传降级路径复用连接池。

        Yields:
            可用的 aiohttp 客户端会话。

        Raises:
            OneBot12ActionError: 会话创建失败时抛出。
        """
        try:
            session = await self._ensure_session()
        except Exception as exc:
            raise OneBot12ActionError(
                "upload_file",
                -1,
                f"HTTP 会话不可用: {exc}",
            ) from exc
        yield session

    # -- 动作请求路由 -----------------------------------------------------

    def _pick_connection(self, prefer: str | None = None) -> OneBot12Connection:
        """挑一条可用连接发送动作请求。

        Args:
            prefer: 优先使用的连接 ID（一般是事件来源连接）。

        Returns:
            可用连接。

        Raises:
            OneBot12ActionError: 没有可用连接，或指定的连接已经不在了。
        """
        if prefer:
            connection = self._connections.get(prefer)
            if connection is not None and not connection.closed():
                return connection
            # 指定的连接没了就报错，不要顺手挑一条别的：多条连接通常就是多个
            # 机器人账号，拿别的连接发回复等于用另一个账号说话，取 get_file 更是
            # 拿 A 的 file_id 去问 B（file_id 是按账号隔离的）。
            active = self.active_connections()
            raise OneBot12ActionError(
                "unknown",
                -1,
                f"事件来源连接 {prefer} 已断开（当前还有 {len(active)} 条其它连接）；"
                "为避免串号，本次动作请求已放弃",
            )
        active = self.active_connections()
        if active:
            return active[0]
        if self._api_connection is not None and not self._api_connection.closed():
            return self._api_connection
        raise OneBot12ActionError(
            "unknown",
            -1,
            "没有可用的 OneBot 12 连接，请检查实现端是否已连接",
        )

    async def request(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        prefer: str | None = None,
        self_info: dict[str, str] | None = None,
    ) -> Any:
        """发送动作请求。

        Args:
            action: 动作名。
            params: 动作参数。
            prefer: 优先使用的连接 ID。
            self_info: 机器人自身标识，非空时会作为顶层 ``self`` 字段一起发出。
                规范要求多账号共享一条连接时，除元动作外的动作请求都带它，否则
                实现端可以回 ``10101 Who Am I``。

        Returns:
            动作响应的 ``data`` 字段。

        Raises:
            OneBot12ActionError: 没有可用连接或实现端返回失败。
        """
        connection = self._pick_connection(prefer)
        return await connection.request(action, params, self_info=self_info)

    # -- 反向 WebSocket ---------------------------------------------------

    async def run_reverse_websocket(
        self,
        *,
        host: str,
        port: int,
        path: str,
    ) -> None:
        """启动反向 WebSocket 服务端并常驻。

        Args:
            host: 监听地址。
            port: 监听端口。
            path: WebSocket 路径，实现端需要连到这个路径。
        """
        self._running = True
        app = web.Application()
        app.router.add_get(path, self._handle_reverse_ws)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host, port)
        await site.start()
        logger.info(
            "[onebot12] 反向 WebSocket 服务已启动: ws://%s:%s%s"
            "（请在 OneBot 12 实现端填写该地址）",
            host,
            port,
            path,
        )
        while self._running:
            await asyncio.sleep(3600)

    async def _handle_reverse_ws(self, request: web.Request) -> web.WebSocketResponse:
        """处理一条反向 WebSocket 连接。

        Args:
            request: HTTP 请求对象。

        Returns:
            已升级的 WebSocket 响应。

        Raises:
            web.HTTPUnauthorized: 鉴权失败。
        """
        if not is_token_valid(
            self.access_token,
            request.headers.get("Authorization"),
            request.query.get("access_token"),
        ):
            logger.warning("[onebot12] 反向 WebSocket 鉴权失败，已拒绝连接")
            raise web.HTTPUnauthorized(text="invalid access token")

        # heartbeat 让 aiohttp 主动 ping：对端主机掉线/NAT 静默断流时，TCP 会一直
        # 停在 ESTABLISHED，closed() 也不会变 True，这条「僵尸连接」还会被
        # _pick_connection 选中，导致每个动作都白等一整个 api_timeout。
        ws = web.WebSocketResponse(heartbeat=30.0, max_msg_size=0)
        await ws.prepare(request)

        connection_id = self._next_connection_id("ws-reverse")
        client_info = (
            request.headers.get("User-Agent")
            or request.headers.get("Sec-WebSocket-Protocol")
            or ""
        )
        connection = OneBot12WebSocketConnection(
            connection_id,
            ws,
            api_timeout=self.api_timeout,
            debug=self.debug,
            client_info=client_info,
        )
        self._register(connection)
        logger.info("[onebot12] 反向 WebSocket 已连接: %s", connection.describe())

        try:
            await self._read_ws(connection)
        finally:
            self._unregister(connection_id)
            with contextlib.suppress(Exception):
                await ws.close()
            logger.info("[onebot12] 反向 WebSocket 已断开: %s", connection_id)
        return ws

    async def _read_ws(self, connection: OneBot12WebSocketConnection) -> None:
        """循环读取 WebSocket 报文，分发事件与动作响应。

        Args:
            connection: WebSocket 连接对象。
        """
        async for msg in connection.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_ws_text(connection, msg.data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                # MessagePack 是规范里的可选实现，这里明确提示而不是静默丢事件
                logger.warning(
                    "[onebot12] 收到 MessagePack 报文（当前仅支持 JSON），已忽略",
                )
            elif msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
            ):
                break

    async def _handle_ws_text(
        self,
        connection: OneBot12WebSocketConnection,
        raw: str,
    ) -> None:
        """解析一段 WebSocket 文本报文。

        Args:
            connection: 来源连接。
            raw: 原始文本。
        """
        if self.debug:
            logger.debug("[onebot12] raw <- %s", raw)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning(
                "[onebot12] 无法解析 WebSocket 报文: %s (%s)",
                raw[:200],
                exc,
            )
            return
        if not isinstance(payload, dict):
            logger.warning("[onebot12] 忽略非对象的报文: %s", dump_compact(payload))
            return
        # 一条 WebSocket 连接上跑三种数据，按各自「必备字段的组合」区分：
        # 动作请求有 action；动作响应有 status + retcode（echo 是请求带了才回）；
        # 其余都是事件。注意不能只看 status——meta.status_update 事件天生带
        # status 字段（规范 interface/meta/events.md），只看它会把这条元事件误判成
        # 动作响应然后丢掉，adapter 里处理它的分支就成了死代码。
        if "action" in payload:
            await self._handle_action_request(connection, payload)
            return
        if "retcode" in payload or "status" in payload:
            if connection.resolve_response(payload):
                return
            # 有 status 但没有 retcode：可能是不带 echo 的响应，也可能就是事件
            if "retcode" not in payload and self._looks_like_event(payload):
                await self.dispatch(payload, connection.connection_id)
                return
            logger.warning(
                "[onebot12] 收到无法匹配的动作响应，已忽略: %s",
                dump_compact(payload),
            )
            return
        await self.dispatch(payload, connection.connection_id)

    @staticmethod
    def _looks_like_event(payload: dict[str, Any]) -> bool:
        """判断一个既没有 ``action`` 也没有 ``retcode`` 的报文是否是事件。

        规范的每个事件都必须有 ``type``（``connect/data-protocol/event.md``），
        而动作响应即使省略 ``echo`` 也不会有 ``type``。

        Args:
            payload: 已解析的报文。

        Returns:
            看起来像事件返回 ``True``。
        """
        return payload.get("type") in ("meta", "message", "notice", "request")

    async def _handle_action_request(
        self,
        connection: OneBot12Connection,
        request: dict[str, Any],
    ) -> None:
        """处理实现端发来的动作请求。

        规范允许实现端在 WebSocket 上反向调用应用端的动作。当前适配器没有需要暴露给
        实现端的动作，按规范返回 ``10002 Unsupported Action``。

        Args:
            connection: 来源连接。
            request: 动作请求 JSON。
        """
        echo = request.get("echo")
        response: dict[str, Any] = {
            "status": "failed",
            "retcode": 10002,
            "data": None,
            "message": "Unsupported Action",
        }
        if isinstance(echo, str):
            response["echo"] = echo
        with contextlib.suppress(Exception):
            # 同一模块内的受保护调用：动作响应就是这条连接上的原始报文
            await connection._deliver(response)  # noqa: SLF001

    # -- 正向 WebSocket ---------------------------------------------------

    async def run_forward_websocket(self, endpoint: OneBot12Endpoint) -> None:
        """连接实现端的 WebSocket 服务并保持重连。

        Args:
            endpoint: 实现端的 WebSocket 地址。
        """
        self._running = True
        delay = 1.0
        while self._running:
            connection: OneBot12WebSocketConnection | None = None
            try:
                connection = await self._connect_forward_once(endpoint)
                started = time.monotonic()
                await self._read_ws(connection)
                # 只有连住了一段时间才算「连上了」，否则退避永远停在 1 秒：
                # 一个接受握手后立刻断开的实现端会变成每秒一次的连接风暴。
                if time.monotonic() - started >= RECONNECT_STABLE_SECONDS:
                    delay = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[onebot12] 正向 WebSocket 连接异常: %s", exc)
            finally:
                if connection is not None:
                    self._unregister(connection.connection_id)
                    with contextlib.suppress(Exception):
                        await connection.close()

            if not self._running:
                break
            logger.info("[onebot12] %.1fs 后重连正向 WebSocket", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    async def _connect_forward_once(
        self,
        endpoint: OneBot12Endpoint,
    ) -> OneBot12WebSocketConnection:
        """建立一次正向 WebSocket 连接。

        Args:
            endpoint: 实现端地址。

        Returns:
            已建立的连接对象。

        Raises:
            Exception: 连接失败时抛出底层异常，由调用方重试。
        """
        session = await self._ensure_session()
        headers = {"Sec-WebSocket-Protocol": "12.astrbot-plugin-onebot12"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        logger.info("[onebot12] 正在连接正向 WebSocket: %s", endpoint.url())
        ws = await session.ws_connect(
            endpoint.url(),
            headers=headers,
            heartbeat=30,
            max_msg_size=0,
        )
        connection_id = self._next_connection_id("ws-forward")
        response = getattr(ws, "_response", None)
        protocol = (
            response.headers.get("Sec-WebSocket-Protocol", "") if response else ""
        )
        connection = OneBot12WebSocketConnection(
            connection_id,
            ws,
            api_timeout=self.api_timeout,
            debug=self.debug,
            client_info=protocol,
        )
        self._register(connection)
        logger.info("[onebot12] 正向 WebSocket 已连接: %s", connection.describe())
        return connection

    # -- HTTP -------------------------------------------------------------

    async def run_http_webhook(
        self,
        *,
        host: str,
        port: int,
        path: str,
        api_endpoint: OneBot12Endpoint | None,
    ) -> None:
        """启动 HTTP Webhook 服务端接收事件推送。

        Args:
            host: 监听地址。
            port: 监听端口。
            path: Webhook 路径。
            api_endpoint: 实现端的 HTTP API 地址，用于发送动作请求；未配置时只能
                接收事件，无法主动发消息。
        """
        self._running = True
        if api_endpoint is not None:
            session = await self._ensure_session()
            self._api_connection = OneBot12HttpConnection(
                "http-api",
                session,
                api_endpoint,
                access_token=self.access_token,
                api_timeout=self.api_timeout,
                debug=self.debug,
            )

        app = web.Application()
        app.router.add_post(path, self._handle_http_webhook)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host, port)
        await site.start()
        logger.info(
            "[onebot12] HTTP Webhook 服务已启动: http://%s:%s%s"
            "（请在 OneBot 12 实现端填写该地址）",
            host,
            port,
            path,
        )
        while self._running:
            await asyncio.sleep(3600)

    async def _handle_http_webhook(self, request: web.Request) -> web.Response:
        """处理实现端的 Webhook 事件推送。

        按规范：``204 No Content`` 表示事件已被接收；``200 OK`` 时可以在响应体里返回
        动作请求列表。这里为了不阻塞事件处理始终返回 ``204``，动作请求走
        :attr:`_api_connection` 的 HTTP 通道，避免把回复塞进 Webhook 响应体。

        Args:
            request: HTTP 请求对象。

        Returns:
            空响应。

        Raises:
            web.HTTPUnauthorized: 鉴权失败。
            web.HTTPBadRequest: 请求体不是合法 JSON。
        """
        if not is_token_valid(
            self.access_token,
            request.headers.get("Authorization"),
            request.query.get("access_token"),
        ):
            logger.warning("[onebot12] HTTP Webhook 鉴权失败，已拒绝请求")
            raise web.HTTPUnauthorized(text="invalid access token")

        try:
            payload = await request.json()
        except Exception as exc:
            logger.warning("[onebot12] Webhook 请求体不是合法 JSON: %s", exc)
            raise web.HTTPBadRequest(text="invalid json") from exc

        # 事件来源没有「连接」这个概念（就是一次 HTTP POST），因此不传连接 ID：
        # 传一个假 ID 会让回复时按它去找连接，找不到就直接失败。传 None 则交给
        # 通信层用 _api_connection 发动作请求。
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    await self.dispatch(item, None)
        elif isinstance(payload, dict):
            await self.dispatch(payload, None)
        return web.Response(status=204)

    async def run_http_poll(
        self,
        endpoint: OneBot12Endpoint,
        *,
        interval: float,
    ) -> None:
        """轮询 ``get_latest_events`` 拉取事件。

        Args:
            endpoint: 实现端的 HTTP API 地址。
            interval: 两次轮询之间的间隔（秒）。
        """
        self._running = True
        session = await self._ensure_session()
        connection = OneBot12HttpConnection(
            "http-poll",
            session,
            endpoint,
            access_token=self.access_token,
            api_timeout=max(self.api_timeout, interval + 10.0),
            debug=self.debug,
        )
        self._api_connection = connection
        self._register(connection)
        logger.info("[onebot12] 已开始轮询事件: %s", endpoint.url())

        while self._running:
            try:
                # timeout 交给实现端长轮询，没事件时它会等到超时再返回空列表
                data = await connection.request(
                    "get_latest_events",
                    {"limit": 0, "timeout": int(interval)},
                )
            except asyncio.CancelledError:
                raise
            except OneBot12ActionError as exc:
                logger.warning("[onebot12] 轮询事件失败: %s", exc)
                await asyncio.sleep(interval)
                continue
            except Exception as exc:
                logger.warning("[onebot12] 轮询事件异常: %s", exc)
                await asyncio.sleep(interval)
                continue

            if isinstance(data, list):
                for event in data:
                    if isinstance(event, dict):
                        await self.dispatch(event, connection.connection_id)
            await asyncio.sleep(interval)
