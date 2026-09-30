"""AstrBot OneBot 12 适配器插件。

插件本体只做两件事：

1. 在导入 ``onebot12_adapter`` 时触发 ``@register_platform_adapter``，把 ``onebot12``
   写进 ``platform_registry``，WebUI「创建机器人 → 消息平台」下拉框里随即出现
   "OneBot 12"（注册发生在 import 期，没有开关能关掉它）；
2. 提供一个只读指令 ``/onebot12 status``，用来在聊天里确认连接状态、当前连接方式和
   实现端版本，省去翻日志。

真正干活的是 ``onebot12_adapter`` / ``onebot12_client`` / ``onebot12_event`` /
``onebot12_message`` 四个模块，分别对应「平台适配器」「通信方式」「事件对象」
「消息段转换」。
"""

from __future__ import annotations

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star

from . import onebot12_adapter  # noqa: F401  导入即注册平台适配器
from .onebot12_adapter import ADAPTER_NAME, DESCRIPTION, DISPLAY_NAME

LOG = "[onebot12]"


class OneBot12Plugin(Star):
    """OneBot 12 适配器插件的 Star 入口。

    Args:
        context: AstrBot 上下文。
    """

    def __init__(self, context: Context) -> None:
        super().__init__(context)
        logger.info("%s 适配器插件已加载，适配器名: %s", LOG, ADAPTER_NAME)

    @filter.command_group("onebot12")
    def onebot12(self) -> None:
        """OneBot 12 适配器辅助指令组。"""

    @onebot12.command("status")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def status(self, event: AstrMessageEvent) -> MessageChain:
        """查看 OneBot 12 适配器的连接状态与实现端信息。

        Args:
            event: 消息事件。

        Returns:
            状态文本。
        """
        instances = [
            platform
            for platform in self.context.platform_manager.platform_insts
            if platform.meta().name == ADAPTER_NAME
        ]
        if not instances:
            return MessageChain(
                [
                    Plain(
                        text=(
                            f"还没有启用任何 {DISPLAY_NAME} 平台。\n"
                            "请在 WebUI「配置 → 消息平台」里新增一个 OneBot 12 机器人并启用。"
                        ),
                    ),
                ],
            )

        lines: list[str] = [f"{DISPLAY_NAME} 适配器共 {len(instances)} 个实例："]
        for platform in instances:
            transport = platform.get_client()
            connections = transport.active_connections()
            meta = platform.meta()
            lines.append(
                f"· {meta.id}｜方式 {transport.mode}｜状态 {platform.status.value}｜"
                f"活动连接 {len(connections)}",
            )
            for connection in connections:
                lines.append(f"    - {connection.describe()}")
            if platform.last_error is not None:
                lines.append(f"    ! 最近错误：{platform.last_error.message}")
            if platform.impl_version:
                version = platform.impl_version
                lines.append(
                    "    实现端："
                    f"{version.get('impl') or 'unknown'} "
                    f"{version.get('version') or ''} "
                    f"(OneBot {version.get('onebot_version') or '?'})",
                )
        return MessageChain([Plain(text="\n".join(lines))])


def get_metadata() -> dict[str, str]:
    """返回适配器注册信息，供测试与排障使用。

    Returns:
        含 ``name`` / ``display_name`` / ``description`` 的字典。
    """
    return {
        "name": ADAPTER_NAME,
        "display_name": DISPLAY_NAME,
        "description": DESCRIPTION,
    }
