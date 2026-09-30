"""测试夹具：把插件目录与 AstrBot 源码目录都放进 ``sys.path``，并隔离运行时数据。

插件内部用的是相对导入（``from .onebot12_message import ...``），因此测试必须以
包的形式导入它，也就是让**插件的父目录**在 ``sys.path`` 上。另外插件依赖
``astrbot.api`` / ``astrbot.core``，所以还需要 AstrBot 源码目录。

隔离说明：AstrBot 的 ``get_astrbot_root()`` 默认取 ``os.getcwd()``，而导入
``astrbot`` 时就会顺手建好 ``<root>/data`` 等目录。直接在插件目录里跑 pytest 会在
插件仓库里长出一个 ``data/``（里面还有 cmd_config.json、data_v4.db、t2i 模板……）。
这里在导入 astrbot 之前把 ``ASTRBOT_ROOT`` 指到一个临时目录，让测试完全不碰仓库。
"""

from __future__ import annotations

import os
import socket
import sys
import tempfile
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
PLUGIN_PARENT = PLUGIN_DIR.parent

ASTRBOT_SRC = PLUGIN_PARENT / "AstrBot"


def _ensure_on_path(path: Path) -> None:
    """把目录加到 ``sys.path`` 最前面（幂等）。

    Args:
        path: 待加入的目录。
    """
    resolved = str(path)
    if path.is_dir() and resolved not in sys.path:
        sys.path.insert(0, resolved)


_ensure_on_path(PLUGIN_PARENT)
_ensure_on_path(ASTRBOT_SRC)

# 必须在 import astrbot 之前设置，之后 get_astrbot_root() 就固定了
os.environ.setdefault(
    "ASTRBOT_ROOT",
    tempfile.mkdtemp(prefix="astrbot-plugin-onebot12-tests-"),
)
os.environ.setdefault("ASTRBOT_TEST_MODE", "true")


def find_free_port() -> int:
    """向系统申请一个当前空闲的 TCP 端口。

    存在极小的竞态窗口（端口在返回后可能被别的进程占用），对测试来说够用。

    Returns:
        空闲端口号。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
