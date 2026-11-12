#!/usr/bin/env python3
"""采集工具可执行入口：python3 collector collect --base-url ... --db ..."""

import os
import sys

# 兼容 `python3 -I collector.py`（隔离模式不自动注入脚本目录）
_COLLECTOR_DIR = os.path.dirname(os.path.abspath(__file__))
if _COLLECTOR_DIR not in sys.path:
    sys.path.insert(0, _COLLECTOR_DIR)

from tea_collector.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
