#!/usr/bin/env bash
# 本地 fixture 端到端演示：启动模拟后端并完整走一遍 CLI 流程。
set -euo pipefail
cd "$(dirname "$0")"
python3 -I demo.py
