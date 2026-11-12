#!/usr/bin/env bash
# 一键运行采集工具全部测试（Python 标准库，无需安装依赖）。
set -euo pipefail
cd "$(dirname "$0")"
python3 -I -m unittest discover -s tests -v
