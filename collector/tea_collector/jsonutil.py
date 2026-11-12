"""JSON 解析工具：数字保留为字符串，确保 Decimal 路径不经过 float。"""

from __future__ import annotations

import json
from typing import Any


def load_json_preserving_decimals(raw: bytes | str) -> Any:
    """解析 JSON，整数/浮点字面量一律以字符串交付给上层校验。

    - 布尔值 true/false 不受影响（parse_int 只作用于整数字面量）；
    - NaN/Infinity 等常量变成字符串，随后会被十进制校验拒绝；
    - 上层 models.parse_id / parse_decimal 明确接受字符串数字。
    """
    return json.loads(raw, parse_float=str, parse_int=str, parse_constant=str)
