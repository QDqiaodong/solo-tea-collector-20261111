"""日志与脱敏。

硬性要求：任何日志都不得包含认证头、令牌或带凭据的 URL。
统一使用 logging，由 setup_logging 配置；URL 经 redact_url 处理后再输出。
"""

from __future__ import annotations

import logging
from urllib.parse import urlsplit, urlunsplit

# 不允许出现在日志里的请求头（小写匹配）
SENSITIVE_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie", "set-cookie"})


def redact_url(url: str) -> str:
    """移除 URL 中的 user:password 与常见敏感查询参数。"""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparseable-url>"
    hostname = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    safe_query = _redact_query(parts.query)
    netloc = f"{hostname}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, safe_query, parts.fragment))


def _redact_query(query: str) -> str:
    if not query:
        return ""
    kept = []
    for pair in query.split("&"):
        key = pair.split("=", 1)[0].lower()
        if key in {"token", "access_token", "refresh_token", "api_key", "apikey", "secret", "password", "signature", "sig"}:
            kept.append(f"{key}=***REDACTED***")
        else:
            kept.append(pair)
    return "&".join(kept)


def redact_headers(headers) -> dict:
    """只保留非敏感头名；敏感头的值一律不输出。"""
    result = {}
    for key, value in dict(headers).items():
        if key.lower() in SENSITIVE_HEADERS:
            result[key] = "***REDACTED***"
        else:
            result[key] = value
    return result


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    # 避免重复 handler（测试中多次调用）
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
    root.addHandler(handler)
    root.setLevel(level)
