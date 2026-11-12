"""HTTP 采集客户端（标准库实现）。

真实后端契约（见 backend/.../common/ApiResponse.java 与 GlobalExceptionHandler.java）：
- 成功：HTTP 200，body 为 {"ok": true, "message": "OK", "data": [...]}
- 业务失败：HTTP 400，body 为 {"ok": false, "message": "...", "data": null}
- 兜底异常：HTTP 500，body 同样为 {ok:false,...}
- 列表接口无分页参数，一次返回全量数组。

重试策略（有上限的指数退避）：
- 可重试：连接失败/重置/读超时、HTTP 429、HTTP 5xx
- 不可重试：其余 4xx（明确业务/参数错误）
- Retry-After（秒数或 HTTP-date）合法时优先采用，并以 max_retry_after 封顶
"""

from __future__ import annotations

import http.client
import logging
import socket
import time
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

from .errors import BusinessError, NetworkError, ProtocolError
from .jsonutil import load_json_preserving_decimals
from .logging_utils import redact_url

log = logging.getLogger("tea_collector.http")

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class RetryPolicy:
    def __init__(self, max_attempts: int = 5, base_delay: float = 0.5,
                 max_delay: float = 30.0, max_retry_after: float = 60.0):
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_retry_after = max_retry_after

    def backoff(self, attempt: int) -> float:
        # attempt 从 1 开始；第 attempt 次失败后的等待
        delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        return delay

    def retry_after_delay(self, raw_value: str) -> float | None:
        if raw_value is None:
            return None
        value = raw_value.strip()
        if not value:
            return None
        # delta-seconds 形式
        try:
            seconds = float(value)
            if seconds < 0:
                return None
            return min(seconds, self.max_retry_after)
        except ValueError:
            pass
        # HTTP-date 形式
        try:
            retry_at = parsedate_to_datetime(value)
            wait = retry_at.timestamp() - time.time()
            if wait < 0:
                return 0.0
            return min(wait, self.max_retry_after)
        except (TypeError, ValueError, OverflowError):
            return None


class HttpClient:
    def __init__(self, base_url: str, connect_timeout: float = 5.0, read_timeout: float = 30.0,
                 retry: RetryPolicy | None = None, auth_header: dict | None = None,
                 sleeper=None):
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"base URL 不合法: {redact_url(base_url)}")
        self.scheme = parsed.scheme
        self.host = parsed.hostname
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.base_path = parsed.path.rstrip("/")
        # 支持 URL 内嵌凭据（仅用于构造 Authorization 头，绝不写日志）
        self._default_headers = {
            "Accept": "application/json",
            "Accept-Charset": "utf-8",
            "User-Agent": "tea-collector/1.0 (+stdlib)",
            "Connection": "close",
        }
        if parsed.username:
            import base64
            cred = f"{parsed.username}:{parsed.password or ''}"
            token = base64.b64encode(cred.encode("utf-8")).decode("ascii")
            self._default_headers["Authorization"] = f"Basic {token}"
        if auth_header:
            for key, value in auth_header.items():
                self._default_headers[key] = value
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.retry = retry or RetryPolicy()
        # 测试可注入睡眠函数，避免真实等待
        self._sleep = sleeper or time.sleep

    def get_list(self, path: str) -> list:
        """GET 一个资源列表，返回 data 数组。任何失败都抛出 CollectorError 子类。"""
        url_path = self.base_path + path
        last_error: Exception | None = None
        for attempt in range(1, self.retry.max_attempts + 1):
            try:
                return self._once(url_path)
            except (socket.timeout, TimeoutError, OSError, http.client.HTTPException) as exc:
                last_error = exc
                if attempt >= self.retry.max_attempts:
                    break
                delay = self.retry.backoff(attempt)
                log.warning("GET %s 第 %d 次尝试网络失败(%s)，%.1fs 后重试",
                            redact_url(self._full_url(url_path)), attempt,
                            type(exc).__name__, delay)
                self._sleep(delay)
            except _RetryableStatus as exc:
                last_error = exc
                if attempt >= self.retry.max_attempts:
                    break
                delay = self._status_delay(exc, attempt)
                log.warning("GET %s 第 %d 次尝试返回 %d，%.1fs 后重试",
                            redact_url(self._full_url(url_path)), attempt, exc.status, delay)
                self._sleep(delay)
            except BusinessError:
                raise
            except ProtocolError:
                raise
        raise NetworkError(f"GET {redact_url(self._full_url(url_path))} 重试 {self.retry.max_attempts} 次后仍失败: {last_error}")

    def _status_delay(self, exc: "_RetryableStatus", attempt: int) -> float:
        header_delay = self.retry.retry_after_delay(exc.retry_after or "")
        if header_delay is not None:
            return header_delay
        return self.retry.backoff(attempt)

    def _full_url(self, url_path: str) -> str:
        return f"{self.scheme}://{self.host}:{self.port}{url_path}"

    def _once(self, url_path: str) -> list:
        if self.scheme == "https":
            conn = http.client.HTTPSConnection(self.host, self.port,
                                               timeout=self.read_timeout)
        else:
            conn = http.client.HTTPConnection(self.host, self.port,
                                              timeout=self.read_timeout)
        # http.client 的 timeout 同时作用于连接与读取；connect_timeout 通过
        # 单独的 socket 连接阶段约束（见下）。
        try:
            conn.connect = self._timed_connect(conn)  # type: ignore[method-assign]
            conn.request("GET", url_path, headers=self._default_headers)
            response = conn.getresponse()
            raw = response.read()
            status = response.status
            retry_after = response.getheader("Retry-After")
            if status in RETRYABLE_STATUS:
                raise _RetryableStatus(status, retry_after)
            if 400 <= status < 500:
                self._raise_non_retryable_client_error(status, raw)
            if status >= 500:  # 理论上已被上面捕获，防御性处理
                raise _RetryableStatus(status, retry_after)
            if status != 200:
                raise ProtocolError(f"非预期 HTTP 状态 {status}（GET {redact_url(self._full_url(url_path))}）")
            return self._parse_envelope(raw, url_path)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - 关闭失败不应掩盖主错误
                pass

    def _timed_connect(self, conn: http.client.HTTPConnection):
        original_connect = conn.connect

        def connect_with_timeout():
            # 连接阶段使用更短的 connect_timeout
            conn.timeout = self.connect_timeout
            try:
                original_connect()
            finally:
                conn.timeout = self.read_timeout

        return connect_with_timeout

    def _raise_non_retryable_client_error(self, status: int, raw: bytes) -> None:
        try:
            body = load_json_preserving_decimals(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise BusinessError(f"HTTP {status} 且响应体不是合法 JSON")
        if isinstance(body, dict) and body.get("ok") is False and isinstance(body.get("message"), str):
            raise BusinessError(f"HTTP {status} 业务失败: {body['message']}")
        raise BusinessError(f"HTTP {status} 客户端错误，响应不符合 {ok:false,message} 契约")

    def _parse_envelope(self, raw: bytes, url_path: str) -> list:
        try:
            # 数字以字符串保留，Decimal 字段不经过 float
            body = load_json_preserving_decimals(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ProtocolError(f"响应不是合法 JSON: {exc}")
        if not isinstance(body, dict):
            raise ProtocolError("响应顶层不是 JSON 对象，不符合 {ok,message,data} 契约")
        if "ok" not in body:
            raise ProtocolError("响应缺少 ok 字段，不符合统一响应包装契约")
        ok = body.get("ok")
        if ok is not True:
            message = body.get("message")
            raise BusinessError(f"服务端返回 ok=false: {message if isinstance(message, str) else '<无 message>'}")
        data = body.get("data")
        if not isinstance(data, list):
            raise ProtocolError("ok=true 但 data 不是数组，不符合列表接口契约")
        return data


class _RetryableStatus(Exception):
    """内部用：可重试的 HTTP 状态。"""

    def __init__(self, status: int, retry_after: str | None):
        super().__init__(f"retryable HTTP {status}")
        self.status = status
        self.retry_after = retry_after
