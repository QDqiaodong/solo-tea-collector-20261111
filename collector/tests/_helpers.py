"""测试公共工具。"""

import os
import sys

COLLECTOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
for path in (COLLECTOR_DIR, TESTS_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from tea_collector.httpclient import HttpClient, RetryPolicy  # noqa: E402
from tea_collector.storage import Storage  # noqa: E402


def fast_client(base_url, max_attempts=5, sleeper=None):
    """退避接近 0 的客户端，重试逻辑真实执行但测试不等待。"""
    retry = RetryPolicy(max_attempts=max_attempts, base_delay=0.01,
                        max_delay=0.05, max_retry_after=0.1)
    return HttpClient(base_url, connect_timeout=3, read_timeout=5,
                      retry=retry, sleeper=sleeper)


def open_storage(path, busy_timeout_ms=5000):
    return Storage(path, busy_timeout_ms=busy_timeout_ms)
