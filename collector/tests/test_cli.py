"""CLI 子进程测试、日志脱敏测试与 HTTP 客户端单元测试。"""

import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import unittest

# 兼容 `python3 -I tests/test_cli.py` 直跑（-I 不自动注入脚本目录）
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _helpers import fast_client
from fixture_server import FixtureServer
from tea_collector.cli import main
from tea_collector.errors import (BusinessError, NetworkError, ProtocolError)
from tea_collector.httpclient import RetryPolicy
from tea_collector.logging_utils import redact_url

COLLECTOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class CliTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "cli.db")
        self.server = FixtureServer().__enter__()

    def tearDown(self):
        self.server.stop()
        self.tmp.cleanup()

    def _run_cli(self, *args):
        return subprocess.run(
            [sys.executable, os.path.join(COLLECTOR_DIR, "collector.py"), *args],
            cwd=COLLECTOR_DIR, capture_output=True, text=True, timeout=60,
        )

    def test_collect_report_export_runs_show_reap_via_cli(self):
        proc = self._run_cli("collect", "--base-url", self.server.base_url,
                             "--db", self.db_path,
                             "--max-attempts", "2", "--lock-wait", "5")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("发布成功", proc.stderr)

        proc = self._run_cli("report", "--db", self.db_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("2026-11-01", proc.stdout)
        self.assertIn("484.50", proc.stdout)

        csv_path = os.path.join(self.tmp.name, "summary.csv")
        proc = self._run_cli("export", "--db", self.db_path, "--out", csv_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(csv_path, encoding="utf-8-sig") as handle:
            content = handle.read()
        self.assertIn("purchase_date,order_count,total_weight_kg", content)
        self.assertIn("2026-11-01,2,484.50", content)

        proc = self._run_cli("runs", "--db", self.db_path, "--json")
        self.assertEqual(proc.returncode, 0)
        run_id = json.loads(proc.stdout)[0]["run_id"]

        proc = self._run_cli("show-run", "--db", self.db_path,
                             "--run-id", run_id)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("farmers", proc.stdout)

        proc = self._run_cli("quarantine", "--db", self.db_path)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("隔离区为空", proc.stdout)

        proc = self._run_cli("reap", "--db", self.db_path)
        self.assertEqual(proc.returncode, 0)

    def test_cli_nonzero_exit_on_business_failure(self):
        self.server.state.set_failures("farmers", [("400-business",)])
        proc = self._run_cli("collect", "--base-url", self.server.base_url,
                             "--db", self.db_path, "--max-attempts", "2")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("业务失败", proc.stderr)

    def test_cli_nonzero_exit_on_retry_exhausted(self):
        self.server.state.set_failures("tea_plots", [("503",)] * 5)
        proc = self._run_cli("collect", "--base-url", self.server.base_url,
                             "--db", self.db_path, "--max-attempts", "2",
                             "--backoff-base", "0.01")
        self.assertEqual(proc.returncode, 2)  # EXIT_NETWORK
        self.assertIn("重试", proc.stderr)

    def test_cli_lock_conflict_exit_code(self):
        from _helpers import open_storage
        from tea_collector.storage import new_run_id
        holder = open_storage(self.db_path)
        holder.acquire_lock(new_run_id(), wait_seconds=2)
        try:
            proc = self._run_cli("collect", "--base-url", self.server.base_url,
                                 "--db", self.db_path, "--lock-wait", "0.3",
                                 "--max-attempts", "1")
            self.assertEqual(proc.returncode, 4)  # EXIT_LOCKED
            self.assertIn("另一个采集进程", proc.stderr)
        finally:
            holder.release_lock()
            holder.close()

    def test_credentials_never_logged(self):
        # URL 内嵌凭据 + token 头
        url = self.server.base_url.replace("http://", "http://user:secret@")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger = logging.getLogger("tea_collector")
        logger.addHandler(handler)
        try:
            code = main(["collect", "--base-url", url, "--db", self.db_path,
                         "--auth-token", "tok-secret-123",
                         "--max-attempts", "1", "--lock-wait", "5"])
        finally:
            logger.removeHandler(handler)
        self.assertEqual(code, 0)
        output = stream.getvalue()
        self.assertNotIn("secret", output)
        self.assertNotIn("tok-secret-123", output)
        self.assertNotIn("user:secret", output)
        # 脱敏后的主机仍在
        self.assertIn("127.0.0.1", output)
        # 认证头确实被发送到服务端（值存在于请求中，只是不进日志）
        sent_headers = self.server.state.config["farmers"]["received_headers"][0]
        self.assertTrue(any(k.lower() == "authorization" for k in sent_headers))


class HttpUnitTestCase(unittest.TestCase):
    def test_redact_url(self):
        self.assertEqual(
            redact_url("http://user:p%40ss@host:8472/api/farmers?token=abc&x=1"),
            "http://host:8472/api/farmers?token=***REDACTED***&x=1")
        self.assertEqual(redact_url("http://host/api"), "http://host/api")

    def test_retry_after_parsing(self):
        policy = RetryPolicy()
        self.assertEqual(policy.retry_after_delay("2"), 2.0)
        self.assertEqual(policy.retry_after_delay("0"), 0.0)
        self.assertIsNone(policy.retry_after_delay(""))
        self.assertIsNone(policy.retry_after_delay("not-a-date"))
        # 非法大值被封顶
        self.assertEqual(policy.retry_after_delay("999999"), policy.max_retry_after)
        # HTTP-date 形式：未来时间得到正等待秒数，过去时间归零
        from email.utils import formatdate
        future = formatdate(time.time() + 1, usegmt=True)
        delay = policy.retry_after_delay(future)
        self.assertIsNotNone(delay)
        self.assertGreaterEqual(delay, 0.0)
        self.assertLessEqual(delay, 1.0)
        past = formatdate(time.time() - 10, usegmt=True)
        self.assertEqual(policy.retry_after_delay(past), 0.0)

    def test_invalid_base_url_rejected(self):
        with self.assertRaises(ValueError):
            fast_client("not-a-url")

    def test_envelope_contract_violations(self):
        with FixtureServer() as server:
            server.state.set_failures("farmers", [("ok-false-200",)])
            client = fast_client(server.base_url, max_attempts=1)
            with self.assertRaises(BusinessError):
                client.get_list("/api/farmers")

            server.state.set_failures("farmers", [("bad-structure",)])
            with self.assertRaises(ProtocolError):
                client.get_list("/api/farmers")

            server.state.set_failures("farmers", [("404",)])
            with self.assertRaises(BusinessError):
                client.get_list("/api/farmers")

            server.state.set_failures("farmers", [("drop-connection",)])
            with self.assertRaises(NetworkError):
                client.get_list("/api/farmers")


if __name__ == "__main__":
    unittest.main(verbosity=2)
