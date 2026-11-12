"""并发与锁测试：同一 SQLite 目标不允许两个采集进程交错发布。"""

import os
import sys

# 兼容 `python3 -I tests/test_lock.py` 直跑（-I 不自动注入脚本目录）
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import tempfile
import threading
import time
import unittest

from _helpers import open_storage
from tea_collector.errors import LockBusyError
from tea_collector.storage import new_run_id


class LockTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "locked.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_second_acquirer_waits_then_fails_with_bound(self):
        first = open_storage(self.db_path)
        first.acquire_lock(new_run_id(), wait_seconds=1)
        try:
            second = open_storage(self.db_path)
            start = time.monotonic()
            with self.assertRaises(LockBusyError):
                second.acquire_lock(new_run_id(), wait_seconds=0.6)
            elapsed = time.monotonic() - start
            self.assertGreaterEqual(elapsed, 0.5)
            self.assertLess(elapsed, 2.0)  # 有界等待，不能无限阻塞
            second.close()
        finally:
            first.release_lock()
            first.close()

    def test_lock_released_then_next_run_proceeds(self):
        first = open_storage(self.db_path)
        first.acquire_lock(new_run_id(), wait_seconds=1)
        first.release_lock()
        first.close()

        second = open_storage(self.db_path)
        second.acquire_lock(new_run_id(), wait_seconds=1)  # 不应抛错
        second.release_lock()
        second.close()

    def test_stale_heartbeat_lock_is_taken_over(self):
        first = open_storage(self.db_path)
        first.acquire_lock(new_run_id(), wait_seconds=1)
        first.close()  # 模拟持有者崩溃，未释放锁
        second = open_storage(self.db_path)
        # TTL 设为负数 -> 立即视为过期，可接管
        second.acquire_lock(new_run_id(), wait_seconds=1,
                            heartbeat_ttl_seconds=-1)
        row = second.conn.execute(
            "SELECT run_id FROM collector_lock WHERE id=1"
        ).fetchone()
        self.assertEqual(row["run_id"], second.run_id)
        second.release_lock()
        second.close()

    def test_concurrent_threads_only_one_holds_lock(self):
        outcomes = []
        barrier = threading.Barrier(4)

        def worker():
            barrier.wait()
            storage = open_storage(self.db_path, busy_timeout_ms=2000)
            try:
                storage.acquire_lock(new_run_id(), wait_seconds=3)
                time.sleep(0.05)
                outcomes.append("acquired")
                storage.release_lock()
            except LockBusyError:
                outcomes.append("busy")
            finally:
                storage.close()

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertEqual(len(outcomes), 4)
        self.assertEqual(outcomes.count("acquired"), 4)  # 串行获得，无人损坏数据


if __name__ == "__main__":
    unittest.main(verbosity=2)
