"""端到端采集流水线测试（本地 fixture，覆盖核心业务场景）。"""

import os
import sys

# 兼容 `python3 -I tests/test_pipeline.py` 直跑（-I 不自动注入脚本目录）
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import tempfile
import unittest
from decimal import Decimal

from _helpers import fast_client, open_storage
from fixture_server import (FixtureServer, make_farmer,
                                  make_plot, make_purchase)
from tea_collector.models import FARMERS, PURCHASE_ORDERS, TEA_PLOTS
from tea_collector.pipeline import Pipeline
from tea_collector.storage import (RUN_ABORTED, RUN_FAILED, RUN_PUBLISHED,
                                   RUN_RUNNING, new_run_id)


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "analysis.db")
        self.server = FixtureServer().__enter__()

    def tearDown(self):
        self.server.stop()
        self.tmp.cleanup()

    def _run_collect(self, max_attempts=5):
        """执行一次采集；连接保持打开，由调用方在用完后 close。"""
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        storage.recover_abandoned_runs()
        pipeline = Pipeline(storage, fast_client(self.server.base_url,
                                                 max_attempts=max_attempts))
        result = pipeline.run()
        storage.release_lock()
        return storage, result

    # ------------------------------------------------------------------ #
    def test_first_success_publishes_visible_version(self):
        storage, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)
        self.assertEqual(result.version, 1)
        counts = storage.visible_counts()
        self.assertEqual(counts, {"farmers": 5, "tea_plots": 5,
                                  "purchase_orders": 5})
        # 审计行完整
        detail = storage.get_run_detail(result.run_id)
        self.assertEqual([r["status"] for r in detail["resources"]],
                         ["fetched", "fetched", "fetched"])
        self.assertTrue(all(r["http_attempts"] == 1 for r in detail["resources"]))
        # 汇总按日期正确
        summary = storage.purchase_summary()
        by_day = {row["purchase_date"]: row for row in summary}
        self.assertEqual(by_day["2026-11-01"]["order_count"], 2)
        self.assertEqual(by_day["2026-11-01"]["total_weight_kg"], "484.50")
        self.assertEqual(by_day["2026-11-02"]["total_weight_kg"], "542.00")
        self.assertEqual(by_day["2026-11-03"]["order_count"], 1)
        storage.close()

    def test_rerun_is_idempotent_no_duplicate_rows(self):
        storage, first = self._run_collect()
        storage.close()
        storage, second = self._run_collect()
        self.assertEqual(second.status, RUN_PUBLISHED)
        self.assertEqual(second.version, 2)
        self.assertEqual(storage.visible_counts(),
                         {"farmers": 5, "tea_plots": 5, "purchase_orders": 5})
        # 版本号前进但行数不翻倍；first_version 保留为 1
        row = storage.conn.execute(
            "SELECT first_version, current_version, lifecycle FROM visible_farmer "
            "WHERE source_id=1"
        ).fetchone()
        self.assertEqual((row["first_version"], row["current_version"],
                          row["lifecycle"]), (1, 2, "active"))
        storage.close()

    def test_revision_is_published_and_summary_updates(self):
        self._run_collect()
        # 源端修订：农户改名、收购单重量调整
        data = self.server.state.dataset
        data["farmers"][0]["name"] = "陈兴国（修订）"
        data["purchase_orders"][0]["weightKg"] = "300.00"
        storage, result = self._run_collect()
        self.assertEqual(result.version, 2)
        row = storage.conn.execute(
            "SELECT name FROM visible_farmer WHERE source_id=1"
        ).fetchone()
        self.assertEqual(row["name"], "陈兴国（修订）")
        weight = storage.conn.execute(
            "SELECT weight_kg FROM visible_purchase_order WHERE source_id=1"
        ).fetchone()["weight_kg"]
        self.assertEqual(weight, "300.00")
        summary = {r["purchase_date"]: r for r in storage.purchase_summary()}
        # 286.50 -> 300.00，当日合计 498.00
        self.assertEqual(summary["2026-11-01"]["total_weight_kg"], "498.00")
        storage.close()

    def test_source_delete_marks_lifecycle_deleted_and_keeps_history(self):
        self._run_collect()
        data = self.server.state.dataset
        removed = data["purchase_orders"].pop(3)  # 删除 source_id=4
        storage, result = self._run_collect()
        self.assertEqual(result.version, 2)
        row = storage.conn.execute(
            "SELECT lifecycle, current_version FROM visible_purchase_order WHERE source_id=4"
        ).fetchone()
        self.assertEqual(row["lifecycle"], "deleted")
        self.assertEqual(row["current_version"], 2)
        # 汇总不再包含该单
        summary = {r["purchase_date"]: r for r in storage.purchase_summary()}
        self.assertEqual(summary["2026-11-02"]["order_count"], 1)
        # 历史行仍可查询（软删除，不物理抹除）
        self.assertEqual(
            storage.conn.execute(
                "SELECT COUNT(*) c FROM visible_purchase_order WHERE source_id=4"
            ).fetchone()["c"], 1)
        # 被删记录重新出现 -> 恢复 active
        data["purchase_orders"].append(removed)
        storage2, result2 = self._run_collect()
        self.assertEqual(result2.version, 3)
        row = storage2.conn.execute(
            "SELECT lifecycle FROM visible_purchase_order WHERE source_id=4"
        ).fetchone()
        self.assertEqual(row["lifecycle"], "active")
        storage.close()
        storage2.close()

    def test_bad_records_are_quarantined_not_treated_as_http_failure(self):
        data = self.server.state.dataset
        data["farmers"].append({"id": 91, "name": "", "phone": "13900000091",
                                "villageGroup": "坏数据村组",
                                "createdAt": "2026-11-01T08:00:00"})  # 姓名空
        data["farmers"].append({"id": "not-an-id", "name": "无名氏",
                                "phone": "13900000092", "villageGroup": "村组",
                                "createdAt": "2026-11-01T08:00:00"})  # ID 坏
        data["tea_plots"].append(make_plot(92, farmer_id=999))  # 外键农户不存在
        data["purchase_orders"].append(
            make_purchase(93, "CG-BAD-1", 1, 1, weight="0",
                          moisture="74.00"))  # 重量必须 > 0
        data["purchase_orders"].append(
            make_purchase(94, "CG-BAD-2", 1, 1, moisture="120"))  # 含水率越界
        data["purchase_orders"].append(
            make_purchase(95, "CG-BAD-3", 1, 1,
                          picked="2026-11-10T06:00:00",
                          date="2026-11-01"))  # 采摘晚于收购
        data["purchase_orders"].append("not-an-object")  # 结构性坏行
        storage, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)  # HTTP 全成功
        quarantined = storage.list_quarantine(result.run_id)
        reasons = {(q["resource"], q["source_id"]): q["reason"] for q in quarantined}
        self.assertIn(("farmers", "91"), reasons)
        self.assertIn(("farmers", "not-an-id"), reasons)
        self.assertIn(("tea_plots", "92"), reasons)
        self.assertEqual(reasons[("tea_plots", "92")], "外键农户不存在或已被隔离")
        for pid in ("93", "94", "95"):
            self.assertIn(("purchase_orders", pid), reasons)
        self.assertIn(("purchase_orders", "<missing-id>"), reasons)
        # 好数据照常可见
        counts = storage.visible_counts()
        self.assertEqual(counts["farmers"], 5)
        self.assertEqual(counts["tea_plots"], 5)
        self.assertEqual(counts["purchase_orders"], 5)
        storage.close()

    def test_duplicate_ids_consistent_dedup_conflict_quarantine(self):
        data = self.server.state.dataset
        # 完全一致的重复农户 -> 去重
        data["farmers"].append(make_farmer(5, "赵德安", "13870010005", "石坎村四组"))
        # 同 ID 不同内容 -> 冲突，两条都隔离
        good_po = make_purchase(5, "CG20261103001", 5, 5, "342.60", "74.60",
                                "2026-11-03T06:15:00", "刘海", "2026-11-03")
        conflict_po = dict(good_po)
        conflict_po["weightKg"] = "999.99"
        data["purchase_orders"].append(conflict_po)
        storage, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)
        detail = storage.get_run_detail(result.run_id)
        stats = {r["resource"]: r for r in detail["resources"]}
        self.assertEqual(stats["farmers"]["duplicate_dropped"], 1)
        self.assertEqual(stats["farmers"]["accepted_count"], 5)
        po_quarantine = [q for q in storage.list_quarantine(result.run_id)
                         if q["resource"] == "purchase_orders"]
        self.assertEqual(len(po_quarantine), 2)
        self.assertTrue(all("重复 ID 内容冲突" in q["reason"] for q in po_quarantine))
        # 冲突 ID 不进入可见表（首次发布即隔离）
        self.assertIsNone(storage.conn.execute(
            "SELECT 1 FROM visible_purchase_order WHERE source_id=5"
        ).fetchone())
        storage.close()

    def test_mid_run_failure_keeps_previously_published_version(self):
        storage1, first = self._run_collect()
        storage1.close()
        # 第三个资源持续 500，前两个资源成功 staging
        self.server.state.set_failures("purchase_orders",
                                       [("500",)] * 6)
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            pipeline = Pipeline(storage, fast_client(self.server.base_url,
                                                     max_attempts=3))
            result = pipeline.run()
            self.assertEqual(result.status, RUN_FAILED)
            failed_run = result.run_id
            # 前两个资源已落 staging
            self.assertEqual(storage.conn.execute(
                "SELECT COUNT(*) c FROM staging_farmer WHERE run_id=?",
                (failed_run,)).fetchone()["c"], 5)
            self.assertEqual(storage.conn.execute(
                "SELECT COUNT(*) c FROM staging_tea_plot WHERE run_id=?",
                (failed_run,)).fetchone()["c"], 5)
            # 未产生新版本，旧版本完整可查
            self.assertEqual(storage.current_version(), 1)
            self.assertEqual(storage.visible_counts(),
                             {"farmers": 5, "tea_plots": 5, "purchase_orders": 5})
            row = storage.conn.execute(
                "SELECT status, failure_stage FROM collect_run WHERE run_id=?",
                (failed_run,)).fetchone()
            self.assertEqual(row["status"], RUN_FAILED)
            self.assertEqual(row["failure_stage"], "purchase_orders")
        finally:
            storage.release_lock()
            storage.close()

        # 服务恢复后重跑 -> 成功发布，证明失败 staging 不影响幂等重跑
        self.server.state.set_failures("purchase_orders", [])
        storage2, result2 = self._run_collect()
        self.assertEqual(result2.status, RUN_PUBLISHED)
        self.assertEqual(storage2.current_version(), 2)
        storage2.close()

    def test_retry_then_success_and_retry_exhausted(self):
        # 前两次 503，第三次成功
        self.server.state.set_failures("farmers", [("503",), ("503",)])
        storage, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)
        self.assertEqual(self.server.state.config["farmers"]["calls"], 3)
        storage.close()

        # 重试耗尽 -> 非零失败，旧数据不动
        self.server.state.set_failures("tea_plots", [("500",)] * 5)
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            pipeline = Pipeline(storage, fast_client(self.server.base_url,
                                                     max_attempts=3))
            result = pipeline.run()
            self.assertEqual(result.status, RUN_FAILED)
            self.assertEqual(storage.current_version(), 1)
        finally:
            storage.release_lock()
            storage.close()

    def test_retry_after_header_is_respected(self):
        sleeps = []
        self.server.state.set_failures(
            "farmers",
            [("429", {"headers": {"Retry-After": "0.05"}}), ("503",)])
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            client = fast_client(self.server.base_url, max_attempts=4,
                                 sleeper=sleeps.append)
            result = Pipeline(storage, client).run()
            self.assertEqual(result.status, RUN_PUBLISHED)
            # 第一次等待来自 Retry-After（受 max_retry_after=0.1 约束）
            self.assertAlmostEqual(sleeps[0], 0.05, places=3)
        finally:
            storage.release_lock()
            storage.close()

    def test_business_4xx_and_protocol_errors_fail_fast(self):
        # 不可重试 4xx：立即失败，不重试
        self.server.state.set_failures("farmers", [("400-business",)])
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            pipeline = Pipeline(storage, fast_client(self.server.base_url,
                                                     max_attempts=4))
            result = pipeline.run()
            self.assertEqual(result.status, RUN_FAILED)
            self.assertIn("业务失败", result.failure_message)
            self.assertEqual(self.server.state.config["farmers"]["calls"], 1)
        finally:
            storage.release_lock()
            storage.close()

        # 非法 JSON -> 协议错误
        self.server.state.set_failures("farmers", [("bad-json",)])
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            result = Pipeline(storage, fast_client(self.server.base_url)).run()
            self.assertEqual(result.status, RUN_FAILED)
            self.assertIn("JSON", result.failure_message)
        finally:
            storage.release_lock()
            storage.close()

        # 结构错误：data 不是数组
        self.server.state.set_failures("farmers", [("data-not-array",)])
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            result = Pipeline(storage, fast_client(self.server.base_url)).run()
            self.assertEqual(result.status, RUN_FAILED)
            self.assertIn("data 不是数组", result.failure_message)
        finally:
            storage.release_lock()
            storage.close()

    def test_aborted_run_recovery_and_stale_staging(self):
        # 模拟一次运行在抓取两个资源后进程崩溃：手工构造 running + staging
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        run_id = storage.start_run(self.server.base_url)
        from tea_collector.models import parse_resource
        rows = self.server.state.dataset["farmers"]
        batch = parse_resource(FARMERS, rows)
        storage.stage_resource(FARMERS, batch)
        storage.mark_resource_fetched(FARMERS, batch, 1, 0)
        storage.release_lock()
        storage.close()  # 未 finish：run 保持 running

        # 新进程启动 -> 恢复标记 aborted，旧 staging 保留可审计，再正常发布
        storage2, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)
        aborted = storage2.conn.execute(
            "SELECT status FROM collect_run WHERE run_id=?", (run_id,)
        ).fetchone()["status"]
        self.assertEqual(aborted, RUN_ABORTED)
        # 旧 staging 仍在（保留策略），新 run 独立
        self.assertEqual(storage2.conn.execute(
            "SELECT COUNT(*) c FROM staging_farmer WHERE run_id=?", (run_id,)
        ).fetchone()["c"], 5)
        storage2.close()

    def test_decimal_precision_preserved_end_to_end(self):
        data = self.server.state.dataset
        # 高精度、尾零、长小数
        data["tea_plots"][0]["areaMu"] = "12.50000000"
        data["purchase_orders"][0]["weightKg"] = "286.55555555"
        data["purchase_orders"][0]["moisturePercent"] = "74.20"
        storage, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)
        self.assertEqual(storage.conn.execute(
            "SELECT area_mu FROM visible_tea_plot WHERE source_id=1"
        ).fetchone()["area_mu"], "12.50000000")
        self.assertEqual(storage.conn.execute(
            "SELECT weight_kg, moisture_percent FROM visible_purchase_order "
            "WHERE source_id=1"
        ).fetchone()["weight_kg"], "286.55555555")
        # 聚合仍是精确十进制
        summary = {r["purchase_date"]: r for r in storage.purchase_summary()}
        expected = Decimal("286.55555555") + Decimal("198.00")
        self.assertEqual(summary["2026-11-01"]["total_weight_kg"],
                         format(expected, "f"))
        storage.close()

    def test_all_rejected_dataset_refuses_publish(self):
        # 所有农户 ID 损坏 -> 农户零接受；地块/收购单外键全部级联失败
        for farmer in self.server.state.dataset["farmers"]:
            farmer["id"] = "bad-id"
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        try:
            storage.recover_abandoned_runs()
            result = Pipeline(storage, fast_client(self.server.base_url)).run()
            self.assertEqual(result.status, RUN_FAILED)
            self.assertIsNone(storage.current_version())  # 从未发布
        finally:
            storage.release_lock()
            storage.close()

    def test_quarantine_cascade_when_farmer_removed(self):
        # 农户 2 消失：其地块 2 与引用地块 2 的收购单 2 应级联隔离
        data = self.server.state.dataset
        data["farmers"] = [f for f in data["farmers"] if f["id"] != 2]
        storage, result = self._run_collect()
        self.assertEqual(result.status, RUN_PUBLISHED)
        q = storage.list_quarantine(result.run_id)
        ids = {(item["resource"], item["source_id"]) for item in q}
        self.assertIn((TEA_PLOTS, "2"), ids)
        self.assertIn((PURCHASE_ORDERS, "2"), ids)
        counts = storage.visible_counts()
        self.assertEqual(counts["farmers"], 4)
        self.assertEqual(counts["tea_plots"], 4)
        self.assertEqual(counts["purchase_orders"], 4)
        storage.close()

    def test_publish_transaction_rollback_keeps_prior_version(self):
        # 先成功发布版本 1
        storage, first = self._run_collect()
        self.assertEqual(first.version, 1)
        storage.close()

        # 注入发布期故障：发布事务中途报错，验证整事务回滚
        storage = open_storage(self.db_path)
        storage.acquire_lock(new_run_id(), wait_seconds=5)
        storage.recover_abandoned_runs()
        pipeline = Pipeline(storage, fast_client(self.server.base_url))
        original_publish = storage.publish
        rollback_seen = []

        def failing_publish(batches):
            try:
                # 让真实发布先执行到一半（insert published_version 之后再炸）
                from tea_collector import storage as storage_mod
                conn = storage.conn
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO published_version(run_id, published_at, "
                    "farmer_count, plot_count, purchase_count) "
                    "VALUES(?,?,1,1,1)",
                    (storage.run_id, storage_mod.utc_now_iso()))
                raise RuntimeError("模拟发布期故障")
            except RuntimeError:
                conn.rollback()
                rollback_seen.append(True)
                raise

        storage.publish = failing_publish  # type: ignore[method-assign]
        result = pipeline.run()
        self.assertEqual(result.status, RUN_FAILED)
        self.assertTrue(rollback_seen)
        storage.publish = original_publish  # type: ignore[method-assign]

        # 无新版本产生，没有残留半成品，版本 1 数据完整可查
        self.assertEqual(storage.current_version(), 1)
        self.assertEqual(storage.visible_counts(),
                         {"farmers": 5, "tea_plots": 5, "purchase_orders": 5})
        self.assertEqual(storage.conn.execute(
            "SELECT COUNT(*) c FROM published_version"
        ).fetchone()["c"], 1)
        storage.release_lock()
        storage.close()

        # 之后重跑仍能正常发布，证明回滚后状态干净
        storage2, result2 = self._run_collect()
        self.assertEqual(result2.status, RUN_PUBLISHED)
        self.assertEqual(storage2.current_version(), 2)
        storage2.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
