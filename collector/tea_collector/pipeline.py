"""一次采集运行的编排。

流程：
1. 取得进程间采集锁，恢复历史未完成运行标记；
2. 创建 run，依次抓取 farmers -> tea_plots -> purchase_orders；
   每个资源：HTTP 抓取 -> 逐行解析（坏行进隔离区，不算 HTTP 失败）
   -> 重复 ID 去重/冲突隔离 -> staging 落盘并提交；
3. 三个资源全部成功后统一做跨资源外键校验（级联隔离），写入隔离区；
4. 单事务发布新可见版本（upsert + 源端删除标记）；
5. 任何环节失败：保留 staging 与审计行，已发布版本不动。

HTTP 尝试次数通过包裹 client 统计，供 resource_run.http_attempts 审计。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .errors import CollectorError, EmptyDatasetError
from .httpclient import HttpClient
from .models import (FARMERS, PURCHASE_ORDERS, RESOURCES, TEA_PLOTS,
                     apply_relations, dedupe_batch, parse_resource)
from .storage import RUN_FAILED, RUN_PUBLISHED, Storage

log = logging.getLogger("tea_collector.pipeline")

RESOURCE_PATHS = {
    FARMERS: "/api/farmers",
    TEA_PLOTS: "/api/tea-plots",
    PURCHASE_ORDERS: "/api/purchase-orders",
}


@dataclass
class RunResult:
    run_id: str
    status: str
    version: int | None
    batches: dict
    failure_stage: str | None = None
    failure_message: str | None = None


class AttemptCountingClient:
    """统计每个资源实际 HTTP 尝试次数（含重试）。"""

    def __init__(self, client: HttpClient):
        self.client = client
        self.attempts: dict[str, int] = {}

    def get_list(self, resource: str) -> list:
        # 复用 client 的重试循环；通过临时替换内部睡眠不可行，
        # 这里用包装 _once 计数：每次真正发请求计一次。
        original_once = self.client._once  # noqa: SLF001
        attempts = {"n": 0}

        def counting_once(url_path):  # noqa: ANN001
            attempts["n"] += 1
            return original_once(url_path)

        self.client._once = counting_once  # type: ignore[method-assign]  # noqa: SLF001
        try:
            data = self.client.get_list(RESOURCE_PATHS[resource])
        finally:
            self.client._once = original_once  # type: ignore[method-assign]  # noqa: SLF001
        self.attempts[resource] = attempts["n"]
        return data


class Pipeline:
    def __init__(self, storage: Storage, client: HttpClient):
        self.storage = storage
        self.client = AttemptCountingClient(client)

    def run(self) -> RunResult:
        storage = self.storage
        run_id = storage.start_run(self._base_url())
        log.info("开始采集运行 run_id=%s", run_id)
        batches = {}
        try:
            for resource in RESOURCES:
                storage.heartbeat()
                self._fetch_and_stage(resource, batches)
            storage.heartbeat()
            self._apply_cross_relations(batches)
            total_accepted = sum(len(batch.accepted) for batch in batches.values())
            if total_accepted == 0:
                # 真实后端三个资源均有初始数据；全部为 0 通常意味着响应异常，
                # 拒绝发布以避免把可见版本整体标记删除
                raise EmptyDatasetError(
                    "三个资源解析后无任何可接受记录，拒绝发布；请检查接口响应与隔离区"
                )
            version = storage.publish(batches)
            log.info("运行 %s 发布成功，版本 %d", run_id, version)
            self._log_quarantine_summary(batches)
            return RunResult(run_id, RUN_PUBLISHED, version, batches)
        except CollectorError as exc:
            log.error("运行 %s 失败: %s", run_id, exc)
            storage.finish_run_failed(getattr(exc, "stage", "collect"), str(exc))
            return RunResult(run_id, RUN_FAILED, None, batches,
                             getattr(exc, "stage", "collect"), str(exc))
        except Exception as exc:  # noqa: BLE001 - 任何意外都要落审计并保旧版本
            log.exception("运行 %s 发生未预期错误", run_id)
            storage.finish_run_failed("unexpected", f"{type(exc).__name__}: {exc}")
            return RunResult(run_id, RUN_FAILED, None, batches,
                             "unexpected", str(exc))

    def _fetch_and_stage(self, resource: str, batches: dict) -> None:
        storage = self.storage
        try:
            rows = self.client.get_list(resource)
        except CollectorError as exc:
            storage.mark_resource_failed(resource, str(exc))
            exc.stage = resource  # type: ignore[attr-defined]
            raise
        attempts = self.client.attempts.get(resource, 1)
        batch = parse_resource(resource, rows)
        before = len(batch.accepted)
        dedupe_batch(batch)
        duplicate_dropped = before - len(batch.accepted)
        # 先写隔离区（validate/duplicate 阶段），再 staging，各自独立事务
        storage.write_quarantine(batch.rejected)
        storage.stage_resource(resource, batch)
        storage.mark_resource_fetched(resource, batch, attempts, duplicate_dropped)
        batches[resource] = batch
        log.info("资源 %s 抓取完成：原始 %d 条，接受 %d 条，隔离 %d 条，重复去重 %d 条（HTTP 尝试 %d 次）",
                 resource, batch.raw_count, len(batch.accepted),
                 len(batch.rejected), duplicate_dropped, attempts)

    def _apply_cross_relations(self, batches: dict) -> None:
        """跨资源外键校验；新增的隔离记录补写 quarantine。"""
        before_counts = {
            resource: len(batch.rejected) for resource, batch in batches.items()
        }
        apply_relations(batches)
        new_rejected = []
        for resource, batch in batches.items():
            newly = batch.rejected[before_counts[resource]:]
            new_rejected.extend(newly)
            if newly:
                # 外键级联隔离后，staging 中对应行必须移除，保证 staging 与
                # 最终 accepted 集合一致（发布只读取 batch，但保留一致性）
                self._remove_staged_rows(resource, [int(item.source_id) for item in newly
                                                    if item.source_id.lstrip("-").isdigit()])
        self.storage.write_quarantine(new_rejected)

    def _remove_staged_rows(self, resource: str, source_ids: list[int]) -> None:
        if not source_ids:
            return
        from .storage import RESOURCE_TABLES
        table = RESOURCE_TABLES[resource][0]
        placeholders = ",".join("?" for _ in source_ids)
        with self.storage._transaction() as conn:  # noqa: SLF001
            conn.execute(
                f"DELETE FROM {table} WHERE run_id=? AND source_id IN ({placeholders})",
                (self.storage.run_id, *source_ids),
            )

    def _log_quarantine_summary(self, batches: dict) -> None:
        total = sum(len(batch.rejected) for batch in batches.values())
        if total:
            log.warning("本次运行共隔离 %d 条记录，可用 quarantine 表或 "
                        "`collector show-run` 查询原因", total)

    def _base_url(self) -> str:
        from .logging_utils import redact_url
        # 不记录凭据，仅存档脱敏后的 base URL
        client = self.client.client
        return redact_url(f"{client.scheme}://{client.host}:{client.port}{client.base_path}")
