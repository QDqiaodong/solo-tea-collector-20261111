"""SQLite 存储层。

设计要点
========
1. 采集状态与质量审计
   - collect_run：每次运行一行（run_id、起止时间、状态、各资源计数）
   - resource_run：每个资源的抓取/校验统计与状态
   - quarantine：隔离区，记录 resource/source_id/reason/detail/raw_json/run_id/stage

2. Staging 与原子发布
   - staging_* 表按 run_id 暂存已接受的记录；抓取一个资源就提交一次，
     进程中途退出后数据仍在，便于审计（恢复策略见 docs/RECOVERY.md）。
   - 三个资源全部抓取并解析成功后，在 *一个* 事务里把新版本发布到
     visible_* 表；事务回滚或进程崩溃都不会改动任何已发布版本。
   - visible_* 以 source_id 为主键，带 current_version / 首次/末次版本与
     生命周期状态（active / deleted），重跑幂等，源端删除可被显式标记。
   - published_version 记录每次成功发布的版本，查询默认只看 active 行。

3. 并发
   - collector_lock 表实现进程间咨询锁（心跳 + 过期接管），同一 SQLite
     目标同一时刻只允许一个采集进程；锁等待有界，超时明确退出。
   - 数据库启用 WAL 与 busy_timeout，读写互不阻塞；发布使用 BEGIN IMMEDIATE。

十进制列一律以 TEXT 存储（Canonical Decimal 字符串），聚合时再转 Decimal，
全程不经过 float。
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from .errors import LockBusyError, PublishError
from .models import (FARMERS, PURCHASE_ORDERS, TEA_PLOTS, RejectedRecord,
                     ResourceBatch)

log = logging.getLogger("tea_collector.storage")

RUN_RUNNING = "running"
RUN_PUBLISHED = "published"
RUN_FAILED = "failed"
RUN_ABORTED = "aborted"

RESOURCE_PENDING = "pending"
RESOURCE_FETCHED = "fetched"          # 已抓取并通过解析/校验，staging 已提交
RESOURCE_FAILED = "failed"

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collect_run (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    base_url TEXT NOT NULL,
    failure_stage TEXT,
    failure_message TEXT,
    published_version INTEGER
);

CREATE TABLE IF NOT EXISTS resource_run (
    run_id TEXT NOT NULL,
    resource TEXT NOT NULL,
    status TEXT NOT NULL,
    http_attempts INTEGER,
    raw_count INTEGER NOT NULL DEFAULT 0,
    accepted_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    duplicate_dropped INTEGER NOT NULL DEFAULT 0,
    fetched_at TEXT,
    PRIMARY KEY (run_id, resource)
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    resource TEXT NOT NULL,
    source_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_farmer (
    run_id TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    village_group TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    PRIMARY KEY (run_id, source_id)
);

CREATE TABLE IF NOT EXISTS staging_tea_plot (
    run_id TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    farmer_id INTEGER NOT NULL,
    plot_name TEXT NOT NULL,
    area_mu TEXT NOT NULL,
    altitude_m INTEGER NOT NULL,
    picking_season TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    PRIMARY KEY (run_id, source_id)
);

CREATE TABLE IF NOT EXISTS staging_purchase_order (
    run_id TEXT NOT NULL,
    source_id INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    order_no TEXT NOT NULL,
    farmer_id INTEGER NOT NULL,
    plot_id INTEGER NOT NULL,
    weight_kg TEXT NOT NULL,
    moisture_percent TEXT NOT NULL,
    picked_at TEXT NOT NULL,
    purchaser TEXT NOT NULL,
    purchase_date TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    PRIMARY KEY (run_id, source_id)
);

CREATE TABLE IF NOT EXISTS published_version (
    version INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    published_at TEXT NOT NULL,
    farmer_count INTEGER NOT NULL,
    plot_count INTEGER NOT NULL,
    purchase_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS visible_farmer (
    source_id INTEGER PRIMARY KEY,
    current_version INTEGER NOT NULL,
    first_version INTEGER NOT NULL,
    lifecycle TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    village_group TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS visible_tea_plot (
    source_id INTEGER PRIMARY KEY,
    current_version INTEGER NOT NULL,
    first_version INTEGER NOT NULL,
    lifecycle TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    farmer_id INTEGER NOT NULL,
    plot_name TEXT NOT NULL,
    area_mu TEXT NOT NULL,
    altitude_m INTEGER NOT NULL,
    picking_season TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS visible_purchase_order (
    source_id INTEGER PRIMARY KEY,
    current_version INTEGER NOT NULL,
    first_version INTEGER NOT NULL,
    lifecycle TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    order_no TEXT NOT NULL,
    farmer_id INTEGER NOT NULL,
    plot_id INTEGER NOT NULL,
    weight_kg TEXT NOT NULL,
    moisture_percent TEXT NOT NULL,
    picked_at TEXT NOT NULL,
    purchaser TEXT NOT NULL,
    purchase_date TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_visible_po_date
    ON visible_purchase_order(purchase_date) WHERE lifecycle = 'active';
CREATE INDEX IF NOT EXISTS idx_quarantine_run ON quarantine(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_farmer_run ON staging_farmer(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_plot_run ON staging_tea_plot(run_id);
CREATE INDEX IF NOT EXISTS idx_staging_po_run ON staging_purchase_order(run_id);

CREATE TABLE IF NOT EXISTS collector_lock (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    run_id TEXT NOT NULL,
    holder_pid INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);
"""

RESOURCE_TABLES = {
    FARMERS: ("staging_farmer", "visible_farmer"),
    TEA_PLOTS: ("staging_tea_plot", "visible_tea_plot"),
    PURCHASE_ORDERS: ("staging_purchase_order", "visible_purchase_order"),
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:12]


class Storage:
    """每个采集进程持有一个 Storage 实例；连接不跨线程共享。"""

    def __init__(self, db_path: str, busy_timeout_ms: int = 30000):
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms
        self._lock = threading.RLock()
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(db_path, timeout=busy_timeout_ms / 1000.0,
                                    isolation_level=None)  # autocommit，事务显式管理
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self._initialize()
        self.run_id: str | None = None
        self._held_lock = False

    # ------------------------------------------------------------------ #
    # 初始化 / 恢复
    # ------------------------------------------------------------------ #
    def _initialize(self) -> None:
        # PRAGMA / DDL 在 autocommit 模式执行，避免 executescript 与显式事务冲突
        conn = self.conn
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(SCHEMA_SQL)
        with self._transaction() as tx:
            row = tx.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                tx.execute(
                    "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(row["value"]) != SCHEMA_VERSION:
                raise PublishError(
                    f"数据库 schema 版本 {row['value']} 与工具版本 {SCHEMA_VERSION} 不兼容"
                )

    def recover_abandoned_runs(self) -> list[str]:
        """把没有锁持有者、仍处于 running 的历史运行标记为 aborted。

        必须在拿到采集锁之后调用，避免与正在运行的进程竞争。
        """
        recovered = []
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT run_id FROM collect_run WHERE status=?", (RUN_RUNNING,)
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE collect_run SET status=?, finished_at=?, "
                    "failure_stage='process_exit', failure_message=? WHERE run_id=?",
                    (RUN_ABORTED, utc_now_iso(),
                     "进程退出或崩溃后由下一次运行恢复标记", row["run_id"]),
                )
                conn.execute(
                    "UPDATE resource_run SET status=? WHERE run_id=? AND status=?",
                    (RESOURCE_FAILED, row["run_id"], RESOURCE_PENDING),
                )
                recovered.append(row["run_id"])
        if recovered:
            log.warning("已将 %d 个未完成运行标记为 aborted: %s",
                        len(recovered), ", ".join(recovered))
        return recovered

    def close(self) -> None:
        with self._lock:
            try:
                if self._held_lock:
                    self.release_lock()
            finally:
                self.conn.close()

    @contextmanager
    def _transaction(self):
        """显式立即事务：失败即回滚，已发布数据不受影响。"""
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.execute("COMMIT")

    # ------------------------------------------------------------------ #
    # 采集锁（进程间互斥，有界等待，心跳过期可接管）
    # ------------------------------------------------------------------ #
    def acquire_lock(self, run_id: str, wait_seconds: float = 30.0,
                     heartbeat_ttl_seconds: float = 120.0,
                     sleeper=None) -> None:
        import time
        sleep = sleeper or time.sleep
        deadline = time.monotonic() + max(0.0, wait_seconds)
        while True:
            with self._transaction() as conn:
                row = conn.execute(
                    "SELECT run_id, holder_pid, heartbeat_at FROM collector_lock WHERE id=1"
                ).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO collector_lock(id, run_id, holder_pid, "
                        "acquired_at, heartbeat_at) VALUES(1, ?, ?, ?, ?)",
                        (run_id, os.getpid(), utc_now_iso(), utc_now_iso()),
                    )
                    self.run_id = run_id
                    self._held_lock = True
                    return
                if self._lock_expired(row["heartbeat_at"], heartbeat_ttl_seconds):
                    log.warning("采集锁持有者 run=%s 心跳过期，强制接管", row["run_id"])
                    conn.execute(
                        "UPDATE collector_lock SET run_id=?, holder_pid=?, "
                        "acquired_at=?, heartbeat_at=? WHERE id=1",
                        (run_id, os.getpid(), utc_now_iso(), utc_now_iso()),
                    )
                    self.run_id = run_id
                    self._held_lock = True
                    return
            if time.monotonic() >= deadline:
                raise LockBusyError(
                    f"目标数据库正被另一个采集进程占用（持有 run={row['run_id']}），"
                    f"等待 {wait_seconds}s 后仍未获得锁；本次退出，未改动任何数据"
                )
            sleep(min(0.5, max(0.0, deadline - time.monotonic())))

    @staticmethod
    def _lock_expired(heartbeat_at: str, ttl_seconds: float) -> bool:
        try:
            text = heartbeat_at[:-1] + "+00:00" if heartbeat_at.endswith("Z") else heartbeat_at
            beat = datetime.fromisoformat(text)
            age = (datetime.now(timezone.utc) - beat).total_seconds()
            return age > ttl_seconds
        except ValueError:
            return True

    def heartbeat(self) -> None:
        if not self._held_lock or not self.run_id:
            return
        with self._transaction() as conn:
            conn.execute(
                "UPDATE collector_lock SET heartbeat_at=? WHERE id=1 AND run_id=?",
                (utc_now_iso(), self.run_id),
            )

    def release_lock(self) -> None:
        if not self._held_lock or not self.run_id:
            return
        with self._transaction() as conn:
            conn.execute("DELETE FROM collector_lock WHERE id=1 AND run_id=?",
                         (self.run_id,))
        self._held_lock = False

    # ------------------------------------------------------------------ #
    # run / resource 审计
    # ------------------------------------------------------------------ #
    def start_run(self, base_url: str) -> str:
        run_id = self.run_id or new_run_id()
        self.run_id = run_id
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO collect_run(run_id, started_at, status, base_url) "
                "VALUES(?, ?, ?, ?)",
                (run_id, utc_now_iso(), RUN_RUNNING, base_url),
            )
            for resource in (FARMERS, TEA_PLOTS, PURCHASE_ORDERS):
                conn.execute(
                    "INSERT INTO resource_run(run_id, resource, status, http_attempts, "
                    "raw_count, accepted_count, rejected_count, duplicate_dropped) "
                    "VALUES(?, ?, ?, 0, 0, 0, 0, 0)",
                    (run_id, resource, RESOURCE_PENDING),
                )
        return run_id

    def mark_resource_fetched(self, resource: str, batch: ResourceBatch,
                              http_attempts: int, duplicate_dropped: int) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE resource_run SET status=?, http_attempts=?, raw_count=?, "
                "accepted_count=?, rejected_count=?, duplicate_dropped=?, fetched_at=? "
                "WHERE run_id=? AND resource=?",
                (RESOURCE_FETCHED, http_attempts, batch.raw_count,
                 len(batch.accepted), len(batch.rejected), duplicate_dropped,
                 utc_now_iso(), self.run_id, resource),
            )

    def mark_resource_failed(self, resource: str, message: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE resource_run SET status=? WHERE run_id=? AND resource=?",
                (RESOURCE_FAILED, self.run_id, resource),
            )
            conn.execute(
                "UPDATE collect_run SET status=?, finished_at=?, failure_stage=?, "
                "failure_message=? WHERE run_id=?",
                (RUN_FAILED, utc_now_iso(), resource, message, self.run_id),
            )

    def finish_run_failed(self, stage: str, message: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE collect_run SET status=?, finished_at=?, failure_stage=?, "
                "failure_message=? WHERE run_id=?",
                (RUN_FAILED, utc_now_iso(), stage, message, self.run_id),
            )

    # ------------------------------------------------------------------ #
    # staging 与隔离区
    # ------------------------------------------------------------------ #
    def stage_resource(self, resource: str, batch: ResourceBatch) -> None:
        staging_table = RESOURCE_TABLES[resource][0]
        columns = {
            FARMERS: ("source_id", "content_hash", "raw_json", "name", "phone",
                      "village_group", "created_at", "updated_at"),
            TEA_PLOTS: ("source_id", "content_hash", "raw_json", "farmer_id",
                        "plot_name", "area_mu", "altitude_m", "picking_season",
                        "created_at", "updated_at"),
            PURCHASE_ORDERS: ("source_id", "content_hash", "raw_json", "order_no",
                              "farmer_id", "plot_id", "weight_kg",
                              "moisture_percent", "picked_at", "purchaser",
                              "purchase_date", "created_at", "updated_at"),
        }[resource]
        placeholders = ",".join("?" for _ in columns)
        sql = (f"INSERT INTO {staging_table}(run_id, {','.join(columns)}) "
               f"VALUES(?,{placeholders})")
        with self._transaction() as conn:
            for record in batch.accepted:
                values = [self.run_id]
                for column in columns:
                    if column == "source_id":
                        values.append(record.source_id)
                    elif column == "content_hash":
                        values.append(record.content_hash)
                    elif column == "raw_json":
                        values.append(record.raw_json)
                    else:
                        values.append(record.data[column])
                conn.execute(sql, values)

    def write_quarantine(self, rejected: Iterable[RejectedRecord]) -> None:
        rows = list(rejected)
        if not rows:
            return
        with self._transaction() as conn:
            for item in rows:
                conn.execute(
                    "INSERT INTO quarantine(run_id, resource, source_id, stage, "
                    "reason, detail, raw_json, created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (self.run_id, item.resource, item.source_id, item.stage,
                     item.reason, item.detail, item.raw_json, utc_now_iso()),
                )

    # ------------------------------------------------------------------ #
    # 原子发布
    # ------------------------------------------------------------------ #
    def publish(self, batches: dict[str, ResourceBatch]) -> int:
        """三资源齐全后在单事务内发布新可见版本，返回版本号。"""
        for resource in (FARMERS, TEA_PLOTS, PURCHASE_ORDERS):
            if resource not in batches:
                raise PublishError(f"资源 {resource} 未完成采集，拒绝发布")
        try:
            with self._transaction() as conn:
                cur = conn.execute(
                    "INSERT INTO published_version(run_id, published_at, "
                    "farmer_count, plot_count, purchase_count) VALUES(?,?,?,?,?)",
                    (self.run_id, utc_now_iso(),
                     len(batches[FARMERS].accepted),
                     len(batches[TEA_PLOTS].accepted),
                     len(batches[PURCHASE_ORDERS].accepted)),
                )
                version = int(cur.lastrowid)
                self._publish_resource(conn, FARMERS, batches[FARMERS], version)
                self._publish_resource(conn, TEA_PLOTS, batches[TEA_PLOTS], version)
                self._publish_resource(conn, PURCHASE_ORDERS,
                                       batches[PURCHASE_ORDERS], version)
                conn.execute(
                    "UPDATE collect_run SET status=?, finished_at=?, "
                    "published_version=? WHERE run_id=?",
                    (RUN_PUBLISHED, utc_now_iso(), version, self.run_id),
                )
            return version
        except sqlite3.Error as exc:
            raise PublishError(f"发布事务失败并已回滚: {exc}") from exc

    def _publish_resource(self, conn: sqlite3.Connection, resource: str,
                          batch: ResourceBatch, version: int) -> None:
        _, visible_table = RESOURCE_TABLES[resource]
        data_columns = {
            FARMERS: ("name", "phone", "village_group", "created_at", "updated_at"),
            TEA_PLOTS: ("farmer_id", "plot_name", "area_mu", "altitude_m",
                        "picking_season", "created_at", "updated_at"),
            PURCHASE_ORDERS: ("order_no", "farmer_id", "plot_id", "weight_kg",
                              "moisture_percent", "picked_at", "purchaser",
                              "purchase_date", "created_at", "updated_at"),
        }[resource]
        accepted_ids = {record.source_id for record in batch.accepted}
        by_id = {record.source_id: record for record in batch.accepted}

        # 1) upsert 本版本接受的记录
        all_columns = ("source_id", "current_version", "first_version",
                       "lifecycle", "content_hash", *data_columns)
        placeholders = ",".join("?" for _ in all_columns)
        update_assignments = ",".join(
            f"{column}=excluded.{column}" for column in all_columns
            if column not in ("source_id", "first_version")
        )
        upsert = (
            f"INSERT INTO {visible_table}({','.join(all_columns)}) "
            f"VALUES({placeholders}) ON CONFLICT(source_id) DO UPDATE SET "
            f"{update_assignments}"
        )
        for record in batch.accepted:
            values = [
                record.source_id, version, version, "active", record.content_hash
            ]
            values.extend(record.data[column] for column in data_columns)
            conn.execute(upsert, values)

        # 2) 上次 active 但本次全量集合已不存在（源端删除或本次被隔离）：标记 deleted
        if accepted_ids:
            placeholders_in = ",".join("?" for _ in accepted_ids)
            conn.execute(
                f"UPDATE {visible_table} SET lifecycle='deleted', "
                f"current_version=? WHERE lifecycle='active' AND source_id NOT IN "
                f"({placeholders_in})",
                (version, *sorted(accepted_ids)),
            )
        else:
            conn.execute(
                f"UPDATE {visible_table} SET lifecycle='deleted', current_version=? "
                f"WHERE lifecycle='active'",
                (version,),
            )

        # 3) 之前被删除、本次重新出现的记录已由 upsert 重新置为 active
        _ = by_id  # 仅用于显式表达按 source_id 全量对齐

    # ------------------------------------------------------------------ #
    # 查询 / 导出
    # ------------------------------------------------------------------ #
    def current_version(self) -> int | None:
        row = self.conn.execute(
            "SELECT MAX(version) AS v FROM published_version"
        ).fetchone()
        return row["v"] if row and row["v"] is not None else None

    def visible_counts(self) -> dict[str, int]:
        result = {}
        for resource, (_, visible_table) in RESOURCE_TABLES.items():
            row = self.conn.execute(
                f"SELECT COUNT(*) AS c FROM {visible_table} WHERE lifecycle='active'"
            ).fetchone()
            result[resource] = row["c"]
        return result

    def purchase_summary(self) -> list[dict[str, Any]]:
        """按 purchaseDate 汇总有效收购重量（Decimal 求和）与收购单数。"""
        rows = self.conn.execute(
            "SELECT purchase_date, weight_kg FROM visible_purchase_order "
            "WHERE lifecycle='active' ORDER BY purchase_date"
        ).fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            day = row["purchase_date"]
            entry = grouped.setdefault(day, {"purchase_date": day, "order_count": 0,
                                             "total_weight_kg": Decimal("0")})
            entry["order_count"] += 1
            entry["total_weight_kg"] += Decimal(row["weight_kg"])
        return [
            {"purchase_date": day,
             "order_count": entry["order_count"],
             "total_weight_kg": format(entry["total_weight_kg"], "f")}
            for day, entry in sorted(grouped.items())
        ]

    def export_purchase_summary_csv(self, path: str) -> int:
        import csv
        summary = self.purchase_summary()
        with open(path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["purchase_date", "order_count", "total_weight_kg"])
            for entry in summary:
                writer.writerow([entry["purchase_date"], entry["order_count"],
                                 entry["total_weight_kg"]])
        return len(summary)

    def list_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM collect_run ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()

    def get_run_detail(self, run_id: str) -> dict[str, Any]:
        run = self.conn.execute(
            "SELECT * FROM collect_run WHERE run_id=?", (run_id,)
        ).fetchone()
        if run is None:
            raise KeyError(run_id)
        resources = self.conn.execute(
            "SELECT * FROM resource_run WHERE run_id=? ORDER BY resource", (run_id,)
        ).fetchall()
        quarantined = self.conn.execute(
            "SELECT resource, source_id, stage, reason, detail FROM quarantine "
            "WHERE run_id=? ORDER BY id", (run_id,)
        ).fetchall()
        return {"run": run, "resources": resources, "quarantine": quarantined}

    def list_quarantine(self, run_id: str | None = None) -> list[sqlite3.Row]:
        if run_id:
            return self.conn.execute(
                "SELECT * FROM quarantine WHERE run_id=? ORDER BY id", (run_id,)
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM quarantine ORDER BY id DESC LIMIT 200"
        ).fetchall()

    # ------------------------------------------------------------------ #
    # 维护：失败 staging 的保留与可复现清理
    # ------------------------------------------------------------------ #
    def reap(self, retain_terminal_runs: int = 10) -> dict[str, int]:
        """清理失败/中止运行的 staging 与隔离区数据。

        策略（详见 docs/RECOVERY.md）：
        - 已发布运行的 staging 可清理（可见版本已独立保存）；
        - 失败/中止运行默认保留最近 retain_terminal_runs 个用于审计，
          更早的连同其 staging、resource_run、quarantine 一并删除；
        - running 状态的运行绝不清理。
        返回各表删除行数。
        """
        deleted = {"staging_farmer": 0, "staging_tea_plot": 0,
                   "staging_purchase_order": 0, "quarantine": 0,
                   "resource_run": 0, "collect_run": 0}
        with self._transaction() as conn:
            terminal = conn.execute(
                "SELECT run_id, status FROM collect_run WHERE status IN (?, ?) "
                "ORDER BY started_at DESC", (RUN_FAILED, RUN_ABORTED)
            ).fetchall()
            stale_ids = [row["run_id"] for row in terminal[retain_terminal_runs:]]
            # 已发布运行的 staging 也可安全清理
            published_ids = [row["run_id"] for row in conn.execute(
                "SELECT run_id FROM published_version").fetchall()]
            cleanup_ids = set(stale_ids) | set(published_ids)
            for run_id in cleanup_ids:
                for table in ("staging_farmer", "staging_tea_plot",
                              "staging_purchase_order"):
                    cur = conn.execute(
                        f"DELETE FROM {table} WHERE run_id=?", (run_id,))
                    deleted[table] += cur.rowcount
            for run_id in stale_ids:
                cur = conn.execute("DELETE FROM quarantine WHERE run_id=?", (run_id,))
                deleted["quarantine"] += cur.rowcount
                cur = conn.execute("DELETE FROM resource_run WHERE run_id=?", (run_id,))
                deleted["resource_run"] += cur.rowcount
                cur = conn.execute("DELETE FROM collect_run WHERE run_id=?", (run_id,))
                deleted["collect_run"] += cur.rowcount
        return deleted

    def fetch_attempts(self, run_id: str, resource: str) -> int:
        row = self.conn.execute(
            "SELECT http_attempts FROM resource_run WHERE run_id=? AND resource=?",
            (run_id, resource),
        ).fetchone()
        return int(row["http_attempts"] or 0) if row else 0
