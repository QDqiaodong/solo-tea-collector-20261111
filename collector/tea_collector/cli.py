"""命令行入口（Python 标准库 argparse）。

用法示例：
  python3 collector collect --base-url http://127.0.0.1:8472 --db ./tea_analysis.db
  python3 collector report --db ./tea_analysis.db
  python3 collector export --db ./tea_analysis.db --out summary.csv
  python3 collector runs --db ./tea_analysis.db
  python3 collector show-run --db ./tea_analysis.db --run-id <id>
  python3 collector quarantine --db ./tea_analysis.db [--run-id <id>]
  python3 collector reap --db ./tea_analysis.db
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys

from .errors import (EXIT_BUSINESS, EXIT_LOCKED, EXIT_NETWORK, EXIT_OK,
                     EXIT_PROTOCOL, EXIT_PUBLISH, EXIT_USAGE, CollectorError,
                     LockBusyError)
from .httpclient import HttpClient, RetryPolicy
from .logging_utils import redact_url, setup_logging
from .pipeline import Pipeline
from .storage import (RESOURCE_FAILED, RESOURCE_FETCHED, RUN_ABORTED,
                      RUN_FAILED, RUN_PUBLISHED, RUN_RUNNING, Storage)

log = logging.getLogger("tea_collector.cli")

RUN_STATUS_TEXT = {
    RUN_RUNNING: "运行中",
    RUN_PUBLISHED: "已发布",
    RUN_FAILED: "失败",
    RUN_ABORTED: "进程中断(已恢复标记)",
}
RESOURCE_STATUS_TEXT = {
    "pending": "待采集",
    RESOURCE_FETCHED: "已采集",
    RESOURCE_FAILED: "失败",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="collector",
        description="茶叶初制加工厂 API 数据采集工具（标准库实现，SQLite 本地分析库）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common_db = argparse.ArgumentParser(add_help=False)
    common_db.add_argument("--db", required=True, help="SQLite 数据库文件路径")

    p_collect = sub.add_parser("collect", help="执行一次全量采集并原子发布")
    p_collect.add_argument("--base-url", required=True,
                           help="业务后端基址，如 http://127.0.0.1:8472（支持 URL 内嵌凭据）")
    p_collect.add_argument("--db", required=True, help="SQLite 数据库文件路径")
    p_collect.add_argument("--connect-timeout", type=float, default=5.0,
                           help="连接超时秒数，默认 5")
    p_collect.add_argument("--read-timeout", type=float, default=30.0,
                           help="读取超时秒数，默认 30")
    p_collect.add_argument("--max-attempts", type=int, default=5,
                           help="单资源最大 HTTP 尝试次数，默认 5")
    p_collect.add_argument("--backoff-base", type=float, default=0.5,
                           help="指数退避起始秒数，默认 0.5")
    p_collect.add_argument("--backoff-max", type=float, default=30.0,
                           help="单次退避上限秒数，默认 30")
    p_collect.add_argument("--lock-wait", type=float, default=30.0,
                           help="采集锁有界等待秒数，默认 30")
    p_collect.add_argument("--auth-token", default=None,
                           help="可选：作为 Authorization 头的令牌（不会写入日志）")
    p_collect.add_argument("--auth-header", default=None,
                           help='可选：自定义认证头，格式 "Name: value"，不写日志')
    p_collect.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    p_collect.set_defaults(func=cmd_collect)

    p_report = sub.add_parser("report", parents=[common_db],
                              help="按 purchaseDate 汇总有效收购重量与单数")
    p_report.set_defaults(func=cmd_report)

    p_export = sub.add_parser("export", parents=[common_db],
                              help="导出收购日汇总 CSV")
    p_export.add_argument("--out", required=True, help="输出 CSV 路径")
    p_export.set_defaults(func=cmd_export)

    p_runs = sub.add_parser("runs", parents=[common_db], help="列出采集运行")
    p_runs.add_argument("--limit", type=int, default=20)
    p_runs.add_argument("--json", action="store_true", help="以 JSON 输出")
    p_runs.set_defaults(func=cmd_runs)

    p_show = sub.add_parser("show-run", parents=[common_db],
                            help="查看某次运行的资源状态与隔离明细")
    p_show.add_argument("--run-id", required=True)
    p_show.set_defaults(func=cmd_show_run)

    p_quar = sub.add_parser("quarantine", parents=[common_db], help="查看隔离区")
    p_quar.add_argument("--run-id", default=None)
    p_quar.set_defaults(func=cmd_quarantine)

    p_reap = sub.add_parser("reap", parents=[common_db],
                            help="按保留策略清理失败/中止运行的 staging（可复现，无需删库）")
    p_reap.add_argument("--retain-terminal-runs", type=int, default=10,
                        help="保留最近 N 个失败/中止运行，默认 10")
    p_reap.set_defaults(func=cmd_reap)

    return parser


def _build_auth_headers(args) -> dict | None:
    headers = None
    if getattr(args, "auth_token", None):
        headers = {"Authorization": f"Bearer {args.auth_token}"}
    if getattr(args, "auth_header", None):
        name, _, value = args.auth_header.partition(":")
        name = name.strip()
        if not name or not value.strip():
            raise ValueError("--auth-header 格式应为 'Name: value'")
        headers = headers or {}
        headers[name] = value.strip()
    return headers


def cmd_collect(args) -> int:
    storage = None
    try:
        auth_headers = _build_auth_headers(args)
    except ValueError as exc:
        log.error("%s", exc)
        return EXIT_USAGE
    try:
        retry = RetryPolicy(max_attempts=args.max_attempts,
                            base_delay=args.backoff_base,
                            max_delay=args.backoff_max)
        client = HttpClient(args.base_url,
                            connect_timeout=args.connect_timeout,
                            read_timeout=args.read_timeout,
                            retry=retry, auth_header=auth_headers)
    except ValueError as exc:
        log.error("参数错误: %s", exc)
        return EXIT_USAGE

    log.info("采集目标 %s，本地库 %s", redact_url(args.base_url), os.path.abspath(args.db))
    try:
        storage = Storage(args.db)
        storage.acquire_lock(run_id=_pending_run_id(storage),
                             wait_seconds=args.lock_wait)
        storage.recover_abandoned_runs()
        _install_signal_handlers(storage)
        pipeline = Pipeline(storage, client)
        result = pipeline.run()
        if result.status == RUN_PUBLISHED:
            counts = storage.visible_counts()
            log.info("当前有效记录：农户 %d、地块 %d、收购单 %d（版本 %d）",
                     counts["farmers"], counts["tea_plots"],
                     counts["purchase_orders"], result.version)
            return EXIT_OK
        log.error("采集失败（阶段 %s）：%s。已发布数据保持不变，staging 已保留待审计",
                  result.failure_stage, result.failure_message)
        return _exit_for_failure(result.failure_stage, result.failure_message)
    except LockBusyError as exc:
        log.error("%s", exc)
        return EXIT_LOCKED
    except CollectorError as exc:
        log.error("采集终止: %s", exc)
        return exc.exit_code
    finally:
        if storage is not None:
            storage.close()


def _pending_run_id(storage: Storage) -> str:
    # run_id 在 start_run 中正式落库；锁只需要一个唯一标识
    from .storage import new_run_id
    return new_run_id()


def _exit_for_failure(stage: str | None, message: str | None) -> int:
    text = (message or "").lower()
    if "业务" in (message or "") or "http 4" in (message or ""):
        return EXIT_BUSINESS
    if "json" in text or "契约" in (message or "") or "结构" in (message or ""):
        return EXIT_PROTOCOL
    if stage == "publish":
        return EXIT_PUBLISH
    return EXIT_NETWORK


def _install_signal_handlers(storage: Storage) -> None:
    def handler(signum, frame):  # noqa: ANN001
        log.warning("收到信号 %s，准备退出：本次运行将保留为未完成，下次运行自动标记 aborted",
                    signum)
        try:
            storage.release_lock()
        finally:
            # 130 = 128 + SIGINT(2)；143 = 128 + SIGTERM(15)
            raise SystemExit(128 + signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError):
            pass  # 非主线程或平台不支持


def cmd_report(args) -> int:
    storage = Storage(args.db)
    try:
        version = storage.current_version()
        if version is None:
            print("尚无已发布的采集版本")
            return EXIT_OK
        print(f"当前发布版本: {version}")
        rows = storage.purchase_summary()
        if not rows:
            print("没有有效收购记录")
            return EXIT_OK
        print(f"{'purchaseDate':<14}{'orders':>8}{'totalWeightKg':>16}")
        for row in rows:
            print(f"{row['purchase_date']:<14}{row['order_count']:>8}"
                  f"{row['total_weight_kg']:>16}")
        return EXIT_OK
    finally:
        storage.close()


def cmd_export(args) -> int:
    storage = Storage(args.db)
    try:
        count = storage.export_purchase_summary_csv(args.out)
        log.info("已导出 %d 个收购日汇总到 %s", count, os.path.abspath(args.out))
        return EXIT_OK
    finally:
        storage.close()


def cmd_runs(args) -> int:
    storage = Storage(args.db)
    try:
        rows = storage.list_runs(limit=args.limit)
        if args.json:
            payload = [dict(row) for row in rows]
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return EXIT_OK
        if not rows:
            print("暂无采集运行")
            return EXIT_OK
        print(f"{'run_id':<30}{'started_at':<26}{'status':<14}{'version':>8}  说明")
        for row in rows:
            note = row["failure_message"] or ""
            print(f"{row['run_id']:<30}{row['started_at']:<26}"
                  f"{RUN_STATUS_TEXT.get(row['status'], row['status']):<14}"
                  f"{str(row['published_version'] or '-'):>8}  {note}")
        return EXIT_OK
    finally:
        storage.close()


def cmd_show_run(args) -> int:
    storage = Storage(args.db)
    try:
        try:
            detail = storage.get_run_detail(args.run_id)
        except KeyError:
            log.error("运行 %s 不存在", args.run_id)
            return EXIT_USAGE
        run = detail["run"]
        print(f"run_id: {run['run_id']}")
        print(f"状态: {RUN_STATUS_TEXT.get(run['status'], run['status'])}")
        print(f"开始: {run['started_at']}  结束: {run['finished_at'] or '-'}")
        if run["failure_stage"]:
            print(f"失败阶段: {run['failure_stage']}  原因: {run['failure_message']}")
        print("\n资源状态:")
        print(f"{'resource':<18}{'status':<10}{'attempts':>10}{'raw':>7}{'accept':>8}{'reject':>8}{'dedup':>7}")
        for row in detail["resources"]:
            print(f"{row['resource']:<18}"
                  f"{RESOURCE_STATUS_TEXT.get(row['status'], row['status']):<10}"
                  f"{str(row['http_attempts'] or '-'):>10}{row['raw_count']:>7}"
                  f"{row['accepted_count']:>8}{row['rejected_count']:>8}"
                  f"{row['duplicate_dropped']:>7}")
        quarantined = detail["quarantine"]
        print(f"\n隔离记录: {len(quarantined)} 条")
        for row in quarantined:
            print(f"  - [{row['resource']}] source_id={row['source_id']} "
                  f"stage={row['stage']} 原因={row['reason']} {row['detail']}")
        return EXIT_OK
    finally:
        storage.close()


def cmd_quarantine(args) -> int:
    storage = Storage(args.db)
    try:
        rows = storage.list_quarantine(args.run_id)
        if not rows:
            print("隔离区为空")
            return EXIT_OK
        print(f"{'resource':<18}{'source_id':>10}  {'stage':<10}原因 / 明细")
        for row in rows:
            print(f"{row['resource']:<18}{row['source_id']:>10}  {row['stage']:<10}"
                  f"{row['reason']} {row['detail']}")
        return EXIT_OK
    finally:
        storage.close()


def cmd_reap(args) -> int:
    storage = Storage(args.db)
    try:
        deleted = storage.reap(retain_terminal_runs=args.retain_terminal_runs)
        log.info("清理完成: %s", deleted)
        return EXIT_OK
    finally:
        storage.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(getattr(args, "verbose", False))
    try:
        return args.func(args)
    except CollectorError as exc:
        log.error("%s", exc)
        return exc.exit_code
    except FileNotFoundError as exc:
        log.error("文件不存在: %s", exc)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
