"""端到端演示：启动本地 fixture 服务，用真实 CLI 子进程跑完整采集流程。

演示内容：
1. 首次采集并发布；
2. 源端修订 + 删除后再次采集（修订生效、删除标记、汇总变化）；
3. 坏数据隔离与 show-run/quarantine 查询；
4. report / export / runs / reap 命令。

运行：python3 -I demo.py
"""

import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "tests"))

from fixture_server import FixtureServer  # noqa: E402


def run_cli(*args):
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "collector.py"), *args],
        cwd=HERE, capture_output=True, text=True, timeout=120,
    )
    return proc


def section(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main():
    tmp = tempfile.mkdtemp(prefix="tea-collector-demo-")
    db_path = os.path.join(tmp, "tea_analysis.db")
    csv_path = os.path.join(tmp, "summary.csv")
    print(f"临时工作目录: {tmp}（演示结束后可手动删除，不产生仓库内数据库）")

    with FixtureServer() as server:
        base_url = server.base_url
        section("1) 首次采集（本地 fixture，模拟 {ok,message,data} 真实契约）")
        proc = run_cli("collect", "--base-url", base_url, "--db", db_path,
                       "--lock-wait", "5")
        print(proc.stderr.strip())
        assert proc.returncode == 0, proc.stderr

        section("2) 按 purchaseDate 汇总有效收购")
        proc = run_cli("report", "--db", db_path)
        print(proc.stdout.strip())

        section("3) 源端修订（农户改名、1 号单重量 286.50 -> 300.00）并删除 4 号收购单")
        data = server.state.dataset
        data["farmers"][0]["name"] = "陈兴国（修订）"
        data["purchase_orders"][0]["weightKg"] = "300.00"
        data["purchase_orders"].pop(3)
        proc = run_cli("collect", "--base-url", base_url, "--db", db_path,
                       "--lock-wait", "5")
        print(proc.stderr.strip())
        assert proc.returncode == 0, proc.stderr

        proc = run_cli("report", "--db", db_path)
        print(proc.stdout.strip())

        section("4) 注入坏数据：空姓名农户 + 不存在外键地块 + 非法重量，重跑")
        from fixture_server import make_farmer, make_plot
        data["farmers"].append(make_farmer(91, "", "13900000091", "坏数据村组"))
        data["tea_plots"].append(make_plot(92, farmer_id=999))
        proc = run_cli("collect", "--base-url", base_url, "--db", db_path,
                       "--lock-wait", "5")
        print(proc.stderr.strip())
        assert proc.returncode == 0  # 坏记录隔离，不影响整批成功

        proc = run_cli("runs", "--db", db_path, "--json")
        latest_run = json.loads(proc.stdout)[0]["run_id"]
        proc = run_cli("show-run", "--db", db_path, "--run-id", latest_run)
        print(proc.stdout.strip())

        section("5) 隔离区明细（每条坏记录都有资源/源 ID/原因）")
        proc = run_cli("quarantine", "--db", db_path, "--run-id", latest_run)
        print(proc.stdout.strip())

        section("6) 导出 CSV 与清理演示")
        proc = run_cli("export", "--db", db_path, "--out", csv_path)
        print(proc.stderr.strip())
        with open(csv_path, encoding="utf-8-sig") as handle:
            print(handle.read().strip())
        proc = run_cli("reap", "--db", db_path)
        print(proc.stderr.strip())

    section("演示完成")
    print(f"数据库文件位于临时目录: {db_path}")
    print("（本演示不会在仓库内留下任何 .db / .env / 凭据文件）")


if __name__ == "__main__":
    main()
