# 茶叶初制加工厂 API 数据采集工具

独立于现有 Spring Boot / Vue 工程的本地采集工具，**仅使用 Python 3.11 标准库**，
无第三方依赖、无外部服务。每晚从业务后端的三个列表接口全量采集当日数据，
写入本地 SQLite 分析库，并提供审计、隔离区与汇总导出能力。

- 采集接口（与后端真实契约一致，均为无分页全量列表）：
  - `GET /api/farmers`
  - `GET /api/tea-plots`
  - `GET /api/purchase-orders`
- 统一响应包装：`{"ok": true|false, "message": "...", "data": [...]}`
- 工具目录完全独立，不修改后端/前端任何代码。

## 目录结构

```
collector/
├── collector.py              # 命令行入口
├── demo.py                   # 本地 fixture 端到端演示
├── run_tests.sh / run_demo.sh
├── tea_collector/            # 工具包
│   ├── httpclient.py         # 超时、退避重试、Retry-After、响应契约校验
│   ├── models.py             # 实体解析/校验、重复 ID 处理、外键级联隔离
│   ├── storage.py            # SQLite：staging、隔离区、版本发布、采集锁
│   ├── pipeline.py           # 一次采集运行的编排
│   ├── cli.py                # argparse 子命令
│   ├── errors.py             # 异常与退出码
│   ├── jsonutil.py           # 数字保精度 JSON 解析
│   └── logging_utils.py      # 日志脱敏
├── tests/                    # unittest 测试 + 本地 HTTP fixture
└── docs/
    ├── DATA_MODEL.md
    ├── RECOVERY.md
    └── LIMITATIONS.md
```

## 快速开始

```bash
# 运行全部测试（自带本地 HTTP fixture，不需要启动 Java 后端）
bash run_tests.sh

# 端到端演示（同样基于本地 fixture，不触碰真实业务数据）
bash run_demo.sh
```

## 采集命令

```bash
python3 collector.py collect \
  --base-url http://127.0.0.1:8472 \
  --db /data/tea/tea_analysis.db
```

常用参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--base-url` | 必填 | 后端基址；支持 `http://user:pass@host` 内嵌凭据（不写日志） |
| `--db` | 必填 | SQLite 文件路径，目录不存在会自动创建 |
| `--connect-timeout` | 5 | 连接超时（秒） |
| `--read-timeout` | 30 | 读取超时（秒） |
| `--max-attempts` | 5 | 单资源最大 HTTP 尝试次数（含首次） |
| `--backoff-base` | 0.5 | 指数退避起始秒数，上限 `--backoff-max`（默认 30） |
| `--lock-wait` | 30 | 采集锁有界等待秒数 |
| `--auth-token` | 无 | 以 `Authorization: Bearer <token>` 发送，不落日志 |
| `--auth-header` | 无 | 自定义认证头，格式 `"Name: value"` |
| `-v/--verbose` | 关 | DEBUG 日志 |

### HTTP 失败处理

- **可重试**：连接失败/重置、读取超时、HTTP 429 与 5xx；指数退避，次数受
  `--max-attempts` 上限约束；合法 `Retry-After`（delta-seconds 或 HTTP-date）
  优先采用，并有 60s 封顶。
- **不可重试**：其余 4xx（含 `{ok:false}` 业务失败）立即终止。
- **协议错误**：非法 JSON、顶层不是对象、缺 `ok`、`ok=true` 但 `data` 非数组，
  立即终止。
- 任何未成功完成的运行都以**非零退出码**结束，错误信息明确区分业务/网络/协议/锁/发布。

### 退出码

| 码 | 含义 |
|---|---|
| 0 | 采集并发布成功 |
| 1 | 业务失败（`ok:false`）、不可重试 4xx、空数据集拒绝发布 |
| 2 | 网络/服务端错误重试耗尽 |
| 3 | 响应协议错误（非法 JSON、结构不符） |
| 4 | 目标库被另一采集进程锁定，等待超时 |
| 5 | 本地发布事务失败（已回滚） |
| 64 | 命令行参数错误 |

## 查询与导出

```bash
# 按 purchaseDate 汇总有效收购重量与单数（只统计 lifecycle=active）
python3 collector.py report --db /data/tea/tea_analysis.db

# 导出同样口径的 CSV（UTF-8 BOM，Excel 可直接打开）
python3 collector.py export --db /data/tea/tea_analysis.db --out summary.csv

# 采集运行审计
python3 collector.py runs --db /data/tea/tea_analysis.db [--json]
python3 collector.py show-run --db ... --run-id <run_id>

# 隔离区（坏数据、重复冲突、外键失败，均含资源/源 ID/阶段/原因）
python3 collector.py quarantine --db ... [--run-id <run_id>]

# 按保留策略清理失败/中止运行的 staging（无需删库，可复现）
python3 collector.py reap --db ... [--retain-terminal-runs 10]
```

## 安全与日志

- 认证头（Authorization/Cookie 等）值**绝不输出**；
- URL 内嵌的 `user:password` 与 `token/api_key/signature` 等查询参数在日志中脱敏；
- 不读取、不写入、不提交任何 `.env`、真实业务数据、凭据或运行产生的数据库文件
  （见 `.gitignore`）。

## 每晚调度示例（cron）

```cron
30 23 * * * /usr/bin/python3 /opt/tea-collector/collector.py collect \
  --base-url http://127.0.0.1:8472 --db /data/tea/tea_analysis.db \
  >> /var/log/tea-collector/collect.log 2>&1
```

数据模型见 [docs/DATA_MODEL.md](docs/DATA_MODEL.md)，失败恢复与 staging
策略见 [docs/RECOVERY.md](docs/RECOVERY.md)，并发与一致性限制见
[docs/LIMITATIONS.md](docs/LIMITATIONS.md)。
