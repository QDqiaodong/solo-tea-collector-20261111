# 数据模型

所有表位于单个 SQLite 文件；启用 WAL 模式与 `busy_timeout`。十进制列一律以
**TEXT 存储规范化 Decimal 字符串**，JSON 抓取阶段即通过 `parse_float=str`
避免任何 float 转换，聚合时才转 `Decimal` 精确求和。

## 审计与运行控制

### collect_run
每次 CLI 采集一行。

| 列 | 说明 |
|---|---|
| run_id | 运行标识（UTC 时间戳 + 随机后缀），也是 staging/隔离区的关联键 |
| started_at / finished_at | UTC ISO-8601 |
| status | `running` / `published` / `failed` / `aborted`（崩溃后由下次运行恢复标记） |
| base_url | 已脱敏的目标地址 |
| failure_stage / failure_message | 失败发生的资源或阶段与原因 |
| published_version | 成功发布后对应的版本号 |

### resource_run
每个 run × 三个资源一行：`status`（pending/fetched/failed）、`http_attempts`
（真实 HTTP 请求次数，含重试）、`raw_count`、`accepted_count`、
`rejected_count`、`duplicate_dropped`、`fetched_at`。

### quarantine（隔离区）
不静默丢弃任何未接受记录：

| 列 | 说明 |
|---|---|
| run_id / resource / source_id | 来源定位（ID 无法解析时原样保存，如 `not-an-id`、`<missing-id>`） |
| stage | `validate`（字段校验）/ `duplicate`（同批重复 ID 冲突）/ `relation`（外键级联） |
| reason / detail | 机器可读原因与上下文（字段名、实际值、引用 ID 等） |
| raw_json | 原始记录 JSON（规范化排序后文本），可追溯 |
| created_at | 入区时间 |

## Staging（发布前暂存）

- `staging_farmer` / `staging_tea_plot` / `staging_purchase_order`
- 主键 `(run_id, source_id)`；每抓取并校验完一个资源即在独立事务中提交。
- staging 与可见表物理分离：发布失败或进程崩溃，旧版本零影响；staging
  默认保留供审计，清理只通过 `reap` 命令按规则执行（见 RECOVERY.md）。

## 已发布版本（可见数据）

- `published_version`：自增版本号，一行对应一次成功发布及当时三资源计数。
- `visible_farmer` / `visible_tea_plot` / `visible_purchase_order`：
  - 主键 `source_id`，重跑天然幂等，不会重复插入；
  - `first_version`（首次出现版本，永不改变）、`current_version`（最近写入版本）；
  - `lifecycle`：`active`（当前有效）/ `deleted`（上次存在、本次全量集合中
    已不存在，含源端删除与本次被隔离两种情形，软删除保留历史行）；
  - `content_hash`：规范化字段内容的 SHA-256，用于同批重复 ID 一致性判定与变更识别；
  - 业务列与后端实体一一对应（蛇形命名）：
    - farmer: name / phone / village_group / 时间戳
    - tea_plot: farmer_id / plot_name / area_mu(TEXT) / altitude_m /
      picking_season（字典 春季/夏季/秋季）/ 时间戳
    - purchase_order: order_no / farmer_id / plot_id / weight_kg(TEXT) /
      moisture_percent(TEXT, 0..100) / picked_at / purchaser /
      purchase_date(yyyy-MM-dd) / 时间戳
- 查询与汇总只统计 `lifecycle='active'`；
  `idx_visible_po_date` 为带条件的日期索引，服务于按日汇总。

### collector_lock
单行咨询锁（`id=1`）：run_id、持有者 PID、获取时间、心跳时间。
配合 WAL + `BEGIN IMMEDIATE` 保证同一 SQLite 文件同一时刻只有一个采集进程
能发布；心跳超过 TTL（默认 120s）的死锁可被新进程强制接管。

## 校验规则摘要（与后端实体对齐）

- ID：正整数（接受 JSON 数字或纯数字字符串，拒绝布尔/负数/非数字）；
- 必填文本：name/phone/villageGroup、plotName、orderNo、purchaser 非空；
- area_mu、weight_kg：有限 Decimal 且 > 0；moisture_percent：0..100；
- altitude_m：正整数；picking_season 必须在字典内；
- purchase_date：`yyyy-MM-dd`；pickedAt/createdAt：ISO-8601 日期时间；
  采摘日期不得晚于收购日期；
- 外键（在同批次 accepted 集合内闭合校验）：
  plot.farmerId → farmer；purchase.farmerId → farmer、purchase.plotId → plot，
  且 plot 必须属于该 farmer；引用的上游被隔离时下游级联隔离；
- 同批重复 source id：内容 hash 一致 → 去重保留一条并计入
  `duplicate_dropped`；内容冲突 → 该 ID 全部记录入隔离区（stage=duplicate）。
