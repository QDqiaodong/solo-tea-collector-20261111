# 失败、Staging 与恢复策略

核心原则：**任何失败都不修改已发布版本**。可见数据只在三个资源全部抓取、
解析、关联校验通过后，由一个 `BEGIN IMMEDIATE` 事务发布；事务回滚或进程
崩溃后，上一版本（首次运行前则为"无版本"）保持完整可查询。

## 一次运行的阶段与失败行为

| 阶段 | 失败后果 | 已发布版本 | staging / 审计 |
|---|---|---|---|
| 获取采集锁 | 有界等待后退出码 4 | 不变 | 无新增 |
| 抓取资源 1/2/3 | 当前 run 标记 `failed`，记录 failure_stage | 不变 | 已完成资源的 staging 与 resource_run 已提交保留 |
| 逐行校验/去重 | 坏行进 quarantine，**不构成** HTTP 失败 | 不变 | 正常落审计 |
| 跨资源外键 | 级联隔离，进 quarantine | 不变 | staging 中对应行同步删除 |
| 发布事务 | 任意 SQL 错误 → 整体 rollback，退出码 5 | 不变 | staging 保留，run 标记 failed |
| 进程被 kill / 断电 | run 停留 `running` | 不变 | staging 保留；锁靠心跳 TTL 接管 |

## 进程中断后的自动恢复（无需操作员删库）

1. 下次 `collect` 取得采集锁后调用 `recover_abandoned_runs()`：
   所有无锁持有者且仍为 `running` 的 run 被标记为 `aborted`
   （failure_stage=`process_exit`），其 pending 资源标记 failed；
2. 崩溃进程遗留在 `collector_lock` 中的锁，在心跳超过 TTL 后由新进程
   强制接管（测试 `test_stale_heartbeat_lock_is_taken_over` 覆盖）；
3. 中断 run 的 staging 与 quarantine **原样保留**，可通过 `show-run` 审计；
4. 新 run 使用全新的 run_id 与独立 staging 分区，重跑天然幂等：
   可见表以 source_id 为主键 upsert，同一源 ID 不会重复插入。

## Staging 的保留与清理（可复现）

- 默认策略：失败/中止运行保留最近 10 个（`reap --retain-terminal-runs N`
  可调），更早的终端 run 连同其 staging、resource_run、quarantine 一起删除；
  `running` 状态的 run 永远不会被清理；
- 已成功发布 run 的 staging 可随时安全清理（可见版本独立保存），`reap`
  会一并处理；
- 清理本身在单个事务内完成，中断后重跑结果一致；
- 任何清理都不需要删除数据库文件；`collect_run`/`published_version` 与
  可见表、被保留 run 的审计链不受影响。

## HTTP 重试与运行结果的关系

- 可重试错误（超时、连接重置、429、5xx）在单资源内按指数退避重试，
  遵守合法 `Retry-After`，尝试次数写入 `resource_run.http_attempts`；
- 重试耗尽 → 该资源 failed、整个 run failed，**不会**用部分数据发布；
- 不可重试 4xx / `ok:false` / 协议错误不重试，立即失败并给出明确原因；
- 单条记录损坏只影响该记录（隔离区），不会被解释成整个 HTTP 响应失败。

## 源端删除的处理

每次发布以本次全量 accepted 集合为准：上一版本 `active` 但本次集合中
不存在的 source_id，在发布事务内统一置为 `deleted` 并更新 current_version
（软删除，历史行仍可查）。因为发布只在完整成功时发生，**失败采集绝不会
误删历史数据**；被删记录若再次出现，下一次成功发布会重新置回 `active`。

## 空数据集保护

若三个资源解析后 accepted 总数为 0（典型于响应异常或全量损坏），拒绝发布，
避免把可见表整体标记删除；run 以失败结束并提示检查接口响应与隔离区。
