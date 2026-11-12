"""茶叶初制加工厂本地采集工具（仅依赖 Python 标准库）。

模块划分：
- httpclient: 带退避重试的 HTTP 采集，识别统一响应包装 {ok, message, data}
- models:     基于真实实体的记录解析与校验（Decimal 保精度）
- storage:    SQLite 存储、staging/隔离区/版本发布/采集锁
- pipeline:   一次采集运行的编排（先 staging，三资源齐全后单事务发布）
- cli:        命令行入口
"""

__version__ = "1.0.0"
