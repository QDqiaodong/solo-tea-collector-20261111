"""采集工具自定义异常与退出码。"""

# CLI 退出码
EXIT_OK = 0
EXIT_BUSINESS = 1       # 业务失败（响应 ok=false）或不可重试 4xx
EXIT_NETWORK = 2        # 重试耗尽仍失败的网络/服务端错误
EXIT_PROTOCOL = 3       # 无效 JSON、响应结构不符合契约
EXIT_LOCKED = 4         # 另一个采集进程持有锁，等待超时
EXIT_USAGE = 64         # 命令行参数错误
EXIT_PUBLISH = 5        # 本地数据库发布失败（事务已回滚）


class CollectorError(Exception):
    """采集工具异常基类。"""

    exit_code = EXIT_NETWORK


class BusinessError(CollectorError):
    """服务端返回 {ok:false, message}（业务失败）或不可重试的 4xx。"""

    exit_code = EXIT_BUSINESS


class ProtocolError(CollectorError):
    """无效 JSON、HTTP 包装结构与 {ok,message,data} 契约不符。"""

    exit_code = EXIT_PROTOCOL


class NetworkError(CollectorError):
    """连接/读取超时、5xx、429 等可重试问题在重试耗尽后抛出。"""

    exit_code = EXIT_NETWORK


class LockBusyError(CollectorError):
    """目标库正被另一个采集进程占用，有界等待后仍未取得锁。"""

    exit_code = EXIT_LOCKED


class PublishError(CollectorError):
    """本地发布事务失败（已回滚，已发布版本保持不变）。"""

    exit_code = EXIT_PUBLISH


class EmptyDatasetError(CollectorError):
    """三资源解析后接受记录总数为 0，拒绝发布以防异常响应误删全表。"""

    exit_code = EXIT_BUSINESS


class RecordError(Exception):
    """单条记录校验/关联/重复 ID 冲突问题（进入隔离区，不影响整批 HTTP 成功）。"""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason if not detail else f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail
