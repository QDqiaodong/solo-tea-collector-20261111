"""实体记录解析与校验。

严格对照后端真实实体：
- Farmer:        id, name, phone, villageGroup, createdAt, updatedAt
- TeaPlot:       id, farmerId, plotName, areaMu(BigDecimal), altitudeM(Integer),
                 pickingSeason(字典: 春季/夏季/秋季), 时间戳
- PurchaseOrder: id, orderNo, farmerId, plotId, weightKg(BigDecimal),
                 moisturePercent(BigDecimal 0..100), pickedAt(LocalDateTime),
                 purchaser, purchaseDate(LocalDate), 时间戳

关键规则：
- 十进制字段以 Decimal 解析，保留原始标度（JSON 数字以字符串形式读取，禁止先转 float）
- 每条记录的问题抛 RecordError，由编排层写入隔离区；一条坏记录不代表整批 HTTP 失败
- 同一批次重复 source id：内容一致去重，冲突则全部隔离
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .errors import RecordError

FARMERS = "farmers"
TEA_PLOTS = "tea_plots"
PURCHASE_ORDERS = "purchase_orders"
# 采集顺序即依赖顺序：农户 -> 茶园地块 -> 收购单
RESOURCES = (FARMERS, TEA_PLOTS, PURCHASE_ORDERS)

PICKING_SEASONS = ("春季", "夏季", "秋季")


# --------------------------------------------------------------------------- #
# 基础类型解析
# --------------------------------------------------------------------------- #

def parse_id(value: Any, field_name: str) -> int:
    """ID 必须是正整数；布尔值不算整数，字符串数字按严格整数处理。"""
    if isinstance(value, bool):
        raise RecordError("ID 类型非法", f"{field_name}={value!r}")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().lstrip("-").isdigit():
        result = int(value.strip())
    else:
        raise RecordError("ID 类型非法", f"{field_name}={value!r}")
    if result <= 0:
        raise RecordError("ID 必须为正整数", f"{field_name}={result}")
    return result


def parse_required_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RecordError("必填文本缺失或为空", field_name)
    return value.strip()


def parse_decimal(value: Any, field_name: str) -> Decimal:
    """十进制解析：接受 JSON 数字（int，不含 bool）与字符串；拒绝 float 与科学计数法风险值。

    Python 的 json 模块会把 12.50 解析成 float——为避免精度损失，存储层在
    抓取时使用 parse_float=str / parse_int=str，因此正常路径下这里收到的是
    str。若上游仍给出 float（例如测试手工构造），拒绝并要求字符串/整数。
    """
    if isinstance(value, bool):
        raise RecordError("十进制字段类型非法", f"{field_name}={value!r}")
    if isinstance(value, float):
        raise RecordError("十进制字段不允许 float 输入（存在精度损失风险）", field_name)
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise RecordError("十进制字段为空", field_name)
        try:
            result = Decimal(text)
        except InvalidOperation:
            raise RecordError("十进制格式非法", f"{field_name}={value!r}")
        if not result.is_finite():
            raise RecordError("十进制必须为有限数", f"{field_name}={value!r}")
        return result
    raise RecordError("十进制字段类型非法", f"{field_name}={value!r}")


def parse_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise RecordError("整数字段类型非法", f"{field_name}={value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    raise RecordError("整数字段类型非法", f"{field_name}={value!r}")


def parse_datetime(value: Any, field_name: str) -> str:
    """解析 LocalDateTime 序列化值（ISO-8601，如 2026-11-01T06:20:00）。

    返回归一化字符串存储；损坏/缺失值隔离。
    """
    if not isinstance(value, str) or not value.strip():
        raise RecordError("日期时间缺失或类型非法", f"{field_name}={value!r}")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        datetime.fromisoformat(text)
    except ValueError:
        raise RecordError("日期时间格式非法", f"{field_name}={value!r}")
    return value.strip()


def parse_date(value: Any, field_name: str) -> str:
    """解析 LocalDate（yyyy-MM-dd）。"""
    if not isinstance(value, str) or not value.strip():
        raise RecordError("日期缺失或类型非法", f"{field_name}={value!r}")
    try:
        date.fromisoformat(value.strip())
    except ValueError:
        raise RecordError("日期格式非法（应为 yyyy-MM-dd）", f"{field_name}={value!r}")
    return value.strip()


# --------------------------------------------------------------------------- #
# 规范化记录
# --------------------------------------------------------------------------- #

@dataclass
class ParsedRecord:
    resource: str
    source_id: int
    data: dict                 # 规范化后的列值（Decimal/字符串/整数）
    raw_json: str              # 原始 JSON 文本，供 staging 审计
    content_hash: str          # 规范化内容指纹，用于重复 ID 判定


@dataclass
class RejectedRecord:
    resource: str
    source_id: str             # 可能无法解析为 int，原样保存
    reason: str
    detail: str
    raw_json: str
    stage: str                 # validate / duplicate / relation


def _canonical_dump(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_content(obj: Any) -> str:
    import hashlib
    return hashlib.sha256(_canonical_dump(obj).encode("utf-8")).hexdigest()


def parse_farmer(raw: dict, raw_json: str) -> ParsedRecord:
    source_id = parse_id(raw.get("id"), "id")
    data = {
        "source_id": source_id,
        "name": parse_required_str(raw.get("name"), "name"),
        "phone": parse_required_str(raw.get("phone"), "phone"),
        "village_group": parse_required_str(raw.get("villageGroup"), "villageGroup"),
        "created_at": parse_datetime(raw.get("createdAt"), "createdAt"),
        "updated_at": _optional_datetime(raw.get("updatedAt")),
    }
    return ParsedRecord(FARMERS, source_id, data, raw_json, _hash_content(data))


def parse_plot(raw: dict, raw_json: str) -> ParsedRecord:
    source_id = parse_id(raw.get("id"), "id")
    area = parse_decimal(raw.get("areaMu"), "areaMu")
    if area <= 0:
        raise RecordError("面积必须大于 0", f"areaMu={area}")
    altitude = parse_int(raw.get("altitudeM"), "altitudeM")
    if altitude <= 0:
        raise RecordError("海拔必须大于 0", f"altitudeM={altitude}")
    season = raw.get("pickingSeason")
    if not isinstance(season, str) or season not in PICKING_SEASONS:
        raise RecordError("采摘季不在字典范围内", f"pickingSeason={season!r}")
    data = {
        "source_id": source_id,
        "farmer_id": parse_id(raw.get("farmerId"), "farmerId"),
        "plot_name": parse_required_str(raw.get("plotName"), "plotName"),
        "area_mu": str(area),
        "altitude_m": altitude,
        "picking_season": season,
        "created_at": parse_datetime(raw.get("createdAt"), "createdAt"),
        "updated_at": _optional_datetime(raw.get("updatedAt")),
    }
    return ParsedRecord(TEA_PLOTS, source_id, data, raw_json, _hash_content(data))


def parse_purchase(raw: dict, raw_json: str) -> ParsedRecord:
    source_id = parse_id(raw.get("id"), "id")
    weight = parse_decimal(raw.get("weightKg"), "weightKg")
    if weight <= 0:
        raise RecordError("鲜叶重量必须大于 0", f"weightKg={weight}")
    moisture = parse_decimal(raw.get("moisturePercent"), "moisturePercent")
    if moisture < 0 or moisture > Decimal("100"):
        raise RecordError("含水率应在 0 到 100 之间", f"moisturePercent={moisture}")
    picked_at = parse_datetime(raw.get("pickedAt"), "pickedAt")
    purchase_date = parse_date(raw.get("purchaseDate"), "purchaseDate")
    # 业务语义：采摘时间不应晚于收购日期（按日期粒度比较）
    picked_dt = datetime.fromisoformat(picked_at[:-1] + "+00:00" if picked_at.endswith("Z") else picked_at)
    if picked_dt.date().isoformat() > purchase_date:
        raise RecordError("采摘时间晚于收购日期", f"pickedAt={picked_at}, purchaseDate={purchase_date}")
    data = {
        "source_id": source_id,
        "order_no": parse_required_str(raw.get("orderNo"), "orderNo"),
        "farmer_id": parse_id(raw.get("farmerId"), "farmerId"),
        "plot_id": parse_id(raw.get("plotId"), "plotId"),
        "weight_kg": str(weight),
        "moisture_percent": str(moisture),
        "picked_at": picked_at,
        "purchaser": parse_required_str(raw.get("purchaser"), "purchaser"),
        "purchase_date": purchase_date,
        "created_at": parse_datetime(raw.get("createdAt"), "createdAt"),
        "updated_at": _optional_datetime(raw.get("updatedAt")),
    }
    return ParsedRecord(PURCHASE_ORDERS, source_id, data, raw_json, _hash_content(data))


def _optional_datetime(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return parse_datetime(value, "updatedAt")


PARSERS = {
    FARMERS: parse_farmer,
    TEA_PLOTS: parse_plot,
    PURCHASE_ORDERS: parse_purchase,
}


# --------------------------------------------------------------------------- #
# 批次级处理：结构校验、重复 ID 去重/冲突、外键一致性
# --------------------------------------------------------------------------- #

@dataclass
class ResourceBatch:
    resource: str
    accepted: list[ParsedRecord] = field(default_factory=list)
    rejected: list[RejectedRecord] = field(default_factory=list)
    raw_count: int = 0


def _raw_source_id(raw: Any) -> str:
    if isinstance(raw, dict) and "id" in raw:
        return str(raw["id"])
    return "<missing-id>"


def parse_resource(resource: str, rows: Iterable[Any]) -> ResourceBatch:
    """逐行解析；结构非法的行进入隔离区，不影响其他行与整批 HTTP 成功判定。"""
    if resource not in PARSERS:
        raise ValueError(f"未知资源 {resource}")
    import json
    batch = ResourceBatch(resource=resource)
    parser = PARSERS[resource]
    for raw in rows:
        batch.raw_count += 1
        raw_json = _canonical_dump(raw)
        if not isinstance(raw, dict):
            batch.rejected.append(RejectedRecord(
                resource, _raw_source_id(raw), "记录不是 JSON 对象", type(raw).__name__,
                raw_json, "validate"))
            continue
        try:
            parsed = parser(raw, raw_json)
        except RecordError as exc:
            batch.rejected.append(RejectedRecord(
                resource, _raw_source_id(raw), exc.reason, exc.detail, raw_json, "validate"))
            continue
        batch.accepted.append(parsed)
    return batch


def dedupe_batch(batch: ResourceBatch) -> None:
    """同一 source id 出现多次：内容一致 -> 去重保留一条；冲突 -> 全部隔离。"""
    grouped: dict[int, list[ParsedRecord]] = {}
    for record in batch.accepted:
        grouped.setdefault(record.source_id, []).append(record)
    kept: list[ParsedRecord] = []
    for source_id, records in grouped.items():
        if len(records) == 1:
            kept.append(records[0])
            continue
        hashes = {record.content_hash for record in records}
        if len(hashes) == 1:
            kept.append(records[0])  # 内容一致，安全去重
            continue
        for record in records:
            batch.rejected.append(RejectedRecord(
                batch.resource, str(source_id),
                "同一批次重复 ID 内容冲突",
                f"出现 {len(records)} 次且内容不一致",
                record.raw_json, "duplicate"))
    kept.sort(key=lambda item: item.source_id)
    batch.accepted = kept


def apply_relations(batches: dict[str, ResourceBatch]) -> None:
    """按依赖顺序应用外键规则（隔离区规则对所有资源一致）：

    1. tea_plot.farmerId 必须指向本批次被接受的 farmer；
    2. purchase_order.farmerId / plotId 必须指向本批次被接受的对应记录；
    3. plot 必须属于该 farmer；purchase 的 plot 必须属于其 farmer。
    被隔离记录引用的下游记录一并隔离（级联隔离），直到不再产生新隔离。
    """
    farmer_ids = {r.source_id for r in batches[FARMERS].accepted}
    plot_ids = {r.source_id for r in batches[TEA_PLOTS].accepted}

    # plots -> farmers
    plot_batch = batches[TEA_PLOTS]
    surviving_plots = []
    for record in plot_batch.accepted:
        farmer_id = record.data["farmer_id"]
        if farmer_id not in farmer_ids:
            plot_batch.rejected.append(RejectedRecord(
                TEA_PLOTS, str(record.source_id),
                "外键农户不存在或已被隔离", f"farmerId={farmer_id}",
                record.raw_json, "relation"))
            continue
        surviving_plots.append(record)
    plot_batch.accepted = surviving_plots
    plot_ids = {r.source_id for r in surviving_plots}
    plot_owner = {r.source_id: r.data["farmer_id"] for r in surviving_plots}

    # purchases -> farmers / plots
    purchase_batch = batches[PURCHASE_ORDERS]
    surviving = []
    for record in purchase_batch.accepted:
        farmer_id = record.data["farmer_id"]
        plot_id = record.data["plot_id"]
        if farmer_id not in farmer_ids:
            purchase_batch.rejected.append(RejectedRecord(
                PURCHASE_ORDERS, str(record.source_id),
                "外键农户不存在或已被隔离", f"farmerId={farmer_id}",
                record.raw_json, "relation"))
            continue
        if plot_id not in plot_ids:
            purchase_batch.rejected.append(RejectedRecord(
                PURCHASE_ORDERS, str(record.source_id),
                "外键茶园地块不存在或已被隔离", f"plotId={plot_id}",
                record.raw_json, "relation"))
            continue
        if plot_owner[plot_id] != farmer_id:
            purchase_batch.rejected.append(RejectedRecord(
                PURCHASE_ORDERS, str(record.source_id),
                "所选地块不属于该农户",
                f"plotId={plot_id} 属于 farmerId={plot_owner[plot_id]}，记录农户 farmerId={farmer_id}",
                record.raw_json, "relation"))
            continue
        surviving.append(record)
    purchase_batch.accepted = surviving
