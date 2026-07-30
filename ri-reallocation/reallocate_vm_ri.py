#!/usr/bin/env python3
"""生成 RI 费用重新分摊后的明细副本，不修改源 CSV。

分摊规则：
1. RI 使用记录：pricingModel=Reservation、chargeType=Usage、reservationId
   等于命令行指定的 RI。
2. 只处理 meterCategory=Virtual Machines 的记录。
3. 原 RI 使用记录的 tags 和 ResourceId 保持不变。
4. 原 RI 使用记录不是目标项目时：
     allocatedCostInBillingCurrency = costInBillingCurrency + RI 使用金额
5. 目标项目中没有实际使用 RI 的 VM 记录按原费用比例扣减 RI 使用金额：
     allocatedCostInBillingCurrency = costInBillingCurrency - 分摊金额

这样 RI 使用记录的 allocatedCost 大于原始 cost，目标项目非 RI 记录的
allocatedCost 小于原始 cost，整体金额保持不变。
"""

from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="生成 RI 费用重新分摊后的 CSV 明细副本。"
    )
    parser.add_argument("inputs", nargs="+", help="源 CSV 文件或 glob")
    parser.add_argument(
        "--output-dir",
        default="ri-reallocated",
        help="输出目录，默认：ri-reallocated",
    )
    parser.add_argument(
        "--reservation-id",
        action="append",
        default=None,
        help=(
            "要重新分摊优惠收益的 reservationId，可重复指定；"
            "不使用 --mapping-file 时必填，所有 RI 共用 --target-tag 目标"
        ),
    )
    parser.add_argument(
        "--target-tag",
        default=None,
        help=(
            "接收优惠收益的标签，格式为 key=value，例如 projname=fota；"
            "不使用 --mapping-file 时必填"
        ),
    )
    parser.add_argument(
        "--mapping-file",
        default=None,
        help=(
            "RI→分摊目标映射文件（JSON 或 CSV）。提供后不再使用 "
            "--reservation-id/--target-tag，可为不同 RI 指定不同目标；"
            "一个 RI 只能有一个目标，不同 RI 可以有不同目标"
        ),
    )
    parser.add_argument(
        "--reservations-file",
        default=None,
        help=(
            "预留分摊定义文件（reservations.json）。按每个预留的 bindings 将 RI "
            "优惠收益按 boundQuantity 比例分摊到多个项目（projectCode）；"
            "与 --mapping-file/--reservation-id/--target-tag 互斥"
        ),
    )
    parser.add_argument(
        "--project-tag-key",
        default="projname",
        help=(
            "reservations.json 模式下，把 binding 的 projectCode 映射为目标标签时"
            "使用的标签键，默认 projname（即目标 projname=<projectCode>）"
        ),
    )
    parser.add_argument(
        "--amount-field",
        default="costInBillingCurrency",
        choices=("costInBillingCurrency", "costInPricingCurrency", "costInUsd"),
        help="RI 分摊金额字段，默认：costInBillingCurrency",
    )
    parser.add_argument(
        "--match-mode",
        default="model",
        choices=("model", "flex-group"),
        help=(
            "RI 收益匹配模式：model 按精确机型和区域匹配（默认）；"
            "flex-group 按 RI 实例大小灵活性分组和区域匹配，缺分组时回退到机型"
        ),
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="只生成汇总文件，不生成明细副本",
    )
    return parser.parse_args()


def expand_inputs(patterns: list[str]) -> list[Path]:
    """展开文件路径或 glob，并校验输入文件存在。"""
    paths: list[Path] = []
    for pattern in patterns:
        matches = [Path(item) for item in glob.glob(pattern)]
        paths.extend(matches or [Path(pattern)])
    result = sorted({path.resolve() for path in paths})
    missing = [path for path in result if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "找不到输入文件：" + ", ".join(str(path) for path in missing)
        )
    return result


def parse_tags(raw: str) -> dict[str, Any]:
    """解析账单中的 JSON 标签字段。"""
    if not raw.strip():
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("tags 字段不是 JSON 对象")
    return value


def parse_target_tag(raw: str) -> tuple[str, str]:
    """解析接收优惠收益的标签条件。"""
    key, separator, value = raw.partition("=")
    key = key.strip()
    value = value.strip()
    if not separator or not key or not value:
        raise ValueError(
            f"目标标签格式无效：{raw!r}，应为 key=value，例如 projname=fota"
        )
    return key, value


def _target_from_value(raw: Any) -> tuple[str, str]:
    """将映射文件中的单个目标标签值解析为 (key, value)。

    支持两种写法：字符串 "key=value"，或对象 {"key": ..., "value": ...}。
    """
    if isinstance(raw, dict):
        key = str(raw.get("key") or "").strip()
        value = str(raw.get("value") or "").strip()
        if not key or not value:
            raise ValueError(
                f"目标标签对象无效：{raw!r}，需要非空的 key 和 value"
            )
        return key, value
    return parse_target_tag(str(raw))


def _build_mapping(pairs: list[tuple[Any, Any]]) -> dict[str, tuple[str, str]]:
    """从 (reservationId, targetTag) 列表构建映射，强制一个 RI 只有一个目标。"""
    mapping: dict[str, tuple[str, str]] = {}
    for reservation_raw, target_raw in pairs:
        reservation_id = str(reservation_raw or "").strip()
        if not reservation_id:
            raise ValueError("映射文件包含空的 reservationId")
        target = _target_from_value(target_raw)
        existing = mapping.get(reservation_id)
        if existing is not None and existing != target:
            raise ValueError(
                f"reservationId {reservation_id!r} 映射到多个不同的分摊目标；"
                "一个 RI 只能有一个分摊目标"
            )
        mapping[reservation_id] = target
    if not mapping:
        raise ValueError("映射文件没有有效的 RI 映射")
    return mapping


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """json object_pairs_hook：任一 JSON 对象出现重复键时报错。

    用于捕获映射文件对象形式里同一 reservationId 出现多次（JSON 默认会静默保留
    最后一个），从而落实"一个 RI 只能有一个分摊目标"。
    """
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"映射文件存在重复键：{key!r}")
        result[key] = value
    return result


def _load_mapping_json(text: str) -> dict[str, tuple[str, str]]:
    """解析 JSON 映射文件。

    支持三种结构：
    - 对象：{"<ri-id>": "key=value", ...}
    - 带 mappings 的对象：{"mappings": [{"reservationId": ..., "targetTag": ...}]}
    - 数组：[{"reservationId": ..., "targetTag": ...}]
    """
    data = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    pairs: list[tuple[Any, Any]] = []
    if isinstance(data, dict):
        if isinstance(data.get("mappings"), list):
            items = data["mappings"]
        else:
            return _build_mapping(list(data.items()))
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError("映射文件 JSON 顶层必须是对象或数组")
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("映射条目必须是包含 reservationId/targetTag 的对象")
        pairs.append((item.get("reservationId"), item.get("targetTag")))
    return _build_mapping(pairs)


def _load_mapping_csv(text: str) -> dict[str, tuple[str, str]]:
    """解析 CSV 映射文件，需包含 reservationId 和 targetTag 两列。"""
    reader = csv.DictReader(io.StringIO(text))
    fields = reader.fieldnames or []
    if "reservationId" not in fields or "targetTag" not in fields:
        raise ValueError("CSV 映射文件需要 reservationId 和 targetTag 两列")
    pairs = [(row.get("reservationId"), row.get("targetTag")) for row in reader]
    return _build_mapping(pairs)


def load_mapping_file(path: Path) -> dict[str, tuple[str, str]]:
    """读取外部映射文件，返回 reservationId → (key, value) 映射。

    根据扩展名选择解析器：.csv 用 CSV，其余按 JSON 解析。
    """
    if not path.is_file():
        raise FileNotFoundError(f"找不到映射文件：{path}")
    text = path.read_text(encoding="utf-8-sig")
    if path.suffix.lower() == ".csv":
        return _load_mapping_csv(text)
    return _load_mapping_json(text)


def build_ri_target_map(args: argparse.Namespace) -> dict[str, tuple[str, str]]:
    """根据命令行参数构建 reservationId → 分摊目标标签的映射。

    优先使用 --mapping-file（可为不同 RI 指定不同目标）；否则回退到
    --reservation-id + --target-tag 的单目标模式（所有 RI 共用一个目标）。
    """
    if args.mapping_file:
        if args.reservation_id or args.target_tag:
            raise ValueError(
                "--mapping-file 不能与 --reservation-id/--target-tag 同时使用"
            )
        return load_mapping_file(Path(args.mapping_file))
    if not args.reservation_id or not args.target_tag:
        raise ValueError(
            "必须提供 --mapping-file，或同时提供 --reservation-id 和 --target-tag"
        )
    target = parse_target_tag(args.target_tag)
    mapping: dict[str, tuple[str, str]] = {}
    for reservation_id in args.reservation_id:
        reservation_id = reservation_id.strip()
        if reservation_id:
            mapping[reservation_id] = target
    if not mapping:
        raise ValueError("--reservation-id 至少需要一个非空值")
    return mapping


# reservationId → 分摊目标列表，每个目标为 ((标签键, 标签值), 权重)。
RiTargets = dict[str, list[tuple[tuple[str, str], Decimal]]]


def _reservation_id_from_external(external: Any) -> str:
    """从 externalReservationId 中提取账单里使用的 reservationId。

    形如 ``/providers/microsoft.capacity/reservationOrders/<order>/reservations/<rid>``，
    取 ``/reservations/`` 之后的一段；没有该分段时按原值返回。这是账单
    ``reservationId`` 列使用的预留 GUID，权威来源；对象里的 ``id`` /
    ``bindings[].reservationId`` 通常是内部记录 UUID，与账单无关。
    """
    text = str(external or "").strip()
    if not text:
        return ""
    marker = "/reservations/"
    if marker in text:
        tail = text.rsplit(marker, 1)[1]
        return tail.strip("/").split("/")[0].strip()
    return text


def load_reservations_file(path: Path, project_tag_key: str = "projname") -> RiTargets:
    """读取 reservations.json，构建 reservationId → [(目标, 权重)] 映射。

    每个预留按 ``bindings`` 拆分：``projectCode`` 作为目标标签值（键由
    ``project_tag_key`` 指定，默认 projname），``boundQuantity`` 作为权重。
    同一预留内相同 projectCode 的多个 binding 权重合并；权重非正的 binding 忽略；
    没有有效 binding 的预留跳过。
    """
    if not path.is_file():
        raise FileNotFoundError(f"找不到 reservations 文件：{path}")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, dict):
        if isinstance(data.get("reservations"), list):
            items = data["reservations"]
        else:
            items = [data]
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError("reservations 文件 JSON 顶层必须是对象或数组")

    key = (project_tag_key or "projname").strip() or "projname"
    result: RiTargets = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("reservation 条目必须是对象")
        reservation_id = _reservation_id_from_external(
            item.get("externalReservationId")
        )
        if not reservation_id:
            reservation_id = str(item.get("reservationId") or "").strip()
        if not reservation_id:
            raise ValueError("reservation 条目缺少 reservationId/externalReservationId")

        weights: dict[str, Decimal] = {}
        for binding in item.get("bindings") or []:
            if not isinstance(binding, dict):
                continue
            code = str(binding.get("projectCode") or "").strip()
            if not code:
                continue
            try:
                weight = Decimal(str(binding.get("boundQuantity", 0)))
            except InvalidOperation:
                weight = Decimal("0")
            if weight <= 0:
                continue
            weights[code] = weights.get(code, Decimal("0")) + weight
        if not weights:
            # 没有有效 binding（未绑定项目）的预留无法分摊，跳过。
            continue
        if reservation_id in result:
            raise ValueError(
                f"reservations 文件存在重复 reservationId：{reservation_id!r}"
            )
        result[reservation_id] = [
            ((key, code), weight) for code, weight in weights.items()
        ]
    if not result:
        raise ValueError(
            "reservations 文件没有可用的 RI 分摊定义（缺少有效 bindings）"
        )
    return result


def build_ri_targets(args: argparse.Namespace) -> tuple[RiTargets, bool]:
    """构建 reservationId → [(目标, 权重)] 映射，并返回是否使用全量按权重再分摊。

    - --reservations-file：按 bindings 权重把一个 RI 分摊到多个项目，返回
      ``redistribute_all=True``（无论 binding 数量，均对全部 RI 使用记录加回并按
      权重再分摊）。
    - --mapping-file / 内联 --reservation-id + --target-tag：单目标，权重恒为 1，
      返回 ``redistribute_all=False``（沿用"已在目标则不搬动"的历史行为）。
    """
    if args.reservations_file:
        if args.mapping_file or args.reservation_id or args.target_tag:
            raise ValueError(
                "--reservations-file 不能与 "
                "--mapping-file/--reservation-id/--target-tag 同时使用"
            )
        targets = load_reservations_file(
            Path(args.reservations_file), args.project_tag_key
        )
        return targets, True
    single = build_ri_target_map(args)
    return {rid: [(target, Decimal("1"))] for rid, target in single.items()}, False


def _row_contributions(
    amount: Decimal,
    targets_list: list[tuple[tuple[str, str], Decimal]],
    row: dict[str, str],
    redistribute_all: bool,
) -> tuple[Decimal | None, list[tuple[tuple[str, str], Decimal]], str]:
    """计算单条 RI 使用记录的加回金额与各目标收益池贡献。

    返回 ``(add_back, contributions, label)``：
    - ``add_back`` 为 None 表示该行保持不变（单目标模式下已在目标项目内）；
    - 否则 ``add_back`` 为加回到该行的金额，``contributions`` 为 [(目标, 贡献)]，
      各贡献之和等于 ``add_back``；``label`` 为写入 allocationTarget 列的目标标识。
    """
    if not redistribute_all and len(targets_list) == 1:
        target = targets_list[0][0]
        if has_target_tag(row, target):
            return None, [], ""
        return amount, [(target, amount)], target[1]
    total_weight = sum((weight for _target, weight in targets_list), Decimal("0"))
    contributions = [
        (target, amount * weight / total_weight)
        for target, weight in targets_list
    ]
    label = "|".join(target[1] for target, _weight in targets_list)
    return amount, contributions, label


def decimal_from_row(row: dict[str, str], field: str) -> Decimal:
    """将指定金额字段转换为 Decimal，避免浮点数精度误差。"""
    raw = (row.get(field) or "").strip()
    if not raw:
        return Decimal("0")
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"{field} 不是有效金额：{raw!r}") from exc


def is_ri_usage(row: dict[str, str], reservation_ids: set[str]) -> bool:
    """判断明细是否属于指定 reservationId 的实际 RI 使用记录。"""
    return (
        row.get("pricingModel") == "Reservation"
        and row.get("chargeType") == "Usage"
        and row.get("reservationId", "").strip() in reservation_ids
    )


def has_target_tag(row: dict[str, str], target_tag: tuple[str, str]) -> bool:
    """判断明细是否包含指定的接收项目标签。"""
    key, expected_value = target_tag
    return str(parse_tags(row.get("tags", "")).get(key) or "") == expected_value


def additional_info(row: dict[str, str]) -> dict[str, Any]:
    """解析账单的 additionalInfo JSON 字段；无效时返回空字典。"""
    try:
        info = json.loads(row.get("additionalInfo") or "{}")
    except json.JSONDecodeError:
        return {}
    return info if isinstance(info, dict) else {}


def vm_model(row: dict[str, str]) -> str:
    """获取 Azure VM 机型，优先使用 additionalInfo.ServiceType。"""
    return str(additional_info(row).get("ServiceType") or row.get("meterName") or "")


def ri_normalization_ratio(row: dict[str, str]) -> Decimal | None:
    """获取 additionalInfo.RINormalizationRatio；缺失或非法时返回 None。

    这是 Azure 账单中真实存在的实例大小灵活性字段：当 RI 以大小灵活性方式
    应用到某个机型时，该比率表示 RI 在该机型上的归一化占用系数。
    """
    raw = additional_info(row).get("RINormalizationRatio")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return Decimal(str(raw))
    except InvalidOperation:
        return None


# 从机型名派生实例大小灵活性组：family + 附加特性 + 版本（去掉 vCPU 核数与受限核数）。
# 例：Standard_D2s_v5 / Standard_D4s_v5 / Standard_D8-2s_v5 → 组 "Ds_v5"；
#     Standard_E8s_v5 → "Es_v5"；Standard_D2_v5 → "D_v5"（非高级存储，另一组）。
# 说明：这是基于命名规则的启发式分组，覆盖 D/E/F 等主流系列的常见场景，
# 并非 Azure 官方灵活性比率表的逐条复刻；边缘系列如有出入可按官方表扩展。
_SIZE_PATTERN = re.compile(r"^([A-Za-z]+)(\d+)(?:-\d+)?([A-Za-z]*)(_.*)?$")


def flexibility_group(model: str) -> str:
    """根据机型名派生实例大小灵活性组标识；无法解析时返回原始机型名。"""
    name = model.strip()
    if name.startswith("Standard_"):
        name = name[len("Standard_") :]
    match = _SIZE_PATTERN.match(name)
    if not match:
        return model.strip()
    family, _cores, features, version = match.groups()
    return f"{family}{features}{version or ''}"


def is_size_flexible(row: dict[str, str]) -> bool:
    """启发式判断该明细是否体现了实例大小灵活性（应用到非基准规格）。

    依据真实字段 RINormalizationRatio：比率存在且不等于 1 时，说明该 RI 被
    归一化到了与基准不同的规格。注意：比率为 1 也可能是基准规格本身，
    因此该判断仅为按行提示，权威的 On/Off 需 Reservation API（账单不含）。
    """
    ratio = ri_normalization_ratio(row)
    return ratio is not None and ratio != Decimal("1")


def allocated_field_name(amount_field: str) -> str:
    """根据金额字段派生分摊后金额的输出列名。

    使输出列货币与 --amount-field 一致，避免用非账单货币时列名产生误导。
    默认 costInBillingCurrency 仍产出 allocatedCostInBillingCurrency（向后兼容）。
    """
    return "allocated" + amount_field[:1].upper() + amount_field[1:]


def vm_region(row: dict[str, str]) -> str:
    """获取 Azure VM 区域。"""
    return (
        row.get("meterRegion")
        or row.get("resourceLocation")
        or row.get("location")
        or ""
    )


def allocation_key(row: dict[str, str], match_mode: str = "model") -> tuple[str, str]:
    """返回 RI 收益匹配使用的键。

    - model：按精确机型和区域匹配（默认，兼容历史行为）。
    - flex-group：按机型派生的实例大小灵活性组和区域匹配，使同一 RI 覆盖的
      同系列不同规格（如 D2s_v5 与 D4s_v5）落入同一收益池，避免"RI 单规格、
      目标项目另一规格"时分摊失败。无法解析机型时回退到精确机型。
    """
    region = vm_region(row)
    if match_mode == "flex-group":
        model = vm_model(row)
        group = flexibility_group(model)
        if group:
            return f"flexgroup:{group}", region
    return vm_model(row), region


def project_of(row: dict[str, str]) -> str:
    """获取明细的 projname；缺失时使用统一占位名称。"""
    return str(parse_tags(row.get("tags", "")).get("projname") or "<missing>")


def main() -> None:
    """读取账单、计算分摊并生成明细副本和汇总报告。"""
    args = parse_args()
    ri_targets, redistribute_all = build_ri_targets(args)
    reservation_ids = set(ri_targets)
    targets = sorted(
        {target for entries in ri_targets.values() for target, _weight in entries}
    )
    input_paths = expand_inputs(args.inputs)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 先将明细加载到内存，第二遍处理时可以按目标项目的总费用计算比例。
    rows: list[dict[str, str]] = []
    fieldnames: list[str] | None = None
    source_files: list[str] = []
    total_rows = 0
    ri_usage_rows = 0
    ri_reallocated_rows = 0
    ri_size_flexible_rows = 0
    ri_amount = Decimal("0")
    project_before: defaultdict[str, Decimal] = defaultdict(Decimal)
    project_ri: defaultdict[str, Decimal] = defaultdict(Decimal)
    # 收益池和目标费用池按 (分摊目标, 匹配键) 隔离，确保每个 RI 的收益只流向自己的目标。
    pool_key_type = tuple[tuple[str, str], tuple[str, str]]
    target_non_ri_indexes: defaultdict[pool_key_type, list[int]] = defaultdict(list)
    target_non_ri_total_by_key: defaultdict[pool_key_type, Decimal] = defaultdict(
        Decimal
    )
    ri_amount_by_key: defaultdict[pool_key_type, Decimal] = defaultdict(Decimal)
    ri_service_types: defaultdict[str, set[str]] = defaultdict(set)

    for input_path in input_paths:
        source_files.append(str(input_path))
        with input_path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames is None:
                raise ValueError(f"{input_path} 没有 CSV 表头")
            if fieldnames is None:
                fieldnames = list(reader.fieldnames)
            elif fieldnames != list(reader.fieldnames):
                raise ValueError(f"{input_path} 的 CSV 表头与其他输入文件不一致")

            for row in reader:
                total_rows += 1
                row["_source_file"] = str(input_path)
                rows.append(row)

                if row.get("meterCategory") != "Virtual Machines":
                    continue

                project = project_of(row)
                amount = decimal_from_row(row, args.amount_field)
                project_before[project] += amount

                if is_ri_usage(row, reservation_ids):
                    reservation_id = row.get("reservationId", "").strip()
                    targets_list = ri_targets[reservation_id]
                    ri_usage_rows += 1
                    ri_service_types[reservation_id].add(vm_model(row))
                    if is_size_flexible(row):
                        ri_size_flexible_rows += 1
                    add_back, contributions, _label = _row_contributions(
                        amount, targets_list, row, redistribute_all
                    )
                    if add_back is not None:
                        ri_reallocated_rows += 1
                        ri_amount += add_back
                        project_ri[project] += add_back
                        alloc_key = allocation_key(row, args.match_mode)
                        for target, contribution in contributions:
                            ri_amount_by_key[(target, alloc_key)] += contribution
                else:
                    matched = [t for t in targets if has_target_tag(row, t)]
                    if len(matched) > 1:
                        labels = "、".join("=".join(t) for t in matched)
                        raise ValueError(
                            f"虚拟机明细同时匹配多个分摊目标（{labels}）；"
                            "一条明细只能归属一个分摊目标。"
                        )
                    if matched:
                        pool_key = (matched[0], allocation_key(row, args.match_mode))
                        target_non_ri_indexes[pool_key].append(len(rows) - 1)
                        target_non_ri_total_by_key[pool_key] += amount

    target_non_ri_total = sum(
        target_non_ri_total_by_key.values(), Decimal("0")
    )

    for pool_key, key_ri_amount in ri_amount_by_key.items():
        target, alloc_key = pool_key
        target_label = "=".join(target)
        # RI 金额为 0 时无需分摊，跳过校验，避免对零成本 RI 记录误报。
        if key_ri_amount == 0:
            continue
        key_target_total = target_non_ri_total_by_key.get(pool_key, Decimal("0"))
        if key_target_total == 0:
            raise ValueError(
                f"找不到分摊目标 {target_label!r} 与 RI 机型和区域 {alloc_key!r} "
                "匹配的非 RI 虚拟机明细。"
            )
        if key_target_total < key_ri_amount:
            raise ValueError(
                f"分摊目标 {target_label!r} 的匹配机型和区域 {alloc_key!r} "
                f"非 RI 虚拟机费用 {key_target_total} 小于待分摊 RI 金额 "
                f"{key_ri_amount}，无法按比例分摊后保持非负费用。"
            )

    # 非目标项目的 RI 使用记录加回自身 RI 金额，体现未人为分配前的原始资源成本。
    # 目标项目的非 RI VM 明细按原始费用比例扣减，承接 RI 优惠收益。
    allocation_by_index: dict[int, Decimal] = {}
    target_value_by_index: dict[int, str] = {}
    for index, row in enumerate(rows):
        if row.get("meterCategory") == "Virtual Machines" and is_ri_usage(
            row, reservation_ids
        ):
            reservation_id = row.get("reservationId", "").strip()
            targets_list = ri_targets[reservation_id]
            amount = decimal_from_row(row, args.amount_field)
            add_back, _contributions, label = _row_contributions(
                amount, targets_list, row, redistribute_all
            )
            if add_back is not None:
                allocation_by_index[index] = add_back
                target_value_by_index[index] = label

    # 每条目标明细只承接相同分摊目标、相同机型和区域 RI 收益池中的金额。
    for pool_key, indexes in target_non_ri_indexes.items():
        target, _alloc_key = pool_key
        key_ri_amount = ri_amount_by_key.get(pool_key, Decimal("0"))
        # 没有 RI 收益可分摊（含仅有接收明细而无对应 RI 的池），保持金额不变。
        if key_ri_amount == 0:
            continue
        key_target_total = target_non_ri_total_by_key[pool_key]
        for index in indexes:
            amount = decimal_from_row(rows[index], args.amount_field)
            allocation_by_index[index] = -(
                key_ri_amount * amount / key_target_total
            )
            target_value_by_index[index] = target[1]

    output_paths: list[str] = []
    allocated_field = allocated_field_name(args.amount_field)
    if not args.summary_only:
        if fieldnames is None:
            raise ValueError("没有读取到 CSV 表头")
        fieldnames = [
            *fieldnames,
            allocated_field,
            "riAllocationAmount",
            "allocationType",
            "allocationTarget",
        ]

        row_offset = 0
        for input_path in input_paths:
            output_path = output_dir / input_path.name
            output_paths.append(str(output_path))
            source_row_count = sum(
                1 for row in rows if row["_source_file"] == str(input_path)
            )
            with output_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fieldnames)
                writer.writeheader()
                for index in range(row_offset, row_offset + source_row_count):
                    row = dict(rows[index])
                    row.pop("_source_file", None)
                    original = decimal_from_row(row, args.amount_field)
                    adjustment = allocation_by_index.get(index, Decimal("0"))
                    row[allocated_field] = str(
                        original + adjustment
                    )
                    row["riAllocationAmount"] = str(adjustment)
                    # 正数表示把 RI 使用金额加回实际使用 RI 的资源。
                    if adjustment > 0:
                        row["allocationType"] = "RI_USAGE_COST_REASSIGNED"
                        row["allocationTarget"] = target_value_by_index.get(index, "")
                    # 负数表示把 RI 优惠收益分配给目标项目。
                    elif adjustment < 0:
                        row["allocationType"] = "RI_BENEFIT_ASSIGNED"
                        row["allocationTarget"] = target_value_by_index.get(index, "")
                    else:
                        row["allocationType"] = ""
                        row["allocationTarget"] = ""
                    writer.writerow(row)
            row_offset += source_row_count

    # 每个分摊目标承接的 RI 收益总额（按目标标签值汇总）。
    assigned_by_target: defaultdict[str, Decimal] = defaultdict(Decimal)
    for (target, _alloc_key), key_ri_amount in ri_amount_by_key.items():
        assigned_by_target[target[1]] += key_ri_amount

    project_after = dict(project_before)
    for project, added_amount in project_ri.items():
        project_after[project] = project_after.get(project, Decimal("0")) + added_amount
    for target_value, assigned_amount in assigned_by_target.items():
        project_after[target_value] = (
            project_after.get(target_value, Decimal("0")) - assigned_amount
        )

    allocation_path = output_dir / "project-allocation.csv"
    with allocation_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(
            [
                "projname",
                "beforeAmount",
                "afterAllocatedAmount",
                "delta",
                "riAmountAdded",
                "riAmountAssigned",
            ]
        )
        projects = sorted(
            set(project_before) | set(project_after),
            key=lambda project: (-project_before.get(project, Decimal("0")), project),
        )
        for project in projects:
            added = project_ri.get(project, Decimal("0"))
            assigned = assigned_by_target.get(project, Decimal("0"))
            before = project_before.get(project, Decimal("0"))
            after = project_after.get(project, Decimal("0"))
            writer.writerow(
                [
                    project,
                    str(before),
                    str(after),
                    str(after - before),
                    str(added),
                    str(assigned),
                ]
            )

    summary = {
        "allocationMode": "reservations" if redistribute_all else "mapping",
        "mappings": [
            {
                "reservationId": reservation_id,
                "targets": [
                    {"key": target[0], "value": target[1], "weight": str(weight)}
                    for target, weight in ri_targets[reservation_id]
                ],
            }
            for reservation_id in sorted(ri_targets)
        ],
        "targets": ["=".join(target) for target in targets],
        "reservationIds": sorted(reservation_ids),
        "amountField": args.amount_field,
        "allocatedCostField": allocated_field,
        "matchMode": args.match_mode,
        "costScope": "meterCategory == Virtual Machines",
        "riSelection": {
            "pricingModel": "Reservation",
            "chargeType": "Usage",
            "reservationId": sorted(reservation_ids),
        },
        "inputFiles": source_files,
        "outputFiles": output_paths + [str(allocation_path)],
        "totalRows": total_rows,
        "riUsageRows": ri_usage_rows,
        "riReallocatedRows": ri_reallocated_rows,
        "riSizeFlexibleRows": ri_size_flexible_rows,
        "riServiceTypes": {
            reservation_id: sorted(models)
            for reservation_id, models in sorted(ri_service_types.items())
        },
        "riUsageAmount": str(ri_amount),
        "targetNonRiVmAmount": str(target_non_ri_total),
        "assignedByTarget": {
            target_value: str(amount)
            for target_value, amount in sorted(assigned_by_target.items())
        },
        "riAllocationKeys": [
            {
                "target": "=".join(target),
                "matchKey": alloc_key[0],
                "region": alloc_key[1],
                "riAmount": str(ri_amount_by_key[(target, alloc_key)]),
                "targetNonRiVmAmount": str(
                    target_non_ri_total_by_key.get((target, alloc_key), Decimal("0"))
                ),
            }
            for (target, alloc_key) in sorted(ri_amount_by_key)
        ],
        "sourceFilesModified": False,
        "resourceTagsModified": False,
        "resourceIdsModified": False,
        "detailRowsHaveAllocatedCost": not args.summary_only,
    }
    summary_path = output_dir / "ri-summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"分摊目标数：{len(targets)}")
    print(f"RI 使用记录：{ri_usage_rows} 条")
    print(f"RI 使用金额：{ri_amount}")
    print(f"目标项目非 RI 虚拟机费用：{target_non_ri_total}")
    print(f"输出目录：{output_dir}")
    print(f"汇总文件：{summary_path}")


if __name__ == "__main__":
    main()
