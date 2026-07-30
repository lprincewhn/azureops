#!/usr/bin/env python3
"""生成 RI 费用重新分摊后的明细副本，不修改源 CSV。

分摊规则：
1. RI 使用记录：pricingModel=Reservation、chargeType=Usage、reservationId
   等于命令行指定的 RI。
2. 只处理 meterCategory=Virtual Machines 的记录。
3. 原 RI 使用记录的 tags 和 ResourceId 保持不变。
4. 原 RI 使用记录不是目标项目时：
     allocatedCostInBillingCurrency = costInBillingCurrency + RI 使用金额
5. 目标项目的 VM 记录按费用比例扣减 RI 使用金额（若某条 RI 使用记录自身命中
   目标，则加回后以全价一并参与该目标分摊）：
     allocatedCostInBillingCurrency = costInBillingCurrency - 分摊金额

这样非目标的 RI 使用记录 allocatedCost 大于原始 cost，目标项目接收记录的
allocatedCost 小于原始 cost，整体金额保持不变。
"""

from __future__ import annotations

import argparse
import csv
import glob
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
        "--reservations-file",
        required=True,
        help=(
            "预留分摊定义文件（reservations.json）。按每个预留的 bindings 将 RI "
            "优惠收益按 boundQuantity 比例分摊到多个项目（projectCode）"
        ),
    )
    parser.add_argument(
        "--project-tag-key",
        default="projname",
        help=(
            "把 binding 的 projectCode 映射为目标标签时使用的标签键，"
            "默认 projname（即目标 projname=<projectCode>）"
        ),
    )
    parser.add_argument(
        "--amount-field",
        default="costInBillingCurrency",
        choices=("costInBillingCurrency", "costInPricingCurrency", "costInUsd"),
        help="RI 分摊金额字段，默认：costInBillingCurrency",
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


# reservationId → 分摊目标列表，每个目标为 ((标签键, 标签值), 权重)。
RiTargets = dict[str, list[tuple[tuple[str, str], Decimal]]]
# reservationId → RI 收益匹配模式（"model" 或 "flex-group"）。
RiModes = dict[str, str]
# reservationId → 权重分母（boundTotal）。绑定项目按 boundQuantity/boundTotal 分摊，
# 剩余 (boundTotal - Σ boundQuantity)/boundTotal 的收益留在原 RI 使用项目（其他项目）。
RiDenominators = dict[str, Decimal]


def _match_mode_from_flexibility(flexibility: Any) -> str:
    """根据预留的 flexibility 字段推导 RI 收益匹配模式。

    实例大小灵活性开启（``on``）时按灵活性组匹配（``flex-group``），
    否则按精确机型匹配（``model``）。
    """
    return "flex-group" if str(flexibility or "").strip().lower() == "on" else "model"


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


def load_reservations_file(
    path: Path, project_tag_key: str = "projname"
) -> tuple[RiTargets, RiModes, RiDenominators]:
    """读取 reservations.json，构建 reservationId → [(目标, 权重)] 映射、匹配模式及权重分母。

    每个预留按 ``bindings`` 拆分：``projectCode`` 作为目标标签值（键由
    ``project_tag_key`` 指定，默认 projname），``boundQuantity`` 作为权重。
    同一预留内相同 projectCode 的多个 binding 权重合并；权重非正的 binding 忽略；
    没有有效 binding 的预留跳过。RI 收益匹配模式由预留的 ``flexibility`` 字段
    推导（``on`` → flex-group，否则 model），无需命令行指定。

    权重分母取 ``boundTotal``：绑定项目各按 ``boundQuantity / boundTotal`` 分摊，
    剩余的 ``(boundTotal - Σ boundQuantity) / boundTotal`` 那部分收益不再搬走，
    留在实际使用该 RI 的原项目（其他项目）。当 ``boundTotal`` 缺失、非正或小于
    Σ boundQuantity 时，回退为 Σ boundQuantity（即无剩余、全额分摊到绑定项目）。

    返回 ``(ri_targets, ri_modes, ri_denominators)``。
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
    modes: RiModes = {}
    denominators: RiDenominators = {}
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
        modes[reservation_id] = _match_mode_from_flexibility(
            item.get("flexibility")
        )
        bound_sum = sum(weights.values(), Decimal("0"))
        try:
            bound_total = Decimal(str(item.get("boundTotal")))
        except (InvalidOperation, TypeError):
            bound_total = None
        # boundTotal 缺失/非正/小于已绑定权重之和时，回退为全额分摊（无其他项目剩余）。
        if bound_total is None or bound_total < bound_sum:
            bound_total = bound_sum
        denominators[reservation_id] = bound_total
    if not result:
        raise ValueError(
            "reservations 文件没有可用的 RI 分摊定义（缺少有效 bindings）"
        )
    return result, modes, denominators


def build_ri_targets(
    args: argparse.Namespace,
) -> tuple[RiTargets, RiModes, RiDenominators]:
    """构建 reservationId → [(目标, 权重)] 映射、匹配模式及权重分母。

    读取 --reservations-file 指定的 reservations.json，按每个预留的 bindings
    权重把一个 RI 分摊到多个项目（projectCode），并按预留的 flexibility 字段
    推导 RI 收益匹配模式；权重分母取 boundTotal（详见 load_reservations_file）。
    """
    return load_reservations_file(Path(args.reservations_file), args.project_tag_key)


def _row_contributions(
    amount: Decimal,
    targets_list: list[tuple[tuple[str, str], Decimal]],
    denominator: Decimal,
) -> tuple[Decimal, list[tuple[tuple[str, str], Decimal]], str]:
    """计算单条 RI 使用记录的加回金额与各目标收益池贡献。

    ``denominator`` 为权重分母（boundTotal）。绑定项目各分得
    ``amount × boundQuantity / boundTotal``；加回金额 ``add_back`` 为各目标贡献之和，
    即 ``amount × Σ boundQuantity / boundTotal``。剩余部分不加回，留在原 RI 使用项目
    （其他项目）。``denominator`` 非正时按 Σ 权重全额分摊。

    返回 ``(add_back, contributions, label)``：``contributions`` 为 [(目标, 贡献)]，
    各贡献之和等于 ``add_back``；``label`` 为写入 allocationTarget 列的目标标识。
    """
    total_weight = sum((weight for _target, weight in targets_list), Decimal("0"))
    if denominator is None or denominator <= 0:
        denominator = total_weight
    contributions = [
        (target, amount * weight / denominator)
        for target, weight in targets_list
    ]
    add_back = sum((c for _target, c in contributions), Decimal("0"))
    label = "|".join(target[1] for target, _weight in targets_list)
    return add_back, contributions, label


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
    ri_targets, ri_modes, ri_denominators = build_ri_targets(args)
    reservation_ids = set(ri_targets)
    targets = sorted(
        {target for entries in ri_targets.values() for target, _weight in entries}
    )
    # 每个分摊目标的匹配模式来自绑定它的预留的 flexibility。若同一目标被匹配模式
    # 不同的预留同时绑定，收益池无法一致隔离，直接报错要求先统一口径。
    target_modes: dict[tuple[str, str], str] = {}
    for reservation_id, entries in ri_targets.items():
        mode = ri_modes[reservation_id]
        for target, _weight in entries:
            existing = target_modes.get(target)
            if existing is not None and existing != mode:
                raise ValueError(
                    f"分摊目标 {'='.join(target)!r} 被匹配模式不同的预留同时绑定"
                    f"（{existing} 与 {mode}）；请统一相关预留的 flexibility。"
                )
            target_modes[target] = mode
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
    ri_raw_total = Decimal("0")
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
    # RI 使用记录加回后的信息：加回金额、目标标签、以及作为接收方时的全价基数。
    ri_usage_indexes: set[int] = set()
    ri_add_back_by_index: dict[int, Decimal] = {}
    ri_label_by_index: dict[int, str] = {}
    receiver_basis_by_index: dict[int, Decimal] = {}

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
                    add_back, contributions, label = _row_contributions(
                        amount, targets_list, ri_denominators[reservation_id]
                    )
                    ri_reallocated_rows += 1
                    ri_amount += add_back
                    ri_raw_total += amount
                    project_ri[project] += add_back
                    row_index = len(rows) - 1
                    ri_usage_indexes.add(row_index)
                    ri_add_back_by_index[row_index] = add_back
                    ri_label_by_index[row_index] = label
                    alloc_key = allocation_key(row, ri_modes[reservation_id])
                    for target, contribution in contributions:
                        ri_amount_by_key[(target, alloc_key)] += contribution
                    # 加回后的 RI 使用记录以全价（原始金额 + 加回金额）参与其自身所属
                    # 目标的收益分摊，避免该目标项目仅靠其它明细承接、拿到的收益少于
                    # binding 权重应得份额。
                    matched = [t for t in targets if has_target_tag(row, t)]
                    if len(matched) > 1:
                        labels = "、".join("=".join(t) for t in matched)
                        raise ValueError(
                            f"虚拟机明细同时匹配多个分摊目标（{labels}）；"
                            "一条明细只能归属一个分摊目标。"
                        )
                    if matched:
                        full_price = amount + add_back
                        pool_key = (
                            matched[0],
                            allocation_key(row, target_modes[matched[0]]),
                        )
                        target_non_ri_indexes[pool_key].append(row_index)
                        target_non_ri_total_by_key[pool_key] += full_price
                        receiver_basis_by_index[row_index] = full_price
                else:
                    matched = [t for t in targets if has_target_tag(row, t)]
                    if len(matched) > 1:
                        labels = "、".join("=".join(t) for t in matched)
                        raise ValueError(
                            f"虚拟机明细同时匹配多个分摊目标（{labels}）；"
                            "一条明细只能归属一个分摊目标。"
                        )
                    if matched:
                        row_index = len(rows) - 1
                        pool_key = (
                            matched[0],
                            allocation_key(row, target_modes[matched[0]]),
                        )
                        target_non_ri_indexes[pool_key].append(row_index)
                        target_non_ri_total_by_key[pool_key] += amount
                        receiver_basis_by_index[row_index] = amount

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
                "匹配的虚拟机明细（含加回后的 RI 使用记录）。"
            )
        if key_target_total < key_ri_amount:
            raise ValueError(
                f"分摊目标 {target_label!r} 的匹配机型和区域 {alloc_key!r} "
                f"虚拟机全价费用 {key_target_total} 小于待分摊 RI 金额 "
                f"{key_ri_amount}，无法按比例分摊后保持非负费用。"
            )

    # RI 使用记录先加回自身 RI 金额（体现未人为分配前的原始资源成本）；若其标签命中
    # 某个分摊目标，则加回后再以全价参与该目标收益分摊，净额为加回金额减去应摊份额。
    allocation_by_index: dict[int, Decimal] = {}
    target_value_by_index: dict[int, str] = {}
    for index in ri_usage_indexes:
        allocation_by_index[index] = ri_add_back_by_index[index]
        target_value_by_index[index] = ri_label_by_index[index]

    # 每条目标明细只承接相同分摊目标、相同机型和区域 RI 收益池中的金额；加回后的
    # RI 使用记录以全价基数参与，普通非 RI 明细以原始费用基数参与。
    for pool_key, indexes in target_non_ri_indexes.items():
        target, _alloc_key = pool_key
        key_ri_amount = ri_amount_by_key.get(pool_key, Decimal("0"))
        # 没有 RI 收益可分摊（含仅有接收明细而无对应 RI 的池），保持金额不变。
        if key_ri_amount == 0:
            continue
        key_target_total = target_non_ri_total_by_key[pool_key]
        for index in indexes:
            basis = receiver_basis_by_index[index]
            share = key_ri_amount * basis / key_target_total
            allocation_by_index[index] = (
                allocation_by_index.get(index, Decimal("0")) - share
            )
            # RI 使用记录本身也是接收方时保留其加回目标标签，其余用池目标标签。
            if index not in ri_usage_indexes:
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
                    # RI 使用记录：其 RI 成本被重新分配（可能同时又接收了本项目应得
                    # 收益），净额可正可负；统一标记为 RI_USAGE_COST_REASSIGNED。
                    if index in ri_usage_indexes and adjustment != 0:
                        row["allocationType"] = "RI_USAGE_COST_REASSIGNED"
                        row["allocationTarget"] = target_value_by_index.get(index, "")
                    # 负数表示把 RI 优惠收益分配给目标项目。
                    elif adjustment < 0:
                        row["allocationType"] = "RI_BENEFIT_ASSIGNED"
                        row["allocationTarget"] = target_value_by_index.get(index, "")
                    # 兜底：非 RI 记录理论上不会出现正调整。
                    elif adjustment > 0:
                        row["allocationType"] = "RI_USAGE_COST_REASSIGNED"
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
        "allocationMode": "reservations",
        "mappings": [
            {
                "reservationId": reservation_id,
                "matchMode": ri_modes[reservation_id],
                "boundTotal": str(ri_denominators[reservation_id]),
                "boundWeightSum": str(
                    sum(
                        (weight for _target, weight in ri_targets[reservation_id]),
                        Decimal("0"),
                    )
                ),
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
        "matchModeByReservation": {
            reservation_id: ri_modes[reservation_id]
            for reservation_id in sorted(ri_modes)
        },
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
        "riAllocatableAmount": str(ri_amount),
        "riRawTotalAmount": str(ri_raw_total),
        "targetVmReceiverAmount": str(target_non_ri_total),
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
                "targetVmReceiverAmount": str(
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
    print(f"RI 原始费用合计：{ri_raw_total}")
    print(f"待分摊 RI 金额：{ri_amount}")
    print(f"目标项目虚拟机接收费用：{target_non_ri_total}")
    print(f"输出目录：{output_dir}")
    print(f"汇总文件：{summary_path}")


if __name__ == "__main__":
    main()
