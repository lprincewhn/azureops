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
import json
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
        required=True,
        action="append",
        help="要重新分摊优惠收益的 reservationId，可重复指定",
    )
    parser.add_argument(
        "--target-tag",
        required=True,
        help="接收优惠收益的标签，格式为 key=value，例如 projname=fota",
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


def instance_flexibility_group(row: dict[str, str]) -> str:
    """获取 RI 实例大小灵活性分组，如 'DSv3 Series'；无则返回空串。"""
    return str(additional_info(row).get("InstanceFlexibilityGroup") or "").strip()


def instance_flexibility_ratio(row: dict[str, str]) -> Decimal | None:
    """获取 RI 实例大小灵活性归一化比率；缺失或非法时返回 None。"""
    raw = str(additional_info(row).get("InstanceFlexibilityRatio") or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def is_size_flexible(row: dict[str, str]) -> bool:
    """判断明细对应的 RI 是否以实例大小灵活性方式应用（存在灵活性分组）。"""
    return bool(instance_flexibility_group(row))


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
    - flex-group：优先按 RI 实例大小灵活性分组和区域匹配，使同一 RI 覆盖的
      不同机型能落入同一收益池；缺少灵活性分组时回退到机型匹配。
    """
    region = vm_region(row)
    if match_mode == "flex-group":
        group = instance_flexibility_group(row)
        if group:
            return f"flexgroup:{group}", region
    return vm_model(row), region


def project_of(row: dict[str, str]) -> str:
    """获取明细的 projname；缺失时使用统一占位名称。"""
    return str(parse_tags(row.get("tags", "")).get("projname") or "<missing>")


def main() -> None:
    """读取账单、计算分摊并生成明细副本和汇总报告。"""
    args = parse_args()
    reservation_ids = {item.strip() for item in args.reservation_id if item.strip()}
    if not reservation_ids:
        raise ValueError("--reservation-id 至少需要一个非空值")
    target_tag = parse_target_tag(args.target_tag)
    target_project = target_tag[1]
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
    target_non_ri_indexes: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
    target_non_ri_total_by_key: defaultdict[tuple[str, str], Decimal] = defaultdict(
        Decimal
    )
    ri_amount_by_key: defaultdict[tuple[str, str], Decimal] = defaultdict(Decimal)

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
                    ri_usage_rows += 1
                    if is_size_flexible(row):
                        ri_size_flexible_rows += 1
                    if not has_target_tag(row, target_tag):
                        ri_reallocated_rows += 1
                        ri_amount += amount
                        ri_amount_by_key[allocation_key(row, args.match_mode)] += amount
                        project_ri[project] += amount
                elif has_target_tag(row, target_tag):
                    key = allocation_key(row, args.match_mode)
                    target_non_ri_indexes[key].append(len(rows) - 1)
                    target_non_ri_total_by_key[key] += amount

    target_non_ri_total = sum(
        target_non_ri_total_by_key.values(), Decimal("0")
    )

    for key, key_ri_amount in ri_amount_by_key.items():
        key_target_total = target_non_ri_total_by_key.get(key, Decimal("0"))
        if key_target_total < key_ri_amount:
            raise ValueError(
                f"目标项目 {target_project!r} 的匹配机型和区域 {key!r} "
                f"非 RI 虚拟机费用 {key_target_total} 小于待分摊 RI 金额 "
                f"{key_ri_amount}，无法按比例分摊后保持非负费用。"
            )
        if key_target_total == 0:
            raise ValueError(
                f"找不到目标项目 {target_project!r} 与 RI 机型和区域 {key!r} "
                "匹配的非 RI 虚拟机明细。"
            )

    # 非目标项目的 RI 使用记录加回自身 RI 金额，体现未人为分配前的原始资源成本。
    # 目标项目的非 RI VM 明细按原始费用比例扣减，承接 RI 优惠收益。
    allocation_by_index: dict[int, Decimal] = {}
    for index, row in enumerate(rows):
        if row.get("meterCategory") == "Virtual Machines" and is_ri_usage(
            row, reservation_ids
        ):
            project = project_of(row)
            amount = decimal_from_row(row, args.amount_field)
            if not has_target_tag(row, target_tag):
                allocation_by_index[index] = amount

    # 每条目标明细只承接相同机型和区域 RI 收益池中的金额。
    for key, indexes in target_non_ri_indexes.items():
        key_ri_amount = ri_amount_by_key.get(key, Decimal("0"))
        key_target_total = target_non_ri_total_by_key[key]
        for index in indexes:
            amount = decimal_from_row(rows[index], args.amount_field)
            allocation_by_index[index] = -(
                key_ri_amount * amount / key_target_total
            )

    output_paths: list[str] = []
    if not args.summary_only:
        if fieldnames is None:
            raise ValueError("没有读取到 CSV 表头")
        fieldnames = [
            *fieldnames,
            "allocatedCostInBillingCurrency",
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
                    row["allocatedCostInBillingCurrency"] = str(
                        original + adjustment
                    )
                    row["riAllocationAmount"] = str(adjustment)
                    # 正数表示把 RI 使用金额加回实际使用 RI 的资源。
                    if adjustment > 0:
                        row["allocationType"] = "RI_USAGE_COST_REASSIGNED"
                        row["allocationTarget"] = target_project
                    # 负数表示把 RI 优惠收益分配给目标项目。
                    elif adjustment < 0:
                        row["allocationType"] = "RI_BENEFIT_ASSIGNED"
                        row["allocationTarget"] = target_project
                    else:
                        row["allocationType"] = ""
                        row["allocationTarget"] = ""
                    writer.writerow(row)
            row_offset += source_row_count

    project_after = dict(project_before)
    for project, amount in project_ri.items():
        if project != target_project:
            project_after[project] += amount
    project_after[target_project] = (
        project_after.get(target_project, Decimal("0")) - ri_amount
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
            assigned = ri_amount if project == target_project else Decimal("0")
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
        "targetProject": target_project,
        "targetTag": {"key": target_tag[0], "value": target_tag[1]},
        "reservationIds": sorted(reservation_ids),
        "amountField": args.amount_field,
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
        "riUsageAmount": str(ri_amount),
        "targetNonRiVmAmount": str(target_non_ri_total),
        "riAllocationKeys": [
            {
                "vmModel": key[0],
                "region": key[1],
                "riAmount": str(ri_amount_by_key[key]),
                "targetNonRiVmAmount": str(target_non_ri_total_by_key.get(key, Decimal("0"))),
            }
            for key in sorted(ri_amount_by_key)
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

    print(f"RI 使用记录：{ri_usage_rows} 条")
    print(f"RI 使用金额：{ri_amount}")
    print(f"目标项目非 RI 虚拟机费用：{target_non_ri_total}")
    print(f"输出目录：{output_dir}")
    print(f"汇总文件：{summary_path}")


if __name__ == "__main__":
    main()
