#!/usr/bin/env python3
"""比较 RI 分摊前后的账单，生成虚拟机级别对账文件。"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from itertools import zip_longest
from pathlib import Path


@dataclass
class ReconcileResult:
    resource_count: int
    changed_resources: int
    before_total: Decimal
    after_total: Decimal
    delta_total: Decimal


def expand_inputs(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path(item) for item in glob.glob(pattern))
    result = sorted({path.resolve() for path in paths})
    if not result or any(not path.is_file() for path in result):
        raise FileNotFoundError("找不到输入账单文件")
    return result


def tags_from_row(row: dict[str, str]) -> dict[str, object]:
    raw = row.get("tags") or ""
    if not raw.strip():
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("tags 字段不是 JSON 对象")
    return value


def vm_model(row: dict[str, str]) -> str:
    try:
        additional_info = json.loads(row.get("additionalInfo") or "{}")
    except json.JSONDecodeError:
        additional_info = {}
    return str(additional_info.get("ServiceType") or row.get("meterName") or "")


def resource_region(row: dict[str, str]) -> str:
    return (
        row.get("meterRegion")
        or row.get("resourceLocation")
        or row.get("location")
        or ""
    )


def amount(row: dict[str, str], field: str) -> Decimal:
    return Decimal(row.get(field) or "0")


def reconcile(
    before_files: list[Path], after_dir: Path, output_dir: Path
) -> ReconcileResult:
    records: defaultdict[str, dict[str, object]] = defaultdict(
        lambda: {
            "project": "<missing>",
            "resourceType": "",
            "region": "",
            "resourceLocation": "",
            "vmModel": "",
            "before": Decimal("0"),
            "after": Decimal("0"),
            "delta": Decimal("0"),
            "rows": 0,
            "changedRows": 0,
        }
    )

    for before_path in before_files:
        after_path = after_dir / before_path.name
        if not after_path.is_file():
            raise FileNotFoundError(f"找不到处理后账单：{after_path}")
        with before_path.open(encoding="utf-8-sig", newline="") as before_file, after_path.open(
            encoding="utf-8-sig", newline=""
        ) as after_file:
            before_rows = csv.DictReader(before_file)
            after_rows = csv.DictReader(after_file)
            sentinel = object()
            for index, pair in enumerate(
                zip_longest(before_rows, after_rows, fillvalue=sentinel),
                start=2,
            ):
                before, after = pair
                if before is sentinel or after is sentinel:
                    raise ValueError(f"{before_path} 与 {after_path} 行数不一致")
                if before.get("ResourceId") != after.get("ResourceId"):
                    raise ValueError(f"{before_path}:{index} 前后 ResourceId 不一致")
                if before.get("meterCategory") != "Virtual Machines":
                    continue
                resource_id = (before.get("ResourceId") or "").strip()
                if not resource_id:
                    continue
                record = records[resource_id]
                tags = tags_from_row(before)
                record["project"] = str(tags.get("projname") or "<missing>")
                record["resourceType"] = (
                    "VMSS"
                    if "/virtualMachineScaleSets/" in resource_id
                    else "VM"
                )
                record["region"] = record["region"] or resource_region(before)
                record["resourceLocation"] = (
                    record["resourceLocation"] or before.get("resourceLocation") or ""
                )
                record["vmModel"] = record["vmModel"] or vm_model(before)
                before_cost = amount(before, "costInBillingCurrency")
                after_cost = amount(
                    after,
                    "allocatedCostInBillingCurrency",
                )
                delta = after_cost - before_cost
                record["before"] += before_cost
                record["after"] += after_cost
                record["delta"] += delta
                record["rows"] += 1
                if delta:
                    record["changedRows"] += 1

    output_dir.mkdir(parents=True, exist_ok=True)
    all_path = output_dir / "vm-cost-comparison.csv"
    changed_path = output_dir / "changed-vm-cost-comparison.csv"
    headers = [
        "projname",
        "resourceType",
        "region",
        "resourceLocation",
        "vmModel",
        "ResourceId",
        "rowCount",
        "changedRowCount",
        "beforeCostInBillingCurrency",
        "afterAllocatedCostInBillingCurrency",
        "feeChangeInBillingCurrency",
    ]
    sorted_records = sorted(
        records.items(), key=lambda item: (item[1]["delta"], item[0])
    )
    for path, only_changed in ((all_path, False), (changed_path, True)):
        with path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.writer(target)
            writer.writerow(headers)
            for resource_id, record in sorted_records:
                if only_changed and not record["delta"]:
                    continue
                writer.writerow(
                    [
                        record["project"],
                        record["resourceType"],
                        record["region"],
                        record["resourceLocation"],
                        record["vmModel"],
                        resource_id,
                        record["rows"],
                        record["changedRows"],
                        str(record["before"]),
                        str(record["after"]),
                        str(record["delta"]),
                    ]
                )

    before_total = sum(
        (record["before"] for record in records.values()), Decimal("0")
    )
    after_total = sum(
        (record["after"] for record in records.values()), Decimal("0")
    )
    return ReconcileResult(
        resource_count=len(records),
        changed_resources=sum(1 for record in records.values() if record["delta"]),
        before_total=before_total,
        after_total=after_total,
        delta_total=after_total - before_total,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成 RI 分摊对账文件。")
    parser.add_argument("inputs", nargs="+", help="分摊前账单文件或 glob")
    parser.add_argument(
        "--after-dir", required=True, help="分摊后账单所在目录"
    )
    parser.add_argument(
        "--output-dir", default="ri-reconciliation", help="对账输出目录"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = reconcile(
        expand_inputs(args.inputs),
        Path(args.after_dir),
        Path(args.output_dir),
    )
    print(f"虚拟机资源数：{result.resource_count}")
    print(f"费用发生变化的虚拟机数：{result.changed_resources}")
    print(f"处理前合计：{result.before_total}")
    print(f"处理后合计：{result.after_total}")
    print(f"变化合计：{result.delta_total}")


if __name__ == "__main__":
    main()
