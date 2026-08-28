#!/usr/bin/env python3
"""Generate per-RI project distributions before and after benefit allocation."""

from __future__ import annotations

import argparse
import csv
import html
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path


MISSING_PROJECT = "<missing>"
ZERO = Decimal("0")
RECONCILIATION_TOLERANCE = Decimal("1E-15")


@dataclass
class DistributionResult:
    configured_reservation_count: int
    reservation_count: int
    project_row_count: int
    gross_savings: Decimal
    csv_path: Path
    html_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取 RI 分摊结果，生成每个 RI 分摊前后的项目收益分布。"
    )
    parser.add_argument("input_dir", help="reallocate_vm_ri.py 的输出目录")
    parser.add_argument(
        "--project-tag-key",
        required=True,
        help="项目名称对应的资源标签键，例如 projname",
    )
    parser.add_argument(
        "--output-dir",
        help="报表输出目录，默认在输入目录下创建 ri-project-distribution",
    )
    parser.add_argument(
        "--price-sheet-file",
        help="Azure Price Sheet；默认使用 ri-summary.json 中的 priceSheetSource",
    )
    parser.add_argument(
        "--reservations-file",
        help="预留定义文件；默认查找输入目录或其上级目录中的 reservations.json",
    )
    return parser.parse_args()


def decimal_value(value: str | None, field: str) -> Decimal:
    try:
        return Decimal(value or "0")
    except InvalidOperation as exc:
        raise ValueError(f"{field} 不是有效金额：{value!r}") from exc


def project_from_row(row: dict[str, str], project_tag_key: str) -> str:
    raw = row.get("tags") or ""
    if not raw.strip():
        tags = {}
    else:
        try:
            tags = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("tags 字段不是有效 JSON") from exc
        if not isinstance(tags, dict):
            raise ValueError("tags 字段不是 JSON 对象")
    return str(
        tags.get(project_tag_key)
        or row.get("resourceGroupName")
        or row.get("subscriptionName")
        or MISSING_PROJECT
    )


def discover_allocated_csvs(input_dir: Path) -> list[Path]:
    result: list[Path] = []
    required = {"reservationId", "riBenefitOrLoss", "tags"}
    for path in sorted(input_dir.glob("*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as source:
            fieldnames = set(csv.DictReader(source).fieldnames or [])
        if required <= fieldnames:
            result.append(path)
    if not result:
        raise FileNotFoundError(
            f"{input_dir} 中找不到包含 RI 分摊字段的账单 CSV"
        )
    return result


def load_summary(
    input_dir: Path,
) -> tuple[set[str], dict[str, Decimal], str]:
    path = input_dir / "ri-summary.json"
    if not path.is_file():
        raise FileNotFoundError(f"找不到分摊汇总文件：{path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    reservation_ids = payload.get("reservationIds")
    if not isinstance(reservation_ids, list):
        raise ValueError("ri-summary.json 缺少 reservationIds 列表")
    ids = {str(value).strip() for value in reservation_ids if str(value).strip()}
    savings = payload.get("riSavingsByReservation") or {}
    if not isinstance(savings, dict):
        raise ValueError("ri-summary.json 的 riSavingsByReservation 不是对象")
    gross_by_reservation = {
        reservation_id: decimal_value(
            (savings.get(reservation_id) or {}).get("netBenefitOrLoss"),
            f"riSavingsByReservation.{reservation_id}.netBenefitOrLoss",
        )
        for reservation_id in ids
    }
    return ids, gross_by_reservation, str(payload.get("priceSheetSource") or "")


def resolve_price_sheet_path(
    input_dir: Path, configured_path: Path | None, summary_source: str
) -> Path:
    if configured_path is not None:
        candidates = [configured_path]
    elif summary_source and not summary_source.startswith(("http://", "https://")):
        source = Path(summary_source)
        candidates = [
            source,
            input_dir / source,
            input_dir.parent / source.name,
            Path(__file__).resolve().parent.parent / source,
        ]
    else:
        candidates = []
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise FileNotFoundError(
        "未分摊 RI 需要 Price Sheet 计算 PAYG 等价成本；"
        "请通过 --price-sheet-file 指定文件"
    )


def reservation_id_from_config(item: dict[str, object]) -> str:
    value = str(
        item.get("externalReservationId") or item.get("reservationId") or ""
    ).strip()
    marker = "/reservations/"
    if marker in value:
        value = value.rsplit(marker, 1)[1].strip("/").split("/", 1)[0]
    return value


def load_reservations_config(
    input_dir: Path, configured_path: Path | None
) -> tuple[set[str], set[str]]:
    candidates = (
        [configured_path]
        if configured_path is not None
        else [input_dir / "reservations.json", input_dir.parent / "reservations.json"]
    )
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        raise FileNotFoundError(
            "找不到 reservations.json；请通过 --reservations-file 指定"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload if isinstance(payload, list) else payload.get("reservations")
    if not isinstance(items, list):
        raise ValueError("reservations 文件必须是数组或包含 reservations 数组")
    all_ids: set[str] = set()
    valid_binding_ids: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("reservations 条目必须是对象")
        reservation_id = reservation_id_from_config(item)
        if not reservation_id:
            raise ValueError("reservations 条目缺少 reservationId")
        if reservation_id in all_ids:
            raise ValueError(f"reservations 文件存在重复 RI：{reservation_id}")
        all_ids.add(reservation_id)
        for binding in item.get("bindings") or []:
            if not isinstance(binding, dict):
                continue
            project = str(binding.get("project") or "").strip()
            quantity = decimal_value(
                str(binding.get("boundQuantity") or "0"), "boundQuantity"
            )
            if project and quantity > ZERO:
                valid_binding_ids.add(reservation_id)
                break
    return all_ids, valid_binding_ids


def build_distribution(
    input_dir: Path,
    output_dir: Path,
    project_tag_key: str,
    price_sheet_file: Path | None = None,
    reservations_file: Path | None = None,
) -> DistributionResult:
    details_path = input_dir / "ri-allocation-details.csv"
    if not details_path.is_file():
        raise FileNotFoundError(
            f"找不到 {details_path}；分摊时不能使用 --summary-only"
        )
    (
        configured_reservation_ids,
        summary_gross_by_reservation,
        summary_price_sheet_source,
    ) = load_summary(input_dir)
    all_reservation_ids, valid_binding_ids = load_reservations_config(
        input_dir, reservations_file
    )
    allocated_paths = discover_allocated_csvs(input_dir)

    row_projects: dict[tuple[str, int], str] = {}
    before: defaultdict[tuple[str, str], Decimal] = defaultdict(Decimal)
    gross_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)
    payg_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)
    original_amortized_by_reservation: defaultdict[str, Decimal] = defaultdict(
        Decimal
    )
    amortized_by_reservation: defaultdict[str, Decimal] = defaultdict(Decimal)
    reservation_names: dict[str, str] = {}
    price_rates = None

    for path in allocated_paths:
        with path.open(encoding="utf-8-sig", newline="") as source:
            for row_number, row in enumerate(csv.DictReader(source), start=2):
                project = project_from_row(row, project_tag_key)
                row_projects[(path.name, row_number)] = project
                reservation_id = (row.get("reservationId") or "").strip()
                is_ri_usage = (
                    reservation_id
                    and row.get("chargeType") == "Usage"
                    and row.get("pricingModel") == "Reservation"
                )
                has_calculated_savings = bool(
                    (row.get("riBenefitOrLoss") or "").strip()
                )
                if reservation_id and (is_ri_usage or has_calculated_savings):
                    reservation_name = (row.get("reservationName") or "").strip()
                    if reservation_name:
                        reservation_names.setdefault(reservation_id, reservation_name)
                if not is_ri_usage and not has_calculated_savings:
                    continue
                if not reservation_id:
                    continue
                original_amortized_cost = decimal_value(
                    row.get("costInBillingCurrency"),
                    "costInBillingCurrency",
                )
                if has_calculated_savings:
                    payg_equivalent = decimal_value(
                        row.get("riPaygEquivalentAmount"),
                        "riPaygEquivalentAmount",
                    )
                    amortized_cost = decimal_value(
                        row.get("riAmortizedCost"), "riAmortizedCost"
                    )
                    gross_savings = decimal_value(
                        row.get("riBenefitOrLoss"), "riBenefitOrLoss"
                    )
                else:
                    if price_rates is None:
                        import reallocate_vm_ri

                        price_path = resolve_price_sheet_path(
                            input_dir,
                            price_sheet_file,
                            summary_price_sheet_source,
                        )
                        price_rates = reallocate_vm_ri.parse_price_sheet(
                            price_path.read_bytes(), str(price_path)
                        )
                    quantity = decimal_value(row.get("quantity"), "quantity")
                    reconciled_cost = row.get(
                        "costInBillingCurrencyAfterActualReconciliation"
                    )
                    amortized_cost = decimal_value(
                        (
                            reconciled_cost
                            if reconciled_cost not in (None, "")
                            else row.get("costInBillingCurrency")
                        ),
                        "costInBillingCurrencyAfterActualReconciliation",
                    )
                    payg_equivalent = (
                        reallocate_vm_ri.price_for_row(row, price_rates) * quantity
                    )
                    gross_savings = payg_equivalent - amortized_cost
                before[(reservation_id, project)] += gross_savings
                gross_by_reservation[reservation_id] += gross_savings
                payg_by_reservation[reservation_id] += payg_equivalent
                original_amortized_by_reservation[
                    reservation_id
                ] += original_amortized_cost
                amortized_by_reservation[reservation_id] += amortized_cost

    moved_from_source: defaultdict[tuple[str, str], Decimal] = defaultdict(Decimal)
    assigned_to_target: defaultdict[tuple[str, str], Decimal] = defaultdict(Decimal)
    with details_path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        required = {
            "sourceFile",
            "sourceRowNumber",
            "allocationType",
            "allocationTarget",
            "riAllocationReservationIds",
            "allocationAmount",
        }
        if not required <= set(reader.fieldnames or []):
            missing = sorted(required - set(reader.fieldnames or []))
            raise ValueError(
                "ri-allocation-details.csv 缺少字段：" + ", ".join(missing)
            )
        for detail_row_number, row in enumerate(reader, start=2):
            reservation_id = (row["riAllocationReservationIds"] or "").strip()
            if not reservation_id:
                raise ValueError(
                    f"{details_path}:{detail_row_number} 缺少 RI ID"
                )
            amount = decimal_value(row["allocationAmount"], "allocationAmount")
            allocation_type = (row["allocationType"] or "").strip()
            if allocation_type == "RI_USAGE_COST_REASSIGNED":
                source_key = (
                    Path(row["sourceFile"]).name,
                    int(row["sourceRowNumber"]),
                )
                if source_key not in row_projects:
                    raise ValueError(
                        f"{details_path}:{detail_row_number} 找不到对应的分摊后账单行 "
                        f"{source_key[0]}:{source_key[1]}"
                    )
                moved_from_source[(reservation_id, row_projects[source_key])] += amount
            elif allocation_type == "RI_BENEFIT_ASSIGNED":
                target = (row["allocationTarget"] or "").strip()
                if not target:
                    raise ValueError(
                        f"{details_path}:{detail_row_number} 缺少分摊目标项目"
                    )
                assigned_to_target[(reservation_id, target)] -= amount
            else:
                raise ValueError(
                    f"{details_path}:{detail_row_number} 存在未知 allocationType："
                    f"{allocation_type!r}"
                )

    after: defaultdict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for key, amount in before.items():
        retained = amount - moved_from_source.get(key, ZERO)
        after[key] += retained
    for key, amount in moved_from_source.items():
        if key not in before and amount:
            raise ValueError(f"RI {key[0]} 项目 {key[1]!r} 没有分摊前收益")
    for key, amount in assigned_to_target.items():
        after[key] += amount

    usage_reservation_ids = set(gross_by_reservation)
    unknown_usage_ids = usage_reservation_ids - all_reservation_ids
    if unknown_usage_ids:
        raise ValueError(
            "账单存在 reservations.json 未定义的 RI："
            + ", ".join(sorted(unknown_usage_ids))
        )
    classification_counts = {
        "valid_with_usage": len(valid_binding_ids & usage_reservation_ids),
        "invalid_with_usage": len(usage_reservation_ids - valid_binding_ids),
        "valid_without_usage": len(valid_binding_ids - usage_reservation_ids),
        "invalid_without_usage": len(
            all_reservation_ids - valid_binding_ids - usage_reservation_ids
        ),
    }
    reservation_ids = sorted(usage_reservation_ids)
    for reservation_id in reservation_ids:
        before_total = sum(
            (
                amount
                for (current_id, _project), amount in before.items()
                if current_id == reservation_id
            ),
            ZERO,
        )
        after_total = sum(
            (
                amount
                for (current_id, _project), amount in after.items()
                if current_id == reservation_id
            ),
            ZERO,
        )
        if abs(before_total - after_total) > RECONCILIATION_TOLERANCE:
            raise ValueError(
                f"RI {reservation_id} 收益不守恒：分摊前 {before_total}，"
                f"分摊后 {after_total}，差额 {after_total - before_total}"
            )
        if (
            reservation_id in configured_reservation_ids
            and abs(
                before_total - summary_gross_by_reservation[reservation_id]
            )
            > RECONCILIATION_TOLERANCE
        ):
            raise ValueError(
                f"RI {reservation_id} 的明细净收益/损失 {before_total} 与 "
                f"ri-summary.json 的 {summary_gross_by_reservation[reservation_id]} 不一致"
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "ri-project-distribution.csv"
    rows: list[dict[str, str]] = []
    for reservation_id in reservation_ids:
        total = gross_by_reservation.get(
            reservation_id, summary_gross_by_reservation.get(reservation_id, ZERO)
        )
        projects = sorted(
            {
                project
                for current_id, project in set(before) | set(after)
                if current_id == reservation_id
            }
        )
        if not projects:
            projects = [""]
        allocation_status = (
            "ALLOCATED"
            if any(
                current_id == reservation_id and amount != ZERO
                for (current_id, _project), amount in assigned_to_target.items()
            )
            else "NOT_ALLOCATED"
        )
        for project in projects:
            before_amount = before.get((reservation_id, project), ZERO)
            after_amount = after.get((reservation_id, project), ZERO)
            rows.append(
                {
                    "reservationId": reservation_id,
                    "reservationName": reservation_names.get(reservation_id, ""),
                    "classification": (
                        "VALID_BINDING_WITH_USAGE"
                        if reservation_id in valid_binding_ids
                        else "NO_VALID_BINDING_WITH_USAGE"
                    ),
                    "allocationStatus": allocation_status,
                    "project": project,
                    "paygEquivalentCost": str(payg_by_reservation[reservation_id]),
                    "riAmortizedCostBeforeActualReconciliation": str(
                        original_amortized_by_reservation[reservation_id]
                    ),
                    "riAmortizedCostAfterActualReconciliation": str(
                        amortized_by_reservation[reservation_id]
                    ),
                    "riAmortizedCost": str(amortized_by_reservation[reservation_id]),
                    "netBenefitOrLoss": str(total),
                    "beforeBenefit": str(before_amount),
                    "beforeShare": str(before_amount / total if total else ZERO),
                    "afterBenefit": str(after_amount),
                    "afterShare": str(after_amount / total if total else ZERO),
                    "benefitChange": str(after_amount - before_amount),
                }
            )

    fieldnames = [
        "reservationId",
        "reservationName",
        "classification",
        "allocationStatus",
        "project",
        "paygEquivalentCost",
        "riAmortizedCostBeforeActualReconciliation",
        "riAmortizedCostAfterActualReconciliation",
        "riAmortizedCost",
        "netBenefitOrLoss",
        "beforeBenefit",
        "beforeShare",
        "afterBenefit",
        "afterShare",
        "benefitChange",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    html_path = output_dir / "ri-project-distribution.html"
    write_html_report(
        html_path, rows, reservation_ids, classification_counts
    )
    return DistributionResult(
        configured_reservation_count=len(all_reservation_ids),
        reservation_count=len(reservation_ids),
        project_row_count=len(rows),
        gross_savings=sum(
            (
                gross_by_reservation.get(
                    reservation_id,
                    summary_gross_by_reservation.get(reservation_id, ZERO),
                )
                for reservation_id in reservation_ids
            ),
            ZERO,
        ),
        csv_path=csv_path,
        html_path=html_path,
    )


def format_amount(value: str) -> str:
    return f"{Decimal(value):,.4f}"


def format_percent(value: str) -> str:
    return f"{Decimal(value) * 100:.2f}%"


def write_html_report(
    path: Path,
    rows: list[dict[str, str]],
    reservation_ids: list[str],
    classification_counts: dict[str, int],
) -> None:
    grouped: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["reservationId"]].append(row)

    sections: list[str] = []
    for reservation_id in reservation_ids:
        ri_rows = grouped[reservation_id]
        if not ri_rows:
            continue
        name = ri_rows[0]["reservationName"]
        total = Decimal(ri_rows[0]["netBenefitOrLoss"])
        payg_cost = Decimal(ri_rows[0]["paygEquivalentCost"])
        amortized_cost_before = Decimal(
            ri_rows[0]["riAmortizedCostBeforeActualReconciliation"]
        )
        amortized_cost_after = Decimal(
            ri_rows[0]["riAmortizedCostAfterActualReconciliation"]
        )
        allocated = ri_rows[0]["allocationStatus"] == "ALLOCATED"
        title = html.escape(reservation_id)
        if name:
            title += f" · {html.escape(name)}"
        table_rows = []
        distribution_rows = [row for row in ri_rows if row["project"]]
        for row in sorted(
            distribution_rows,
            key=lambda item: (-Decimal(item["afterBenefit"]), item["project"]),
        ):
            before_width = max(ZERO, Decimal(row["beforeShare"]) * 100)
            after_width = max(ZERO, Decimal(row["afterShare"]) * 100)
            change = Decimal(row["benefitChange"])
            change_class = "positive" if change > ZERO else "negative" if change < ZERO else ""
            table_rows.append(
                "<tr>"
                f"<td>{html.escape(row['project'])}</td>"
                f"<td class=\"number\">{format_amount(row['beforeBenefit'])}</td>"
                f"<td class=\"number\">{format_percent(row['beforeShare'])}</td>"
                f"<td><div class=\"bar before\" style=\"width:{before_width}%\"></div></td>"
                f"<td class=\"number\">{format_amount(row['afterBenefit'])}</td>"
                f"<td class=\"number\">{format_percent(row['afterShare'])}</td>"
                f"<td><div class=\"bar after\" style=\"width:{after_width}%\"></div></td>"
                f"<td class=\"number {change_class}\">{format_amount(row['benefitChange'])}</td>"
                "</tr>"
            )
        sections.append(
            "<section>"
            f"<h2>{title}</h2>"
            "<div class=\"metrics\">"
            f"<span>按需等价成本<strong>{payg_cost:,.4f}</strong></span>"
            f"<span>差额调整前 RI 摊销成本<strong>{amortized_cost_before:,.4f}</strong></span>"
            f"<span>差额调整后 RI 摊销成本<strong>{amortized_cost_after:,.4f}</strong></span>"
            f"<span>RI 净收益/损失<strong>{total:,.4f}</strong></span>"
            f"<span>状态<strong>{'已分摊' if allocated else '未分摊'}</strong></span>"
            "</div>"
            + (
                "<p class=\"empty\">该 RI 没有发生项目收益分摊。</p>"
                if not distribution_rows
                else
            "<table><thead><tr>"
            "<th>项目</th><th>分摊前收益</th><th>分摊前占比</th><th>分摊前分布</th>"
            "<th>分摊后收益</th><th>分摊后占比</th><th>分摊后分布</th><th>收益变化</th>"
            "</tr></thead><tbody>"
            + "".join(table_rows)
            + "</tbody></table>"
            )
            + "</section>"
        )

    document = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RI 经济责任项目分布</title>
<style>
body{font-family:Arial,"Microsoft YaHei",sans-serif;margin:32px;color:#172033;background:#f5f7fb}
h1{margin-bottom:8px}section{background:#fff;padding:20px;margin:20px 0;border-radius:8px;box-shadow:0 1px 4px #0002}
table{width:100%;border-collapse:collapse}th,td{padding:8px;border-bottom:1px solid #e5e9f0;text-align:left}
th{background:#f0f3f8}.number{text-align:right;font-variant-numeric:tabular-nums}
.bar{height:12px;border-radius:3px;min-width:1px}.before{background:#8da2c0}.after{background:#2f72d6}
.positive{color:#087f5b}.negative{color:#c92a2a}
.metrics{display:flex;gap:12px;flex-wrap:wrap;margin:12px 0 18px}.metrics span{background:#f0f3f8;padding:10px 14px;border-radius:6px}
.metrics strong{display:block;margin-top:4px}.empty{color:#667085;padding:16px 0}
</style>
</head>
<body>
<h1>RI 经济责任分摊前后项目分布</h1>
<p>正数表示 RI 收益，负数表示 RI 超额成本；分摊后包含未绑定保留部分和分配给目标项目的经济责任。</p>
<table class="classification">
<thead><tr><th>分类</th><th>RI 数量</th><th>项目分布展示</th></tr></thead>
<tbody>
<tr><td>有有效 binding 且有实际 Usage</td><td>""" + str(classification_counts["valid_with_usage"]) + """</td><td>展示</td></tr>
<tr><td>无有效 binding 但有实际 Usage</td><td>""" + str(classification_counts["invalid_with_usage"]) + """</td><td>展示，分摊前后不变</td></tr>
<tr><td>有有效 binding 但本账期无 Usage</td><td>""" + str(classification_counts["valid_without_usage"]) + """</td><td>不展示</td></tr>
<tr><td>无有效 binding 且本账期无 Usage</td><td>""" + str(classification_counts["invalid_without_usage"]) + """</td><td>不展示</td></tr>
</tbody></table>
""" + "".join(sections) + """
</body>
</html>
"""
    path.write_text(document, encoding="utf-8")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else input_dir / "ri-project-distribution"
    )
    result = build_distribution(
        input_dir,
        output_dir,
        args.project_tag_key,
        Path(args.price_sheet_file) if args.price_sheet_file else None,
        Path(args.reservations_file) if args.reservations_file else None,
    )
    print(f"reservations.json RI 数量：{result.configured_reservation_count}")
    print(f"展示项目分布的 RI 数量：{result.reservation_count}")
    print(f"RI/项目分布行数：{result.project_row_count}")
    print(f"RI 净收益/损失合计：{result.gross_savings}")
    print(f"CSV：{result.csv_path}")
    print(f"HTML：{result.html_path}")


if __name__ == "__main__":
    main()
