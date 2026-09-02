#!/usr/bin/env python3

import argparse
import csv
import glob
import re
import sys
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext
from pathlib import Path


getcontext().prec = 40

ALLOCATION_COLUMN = "riActualAmortizedAdjustment"
ORDER_ID_PATTERN = re.compile(r"/reservationOrders/([^/]+)", re.IGNORECASE)
ALLOCATION_QUANTUM = Decimal("0.000000000000001")


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Allocate each VM RI's Actual-versus-Amortized difference back to "
            "the corresponding VM rows in amortized Azure cost exports."
        )
    )
    parser.add_argument(
        "--actual",
        nargs="+",
        default=["Actual_*.csv"],
        help="Actual CSV files or glob patterns (default: Actual_*.csv)",
    )
    parser.add_argument(
        "--amortized",
        nargs="+",
        default=["Amortized_*.csv"],
        help="Amortized CSV files or glob patterns (default: Amortized_*.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="ri_reallocated",
        help="Directory for new amortized CSV files (default: ri_reallocated)",
    )
    parser.add_argument(
        "--actual-amount-field",
        default="costInBillingCurrency",
        choices=("costInBillingCurrency", "costInPricingCurrency", "costInUsd"),
        help=(
            "Cost column read from Actual CSV files "
            "(default: costInBillingCurrency)"
        ),
    )
    parser.add_argument(
        "--amortized-amount-field",
        default="costInBillingCurrency",
        choices=("costInBillingCurrency", "costInPricingCurrency", "costInUsd"),
        help=(
            "Cost column read from Amortized CSV files and used for allocation "
            "(default: costInBillingCurrency)"
        ),
    )
    return parser.parse_args()


def expand_inputs(patterns):
    paths = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            paths.extend(Path(match) for match in matches)
        elif Path(pattern).is_file():
            paths.append(Path(pattern))

    unique_paths = sorted({path.resolve() for path in paths})
    if not unique_paths:
        raise ValueError(f"No CSV files matched: {patterns}")
    return unique_paths


def decimal_value(row, column):
    value = row.get(column, "")
    try:
        return Decimal(value or "0")
    except InvalidOperation as error:
        raise ValueError(f"Invalid decimal in column {column}: {value!r}") from error


def decimal_text(value):
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def is_vm_reservation(row):
    return (
        row.get("pricingModel", "").casefold() == "reservation"
        and row.get("meterCategory", "").casefold() == "virtual machines"
    )


def is_vm_resource_id(resource_id):
    normalized = resource_id.casefold()
    return (
        "/virtualmachines/" in normalized
        or "/virtualmachinescalesets/" in normalized
    )


def reservation_order_id(row, actual=False):
    if actual:
        return row.get("reservationId", "").strip()

    match = ORDER_ID_PATTERN.search(row.get("benefitId", ""))
    return match.group(1) if match else ""


def reconciled_cost_column(amount_field):
    return f"{amount_field}AfterActualReconciliation"


def read_csv_files(paths, amount_field):
    files = []
    required_columns = {
        "chargeType",
        amount_field,
        "meterCategory",
        "pricingModel",
        "quantity",
        "reservationId",
        "benefitId",
        "ResourceId",
    }

    for path in paths:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            missing = required_columns.difference(fieldnames)
            if missing:
                raise ValueError(
                    f"{path} is missing required columns: {', '.join(sorted(missing))}"
                )
            files.append(
                {
                    "path": path,
                    "fieldnames": fieldnames,
                    "rows": list(reader),
                }
            )
    return files


def collect_actual_costs(files, amount_field):
    actual_costs = defaultdict(Decimal)
    names = {}

    for file_data in files:
        for row in file_data["rows"]:
            if not is_vm_reservation(row):
                continue
            if row.get("chargeType", "").casefold() != "purchase":
                continue

            order_id = reservation_order_id(row, actual=True)
            if not order_id:
                raise ValueError(
                    f"VM RI Purchase row has no reservationId in {file_data['path']}"
                )
            actual_costs[order_id] += decimal_value(row, amount_field)
            names[order_id] = row.get("reservationName", "")

    return actual_costs, names


def collect_amortized_data(files, amount_field):
    amortized_costs = defaultdict(Decimal)
    eligible_rows = defaultdict(list)
    names = {}
    unused_reservations = defaultdict(
        lambda: {
            "name": "",
            "row_count": 0,
            "dates": set(),
            "quantity": Decimal(0),
            "cost": Decimal(0),
        }
    )

    for file_index, file_data in enumerate(files):
        for row_index, row in enumerate(file_data["rows"]):
            if not is_vm_reservation(row):
                continue

            order_id = reservation_order_id(row)
            if not order_id:
                raise ValueError(
                    "VM RI amortized row has no reservation order ID in benefitId: "
                    f"{file_data['path']} row {row_index + 2}"
                )

            amortized_costs[order_id] += decimal_value(
                row, amount_field
            )
            names[order_id] = row.get("reservationName", "")

            if row.get("chargeType", "").casefold() == "unusedreservation":
                unused = unused_reservations[order_id]
                unused["name"] = row.get("reservationName", "")
                unused["row_count"] += 1
                if row.get("date"):
                    unused["dates"].add(row["date"])
                unused["quantity"] += decimal_value(row, "quantity")
                unused["cost"] += decimal_value(
                    row, amount_field
                )

            resource_id = row.get("ResourceId", "").casefold()
            if (
                row.get("chargeType", "").casefold() == "usage"
                and is_vm_resource_id(resource_id)
            ):
                eligible_rows[order_id].append((file_index, row_index))

    return amortized_costs, eligible_rows, unused_reservations, names


def allocate_differences(
    amortized_files,
    actual_costs,
    amortized_costs,
    eligible_rows,
    amount_field,
):
    allocations = {}
    summary = []

    order_ids = sorted(set(actual_costs) | set(amortized_costs))
    for order_id in order_ids:
        actual_cost = actual_costs.get(order_id, Decimal(0))
        amortized_cost = amortized_costs.get(order_id, Decimal(0))
        difference = actual_cost - amortized_cost
        targets = eligible_rows.get(order_id, [])

        if difference == 0:
            summary.append((order_id, actual_cost, amortized_cost, difference))
            continue

        if not targets:
            raise ValueError(
                f"RI order {order_id} has a difference of {difference}, "
                "but no corresponding VM Usage rows were found"
            )

        weights = []
        for file_index, row_index in targets:
            row = amortized_files[file_index]["rows"][row_index]
            weights.append(decimal_value(row, amount_field))

        total_weight = sum(weights, Decimal(0))
        if total_weight == 0:
            weights = [
                decimal_value(
                    amortized_files[file_index]["rows"][row_index], "quantity"
                )
                for file_index, row_index in targets
            ]
            total_weight = sum(weights, Decimal(0))
            print(
                "WARNING: RI order "
                f"{order_id} has VM Usage rows but their {amount_field} "
                "total is zero; falling back to quantity-based allocation "
                f"(rows={len(targets)}, quantityTotal={decimal_text(total_weight)})",
                file=sys.stderr,
            )

        if total_weight == 0:
            raise ValueError(
                f"RI order {order_id} has no positive cost or quantity available "
                "for proportional allocation"
            )

        allocated = Decimal(0)
        for target_index, ((file_index, row_index), weight) in enumerate(
            zip(targets, weights)
        ):
            if target_index == len(targets) - 1:
                amount = difference - allocated
            else:
                amount = (difference * weight / total_weight).quantize(
                    ALLOCATION_QUANTUM, rounding=ROUND_HALF_UP
                )
                allocated += amount
            allocations[(file_index, row_index)] = amount

        summary.append((order_id, actual_cost, amortized_cost, difference))

    return allocations, summary


def write_outputs(files, output_dir, allocations, amount_field):
    output_dir.mkdir(parents=True, exist_ok=True)
    written_paths = []

    for file_index, file_data in enumerate(files):
        output_path = output_dir / file_data["path"].name
        if output_path.resolve() == file_data["path"].resolve():
            raise ValueError(f"Output path would overwrite input file: {output_path}")

        fieldnames = list(file_data["fieldnames"])
        reallocated_cost_column = reconciled_cost_column(amount_field)
        for column in (ALLOCATION_COLUMN, reallocated_cost_column):
            if column not in fieldnames:
                fieldnames.append(column)

        with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row_index, original_row in enumerate(file_data["rows"]):
                row = dict(original_row)
                allocation = allocations.get(
                    (file_index, row_index), Decimal(0)
                )
                original_cost = decimal_value(row, amount_field)
                row[ALLOCATION_COLUMN] = decimal_text(allocation)
                row[reallocated_cost_column] = decimal_text(
                    original_cost + allocation
                )
                writer.writerow(row)

        written_paths.append(output_path)
    return written_paths


def write_changed_rows(output_dir, files, allocations, amount_field):
    output_path = output_dir / "changed-amortized-vm-rows.csv"
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            [
                "reservationOrderId",
                "date",
                "subscriptionName",
                "resourceGroupName",
                "ResourceId",
                "originalCost",
                ALLOCATION_COLUMN,
                reconciled_cost_column(amount_field),
            ]
        )
        for (file_index, row_index), allocation in sorted(allocations.items()):
            if allocation == 0:
                continue
            row = files[file_index]["rows"][row_index]
            original_cost = decimal_value(row, amount_field)
            writer.writerow(
                [
                    reservation_order_id(row),
                    row.get("date", ""),
                    row.get("subscriptionName", ""),
                    row.get("resourceGroupName", ""),
                    row.get("ResourceId", ""),
                    decimal_text(original_cost),
                    decimal_text(allocation),
                    decimal_text(original_cost + allocation),
                ]
            )
    return output_path


def write_allocation_summary(
    output_dir,
    summary,
    names,
    actual_amount_field,
    amortized_amount_field,
):
    output_path = output_dir / "ri-allocation-summary.csv"
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(
            [
                "reservationOrderId",
                f"actual[{actual_amount_field}]",
                f"amortized[{amortized_amount_field}]",
                "difference",
                "reservationName",
            ]
        )
        for order_id, actual, amortized, difference in summary:
            writer.writerow(
                [
                    order_id,
                    decimal_text(actual),
                    decimal_text(amortized),
                    decimal_text(difference),
                    names.get(order_id, ""),
                ]
            )
        writer.writerow(
            [
                "TOTAL",
                decimal_text(sum((item[1] for item in summary), Decimal(0))),
                decimal_text(sum((item[2] for item in summary), Decimal(0))),
                decimal_text(sum((item[3] for item in summary), Decimal(0))),
                "",
            ]
        )
    return output_path


def print_unused_reservation_warnings(unused_reservations, amount_field):
    if not unused_reservations:
        print("No UnusedReservation rows detected", file=sys.stderr)
        return

    print(
        "WARNING: Unused VM RI records detected in the amortized bill",
        file=sys.stderr,
    )
    writer = csv.writer(sys.stderr, lineterminator="\n")
    writer.writerow(
        [
            "reservationOrderId",
            "reservationName",
            "unusedRecordCount",
            "firstDate",
            "lastDate",
            "unusedQuantity",
            f"unusedAmount[{amount_field}]",
            "note",
        ]
    )
    for order_id, unused in sorted(unused_reservations.items()):
        dates = sorted(unused["dates"])
        quantity = unused["quantity"]
        cost = unused["cost"]
        note = ""
        if quantity == 0 and cost == 0:
            note = (
                "UnusedReservation rows exist, but exported quantity and cost "
                "are both zero; unused amount cannot be quantified"
            )
        writer.writerow(
            [
                order_id,
                unused["name"],
                unused["row_count"],
                dates[0] if dates else "",
                dates[-1] if dates else "",
                decimal_text(quantity),
                decimal_text(cost),
                note,
            ]
        )


def main():
    args = parse_args()
    actual_paths = expand_inputs(args.actual)
    amortized_paths = expand_inputs(args.amortized)
    output_dir = Path(args.output_dir).resolve()

    actual_files = read_csv_files(actual_paths, args.actual_amount_field)
    amortized_files = read_csv_files(
        amortized_paths, args.amortized_amount_field
    )
    actual_costs, actual_names = collect_actual_costs(
        actual_files, args.actual_amount_field
    )
    amortized_costs, eligible_rows, unused_reservations, amortized_names = (
        collect_amortized_data(
            amortized_files, args.amortized_amount_field
        )
    )
    names = {**amortized_names, **actual_names}
    allocations, summary = allocate_differences(
        amortized_files,
        actual_costs,
        amortized_costs,
        eligible_rows,
        args.amortized_amount_field,
    )
    written_paths = write_outputs(
        amortized_files,
        output_dir,
        allocations,
        args.amortized_amount_field,
    )

    print_unused_reservation_warnings(
        unused_reservations, args.amortized_amount_field
    )
    write_changed_rows(
        output_dir,
        amortized_files,
        allocations,
        args.amortized_amount_field,
    )
    write_allocation_summary(
        output_dir,
        summary,
        names,
        args.actual_amount_field,
        args.amortized_amount_field,
    )


if __name__ == "__main__":
    main()
