#!/usr/bin/env python3
"""Combine VM, MySQL and AMR RI reallocations from one reconciled bill."""

import argparse
import csv
import json
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

from reallocate_vm_ri import parse_tags


AMOUNT = "costInBillingCurrencyAfterActualReconciliation"
ALLOCATED = "allocatedCostInBillingCurrency"
ADJUSTMENT = "riAllocationAmount"
FIELDS = (
    ALLOCATED, ADJUSTMENT, "riPaygEquivalentAmount", "riAmortizedCost",
    "riBenefitOrLoss", "allocationType", "allocationTarget",
)
SCRIPTS = ("reallocate_vm_ri.py", "reallocate_mysql_ri.py", "reallocate_amr_ri.py")


def combine(input_paths, branches, output_dir, project_tag_key):
    project_before = {}
    project_delta = {}
    total_adjustment = Decimal(0)
    summaries = [json.loads((branch / "ri-summary.json").read_text()) for branch in branches]
    ids = set()
    savings = {}
    for summary in summaries:
        overlap = ids.intersection(summary["reservationIds"])
        if overlap:
            raise ValueError(f"RI IDs repeated across services: {sorted(overlap)}")
        ids.update(summary["reservationIds"])
        savings.update(summary["riSavingsByReservation"])

    for source in input_paths:
        readers = []
        handles = []
        try:
            for branch in branches:
                handle = (branch / source.name).open(newline="", encoding="utf-8-sig")
                handles.append(handle)
                readers.append(csv.DictReader(handle))
            names = readers[0].fieldnames
            if not names or any(reader.fieldnames != names for reader in readers[1:]):
                raise ValueError(f"Branch bill headers differ: {source.name}")
            with (output_dir / source.name).open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=names)
                writer.writeheader()
                sentinel = object()
                while True:
                    records = [next(reader, sentinel) for reader in readers]
                    if all(record is sentinel for record in records):
                        break
                    if any(record is sentinel for record in records):
                        raise ValueError(f"Branch bill lengths differ: {source.name}")
                    originals = [
                        {key: value for key, value in row.items() if key not in FIELDS}
                        for row in records
                    ]
                    if any(original != originals[0] for original in originals[1:]):
                        raise ValueError(f"Branch bill rows differ: {source.name}")
                    adjustments = [Decimal(row[ADJUSTMENT]) for row in records]
                    active = [index for index, row in enumerate(records)
                              if row["riBenefitOrLoss"] or adjustments[index]]
                    if len(active) > 1:
                        raise ValueError(f"Multiple RI services changed one bill row: {source.name}")
                    result = dict(records[active[0] if active else 0])
                    adjustment = sum(adjustments, Decimal(0))
                    base = Decimal(result[AMOUNT])
                    result[ADJUSTMENT] = str(adjustment)
                    result[ALLOCATED] = str(base + adjustment)
                    writer.writerow(result)
                    project = str(parse_tags(result.get("tags", "")).get(project_tag_key)
                                  or "<missing>")
                    project_before[project] = project_before.get(project, Decimal(0)) + base
                    project_delta[project] = project_delta.get(project, Decimal(0)) + adjustment
                    total_adjustment += adjustment
        finally:
            for handle in handles:
                handle.close()
    if abs(total_adjustment) > Decimal("1E-15"):
        raise ValueError(f"Combined RI adjustment is not conserved: {total_adjustment}")

    details = output_dir / "ri-allocation-details.csv"
    with details.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        for index, branch in enumerate(branches):
            with (branch / "ri-allocation-details.csv").open(newline="", encoding="utf-8-sig") as source:
                reader = csv.reader(source)
                header = next(reader)
                if index == 0:
                    writer.writerow(header)
                else:
                    if header != detail_header:
                        raise ValueError("Branch detail headers differ")
                detail_header = header
                writer.writerows(reader)

    added = {}
    assigned = {}
    for branch in branches:
        with (branch / "project-allocation.csv").open(newline="", encoding="utf-8-sig") as source:
            for row in csv.DictReader(source):
                project = row["projname"]
                for target, field in ((added, "riAmountAdded"), (assigned, "riAmountAssigned")):
                    target[project] = target.get(project, Decimal(0)) + Decimal(row[field])
    with (output_dir / "project-allocation.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["projname", "beforeAmount", "afterAllocatedAmount",
                         "delta", "riAmountAdded", "riAmountAssigned"])
        for project in sorted(project_before):
            before = project_before[project]
            delta = project_delta[project]
            writer.writerow([project, str(before), str(before + delta), str(delta),
                             str(added.get(project, 0)), str(assigned.get(project, 0))])

    summary = dict(summaries[0])
    for field in ("mappings", "targets", "reservationIds", "riAllocationKeys"):
        summary[field] = sorted(
            (entry for item in summaries for entry in item[field]),
            key=str,
        )
    for field in ("riUsageRows", "riReallocatedRows", "riSizeFlexibleRows"):
        summary[field] = sum(item[field] for item in summaries)
    for field in ("riAllocatableAmount", "riRawTotalAmount", "riPaygEquivalentAmount",
                  "riAmortizedCost", "riGrossBenefit", "riExcessCost",
                  "riNetBenefitOrLoss", "riUnusedCost", "riPortfolioNetSavings",
                  "targetVmReceiverAmount"):
        summary[field] = str(sum((Decimal(item[field]) for item in summaries), Decimal(0)))
    for field in ("riServiceTypes", "matchModeByReservation"):
        summary[field] = {key: value for item in summaries for key, value in item[field].items()}
    summary["riSavingsByReservation"] = savings
    summary["skippedAllocations"] = [
        entry for item in summaries for entry in item.get("skippedAllocations", [])
    ]
    summary["skipUnallocatable"] = any(
        item.get("skipUnallocatable", False) for item in summaries
    )
    summary["skippedAllocationAmount"] = str(sum(
        (Decimal(item.get("skippedAllocationAmount", "0")) for item in summaries),
        Decimal(0),
    ))
    summary["assignedByTarget"] = {
        project: str(value) for project, value in sorted(assigned.items())
    }
    summary["allocationMode"] = "vm-mysql-amr"
    summary["costScope"] = "VM + MySQL Compute + Azure Managed Redis Balanced"
    summary["outputFiles"] = [str(output_dir / path.name) for path in input_paths] + [
        str(details), str(output_dir / "project-allocation.csv")
    ]
    (output_dir / "ri-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser(description="Merge VM, MySQL and AMR RI allocations.")
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--reservations-file", required=True, type=Path)
    parser.add_argument("--price-sheet-file", required=True, type=Path)
    parser.add_argument("--project-tag-key", required=True)
    parser.add_argument("--tenant-id",
                        help="Azure tenant for ManagementGroup lookup; passed to each RI branch")
    parser.add_argument("--skip-unallocatable", action="store_true",
                        help="Report unallocatable target pools instead of stopping")
    args = parser.parse_args()
    inputs = sorted(path.resolve() for path in args.input_dir.glob("*.csv")
                    if path.name not in ("ri-allocation-summary.csv", "changed-amortized-ri-rows.csv",
                                         "changed-amortized-vm-rows.csv"))
    if not inputs:
        raise ValueError(f"No reconciled bill CSVs in {args.input_dir}")
    if len({path.name for path in inputs}) != len(inputs):
        raise ValueError("Reconciled bill filenames must be unique")
    output = args.output_dir.resolve()
    if output == args.input_dir.resolve():
        raise ValueError("Output directory cannot overwrite input directory")
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"Output directory is not empty: {output}")
    for name in SCRIPTS:
        if not (Path(__file__).with_name(name)).is_file():
            raise FileNotFoundError(name)
    with tempfile.TemporaryDirectory(prefix="ri-branches-") as directory:
        branches = [Path(directory) / str(index) for index in range(len(SCRIPTS))]
        for script, branch in zip(SCRIPTS, branches):
            subprocess.run(
                [sys.executable, str(Path(__file__).with_name(script)),
                 *(str(path) for path in inputs),
                 "--reservations-file", str(args.reservations_file.resolve()),
                 "--project-tag-key", args.project_tag_key,
                 "--amount-field", AMOUNT,
                 "--price-sheet-file", str(args.price_sheet_file.resolve()),
                 "--output-dir", str(branch),
                 *(["--tenant-id", args.tenant_id] if args.tenant_id is not None else []),
                 *(["--skip-unallocatable"] if args.skip_unallocatable else [])],
                check=True,
            )
        output.mkdir(parents=True, exist_ok=True)
        combine(inputs, branches, output, args.project_tag_key)
    print(f"Combined allocation: {output}")


if __name__ == "__main__":
    main()
