import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("reconcile_vm_ri_allocation.py")
SPEC = importlib.util.spec_from_file_location("reconcile_vm_ri_allocation", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ReconcileTests(unittest.TestCase):
    def test_reconcile_writes_changed_vm_rows_with_region_and_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            before = root / "before.csv"
            after_dir = root / "after"
            output_dir = root / "comparison"
            after_dir.mkdir()
            headers = [
                "meterCategory",
                "ResourceId",
                "meterRegion",
                "resourceLocation",
                "meterName",
                "additionalInfo",
                "tags",
                "costInBillingCurrency",
                "allocatedCostInBillingCurrency",
            ]
            rows = [
                {
                    "meterCategory": "Virtual Machines",
                    "ResourceId": "/subscriptions/s/resourceGroups/r/providers/Microsoft.Compute/virtualMachines/vm1",
                    "meterRegion": "AP Southeast",
                    "resourceLocation": "",
                    "meterName": "D8s v5",
                    "additionalInfo": '{"ServiceType":"Standard_D8s_v5"}',
                    "tags": '{"projname":"fota"}',
                    "costInBillingCurrency": "10",
                    "allocatedCostInBillingCurrency": "",
                }
            ]
            with before.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            with (after_dir / before.name).open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                rows[0]["allocatedCostInBillingCurrency"] = "8.5"
                writer.writerows(rows)

            result = MODULE.reconcile([before], after_dir, output_dir)
            self.assertEqual(result.changed_resources, 1)
            with (output_dir / "changed-vm-cost-comparison.csv").open(
                encoding="utf-8", newline=""
            ) as f:
                row = next(csv.DictReader(f))
            self.assertEqual(row["region"], "AP Southeast")
            self.assertEqual(row["vmModel"], "Standard_D8s_v5")
            self.assertEqual(row["feeChangeInBillingCurrency"], "-1.5")

    def test_reconcile_writes_per_project_ri_and_ondemand_costs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            before = root / "before.csv"
            after_dir = root / "after"
            output_dir = root / "comparison"
            after_dir.mkdir()
            headers = [
                "meterCategory",
                "ResourceId",
                "meterRegion",
                "resourceLocation",
                "meterName",
                "additionalInfo",
                "pricingModel",
                "tags",
                "costInBillingCurrency",
                "allocatedCostInBillingCurrency",
            ]
            base = {
                "meterCategory": "Virtual Machines",
                "meterRegion": "AP Southeast",
                "resourceLocation": "",
                "meterName": "D8s v5",
                "additionalInfo": '{"ServiceType":"Standard_D8s_v5"}',
            }
            before_rows = [
                {
                    **base,
                    "ResourceId": "/subscriptions/s/resourceGroups/r/providers/Microsoft.Compute/virtualMachines/vm-ri",
                    "pricingModel": "Reservation",
                    "tags": '{"projname":"alpha"}',
                    "costInBillingCurrency": "10",
                    "allocatedCostInBillingCurrency": "",
                },
                {
                    **base,
                    "ResourceId": "/subscriptions/s/resourceGroups/r/providers/Microsoft.Compute/virtualMachines/vm-od",
                    "pricingModel": "OnDemand",
                    "tags": '{"projname":"alpha"}',
                    "costInBillingCurrency": "6",
                    "allocatedCostInBillingCurrency": "",
                },
            ]
            after_rows = [dict(row) for row in before_rows]
            after_rows[0]["allocatedCostInBillingCurrency"] = "12"
            after_rows[1]["allocatedCostInBillingCurrency"] = "4"
            with before.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(before_rows)
            with (after_dir / before.name).open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(after_rows)

            MODULE.reconcile([before], after_dir, output_dir)
            with (output_dir / "project-ri-ondemand-comparison.csv").open(
                encoding="utf-8", newline=""
            ) as f:
                row = next(csv.DictReader(f))
            self.assertEqual(row["projname"], "alpha")
            self.assertEqual(row["beforeRiCostInBillingCurrency"], "10")
            self.assertEqual(row["beforeOnDemandCostInBillingCurrency"], "6")
            self.assertEqual(row["afterRiCostInBillingCurrency"], "12")
            self.assertEqual(row["afterOnDemandCostInBillingCurrency"], "4")

    def test_reconcile_rejects_mismatched_row_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            before = root / "before.csv"
            after_dir = root / "after"
            after_dir.mkdir()
            before.write_text("meterCategory\nVirtual Machines\n", encoding="utf-8")
            (after_dir / before.name).write_text(
                "meterCategory\n", encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                MODULE.reconcile([before], after_dir, root / "comparison")


if __name__ == "__main__":
    unittest.main()
