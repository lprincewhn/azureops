import importlib.util
import csv
import tempfile
import unittest
from pathlib import Path
from decimal import Decimal


MODULE_PATH = Path(__file__).with_name("allocate_ri_difference.py")
SPEC = importlib.util.spec_from_file_location("allocate_ri_difference", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class VmResourceIdTests(unittest.TestCase):
    def test_accepts_virtual_machine_resource_id(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.Compute/virtualMachines/vm"
        )
        self.assertTrue(MODULE.is_vm_resource_id(resource_id))

    def test_accepts_virtual_machine_scale_set_resource_id(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.Compute/virtualMachineScaleSets/vmss"
        )
        self.assertTrue(MODULE.is_vm_resource_id(resource_id))

    def test_rejects_non_vm_resource_id(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.Storage/storageAccounts/account"
        )
        self.assertFalse(MODULE.is_vm_resource_id(resource_id))


class MultiServiceReconciliationTests(unittest.TestCase):
    def test_usage_selection_does_not_depend_on_meter_subcategory(self):
        for category, service, resource in (
            ("Azure Database for MySQL", "mysql",
             "/providers/Microsoft.DBforMySQL/flexibleServers/db"),
            ("Redis Cache", "amr",
             "/providers/Microsoft.Cache/redisEnterprise/cache"),
        ):
            with self.subTest(service=service):
                row = {
                    "meterCategory": category,
                    "meterSubCategory": "Other Reservation Subcategory",
                    "pricingModel": "Reservation", "chargeType": "Usage",
                    "benefitId": "/reservationOrders/order-a/reservations/ri-a",
                    "reservationId": "ri-a", "ResourceId": resource,
                    "costInBillingCurrency": "10", "quantity": "1",
                    "reservationName": "reservation",
                }
                data = [{"path": Path("bill.csv"), "rows": [row]}]
                self.assertEqual(MODULE.reservation_service(row), service)
                costs, eligible, _, _ = MODULE.collect_amortized_data(
                    data, "costInBillingCurrency"
                )
                self.assertEqual(costs[(service, "order-a")], Decimal("10"))
                self.assertEqual(eligible[(service, "order-a")], [(0, 0)])

    def test_purchase_and_usage_are_reconciled_per_service_and_order(self):
        order = "same-order"
        def row(category, subcategory, charge, cost, resource="", benefit=""):
            return {
                "meterCategory": category, "meterSubCategory": subcategory,
                "pricingModel": "Reservation", "chargeType": charge,
                "reservationId": order if charge == "Purchase" else "ri-id",
                "benefitId": benefit, "ResourceId": resource, "quantity": "1",
                "costInBillingCurrency": str(cost),
            }
        sources = [
            ("vm", "Virtual Machines", "VM",
             "/providers/Microsoft.Compute/virtualMachines/vm"),
            ("mysql", "Azure Database for MySQL", "Flexible Server Compute",
             "/providers/Microsoft.DBforMySQL/flexibleServers/db"),
            ("amr", "Redis Cache", "Azure Managed Redis - Balanced",
             "/providers/Microsoft.Cache/redisEnterprise/cache"),
        ]
        purchase = [row(cat, sub, "Purchase", 20) for _, cat, sub, _ in sources]
        usage = [
            row(cat, sub, "Usage", 10, resource,
                "/providers/Microsoft.Capacity/reservationOrders/same-order/reservations/ri-id")
            for _, cat, sub, resource in sources
        ]
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            for name, records in (("actual.csv", purchase), ("amortized.csv", usage)):
                with (path / name).open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
            actual = MODULE.read_csv_files([path / "actual.csv"], "costInBillingCurrency")
            amortized = MODULE.read_csv_files([path / "amortized.csv"], "costInBillingCurrency")
            actual_costs, _ = MODULE.collect_actual_costs(actual, "costInBillingCurrency")
            amortized_costs, eligible, _, _ = MODULE.collect_amortized_data(
                amortized, "costInBillingCurrency"
            )
            allocations, summary = MODULE.allocate_differences(
                amortized, actual_costs, amortized_costs, eligible, "costInBillingCurrency"
            )
            self.assertEqual(len(summary), 3)
            self.assertEqual(set(actual_costs), {(service, order) for service, *_ in sources})
            self.assertEqual(set(allocations.values()), {Decimal("10")})
            self.assertEqual(len(allocations), 3)
            MODULE.write_outputs(amortized, path / "out", allocations, "costInBillingCurrency")
            with (path / "out/amortized.csv").open(newline="", encoding="utf-8-sig") as stream:
                self.assertEqual(
                    [Decimal(r["costInBillingCurrencyAfterActualReconciliation"])
                     for r in csv.DictReader(stream)], [Decimal("20")] * 3
                )

if __name__ == "__main__":
    unittest.main()
