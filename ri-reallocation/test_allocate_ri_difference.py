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
    def test_refund_and_unused_reservation_reconcile_without_usage(self):
        order = "order-a"
        actual = [{"path": Path("actual.csv"), "rows": [
            {"meterCategory": "Azure Database for MySQL", "pricingModel": "Reservation",
             "chargeType": charge, "reservationId": order,
             "costInBillingCurrency": amount}
            for charge, amount in (("Purchase", "493.3"), ("Refund", "-312.4"))
        ]}]
        amortized_rows = [
            {"meterCategory": "Azure Database for MySQL", "pricingModel": "Reservation",
             "chargeType": "UnusedReservation", "benefitId": f"/reservationOrders/{order}/reservations/ri",
             "costInBillingCurrency": amount, "quantity": "1"}
            for amount in ("100", "81.777671232876725")
        ]
        amortized = [{"path": Path("amortized.csv"), "fieldnames": list(amortized_rows[0]),
                      "rows": amortized_rows}]
        actual_costs, _ = MODULE.collect_actual_costs(actual, "costInBillingCurrency")
        costs, eligible, unused, _ = MODULE.collect_amortized_data(
            amortized, "costInBillingCurrency"
        )
        self.assertEqual(actual_costs[("mysql", order)], Decimal("180.9"))
        self.assertEqual(costs[("mysql", order)], Decimal("181.777671232876725"))
        self.assertEqual(eligible, {})
        allocations, summary = MODULE.allocate_differences(
            amortized, actual_costs, costs, eligible, "costInBillingCurrency", unused
        )
        self.assertEqual(summary[0][4], Decimal("-0.877671232876725"))
        self.assertEqual(sum(allocations.values()), summary[0][4])
        with tempfile.TemporaryDirectory() as root:
            output = Path(root)
            MODULE.write_outputs(amortized, output, allocations, "costInBillingCurrency")
            MODULE.write_changed_rows(output, amortized, allocations, "costInBillingCurrency")
            with (output / "amortized.csv").open(newline="", encoding="utf-8-sig") as stream:
                records = list(csv.DictReader(stream))
            self.assertEqual(
                sum(Decimal(row["costInBillingCurrencyAfterActualReconciliation"])
                    for row in records), Decimal("180.9")
            )
            self.assertEqual(
                [row["riActualAmortizedAdjustmentType"] for row in records],
                ["UnusedReservation", "UnusedReservation"]
            )
            with (output / "changed-amortized-ri-rows.csv").open(newline="", encoding="utf-8-sig") as stream:
                self.assertEqual(
                    [row["riActualAmortizedAdjustmentType"] for row in csv.DictReader(stream)],
                    ["UnusedReservation", "UnusedReservation"]
                )

    def test_usage_precedes_unused_reservation_and_zero_difference_changes_nothing(self):
        order = "order-b"
        rows = [
            {"meterCategory": "Virtual Machines", "pricingModel": "Reservation",
             "chargeType": charge, "benefitId": f"/reservationOrders/{order}/reservations/ri",
             "ResourceId": resource, "costInBillingCurrency": "5", "quantity": "1"}
            for charge, resource in (
                ("Usage", "/providers/Microsoft.Compute/virtualMachines/vm"),
                ("UnusedReservation", ""),
            )
        ]
        files = [{"path": Path("bill.csv"), "rows": rows}]
        costs, eligible, unused, _ = MODULE.collect_amortized_data(files, "costInBillingCurrency")
        allocations, _ = MODULE.allocate_differences(
            files, {("vm", order): Decimal("12")}, costs, eligible,
            "costInBillingCurrency", unused
        )
        self.assertEqual(allocations, {(0, 0): Decimal("2")})
        allocations, _ = MODULE.allocate_differences(
            files, {("vm", order): Decimal("10")}, costs, eligible,
            "costInBillingCurrency", unused
        )
        self.assertEqual(allocations, {})

    def test_unused_reservation_cannot_become_negative(self):
        order = "order-c"
        row = {"meterCategory": "Redis Cache", "pricingModel": "Reservation",
               "chargeType": "UnusedReservation", "benefitId": f"/reservationOrders/{order}/reservations/ri",
               "costInBillingCurrency": "5", "quantity": "1"}
        files = [{"path": Path("bill.csv"), "rows": [row]}]
        costs, eligible, unused, _ = MODULE.collect_amortized_data(files, "costInBillingCurrency")
        with self.assertRaisesRegex(ValueError, "negative cost"):
            MODULE.allocate_differences(
                files, {("amr", order): Decimal("-1")}, costs, eligible,
                "costInBillingCurrency", unused
            )

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
