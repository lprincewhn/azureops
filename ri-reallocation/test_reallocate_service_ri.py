import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

import reallocate_vm_ri as allocation


class ServiceAllocationTests(unittest.TestCase):
    def run_allocation(self, profile, source_rows, reservation, price):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        directory = Path(root.name)
        bill = directory / "bill.csv"
        with bill.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(source_rows[0]))
            writer.writeheader()
            writer.writerows(source_rows)
        config = directory / "reservations.json"
        config.write_text(json.dumps([reservation]), encoding="utf-8")
        rates = directory / "prices.json"
        rates.write_text(json.dumps([{
            "meterId": "meter-ri",
            "priceType": "Consumption",
            "tierMinimumUnits": 0,
            "unitPrice": price,
            "billingCurrency": "USD",
            "effectiveStartDate": "2026-11-01",
            "effectiveEndDate": "2026-11-30",
        }]), encoding="utf-8")
        output = directory / "out"
        argv = [
            "program", str(bill), "--reservations-file", str(config),
            "--project-tag-key", "projname", "--price-sheet-file", str(rates),
            "--output-dir", str(output),
        ]
        with mock.patch.object(sys, "argv", argv), contextlib.redirect_stdout(io.StringIO()):
            allocation.main(profile)
        with (output / "bill.csv").open(newline="", encoding="utf-8") as handle:
            result = list(csv.DictReader(handle))
        summary = json.loads((output / "ri-summary.json").read_text())
        return result, summary

    @staticmethod
    def row(category, subcategory, meter_name, sku, project, cost,
            pricing="OnDemand", reservation="", quantity="1"):
        return {
            "meterCategory": category,
            "meterSubCategory": subcategory,
            "meterName": meter_name,
            "meterRegion": "US West 3",
            "additionalInfo": json.dumps({"ServerSku": sku, "ReservationOrderId": "order-a"}),
            "tags": json.dumps({"projname": project}),
            "costInBillingCurrency": str(cost),
            "pricingModel": pricing,
            "chargeType": "Usage",
            "reservationId": reservation,
            "quantity": quantity,
            "meterId": "meter-ri",
            "date": "2026-11-05",
            "billingCurrency": "USD",
            "ResourceId": "/subscriptions/sub-a/resourceGroups/rg-a/providers/example/item",
        }

    def test_mysql_order_maps_to_ri_and_excludes_storage_and_other_services(self):
        mysql = "Azure Database for MySQL"
        compute = "Azure Database for MySQL Flexible Server General Purpose Series Compute"
        rows = [
            self.row(mysql, compute, "vCore", "Standard_D2ds_v4", "origin", 10,
                     "Reservation", "ri-mysql"),
            self.row(mysql, compute, "vCore", "Standard_D4ds_v4", "target", 40),
            self.row(mysql, "Azure Database for MySQL Flexible Server Storage",
                     "Storage Data Stored", "", "target", 100),
            self.row("Virtual Machines", "VM", "vCore", "Standard_D4ds_v4", "target", 100),
        ]
        reservation = {
            "productType": "mysql", "externalReservationId": "order:order-a",
            "flexibility": "on", "appliedScopeType": "Shared",
            "bindings": [{"project": "target", "boundQuantity": 1}],
        }
        result, summary = self.run_allocation(allocation.MYSQL, rows, reservation, 20)
        self.assertEqual(summary["reservationIds"], ["ri-mysql"])
        self.assertEqual(summary["riAllocatableAmount"], "10")
        self.assertEqual(Decimal(result[0]["riAllocationAmount"]), Decimal("10"))
        self.assertEqual(Decimal(result[1]["riAllocationAmount"]), Decimal("-10"))
        self.assertTrue(all(not row["riAllocationAmount"] or Decimal(row["riAllocationAmount"]) == 0
                            for row in result[2:]))
        self.assertEqual(
            sum(Decimal(row["riAllocationAmount"]) for row in result), Decimal("0")
        )

    def test_amr_flexible_balanced_skips_memory_optimized(self):
        redis = "Redis Cache"
        balanced = "Azure Managed Redis - Balanced"
        rows = [
            self.row(redis, balanced, "B1 Cache Instance", "", "origin", 10,
                     "Reservation", "ri-redis"),
            self.row(redis, balanced, "B3 Cache Instance", "", "target", 40),
            self.row(redis, "Azure Managed Redis - Memory Optimized",
                     "M10 Cache Instance", "", "target", 100),
        ]
        reservation = {
            "productType": "redis", "externalReservationId": "order:order-a",
            "flexibility": "on", "appliedScopeType": "Shared",
            "bindings": [{"project": "target", "boundQuantity": 1}],
        }
        result, summary = self.run_allocation(allocation.AMR, rows, reservation, 20)
        self.assertEqual(summary["reservationIds"], ["ri-redis"])
        self.assertEqual(summary["riAllocatableAmount"], "10")
        self.assertEqual(Decimal(result[0]["riAllocationAmount"]), Decimal("10"))
        self.assertEqual(Decimal(result[1]["riAllocationAmount"]), Decimal("-10"))
        self.assertEqual(Decimal(result[2]["riAllocationAmount"]), Decimal("0"))

    def test_amr_exact_sku_does_not_match_another_size(self):
        row = self.row("Redis Cache", "Azure Managed Redis - Balanced",
                       "B1 Cache Instance", "", "origin", 10)
        other = dict(row, meterName="B3 Cache Instance")
        self.assertNotEqual(
            allocation.resource_key(row, "model", allocation.AMR),
            allocation.resource_key(other, "model", allocation.AMR),
        )

    def test_missing_order_usage_fails_explicitly(self):
        with tempfile.TemporaryDirectory() as root:
            config = Path(root) / "reservations.json"
            config.write_text(json.dumps([{
                "productType": "mysql", "externalReservationId": "order:missing",
                "bindings": [{"project": "target", "boundQuantity": 1}],
            }]))
            with self.assertRaisesRegex(ValueError, "找不到 reservationId"):
                allocation.load_reservations_config(
                    config, "projname", allocation.MYSQL, {}
                )
