import csv
import json
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).parent


class CombinedPipelineTests(unittest.TestCase):
    def test_reconciles_merges_and_reports_three_services(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fields = [
                "meterCategory", "meterSubCategory", "meterName", "meterRegion",
                "pricingModel", "chargeType", "reservationId", "benefitId",
                "ResourceId", "tags", "additionalInfo", "quantity", "meterId",
                "date", "billingCurrency", "costInBillingCurrency", "reservationName",
            ]
            services = [
                ("vm", "Virtual Machines", "VM Compute", "D2",
                 "Microsoft.Compute/virtualMachines/vm", "Standard_D2s_v5"),
                ("mysql", "Azure Database for MySQL", "Flexible Server General Purpose Compute",
                 "vCore", "Microsoft.DBforMySQL/flexibleServers/db", "Standard_D2ds_v4"),
                ("redis", "Redis Cache", "Azure Managed Redis - Balanced",
                 "B0 Cache Instance", "Microsoft.Cache/redisEnterprise/cache", "Balanced_B0"),
            ]
            actual = []
            amortized = []
            reservations = []
            for index, (service, category, subcategory, meter_name, resource, sku) in enumerate(services):
                order = f"order-{index}"
                ri = f"ri-{index}"
                base = {
                    "meterCategory": category, "meterSubCategory": subcategory,
                    "meterName": meter_name, "meterRegion": "US West 3",
                    "pricingModel": "Reservation", "chargeType": "Usage",
                    "reservationId": ri,
                    "benefitId": f"/reservationOrders/{order}/reservations/{ri}",
                    "ResourceId": f"/subscriptions/sub/resourceGroups/rg/providers/{resource}",
                    "tags": json.dumps({"projname": "origin"}),
                    "additionalInfo": json.dumps({"ReservationOrderId": order,
                                                  "ServerSku": sku, "ServiceType": sku}),
                    "quantity": "1", "meterId": "meter-a", "date": "2026-11-05",
                    "billingCurrency": "USD", "costInBillingCurrency": "10",
                    "reservationName": service,
                }
                amortized.append(base)
                receiver = dict(base, pricingModel="OnDemand", reservationId="",
                                benefitId="", tags=json.dumps({"projname": "target"}),
                                costInBillingCurrency="100")
                amortized.append(receiver)
                actual.append(dict(base, chargeType="Purchase", reservationId=order,
                                   benefitId="", ResourceId="", costInBillingCurrency="12"))
                reservations.append({
                    "productType": service,
                    "externalReservationId": (
                        f"order:{order}" if service != "vm"
                        else f"/reservationOrders/{order}/reservations/{ri}"
                    ),
                    "appliedScopeType": "Shared", "flexibility": "off",
                    "bindings": [{"project": "target", "boundQuantity": 1}],
                })

            def write_bill(name, rows):
                with (root / name).open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(rows)

            write_bill("actual.csv", actual)
            write_bill("amortized.csv", amortized)
            config = root / "reservations.json"
            config.write_text(json.dumps(reservations))
            prices = root / "price.json"
            prices.write_text(json.dumps([{
                "meterId": "meter-a", "unitPrice": 20, "priceType": "Consumption",
                "tierMinimumUnits": 0, "billingCurrency": "USD",
                "effectiveStartDate": "2026-11-01", "effectiveEndDate": "2026-11-30",
            }]))
            subprocess.run([
                sys.executable, str(ROOT / "allocate_ri_difference.py"),
                "--actual", str(root / "actual.csv"), "--amortized", str(root / "amortized.csv"),
                "--output-dir", str(root / "reconciled"),
            ], check=True, capture_output=True)
            subprocess.run([
                sys.executable, str(ROOT / "reallocate_all_ri.py"),
                "--input-dir", str(root / "reconciled"),
                "--output-dir", str(root / "combined"),
                "--reservations-file", str(config),
                "--price-sheet-file", str(prices),
                "--project-tag-key", "projname",
            ], check=True, capture_output=True)
            with (root / "combined/amortized.csv").open(newline="") as stream:
                results = list(csv.DictReader(stream))
            self.assertEqual(len(results), 6)
            self.assertEqual(
                [Decimal(row["costInBillingCurrencyAfterActualReconciliation"])
                 for row in results[::2]], [Decimal("12")] * 3
            )
            self.assertEqual(
                sum(Decimal(row["riAllocationAmount"]) for row in results),
                Decimal("0"),
            )
            self.assertEqual(
                [Decimal(row["riBenefitOrLoss"]) for row in results[::2]],
                [Decimal("8")] * 3,
            )
            summary = json.loads((root / "combined/ri-summary.json").read_text())
            self.assertEqual(set(summary["reservationIds"]), {"ri-0", "ri-1", "ri-2"})
            subprocess.run([
                sys.executable, str(ROOT / "report_ri_project_distribution.py"),
                str(root / "combined"), "--project-tag-key", "projname",
                "--reservations-file", str(config), "--price-sheet-file", str(prices),
                "--output-dir", str(root / "report"),
            ], check=True, capture_output=True)
            reports = list((root / "report").glob("*.csv"))
            self.assertEqual(len(reports), 1)
            with reports[0].open(newline="", encoding="utf-8-sig") as stream:
                report_rows = list(csv.DictReader(stream))
            self.assertEqual(
                {row["resourceType"] for row in report_rows},
                {"VM", "MySQL", "AMR"},
            )
            self.assertEqual(
                {row["reservationId"] for row in report_rows},
                {"ri-0", "ri-1", "ri-2"},
            )
