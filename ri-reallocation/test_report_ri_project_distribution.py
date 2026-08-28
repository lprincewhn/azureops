import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("report_ri_project_distribution.py")
SPEC = importlib.util.spec_from_file_location(
    "report_ri_project_distribution", MODULE_PATH
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class DistributionReportTests(unittest.TestCase):
    def _write_result(
        self,
        root: Path,
        bill_rows: list[dict[str, str]],
        detail_rows: list[dict[str, str]],
    ) -> None:
        (root / "reservations.json").write_text(
            json.dumps(
                [
                    {
                        "externalReservationId": "/reservations/ri-1",
                        "bindings": [
                            {"project": "target-a", "boundQuantity": 1}
                        ],
                    }
                ]
            ),
            encoding="utf-8",
        )
        (root / "ri-summary.json").write_text(
            json.dumps(
                {
                    "reservationIds": ["ri-1"],
                    "riSavingsByReservation": {
                        "ri-1": {"grossSavings": "30"}
                    },
                }
            ),
            encoding="utf-8",
        )
        bill_fields = [
            "reservationId",
            "reservationName",
            "chargeType",
            "pricingModel",
            "tags",
            "resourceGroupName",
            "subscriptionName",
            "meterId",
            "date",
            "billingCurrency",
            "quantity",
            "costInBillingCurrency",
            "costInBillingCurrencyAfterActualReconciliation",
            "riGrossSavings",
            "riPaygEquivalentAmount",
            "riAmortizedCost",
        ]
        for row in bill_rows:
            if row.get("riGrossSavings"):
                row.setdefault("riPaygEquivalentAmount", "50")
                row.setdefault("riAmortizedCost", "20")
                row.setdefault(
                    "costInBillingCurrency", row["riAmortizedCost"]
                )
        with (root / "bill.csv").open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=bill_fields)
            writer.writeheader()
            writer.writerows(bill_rows)
        detail_fields = [
            "sourceFile",
            "sourceRowNumber",
            "ResourceId",
            "allocationType",
            "allocationTarget",
            "riAllocationReservationIds",
            "allocationAmount",
        ]
        with (root / "ri-allocation-details.csv").open(
            "w", encoding="utf-8", newline=""
        ) as target:
            writer = csv.DictWriter(target, fieldnames=detail_fields)
            writer.writeheader()
            writer.writerows(detail_rows)

    def test_reports_before_retained_and_assigned_distribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "RI One",
                        "tags": '{"projname":"source"}',
                        "riGrossSavings": "30",
                    }
                ],
                [
                    {
                        "sourceFile": "/input/bill.csv",
                        "sourceRowNumber": "2",
                        "ResourceId": "/vm/source",
                        "allocationType": "RI_USAGE_COST_REASSIGNED",
                        "allocationTarget": "target-a|target-b",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "20",
                    },
                    {
                        "sourceFile": "/input/bill.csv",
                        "sourceRowNumber": "3",
                        "ResourceId": "/vm/a",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "target-a",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-12",
                    },
                    {
                        "sourceFile": "/input/bill.csv",
                        "sourceRowNumber": "4",
                        "ResourceId": "/vm/b",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "target-b",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-8",
                    },
                ],
            )
            result = MODULE.build_distribution(root, root / "report", "projname")
            with result.csv_path.open(encoding="utf-8-sig", newline="") as source:
                rows = {row["project"]: row for row in csv.DictReader(source)}

            self.assertEqual(rows["source"]["beforeBenefit"], "30")
            self.assertEqual(rows["source"]["afterBenefit"], "10")
            self.assertEqual(rows["target-a"]["afterBenefit"], "12")
            self.assertEqual(rows["target-b"]["afterBenefit"], "8")
            self.assertEqual(rows["source"]["paygEquivalentCost"], "50")
            self.assertEqual(rows["source"]["riAmortizedCost"], "20")
            self.assertEqual(
                rows["source"][
                    "riAmortizedCostBeforeActualReconciliation"
                ],
                "20",
            )
            self.assertEqual(
                rows["source"][
                    "riAmortizedCostAfterActualReconciliation"
                ],
                "20",
            )
            self.assertEqual(result.gross_savings, MODULE.Decimal("30"))
            self.assertTrue(result.html_path.is_file())

    def test_reports_amortized_cost_before_and_after_actual_reconciliation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "RI One",
                        "tags": '{"projname":"source"}',
                        "costInBillingCurrency": "12",
                        "costInBillingCurrencyAfterActualReconciliation": "20",
                        "riAmortizedCost": "20",
                        "riPaygEquivalentAmount": "50",
                        "riGrossSavings": "30",
                    }
                ],
                [],
            )
            result = MODULE.build_distribution(
                root, root / "report", "projname"
            )
            with result.csv_path.open(
                encoding="utf-8-sig", newline=""
            ) as source:
                row = next(csv.DictReader(source))
            self.assertEqual(
                row["riAmortizedCostBeforeActualReconciliation"], "12"
            )
            self.assertEqual(
                row["riAmortizedCostAfterActualReconciliation"], "20"
            )
            html_report = result.html_path.read_text(encoding="utf-8")
            self.assertIn("差额调整前 RI 摊销成本", html_report)
            self.assertIn("12.0000", html_report)
            self.assertIn("差额调整后 RI 摊销成本", html_report)
            self.assertIn("20.0000", html_report)

    def test_includes_ri_without_allocation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "Unused RI",
                        "tags": '{"projname":"source"}',
                        "riGrossSavings": "",
                        "riPaygEquivalentAmount": "",
                        "riAmortizedCost": "",
                    }
                ],
                [],
            )
            (root / "ri-summary.json").write_text(
                json.dumps(
                    {
                        "reservationIds": ["ri-1", "ri-2"],
                        "riSavingsByReservation": {
                            "ri-1": {"grossSavings": "0"},
                            "ri-2": {"grossSavings": "0"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "reservations.json").write_text(
                json.dumps(
                    [
                        {
                            "externalReservationId": "/reservations/ri-1",
                            "bindings": [{"project": "a", "boundQuantity": 1}],
                        },
                        {
                            "externalReservationId": "/reservations/ri-2",
                            "bindings": [{"project": "b", "boundQuantity": 1}],
                        },
                    ]
                ),
                encoding="utf-8",
            )

            result = MODULE.build_distribution(root, root / "report", "projname")
            with result.csv_path.open(encoding="utf-8-sig", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(result.configured_reservation_count, 2)
            self.assertEqual(result.reservation_count, 0)
            self.assertEqual(rows, [])
            report = result.html_path.read_text(encoding="utf-8")
            self.assertIn(
                "有有效 binding 但本账期无 Usage</td><td>2", report
            )

    def test_unconfigured_ri_uses_actual_consuming_project(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-unconfigured",
                        "reservationName": "Unconfigured RI",
                        "chargeType": "Usage",
                        "pricingModel": "Reservation",
                        "tags": "",
                        "resourceGroupName": "actual-project-rg",
                        "meterId": "meter-a",
                        "date": "2026-07-01",
                        "billingCurrency": "USD",
                        "quantity": "2",
                        "costInBillingCurrency": "4",
                        "riGrossSavings": "",
                        "riPaygEquivalentAmount": "",
                        "riAmortizedCost": "",
                    }
                ],
                [],
            )
            price_sheet = root / "prices.json"
            price_sheet.write_text(
                json.dumps(
                    [
                        {
                            "meterId": "meter-a",
                            "tierMinimumUnits": 0,
                            "unitPrice": 5,
                            "billingCurrency": "USD",
                            "effectiveStartDate": "2026-07-01",
                            "effectiveEndDate": "2026-07-31",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            (root / "ri-summary.json").write_text(
                json.dumps(
                    {
                        "reservationIds": [],
                        "riSavingsByReservation": {},
                        "priceSheetSource": str(price_sheet),
                    }
                ),
                encoding="utf-8",
            )
            (root / "reservations.json").write_text(
                json.dumps(
                    [
                        {
                            "externalReservationId": "/reservations/ri-unconfigured",
                            "bindings": [],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            result = MODULE.build_distribution(
                root, root / "report", "projname", price_sheet
            )
            with result.csv_path.open(encoding="utf-8-sig", newline="") as source:
                rows = {
                    row["reservationId"]: row for row in csv.DictReader(source)
                }
            row = rows["ri-unconfigured"]
            self.assertEqual(row["allocationStatus"], "NOT_ALLOCATED")
            self.assertEqual(
                row["classification"], "NO_VALID_BINDING_WITH_USAGE"
            )
            self.assertEqual(row["project"], "actual-project-rg")
            self.assertEqual(row["paygEquivalentCost"], "10")
            self.assertEqual(row["riAmortizedCost"], "4")
            self.assertEqual(row["grossSavings"], "6")
            self.assertEqual(row["beforeBenefit"], "6")
            self.assertEqual(row["afterBenefit"], "6")

    def test_unconfigured_ri_uses_reconciled_cost_when_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-unconfigured",
                        "reservationName": "Unconfigured RI",
                        "chargeType": "Usage",
                        "pricingModel": "Reservation",
                        "tags": '{"projname":"source"}',
                        "meterId": "meter-a",
                        "date": "2026-07-01",
                        "billingCurrency": "USD",
                        "quantity": "2",
                        "costInBillingCurrency": "4",
                        "costInBillingCurrencyAfterActualReconciliation": "12",
                        "riGrossSavings": "",
                    }
                ],
                [],
            )
            price_sheet = root / "prices.json"
            price_sheet.write_text(
                json.dumps(
                    [{
                        "meterId": "meter-a",
                        "tierMinimumUnits": 0,
                        "unitPrice": 5,
                        "billingCurrency": "USD",
                        "effectiveStartDate": "2026-07-01",
                        "effectiveEndDate": "2026-07-31",
                    }]
                ),
                encoding="utf-8",
            )
            (root / "ri-summary.json").write_text(
                json.dumps({
                    "reservationIds": [],
                    "riSavingsByReservation": {},
                    "priceSheetSource": str(price_sheet),
                }),
                encoding="utf-8",
            )
            (root / "reservations.json").write_text(
                json.dumps([{
                    "externalReservationId": "/reservations/ri-unconfigured",
                    "bindings": [],
                }]),
                encoding="utf-8",
            )
            result = MODULE.build_distribution(
                root, root / "report", "projname", price_sheet
            )
            with result.csv_path.open(
                encoding="utf-8-sig", newline=""
            ) as source:
                row = next(csv.DictReader(source))
            self.assertEqual(
                row["riAmortizedCostBeforeActualReconciliation"], "4"
            )
            self.assertEqual(
                row["riAmortizedCostAfterActualReconciliation"], "12"
            )
            self.assertEqual(row["grossSavings"], "-2")

    def test_self_receiver_is_counted_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "",
                        "tags": '{"projname":"source"}',
                        "riGrossSavings": "30",
                    }
                ],
                [
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "2",
                        "ResourceId": "/vm/source",
                        "allocationType": "RI_USAGE_COST_REASSIGNED",
                        "allocationTarget": "source|target",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "30",
                    },
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "2",
                        "ResourceId": "/vm/source",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "source",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-20",
                    },
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "3",
                        "ResourceId": "/vm/target",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "target",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-10",
                    },
                ],
            )
            result = MODULE.build_distribution(root, root / "report", "projname")
            with result.csv_path.open(encoding="utf-8-sig", newline="") as source:
                rows = {row["project"]: row for row in csv.DictReader(source)}
            self.assertEqual(rows["source"]["afterBenefit"], "20")
            self.assertEqual(rows["target"]["afterBenefit"], "10")

    def test_rejects_non_conserving_details(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "",
                        "tags": '{"projname":"source"}',
                        "riGrossSavings": "30",
                    }
                ],
                [
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "2",
                        "ResourceId": "/vm/source",
                        "allocationType": "RI_USAGE_COST_REASSIGNED",
                        "allocationTarget": "target",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "20",
                    },
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "3",
                        "ResourceId": "/vm/target",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "target",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-19",
                    },
                ],
            )
            with self.assertRaisesRegex(ValueError, "收益不守恒"):
                MODULE.build_distribution(root, root / "report", "projname")

    def test_reports_negative_benefit_as_excess_cost_distribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "RI One",
                        "tags": '{"projname":"source"}',
                        "riGrossSavings": "-20",
                        "riPaygEquivalentAmount": "100",
                        "riAmortizedCost": "120",
                    }
                ],
                [
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "2",
                        "ResourceId": "/vm/source",
                        "allocationType": "RI_USAGE_COST_REASSIGNED",
                        "allocationTarget": "target-a",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-20",
                    },
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "3",
                        "ResourceId": "/vm/target",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "target-a",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "20",
                    },
                ],
            )
            (root / "ri-summary.json").write_text(
                json.dumps(
                    {
                        "reservationIds": ["ri-1"],
                        "riSavingsByReservation": {
                            "ri-1": {"grossSavings": "-20"}
                        },
                    }
                ),
                encoding="utf-8",
            )
            result = MODULE.build_distribution(
                root, root / "report", "projname"
            )
            with result.csv_path.open(
                encoding="utf-8-sig", newline=""
            ) as source:
                rows = {row["project"]: row for row in csv.DictReader(source)}
            self.assertEqual(rows["source"]["beforeBenefit"], "-20")
            self.assertEqual(rows["source"]["afterBenefit"], "0")
            self.assertEqual(rows["target-a"]["afterBenefit"], "-20")
            self.assertEqual(result.gross_savings, MODULE.Decimal("-20"))

    def test_allows_sub_precision_conservation_residue(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_result(
                root,
                [
                    {
                        "reservationId": "ri-1",
                        "reservationName": "RI One",
                        "tags": '{"projname":"source"}',
                        "riGrossSavings": "1",
                    }
                ],
                [
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "2",
                        "ResourceId": "/vm/source",
                        "allocationType": "RI_USAGE_COST_REASSIGNED",
                        "allocationTarget": "target-a",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "1",
                    },
                    {
                        "sourceFile": "bill.csv",
                        "sourceRowNumber": "3",
                        "ResourceId": "/vm/target",
                        "allocationType": "RI_BENEFIT_ASSIGNED",
                        "allocationTarget": "target-a",
                        "riAllocationReservationIds": "ri-1",
                        "allocationAmount": "-0.999999999999999999999999999",
                    },
                ],
            )
            (root / "ri-summary.json").write_text(
                json.dumps(
                    {
                        "reservationIds": ["ri-1"],
                        "riSavingsByReservation": {
                            "ri-1": {"grossSavings": "1"}
                        },
                    }
                ),
                encoding="utf-8",
            )
            result = MODULE.build_distribution(
                root, root / "report", "projname"
            )
            self.assertTrue(result.csv_path.is_file())


if __name__ == "__main__":
    unittest.main()
