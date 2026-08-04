import argparse
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("reallocate_vm_ri.py")
SPEC = importlib.util.spec_from_file_location("reallocate_vm_ri", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ReallocationFilterTests(unittest.TestCase):
    def test_ri_usage_requires_selected_reservation_id(self):
        row = {
            "pricingModel": "Reservation",
            "chargeType": "Usage",
            "reservationId": "ri-selected",
        }
        self.assertTrue(MODULE.is_ri_usage(row, {"ri-selected"}))
        self.assertFalse(MODULE.is_ri_usage(row, {"ri-other"}))

    def test_target_tag_selects_receiver_project(self):
        row = {"tags": '{"projname":"fota"}'}
        self.assertTrue(MODULE.has_target_tag(row, ("projname", "fota")))
        self.assertFalse(MODULE.has_target_tag(row, ("projname", "other")))

    def test_allocation_key_uses_vm_model_and_region(self):
        row = {
            "meterRegion": "AP Southeast",
            "additionalInfo": '{"ServiceType":"Standard_D8s_v5"}',
            "meterName": "D8s v5",
        }
        self.assertEqual(
            MODULE.allocation_key(row),
            ("Standard_D8s_v5", "AP Southeast"),
        )

    def test_ri_normalization_ratio_reads_real_field(self):
        row = {"additionalInfo": '{"ServiceType":"Standard_D2s_v5","RINormalizationRatio":1.0}'}
        self.assertEqual(MODULE.ri_normalization_ratio(row), MODULE.Decimal("1.0"))

    def test_ri_normalization_ratio_missing_returns_none(self):
        self.assertIsNone(MODULE.ri_normalization_ratio({"additionalInfo": "{}"}))
        self.assertIsNone(MODULE.ri_normalization_ratio({}))

    def test_is_size_flexible_uses_normalization_ratio(self):
        base = {"additionalInfo": '{"RINormalizationRatio":1.0}'}
        flexed = {"additionalInfo": '{"RINormalizationRatio":2.0}'}
        missing = {"additionalInfo": "{}"}
        self.assertFalse(MODULE.is_size_flexible(base))
        self.assertTrue(MODULE.is_size_flexible(flexed))
        self.assertFalse(MODULE.is_size_flexible(missing))

    def test_flexibility_group_derivation(self):
        self.assertEqual(MODULE.flexibility_group("Standard_D2s_v5"), "Ds_v5")
        self.assertEqual(MODULE.flexibility_group("Standard_D4s_v5"), "Ds_v5")
        self.assertEqual(MODULE.flexibility_group("Standard_D8-2s_v5"), "Ds_v5")
        self.assertEqual(MODULE.flexibility_group("Standard_E8s_v5"), "Es_v5")
        self.assertEqual(MODULE.flexibility_group("Standard_D2_v5"), "D_v5")
        self.assertEqual(MODULE.flexibility_group("Standard_D2ads_v5"), "Dads_v5")

    def test_flex_group_match_mode_groups_different_sizes(self):
        d2 = {
            "meterRegion": "US West 3",
            "additionalInfo": '{"ServiceType":"Standard_D2s_v5"}',
        }
        d4 = {
            "meterRegion": "US West 3",
            "additionalInfo": '{"ServiceType":"Standard_D4s_v5"}',
        }
        # model 模式下不同规格分到不同池
        self.assertNotEqual(
            MODULE.allocation_key(d2, "model"), MODULE.allocation_key(d4, "model")
        )
        # flex-group 模式下同系列不同规格落入同一池
        self.assertEqual(
            MODULE.allocation_key(d2, "flex-group"),
            MODULE.allocation_key(d4, "flex-group"),
        )

    def test_flex_group_separates_different_series(self):
        d2 = {
            "meterRegion": "US West 3",
            "additionalInfo": '{"ServiceType":"Standard_D2s_v5"}',
        }
        e2 = {
            "meterRegion": "US West 3",
            "additionalInfo": '{"ServiceType":"Standard_E2s_v5"}',
        }
        self.assertNotEqual(
            MODULE.allocation_key(d2, "flex-group"),
            MODULE.allocation_key(e2, "flex-group"),
        )

    def test_allocation_key_does_not_match_different_region_or_model(self):
        source = {
            "meterRegion": "US West 3",
            "additionalInfo": '{"ServiceType":"Standard_D2s_v5"}',
        }
        target = {
            "meterRegion": "AP Southeast",
            "additionalInfo": '{"ServiceType":"Standard_D8s_v5"}',
        }
        self.assertNotEqual(MODULE.allocation_key(source), MODULE.allocation_key(target))


    def test_allocated_field_name_follows_amount_field(self):
        self.assertEqual(
            MODULE.allocated_field_name("costInBillingCurrency"),
            "allocatedCostInBillingCurrency",
        )
        self.assertEqual(
            MODULE.allocated_field_name("costInUsd"), "allocatedCostInUsd"
        )
        self.assertEqual(
            MODULE.allocated_field_name("costInPricingCurrency"),
            "allocatedCostInPricingCurrency",
        )


class ReservationsFileTests(unittest.TestCase):
    def _write(self, data):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "reservations.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    def test_reservation_id_extracted_from_external(self):
        self.assertEqual(
            MODULE._reservation_id_from_external(
                "/providers/microsoft.capacity/reservationOrders/ord/reservations/rid-1"
            ),
            "rid-1",
        )
        self.assertEqual(MODULE._reservation_id_from_external("rid-2"), "rid-2")
        self.assertEqual(MODULE._reservation_id_from_external(""), "")

    def test_bindings_become_weighted_targets(self):
        path = self._write(
            [
                {
                    "externalReservationId": ".../reservations/ri-a",
                    "bindings": [
                        {"project": "alpha", "boundQuantity": 2},
                        {"project": "beta", "boundQuantity": 1},
                    ],
                }
            ]
        )
        result, _modes, _dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(
            result["ri-a"],
            [
                (("projname", "alpha"), MODULE.Decimal("2")),
                (("projname", "beta"), MODULE.Decimal("1")),
            ],
        )

    def test_custom_project_tag_key(self):
        path = self._write(
            [{"reservationId": "ri-a", "bindings": [{"project": "x", "boundQuantity": 1}]}]
        )
        result, _modes, _dens = MODULE.load_reservations_file(path, project_tag_key="costcenter")
        self.assertEqual(result["ri-a"], [(("costcenter", "x"), MODULE.Decimal("1"))])

    def test_same_project_code_weights_merged(self):
        path = self._write(
            [
                {
                    "reservationId": "ri-a",
                    "bindings": [
                        {"project": "x", "boundQuantity": 2},
                        {"project": "x", "boundQuantity": 3},
                    ],
                }
            ]
        )
        result, _modes, _dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(result["ri-a"], [(("projname", "x"), MODULE.Decimal("5"))])

    def test_non_positive_quantity_ignored(self):
        path = self._write(
            [
                {
                    "reservationId": "ri-a",
                    "bindings": [
                        {"project": "x", "boundQuantity": 1},
                        {"project": "y", "boundQuantity": 0},
                    ],
                }
            ]
        )
        result, _modes, _dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(result["ri-a"], [(("projname", "x"), MODULE.Decimal("1"))])

    def test_reservation_without_bindings_skipped(self):
        path = self._write(
            [
                {"reservationId": "ri-empty", "bindings": []},
                {"reservationId": "ri-a", "bindings": [{"project": "x", "boundQuantity": 1}]},
            ]
        )
        result, _modes, _dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertNotIn("ri-empty", result)
        self.assertIn("ri-a", result)

    def test_no_usable_definition_raises(self):
        path = self._write([{"reservationId": "ri-a", "bindings": []}])
        with self.assertRaises(ValueError):
            MODULE.load_reservations_file(path, project_tag_key="projname")

    def test_duplicate_reservation_id_raises(self):
        path = self._write(
            [
                {"reservationId": "ri-a", "bindings": [{"project": "x", "boundQuantity": 1}]},
                {"reservationId": "ri-a", "bindings": [{"project": "y", "boundQuantity": 1}]},
            ]
        )
        with self.assertRaises(ValueError):
            MODULE.load_reservations_file(path, project_tag_key="projname")

    def test_external_reservation_id_takes_precedence(self):
        # Real reservations.json has no top-level reservationId; the billing GUID
        # lives in externalReservationId while `id` is an unrelated record UUID.
        path = self._write(
            [
                {
                    "id": "internal-record-uuid",
                    "externalReservationId": ".../reservations/billing-rid",
                    "bindings": [{"project": "x", "boundQuantity": 1}],
                }
            ]
        )
        result, _modes, _dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertIn("billing-rid", result)
        self.assertNotIn("internal-record-uuid", result)

    def test_build_ri_targets_reads_reservations_file(self):
        path = self._write(
            [{"reservationId": "ri-a", "bindings": [{"project": "x", "boundQuantity": 1}]}]
        )
        args = argparse.Namespace(
            reservations_file=str(path),
            project_tag_key="projname",
        )
        targets, _modes, _dens = MODULE.build_ri_targets(args)
        self.assertEqual(targets, {"ri-a": [(("projname", "x"), MODULE.Decimal("1"))]})

    def test_bound_total_used_as_denominator(self):
        path = self._write(
            [
                {
                    "reservationId": "ri-a",
                    "boundTotal": 4,
                    "bindings": [
                        {"project": "x", "boundQuantity": 2},
                        {"project": "y", "boundQuantity": 1},
                    ],
                }
            ]
        )
        _result, _modes, dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(dens["ri-a"], MODULE.Decimal("4"))

    def test_bound_total_missing_falls_back_to_weight_sum(self):
        path = self._write(
            [
                {
                    "reservationId": "ri-a",
                    "bindings": [
                        {"project": "x", "boundQuantity": 2},
                        {"project": "y", "boundQuantity": 1},
                    ],
                }
            ]
        )
        _result, _modes, dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(dens["ri-a"], MODULE.Decimal("3"))

    def test_bound_total_below_weight_sum_falls_back(self):
        path = self._write(
            [
                {
                    "reservationId": "ri-a",
                    "boundTotal": 1,
                    "bindings": [
                        {"project": "x", "boundQuantity": 2},
                        {"project": "y", "boundQuantity": 1},
                    ],
                }
            ]
        )
        _result, _modes, dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(dens["ri-a"], MODULE.Decimal("3"))

    def test_row_contributions_partial_when_bound_total_larger(self):
        targets_list = [
            (("projname", "x"), MODULE.Decimal("2")),
            (("projname", "y"), MODULE.Decimal("1")),
        ]
        add_back, contributions, _label = MODULE._row_contributions(
            MODULE.Decimal("40"), targets_list, MODULE.Decimal("4")
        )
        amounts = {t[1]: amt for t, amt in contributions}
        self.assertEqual(amounts["x"], MODULE.Decimal("20"))
        self.assertEqual(amounts["y"], MODULE.Decimal("10"))
        # 未绑定部分（40*1/4=10）保留在原 RI 明细上，不再分摊。
        self.assertEqual(add_back, MODULE.Decimal("30"))

    def test_match_mode_from_flexibility(self):
        self.assertEqual(MODULE._match_mode_from_flexibility("on"), "flex-group")
        self.assertEqual(MODULE._match_mode_from_flexibility("On"), "flex-group")
        self.assertEqual(MODULE._match_mode_from_flexibility("off"), "model")
        self.assertEqual(MODULE._match_mode_from_flexibility(""), "model")
        self.assertEqual(MODULE._match_mode_from_flexibility(None), "model")

    def test_flexibility_field_derives_match_mode(self):
        path = self._write(
            [
                {
                    "reservationId": "ri-flex",
                    "flexibility": "on",
                    "bindings": [{"project": "x", "boundQuantity": 1}],
                },
                {
                    "reservationId": "ri-fixed",
                    "flexibility": "off",
                    "bindings": [{"project": "y", "boundQuantity": 1}],
                },
            ]
        )
        _result, modes, _dens = MODULE.load_reservations_file(path, project_tag_key="projname")
        self.assertEqual(modes["ri-flex"], "flex-group")
        self.assertEqual(modes["ri-fixed"], "model")


class ReservationsReallocationTests(unittest.TestCase):
    HEADERS = [
        "meterCategory",
        "ResourceId",
        "pricingModel",
        "chargeType",
        "reservationId",
        "meterRegion",
        "meterName",
        "additionalInfo",
        "tags",
        "costInBillingCurrency",
    ]

    def _row(self, **kw):
        row = {h: "" for h in self.HEADERS}
        row["meterCategory"] = "Virtual Machines"
        row.update(kw)
        return row

    def _run(self, rows, reservations):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        src = root / "bill.csv"
        with src.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.HEADERS)
            writer.writeheader()
            writer.writerows(rows)
        resv_path = root / "reservations.json"
        resv_path.write_text(json.dumps(reservations), encoding="utf-8")
        out_dir = root / "out"
        argv = [
            "prog",
            str(src),
            "--reservations-file",
            str(resv_path),
            "--project-tag-key",
            "projname",
            "--output-dir",
            str(out_dir),
        ]
        old_argv = sys.argv
        sys.argv = argv
        try:
            MODULE.main()
        finally:
            sys.argv = old_argv
        with (out_dir / "bill.csv").open(encoding="utf-8", newline="") as f:
            result_rows = list(csv.DictReader(f))
        summary = json.loads((out_dir / "ri-summary.json").read_text("utf-8"))
        return result_rows, summary

    def test_single_ri_split_to_two_projects_by_weight(self):
        rows = [
            # RI usage physically tagged to an unrelated project, cost 30
            self._row(
                ResourceId="/vm/ri-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="30",
            ),
            # alpha receiver (weight 2 -> gets 20)
            self._row(
                ResourceId="/vm/alpha-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
            # beta receiver (weight 1 -> gets 10)
            self._row(
                ResourceId="/vm/beta-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"beta"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [
            {
                "externalReservationId": ".../reservations/ri-a",
                "bindings": [
                    {"project": "alpha", "boundQuantity": 2},
                    {"project": "beta", "boundQuantity": 1},
                ],
            }
        ]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        self.assertEqual(summary["allocationMode"], "reservations")
        # RI usage fully added back +30
        self.assertEqual(by_id["/vm/ri-usage"]["riAllocationAmount"], "30")
        self.assertEqual(
            by_id["/vm/ri-usage"]["allocationType"], "RI_USAGE_COST_REASSIGNED"
        )
        # weighted split: alpha -20 (2/3), beta -10 (1/3)
        self.assertEqual(by_id["/vm/alpha-recv"]["riAllocationAmount"], "-20")
        self.assertEqual(by_id["/vm/beta-recv"]["riAllocationAmount"], "-10")
        self.assertEqual(
            summary["assignedByTarget"], {"alpha": "20", "beta": "10"}
        )
        # conservation
        total = sum(MODULE.Decimal(r["riAllocationAmount"]) for r in result_rows)
        self.assertEqual(total, MODULE.Decimal("0"))

    def test_bound_total_leaves_unbound_remainder_on_ri_row(self):
        # boundTotal=4 但绑定权重合计=3，未绑定部分 (1/4) 保留在原 RI 明细上。
        rows = [
            self._row(
                ResourceId="/vm/ri-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="40",
            ),
            self._row(
                ResourceId="/vm/alpha-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
            self._row(
                ResourceId="/vm/beta-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"beta"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [
            {
                "externalReservationId": ".../reservations/ri-a",
                "boundTotal": 4,
                "bindings": [
                    {"project": "alpha", "boundQuantity": 2},
                    {"project": "beta", "boundQuantity": 1},
                ],
            }
        ]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        # 40*2/4=20 给 alpha，40*1/4=10 给 beta，仅加回 30；剩余 10 留在原明细。
        self.assertEqual(by_id["/vm/ri-usage"]["riAllocationAmount"], "30")
        self.assertEqual(by_id["/vm/alpha-recv"]["riAllocationAmount"], "-20")
        self.assertEqual(by_id["/vm/beta-recv"]["riAllocationAmount"], "-10")
        self.assertEqual(summary["assignedByTarget"], {"alpha": "20", "beta": "10"})
        # 原始费用合计=40，待分摊=40*3/4=30，未绑定 1/4 不计入待分摊金额。
        self.assertEqual(summary["riRawTotalAmount"], "40")
        self.assertEqual(summary["riAllocatableAmount"], "30")
        total = sum(MODULE.Decimal(r["riAllocationAmount"]) for r in result_rows)
        self.assertEqual(total, MODULE.Decimal("0"))

    def test_ri_usage_in_bound_target_also_receives_benefit(self):
        # An RI usage record physically tagged with a bound target is added back
        # to full price AND participates as a receiver of that target's benefit
        # pool. Otherwise the target could receive less than its binding weight.
        rows = [
            self._row(
                ResourceId="/vm/ri-usage-in-alpha",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="12",
            ),
            self._row(
                ResourceId="/vm/alpha-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [
            {
                "reservationId": "ri-a",
                "bindings": [{"project": "alpha", "boundQuantity": 1}],
            }
        ]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        # add_back=12, full price of RI row=24, receiver basis total=24+100=124.
        # RI row share = 12*24/124, recv share = 12*100/124.
        ri_amt = MODULE.Decimal(by_id["/vm/ri-usage-in-alpha"]["riAllocationAmount"])
        recv_amt = MODULE.Decimal(by_id["/vm/alpha-recv"]["riAllocationAmount"])
        self.assertEqual(ri_amt, MODULE.Decimal("12") - MODULE.Decimal("12") * 24 / 124)
        self.assertEqual(recv_amt, -MODULE.Decimal("12") * 100 / 124)
        # RI usage row keeps a positive net (its share of its own benefit).
        self.assertGreater(ri_amt, MODULE.Decimal("0"))
        # allocationType still classifies the RI record as a reassignment.
        self.assertEqual(
            by_id["/vm/ri-usage-in-alpha"]["allocationType"],
            "RI_USAGE_COST_REASSIGNED",
        )
        total = sum(MODULE.Decimal(r["riAllocationAmount"]) for r in result_rows)
        self.assertEqual(total, MODULE.Decimal("0"))

    def test_ri_usage_can_be_sole_receiver_of_its_target(self):
        # If the bound target has no other VM, the added-back RI usage record is
        # itself the receiver, so allocation still succeeds (no error) and nets 0.
        rows = [
            self._row(
                ResourceId="/vm/ri-usage-only",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="12",
            ),
        ]
        reservations = [
            {
                "reservationId": "ri-a",
                "bindings": [{"project": "alpha", "boundQuantity": 1}],
            }
        ]
        result_rows, _summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        # add_back=12, full price=24, sole receiver basis=24 -> share=12.
        # net = 12 - 12 = 0.
        self.assertEqual(by_id["/vm/ri-usage-only"]["riAllocationAmount"], "0")



    def test_flexibility_on_enables_flex_group_matching(self):
        # RI usage is D2s_v5 but the receiver only runs D4s_v5. With
        # flexibility=on the reservation matches by flex-group, so the benefit
        # still flows; without it, model matching would fail to find a receiver.
        rows = [
            self._row(
                ResourceId="/vm/ri-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="10",
            ),
            self._row(
                ResourceId="/vm/alpha-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D4s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [
            {
                "externalReservationId": ".../reservations/ri-a",
                "flexibility": "on",
                "bindings": [{"project": "alpha", "boundQuantity": 1}],
            }
        ]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        self.assertEqual(summary["matchModeByReservation"], {"ri-a": "flex-group"})
        self.assertEqual(by_id["/vm/ri-usage"]["riAllocationAmount"], "10")
        self.assertEqual(by_id["/vm/alpha-recv"]["riAllocationAmount"], "-10")
        total = sum(MODULE.Decimal(r["riAllocationAmount"]) for r in result_rows)
        self.assertEqual(total, MODULE.Decimal("0"))

    def test_flexibility_off_requires_exact_model(self):
        # Same layout but flexibility off -> model matching -> no D2s_v5 receiver
        # in target alpha -> validation error.
        rows = [
            self._row(
                ResourceId="/vm/ri-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="10",
            ),
            self._row(
                ResourceId="/vm/alpha-recv",
                pricingModel="OnDemand",
                chargeType="Usage",
                reservationId="",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D4s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [
            {
                "externalReservationId": ".../reservations/ri-a",
                "flexibility": "off",
                "bindings": [{"project": "alpha", "boundQuantity": 1}],
            }
        ]
        with self.assertRaises(ValueError):
            self._run(rows, reservations)


if __name__ == "__main__":
    unittest.main()
