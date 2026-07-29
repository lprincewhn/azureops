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
    def test_parse_target_tag_requires_key_and_value(self):
        self.assertEqual(MODULE.parse_target_tag("projname=fota"), ("projname", "fota"))

    def test_parse_target_tag_rejects_missing_separator(self):
        with self.assertRaises(ValueError):
            MODULE.parse_target_tag("fota")

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


class MappingFileTests(unittest.TestCase):
    def _write(self, name, text):
        path = Path(self._tmp.name) / name
        path.write_text(text, encoding="utf-8")
        return path

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_load_json_object_form(self):
        path = self._write(
            "m.json", '{"ri-a": "projname=fota", "ri-b": "projname=beta"}'
        )
        mapping = MODULE.load_mapping_file(path)
        self.assertEqual(
            mapping, {"ri-a": ("projname", "fota"), "ri-b": ("projname", "beta")}
        )

    def test_load_json_mappings_list_form(self):
        path = self._write(
            "m.json",
            '{"mappings": [{"reservationId": "ri-a", "targetTag": "app=nacos"},'
            ' {"reservationId": "ri-b", "targetTag": {"key": "projname", "value": "beta"}}]}',
        )
        mapping = MODULE.load_mapping_file(path)
        self.assertEqual(
            mapping, {"ri-a": ("app", "nacos"), "ri-b": ("projname", "beta")}
        )

    def test_load_json_top_level_array(self):
        path = self._write(
            "m.json",
            '[{"reservationId": "ri-a", "targetTag": "projname=fota"}]',
        )
        self.assertEqual(
            MODULE.load_mapping_file(path), {"ri-a": ("projname", "fota")}
        )

    def test_load_csv_form(self):
        path = self._write(
            "m.csv",
            "reservationId,targetTag\nri-a,projname=fota\nri-b,projname=beta\n",
        )
        mapping = MODULE.load_mapping_file(path)
        self.assertEqual(
            mapping, {"ri-a": ("projname", "fota"), "ri-b": ("projname", "beta")}
        )

    def test_csv_missing_columns_rejected(self):
        path = self._write("m.csv", "reservationId\nri-a\n")
        with self.assertRaises(ValueError):
            MODULE.load_mapping_file(path)

    def test_duplicate_conflicting_reservation_rejected(self):
        path = self._write(
            "m.csv",
            "reservationId,targetTag\nri-a,projname=fota\nri-a,projname=beta\n",
        )
        with self.assertRaises(ValueError):
            MODULE.load_mapping_file(path)

    def test_empty_reservation_id_rejected(self):
        path = self._write("m.json", '{"": "projname=fota"}')
        with self.assertRaises(ValueError):
            MODULE.load_mapping_file(path)

    def test_json_duplicate_object_key_rejected(self):
        path = self._write(
            "m.json", '{"ri-a": "projname=fota", "ri-a": "projname=beta"}'
        )
        with self.assertRaises(ValueError):
            MODULE.load_mapping_file(path)

    def test_build_map_mapping_file_conflicts_with_inline(self):
        args = argparse.Namespace(
            mapping_file="x.json", reservation_id=["ri-a"], target_tag=None
        )
        with self.assertRaises(ValueError):
            MODULE.build_ri_target_map(args)

    def test_build_map_inline_single_target(self):
        args = argparse.Namespace(
            mapping_file=None,
            reservation_id=["ri-a", "ri-b"],
            target_tag="projname=fota",
        )
        self.assertEqual(
            MODULE.build_ri_target_map(args),
            {"ri-a": ("projname", "fota"), "ri-b": ("projname", "fota")},
        )

    def test_build_map_requires_something(self):
        args = argparse.Namespace(
            mapping_file=None, reservation_id=None, target_tag=None
        )
        with self.assertRaises(ValueError):
            MODULE.build_ri_target_map(args)


class MultiTargetReallocationTests(unittest.TestCase):
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

    def _run(self, rows, mapping_json):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        src = root / "bill.csv"
        with src.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.HEADERS)
            writer.writeheader()
            writer.writerows(rows)
        mapping_path = root / "map.json"
        mapping_path.write_text(json.dumps(mapping_json), encoding="utf-8")
        out_dir = root / "out"
        argv = [
            "prog",
            str(src),
            "--mapping-file",
            str(mapping_path),
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

    def test_different_ri_go_to_different_targets(self):
        rows = [
            # RI-A usage (belongs to shared project), reallocated to fota
            self._row(
                ResourceId="/vm/ri-a-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"shared"}',
                costInBillingCurrency="10",
            ),
            # RI-B usage, reallocated to beta
            self._row(
                ResourceId="/vm/ri-b-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-b",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_E2s_v5"}',
                tags='{"projname":"shared"}',
                costInBillingCurrency="20",
            ),
            # fota receiver (same model/region as ri-a)
            self._row(
                ResourceId="/vm/fota-recv",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"fota"}',
                costInBillingCurrency="100",
            ),
            # beta receiver (same model/region as ri-b)
            self._row(
                ResourceId="/vm/beta-recv",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_E2s_v5"}',
                tags='{"projname":"beta"}',
                costInBillingCurrency="200",
            ),
        ]
        mapping = {"ri-a": "projname=fota", "ri-b": "projname=beta"}
        result_rows, summary = self._run(rows, mapping)
        by_id = {r["ResourceId"]: r for r in result_rows}
        # RI-A usage added back +10, target fota
        self.assertEqual(by_id["/vm/ri-a-usage"]["riAllocationAmount"], "10")
        self.assertEqual(by_id["/vm/ri-a-usage"]["allocationTarget"], "fota")
        # RI-B usage added back +20, target beta
        self.assertEqual(by_id["/vm/ri-b-usage"]["riAllocationAmount"], "20")
        self.assertEqual(by_id["/vm/ri-b-usage"]["allocationTarget"], "beta")
        # fota receiver deducted -10 (only ri-a pool)
        self.assertEqual(by_id["/vm/fota-recv"]["riAllocationAmount"], "-10")
        self.assertEqual(by_id["/vm/fota-recv"]["allocationTarget"], "fota")
        # beta receiver deducted -20 (only ri-b pool)
        self.assertEqual(by_id["/vm/beta-recv"]["riAllocationAmount"], "-20")
        self.assertEqual(by_id["/vm/beta-recv"]["allocationTarget"], "beta")
        # conservation: sum of adjustments == 0
        total = sum(
            MODULE.Decimal(r["riAllocationAmount"]) for r in result_rows
        )
        self.assertEqual(total, MODULE.Decimal("0"))
        self.assertEqual(
            summary["assignedByTarget"], {"beta": "20", "fota": "10"}
        )

    def test_ri_benefit_isolated_between_targets(self):
        # fota receiver must NOT absorb ri-b benefit even though same model/region
        rows = [
            self._row(
                ResourceId="/vm/ri-b-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-b",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"shared"}',
                costInBillingCurrency="5",
            ),
            self._row(
                ResourceId="/vm/fota-recv",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"fota"}',
                costInBillingCurrency="100",
            ),
            self._row(
                ResourceId="/vm/beta-recv",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"beta"}',
                costInBillingCurrency="100",
            ),
        ]
        mapping = {"ri-b": "projname=beta"}
        by_id = {r["ResourceId"]: r for r in self._run(rows, mapping)[0]}
        # beta absorbs the -5, fota untouched
        self.assertEqual(by_id["/vm/beta-recv"]["riAllocationAmount"], "-5")
        self.assertEqual(by_id["/vm/fota-recv"]["riAllocationAmount"], "0")
        self.assertEqual(by_id["/vm/fota-recv"]["allocationType"], "")

    def test_zero_cost_ri_with_no_receiver_does_not_crash(self):
        # RI usage row with 0 cost and no matching receiver: nothing to allocate,
        # must not raise the "找不到分摊目标" validation error.
        rows = [
            self._row(
                ResourceId="/vm/ri-zero",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-z",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"shared"}',
                costInBillingCurrency="0",
            ),
        ]
        mapping = {"ri-z": "projname=fota"}
        result_rows, _ = self._run(rows, mapping)
        self.assertEqual(result_rows[0]["riAllocationAmount"], "0")
        self.assertEqual(result_rows[0]["allocationType"], "")


if __name__ == "__main__":
    unittest.main()
