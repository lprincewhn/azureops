import importlib.util
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


if __name__ == "__main__":
    unittest.main()
