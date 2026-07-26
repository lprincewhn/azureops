import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("reallocate_ri.py")
SPEC = importlib.util.spec_from_file_location("reallocate_ri", MODULE_PATH)
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

    def test_size_flexibility_helpers_read_additional_info(self):
        row = {
            "additionalInfo": (
                '{"ServiceType":"Standard_D2s_v3",'
                '"InstanceFlexibilityGroup":"DSv3 Series",'
                '"InstanceFlexibilityRatio":"1"}'
            )
        }
        self.assertTrue(MODULE.is_size_flexible(row))
        self.assertEqual(MODULE.instance_flexibility_group(row), "DSv3 Series")
        self.assertEqual(MODULE.instance_flexibility_ratio(row), MODULE.Decimal("1"))

    def test_size_flexibility_helpers_default_when_absent(self):
        row = {"additionalInfo": '{"ServiceType":"Standard_D2s_v3"}'}
        self.assertFalse(MODULE.is_size_flexible(row))
        self.assertEqual(MODULE.instance_flexibility_group(row), "")
        self.assertIsNone(MODULE.instance_flexibility_ratio(row))

    def test_flex_group_match_mode_groups_different_models(self):
        d2 = {
            "meterRegion": "AP Southeast",
            "additionalInfo": (
                '{"ServiceType":"Standard_D2s_v3","InstanceFlexibilityGroup":"DSv3 Series"}'
            ),
        }
        d4 = {
            "meterRegion": "AP Southeast",
            "additionalInfo": (
                '{"ServiceType":"Standard_D4s_v3","InstanceFlexibilityGroup":"DSv3 Series"}'
            ),
        }
        # model 模式下不同机型分到不同池
        self.assertNotEqual(
            MODULE.allocation_key(d2, "model"), MODULE.allocation_key(d4, "model")
        )
        # flex-group 模式下同一灵活性分组落入同一池
        self.assertEqual(
            MODULE.allocation_key(d2, "flex-group"),
            MODULE.allocation_key(d4, "flex-group"),
        )

    def test_flex_group_match_mode_falls_back_to_model(self):
        row = {
            "meterRegion": "US West 3",
            "additionalInfo": '{"ServiceType":"Standard_D2s_v5"}',
        }
        self.assertEqual(
            MODULE.allocation_key(row, "flex-group"),
            ("Standard_D2s_v5", "US West 3"),
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
