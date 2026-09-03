import argparse
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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
        self.assertEqual(
            MODULE.allocated_field_name(
                "costInBillingCurrencyAfterActualReconciliation"
            ),
            "allocatedCostInBillingCurrency",
        )


class PriceSheetTests(unittest.TestCase):
    def test_consumption_unit_price_matches_meter_date_and_currency(self):
        content = (
            "meterId,priceType,tierMinimumUnits,unitPrice,billingCurrency,"
            "effectiveStartDate,effectiveEndDate\n"
            "meter-a,Consumption,0,2.5,USD,2026-07-01,2026-07-31\n"
            "meter-a,ReservedInstance,0,1.0,USD,2026-07-01,2026-07-31\n"
        ).encode()
        rates = MODULE.parse_price_sheet(content, "prices.csv")
        self.assertEqual(
            MODULE.price_for_row(
                {
                    "meterId": "METER-A",
                    "date": "07/18/2026",
                    "billingCurrency": "USD",
                },
                rates,
            ),
            MODULE.Decimal("2.5"),
        )

    def test_json_price_sheet_without_price_type(self):
        content = json.dumps(
            [
                {
                    "meterId": "meter-a",
                    "tierMinimumUnits": 0,
                    "unitPrice": 2.5,
                    "billingCurrency": "USD",
                    "effectiveStartDate": "2026-07-01T00:00:00Z",
                    "effectiveEndDate": "2026-07-31T23:59:59Z",
                }
            ]
        ).encode()
        rates = MODULE.parse_price_sheet(content, "prices.json")
        self.assertEqual(
            MODULE.price_for_row(
                {
                    "meterId": "meter-a",
                    "date": "07/18/2026",
                    "billingCurrency": "USD",
                },
                rates,
            ),
            MODULE.Decimal("2.5"),
        )

    def test_missing_meter_price_raises(self):
        rates = MODULE.parse_price_sheet(
            b"meterId,priceType,tierMinimumUnits,unitPrice,billingCurrency\n"
            b"meter-a,Consumption,0,2.5,USD\n",
            "prices.csv",
        )
        with self.assertRaises(ValueError):
            MODULE.price_for_row(
                {
                    "meterId": "meter-b",
                    "date": "2026-07-18",
                    "billingCurrency": "USD",
                },
                rates,
            )

    def test_resolve_short_billing_account_name(self):
        class Credential:
            def get_token(self, _scope):
                return argparse.Namespace(token="token")

        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            {
                "value": [
                    {"name": "other:tenant_2020-01-01"},
                    {"name": "account:tenant_2019-05-31"},
                ]
            }
        ).encode()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            self.assertEqual(
                MODULE.resolve_billing_account_name("account", Credential()),
                "account:tenant_2019-05-31",
            )

    def test_explicit_invoice_ignores_other_invoice_ids(self):
        rows = [
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "invoiceId": "invoice-a",
                "date": "07/01/2026",
            },
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "invoiceId": "invoice-b",
                "date": "07/02/2026",
            },
        ]
        result = argparse.Namespace(download_url="https://example.test/prices")
        poller = mock.Mock()
        poller.done.return_value = True
        poller.result.return_value = result
        client = mock.Mock()
        client.price_sheet.begin_download_by_invoice.return_value = poller
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"price-sheet"
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            payload = MODULE.download_price_sheet(
                rows,
                invoice_id="selected-invoice",
                billing_account_name="full-account",
                timeout=30,
                client=client,
            )
        self.assertEqual(payload, b"price-sheet")
        client.price_sheet.begin_download_by_invoice.assert_called_once_with(
            billing_account_name="full-account",
            billing_profile_name="profile",
            invoice_name="selected-invoice",
        )
        poller.wait.assert_called_once_with(timeout=30)
        poller.result.assert_called_once_with()

    def test_incomplete_invoice_ids_fall_back_to_billing_profile(self):
        rows = [
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "invoiceId": "invoice-a",
                "date": "07/01/2026",
            },
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "invoiceId": "",
                "date": "07/02/2026",
            },
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "date": "07/03/2026",
            },
        ]
        result = argparse.Namespace(download_url="https://example.test/prices")
        poller = mock.Mock()
        poller.done.return_value = True
        poller.result.return_value = result
        client = mock.Mock()
        client.price_sheet.begin_download_by_billing_profile.return_value = poller
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"price-sheet"
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            payload = MODULE.download_price_sheet(
                rows,
                billing_account_name="full-account",
                timeout=30,
                client=client,
            )
        self.assertEqual(payload, b"price-sheet")
        client.price_sheet.begin_download_by_billing_profile.assert_called_once_with(
            billing_account_name="full-account",
            billing_profile_name="profile",
        )
        client.price_sheet.begin_download_by_invoice.assert_not_called()
        poller.wait.assert_called_once_with(timeout=30)
        poller.result.assert_called_once_with()

    def test_complete_invoice_ids_still_use_invoice(self):
        rows = [
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "invoiceId": "invoice-a",
                "date": "07/01/2026",
            },
            {
                "billingAccountId": "short-account",
                "billingProfileId": "profile",
                "invoiceId": "invoice-a",
                "date": "07/02/2026",
            },
        ]
        result = argparse.Namespace(download_url="https://example.test/prices")
        poller = mock.Mock()
        poller.done.return_value = True
        poller.result.return_value = result
        client = mock.Mock()
        client.price_sheet.begin_download_by_invoice.return_value = poller
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"price-sheet"
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            payload = MODULE.download_price_sheet(
                rows,
                billing_account_name="full-account",
                timeout=30,
                client=client,
            )
        self.assertEqual(payload, b"price-sheet")
        client.price_sheet.begin_download_by_invoice.assert_called_once_with(
            billing_account_name="full-account",
            billing_profile_name="profile",
            invoice_name="invoice-a",
        )
        client.price_sheet.begin_download_by_billing_profile.assert_not_called()

    def test_price_sheet_timeout_is_explicit(self):
        poller = mock.Mock()
        poller.done.return_value = False
        with self.assertRaisesRegex(TimeoutError, "30 秒内未生成完成"):
            MODULE.wait_for_price_sheet(poller, 30)
        poller.wait.assert_called_once_with(timeout=30)
        poller.result.assert_not_called()

    def test_wrapped_price_sheet_download_url(self):
        result = {
            "publishedEntity": {
                "properties": {
                    "downloadUrl": "https://example.test/prices"
                }
            }
        }
        self.assertEqual(
            MODULE.price_sheet_download_url(result),
            "https://example.test/prices",
        )

    def test_invoice_polling_accepts_completed_status(self):
        initial = argparse.Namespace(
            status=202,
            headers={
                "Azure-AsyncOperation": "https://example.test/status",
                "Location": "https://example.test/result",
                "Retry-After": "0",
            },
        )
        processing = argparse.Namespace(status=200, headers={})
        final = argparse.Namespace(status=200, headers={})
        wrapped = {
            "publishedEntity": {
                "properties": {
                    "downloadUrl": "https://example.test/prices"
                }
            }
        }
        with mock.patch.object(
            MODULE,
            "_authorized_json_request",
            side_effect=[
                (initial, {}),
                (processing, {"status": "Completed"}),
                (final, wrapped),
            ],
        ):
            result = MODULE.download_price_sheet_by_invoice(
                "account",
                "profile",
                "invoice",
                argparse.Namespace(),
                30,
            )
        self.assertEqual(result, wrapped)

    def test_billing_profile_polling_accepts_completed_status(self):
        initial = argparse.Namespace(
            status=202,
            headers={
                "Azure-Consumption-AsyncOperation": (
                    "https://example.test/status"
                ),
                "Retry-After": "0",
            },
        )
        final = argparse.Namespace(status=200, headers={})
        wrapped = {
            "status": "Completed",
            "properties": {
                "downloadUrl": "https://example.test/prices"
            },
        }
        with mock.patch.object(
            MODULE,
            "_authorized_json_request",
            side_effect=[
                (initial, {}),
                (final, wrapped),
            ],
        ):
            result = MODULE.download_price_sheet_by_billing_profile(
                "account",
                "profile",
                argparse.Namespace(),
                30,
            )
        self.assertEqual(result, wrapped)


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

    def test_single_scope_is_loaded_and_matches_resource_id(self):
        path = self._write(
            [{
                "reservationId": "ri-a",
                "appliedScopeType": "Single",
                "appliedScopeId": "/subscriptions/sub-a",
                "bindings": [{"project": "x", "boundQuantity": 1}],
            }]
        )
        _targets, _modes, _dens, scopes = MODULE.load_reservations_config(
            path, project_tag_key="projname"
        )
        self.assertEqual(scopes["ri-a"], ("single", "/subscriptions/sub-a"))
        self.assertTrue(
            MODULE.row_matches_ri_scope(
                {"ResourceId": "/subscriptions/SUB-A/resourceGroups/rg/providers/x/y"},
                scopes["ri-a"],
            )
        )
        self.assertFalse(
            MODULE.row_matches_ri_scope(
                {"ResourceId": "/subscriptions/sub-b/resourceGroups/rg/providers/x/y"},
                scopes["ri-a"],
            )
        )

    def test_single_scope_requires_scope_id(self):
        path = self._write(
            [{
                "reservationId": "ri-a",
                "appliedScopeType": "Single",
                "bindings": [{"project": "x", "boundQuantity": 1}],
            }]
        )
        with self.assertRaises(ValueError):
            MODULE.load_reservations_config(path, project_tag_key="projname")

    def test_management_group_scope_resolves_descendant_subscriptions(self):
        class FakeManagementGroups:
            def get_descendants(self, group_name):
                self.group_name = group_name
                return [
                    {
                        "type": "Microsoft.Management/managementGroups",
                        "name": "child-group",
                    },
                    {
                        "type": "Microsoft.Management/managementGroups/subscriptions",
                        "name": "SUB-A",
                    },
                ]

        class FakeClient:
            management_groups = FakeManagementGroups()

        scope = (
            "managementgroup",
            "/providers/microsoft.management/managementgroups/ffalcon-us",
        )
        resolved = MODULE.resolve_management_group_scopes(
            {"ri-a": scope}, client=FakeClient()
        )
        self.assertEqual(FakeClient.management_groups.group_name, "ffalcon-us")
        self.assertEqual(resolved[scope], frozenset({"sub-a"}))

    def test_management_group_entities_resolve_parent_chain(self):
        class Credential:
            def get_token(self, _scope):
                return argparse.Namespace(token="token")

        scope = (
            "managementgroup",
            "/providers/microsoft.management/managementgroups/ffalcon-us",
        )
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            {
                "value": [
                    {
                        "name": "sub-a",
                        "type": "/subscriptions",
                        "properties": {
                            "parentNameChain": ["root", "ffalcon-us"]
                        },
                    },
                    {
                        "name": "sub-b",
                        "type": "/subscriptions",
                        "properties": {"parentNameChain": ["root", "other"]},
                    },
                ]
            }
        ).encode()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            resolved = MODULE.resolve_management_group_scopes_from_entities(
                {"ri-a": scope}, Credential()
            )
        self.assertEqual(resolved[scope], frozenset({"sub-a"}))
        self.assertTrue(
            MODULE.row_matches_ri_scope(
                {"ResourceId": "/subscriptions/sub-a/resourceGroups/rg/providers/x/y"},
                scope,
                resolved,
            )
        )
        self.assertFalse(
            MODULE.row_matches_ri_scope(
                {"SubscriptionId": "sub-b"}, scope, resolved
            )
        )


class ReservationsReallocationTests(unittest.TestCase):
    HEADERS = [
        "meterCategory",
        "ResourceId",
        "pricingModel",
        "chargeType",
        "reservationId",
        "SubscriptionId",
        "meterId",
        "meterRegion",
        "meterName",
        "date",
        "quantity",
        "billingCurrency",
        "billingAccountId",
        "billingProfileId",
        "invoiceId",
        "additionalInfo",
        "tags",
        "costInBillingCurrency",
        "costInBillingCurrencyAfterActualReconciliation",
    ]

    def _row(self, **kw):
        row = {h: "" for h in self.HEADERS}
        row["meterCategory"] = "Virtual Machines"
        row["meterId"] = "meter-vm"
        row["date"] = "07/01/2026"
        row["quantity"] = "1"
        row["billingCurrency"] = "USD"
        row.update(kw)
        return row

    def _run(
        self,
        rows,
        reservations,
        amount_field="costInBillingCurrency",
        unit_prices=None,
    ):
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
        prices: dict[str, MODULE.Decimal] = {}
        for row in rows:
            if (
                row.get("pricingModel") == "Reservation"
                and row.get("chargeType") == "Usage"
            ):
                quantity = MODULE.Decimal(row.get("quantity") or "0")
                amount = MODULE.Decimal(row.get("costInBillingCurrency") or "0")
                prices[row["meterId"]] = (
                    MODULE.Decimal(str(unit_prices[row["meterId"]]))
                    if unit_prices and row["meterId"] in unit_prices
                    else amount * 2 / quantity
                )
        price_path = root / "price-sheet.csv"
        with price_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "meterId",
                    "priceType",
                    "tierMinimumUnits",
                    "unitPrice",
                    "billingCurrency",
                ],
            )
            writer.writeheader()
            for meter_id, unit_price in prices.items():
                writer.writerow(
                    {
                        "meterId": meter_id,
                        "priceType": "Consumption",
                        "tierMinimumUnits": "0",
                        "unitPrice": str(unit_price),
                        "billingCurrency": "USD",
                    }
                )
        out_dir = root / "out"
        argv = [
            "prog",
            str(src),
            "--reservations-file",
            str(resv_path),
            "--project-tag-key",
            "projname",
            "--price-sheet-file",
            str(price_path),
            "--output-dir",
            str(out_dir),
            "--amount-field",
            amount_field,
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
        with (out_dir / "ri-allocation-details.csv").open(
            encoding="utf-8", newline=""
        ) as f:
            summary["_testAllocationDetails"] = list(csv.DictReader(f))
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
        self.assertNotIn("riAllocationReservationIds", by_id["/vm/ri-usage"])
        # weighted split: alpha -20 (2/3), beta -10 (1/3)
        self.assertEqual(by_id["/vm/alpha-recv"]["riAllocationAmount"], "-20")
        self.assertEqual(by_id["/vm/beta-recv"]["riAllocationAmount"], "-10")
        details = summary["_testAllocationDetails"]
        self.assertEqual(
            {
                (
                    row["ResourceId"],
                    row["allocationType"],
                    row["riAllocationReservationIds"],
                    row["allocationAmount"],
                )
                for row in details
            },
            {
                (
                    "/vm/ri-usage",
                    "RI_USAGE_COST_REASSIGNED",
                    "ri-a",
                    "30",
                ),
                (
                    "/vm/alpha-recv",
                    "RI_BENEFIT_ASSIGNED",
                    "ri-a",
                    "-20",
                ),
                (
                    "/vm/beta-recv",
                    "RI_BENEFIT_ASSIGNED",
                    "ri-a",
                    "-10",
                ),
            },
        )
        self.assertEqual(
            summary["assignedByTarget"], {"alpha": "20", "beta": "10"}
        )
        # conservation
        total = sum(MODULE.Decimal(r["riAllocationAmount"]) for r in result_rows)
        self.assertEqual(total, MODULE.Decimal("0"))

    def test_receiver_records_all_contributing_reservation_ids(self):
        rows = [
            self._row(
                ResourceId="/vm/ri-a",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterId="meter-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="10",
            ),
            self._row(
                ResourceId="/vm/ri-b",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-b",
                meterId="meter-b",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="10",
            ),
            self._row(
                ResourceId="/vm/receiver",
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
                "reservationId": reservation_id,
                "bindings": [{"project": "alpha", "boundQuantity": 1}],
            }
            for reservation_id in ("ri-a", "ri-b")
        ]
        result_rows, summary = self._run(rows, reservations)
        by_id = {row["ResourceId"]: row for row in result_rows}
        self.assertNotIn("riAllocationReservationIds", by_id["/vm/receiver"])
        receiver_details = [
            row
            for row in summary["_testAllocationDetails"]
            if row["ResourceId"] == "/vm/receiver"
        ]
        self.assertEqual(
            {
                (
                    row["riAllocationReservationIds"],
                    row["allocationAmount"],
                )
                for row in receiver_details
            },
            {("ri-a", "-10"), ("ri-b", "-10")},
        )

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

    def test_single_scope_only_allocates_to_eligible_subscription(self):
        rows = [
            self._row(
                ResourceId="/subscriptions/sub-a/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/ri",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="10",
            ),
            self._row(
                ResourceId="/subscriptions/sub-a/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/eligible",
                pricingModel="OnDemand",
                chargeType="Usage",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
            self._row(
                ResourceId="/subscriptions/sub-b/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/ineligible",
                pricingModel="OnDemand",
                chargeType="Usage",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [{
            "reservationId": "ri-a",
            "appliedScopeType": "Single",
            "appliedScopeId": "/subscriptions/sub-a",
            "bindings": [{"project": "alpha", "boundQuantity": 1}],
        }]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        self.assertEqual(by_id[next(k for k in by_id if k.endswith("/eligible"))]["riAllocationAmount"], "-10")
        self.assertEqual(by_id[next(k for k in by_id if k.endswith("/ineligible"))]["riAllocationAmount"], "0")
        self.assertEqual(summary["mappings"][0]["appliedScopeType"], "single")

    def test_management_group_scope_only_allocates_to_descendant_subscription(self):
        original_resolver = MODULE.resolve_management_group_scopes
        MODULE.resolve_management_group_scopes = lambda scopes: {
            next(iter(scopes.values())): frozenset({"sub-a"})
        }
        self.addCleanup(
            setattr, MODULE, "resolve_management_group_scopes", original_resolver
        )
        rows = [
            self._row(
                ResourceId="/subscriptions/sub-a/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/ri",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"misc"}',
                costInBillingCurrency="10",
            ),
            self._row(
                ResourceId="/subscriptions/sub-a/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/eligible",
                pricingModel="OnDemand",
                chargeType="Usage",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
            self._row(
                ResourceId="/subscriptions/sub-b/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/ineligible",
                pricingModel="OnDemand",
                chargeType="Usage",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [{
            "reservationId": "ri-a",
            "appliedScopeType": "ManagementGroup",
            "appliedScopeId": "/providers/Microsoft.Management/managementGroups/ffalcon-us",
            "bindings": [{"project": "alpha", "boundQuantity": 1}],
        }]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        eligible = next(k for k in by_id if k.endswith("/eligible"))
        ineligible = next(k for k in by_id if k.endswith("/ineligible"))
        self.assertEqual(by_id[eligible]["riAllocationAmount"], "-10")
        self.assertEqual(by_id[ineligible]["riAllocationAmount"], "0")
        self.assertEqual(
            summary["mappings"][0]["managementGroupSubscriptionCount"], 1
        )

    def test_unused_reservation_cost_is_reported_but_not_allocated(self):
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
                ResourceId="",
                pricingModel="Reservation",
                chargeType="UnusedReservation",
                reservationId="ri-a",
                costInBillingCurrency="4",
            ),
            self._row(
                ResourceId="/vm/alpha",
                pricingModel="OnDemand",
                chargeType="Usage",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
            ),
        ]
        reservations = [{
            "reservationId": "ri-a",
            "bindings": [{"project": "alpha", "boundQuantity": 1}],
        }]
        result_rows, summary = self._run(rows, reservations)
        by_id = {r["ResourceId"]: r for r in result_rows}
        self.assertEqual(by_id["/vm/ri-usage"]["riPaygEquivalentAmount"], "20")
        self.assertEqual(by_id["/vm/ri-usage"]["riBenefitOrLoss"], "10")
        self.assertNotIn("riGrossSavings", by_id["/vm/ri-usage"])
        self.assertEqual(by_id["/vm/ri-usage"]["riAllocationAmount"], "10")
        self.assertEqual(by_id["/vm/alpha"]["riAllocationAmount"], "-10")
        self.assertEqual(summary["riNetBenefitOrLoss"], "10")
        self.assertNotIn("riGrossSavings", summary)
        self.assertEqual(summary["riUnusedCost"], "4")
        self.assertEqual(summary["riPortfolioNetSavings"], "6")

    def test_reconciled_cost_above_payg_reallocates_excess_cost(self):
        rows = [
            self._row(
                ResourceId="/vm/ri-usage",
                pricingModel="Reservation",
                chargeType="Usage",
                reservationId="ri-a",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"source"}',
                costInBillingCurrency="60",
                costInBillingCurrencyAfterActualReconciliation="120",
            ),
            self._row(
                ResourceId="/vm/target",
                pricingModel="OnDemand",
                chargeType="Usage",
                meterRegion="US West 3",
                additionalInfo='{"ServiceType":"Standard_D2s_v5"}',
                tags='{"projname":"alpha"}',
                costInBillingCurrency="100",
                costInBillingCurrencyAfterActualReconciliation="100",
            ),
        ]
        reservations = [{
            "reservationId": "ri-a",
            "bindings": [{"project": "alpha", "boundQuantity": 1}],
        }]
        result_rows, summary = self._run(
            rows,
            reservations,
            amount_field="costInBillingCurrencyAfterActualReconciliation",
            unit_prices={"meter-vm": "100"},
        )
        by_id = {row["ResourceId"]: row for row in result_rows}
        self.assertEqual(by_id["/vm/ri-usage"]["riBenefitOrLoss"], "-20")
        self.assertEqual(by_id["/vm/ri-usage"]["riAllocationAmount"], "-20")
        self.assertEqual(
            by_id["/vm/ri-usage"]["allocatedCostInBillingCurrency"], "100"
        )
        self.assertEqual(by_id["/vm/target"]["riAllocationAmount"], "20")
        self.assertEqual(
            by_id["/vm/target"]["allocationType"], "RI_BENEFIT_ASSIGNED"
        )
        self.assertEqual(
            by_id["/vm/target"]["allocatedCostInBillingCurrency"], "120"
        )
        self.assertEqual(summary["riGrossBenefit"], "0")
        self.assertEqual(summary["riExcessCost"], "20")
        self.assertEqual(summary["riNetBenefitOrLoss"], "-20")
        self.assertEqual(
            sum(
                MODULE.Decimal(row["riAllocationAmount"])
                for row in result_rows
            ),
            MODULE.Decimal("0"),
        )


if __name__ == "__main__":
    unittest.main()
