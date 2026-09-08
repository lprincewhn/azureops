import csv
import io
import json
import sys
import tempfile
import tracemalloc
import unittest
import urllib.error
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

import download_and_validate_billing as billing


def usage(name="row1", invoice="G123456789", amount="10.00", code="USD", kind="modern"):
    return {
        "id": name, "kind": kind, "tags": {"project": "demo"},
        "properties": {
            "invoiceId": invoice,
            "costInBillingCurrency" if kind == "modern" else "cost": amount,
            "billingCurrencyCode" if kind == "modern" else "billingCurrency": code,
            "resourceId": "/subscriptions/example/resources/vm",
            "meterDetails": {"meterCategory": "Virtual Machines", "meterName": "D2 v5"},
            "reservationId": "reservation", "benefitId": "benefit",
            "pricingModel": "Reservation", "chargeType": "Usage", "quantity": "1",
        },
    }


def invoice(name="G123456789", amount="10.00", code="USD"):
    return {"name": name, "properties": {
        "subTotal": {"value": amount, "currency": code},
        "taxAmount": {"value": "2", "currency": code},
        "totalAmount": {"value": "12", "currency": code},
        "amountDue": {"value": "0", "currency": code},
    }}


def rows(*records):
    return billing.normalize_usage(list(records), "modern")


class ReconciliationTests(unittest.TestCase):
    def test_single_pass_iterators_and_streamed_issues_match_list_results(self):
        actual = rows(usage(), usage("r2", "", "0"), usage("r3", "unknown", "-2"))
        amortized = rows(usage(amount="6"))
        expected, expected_issues = billing.reconcile(actual, amortized, [invoice()])
        emitted = []
        report, retained = billing.reconcile(
            iter(actual), iter(amortized), [invoice()], issue_sink=emitted.append,
        )
        self.assertEqual(report, expected)
        self.assertEqual(emitted, expected_issues)
        self.assertEqual(retained, [])
        empty, _ = billing.reconcile(iter([]), iter([]), [])
        self.assertEqual(empty["emptyDatasets"], ["actual", "amortized", "invoices"])
        self.assertFalse(empty["passed"])

    def test_uses_pretax_not_amount_due_or_amortized(self):
        report, issues = billing.reconcile(rows(usage()), rows(usage(amount="6")), [invoice()])
        self.assertTrue(report["passed"])
        self.assertFalse(issues)
        self.assertEqual(report["totalsByCurrency"][0]["amortizedAmount"], Decimal("6"))
        report, _ = billing.reconcile(rows(usage()), rows(usage()), [invoice()], "totalAmount")
        self.assertFalse(report["passed"])

    def test_missing_and_unknown_ids_include_zero_cost_rows(self):
        actual = rows(usage(), usage("r2", "", "0"), usage("r3", "unknown", "-2"))
        report, issues = billing.reconcile(actual, rows(usage()), [invoice()])
        self.assertFalse(report["passed"])
        self.assertEqual(report["missingInvoiceIdRows"], 1)
        self.assertEqual(report["unknownInvoiceIds"], ["unknown"])
        self.assertEqual([issue["csvRow"] for issue in issues], [3, 4])
        self.assertEqual(report["totalsByCurrency"][0]["actualAmount"], Decimal("8"))

    def test_invoice_without_actual_rows_does_not_match_even_if_zero(self):
        report, _ = billing.reconcile(rows(usage()), rows(usage()), [invoice(), invoice("other", "0")])
        self.assertEqual(report["invoicesWithoutActualRows"], ["other"])
        self.assertFalse(report["passed"])

    def test_equal_global_total_does_not_hide_invoice_differences(self):
        report, _ = billing.reconcile(
            rows(usage(amount="11"), usage("r2", "other", "9")), rows(usage()),
            [invoice(), invoice("other")],
        )
        self.assertEqual(report["totalsByCurrency"][0]["difference"], 0)
        self.assertFalse(report["invoiceAmountsMatch"])

    def test_currency_mismatch_and_separate_currency_totals(self):
        report, _ = billing.reconcile(rows(usage(code="EUR")), rows(usage()), [invoice()])
        self.assertEqual(report["invoices"][0]["status"], "currency_mismatch")
        self.assertEqual(len(report["totalsByCurrency"]), 2)
        self.assertFalse(report["passed"])
        report, _ = billing.reconcile(
            rows(usage(), usage("r2", "other", "10", "EUR")), rows(usage()),
            [invoice(), invoice("other", code="EUR")],
        )
        self.assertTrue(report["passed"])

    def test_decimal_tolerance_and_refunds(self):
        actual = rows(usage(amount="12.005"), usage("r2", amount="-2"))
        report, _ = billing.reconcile(actual, rows(usage()), [invoice()])
        self.assertTrue(report["passed"])
        self.assertEqual(report["invoices"][0]["difference"], Decimal("0.005"))
        report, _ = billing.reconcile(actual, rows(usage()), [invoice()], tolerance=Decimal("0.001"))
        self.assertFalse(report["passed"])

    def test_empty_datasets_cannot_pass(self):
        for actual, amortized, invoices in (
            ([], [], []), (rows(usage()), [], [invoice()]),
            ([], rows(usage()), [invoice()]), (rows(usage()), rows(usage()), []),
        ):
            with self.subTest(actual=actual, amortized=amortized, invoices=invoices):
                report, _ = billing.reconcile(actual, amortized, invoices)
                self.assertFalse(report["passed"])
                self.assertTrue(report["emptyDatasets"])

    def test_invalid_amounts_currency_and_duplicate_invoices_raise(self):
        for amount in (None, "", "NaN", "Infinity", "bad"):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                rows(usage(amount=amount))
        with self.assertRaises(ValueError):
            rows(usage(code=""))
        for invoices in ([invoice(), invoice()], [{"name": "x", "properties": {}}]):
            with self.assertRaises(ValueError):
                billing.reconcile(rows(usage()), rows(usage()), invoices)


class DownloadTests(unittest.TestCase):
    def args(self, *extra):
        return billing.parse_args([
            "--billing-account-name", "example:account",
            "--billing-profile-name", "profile",
            "--start-date", "2026-08-01", "--end-date", "2026-08-31", *extra,
        ])

    def test_urls_modern_and_legacy_use_correct_dates_and_metrics(self):
        args = self.args("--invoice-end-date", "2026-09-30")
        _, urls, invoice_url = billing.build_urls(args)
        for metric, url in urls.items():
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["metric"], [metric])
            self.assertEqual(query["startDate"], ["2026-08-01"])
            self.assertNotIn("$filter", query)
            self.assertIn("/billingProfiles/profile/", url)
        self.assertEqual(parse_qs(urlsplit(invoice_url).query)["periodEndDate"], ["2026-09-30"])
        args.usage_schema = "legacy"
        args.billing_profile_name = None
        _, urls, _ = billing.build_urls(args)
        query = parse_qs(urlsplit(urls["ActualCost"]).query)
        self.assertIn("properties/usageEnd le '2026-08-31'", query["$filter"][0])
        self.assertNotIn("startDate", query)

    def test_normalizes_nested_meter_tags_resource_and_legacy_cost(self):
        row = rows(usage())[0]
        self.assertEqual(row["meterCategory"], "Virtual Machines")
        self.assertEqual(row["tags"], {"project": "demo"})
        self.assertEqual(row["ResourceId"], "/subscriptions/example/resources/vm")
        row = billing.normalize_usage([usage(kind="legacy")], "legacy")[0]
        self.assertEqual(row["costInBillingCurrency"], Decimal("10.00"))
        with self.assertRaises(ValueError):
            billing.normalize_usage([usage(kind="legacy")], "modern")
        repeated = rows(usage(), usage(amount="2"), usage())
        self.assertEqual(len(repeated), 3)
        self.assertEqual(
            sum(row["costInBillingCurrency"] for row in repeated), Decimal("22"),
        )
        modern = usage()
        modern["properties"]["instanceName"] = modern["properties"].pop("resourceId")
        modern["properties"]["subscriptionGuid"] = "subscription"
        modern["properties"]["costInUSD"] = "10"
        row = rows(modern)[0]
        self.assertEqual(row["ResourceId"], modern["properties"]["instanceName"])
        self.assertEqual(row["subscriptionId"], "subscription")
        self.assertEqual(row["costInUsd"], "10")

    def test_pagination_follows_nextlink_without_rebuilding_query(self):
        api = billing.AzureAPI(Mock())
        first = billing.MANAGEMENT_URL + "/first?metric=ActualCost"
        second = billing.MANAGEMENT_URL + "/second?$skiptoken=a%2Bb"
        api.request = Mock(side_effect=[
            (200, {"value": [usage()], "nextLink": second}, {}),
            (200, {"value": [usage("r2")]}, {}),
        ])
        self.assertEqual(len(api.list(first)), 2)
        self.assertEqual(api.request.call_args_list[1].args[0], second)
        api.request = Mock(return_value=(200, {"value": [], "nextLink": first}, {}))
        with self.assertRaises(ValueError):
            api.list(first)
        api.request = Mock(return_value=(200, {}, {}))
        with self.assertRaises(ValueError):
            api.list(first)
        api.request = Mock(return_value=(204, {}, {}))
        self.assertEqual(api.list(first), [])

    def test_pagination_iterator_fetches_only_when_consumed(self):
        api = billing.AzureAPI(Mock())
        api.request = Mock(side_effect=[
            (200, {"value": [usage(), usage("r2")], "nextLink": "/second"}, {}),
            (200, {"value": [usage("r3")]}, {}),
        ])
        records = api.iter_list(billing.MANAGEMENT_URL + "/first")
        api.request.assert_not_called()
        self.assertEqual(next(records)["id"], "row1")
        self.assertEqual(next(records)["id"], "r2")
        self.assertEqual(api.request.call_count, 1)
        self.assertEqual(next(records)["id"], "r3")
        self.assertEqual(list(records), [])
        self.assertEqual(api.request.call_count, 2)

    def test_request_protects_tokens_and_parses_exact_decimals(self):
        credential = Mock()
        credential.get_token.return_value = SimpleNamespace(token="test-token")
        api = billing.AzureAPI(credential)
        for url in ("http://management.azure.com/x", "https://example.com/x", "https://management.azure.com@evil.test/x"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                api.request(url)
        credential.get_token.assert_not_called()
        response = Mock(status=200, headers={})
        response.read.return_value = b'{"value": 0.1234567890123456789}'
        api.opener = Mock()
        api.opener.open.return_value.__enter__ = Mock(return_value=response)
        api.opener.open.return_value.__exit__ = Mock(return_value=False)
        _, result, _ = api.request(billing.MANAGEMENT_URL + "/x")
        self.assertEqual(result["value"], Decimal("0.1234567890123456789"))

    def test_http_errors_are_not_empty_success_and_retries_are_bounded(self):
        api = billing.AzureAPI(Mock())
        api.credential.get_token.return_value = SimpleNamespace(token="test")
        api.opener = Mock()
        for code, attempts in ((403, 1), (429, 5), (503, 5)):
            api.opener.open.reset_mock()
            api.opener.open.side_effect = urllib.error.HTTPError(
                billing.MANAGEMENT_URL, code, "error", {"Retry-After": "0"}, None,
            )
            with patch.object(billing.time, "sleep"), self.assertRaises(urllib.error.HTTPError):
                api.request(billing.MANAGEMENT_URL)
            self.assertEqual(api.opener.open.call_count, attempts)

    def test_run_writes_pipeline_csv_and_report_even_when_validation_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = self.args("--output-dir", str(Path(tmp) / "output"))
            api = Mock()
            api.list.return_value = [invoice()]
            api.iter_list.side_effect = [iter([usage(invoice="")]), iter([usage(amount="3")])]
            report = billing.run(args, api)
            self.assertFalse(report["passed"])
            saved = json.loads((args.output_dir / "validation-summary.json").read_text())
            self.assertEqual(saved["missingInvoiceIdRows"], 1)
            actual_csv = next((args.output_dir / "actual").glob("Actual_*.csv"))
            with actual_csv.open(encoding="utf-8-sig") as source:
                row = next(csv.DictReader(source))
            self.assertEqual(row["costInBillingCurrency"], "10.00")
            self.assertEqual(json.loads(row["tags"]), {"project": "demo"})
            self.assertEqual(row["meterCategory"], "Virtual Machines")
            with self.assertRaises(FileExistsError):
                billing.run(args, api)

    def test_consumption_specific_retry_header_is_respected(self):
        api = billing.AzureAPI(Mock())
        api.credential.get_token.return_value = SimpleNamespace(token="test")
        api.opener = Mock()
        api.opener.open.side_effect = urllib.error.HTTPError(
            billing.MANAGEMENT_URL, 429, "throttled",
            {
                "Retry-After": "2",
                "x-ms-ratelimit-microsoft.consumption-commercial-retry-after": "60",
            }, None,
        )
        with patch.object(billing.time, "sleep") as sleep, \
                self.assertRaises(urllib.error.HTTPError):
            api.request(billing.MANAGEMENT_URL)
        self.assertEqual(sleep.call_count, 4)
        self.assertTrue(all(call.args == (60,) for call in sleep.call_args_list))

    def test_download_invoice_polls_and_does_not_forward_token(self):
        api = Mock()
        api.request.side_effect = [
            (202, {}, {"Location": billing.MANAGEMENT_URL + "/operation", "Retry-After": "0"}),
            (200, {"url": "https://example.test/invoice?signature=test"}, {}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(billing.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value = io.BytesIO(b"%PDF-example")
            output = Path(tmp) / "invoice.pdf"
            billing.download_invoice(api, billing.MANAGEMENT_URL + "/account", "G123", output, 60)
            self.assertEqual(output.read_bytes(), b"%PDF-example")
            self.assertEqual(open_url.call_args.args, ("https://example.test/invoice?signature=test",))
            self.assertFalse(output.with_suffix(".part").exists())

    def test_invoice_download_invalid_lro_and_timeout(self):
        api = Mock()
        for headers in ({}, {"Location": "/operation", "Retry-After": "60"}):
            api.request.return_value = (202, {}, headers)
            with self.assertRaises((ValueError, TimeoutError)):
                billing.download_invoice(api, billing.MANAGEMENT_URL + "/account", "G123", Path("unused"), 1)

    def test_invoice_zip_extension_and_invalid_content(self):
        api = Mock()
        api.request.return_value = (200, {"url": "https://example.test/document"}, {})
        with tempfile.TemporaryDirectory() as tmp, patch.object(billing.urllib.request, "urlopen") as open_url:
            output = Path(tmp) / "invoice.pdf"
            open_url.return_value.__enter__.return_value = io.BytesIO(b"PK\x03\x04example")
            downloaded = billing.download_invoice(api, billing.MANAGEMENT_URL, "G123", output, 60)
            self.assertEqual(downloaded.suffix, ".zip")
            self.assertFalse(output.exists())
            open_url.return_value.__enter__.return_value = io.BytesIO(b"<html>Error</html>")
            with self.assertRaises(ValueError):
                billing.download_invoice(api, billing.MANAGEMENT_URL, "G123", output, 60)
            self.assertFalse(output.exists())
            self.assertFalse(output.with_suffix(".part").exists())

    def test_main_exit_code_reflects_validation(self):
        identity = SimpleNamespace(AzureCliCredential=Mock())
        with patch.dict(sys.modules, {"azure.identity": identity}), \
                patch.object(billing, "parse_args", return_value=self.args()), \
                patch.object(billing, "run") as run, \
                patch("sys.stdout", new=io.StringIO()):
            run.return_value = {"passed": True}
            self.assertEqual(billing.main(), 0)
            run.return_value = {"passed": False}
            self.assertEqual(billing.main(), 2)

    def test_downloaded_csv_can_enter_actual_reconciliation(self):
        import allocate_ri_difference as allocation

        purchase = usage()
        purchase["properties"]["chargeType"] = "Purchase"
        amortized = usage(amount="6")
        amortized["properties"]["benefitId"] = (
            "/providers/Microsoft.Capacity/reservationOrders/reservation/reservations/ri"
        )
        amortized["properties"]["resourceId"] = (
            "/subscriptions/example/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm"
        )
        with tempfile.TemporaryDirectory() as tmp:
            args = self.args("--output-dir", str(Path(tmp) / "output"))
            api = Mock()
            api.list.return_value = [invoice()]
            api.iter_list.side_effect = [iter([purchase]), iter([amortized])]
            self.assertTrue(billing.run(args, api)["passed"])
            actual_files = allocation.read_csv_files(
                list((args.output_dir / "actual").glob("*.csv")), "costInBillingCurrency",
            )
            amortized_files = allocation.read_csv_files(
                list((args.output_dir / "amortized").glob("*.csv")), "costInBillingCurrency",
            )
            actual_costs, _ = allocation.collect_actual_costs(actual_files, "costInBillingCurrency")
            amortized_costs = allocation.collect_amortized_data(amortized_files, "costInBillingCurrency")[0]
            self.assertEqual(actual_costs["reservation"], Decimal("10"))
            self.assertEqual(amortized_costs["reservation"], Decimal("6"))

    def test_streamed_exports_preserve_late_columns_raw_records_and_duplicate_ids(self):
        first = usage()
        last = usage(amount="0.1234567890123456789")
        last["properties"]["lateColumn"] = {"nested": ["value", None]}
        last["properties"]["nullable"] = None
        with tempfile.TemporaryDirectory() as tmp:
            raw_path, csv_path = Path(tmp) / "raw.json", Path(tmp) / "rows.csv"
            exported = list(billing.export_usage(iter([first, last]), "modern", raw_path, csv_path))
            self.assertEqual(exported, rows(first, last))
            self.assertEqual(json.loads(raw_path.read_text()), [first, last])
            expected = Path(tmp) / "expected.csv"
            billing.write_csv(expected, rows(first, last), billing.CSV_FIELDS)
            self.assertEqual(csv_path.read_bytes(), expected.read_bytes())
            self.assertEqual({path.name for path in Path(tmp).iterdir()},
                             {"raw.json", "rows.csv", "expected.csv"})

    def test_empty_stream_exports_valid_json_and_csv_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_path, csv_path = Path(tmp) / "raw.json", Path(tmp) / "rows.csv"
            self.assertEqual(list(billing.export_usage(iter([]), "modern", raw_path, csv_path)), [])
            self.assertEqual(json.loads(raw_path.read_text()), [])
            with csv_path.open(encoding="utf-8-sig") as source:
                self.assertEqual(list(csv.reader(source)), [list(billing.CSV_FIELDS)])

    def test_late_page_failure_cannot_produce_success_report_or_leave_spool(self):
        def records():
            yield usage()
            raise ValueError("Invalid later page")

        with tempfile.TemporaryDirectory() as tmp:
            args = self.args("--output-dir", str(Path(tmp) / "output"))
            api = Mock()
            api.list.return_value = [invoice()]
            api.iter_list.side_effect = [records(), iter([usage()])]
            with self.assertRaisesRegex(ValueError, "Invalid later page"):
                billing.run(args, api)
            self.assertFalse((args.output_dir / "validation-summary.json").exists())
            self.assertEqual(list((args.output_dir / "actual").iterdir()), [])

    def test_download_memory_does_not_scale_with_usage_or_issue_row_count(self):
        class PagedAPI(billing.AzureAPI):
            def __init__(self, count):
                self.count = count

            def request(self, url, method="GET"):
                if "/invoices?" in url:
                    return 200, {"value": [invoice()]}, {}
                query = parse_qs(urlsplit(url).query)
                start = int(query.get("offset", ["0"])[0])
                end = min(start + 25, self.count)
                values = []
                for index in range(start, end):
                    record = usage(str(index), invoice="", amount="0.000000000000000001")
                    record["properties"]["additionalInfo"] = str(index) + "x" * 2048
                    values.append(record)
                payload = {"value": values}
                if end < self.count:
                    payload["nextLink"] = billing.MANAGEMENT_URL + f"/usage?offset={end}"
                return 200, payload, {}

        peaks = []
        for count in (100, 5000):
            with tempfile.TemporaryDirectory() as tmp:
                args = self.args("--output-dir", str(Path(tmp) / "output"))
                tracemalloc.start()
                try:
                    report = billing.run(args, PagedAPI(count))
                    peaks.append(tracemalloc.get_traced_memory()[1])
                finally:
                    tracemalloc.stop()
                self.assertEqual(report["actualRowCount"], count)
                self.assertEqual(report["amortizedRowCount"], count)
                self.assertEqual(report["missingInvoiceIdRows"], count)
                with (args.output_dir / "invoice-id-issues.csv").open(encoding="utf-8-sig") as source:
                    self.assertEqual(sum(1 for _ in csv.DictReader(source)), count)
        self.assertLess(peaks[1], peaks[0] + 2 * 1024 * 1024)

    def test_invalid_arguments_rejected(self):
        for extra in (
            ("--tolerance", "NaN"), ("--tolerance", "-1"),
            ("--end-date", "2026-07-31"), ("--invoice-download-timeout", "0"),
            ("--billing-account-name", "/providers/example"),
        ):
            with self.subTest(extra=extra), patch("sys.stderr", new=io.StringIO()), self.assertRaises(SystemExit):
                self.args(*extra)


if __name__ == "__main__":
    unittest.main()
