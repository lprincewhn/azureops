import argparse
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import price_sheet_download as module


class PriceSheetRequestTests(unittest.TestCase):
    def setUp(self):
        self.credential = argparse.Namespace()

    @mock.patch.object(module.time, "sleep")
    @mock.patch.object(module, "_request_json")
    def test_billing_profile_accepts_completed_status(self, request, _sleep):
        request.side_effect = [
            (
                202,
                {},
                {
                    "Azure-Consumption-AsyncOperation": (
                        "https://example.test/status"
                    ),
                    "Retry-After": "0",
                },
            ),
            (
                200,
                {
                    "status": "Completed",
                    "properties": {
                        "downloadUrl": "https://example.test/prices"
                    },
                },
                {},
            ),
        ]

        result = module.request_by_billing_profile(
            "account", "profile", self.credential, 30
        )

        self.assertEqual(
            module.extract_download_url(result),
            "https://example.test/prices",
        )

    @mock.patch.object(module.time, "sleep")
    @mock.patch.object(module, "_request_json")
    def test_invoice_fetches_result_from_location(self, request, _sleep):
        request.side_effect = [
            (
                202,
                {},
                {
                    "Azure-AsyncOperation": "https://example.test/status",
                    "Location": "https://example.test/result",
                    "Retry-After": "0",
                },
            ),
            (200, {"status": "Succeeded"}, {},),
            (
                200,
                {
                    "publishedEntity": {
                        "properties": {
                            "downloadUrl": "https://example.test/prices"
                        }
                    }
                },
                {},
            ),
        ]

        result = module.request_by_invoice(
            "account", "profile", "invoice", self.credential, 30
        )

        self.assertEqual(
            module.extract_download_url(result),
            "https://example.test/prices",
        )
        self.assertEqual(
            request.call_args_list[-1],
            mock.call("https://example.test/result", self.credential),
        )

    @mock.patch.object(module, "_request_json")
    def test_failed_generation_is_reported(self, request):
        request.side_effect = [
            (
                202,
                {},
                {
                    "Azure-AsyncOperation": "https://example.test/status",
                    "Location": "https://example.test/result",
                },
            ),
            (200, {"status": "Failed", "error": {"code": "Denied"}}, {}),
        ]

        with self.assertRaisesRegex(RuntimeError, "generation failed"):
            module.request_by_invoice(
                "account", "profile", "invoice", self.credential, 30
            )


class DownloadTests(unittest.TestCase):
    @mock.patch.object(module.urllib.request, "urlopen")
    def test_download_file_replaces_output(self, urlopen):
        response = mock.MagicMock()
        response.__enter__.return_value.read.side_effect = [b"price-sheet", b""]
        urlopen.return_value = response
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "pricesheet.zip"

            module.download_file("https://example.test/prices", output)

            self.assertEqual(output.read_bytes(), b"price-sheet")
            self.assertFalse((output.parent / ".pricesheet.zip.part").exists())

    def test_missing_download_url_is_reported(self):
        with self.assertRaisesRegex(RuntimeError, "download URL"):
            module.extract_download_url({"status": "Completed"})


if __name__ == "__main__":
    unittest.main()
