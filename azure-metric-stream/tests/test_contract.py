import json
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from contract import normalize_batch, parse_event, samples
from exporter import Store, handler, process_event


RESOURCE = "/subscriptions/example/resourceGroups/demo/providers/Microsoft.Storage/storageAccounts/example"
TIME = "2026-11-25T03:00:00Z"


class ContractTests(unittest.TestCase):
    def test_dcr_and_batch_take_same_path(self):
        dcr = {
            "resourceId": RESOURCE, "time": TIME, "unit": "Count",
            "metricName": "Transactions", "timeGrain": "PT1M",
            "dimension": {"ApiName": "GetBlob", "ResponseType": "Success"},
            "Total": "12", "Count": "4",
        }
        batch = {
            "values": [{
                "resourceid": RESOURCE,
                "value": [{
                    "name": {"value": "Transactions"}, "unit": "Count", "errorCode": "Success",
                    "timeseries": [{
                        "metadatavalues": [
                            {"name": {"value": "ApiName"}, "value": "GetBlob"},
                            {"name": {"value": "ResponseType"}, "value": "Success"},
                        ],
                        "data": [{"timeStamp": TIME, "total": 12}],
                    }],
                }],
            }],
        }
        generated = normalize_batch(batch, "total", {"start": TIME, "end": TIME})
        self.assertEqual(samples(dcr)[-1], samples(generated[0])[0])
        self.assertEqual(len(parse_event(json.dumps(generated))), 1)
        store = Store(ttl_seconds=900)
        now = datetime.fromisoformat(TIME.replace("Z", "+00:00")) + timedelta(minutes=2)
        store.accept([dcr, *generated], now=now)
        output = store.render(now)
        self.assertIn('dimension_apiname="GetBlob"', output)
        self.assertIn('aggregation="total"', output)
        self.assertIn("azure_platform_metric{", output)
        self.assertEqual(output.count("azure_platform_metric{"), 2)

    def test_age_dedup_and_cardinality(self):
        item = {
            "resourceId": RESOURCE, "time": TIME, "unit": "Count",
            "metricName": "Transactions", "timeGrain": "PT1M",
            "dimension": {"ApiName": "GetBlob"}, "Total": 3,
        }
        now = datetime.fromisoformat(TIME.replace("Z", "+00:00")) + timedelta(seconds=30)
        store = Store(ttl_seconds=120, max_series=1)
        store.accept([item], now)
        older = {**item, "time": "2026-11-25T02:59:00Z", "Total": 99}
        store.accept([older], now)
        self.assertIn(" 3\n", store.render(now))
        with self.assertRaisesRegex(ValueError, "limit"):
            store.accept([{**item, "dimension": {"ApiName": "PutBlob"}}], now)
        self.assertEqual(len(store.series), 1)
        self.assertNotIn("azure_platform_metric{", store.render(now + timedelta(minutes=3)))

    def test_reject_bad_inputs(self):
        item = {
            "resourceId": RESOURCE, "time": TIME, "unit": "Count",
            "metricName": "Transactions", "timeGrain": "PT1M",
            "dimension": {"A-B": "1", "A_B": "2"}, "Total": 1,
        }
        with self.assertRaisesRegex(ValueError, "collision"):
            samples(item)
        with self.assertRaises(ValueError):
            parse_event('"invalid"')
        with self.assertRaises(ValueError):
            parse_event('{"Total": 1}{"Total": 2}')
        with self.assertRaises(ValueError):
            parse_event('{"Total": 1}\nnot-json')
        with self.assertRaises(ValueError):
            parse_event(" \n ")
        with self.assertRaises(ValueError):
            samples({**item, "dimension": {}, "Total": "NaN"})
        with self.assertRaises(ValueError):
            normalize_batch({"values": [{"resourceid": RESOURCE, "value": [
                {"errorCode": "BadRequest"}]}]}, "total", {})

    def test_newline_delimited_dcr_records_checkpoint_as_one_event(self):
        first = {
            "resourceId": RESOURCE, "time": TIME, "unit": "Count",
            "metricName": "Transactions", "timeGrain": "PT1M",
            "dimension": {"ApiName": "GetBlob"}, "Total": 2,
        }
        second = {**first, "dimension": {"ApiName": "PutBlob"}, "Total": 3}
        payload = json.dumps(first) + "\r\n" + json.dumps(second) + "\n"
        self.assertEqual(parse_event(payload), [first, second])

        class Context:
            updates = 0

            def update_checkpoint(self, event):
                self.updates += 1

        class Event:
            def body_as_str(self):
                return payload

        store = Store()
        context = Context()
        process_event(store, context, Event())
        self.assertEqual(context.updates, 1)
        self.assertIsNone(store.error)

    def test_checkpoint_only_on_success(self):
        class Context:
            updates = 0

            def update_checkpoint(self, event):
                self.updates += 1

        class Event:
            def __init__(self, value):
                self.value = value

            def body_as_str(self):
                return self.value

        context = Context()
        store = Store()
        with self.assertRaises(ValueError):
            process_event(store, context, Event('{"invalid":true}'))
        self.assertEqual(context.updates, 0)
        self.assertIsNotNone(store.error)
        with self.assertRaises(RuntimeError):
            process_event(store, context, Event("{}"))

    def test_http_metrics_and_expiry(self):
        store = Store(ttl_seconds=60)
        now = datetime.now(timezone.utc)
        store.accept([{
            "resourceId": RESOURCE, "time": now.isoformat(), "unit": "Count",
            "metricName": "Transactions", "timeGrain": "PT1M",
            "dimension": {"ApiName": "GetBlob"}, "Total": 3,
        }], now)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler(store))
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            root = f"http://127.0.0.1:{server.server_port}"
            with urlopen(root + "/metrics") as response:
                body = response.read().decode()
                self.assertEqual(response.headers["Content-Type"], "text/plain; version=0.0.4; charset=utf-8")
                self.assertIn('dimension_apiname="GetBlob"', body)
            with urlopen(root + "/health") as response:
                self.assertEqual(response.status, 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
