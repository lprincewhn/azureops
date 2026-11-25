import io
import json
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from batch_collector import (
    MetricsBatchClient,
    SCOPE,
    collect_once,
    guard_batch_response,
    load_config,
    plan_batches,
    publish_records,
    read_watermark,
    request_url,
    retry_delay,
    validate_config,
)

SUB = "00000000-0000-0000-0000-000000000001"


def resource(index, kind="loadBalancers", subscription=SUB):
    return f"/subscriptions/{subscription}/resourceGroups/example/providers/Microsoft.Network/{kind}/resource{index}"


def config(path):
    return validate_config({
        "resources": [{"id": resource(1), "region": "eastus"}],
        "metricSpecs": [{
            "resourceType": "Microsoft.Network/loadBalancers",
            "metricNames": ["DipAvailability"],
            "aggregation": "Average",
            "dimensions": ["BackendIPAddress"],
            "filter": "BackendIPAddress eq '*'",
            "top": 3,
        }],
        "eventHub": {"fullyQualifiedNamespace": "example.servicebus.windows.net", "name": "metrics"},
        "statePath": str(path),
    })


def batch_response(ids, series=1):
    return {"values": [
        {
            "resourceid": resource_id,
            "value": [{
                "name": {"value": "DipAvailability"}, "unit": "Percent", "errorCode": "Success",
                "timeseries": [{
                    "metadatavalues": [{"name": {"value": "BackendIPAddress"}, "value": f"10.0.0.{i}"}],
                    "data": [{"timeStamp": "2026-11-25T03:45:00Z", "average": 99}],
                } for i in range(series)],
            }],
        } for resource_id in ids
    ]}


class EventData:
    def __init__(self, value):
        self.value = value


class FakeBatch:
    def __init__(self, capacity):
        self.events = []
        self.capacity = capacity

    def add(self, event):
        if len(self.events) >= self.capacity:
            raise ValueError("full")
        self.events.append(event)


class FakeProducer:
    def __init__(self, capacity=10):
        self.capacity = capacity
        self.sent = []
        self.fail = False

    def create_batch(self):
        return FakeBatch(self.capacity)

    def send_batch(self, batch):
        if self.fail:
            raise RuntimeError("Event Hubs unavailable")
        self.sent.extend(json.loads(event.value) for event in batch.events)


def normalize(response, aggregation, window):
    for item in response["values"]:
        for metric in item["value"]:
            for series in metric["timeseries"]:
                for point in series["data"]:
                    yield {
                        "schemaVersion": 1,
                        "resourceId": item["resourceid"],
                        "metricName": metric["name"]["value"],
                        "unit": metric["unit"],
                        "time": point["timeStamp"],
                        "timeGrain": "PT1M",
                        "dimension": {entry["name"]["value"]: entry["value"] for entry in series["metadatavalues"]},
                        aggregation.capitalize(): point[aggregation.lower()],
                        "queryWindow": window,
                    }


class ConfigTests(unittest.TestCase):
    def test_partition_by_subscription_region_type_and_50(self):
        cfg = config("state.json")
        cfg["resources"] = [
            {"id": resource(i), "region": "eastus" if i < 53 else "westus"}
            for i in range(54)
        ] + [
            {"id": resource(70, "applicationGateways"), "region": "eastus"},
            {"id": resource(71, subscription="00000000-0000-0000-0000-000000000002"), "region": "eastus"},
        ]
        batches = list(plan_batches(cfg))
        self.assertEqual([len(ids) for _, _, _, ids in batches], [50, 3, 1, 1])
        self.assertEqual([region for _, _, region, _ in batches], ["eastus", "eastus", "westus", "eastus"])
        self.assertEqual(len({subscription for _, subscription, _, _ in batches}), 2)
        self.assertEqual(sum(len(ids) for _, _, _, ids in batches), 55)

    def test_filter_top_and_api_url(self):
        cfg = config("state.json")
        window = {"startTime": "2026-11-25T03:40:00.000Z", "endTime": "2026-11-25T03:50:00.000Z"}
        url = request_url(cfg["metricSpecs"][0], SUB, "eastus", window)
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(url)
        self.assertEqual(parsed.hostname, "eastus.metrics.monitor.azure.com")
        self.assertEqual(parsed.path, f"/subscriptions/{SUB}/metrics:getBatch")
        args = parse_qs(parsed.query)
        self.assertEqual(args["api-version"], ["2023-10-01"])
        self.assertEqual(args["filter"], ["BackendIPAddress eq '*'"])
        self.assertEqual(args["top"], ["3"])
        self.assertEqual(args["endtime"], [window["endTime"]])

    def test_explicit_wildcard_required(self):
        cfg = config("state.json")
        cfg["metricSpecs"][0]["filter"] = "BackendIPAddress eq '10.0.0.1'"
        with self.assertRaisesRegex(ValueError, "wildcard"):
            validate_config(cfg)

    def test_no_default_top_without_filter(self):
        cfg = config("state.json")
        spec = cfg["metricSpecs"][0]
        spec["dimensions"] = []
        spec.pop("filter")
        validate_config(cfg)
        self.assertNotIn("top", spec)

    def test_env_config_validation(self):
        cfg = config("state.json")
        self.assertEqual(load_config({"BATCH_CONFIG_JSON": json.dumps(cfg)})["eventHub"]["name"], "metrics")
        with self.assertRaises(ValueError):
            load_config({"BATCH_CONFIG_JSON": json.dumps(cfg), "BATCH_CONFIG_PATH": "other.json"})

    def test_multi_aggregation_and_other_grain_rejected_until_contract_supports_them(self):
        cfg = config("state.json")
        cfg["metricSpecs"][0]["aggregation"] = "Average,Total"
        with self.assertRaisesRegex(ValueError, "one supported aggregation"):
            validate_config(cfg)
        cfg["metricSpecs"][0]["aggregation"] = "Average"
        cfg["metricSpecs"][0]["interval"] = "PT5M"
        with self.assertRaisesRegex(ValueError, "PT1M"):
            validate_config(cfg)


class ClientTests(unittest.TestCase):
    def test_retries_429_with_retry_after_and_refreshes_token(self):
        credential = Mock()
        credential.get_token.side_effect = [SimpleNamespace(token="first"), SimpleNamespace(token="second")]
        response = io.BytesIO(json.dumps(batch_response([resource(1)])).encode())
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        calls = []

        def opener(request, timeout):
            calls.append((request, timeout))
            if len(calls) == 1:
                raise urllib.error.HTTPError(request.full_url, 429, "busy", {"Retry-After": "4"}, None)
            return response

        sleep = Mock()
        result = MetricsBatchClient(credential, opener=opener, sleep=sleep, attempts=2).query(
            "https://eastus.metrics.monitor.azure.com/x", [resource(1)],
        )
        self.assertIn("values", result)
        self.assertEqual(sleep.call_args.args, (4.0,))
        self.assertEqual([request.get_header("Authorization") for request, _ in calls], ["Bearer first", "Bearer second"])
        self.assertEqual(json.loads(calls[0][0].data), {"resourceids": [resource(1)]})
        credential.get_token.assert_any_call(SCOPE)

    def test_503_retries_and_400_does_not(self):
        credential = Mock(get_token=Mock(return_value=SimpleNamespace(token="token")))
        sleep = Mock()
        errors = [urllib.error.HTTPError("url", status, "failure", {}, None) for status in (503, 400)]
        opener = Mock(side_effect=errors)
        with self.assertRaises(urllib.error.HTTPError) as raised:
            MetricsBatchClient(credential, opener=opener, sleep=sleep).query("https://example", [resource(1)])
        self.assertEqual(raised.exception.code, 400)
        sleep.assert_called_once_with(1)

    def test_retry_after_http_date(self):
        now = datetime(2026, 11, 25, tzinfo=timezone.utc)
        self.assertEqual(retry_delay("Wed, 25 Nov 2026 00:00:07 GMT", 2, lambda: now), 7)
        self.assertEqual(retry_delay(None, 3), 4)


class CollectorTests(unittest.TestCase):
    def test_cap_fails_closed_even_when_exactly_at_top(self):
        cfg = config("state.json")
        with self.assertRaisesRegex(ValueError, "possible dimension truncation"):
            guard_batch_response(batch_response([resource(1)], series=3), [resource(1)], cfg["metricSpecs"][0])

    def test_missing_metric_or_resource_fails_closed(self):
        cfg = config("state.json")
        with self.assertRaisesRegex(ValueError, "omitted a requested resource"):
            guard_batch_response(batch_response([]), [resource(1)], cfg["metricSpecs"][0])
        bad = batch_response([resource(1)])
        bad["values"][0]["value"][0]["errorCode"] = "BadRequest"
        with self.assertRaisesRegex(ValueError, "BadRequest"):
            guard_batch_response(bad, [resource(1)], cfg["metricSpecs"][0])

    def test_send_failure_keeps_watermark_then_recovers_with_overlap(self):
        with tempfile.TemporaryDirectory(dir=".") as scratch:
            path = Path(scratch) / "state.json"
            cfg = config(path)
            client = Mock(query=Mock(return_value=batch_response([resource(1)])))
            producer = FakeProducer()
            now = datetime(2026, 11, 25, 4, 0, tzinfo=timezone.utc)
            producer.fail = True
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                collect_once(cfg, client, producer, normalize, EventData, now=now)
            self.assertIsNone(read_watermark(path))
            producer.fail = False
            self.assertEqual(collect_once(cfg, client, producer, normalize, EventData, now=now), 1)
            first = client.query.call_args.args[0]
            self.assertEqual(read_watermark(path), now - timedelta(seconds=120))
            self.assertEqual(collect_once(cfg, client, producer, normalize, EventData, now=now), 0)
            self.assertEqual(collect_once(cfg, client, producer, normalize, EventData, now=now + timedelta(minutes=1)), 1)
            second = client.query.call_args.args[0]
            from urllib.parse import parse_qs, urlparse
            self.assertEqual(parse_qs(urlparse(first).query)["starttime"], ["2026-11-25T03:48:00.000Z"])
            self.assertEqual(parse_qs(urlparse(second).query)["starttime"], ["2026-11-25T03:55:00.000Z"])
            self.assertEqual(len(producer.sent), 2)
            self.assertEqual(producer.sent[0]["dimension"], {"BackendIPAddress": "10.0.0.0"})
            self.assertEqual(producer.sent[0]["queryWindow"]["endTime"], "2026-11-25T03:58:00.000Z")

    def test_top_saturation_never_sends_or_advances(self):
        with tempfile.TemporaryDirectory(dir=".") as scratch:
            cfg = config(Path(scratch) / "state.json")
            client = Mock(query=Mock(return_value=batch_response([resource(1)], series=3)))
            producer = FakeProducer()
            with self.assertRaisesRegex(ValueError, "top=3"):
                collect_once(cfg, client, producer, normalize, EventData,
                             now=datetime(2026, 11, 25, 4, 0, tzinfo=timezone.utc))
            self.assertIsNone(read_watermark(Path(cfg["statePath"])))
            self.assertFalse(producer.sent)

    def test_shared_contract_normalizer_writes_event(self):
        from contract import normalize_batch
        with tempfile.TemporaryDirectory(dir=".") as scratch:
            cfg = config(Path(scratch) / "state.json")
            producer = FakeProducer()
            client = Mock(query=Mock(return_value=batch_response([resource(1)])))
            sent = collect_once(cfg, client, producer, normalize_batch, EventData,
                                now=datetime(2026, 11, 25, 4, 0, tzinfo=timezone.utc))
            self.assertEqual(sent, 1)
            self.assertEqual(producer.sent[0]["resourceId"], resource(1))
            self.assertEqual(producer.sent[0]["metricName"], "DipAvailability")
            self.assertEqual(producer.sent[0]["time"], "2026-11-25T03:45:00Z")
            self.assertEqual(producer.sent[0]["timeGrain"], "PT1M")
            self.assertEqual(producer.sent[0]["dimension"], {"BackendIPAddress": "10.0.0.0"})
            self.assertEqual(producer.sent[0]["Average"], 99)
            self.assertNotIn("aggregation", producer.sent[0])

    def test_batch_overflow_preserves_records_and_too_large_fails(self):
        producer = FakeProducer(capacity=1)
        self.assertEqual(publish_records(producer, [{"a": 1}, {"a": 2}], EventData), 2)
        self.assertEqual(producer.sent, [{"a": 1}, {"a": 2}])
        with self.assertRaisesRegex(ValueError, "maximum batch size"):
            publish_records(FakeProducer(capacity=0), [{"a": 1}], EventData)

    def test_corrupt_state_fails_closed(self):
        with tempfile.TemporaryDirectory(dir=".") as scratch:
            state = Path(scratch) / "state.json"
            state.write_text("invalid", encoding="utf-8")
            with self.assertRaises(json.JSONDecodeError):
                read_watermark(state)

    def test_long_downtime_backfills_bounded_windows_and_config_changes_fail_closed(self):
        with tempfile.TemporaryDirectory(dir=".") as scratch:
            cfg = config(Path(scratch) / "state.json")
            cfg["maxWindowSeconds"] = 600
            client = Mock(query=Mock(return_value=batch_response([resource(1)])))
            producer = FakeProducer()
            initial_now = datetime(2026, 11, 25, 4, 0, tzinfo=timezone.utc)
            collect_once(cfg, client, producer, normalize, EventData, now=initial_now)
            collect_once(cfg, client, producer, normalize, EventData, now=initial_now + timedelta(hours=5))
            self.assertEqual(read_watermark(Path(cfg["statePath"])), datetime(2026, 11, 25, 4, 5, tzinfo=timezone.utc))
            from urllib.parse import parse_qs, urlparse
            query = parse_qs(urlparse(client.query.call_args.args[0]).query)
            self.assertEqual(query["starttime"], ["2026-11-25T03:55:00.000Z"])
            self.assertEqual(query["endtime"], ["2026-11-25T04:05:00.000Z"])
            cfg["metricSpecs"][0]["metricNames"] = ["AnotherMetric"]
            with self.assertRaisesRegex(ValueError, "configuration changed"):
                collect_once(cfg, client, producer, normalize, EventData,
                             now=initial_now + timedelta(hours=5))


if __name__ == "__main__":
    unittest.main()
