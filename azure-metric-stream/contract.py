"""DCR Event Hubs metric records and Metrics Batch response normalization."""

import json
import math
import re
from datetime import datetime, timezone

AGGREGATIONS = ("Average", "Count", "Maximum", "Minimum", "Total")
SCHEMA_VERSION = 1


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("metric time must be an ISO 8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("metric time must include a timezone")
    return parsed.astimezone(timezone.utc)


def normalize_batch(response, aggregation, query_window):
    """Translate Batch values/resourceid/value/timeseries/data to DCR wire records."""
    aggregate = aggregation.capitalize()
    if aggregate not in AGGREGATIONS:
        raise ValueError(f"unsupported aggregation: {aggregation}")
    records = []
    for resource in response["values"]:
        resource_id = resource["resourceid"]
        for metric in resource["value"]:
            error = metric.get("errorCode", "Success")
            if error.lower() != "success":
                raise ValueError(f"Batch metric failed: {error}")
            name = metric["name"]["value"]
            for series in metric["timeseries"]:
                dimensions = {}
                for entry in series.get("metadatavalues", []):
                    key, val = entry["name"]["value"], entry["value"]
                    if key in dimensions:
                        raise ValueError(f"duplicate dimension: {key}")
                    dimensions[key] = val
                for point in series["data"]:
                    value = point.get(aggregate.lower())
                    if value is None:
                        continue
                    records.append({
                        "resourceId": resource_id,
                        "metricName": name,
                        "unit": metric["unit"],
                        "time": point["timeStamp"],
                        "timeGrain": "PT1M",
                        "dimension": dimensions.copy(),
                        aggregate: value,
                        "schemaVersion": SCHEMA_VERSION,
                        "queryWindow": query_window,
                    })
    return records


def parse_event(payload):
    """A DCR event is an object; some Event Hubs producers send an array."""
    event = json.loads(payload)
    if isinstance(event, dict):
        return [event]
    if isinstance(event, list) and all(isinstance(item, dict) for item in event):
        return event
    raise ValueError("Event Hubs body must contain metric objects")


def samples(record):
    """Produce gauge samples, preserving each aggregation and every dimension."""
    if record.get("schemaVersion", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise ValueError("unsupported event schema version")
    required = ("resourceId", "metricName", "unit", "timeGrain", "time")
    missing = [key for key in required if key not in record]
    if missing:
        raise ValueError(f"missing metric fields: {', '.join(missing)}")
    dimensions = record.get("dimension", {})
    if not isinstance(dimensions, dict):
        raise ValueError("dimension must be an object")
    labels = {
        "resource_id": record["resourceId"],
        "metric_name": record["metricName"],
        "unit": record["unit"],
        "time_grain": record["timeGrain"],
    }
    if not all(isinstance(v, str) and v for v in labels.values()):
        raise ValueError("metric metadata must be nonempty strings")
    for name, value in dimensions.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError("dimensions must be string name/value pairs")
        key = "dimension_" + re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()
        if key in labels:
            raise ValueError(f"dimension label collision: {name}")
        labels[key] = value
    observed = timestamp(record["time"])
    result = []
    for aggregate in AGGREGATIONS:
        if aggregate not in record or record[aggregate] is None:
            continue
        value = float(record[aggregate])
        if not math.isfinite(value):
            raise ValueError("nonfinite metric value")
        result.append((tuple(sorted({**labels, "aggregation": aggregate.lower()}.items())), observed, value))
    if not result:
        raise ValueError("metric record has no aggregation")
    return result
