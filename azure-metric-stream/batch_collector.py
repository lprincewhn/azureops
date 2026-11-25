"""Poll regional Azure Metrics Batch endpoints and publish normalized Event Hubs records.

Set BATCH_CONFIG_JSON or BATCH_CONFIG_PATH to a JSON object with resources,
metricSpecs, eventHub, and either statePath or stateBlob. For example, each resource is
{"id": "/subscriptions/.../resourceGroups/.../providers/Microsoft.Network/loadBalancers/...",
 "region": "eastus"}; a metric spec is
{"resourceType": "Microsoft.Network/loadBalancers",
 "metricNames": ["DipAvailability"], "aggregation": "Average",
 "dimensions": ["BackendIPAddress"], "filter": "BackendIPAddress eq '*'",
 "top": 100}. The eventHub object has fullyQualifiedNamespace and name.
The statePath must reside on a persistent, single-writer volume; stateBlob uses
Azure Blob Storage with a renewable, cycle-scoped lease instead.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable

LOG = logging.getLogger("batch_collector")
SCOPE = "https://metrics.monitor.azure.com/.default"
RESOURCE_PATTERN = re.compile(
    r"^/subscriptions/([^/]+)/resourceGroups/[^/]+/providers/([^/]+/[^/]+)(?:/[^/]+)+$",
    re.IGNORECASE,
)
AGGREGATIONS = {"average", "minimum", "maximum", "total", "count"}
SPEC_KEYS = {
    "resourceType", "metricNamespace", "metricNames", "aggregation",
    "interval", "dimensions", "filter", "top", "resourceIds",
}


def utc_stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def load_config(env: dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ if env is None else env
    inline, path = env.get("BATCH_CONFIG_JSON"), env.get("BATCH_CONFIG_PATH")
    if bool(inline) == bool(path):
        raise ValueError("Set exactly one of BATCH_CONFIG_JSON and BATCH_CONFIG_PATH")
    config = json.loads(inline if inline else Path(path).read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("Batch configuration must be a JSON object")
    return validate_config(config)


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    resources = config.get("resources")
    specs = config.get("metricSpecs")
    hub = config.get("eventHub")
    state_path = config.get("statePath")
    if not isinstance(resources, list) or not resources:
        raise ValueError("resources must be a nonempty array")
    if not isinstance(specs, list) or not specs:
        raise ValueError("metricSpecs must be a nonempty array")
    state_blob = config.get("stateBlob")
    if not isinstance(hub, dict):
        raise ValueError("eventHub is required")
    if (isinstance(state_path, str) and bool(state_path)) == (state_blob is not None):
        raise ValueError("Configure exactly one of statePath and stateBlob")
    if state_blob is not None:
        if not isinstance(state_blob, dict) or set(state_blob) != {"accountName", "containerName", "blobName"}:
            raise ValueError("stateBlob requires accountName, containerName and blobName")
        if not isinstance(state_blob["accountName"], str) or not re.fullmatch(r"[a-z0-9]{3,24}", state_blob["accountName"]):
            raise ValueError("stateBlob.accountName must be an Azure Storage account name")
        if not isinstance(state_blob["containerName"], str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?", state_blob["containerName"]):
            raise ValueError("stateBlob.containerName must be a valid container name")
        if not isinstance(state_blob["blobName"], str) or not state_blob["blobName"] or state_blob["blobName"].startswith("/"):
            raise ValueError("stateBlob.blobName must be a nonempty relative blob name")
    elif not isinstance(state_path, str) or not state_path:
        raise ValueError("statePath must be a nonempty path")
    if not re.fullmatch(r"[a-zA-Z0-9.-]+\.servicebus\.windows\.net", str(hub.get("fullyQualifiedNamespace", ""))):
        raise ValueError("eventHub.fullyQualifiedNamespace must be an Azure Event Hubs namespace")
    if not isinstance(hub.get("name"), str) or not hub["name"]:
        raise ValueError("eventHub.name is required")
    for key, default in (
        ("pollSeconds", 60), ("initialLookbackSeconds", 600),
        ("overlapSeconds", 180), ("ingestLagSeconds", 120),
        ("maxWindowSeconds", 3600), ("httpTimeoutSeconds", 30), ("httpMaxAttempts", 5),
    ):
        value = config.setdefault(key, default)
        if type(value) is not int or value < (0 if key == "ingestLagSeconds" else 1):
            raise ValueError(f"{key} must be a positive integer (ingestLagSeconds may be zero)")
    if config["initialLookbackSeconds"] <= config["overlapSeconds"]:
        raise ValueError("initialLookbackSeconds must exceed overlapSeconds")
    if config["maxWindowSeconds"] <= config["overlapSeconds"]:
        raise ValueError("maxWindowSeconds must exceed overlapSeconds")
    seen = set()
    for resource in resources:
        if not isinstance(resource, dict) or not isinstance(resource.get("id"), str):
            raise ValueError("Each resource requires an ARM id and region")
        match = RESOURCE_PATTERN.fullmatch(resource["id"])
        if not match:
            raise ValueError(f"Invalid ARM resource id: {resource['id']}")
        if not re.fullmatch(r"[a-z0-9-]+", str(resource.get("region", ""))):
            raise ValueError("Each resource requires a valid Azure region")
        identity = resource["id"].lower()
        if identity in seen:
            raise ValueError("Duplicate resource id")
        seen.add(identity)
    types = {RESOURCE_PATTERN.fullmatch(r["id"]).group(2).lower() for r in resources}
    for spec in specs:
        if not isinstance(spec, dict) or set(spec) - SPEC_KEYS:
            raise ValueError("Invalid metric spec or unknown keys (rollupby is not supported)")
        resource_type = spec.get("resourceType")
        if not isinstance(resource_type, str) or resource_type.lower() not in types:
            raise ValueError("Each metric spec must match a configured resourceType")
        if "resourceIds" in spec:
            ids = spec["resourceIds"]
            matching = {
                resource["id"].lower() for resource in resources
                if RESOURCE_PATTERN.fullmatch(resource["id"]).group(2).lower() == resource_type.lower()
            }
            if (not isinstance(ids, list) or not ids
                    or any(not isinstance(item, str) or item.lower() not in matching for item in ids)
                    or len({item.lower() for item in ids}) != len(ids)):
                raise ValueError("resourceIds must be distinct configured resources of resourceType")
        namespace = spec.setdefault("metricNamespace", resource_type)
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("metricNamespace must be a nonempty string")
        names = spec.get("metricNames")
        if not isinstance(names, list) or not names or any(not isinstance(n, str) or not n for n in names):
            raise ValueError("metricNames must be a nonempty string array")
        if len(set(names)) != len(names):
            raise ValueError("Duplicate metric names in spec")
        aggregations = spec.get("aggregation")
        if not isinstance(aggregations, str) or aggregations.lower() not in AGGREGATIONS:
            raise ValueError("aggregation must be one supported aggregation per metric spec")
        spec.setdefault("interval", "PT1M")
        if spec["interval"] != "PT1M":
            raise ValueError("Only PT1M is supported by the shared normalizer timeGrain")
        dims = spec.setdefault("dimensions", [])
        if not isinstance(dims, list) or any(not isinstance(d, str) or not re.fullmatch(r"[\w.]+", d) for d in dims):
            raise ValueError("dimensions must be an array of dimension names")
        filter_text = spec.get("filter", "")
        if not isinstance(filter_text, str):
            raise ValueError("filter must be a string")
        for dim in dims:
            if not re.search(rf"\b{re.escape(dim)}\s+eq\s+(['\"])\*\1", filter_text, re.IGNORECASE):
                raise ValueError(f"Dimension {dim} needs an explicit wildcard filter")
        top = spec.setdefault("top", 100)
        if type(top) is not int or not 1 <= top <= 10000:
            raise ValueError("top must be an integer between 1 and 10000")
        if not filter_text and not dims:
            # The Batch API accepts top only when a filter is provided.
            spec.pop("top")
        if dims and not filter_text:
            raise ValueError("A dimension split requires a wildcard filter")
    return config


def plan_batches(config: dict[str, Any]) -> Iterable[tuple[dict[str, Any], str, str, list[str]]]:
    """Partition by spec, subscription, region and resource type; never exceed 50 IDs."""
    for spec in config["metricSpecs"]:
        groups: dict[tuple[str, str, str], list[str]] = {}
        selected = {item.lower() for item in spec["resourceIds"]} if "resourceIds" in spec else None
        for resource in config["resources"]:
            subscription, resource_type = RESOURCE_PATTERN.fullmatch(resource["id"]).groups()
            if resource_type.lower() != spec["resourceType"].lower() or (
                selected is not None and resource["id"].lower() not in selected
            ):
                continue
            groups.setdefault((subscription.lower(), resource["region"].lower(), resource_type.lower()), []).append(resource["id"])
        for (subscription, region, _), ids in groups.items():
            for offset in range(0, len(ids), 50):
                yield spec, subscription, region, ids[offset:offset + 50]


def request_url(spec: dict[str, Any], subscription: str, region: str, window: dict[str, str]) -> str:
    query = {
        "api-version": "2023-10-01",
        "metricnamespace": spec["metricNamespace"],
        "metricnames": ",".join(spec["metricNames"]),
        "aggregation": spec["aggregation"],
        "interval": spec["interval"],
        "starttime": window["startTime"],
        "endtime": window["endTime"],
    }
    if spec.get("filter"):
        query.update(filter=spec["filter"], top=spec["top"])
    return (
        f"https://{region}.metrics.monitor.azure.com/subscriptions/"
        f"{urllib.parse.quote(subscription, safe='')}/metrics:getBatch?"
        + urllib.parse.urlencode(query)
    )


def retry_delay(header: str | None, attempt: int, now: Callable[[], datetime] | None = None) -> float:
    if header:
        try:
            return max(0.0, float(header))
        except ValueError:
            try:
                date = parsedate_to_datetime(header)
                current = now() if now else datetime.now(timezone.utc)
                return max(0.0, (date - current).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return min(60.0, 2 ** (attempt - 1))


class MetricsBatchClient:
    def __init__(
        self, credential: Any, *, opener: Callable = urllib.request.urlopen,
        sleep: Callable[[float], None] = time.sleep, timeout: int = 30, attempts: int = 5,
    ) -> None:
        self.credential = credential
        self.opener = opener
        self.sleep = sleep
        self.timeout = timeout
        self.attempts = attempts

    def query(self, url: str, resource_ids: list[str]) -> dict[str, Any]:
        body = json.dumps({"resourceids": resource_ids}).encode("utf-8")
        for attempt in range(1, self.attempts + 1):
            token = self.credential.get_token(SCOPE).token
            request = urllib.request.Request(
                url, body, {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                method="POST",
            )
            try:
                with self.opener(request, timeout=self.timeout) as response:
                    return json.load(response)
            except urllib.error.HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504) or attempt == self.attempts:
                    raise
                delay = retry_delay(error.headers.get("Retry-After"), attempt)
                LOG.warning("Metrics Batch HTTP %s; retrying in %.1fs", error.code, delay)
            except (urllib.error.URLError, TimeoutError) as error:
                if attempt == self.attempts:
                    raise
                delay = retry_delay(None, attempt)
                LOG.warning("Metrics Batch transport failure %s; retrying in %.1fs", type(error).__name__, delay)
            self.sleep(delay)
        raise AssertionError("unreachable")


def guard_batch_response(response: dict[str, Any], resource_ids: list[str], spec: dict[str, Any]) -> None:
    """Reject partial, erroneous, and possibly truncated responses before publishing."""
    if not isinstance(response, dict) or not isinstance(response.get("values"), list):
        raise ValueError("Metrics Batch response is missing values")
    requested = {resource_id.lower() for resource_id in resource_ids}
    seen: set[str] = set()
    requested_metrics = {name.lower() for name in spec["metricNames"]}
    for item in response["values"]:
        resource_id = item.get("resourceid", "").lower()
        if resource_id not in requested or resource_id in seen:
            raise ValueError("Unexpected or duplicate resource in Metrics Batch response")
        seen.add(resource_id)
        metrics = item.get("value")
        if not isinstance(metrics, list):
            raise ValueError("Metrics Batch resource has no metric values")
        seen_metrics: set[str] = set()
        for metric in metrics:
            name = metric.get("name", {}).get("value", "").lower()
            if name not in requested_metrics or name in seen_metrics:
                raise ValueError("Unexpected or duplicate metric in Metrics Batch response")
            seen_metrics.add(name)
            if metric.get("errorCode", "Success").lower() != "success":
                raise ValueError(f"Metric {name} returned error: {metric.get('errorCode')}")
            series = metric.get("timeseries")
            if not isinstance(series, list):
                raise ValueError("Metric timeseries missing")
            if spec.get("filter") and len(series) >= spec["top"]:
                raise ValueError(f"Metric {name} reached top={spec['top']}; possible dimension truncation")
        if seen_metrics != requested_metrics:
            raise ValueError("Metrics Batch response omitted a requested metric")
    if seen != requested:
        raise ValueError("Metrics Batch response omitted a requested resource")


def publish_records(producer: Any, records: Iterable[dict[str, Any]], event_data_class: type) -> int:
    batch = producer.create_batch()
    count = 0
    pending = 0
    for record in records:
        event = event_data_class(json.dumps(record, separators=(",", ":"), allow_nan=False))
        try:
            batch.add(event)
        except ValueError:
            if not pending:
                raise ValueError("Normalized record exceeds Event Hubs maximum batch size")
            producer.send_batch(batch)
            batch = producer.create_batch()
            batch.add(event)
            pending = 0
        pending += 1
        count += 1
    if pending:
        producer.send_batch(batch)
    return count


def config_fingerprint(config: dict[str, Any]) -> str:
    relevant = {"resources": config["resources"], "metricSpecs": config["metricSpecs"]}
    return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode("utf-8")).hexdigest()


def read_watermark(path: Path, fingerprint: str | None = None) -> datetime | None:
    try:
        payload = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    return parse_watermark(payload, fingerprint)


def write_watermark(path: Path, watermark: datetime, fingerprint: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(path.name + ".pending")
    try:
        with staging.open("w", encoding="utf-8") as file:
            json.dump({"endTime": utc_stamp(watermark), "configHash": fingerprint}, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(staging, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        staging.unlink(missing_ok=True)


@contextmanager
def single_writer(path: Path) -> Iterable[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class BlobState:
    """Lease the checkpoint through each publish cycle; condition writes on its ETag."""

    def __init__(self, config: dict[str, str], credential: Any) -> None:
        from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
        from azure.storage.blob import BlobLeaseClient, BlobServiceClient

        service = BlobServiceClient(
            f"https://{config['accountName']}.blob.core.windows.net", credential=credential,
        )
        self.blob = service.get_blob_client(config["containerName"], config["blobName"])
        self.lease_factory = BlobLeaseClient
        try:
            self.blob.get_blob_properties()
        except ResourceNotFoundError:
            try:
                self.blob.upload_blob(b"", overwrite=False, if_none_match="*")
            except ResourceExistsError:
                pass
            self.blob.get_blob_properties()

    @contextmanager
    def cycle(self) -> Iterable[None]:
        lease = self.lease_factory(self.blob)
        lease.acquire(lease_duration=60)
        stopped = threading.Event()
        failures: list[Exception] = []

        def renew() -> None:
            while not stopped.wait(20):
                try:
                    lease.renew()
                except Exception as error:
                    failures.append(error)
                    LOG.exception("Blob checkpoint lease renewal failed")
                    return

        worker = threading.Thread(target=renew, name="checkpoint-lease-renewal", daemon=True)
        self._failures = failures
        self._lease = lease
        try:
            worker.start()
            yield
            self.check_lease()
        finally:
            stopped.set()
            if worker.ident is not None:
                worker.join()
            active_error = sys.exc_info()[0]
            try:
                lease.release()
            except Exception:
                if active_error is not None:
                    LOG.exception("Blob checkpoint lease release failed")
                else:
                    raise
            finally:
                self._lease = None

    def check_lease(self) -> None:
        if self._failures:
            raise RuntimeError("Blob checkpoint lease renewal failed") from self._failures[0]

    def read(self, fingerprint: str) -> datetime | None:
        self.check_lease()
        props = self.blob.get_blob_properties(lease=self._lease)
        self.etag = props.etag
        payload = self.blob.download_blob(lease=self._lease, if_match=self.etag).readall()
        self.check_lease()
        if not payload:
            return None
        return parse_watermark(payload, fingerprint)

    def write(self, watermark: datetime, fingerprint: str) -> None:
        self.check_lease()
        self.blob.upload_blob(
            json.dumps({"endTime": utc_stamp(watermark), "configHash": fingerprint}).encode("utf-8"),
            overwrite=True, if_match=self.etag, lease=self._lease,
        )
        self.check_lease()


def parse_watermark(payload: str | bytes, fingerprint: str | None = None) -> datetime:
    data = json.loads(payload)
    if fingerprint is not None and data.get("configHash") != fingerprint:
        raise ValueError("Resource or metric configuration changed; migrate/reset the watermark explicitly")
    stamp = datetime.fromisoformat(data["endTime"].replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("State watermark must be timezone aware")
    return stamp.astimezone(timezone.utc)


def collect_once(
    config: dict[str, Any], client: MetricsBatchClient, producer: Any,
    normalizer: Callable[[dict[str, Any], str, dict[str, str]], Iterable[dict[str, Any]]],
    event_data_class: type, *, now: datetime | None = None, state: BlobState | None = None,
) -> int:
    """Advance the durable high-water mark only after every Event Hubs send succeeds."""
    now = now or datetime.now(timezone.utc)
    target_end = (now - timedelta(seconds=config["ingestLagSeconds"])).replace(second=0, microsecond=0)
    fingerprint = config_fingerprint(config)
    with state.cycle() if state is not None else nullcontext():
        watermark = state.read(fingerprint) if state is not None else read_watermark(Path(config["statePath"]), fingerprint)
        if watermark is not None and target_end <= watermark:
            return 0
        start = (
            watermark - timedelta(seconds=config["overlapSeconds"]) if watermark
            else target_end - timedelta(seconds=config["initialLookbackSeconds"])
        )
        end = min(target_end, start + timedelta(seconds=config["maxWindowSeconds"]))
        window = {"startTime": utc_stamp(start), "endTime": utc_stamp(end)}
        sent = 0
        for spec, subscription, region, resource_ids in plan_batches(config):
            if state is not None:
                state.check_lease()
            response = client.query(request_url(spec, subscription, region, window), resource_ids)
            guard_batch_response(response, resource_ids, spec)
            records = list(normalizer(response, spec["aggregation"], window))
            if state is not None:
                state.check_lease()
            sent += publish_records(producer, records, event_data_class)
        if state is not None:
            state.write(end, fingerprint)
        else:
            write_watermark(Path(config["statePath"]), end, fingerprint)
        LOG.info("Metrics Batch window %s to %s: published %d records", window["startTime"], window["endTime"], sent)
        return sent


def main() -> None:
    from azure.eventhub import EventData, EventHubProducerClient
    from azure.identity import ManagedIdentityCredential
    from contract import normalize_batch

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = load_config()
    credential = ManagedIdentityCredential(client_id=os.environ.get("AZURE_CLIENT_ID") or None)
    state = BlobState(config["stateBlob"], credential) if "stateBlob" in config else None
    client = MetricsBatchClient(
        credential, timeout=config["httpTimeoutSeconds"], attempts=config["httpMaxAttempts"],
    )
    producer = EventHubProducerClient(
        fully_qualified_namespace=config["eventHub"]["fullyQualifiedNamespace"],
        eventhub_name=config["eventHub"]["name"], credential=credential,
    )
    with (nullcontext() if state is not None else single_writer(Path(config["statePath"]))), producer:
        while True:
            try:
                collect_once(config, client, producer, normalize_batch, EventData, state=state)
            except Exception:
                LOG.exception("Metrics Batch cycle failed; exiting without advancing the high-water mark")
                raise
            time.sleep(config["pollSeconds"])


if __name__ == "__main__":
    main()
