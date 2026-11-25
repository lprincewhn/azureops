"""Single-replica Event Hubs consumer and Prometheus pull endpoint."""

import logging
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from contract import parse_event, samples

LOG = logging.getLogger(__name__)


class Store:
    def __init__(self, ttl_seconds=900, max_series=10000):
        self.ttl_seconds = ttl_seconds
        self.max_series = max_series
        self.series = {}
        self.lock = threading.Lock()
        self.last_event = None
        self.error = None

    def accept(self, records, now=None):
        now = now or datetime.now(timezone.utc)
        additions = {}
        for record in records:
            for labels, observed, value in samples(record):
                if (now - observed).total_seconds() > self.ttl_seconds:
                    continue
                previous = additions.get(labels)
                if previous is None or observed >= previous[0]:
                    additions[labels] = (observed, value)
        with self.lock:
            self._prune(now)
            if len(set(self.series) | set(additions)) > self.max_series:
                raise ValueError("metric series limit reached")
            for key, entry in additions.items():
                previous = self.series.get(key)
                if previous is None or entry[0] >= previous[0]:
                    self.series[key] = entry
            self.last_event = now

    def _prune(self, now):
        for key, (observed, _) in list(self.series.items()):
            if (now - observed).total_seconds() > self.ttl_seconds:
                del self.series[key]

    def render(self, now=None):
        now = now or datetime.now(timezone.utc)
        with self.lock:
            self._prune(now)
            entries = sorted(self.series.items())
            last_event = self.last_event
            error = self.error
        lines = [
            "# HELP azure_platform_metric Latest Azure platform metric interval value.",
            "# TYPE azure_platform_metric gauge",
        ]
        for labels, (_, value) in entries:
            label_text = ",".join(f'{key}="{escape(val)}"' for key, val in labels)
            lines.append(f"azure_platform_metric{{{label_text}}} {value:g}")
        lines.extend((
            "# HELP azure_metric_stream_series Current active metric series.",
            "# TYPE azure_metric_stream_series gauge",
            f"azure_metric_stream_series {len(entries)}",
            "# HELP azure_metric_stream_last_event_timestamp_seconds Last accepted Event Hubs event.",
            "# TYPE azure_metric_stream_last_event_timestamp_seconds gauge",
            f"azure_metric_stream_last_event_timestamp_seconds {last_event.timestamp() if last_event else 0:g}",
            "# HELP azure_metric_stream_healthy Consumer processing health.",
            "# TYPE azure_metric_stream_healthy gauge",
            f"azure_metric_stream_healthy {0 if error else 1}",
        ))
        return "\n".join(lines) + "\n"


def escape(value):
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def handler(store):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(503 if store.error else 200)
                self.end_headers()
            elif self.path == "/metrics":
                body = store.render().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

    return Handler


def process_event(store, context, event):
    if store.error:
        raise RuntimeError("consumer stopped after processing failure")
    try:
        store.accept(parse_event(event.body_as_str()))
        context.update_checkpoint(event)
    except Exception as exc:
        LOG.exception("Event processing failed; checkpoint not advanced")
        store.error = str(exc)
        raise


def consume(store, namespace, hub, group, checkpoint_account, checkpoint_container):
    from azure.eventhub import EventHubConsumerClient
    from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
    from azure.identity import DefaultAzureCredential
    credential = DefaultAzureCredential()
    checkpoint_store = BlobCheckpointStore(
        f"https://{checkpoint_account}.blob.core.windows.net",
        checkpoint_container, credential=credential,
    )

    def on_event(context, event):
        process_event(store, context, event)

    def on_error(context, error):
        LOG.error("Event Hubs consumer error on %s: %s", context.partition_id if context else "client", error)
        store.error = str(error)

    with EventHubConsumerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=hub,
        consumer_group=group,
        credential=credential,
        checkpoint_store=checkpoint_store,
    ) as client:
        client.receive(on_event=on_event, on_error=on_error, starting_position="-1")
    if store.error:
        raise RuntimeError(store.error)


def main():
    logging.basicConfig(level=logging.INFO)
    store = Store(int(os.getenv("SERIES_TTL_SECONDS", "900")), int(os.getenv("MAX_SERIES", "10000")))
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8000"))), handler(store))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        consume(store, os.environ["EVENTHUB_NAMESPACE"], os.environ["EVENTHUB_NAME"],
                os.environ["EVENTHUB_CONSUMER_GROUP"], os.environ["CHECKPOINT_STORAGE_ACCOUNT"],
                os.environ["CHECKPOINT_CONTAINER"])
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
