#!/usr/bin/env python3
"""Azure Managed Redis slow query log exporter.

Periodically polls the Redis SLOWLOG and appends new entries to a JSONL file.
Supports both Access Key and Microsoft Entra ID (service principal / managed identity) auth.

Connection notes:
  - Azure Managed Redis always uses SSL on port 10000.
  - Enterprise cluster policy (default): single-endpoint, use plain redis.Redis.
  - OSS cluster policy: use RedisCluster (add AMR_CLUSTER_POLICY=oss).
  - SLOWLOG in a clustered OSS deployment only returns entries from the shard
    that handles the connection; for full coverage use Enterprise cluster policy
    or query each node separately.
"""

import hashlib
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis
from azure.core.exceptions import AzureError
from azure.identity import WorkloadIdentityCredential
from azure.monitor.ingestion import LogsIngestionClient
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

AMR_HOST = os.environ.get("AMR_HOST", "")
AMR_PORT = int(os.getenv("AMR_PORT", "10000"))
AMR_ACCESS_KEY = os.getenv("AMR_ACCESS_KEY", "")
AMR_CLUSTER_POLICY = os.getenv("AMR_CLUSTER_POLICY", "enterprise")  # "enterprise" | "oss"

# Entra ID auth — set AMR_USE_ENTRA=true to enable, mutually exclusive with access key
USE_ENTRA = os.getenv("AMR_USE_ENTRA", "false").lower() == "true"
ENTRA_CLIENT_ID = os.getenv("AMR_ENTRA_CLIENT_ID", "")
ENTRA_CLIENT_SECRET = os.getenv("AMR_ENTRA_CLIENT_SECRET", "")
ENTRA_TENANT_ID = os.getenv("AMR_ENTRA_TENANT_ID", "")
USE_MANAGED_IDENTITY = os.getenv("AMR_USE_MANAGED_IDENTITY", "false").lower() == "true"

# Exporter behaviour
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
SLOWLOG_BATCH_SIZE = int(os.getenv("SLOWLOG_BATCH_SIZE", "128"))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "slowquery.jsonl")
STATE_FILE = os.getenv("STATE_FILE", ".slowquery_state.json")
SSL_VERIFY = os.getenv("AMR_SSL_VERIFY", "true").lower() != "false"
CLUSTER_NAME = os.getenv("AMR_CLUSTER_NAME", AMR_HOST)  # human-friendly label for this instance

# StatefulSet multi-cluster mode
POD_NAME = os.getenv("POD_NAME", "")
CLUSTERS_CONFIG_FILE = os.getenv("CLUSTERS_CONFIG_FILE", "")

# Azure Monitor Log Analytics — injected automatically by Workload Identity webhook
DCE_ENDPOINT = os.environ.get("DCE_ENDPOINT", "")
DCR_RULE_ID = os.environ.get("DCR_RULE_ID", "")
DCR_STREAM_NAME = "Custom-AMRSlowQuery_CL"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Redis connection ──────────────────────────────────────────────────────────


def _build_credential_provider():
    try:
        from redis_entraid.cred_provider import (
            ManagedIdentityType,
            create_from_managed_identity,
            create_from_service_principal,
        )
    except ImportError:
        sys.exit(
            "Package redis-entraid is required for Entra ID auth.\n"
            "Install it with: pip install redis-entraid"
        )

    if USE_MANAGED_IDENTITY:
        log.info("Auth: managed identity (system-assigned)")
        return create_from_managed_identity(
            identity_type=ManagedIdentityType.SYSTEM_ASSIGNED
        )

    if not all([ENTRA_CLIENT_ID, ENTRA_CLIENT_SECRET, ENTRA_TENANT_ID]):
        sys.exit(
            "AMR_ENTRA_CLIENT_ID, AMR_ENTRA_CLIENT_SECRET, and AMR_ENTRA_TENANT_ID "
            "are all required when AMR_USE_ENTRA=true and AMR_USE_MANAGED_IDENTITY=false"
        )
    log.info("Auth: service principal client_id=%s", ENTRA_CLIENT_ID)
    return create_from_service_principal(
        ENTRA_CLIENT_ID, ENTRA_CLIENT_SECRET, ENTRA_TENANT_ID
    )


def _connection_kwargs() -> dict:
    kwargs = dict(
        host=AMR_HOST,
        port=AMR_PORT,
        ssl=True,
        ssl_cert_reqs="required" if SSL_VERIFY else "none",
        socket_timeout=15,
        socket_connect_timeout=15,
        health_check_interval=30,
        retry_on_timeout=True,
        decode_responses=False,
    )
    if USE_ENTRA:
        kwargs["credential_provider"] = _build_credential_provider()
    else:
        kwargs["password"] = AMR_ACCESS_KEY
    return kwargs


def connect() -> redis.Redis:
    kwargs = _connection_kwargs()
    if AMR_CLUSTER_POLICY == "oss":
        from redis.cluster import RedisCluster

        log.info("Connecting to %s:%d (OSS cluster policy, SSL)", AMR_HOST, AMR_PORT)
        # RedisCluster does not accept decode_responses in the same constructor path
        oss_kwargs = {k: v for k, v in kwargs.items() if k != "decode_responses"}
        return RedisCluster(decode_responses=False, **oss_kwargs)

    log.info("Connecting to %s:%d (Enterprise cluster policy, SSL)", AMR_HOST, AMR_PORT)
    return redis.Redis(**kwargs)


# ── State persistence ─────────────────────────────────────────────────────────
#
# Enterprise mode state:  {"last_id": 42}
# OSS cluster mode state: {"nodes": {"host:port": 42, ...}}
# While an upload is pending, "_jsonl" holds the independently persisted local
# backup cursor so a retry does not append the same rows again.


def load_state() -> dict:
    try:
        return json.loads(Path(STATE_FILE).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_state(state: dict) -> None:
    Path(STATE_FILE).write_text(
        json.dumps(state, ensure_ascii=False), encoding="utf-8"
    )


# ── Slow log fetching ─────────────────────────────────────────────────────────


def _filter_new(raw: list, last_id: int, node: str) -> tuple[list, int]:
    """Return (new_entries_oldest_first, new_last_id) for a single node's raw entries."""
    if not raw:
        return [], last_id

    max_id = max(e["id"] for e in raw)
    if 0 < last_id and max_id < last_id:
        log.warning(
            "SLOWLOG counter reset on %s (max_id=%d < last_id=%d); re-exporting visible entries",
            node,
            max_id,
            last_id,
        )
        last_id = -1

    new = [e for e in raw if e["id"] > last_id]
    if not new:
        return [], last_id

    return list(reversed(new)), max(e["id"] for e in new)


def _entry_content_fingerprint(entry: dict) -> str:
    """Identify repeated command content independently of the recyclable ID."""
    identity = {
        "start_time": entry.get("start_time"),
        "duration": entry.get("duration"),
        "command": _decode(entry.get("command")),
        "client_address": _decode(entry.get("client_address")),
        "client_name": _decode(entry.get("client_name")),
        "node": entry.get("_node"),
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _jsonl_cursor_reset(
    raw: list,
    last_id: int,
    fingerprints: dict | None,
    node: str,
) -> bool:
    """Detect a new SLOWLOG generation against the persisted backup cursor."""
    if not raw or last_id <= 0:
        return False

    max_id = max(entry["id"] for entry in raw)
    if max_id < last_id:
        return True

    overlap = [entry for entry in raw if entry["id"] <= last_id]
    if not overlap:
        return False

    if fingerprints and any(
        fingerprints.get(str(entry["id"])) != _entry_fingerprint(entry)
        for entry in overlap
    ):
        return True

    # AMR can return rows with no generation-distinguishing fields. In that
    # irreducibly ambiguous case, prefer re-exporting a uniform burst over
    # silently losing an entire post-reset generation.
    signatures = {_entry_content_fingerprint(entry) for entry in raw}
    if (
        fingerprints is not None
        and len(raw) > 1
        and min(entry["id"] for entry in raw) == 1
        and len(signatures) == 1
    ):
        log.warning(
            "Ambiguous SLOWLOG generation on %s at backup cursor %d; "
            "re-exporting the visible uniform burst",
            node,
            last_id,
        )
        return True

    return False


def fetch_new_entries(client: redis.Redis, state: dict) -> tuple[list, dict]:
    """Return (new_entries_oldest_first, updated_state).

    Enterprise mode: queries the single endpoint; state key is "last_id".
    OSS cluster mode: broadcasts to all primaries; state key is "nodes" (per-node last_id map).
    """
    if AMR_CLUSTER_POLICY == "oss":
        return _fetch_oss(client, state)
    return _fetch_enterprise(client, state)


def _fetch_enterprise(client: redis.Redis, state: dict) -> tuple[list, dict]:
    last_id: int = state.get("last_id", -1)
    try:
        raw: list = client.slowlog_get(SLOWLOG_BATCH_SIZE)
    except redis.RedisError as exc:
        log.error("SLOWLOG GET failed: %s", exc)
        return [], state

    jsonl_state = state.get("_jsonl", state)
    jsonl_last_id = jsonl_state.get("last_id", last_id)
    fingerprints = (
        jsonl_state.get("fingerprints")
        if "_jsonl" in state
        else jsonl_state.get("fingerprints", {})
    )
    reset = _jsonl_cursor_reset(raw, jsonl_last_id, fingerprints, AMR_HOST)
    new, new_last_id = _filter_new(raw, -1 if reset else last_id, AMR_HOST)
    if not new:
        return [], state
    if reset:
        for entry in new:
            entry["_jsonl_reset"] = True
    return new, {"last_id": new_last_id}


def _fetch_oss(client, state: dict) -> tuple[list, dict]:
    """Query all primary nodes and merge results.

    Iterates client.get_primaries() and queries each node individually via
    client.get_redis_connection(node).  This avoids the ambiguous return type of
    execute_command(target_nodes=PRIMARIES), which returns a list when there is
    only one primary and a dict when there are multiple.

    Each node has its own independent SLOWLOG counter, so last_id is tracked
    per node in state["nodes"].
    """
    node_last_ids: dict = dict(state.get("nodes", {}))
    all_new: list = []

    try:
        primaries = client.get_primaries()
    except redis.RedisError as exc:
        log.error("Failed to get primary nodes: %s", exc)
        return [], state

    for node in primaries:
        node_key = f"{node.host}:{node.port}"
        try:
            conn = client.get_redis_connection(node)
            raw: list = conn.slowlog_get(SLOWLOG_BATCH_SIZE)
        except redis.RedisError as exc:
            log.error("SLOWLOG GET failed on %s: %s", node_key, exc)
            continue

        for entry in raw or []:
            entry["_node"] = node_key
        last_id = node_last_ids.get(node_key, -1)
        jsonl_state = state.get("_jsonl", state)
        jsonl_last_id = jsonl_state.get("nodes", {}).get(node_key, last_id)
        fingerprints = (
            jsonl_state.get("fingerprints", {}).get(node_key)
            if "_jsonl" in state
            else jsonl_state.get("fingerprints", {}).get(node_key, {})
        )
        reset = _jsonl_cursor_reset(
            raw or [], jsonl_last_id, fingerprints, node_key
        )
        new, new_last_id = _filter_new(
            raw or [], -1 if reset else last_id, node_key
        )
        if new:
            for entry in new:
                if reset:
                    entry["_jsonl_reset"] = True
            all_new.extend(new)
            node_last_ids[node_key] = new_last_id

    # Sort merged results by start_time so the JSONL file stays roughly chronological
    all_new.sort(key=lambda e: (e.get("start_time", 0), e.get("id", 0)))
    return all_new, {"nodes": node_last_ids}


# ── Entry formatting ──────────────────────────────────────────────────────────


def _decode(val) -> str:
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace")
    return val or ""


def _entry_fingerprint(entry: dict) -> str:
    """Identify a SLOWLOG row across retries without relying on its recyclable ID."""
    identity = {
        "id": entry.get("id"),
        "start_time": entry.get("start_time"),
        "duration": entry.get("duration"),
        "command": _decode(entry.get("command")),
        "client_address": _decode(entry.get("client_address")),
        "client_name": _decode(entry.get("client_name")),
        "node": entry.get("_node"),
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def format_entry(entry: dict, exported_at: str) -> dict:
    ts = entry.get("start_time", 0)
    duration_us = entry.get("duration", 0)
    result = {
        "id": entry.get("id"),
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "duration_us": duration_us,
        "duration_ms": round(duration_us / 1000, 3),
        "command": _decode(entry.get("command")),
        "redis_host": AMR_HOST,
        "cluster_name": CLUSTER_NAME,
        "exported_at": exported_at,
    }
    # OSS cluster mode attaches the originating shard so entries are distinguishable
    if "_node" in entry:
        result["node"] = entry["_node"]
    return result


def append_to_jsonl(entries: list[dict]) -> None:
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _jsonl_append_indexes(entries: list[dict], state: dict) -> tuple[list[int], dict]:
    """Return indexes not yet backed up and the advanced JSONL cursor."""
    jsonl_state = state.get("_jsonl", state)

    if AMR_CLUSTER_POLICY != "oss":
        last_id = jsonl_state.get("last_id", -1)
        max_id = max(entry["id"] for entry in entries)
        fingerprints = dict(jsonl_state.get("fingerprints", {}))
        overlap = [entry for entry in entries if entry["id"] <= last_id]
        reset = any(entry.get("_jsonl_reset") for entry in entries) or (
            bool(overlap)
            and bool(fingerprints)
            and any(
                fingerprints.get(str(entry["id"])) != _entry_fingerprint(entry)
                for entry in overlap
            )
        )
        indexes = [
            index
            for index, entry in enumerate(entries)
            if reset or entry["id"] > last_id
        ]
        if reset:
            fingerprints = {}
        fingerprints.update(
            {str(entry["id"]): _entry_fingerprint(entry) for entry in entries}
        )
        fingerprints = dict(
            sorted(fingerprints.items(), key=lambda item: int(item[0]))[
                -SLOWLOG_BATCH_SIZE:
            ]
        )
        return indexes, {
            "last_id": max_id if reset else max(last_id, max_id),
            "fingerprints": fingerprints,
        }

    node_last_ids = dict(jsonl_state.get("nodes", {}))
    node_fingerprints = {
        node: dict(fingerprints)
        for node, fingerprints in jsonl_state.get("fingerprints", {}).items()
    }
    by_node: dict[str, list[tuple[int, dict]]] = {}
    for index, entry in enumerate(entries):
        by_node.setdefault(entry["_node"], []).append((index, entry))

    indexes: list[int] = []
    for node, node_entries in by_node.items():
        last_id = node_last_ids.get(node, -1)
        max_id = max(entry["id"] for _, entry in node_entries)
        fingerprints = node_fingerprints.get(node, {})
        overlap = [entry for _, entry in node_entries if entry["id"] <= last_id]
        reset = any(entry.get("_jsonl_reset") for _, entry in node_entries) or (
            bool(overlap)
            and bool(fingerprints)
            and any(
                fingerprints.get(str(entry["id"])) != _entry_fingerprint(entry)
                for entry in overlap
            )
        )
        indexes.extend(
            index
            for index, entry in node_entries
            if reset or entry["id"] > last_id
        )
        node_last_ids[node] = max_id if reset else max(last_id, max_id)
        if reset:
            fingerprints = {}
        fingerprints.update(
            {str(entry["id"]): _entry_fingerprint(entry) for _, entry in node_entries}
        )
        fingerprints = dict(
            sorted(fingerprints.items(), key=lambda item: int(item[0]))[
                -SLOWLOG_BATCH_SIZE:
            ]
        )
        node_fingerprints[node] = fingerprints

    indexes.sort()
    return indexes, {
        "nodes": node_last_ids,
        "fingerprints": node_fingerprints,
    }


# ── Azure Monitor Log Analytics ───────────────────────────────────────────────

_logs_client: LogsIngestionClient | None = None


def _get_logs_client() -> LogsIngestionClient:
    global _logs_client
    if _logs_client is None:
        # WorkloadIdentityCredential reads AZURE_CLIENT_ID / AZURE_TENANT_ID /
        # AZURE_FEDERATED_TOKEN_FILE injected by the AKS Workload Identity webhook
        _logs_client = LogsIngestionClient(
            endpoint=DCE_ENDPOINT,
            credential=WorkloadIdentityCredential(),
            logging_enable=False,
        )
    return _logs_client


def send_to_log_analytics(entries: list[dict]) -> None:
    body = [
        {
            "TimeGenerated": e["timestamp"],
            "SlowlogId":     e["id"],
            "Duration_us":   e["duration_us"],
            "Duration_ms":   e["duration_ms"],
            "Command":       e["command"],
            "RedisHost":     e["redis_host"],
            "ClusterName":   e["cluster_name"],
            "Node":          e.get("node", ""),
            "ExportedAt":    e["exported_at"],
        }
        for e in entries
    ]
    _get_logs_client().upload(
        rule_id=DCR_RULE_ID,
        stream_name=DCR_STREAM_NAME,
        logs=body,
    )
    log.info("Sent %d entries to Log Analytics", len(body))


# ── Main loop ─────────────────────────────────────────────────────────────────


def run_once(client: redis.Redis, state: dict) -> tuple[list[dict], dict]:
    """Run one poll, returning the formatted entries and resulting durable state."""
    new_entries, next_state = fetch_new_entries(client, state)
    if not new_entries:
        log.debug("No new slow query entries")
        return [], state

    exported_at = datetime.now(tz=timezone.utc).isoformat()
    formatted = [format_entry(entry, exported_at) for entry in new_entries]
    append_indexes, jsonl_state = _jsonl_append_indexes(new_entries, state)
    append_to_jsonl([formatted[index] for index in append_indexes])

    pending_state = dict(state)
    pending_state["_jsonl"] = jsonl_state
    save_state(pending_state)

    try:
        send_to_log_analytics(formatted)
    except (AzureError, ValueError) as exc:
        log.error(
            "Failed to send to Log Analytics; cursor not advanced: %s",
            exc,
        )
        return formatted, pending_state

    save_state(next_state)
    return formatted, next_state


class _Stop(BaseException):
    pass


def _load_cluster_config() -> None:
    """StatefulSet mode: override config vars from clusters JSON using this pod's ordinal."""
    if not CLUSTERS_CONFIG_FILE:
        return

    if not POD_NAME:
        sys.exit("POD_NAME is required when CLUSTERS_CONFIG_FILE is set (inject via downward API)")

    m = re.search(r"-(\d+)$", POD_NAME)
    if not m:
        sys.exit(f"Cannot parse ordinal from POD_NAME={POD_NAME!r}; expected suffix like '-0'")
    ordinal = int(m.group(1))

    try:
        clusters = json.loads(Path(CLUSTERS_CONFIG_FILE).read_text(encoding="utf-8"))
    except Exception as exc:
        sys.exit(f"Failed to read {CLUSTERS_CONFIG_FILE}: {exc}")

    if ordinal >= len(clusters):
        sys.exit(f"Pod ordinal {ordinal} out of range: clusters.json has {len(clusters)} entr(ies)")

    cfg = clusters[ordinal]

    global AMR_HOST, AMR_PORT, AMR_ACCESS_KEY, AMR_CLUSTER_POLICY, SSL_VERIFY
    global CLUSTER_NAME, POLL_INTERVAL, SLOWLOG_BATCH_SIZE

    AMR_HOST           = cfg.get("AMR_HOST", AMR_HOST)
    AMR_PORT           = int(cfg.get("AMR_PORT", AMR_PORT))
    AMR_ACCESS_KEY     = cfg.get("AMR_ACCESS_KEY", AMR_ACCESS_KEY)
    AMR_CLUSTER_POLICY = cfg.get("AMR_CLUSTER_POLICY", AMR_CLUSTER_POLICY)
    SSL_VERIFY         = str(cfg.get("AMR_SSL_VERIFY", str(SSL_VERIFY))).lower() != "false"
    CLUSTER_NAME       = cfg.get("AMR_CLUSTER_NAME", AMR_HOST)
    POLL_INTERVAL      = int(cfg.get("POLL_INTERVAL_SECONDS", POLL_INTERVAL))
    SLOWLOG_BATCH_SIZE = int(cfg.get("SLOWLOG_BATCH_SIZE", SLOWLOG_BATCH_SIZE))

    log.info(
        "Pod %s (ordinal=%d) → cluster '%s' (%s:%d)",
        POD_NAME, ordinal, CLUSTER_NAME, AMR_HOST, AMR_PORT,
    )


def _validate_config() -> None:
    if not AMR_HOST:
        sys.exit("AMR_HOST is required (e.g. mycache.eastus.redis.azure.net)")
    if not USE_ENTRA and not AMR_ACCESS_KEY:
        sys.exit(
            "Either set AMR_ACCESS_KEY (access key auth) "
            "or AMR_USE_ENTRA=true (Entra ID auth)"
        )
    if not DCE_ENDPOINT or not DCR_RULE_ID:
        sys.exit("DCE_ENDPOINT and DCR_RULE_ID are required for Log Analytics output")


def main() -> None:
    _load_cluster_config()
    _validate_config()

    log.info(
        "Slow query exporter starting — host=%s:%d  poll=%ds  output=%s",
        AMR_HOST,
        AMR_PORT,
        POLL_INTERVAL,
        OUTPUT_FILE,
    )

    def _stop(signum, frame):
        raise _Stop

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    client = connect()
    state = load_state()
    if AMR_CLUSTER_POLICY == "oss":
        log.info("Resuming from per-node state: %s", state.get("nodes", {}))
    else:
        log.info("Resuming from last exported slowlog ID=%d", state.get("last_id", -1))

    try:
        while True:
            try:
                _, state = run_once(client, state)

            except redis.ConnectionError as exc:
                log.warning("Connection lost: %s — reconnecting in 10s", exc)
                time.sleep(10)
                try:
                    client = connect()
                except redis.RedisError as exc2:
                    log.error("Reconnect failed: %s", exc2)

            except redis.RedisError as exc:
                log.error("Redis error: %s", exc)

            time.sleep(POLL_INTERVAL)

    except _Stop:
        log.info("Exporter stopped gracefully")
        save_state(state)


if __name__ == "__main__":
    main()
