"""Live end-to-end tests against a real Azure Managed Redis instance.

Tests are skipped automatically when AMR_HOST / AMR_ACCESS_KEY are absent.
Credentials are read from .env (loaded by exporter at import time) or from
environment variables set directly in the shell.

Run:
    pytest -m live                 # only live tests
    pytest tests/test_live.py -v   # verbose output
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
import redis as redis_lib

import exporter

# ── Live config ───────────────────────────────────────────────────────────────


def _get_live_config() -> dict | None:
    host = os.getenv("AMR_HOST", "")
    access_key = os.getenv("AMR_ACCESS_KEY", "")
    use_entra = os.getenv("AMR_USE_ENTRA", "false").lower() == "true"
    if not host or (not access_key and not use_entra):
        return None
    return {
        "host": host,
        "port": int(os.getenv("AMR_PORT", "10000")),
        "access_key": access_key,
        "cluster_policy": os.getenv("AMR_CLUSTER_POLICY", "enterprise"),
        "ssl_verify": os.getenv("AMR_SSL_VERIFY", "true").lower() != "false",
        "cluster_name": os.getenv("AMR_CLUSTER_NAME", host),
        "dce_endpoint": os.getenv("DCE_ENDPOINT", ""),
        "dcr_rule_id": os.getenv("DCR_RULE_ID", ""),
    }


_LIVE = _get_live_config()

pytestmark = pytest.mark.live

needs_redis = pytest.mark.skipif(
    _LIVE is None,
    reason="AMR_HOST / AMR_ACCESS_KEY not set — add to .env or environment",
)

# ── Upload capture ────────────────────────────────────────────────────────────


class UploadCapture:
    def __init__(self):
        self.batches: list[list[dict]] = []

    def upload(self, rule_id, stream_name, logs):
        self.batches.append(list(logs))

    @property
    def rows(self) -> list[dict]:
        return [r for b in self.batches for r in b]


# ── ExporterRun helper ────────────────────────────────────────────────────────


class ExporterRun:
    """Mirrors the main() poll loop body for test orchestration."""

    def __init__(self):
        self.state = exporter.load_state()

    def poll(self, client) -> list[dict]:
        new_entries, self.state = exporter.fetch_new_entries(client, self.state)
        if not new_entries:
            return []
        exported_at = datetime.now(tz=timezone.utc).isoformat()
        formatted = [exporter.format_entry(e, exported_at) for e in new_entries]
        exporter.append_to_jsonl(formatted)
        exporter.send_to_log_analytics(formatted)
        exporter.save_state(self.state)
        return formatted


# ── File helpers ──────────────────────────────────────────────────────────────


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _read_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


# ── Redis helpers ─────────────────────────────────────────────────────────────


def _is_oss() -> bool:
    return exporter.AMR_CLUSTER_POLICY == "oss"


def _config_set(client, param: str, value: str) -> bool:
    """Attempt CONFIG SET on all primaries. Returns False if not permitted."""
    try:
        if _is_oss():
            for node in client.get_primaries():
                client.get_redis_connection(node).execute_command(
                    "CONFIG", "SET", param, value
                )
        else:
            client.execute_command("CONFIG", "SET", param, value)
        return True
    except redis_lib.RedisError:
        return False


def _slowlog_reset(client) -> None:
    """SLOWLOG RESET on all primaries, ignoring errors."""
    try:
        if _is_oss():
            for node in client.get_primaries():
                client.get_redis_connection(node).slowlog_reset()
        else:
            client.slowlog_reset()
    except redis_lib.RedisError:
        pass


def _fetch_all_raw(client) -> list[dict]:
    """Fetch raw SLOWLOG entries from all primaries."""
    if _is_oss():
        results = []
        for node in client.get_primaries():
            entries = client.get_redis_connection(node).slowlog_get(128) or []
            results.extend(entries)
        return results
    return client.slowlog_get(128) or []


def _run_commands(client, count: int = 3) -> None:
    """Run PING commands against every primary to generate SLOWLOG entries."""
    if _is_oss():
        for node in client.get_primaries():
            conn = client.get_redis_connection(node)
            for _ in range(count):
                conn.ping()
    else:
        for _ in range(count):
            client.ping()


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def live_env(tmp_path, monkeypatch):
    """Patch exporter module globals with live config; redirect files to tmp_path."""
    cfg = _LIVE
    monkeypatch.setattr(exporter, "AMR_HOST", cfg["host"])
    monkeypatch.setattr(exporter, "AMR_PORT", cfg["port"])
    monkeypatch.setattr(exporter, "AMR_ACCESS_KEY", cfg["access_key"])
    monkeypatch.setattr(exporter, "AMR_CLUSTER_POLICY", cfg["cluster_policy"])
    monkeypatch.setattr(exporter, "SSL_VERIFY", cfg["ssl_verify"])
    monkeypatch.setattr(exporter, "CLUSTER_NAME", cfg["cluster_name"])
    monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
    monkeypatch.setattr(exporter, "OUTPUT_FILE", str(tmp_path / "slowquery.jsonl"))
    monkeypatch.setattr(exporter, "STATE_FILE", str(tmp_path / ".state.json"))
    monkeypatch.setattr(
        exporter, "DCR_RULE_ID", cfg["dcr_rule_id"] or "dcr-live-placeholder"
    )
    return tmp_path


@pytest.fixture
def live_client(live_env):
    """Open a live Redis connection; close it after the test."""
    client = exporter.connect()
    yield client
    try:
        client.close()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def la_intercept():
    """Intercept Log Analytics uploads so no real Azure calls are made."""
    capture = UploadCapture()
    exporter._logs_client = capture
    yield capture
    exporter._logs_client = None


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestLiveRedis:
    @needs_redis
    def test_live_01_ping(self, live_client):
        """LIVE-01: Live Redis connection responds to PING."""
        assert live_client.ping()

    @needs_redis
    def test_live_02_slowlog_structure(self, live_client):
        """LIVE-02: SLOWLOG entries contain required fields with correct types."""
        entries = _fetch_all_raw(live_client)
        if not entries:
            if not _config_set(live_client, "slowlog-log-slower-than", "0"):
                pytest.skip("No SLOWLOG entries and CONFIG SET not permitted")
            _run_commands(live_client, 3)
            entries = _fetch_all_raw(live_client)
        if not entries:
            pytest.skip("No SLOWLOG entries available on this instance")

        entry = entries[0]
        for field in ("id", "start_time", "duration", "command"):
            assert field in entry, f"SLOWLOG entry missing field '{field}'"
        assert isinstance(entry["id"], int)
        assert isinstance(entry["duration"], int)

    @needs_redis
    def test_live_03_full_pipeline_jsonl(self, live_client, live_env):
        """LIVE-03: Full export pipeline produces well-formed JSONL entries."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        formatted = run.poll(live_client)
        if not formatted:
            pytest.skip("No SLOWLOG entries captured after generating commands")

        rows = _read_jsonl(live_env / "slowquery.jsonl")
        assert len(rows) == len(formatted)
        required = {
            "id", "timestamp", "duration_us", "duration_ms",
            "command", "redis_host", "cluster_name", "exported_at",
        }
        for row in rows:
            missing = required - set(row.keys())
            assert not missing, f"JSONL row missing fields: {missing}"

    @needs_redis
    def test_live_04_field_types(self, live_client):
        """LIVE-04: Formatted entry fields have correct Python types."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        formatted = run.poll(live_client)
        if not formatted:
            pytest.skip("No SLOWLOG entries captured")

        entry = formatted[0]
        assert isinstance(entry["id"], int)
        assert isinstance(entry["duration_us"], int)
        assert isinstance(entry["duration_ms"], float)
        assert isinstance(entry["command"], str)
        assert isinstance(entry["redis_host"], str)
        assert isinstance(entry["cluster_name"], str)
        datetime.fromisoformat(entry["timestamp"])
        datetime.fromisoformat(entry["exported_at"])

    @needs_redis
    def test_live_05_state_persistence(self, live_client, live_env):
        """LIVE-05: State file written with last_id (enterprise) or per-node IDs (OSS)."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        if not run.poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        state = _read_state(live_env / ".state.json")
        if _is_oss():
            assert "nodes" in state, "OSS state must have 'nodes' key"
            for node_key, last_id in state["nodes"].items():
                assert (
                    isinstance(last_id, int) and last_id >= 0
                ), f"Invalid last_id for node {node_key}: {last_id}"
        else:
            assert "last_id" in state, "Enterprise state must have 'last_id' key"
            assert isinstance(state["last_id"], int) and state["last_id"] >= 0

    @needs_redis
    def test_live_06_restart_dedup(self, live_client):
        """LIVE-06: Second poll after state save exports nothing (deduplication)."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        if not run.poll(live_client):
            pytest.skip("No SLOWLOG entries captured in first poll")

        run2 = ExporterRun()  # simulates exporter restart — reads state from disk
        assert run2.poll(live_client) == []

    @needs_redis
    def test_live_07_incremental_export(self, live_client):
        """LIVE-07: Entries generated after first poll appear only in the second poll."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        first = run.poll(live_client)
        if not first:
            pytest.skip("No SLOWLOG entries in first poll")

        state_after_first = json.loads(json.dumps(run.state))
        _run_commands(live_client, 3)

        second = run.poll(live_client)
        assert second, "Second poll must find new entries after generating more commands"

        if _is_oss():
            advanced = any(
                run.state["nodes"].get(k, -1) > state_after_first["nodes"].get(k, -1)
                for k in run.state.get("nodes", {})
            )
            assert advanced, "State must advance for at least one node after second poll"
        else:
            assert run.state["last_id"] > state_after_first["last_id"]

    @needs_redis
    def test_live_08_la_payload_fields(self, live_client, la_intercept):
        """LIVE-08: Log Analytics payload contains the correct field names."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        if not run.poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        assert la_intercept.rows, "Expected LA rows to be captured"
        expected_fields = {
            "TimeGenerated", "SlowlogId", "Duration_us", "Duration_ms",
            "Command", "RedisHost", "ClusterName", "Node", "ExportedAt",
        }
        assert set(la_intercept.rows[0].keys()) == expected_fields

    @needs_redis
    def test_live_09_oss_node_tagging(self, live_client):
        """LIVE-09: OSS entries have a non-empty 'node' field; enterprise entries do not."""
        if not _config_set(live_client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(live_client)
        _run_commands(live_client, 3)

        run = ExporterRun()
        formatted = run.poll(live_client)
        if not formatted:
            pytest.skip("No SLOWLOG entries captured")

        if _is_oss():
            for entry in formatted:
                assert "node" in entry, f"OSS entry missing 'node': {entry}"
                assert entry["node"], f"OSS 'node' is empty in: {entry}"
        else:
            for entry in formatted:
                assert "node" not in entry, f"Enterprise entry has unexpected 'node': {entry}"
