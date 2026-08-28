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
from datetime import datetime
from pathlib import Path

import pytest
import redis as redis_lib
from azure.core.exceptions import AzureError

import exporter
from tests.failure_injection import InjectableUpload

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
#
# Uploads go through InjectableUpload (tests/failure_injection.py) so live tests can
# inject upload failures against a real Redis instance, not just assert the happy path.


# ── ExporterRun helper ────────────────────────────────────────────────────────


class ExporterRun:
    """Runs the production poll function while retaining state between calls."""

    def __init__(self):
        self.state = exporter.load_state()

    def poll(self, client) -> list[dict]:
        formatted, self.state = exporter.run_once(client, self.state)
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
    """Intercept Log Analytics uploads so no real Azure calls are made.

    InjectableUpload behaves like the old always-succeed capture until a test
    programs a failure, so the existing LIVE-01..09 tests are unaffected while
    upload-failure scenarios become expressible against a real Redis.
    """
    capture = InjectableUpload()
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


# ── Upload failure injection against real Redis ───────────────────────────────
#
# The suite originally injected no upload failures at all, which is why B2 and B3
# went undetected under a fully green run.  These tests exercise the same failure
# paths as tests/test_failure_paths.py, but with SLOWLOG rows produced by a real
# Azure Managed Redis — so the fields the exporter fingerprints are the fields the
# instance actually returns, rather than the ones a fake chose to provide.


def _row_key(row: dict) -> tuple:
    """Identity of a JSONL row for duplicate detection."""
    return (row["id"], row["timestamp"], row["duration_us"], row["command"], row.get("node"))


def _duplicates(rows: list[dict]) -> list[tuple]:
    """Row identities that appear more than once."""
    seen: set = set()
    duplicated: list[tuple] = []
    for row in rows:
        key = _row_key(row)
        if key in seen:
            duplicated.append(key)
        seen.add(key)
    return duplicated


class TestLiveUploadFailure:
    """Assertions here are phrased as "nothing was duplicated / nothing was lost"
    rather than "the file is unchanged".  A live instance keeps logging while the
    test runs — with a low slowlog threshold even the exporter's own SLOWLOG GET
    is recorded — so row counts legitimately grow between polls.  Growth is not the
    defect; re-appending a row that was already backed up is."""

    def _prepare(self, client) -> None:
        if not _config_set(client, "slowlog-log-slower-than", "0"):
            pytest.skip("CONFIG SET not permitted on this instance")
        _slowlog_reset(client)

    @needs_redis
    def test_live_10_upload_failure_preserves_cursor(self, live_client, live_env, la_intercept):
        """LIVE-10: A failed upload leaves the durable cursor unadvanced."""
        self._prepare(live_client)
        _run_commands(live_client, 3)
        la_intercept.fail_always(AzureError("simulated Log Analytics outage"))

        run = ExporterRun()
        if not run.poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        state = _read_state(live_env / ".state.json")
        assert "_jsonl" in state, "pending backup cursor not persisted"
        cursor_key = "nodes" if _is_oss() else "last_id"
        assert cursor_key not in state, (
            f"durable cursor advanced despite upload failure: {state}"
        )
        assert la_intercept.accepted == []

    @needs_redis
    def test_live_11_retry_after_failure_appends_once(self, live_client, live_env, la_intercept):
        """LIVE-11: Recovery after an outage uploads the batch once and appends once."""
        self._prepare(live_client)
        _run_commands(live_client, 3)
        la_intercept.fail_always(AzureError("simulated Log Analytics outage"))

        if not ExporterRun().poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        rows_after_failure = _read_jsonl(live_env / "slowquery.jsonl")
        assert rows_after_failure, "rows must be backed up locally before the retry"

        la_intercept.succeed()
        ExporterRun().poll(live_client)   # restart — reloads state from disk

        rows_after_retry = _read_jsonl(live_env / "slowquery.jsonl")
        assert _duplicates(rows_after_retry) == [], (
            "retry re-appended rows that were already backed up locally"
        )
        assert rows_after_retry[: len(rows_after_failure)] == rows_after_failure, (
            "the retry rewrote or reordered rows that were already backed up"
        )

    @needs_redis
    def test_live_12_restarts_during_outage_do_not_duplicate(self, live_client, live_env, la_intercept):
        """LIVE-12: Repeated restarts while Log Analytics is down do not grow the JSONL."""
        self._prepare(live_client)
        _run_commands(live_client, 3)
        la_intercept.fail_always(AzureError("simulated Log Analytics outage"))

        if not ExporterRun().poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        baseline = _read_jsonl(live_env / "slowquery.jsonl")
        for _ in range(2):
            ExporterRun().poll(live_client)

        rows = _read_jsonl(live_env / "slowquery.jsonl")
        assert _duplicates(rows) == [], (
            f"restarts during the outage duplicated already-backed-up rows: "
            f"{_duplicates(rows)}"
        )
        assert rows[: len(baseline)] == baseline

    @needs_redis
    def test_live_13_reset_during_pending_upload_keeps_every_row(
        self, live_client, live_env, la_intercept
    ):
        """LIVE-13: SLOWLOG RESET while an upload is pending must not drop rows.

        Everything Log Analytics accepts has to be in the local JSONL too.  On a real
        instance the new generation's rows carry whatever entropy that instance
        provides, so this is the honest version of the B2 scenario.
        """
        self._prepare(live_client)
        _run_commands(live_client, 5)
        la_intercept.fail_always(AzureError("simulated Log Analytics outage"))

        run = ExporterRun()
        if not run.poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        rows_before_reset = len(_read_jsonl(live_env / "slowquery.jsonl"))

        _slowlog_reset(live_client)
        _run_commands(live_client, 8)
        la_intercept.succeed()
        if not run.poll(live_client):
            pytest.skip("No SLOWLOG entries captured after reset")

        appended = _read_jsonl(live_env / "slowquery.jsonl")[rows_before_reset:]
        local_keys = {(row["id"], row["command"]) for row in appended}
        missing = [
            (row["SlowlogId"], row["Command"])
            for row in la_intercept.accepted_rows
            if (row["SlowlogId"], row["Command"]) not in local_keys
        ]
        assert not missing, (
            f"{len(missing)} row(s) reached Log Analytics but never reached the local "
            f"JSONL: {missing[:10]}"
        )

    @needs_redis
    @pytest.mark.xfail(
        reason="待真实 Redis 验证 / pending real-Redis verification: B3 is fixed and its "
               "offline counterparts (FP-11 / FP-12) pass, but this live case has not "
               "been run against a real Azure Managed Redis instance yet. Remove this "
               "marker once it is confirmed green there.",
        strict=False,
    )
    def test_live_14_legacy_state_does_not_duplicate(self, live_client, live_env, la_intercept):
        """LIVE-14: Upgrading over a pre-fingerprint pending state must not re-append.

        Writes the state file shape the previous version left behind mid-outage
        (a `_jsonl` cursor with no `fingerprints` key), then polls.
        """
        self._prepare(live_client)
        _run_commands(live_client, 3)
        la_intercept.fail_always(AzureError("simulated Log Analytics outage"))

        if not ExporterRun().poll(live_client):
            pytest.skip("No SLOWLOG entries captured")

        rows = _read_jsonl(live_env / "slowquery.jsonl")
        state = _read_state(live_env / ".state.json")
        pending = state["_jsonl"]
        # Strip fingerprints to reproduce the legacy on-disk shape exactly.
        legacy = {"nodes": pending["nodes"]} if _is_oss() else {"last_id": pending["last_id"]}
        exporter.save_state({"_jsonl": legacy})

        la_intercept.succeed()
        ExporterRun().poll(live_client)

        after = _read_jsonl(live_env / "slowquery.jsonl")
        assert _duplicates(after) == [], (
            f"legacy state file caused already-backed-up rows to be appended again: "
            f"{_duplicates(after)}"
        )
        assert after[: len(rows)] == rows
