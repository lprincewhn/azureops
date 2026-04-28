"""End-to-end functional tests for exporter.py

Every test exercises the complete data pipeline without any external services:

    FakeSlowlog  ──►  fetch_new_entries  ──►  format_entry
                                          ──►  append_to_jsonl     (real file I/O)
                                          ──►  send_to_log_analytics  (UploadCapture)
                                          ──►  save_state          (real file I/O)

State persistence between polls is real: ExporterRun.poll() saves to disk and
a new ExporterRun() reloads from disk, so restart deduplication is exercised for real.

Test groups
-----------
E2E-01 ~ E2E-11   Enterprise cluster mode  (single endpoint)
E2E-12 ~ E2E-17   OSS cluster mode         (multi-shard, per-node state)
E2E-18 ~ E2E-19   StatefulSet multi-cluster config loading
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import redis as redis_lib

import exporter

# ── In-process Redis simulation ───────────────────────────────────────────────


class FakeSlowlog:
    """Simulates a single-node Redis SLOWLOG (newest-entry first, like real Redis)."""

    def __init__(self):
        self._entries: list[dict] = []

    def add(
        self,
        id: int,
        command: bytes = b"GET key",
        duration_us: int = 5_000,
        start_time: int = 1_700_000_000,
    ) -> None:
        self._entries.insert(0, {
            "id": id,
            "start_time": start_time,
            "duration": duration_us,
            "command": command,
        })

    def reset(self) -> None:
        """Simulate SLOWLOG RESET — all entries cleared, counter restarts from 0."""
        self._entries.clear()

    def slowlog_get(self, count: int) -> list:
        return list(self._entries[:count])


class FailingSlowlog(FakeSlowlog):
    """Raises RedisError on every slowlog_get call (simulates unreachable shard)."""

    def slowlog_get(self, count: int) -> list:
        raise redis_lib.RedisError("simulated shard unavailable")


class FakeEnterpriseClient:
    """Minimal redis.Redis replacement for enterprise cluster mode."""

    def __init__(self, slowlog: FakeSlowlog):
        self._sl = slowlog

    def slowlog_get(self, count: int) -> list:
        return self._sl.slowlog_get(count)


class FakeOssClient:
    """Minimal RedisCluster replacement for OSS cluster mode.

    nodes: mapping of 'host:port' → FakeSlowlog (or FailingSlowlog).
    """

    def __init__(self, nodes: dict):
        self._node_map: dict[str, tuple] = {}
        for key, slowlog in nodes.items():
            host, port_str = key.rsplit(":", 1)
            node_mock = _mock_node(host, int(port_str))
            conn_mock = _mock_conn(slowlog)
            self._node_map[key] = (node_mock, conn_mock)

    def get_primaries(self):
        return [pair[0] for pair in self._node_map.values()]

    def get_redis_connection(self, node):
        key = f"{node.host}:{node.port}"
        return self._node_map[key][1]


def _mock_node(host: str, port: int):
    from unittest.mock import MagicMock
    n = MagicMock()
    n.host = host
    n.port = port
    return n


def _mock_conn(slowlog: FakeSlowlog):
    from unittest.mock import MagicMock
    c = MagicMock()
    # Assign bound method directly so the MagicMock delegates to our FakeSlowlog
    c.slowlog_get = slowlog.slowlog_get
    return c


# ── In-process Log Analytics capture ─────────────────────────────────────────


class UploadCapture:
    """Collects every Log Analytics upload batch; optionally simulates failures."""

    def __init__(self, fail: bool = False):
        self.batches: list[list[dict]] = []
        self._fail = fail

    def upload(self, rule_id: str, stream_name: str, logs: list) -> None:
        if self._fail:
            raise Exception("simulated upload failure")
        self.batches.append(list(logs))

    @property
    def rows(self) -> list[dict]:
        return [row for batch in self.batches for row in batch]

    @property
    def total(self) -> int:
        return len(self.rows)


# ── Test fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Isolate all file paths and module globals for one E2E test.

    AMR_CLUSTER_POLICY is intentionally not set here so that class-level
    fixtures (enterprise_mode / oss_mode) can control it without being
    overridden by this fixture.
    """
    monkeypatch.setattr(exporter, "AMR_HOST",          "test.redis.azure.net")
    monkeypatch.setattr(exporter, "AMR_PORT",           10000)
    monkeypatch.setattr(exporter, "CLUSTER_NAME",       "test-cluster")
    monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
    monkeypatch.setattr(exporter, "OUTPUT_FILE",        str(tmp_path / "slowquery.jsonl"))
    monkeypatch.setattr(exporter, "STATE_FILE",         str(tmp_path / ".state.json"))
    monkeypatch.setattr(exporter, "DCR_RULE_ID",        "dcr-test-rule")
    return tmp_path


@pytest.fixture
def capture():
    """Wire an UploadCapture into the exporter as the cached Log Analytics client."""
    uc = UploadCapture()
    exporter._logs_client = uc
    yield uc
    exporter._logs_client = None


@pytest.fixture
def failing_capture():
    """Same as capture but upload always raises, to test error-resilience paths."""
    uc = UploadCapture(fail=True)
    exporter._logs_client = uc
    yield uc
    exporter._logs_client = None


# ── Poll-cycle helper (mirrors main() while-loop body exactly) ────────────────


class ExporterRun:
    """Simulates one exporter process lifetime.

    Calling poll() once matches a single iteration of the while loop in main():
      1. fetch_new_entries  (reads current in-memory state)
      2. format_entry       for each new entry
      3. append_to_jsonl    (real file I/O)
      4. send_to_log_analytics
      5. save_state         (real file I/O)

    Constructing a new ExporterRun() loads state from disk — this is how restart
    deduplication is tested without touching the main() function directly.
    """

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


# ── JSONL file helper ─────────────────────────────────────────────────────────


def read_jsonl() -> list[dict]:
    """Read every row from the current OUTPUT_FILE."""
    p = Path(exporter.OUTPUT_FILE)
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


def read_state() -> dict:
    p = Path(exporter.STATE_FILE)
    if not p.exists():
        return {}
    return json.loads(p.read_text())


# ─────────────────────────────────────────────────────────────────────────────
# Enterprise cluster mode E2E  (TC E2E-01 ~ E2E-11)
# ─────────────────────────────────────────────────────────────────────────────


class TestEnterpriseE2E:

    @pytest.fixture(autouse=True)
    def enterprise_mode(self, monkeypatch):
        monkeypatch.setattr(exporter, "AMR_CLUSTER_POLICY", "enterprise")

    # E2E-01 ──────────────────────────────────────────────────────────────────
    def test_first_poll_exports_all_entries_oldest_first(self, env, capture):
        """First run with no prior state exports all SLOWLOG entries, oldest first."""
        sl = FakeSlowlog()
        # Add in increasing ID order so FakeSlowlog returns newest (highest ID) first,
        # matching real Redis SLOWLOG behavior.
        sl.add(1, command=b"GET foo",           duration_us=5_000, start_time=1_700_000_001)
        sl.add(2, command=b"MGET k1 k2",       duration_us=6_000, start_time=1_700_000_002)
        sl.add(3, command=b"HGETALL big-hash", duration_us=8_000, start_time=1_700_000_003)

        exported = ExporterRun().poll(FakeEnterpriseClient(sl))

        assert len(exported) == 3
        assert [e["id"] for e in exported] == [1, 2, 3]

    # E2E-02 ──────────────────────────────────────────────────────────────────
    def test_first_poll_writes_jsonl_with_correct_content(self, env, capture):
        """JSONL file is created with one line per entry after the first poll."""
        sl = FakeSlowlog()
        sl.add(1, command=b"GET k",   duration_us=5_000)
        sl.add(2, command=b"SET k v", duration_us=7_000)

        ExporterRun().poll(FakeEnterpriseClient(sl))

        rows = read_jsonl()
        assert len(rows) == 2
        assert rows[0]["id"] == 1 and rows[0]["command"] == "GET k"
        assert rows[1]["id"] == 2 and rows[1]["command"] == "SET k v"

    # E2E-03 ──────────────────────────────────────────────────────────────────
    def test_first_poll_saves_state_with_max_id(self, env, capture):
        """State file records the highest SLOWLOG ID seen after the first poll."""
        sl = FakeSlowlog()
        sl.add(1, command=b"GET c")
        sl.add(3, command=b"GET b")
        sl.add(5, command=b"GET a")

        ExporterRun().poll(FakeEnterpriseClient(sl))

        assert read_state() == {"last_id": 5}

    # E2E-04 ──────────────────────────────────────────────────────────────────
    def test_restart_does_not_re_export_already_seen_entries(self, env, capture):
        """After a restart, entries already in the JSONL are not written again."""
        sl = FakeSlowlog()
        sl.add(1, command=b"GET c")
        sl.add(2, command=b"GET b")
        sl.add(3, command=b"GET a")
        client = FakeEnterpriseClient(sl)

        ExporterRun().poll(client)            # first run — exports 3
        row_count_after_first = len(read_jsonl())

        ExporterRun().poll(client)            # restart with same SLOWLOG
        assert len(read_jsonl()) == row_count_after_first   # no duplicates

    # E2E-05 ──────────────────────────────────────────────────────────────────
    def test_incremental_export_after_restart_picks_up_only_new_entries(self, env, capture):
        """After a restart, only entries with id > saved last_id are exported."""
        sl = FakeSlowlog()
        sl.add(1, command=b"GET b")
        sl.add(2, command=b"GET a")
        client = FakeEnterpriseClient(sl)

        ExporterRun().poll(client)        # exports id=1,2

        sl.add(3, command=b"DEL z")
        sl.add(4, command=b"SET x y")
        ExporterRun().poll(client)        # restart, should export id=3,4 only

        rows = read_jsonl()
        assert len(rows) == 4
        assert [r["id"] for r in rows] == [1, 2, 3, 4]

    # E2E-06 ──────────────────────────────────────────────────────────────────
    def test_multiple_polls_same_session_accumulate_without_duplicates(self, env, capture):
        """Within one process lifetime, successive polls extend the JSONL correctly."""
        sl = FakeSlowlog()
        sl.add(1, command=b"GET a")
        client = FakeEnterpriseClient(sl)

        run = ExporterRun()
        run.poll(client)            # exports id=1

        sl.add(2, command=b"GET b")
        sl.add(3, command=b"GET c")
        run.poll(client)            # exports id=2,3

        rows = read_jsonl()
        assert len(rows) == 3
        assert [r["id"] for r in rows] == [1, 2, 3]
        assert read_state() == {"last_id": 3}

    # E2E-07 ──────────────────────────────────────────────────────────────────
    def test_empty_slowlog_writes_nothing_and_creates_no_files(self, env, capture):
        """When SLOWLOG is empty, neither the JSONL file nor the state file is created."""
        ExporterRun().poll(FakeEnterpriseClient(FakeSlowlog()))

        assert not Path(exporter.OUTPUT_FILE).exists()
        assert not Path(exporter.STATE_FILE).exists()
        assert capture.total == 0

    # E2E-08 ──────────────────────────────────────────────────────────────────
    def test_slowlog_counter_reset_reexports_all_visible_entries(self, env, capture):
        """When the SLOWLOG is cleared and new IDs start below last_id, all visible
        entries are re-exported and the exporter continues from the new counter."""
        sl = FakeSlowlog()
        sl.add(49, command=b"GET b")
        sl.add(50, command=b"GET a")
        client = FakeEnterpriseClient(sl)

        ExporterRun().poll(client)          # last_id → 50, 2 rows written

        sl.reset()                          # SLOWLOG RESET
        sl.add(1, command=b"GET after x")
        sl.add(2, command=b"SET after y")
        ExporterRun().poll(client)          # max_id=2 < last_id=50 → reset detected

        rows = read_jsonl()
        assert len(rows) == 4                          # 2 original + 2 after reset
        assert rows[2]["id"] == 1 and rows[3]["id"] == 2
        assert read_state() == {"last_id": 2}          # counter continues from new base

    # E2E-09 ──────────────────────────────────────────────────────────────────
    def test_upload_failure_still_persists_jsonl_and_state(self, env, failing_capture):
        """If Log Analytics upload fails (silently), JSONL and state file are still written."""
        sl = FakeSlowlog()
        sl.add(1, command=b"GET key", duration_us=9_000)

        ExporterRun().poll(FakeEnterpriseClient(sl))

        assert len(read_jsonl()) == 1
        assert read_state() == {"last_id": 1}

    # E2E-10 ──────────────────────────────────────────────────────────────────
    def test_jsonl_row_has_correct_field_types_and_values(self, env, capture):
        """Every field in a JSONL row has the correct Python type and expected value."""
        sl = FakeSlowlog()
        sl.add(
            7,
            command=b"ZADD leaderboard 100 player1",
            duration_us=15_000,
            start_time=1_700_000_007,
        )
        ExporterRun().poll(FakeEnterpriseClient(sl))

        row = read_jsonl()[0]
        assert isinstance(row["id"],          int)
        assert isinstance(row["timestamp"],   str) and row["timestamp"].endswith("+00:00")
        assert isinstance(row["duration_us"], int) and row["duration_us"] == 15_000
        assert isinstance(row["duration_ms"], float)
        assert row["duration_ms"] == pytest.approx(15.0, rel=1e-3)
        assert row["command"]      == "ZADD leaderboard 100 player1"
        assert row["redis_host"]   == "test.redis.azure.net"
        assert row["cluster_name"] == "test-cluster"
        assert isinstance(row["exported_at"], str)
        assert "node" not in row               # enterprise mode has no node field

    # E2E-11 ──────────────────────────────────────────────────────────────────
    def test_log_analytics_payload_matches_jsonl_output(self, env, capture):
        """Fields written to JSONL and fields uploaded to Log Analytics are consistent."""
        sl = FakeSlowlog()
        sl.add(1, command=b"DEL c",   duration_us=2_000, start_time=1_700_000_001)
        sl.add(2, command=b"SET b 1", duration_us=6_000, start_time=1_700_000_002)
        sl.add(3, command=b"GET a",   duration_us=3_000, start_time=1_700_000_003)

        ExporterRun().poll(FakeEnterpriseClient(sl))

        rows     = read_jsonl()
        uploaded = capture.rows

        assert len(uploaded) == 3

        # Cross-check a few key fields between JSONL and Log Analytics body
        for jsonl_row, la_row in zip(rows, uploaded):
            assert la_row["SlowlogId"]    == jsonl_row["id"]
            assert la_row["Duration_us"]  == jsonl_row["duration_us"]
            assert la_row["Command"]      == jsonl_row["command"]
            assert la_row["ClusterName"]  == jsonl_row["cluster_name"]
            assert la_row["TimeGenerated"] == jsonl_row["timestamp"]

        # Verify all mandatory Log Analytics fields are present
        required_la_fields = (
            "TimeGenerated", "SlowlogId", "Duration_us", "Duration_ms",
            "Command", "RedisHost", "ClusterName", "Node", "ExportedAt",
        )
        for la_row in uploaded:
            for field in required_la_fields:
                assert field in la_row, f"Missing Log Analytics field: {field}"


# ─────────────────────────────────────────────────────────────────────────────
# OSS cluster mode E2E  (TC E2E-12 ~ E2E-17)
# ─────────────────────────────────────────────────────────────────────────────


class TestOssE2E:

    @pytest.fixture(autouse=True)
    def oss_mode(self, env, monkeypatch):
        # Depend on env so this fixture always runs after env, ensuring our
        # setattr wins even though both use the same monkeypatch instance.
        monkeypatch.setattr(exporter, "AMR_CLUSTER_POLICY", "oss")

    # E2E-12 ──────────────────────────────────────────────────────────────────
    def test_first_poll_collects_entries_from_all_shards(self, capture):
        """All primary nodes are queried and their entries are merged into one JSONL."""
        shard_a = FakeSlowlog()
        shard_a.add(1, command=b"GET a",          start_time=1_700_000_001)
        shard_a.add(2, command=b"HSET h f v",     start_time=1_700_000_002)
        shard_b = FakeSlowlog()
        shard_b.add(3, command=b"LPUSH list val", start_time=1_700_000_003)

        ExporterRun().poll(FakeOssClient({
            "shard-0:10000": shard_a,
            "shard-1:10000": shard_b,
        }))

        assert len(read_jsonl()) == 3
        assert capture.total == 3

    # E2E-13 ──────────────────────────────────────────────────────────────────
    def test_oss_entries_carry_node_tag_in_jsonl(self, capture):
        """Each JSONL row from an OSS cluster includes the originating shard 'node' field."""
        shard = FakeSlowlog()
        shard.add(1, command=b"GET x")

        ExporterRun().poll(FakeOssClient({"shard-0:10000": shard}))

        row = read_jsonl()[0]
        assert row["node"] == "shard-0:10000"

    # E2E-14 ──────────────────────────────────────────────────────────────────
    def test_oss_state_tracks_last_id_per_shard_independently(self, capture):
        """State file stores a separate last_id for every primary shard."""
        shard_a = FakeSlowlog()
        shard_a.add(5, command=b"GET a")
        shard_b = FakeSlowlog()
        shard_b.add(3, command=b"GET b")

        ExporterRun().poll(FakeOssClient({
            "shard-0:10000": shard_a,
            "shard-1:10000": shard_b,
        }))

        state = read_state()
        assert state["nodes"]["shard-0:10000"] == 5
        assert state["nodes"]["shard-1:10000"] == 3

    # E2E-15 ──────────────────────────────────────────────────────────────────
    def test_oss_incremental_poll_deduplicates_per_shard_after_restart(self, capture):
        """After restart, each shard's last_id is used independently for deduplication."""
        shard_a = FakeSlowlog()
        shard_a.add(1, command=b"GET a1")
        shard_a.add(2, command=b"GET a2")
        shard_b = FakeSlowlog()
        shard_b.add(1, command=b"GET b1")
        client = FakeOssClient({
            "shard-0:10000": shard_a,
            "shard-1:10000": shard_b,
        })

        ExporterRun().poll(client)          # 3 entries total

        shard_a.add(3, command=b"GET a3")   # 1 new on shard-a
        shard_b.add(2, command=b"GET b2")   # 2 new on shard-b
        shard_b.add(3, command=b"GET b3")
        ExporterRun().poll(client)          # restart — should add exactly 3 more

        assert len(read_jsonl()) == 6

    # E2E-16 ──────────────────────────────────────────────────────────────────
    def test_oss_failed_shard_does_not_prevent_other_shards_from_exporting(self, capture):
        """A shard that raises RedisError is skipped; healthy shards are still exported."""
        shard_b = FakeSlowlog()
        shard_b.add(1, command=b"GET ok1")
        shard_b.add(2, command=b"GET ok2")

        client = FakeOssClient({
            "shard-0:10000": FailingSlowlog(),   # always raises
            "shard-1:10000": shard_b,
        })
        ExporterRun().poll(client)

        rows  = read_jsonl()
        state = read_state()

        assert len(rows) == 2
        assert all(r["node"] == "shard-1:10000" for r in rows)
        assert "shard-1:10000" in state["nodes"]
        assert "shard-0:10000" not in state["nodes"]   # failed shard absent from state

    # E2E-17 ──────────────────────────────────────────────────────────────────
    def test_oss_merged_jsonl_rows_are_in_chronological_order(self, capture):
        """Entries from multiple shards are sorted by start_time in the output file."""
        shard_a = FakeSlowlog()
        shard_a.add(1, command=b"GET a", start_time=1_700_000_010)   # later
        shard_b = FakeSlowlog()
        shard_b.add(1, command=b"GET b", start_time=1_700_000_005)   # earlier

        ExporterRun().poll(FakeOssClient({
            "shard-0:10000": shard_a,
            "shard-1:10000": shard_b,
        }))

        timestamps = [r["timestamp"] for r in read_jsonl()]
        assert timestamps == sorted(timestamps)    # ISO 8601 UTC sorts lexicographically


# ─────────────────────────────────────────────────────────────────────────────
# StatefulSet multi-cluster config E2E  (TC E2E-18 ~ E2E-19)
# ─────────────────────────────────────────────────────────────────────────────


class TestMultiClusterE2E:

    def _write_clusters(self, tmp_path, clusters: list) -> str:
        f = tmp_path / "clusters.json"
        f.write_text(json.dumps(clusters))
        return str(f)

    def _apply_cluster_config(self, tmp_path, monkeypatch, pod_name: str, clusters: list):
        """Call _load_cluster_config() for the given pod and register all modified
        globals with monkeypatch so they are restored after the test."""
        cfg_file = self._write_clusters(tmp_path, clusters)
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE", cfg_file)
        monkeypatch.setattr(exporter, "POD_NAME", pod_name)
        # Register all globals that _load_cluster_config() may overwrite
        for attr in ("AMR_HOST", "AMR_PORT", "AMR_ACCESS_KEY",
                     "AMR_CLUSTER_POLICY", "SSL_VERIFY",
                     "CLUSTER_NAME", "POLL_INTERVAL", "SLOWLOG_BATCH_SIZE"):
            monkeypatch.setattr(exporter, attr, getattr(exporter, attr))
        exporter._load_cluster_config()

    # E2E-18 ──────────────────────────────────────────────────────────────────
    def test_pod0_exports_entries_tagged_with_its_cluster_name(self, env, capture, monkeypatch):
        """Pod-0 loads clusters[0] from clusters.json; JSONL rows carry that cluster's
        name and host, not the values from the other cluster entry."""
        clusters = [
            {"AMR_HOST": "prod-a.redis.azure.net", "AMR_ACCESS_KEY": "key-a",
             "AMR_CLUSTER_NAME": "prod-a", "AMR_CLUSTER_POLICY": "enterprise"},
            {"AMR_HOST": "prod-b.redis.azure.net", "AMR_ACCESS_KEY": "key-b",
             "AMR_CLUSTER_NAME": "prod-b", "AMR_CLUSTER_POLICY": "enterprise"},
        ]
        self._apply_cluster_config(env, monkeypatch, "amr-slowquery-exporter-0", clusters)

        assert exporter.AMR_HOST    == "prod-a.redis.azure.net"
        assert exporter.CLUSTER_NAME == "prod-a"

        sl = FakeSlowlog()
        sl.add(1, command=b"GET x")
        ExporterRun().poll(FakeEnterpriseClient(sl))

        row = read_jsonl()[0]
        assert row["cluster_name"] == "prod-a"
        assert row["redis_host"]   == "prod-a.redis.azure.net"

    # E2E-19 ──────────────────────────────────────────────────────────────────
    def test_pod1_exports_entries_tagged_with_its_cluster_name(self, env, capture, monkeypatch):
        """Pod-1 loads clusters[1]; JSONL rows carry that cluster's name and host."""
        clusters = [
            {"AMR_HOST": "prod-a.redis.azure.net", "AMR_ACCESS_KEY": "key-a",
             "AMR_CLUSTER_NAME": "prod-a", "AMR_CLUSTER_POLICY": "enterprise"},
            {"AMR_HOST": "prod-b.redis.azure.net", "AMR_ACCESS_KEY": "key-b",
             "AMR_CLUSTER_NAME": "prod-b", "AMR_CLUSTER_POLICY": "enterprise"},
        ]
        self._apply_cluster_config(env, monkeypatch, "amr-slowquery-exporter-1", clusters)

        assert exporter.AMR_HOST    == "prod-b.redis.azure.net"
        assert exporter.CLUSTER_NAME == "prod-b"

        sl = FakeSlowlog()
        sl.add(1, command=b"GET y")
        ExporterRun().poll(FakeEnterpriseClient(sl))

        row = read_jsonl()[0]
        assert row["cluster_name"] == "prod-b"
        assert row["redis_host"]   == "prod-b.redis.azure.net"
