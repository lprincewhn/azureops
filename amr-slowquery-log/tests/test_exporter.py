"""Functional tests for exporter.py

Test areas
----------
TC-01 ~ TC-06   State persistence      (load_state / save_state)
TC-07 ~ TC-13   Deduplication logic    (_filter_new)
TC-14 ~ TC-20   Entry formatting       (format_entry)
TC-21 ~ TC-23   JSONL output           (append_to_jsonl)
TC-24 ~ TC-27   Enterprise-mode fetch  (_fetch_enterprise)
TC-28 ~ TC-33   OSS cluster-mode fetch (_fetch_oss)
TC-34 ~ TC-36   Azure Monitor upload   (send_to_log_analytics)
TC-37 ~ TC-43   Multi-cluster config   (_load_cluster_config)
TC-44 ~ TC-49   Config validation      (_validate_config)
"""

import json
from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest
import redis

import exporter

# ── Shared helpers ────────────────────────────────────────────────────────────

EXPORTED_AT = "2024-01-15T12:00:00+00:00"


def make_raw_entry(id, start_time=1_700_000_000, duration=12_345, command=b"GET foo"):
    """Minimal SLOWLOG entry as returned by redis-py."""
    return {
        "id": id,
        "start_time": start_time,
        "duration": duration,
        "command": command,
        "client_address": b"127.0.0.1:54321",
        "client_name": b"",
    }


def make_formatted_entry(i=1):
    return {
        "id": i,
        "timestamp": "2024-01-15T00:00:00+00:00",
        "duration_us": 10_000 * i,
        "duration_ms": 10.0 * i,
        "command": f"GET key{i}",
        "redis_host": "cache.example.com",
        "cluster_name": "prod",
        "exported_at": EXPORTED_AT,
    }


# ── TC-01 ~ TC-06  State persistence ─────────────────────────────────────────


class TestStatePersistence:

    # TC-01
    def test_load_missing_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter, "STATE_FILE", str(tmp_path / "no_such.json"))
        assert exporter.load_state() == {}

    # TC-02
    def test_load_invalid_json_returns_empty(self, tmp_path, monkeypatch):
        f = tmp_path / "state.json"
        f.write_text("not json")
        monkeypatch.setattr(exporter, "STATE_FILE", str(f))
        assert exporter.load_state() == {}

    # TC-03
    def test_load_valid_enterprise_state(self, tmp_path, monkeypatch):
        f = tmp_path / "state.json"
        f.write_text(json.dumps({"last_id": 42}))
        monkeypatch.setattr(exporter, "STATE_FILE", str(f))
        assert exporter.load_state() == {"last_id": 42}

    # TC-04
    def test_load_valid_oss_state(self, tmp_path, monkeypatch):
        state = {"nodes": {"host-a:10000": 10, "host-b:10000": 5}}
        f = tmp_path / "state.json"
        f.write_text(json.dumps(state))
        monkeypatch.setattr(exporter, "STATE_FILE", str(f))
        assert exporter.load_state() == state

    # TC-05
    def test_save_writes_valid_json(self, tmp_path, monkeypatch):
        f = tmp_path / "state.json"
        monkeypatch.setattr(exporter, "STATE_FILE", str(f))
        exporter.save_state({"last_id": 99})
        assert json.loads(f.read_text()) == {"last_id": 99}

    # TC-06
    def test_save_then_load_roundtrip(self, tmp_path, monkeypatch):
        f = tmp_path / "state.json"
        monkeypatch.setattr(exporter, "STATE_FILE", str(f))
        state = {"nodes": {"host:10000": 7}}
        exporter.save_state(state)
        assert exporter.load_state() == state


# ── TC-07 ~ TC-13  Deduplication logic ───────────────────────────────────────


class TestFilterNew:

    # TC-07
    def test_empty_raw_returns_unchanged_last_id(self):
        new, last_id = exporter._filter_new([], 10, "node:port")
        assert new == []
        assert last_id == 10

    # TC-08
    def test_all_entries_already_exported(self):
        raw = [make_raw_entry(5), make_raw_entry(3), make_raw_entry(1)]
        new, last_id = exporter._filter_new(raw, 5, "node:port")
        assert new == []
        assert last_id == 5

    # TC-09
    def test_first_run_exports_all_oldest_first(self):
        raw = [make_raw_entry(3), make_raw_entry(2), make_raw_entry(1)]
        new, last_id = exporter._filter_new(raw, -1, "node:port")
        assert [e["id"] for e in new] == [1, 2, 3]
        assert last_id == 3

    # TC-10
    def test_incremental_export_only_new_entries(self):
        raw = [make_raw_entry(5), make_raw_entry(4), make_raw_entry(3), make_raw_entry(2)]
        new, last_id = exporter._filter_new(raw, 3, "node:port")
        assert [e["id"] for e in new] == [4, 5]
        assert last_id == 5

    # TC-11
    def test_counter_reset_reexports_all_visible(self):
        """When max_id < last_id the SLOWLOG was cleared; re-export all visible entries."""
        raw = [make_raw_entry(3), make_raw_entry(2), make_raw_entry(1)]
        new, last_id = exporter._filter_new(raw, 100, "node:port")
        assert [e["id"] for e in new] == [1, 2, 3]
        assert last_id == 3

    # TC-12
    def test_no_reset_when_last_id_not_positive(self):
        """Reset guard requires last_id > 0; last_id=0 must not trigger it."""
        raw = [make_raw_entry(2), make_raw_entry(1)]
        new, last_id = exporter._filter_new(raw, 0, "node:port")
        # Both entries have id > 0, so both are returned
        assert len(new) == 2
        assert last_id == 2

    # TC-13
    def test_output_is_oldest_first(self):
        """Redis returns SLOWLOG newest-first; _filter_new must reverse the order."""
        raw = [
            make_raw_entry(id=5, start_time=1_700_000_005),
            make_raw_entry(id=4, start_time=1_700_000_004),
            make_raw_entry(id=3, start_time=1_700_000_003),
        ]
        new, _ = exporter._filter_new(raw, 2, "node:port")
        assert [e["id"] for e in new] == [3, 4, 5]


# ── TC-14 ~ TC-20  Entry formatting ──────────────────────────────────────────


class TestFormatEntry:

    def _entry(self, **overrides):
        base = make_raw_entry(id=7, start_time=1_700_000_000, duration=25_000)
        base.update(overrides)
        return base

    def _patch(self, monkeypatch, host="cache.example.com", cluster="prod"):
        monkeypatch.setattr(exporter, "AMR_HOST", host)
        monkeypatch.setattr(exporter, "CLUSTER_NAME", cluster)

    # TC-14
    def test_all_required_fields_present(self, monkeypatch):
        self._patch(monkeypatch)
        r = exporter.format_entry(self._entry(), EXPORTED_AT)
        for field in ("id", "timestamp", "duration_us", "duration_ms",
                      "command", "redis_host", "cluster_name", "exported_at"):
            assert field in r, f"Missing field: {field}"

    # TC-15
    def test_duration_conversion_microseconds_to_milliseconds(self, monkeypatch):
        self._patch(monkeypatch)
        r = exporter.format_entry(self._entry(duration=12_345), EXPORTED_AT)
        assert r["duration_us"] == 12_345
        assert r["duration_ms"] == pytest.approx(12.345, rel=1e-3)

    # TC-16
    def test_timestamp_is_utc_iso8601(self, monkeypatch):
        self._patch(monkeypatch)
        r = exporter.format_entry(self._entry(start_time=0), EXPORTED_AT)
        assert r["timestamp"] == "1970-01-01T00:00:00+00:00"

    # TC-17
    def test_bytes_command_decoded_to_string(self, monkeypatch):
        self._patch(monkeypatch)
        r = exporter.format_entry(self._entry(command=b"SET key value"), EXPORTED_AT)
        assert r["command"] == "SET key value"

    # TC-18
    def test_string_command_passed_through(self, monkeypatch):
        self._patch(monkeypatch)
        r = exporter.format_entry(self._entry(command="HGETALL hash"), EXPORTED_AT)
        assert r["command"] == "HGETALL hash"

    # TC-19
    def test_node_field_absent_in_enterprise_mode(self, monkeypatch):
        self._patch(monkeypatch)
        r = exporter.format_entry(self._entry(), EXPORTED_AT)
        assert "node" not in r

    # TC-20
    def test_node_field_present_in_oss_mode(self, monkeypatch):
        self._patch(monkeypatch)
        entry = self._entry()
        entry["_node"] = "shard-0:10000"
        r = exporter.format_entry(entry, EXPORTED_AT)
        assert r["node"] == "shard-0:10000"


# ── TC-21 ~ TC-23  JSONL output ──────────────────────────────────────────────


class TestAppendToJsonl:

    # TC-21
    def test_writes_one_line_per_entry(self, tmp_path, monkeypatch):
        out = tmp_path / "out.jsonl"
        monkeypatch.setattr(exporter, "OUTPUT_FILE", str(out))
        exporter.append_to_jsonl([{"id": 1, "cmd": "GET"}, {"id": 2, "cmd": "SET"}])
        lines = out.read_text().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"id": 1, "cmd": "GET"}
        assert json.loads(lines[1]) == {"id": 2, "cmd": "SET"}

    # TC-22
    def test_appends_without_overwriting_existing_content(self, tmp_path, monkeypatch):
        out = tmp_path / "out.jsonl"
        out.write_text('{"id": 0}\n')
        monkeypatch.setattr(exporter, "OUTPUT_FILE", str(out))
        exporter.append_to_jsonl([{"id": 1}])
        lines = out.read_text().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"id": 0}
        assert json.loads(lines[1]) == {"id": 1}

    # TC-23
    def test_empty_list_writes_nothing(self, tmp_path, monkeypatch):
        out = tmp_path / "out.jsonl"
        monkeypatch.setattr(exporter, "OUTPUT_FILE", str(out))
        exporter.append_to_jsonl([])
        assert not out.exists() or out.read_text() == ""


# ── TC-24 ~ TC-27  Enterprise-mode fetch ─────────────────────────────────────


class TestFetchEnterprise:

    def _client(self, entries):
        c = MagicMock()
        c.slowlog_get.return_value = entries
        return c

    # TC-24
    def test_new_entries_returned_and_state_updated(self, monkeypatch):
        monkeypatch.setattr(exporter, "AMR_HOST", "cache.example.com")
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        raw = [make_raw_entry(3), make_raw_entry(2), make_raw_entry(1)]
        entries, state = exporter._fetch_enterprise(self._client(raw), {"last_id": -1})
        assert len(entries) == 3
        assert state == {"last_id": 3}

    # TC-25
    def test_no_new_entries_when_already_up_to_date(self, monkeypatch):
        monkeypatch.setattr(exporter, "AMR_HOST", "cache.example.com")
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        raw = [make_raw_entry(5), make_raw_entry(4)]
        entries, state = exporter._fetch_enterprise(self._client(raw), {"last_id": 5})
        assert entries == []
        assert state == {"last_id": 5}

    # TC-26
    def test_redis_error_returns_empty_and_preserves_state(self, monkeypatch):
        monkeypatch.setattr(exporter, "AMR_HOST", "cache.example.com")
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        c = MagicMock()
        c.slowlog_get.side_effect = redis.RedisError("connection refused")
        original = {"last_id": 10}
        entries, state = exporter._fetch_enterprise(c, original)
        assert entries == []
        assert state is original

    # TC-27
    def test_calls_slowlog_get_with_configured_batch_size(self, monkeypatch):
        monkeypatch.setattr(exporter, "AMR_HOST", "cache.example.com")
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 64)
        c = self._client([])
        exporter._fetch_enterprise(c, {})
        c.slowlog_get.assert_called_once_with(64)


# ── TC-28 ~ TC-33  OSS cluster-mode fetch ────────────────────────────────────


class TestFetchOss:

    def _node(self, host, port=10000):
        n = MagicMock()
        n.host = host
        n.port = port
        return n

    def _conn(self, entries):
        c = MagicMock()
        c.slowlog_get.return_value = entries
        return c

    # TC-28
    def test_queries_all_primary_nodes(self, monkeypatch):
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        client = MagicMock()
        client.get_primaries.return_value = [self._node("a"), self._node("b")]
        client.get_redis_connection.side_effect = [
            self._conn([make_raw_entry(1)]),
            self._conn([make_raw_entry(2)]),
        ]
        entries, _ = exporter._fetch_oss(client, {})
        assert len(entries) == 2
        assert client.get_redis_connection.call_count == 2

    # TC-29
    def test_per_node_state_tracked_independently(self, monkeypatch):
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        client = MagicMock()
        client.get_primaries.return_value = [self._node("host-a"), self._node("host-b")]
        client.get_redis_connection.side_effect = [
            self._conn([make_raw_entry(5), make_raw_entry(3)]),
            self._conn([make_raw_entry(7), make_raw_entry(6)]),
        ]
        _, state = exporter._fetch_oss(client, {})
        assert state["nodes"]["host-a:10000"] == 5
        assert state["nodes"]["host-b:10000"] == 7

    # TC-30
    def test_failed_node_skipped_others_still_queried(self, monkeypatch):
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        client = MagicMock()
        client.get_primaries.return_value = [self._node("host-a"), self._node("host-b")]
        conn_err = MagicMock()
        conn_err.slowlog_get.side_effect = redis.RedisError("node down")
        client.get_redis_connection.side_effect = [
            conn_err,
            self._conn([make_raw_entry(3), make_raw_entry(2)]),
        ]
        entries, state = exporter._fetch_oss(client, {})
        assert len(entries) == 2
        assert "host-b:10000" in state["nodes"]
        assert "host-a:10000" not in state["nodes"]

    # TC-31
    def test_entries_tagged_with_originating_node(self, monkeypatch):
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        client = MagicMock()
        client.get_primaries.return_value = [self._node("shard-0")]
        client.get_redis_connection.return_value = self._conn([make_raw_entry(1)])
        entries, _ = exporter._fetch_oss(client, {})
        assert entries[0]["_node"] == "shard-0:10000"

    # TC-32
    def test_merged_results_sorted_by_start_time(self, monkeypatch):
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        client = MagicMock()
        client.get_primaries.return_value = [self._node("host-a"), self._node("host-b")]
        client.get_redis_connection.side_effect = [
            self._conn([make_raw_entry(2, start_time=1_700_000_010)]),
            self._conn([make_raw_entry(1, start_time=1_700_000_005)]),
        ]
        entries, _ = exporter._fetch_oss(client, {})
        times = [e["start_time"] for e in entries]
        assert times == sorted(times)

    # TC-33
    def test_get_primaries_error_returns_empty_and_preserves_state(self, monkeypatch):
        monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
        client = MagicMock()
        client.get_primaries.side_effect = redis.RedisError("cluster unavailable")
        original = {"nodes": {"host:10000": 5}}
        entries, state = exporter._fetch_oss(client, original)
        assert entries == []
        assert state is original


# ── TC-34 ~ TC-36  Azure Monitor upload ──────────────────────────────────────


class TestSendToLogAnalytics:

    # TC-34
    def test_upload_called_once_per_batch(self, monkeypatch):
        monkeypatch.setattr(exporter, "DCR_RULE_ID", "dcr-123")
        mock_client = MagicMock()
        with patch.object(exporter, "_get_logs_client", return_value=mock_client):
            exporter.send_to_log_analytics([make_formatted_entry(1), make_formatted_entry(2)])
        mock_client.upload.assert_called_once()

    # TC-35
    def test_upload_body_contains_all_required_log_analytics_fields(self, monkeypatch):
        monkeypatch.setattr(exporter, "DCR_RULE_ID", "dcr-123")
        captured = {}

        def fake_upload(rule_id, stream_name, logs):
            captured["logs"] = logs

        mock_client = MagicMock()
        mock_client.upload.side_effect = fake_upload
        with patch.object(exporter, "_get_logs_client", return_value=mock_client):
            exporter.send_to_log_analytics([make_formatted_entry(1), make_formatted_entry(2)])

        assert len(captured["logs"]) == 2
        required = ("TimeGenerated", "SlowlogId", "Duration_us", "Duration_ms",
                    "Command", "RedisHost", "ClusterName", "Node", "ExportedAt")
        for row in captured["logs"]:
            for field in required:
                assert field in row, f"Missing Log Analytics field: {field}"

    # TC-36
    def test_upload_failure_does_not_propagate_exception(self, monkeypatch):
        monkeypatch.setattr(exporter, "DCR_RULE_ID", "dcr-123")
        mock_client = MagicMock()
        mock_client.upload.side_effect = Exception("network error")
        with patch.object(exporter, "_get_logs_client", return_value=mock_client):
            exporter.send_to_log_analytics([make_formatted_entry(1)])  # must not raise


# ── TC-37 ~ TC-43  Multi-cluster config loading ───────────────────────────────


class TestLoadClusterConfig:

    def _write_clusters(self, tmp_path, clusters):
        f = tmp_path / "clusters.json"
        f.write_text(json.dumps(clusters))
        return str(f)

    def _save_globals(self, monkeypatch):
        """Register current global values with monkeypatch so they are restored after the test."""
        for attr in ("AMR_HOST", "AMR_PORT", "AMR_ACCESS_KEY", "AMR_CLUSTER_POLICY",
                     "SSL_VERIFY", "CLUSTER_NAME", "POLL_INTERVAL", "SLOWLOG_BATCH_SIZE"):
            monkeypatch.setattr(exporter, attr, getattr(exporter, attr))

    # TC-37
    def test_no_config_file_is_noop(self, monkeypatch):
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE", "")
        original_host = exporter.AMR_HOST
        exporter._load_cluster_config()
        assert exporter.AMR_HOST == original_host

    # TC-38
    def test_pod0_loads_first_cluster_entry(self, tmp_path, monkeypatch):
        clusters = [
            {"AMR_HOST": "cluster-a.redis.azure.net", "AMR_ACCESS_KEY": "key-a",
             "AMR_CLUSTER_NAME": "prod-a"},
            {"AMR_HOST": "cluster-b.redis.azure.net", "AMR_ACCESS_KEY": "key-b",
             "AMR_CLUSTER_NAME": "prod-b"},
        ]
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE", self._write_clusters(tmp_path, clusters))
        monkeypatch.setattr(exporter, "POD_NAME", "amr-slowquery-exporter-0")
        self._save_globals(monkeypatch)

        exporter._load_cluster_config()

        assert exporter.AMR_HOST == "cluster-a.redis.azure.net"
        assert exporter.AMR_ACCESS_KEY == "key-a"
        assert exporter.CLUSTER_NAME == "prod-a"

    # TC-39
    def test_pod1_loads_second_cluster_entry(self, tmp_path, monkeypatch):
        clusters = [
            {"AMR_HOST": "cluster-a.redis.azure.net", "AMR_ACCESS_KEY": "key-a",
             "AMR_CLUSTER_NAME": "prod-a"},
            {"AMR_HOST": "cluster-b.redis.azure.net", "AMR_ACCESS_KEY": "key-b",
             "AMR_CLUSTER_NAME": "prod-b"},
        ]
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE", self._write_clusters(tmp_path, clusters))
        monkeypatch.setattr(exporter, "POD_NAME", "amr-slowquery-exporter-1")
        self._save_globals(monkeypatch)

        exporter._load_cluster_config()

        assert exporter.AMR_HOST == "cluster-b.redis.azure.net"
        assert exporter.CLUSTER_NAME == "prod-b"

    # TC-40
    def test_cluster_config_overrides_poll_interval(self, tmp_path, monkeypatch):
        clusters = [{"AMR_HOST": "h", "AMR_ACCESS_KEY": "k",
                     "AMR_CLUSTER_NAME": "test", "POLL_INTERVAL_SECONDS": 30}]
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE", self._write_clusters(tmp_path, clusters))
        monkeypatch.setattr(exporter, "POD_NAME", "amr-slowquery-exporter-0")
        self._save_globals(monkeypatch)

        exporter._load_cluster_config()

        assert exporter.POLL_INTERVAL == 30

    # TC-41
    def test_ordinal_out_of_range_causes_exit(self, tmp_path, monkeypatch):
        clusters = [{"AMR_HOST": "cluster-a", "AMR_ACCESS_KEY": "key"}]
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE", self._write_clusters(tmp_path, clusters))
        monkeypatch.setattr(exporter, "POD_NAME", "amr-slowquery-exporter-5")
        with pytest.raises(SystemExit):
            exporter._load_cluster_config()

    # TC-42
    def test_unparseable_pod_name_causes_exit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE",
                            self._write_clusters(tmp_path, [{"AMR_HOST": "h", "AMR_ACCESS_KEY": "k"}]))
        monkeypatch.setattr(exporter, "POD_NAME", "amr-exporter-no-ordinal")
        with pytest.raises(SystemExit):
            exporter._load_cluster_config()

    # TC-43
    def test_empty_pod_name_causes_exit(self, tmp_path, monkeypatch):
        monkeypatch.setattr(exporter, "CLUSTERS_CONFIG_FILE",
                            self._write_clusters(tmp_path, [{"AMR_HOST": "h", "AMR_ACCESS_KEY": "k"}]))
        monkeypatch.setattr(exporter, "POD_NAME", "")
        with pytest.raises(SystemExit):
            exporter._load_cluster_config()


# ── TC-44 ~ TC-49  Config validation ─────────────────────────────────────────


class TestValidateConfig:

    def _valid(self, monkeypatch):
        monkeypatch.setattr(exporter, "AMR_HOST", "cache.example.com")
        monkeypatch.setattr(exporter, "AMR_ACCESS_KEY", "secret-key")
        monkeypatch.setattr(exporter, "USE_ENTRA", False)
        monkeypatch.setattr(exporter, "DCE_ENDPOINT", "https://dce.example.com")
        monkeypatch.setattr(exporter, "DCR_RULE_ID", "dcr-abc123")

    # TC-44
    def test_valid_config_passes_without_error(self, monkeypatch):
        self._valid(monkeypatch)
        exporter._validate_config()  # must not raise

    # TC-45
    def test_missing_amr_host_causes_exit(self, monkeypatch):
        self._valid(monkeypatch)
        monkeypatch.setattr(exporter, "AMR_HOST", "")
        with pytest.raises(SystemExit):
            exporter._validate_config()

    # TC-46
    def test_missing_access_key_without_entra_causes_exit(self, monkeypatch):
        self._valid(monkeypatch)
        monkeypatch.setattr(exporter, "AMR_ACCESS_KEY", "")
        monkeypatch.setattr(exporter, "USE_ENTRA", False)
        with pytest.raises(SystemExit):
            exporter._validate_config()

    # TC-47
    def test_entra_auth_without_access_key_is_valid(self, monkeypatch):
        self._valid(monkeypatch)
        monkeypatch.setattr(exporter, "AMR_ACCESS_KEY", "")
        monkeypatch.setattr(exporter, "USE_ENTRA", True)
        exporter._validate_config()  # must not raise

    # TC-48
    def test_missing_dce_endpoint_causes_exit(self, monkeypatch):
        self._valid(monkeypatch)
        monkeypatch.setattr(exporter, "DCE_ENDPOINT", "")
        with pytest.raises(SystemExit):
            exporter._validate_config()

    # TC-49
    def test_missing_dcr_rule_id_causes_exit(self, monkeypatch):
        self._valid(monkeypatch)
        monkeypatch.setattr(exporter, "DCR_RULE_ID", "")
        with pytest.raises(SystemExit):
            exporter._validate_config()
