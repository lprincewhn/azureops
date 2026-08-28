"""Failure-path regression tests for the slow query exporter.

Every regression found in this component so far was caught by a hand-written probe
during review, never by the test suite — because the suite could not express "the
upload failed".  These tests close that gap.  They use the shared harness in
`failure_injection.py` and drive only production code paths (`run_once`, `main`).

Tests marked xfail describe behaviour the exporter does not yet have.  They are the
red-light list for the follow-up fix task: making them pass IS the acceptance
criterion.  The reason string on each names the blocker it pins down.

    B1  `except Exception` in run_once swallows the _Stop sentinel that main()'s
        SIGTERM handler raises, so graceful shutdown never happens and batches the
        service already accepted get re-uploaded after restart.
    B2  Content fingerprints collide on deterministic periodic workloads, so a whole
        generation of SLOWLOG rows can be judged "already backed up" and never reach
        the local JSONL.
    B3  A pending-upload state file written before fingerprints existed has no
        `fingerprints` key, and `not fingerprints` reads that as a reset — so the
        first poll after upgrade re-appends an already-backed-up batch.

Groups
------
FP-01 ~ FP-06   B1  signal handling / exception scope in the poll loop
FP-07 ~ FP-10   B2  fingerprint collision (enterprise + OSS)
FP-11 ~ FP-12   B3  legacy state upgrade (enterprise + OSS)
FP-13 ~ FP-20   round-2 behaviour that must not regress again
FP-21           state file growth bound
"""

import json
from pathlib import Path

import pytest
from azure.core.exceptions import AzureError, SerializationError

import exporter
from tests.failure_injection import (
    AmrEnterpriseClient,
    AmrOssClient,
    AmrSlowlog,
    InjectableUpload,
    MainLoopDriver,
    raise_sigterm_once,
)

SHARD_0 = "shard-0:10000"
SHARD_1 = "shard-1:10000"


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Isolate exporter file paths and config for one test."""
    monkeypatch.setattr(exporter, "AMR_HOST", "test.redis.azure.net")
    monkeypatch.setattr(exporter, "AMR_PORT", 10000)
    monkeypatch.setattr(exporter, "CLUSTER_NAME", "test-cluster")
    monkeypatch.setattr(exporter, "SLOWLOG_BATCH_SIZE", 128)
    monkeypatch.setattr(exporter, "OUTPUT_FILE", str(tmp_path / "slowquery.jsonl"))
    monkeypatch.setattr(exporter, "STATE_FILE", str(tmp_path / ".state.json"))
    monkeypatch.setattr(exporter, "DCR_RULE_ID", "dcr-test-rule")
    return tmp_path


@pytest.fixture
def enterprise(env, monkeypatch):
    monkeypatch.setattr(exporter, "AMR_CLUSTER_POLICY", "enterprise")
    return env


@pytest.fixture
def oss(env, monkeypatch):
    monkeypatch.setattr(exporter, "AMR_CLUSTER_POLICY", "oss")
    return env


@pytest.fixture
def upload():
    """Install a programmable Log Analytics client via the existing seam."""
    injector = InjectableUpload()
    exporter._logs_client = injector
    yield injector
    exporter._logs_client = None


# ── Helpers ───────────────────────────────────────────────────────────────────


def read_jsonl() -> list[dict]:
    path = Path(exporter.OUTPUT_FILE)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def jsonl_commands() -> list[str]:
    return [row["command"] for row in read_jsonl()]


def read_state() -> dict:
    path = Path(exporter.STATE_FILE)
    return json.loads(path.read_text()) if path.exists() else {}


def poll(client, state: dict) -> tuple[list[dict], dict]:
    """One poll through the production entry point."""
    return exporter.run_once(client, state)


def poll_from_disk(client) -> dict:
    """One poll that loads state from disk first — models a process restart."""
    _, state = exporter.run_once(client, exporter.load_state())
    return state


def seed_backed_up_jsonl(commands: list[str], node: str | None = None) -> None:
    """Pre-populate the JSONL as an earlier exporter version would have left it."""
    with open(exporter.OUTPUT_FILE, "w", encoding="utf-8") as handle:
        for index, command in enumerate(commands, start=1):
            row = {"id": index, "command": command, "duration_us": 5_000}
            if node:
                row["node"] = node
            handle.write(json.dumps(row) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# B1 — signal handling and exception scope in the poll loop  (FP-01 ~ FP-06)
# ─────────────────────────────────────────────────────────────────────────────


class TestSigtermDuringUpload:
    """The upload is the slowest, most network-bound part of a poll, so it is where
    SIGTERM most often lands during a rolling update.  main() raises _Stop from its
    handler and relies on `except _Stop` to shut down; run_once's `except Exception`
    sits in between."""

    # FP-01 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B1: _Stop subclasses Exception, so run_once's `except Exception` "
               "swallows the SIGTERM sentinel and main() never exits",
        strict=True,
    )
    def test_fp01_sigterm_during_upload_exits_the_process(self, enterprise, upload, monkeypatch):
        """SIGTERM delivered inside the upload window must terminate the poll loop."""
        slowlog = AmrSlowlog()
        for entry_id in range(1, 4):
            slowlog.add(entry_id, command=b"GET k%d" % entry_id)
        upload.on_upload(raise_sigterm_once())

        driver = MainLoopDriver(monkeypatch, AmrEnterpriseClient(slowlog), max_polls=20)
        result = driver.run()

        assert result.returned_gracefully, (
            f"main() did not exit after SIGTERM; it kept polling "
            f"({result.polls} polls executed)"
        )
        assert not result.budget_exhausted

    # FP-02 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B1: the swallowed _Stop leaves the loop spinning until SIGKILL "
               "instead of stopping after the interrupted poll",
        strict=True,
    )
    def test_fp02_sigterm_stops_polling_immediately(self, enterprise, upload, monkeypatch):
        """No further poll may start after the signal — the container is shutting down."""
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET k")
        upload.on_upload(raise_sigterm_once())

        result = MainLoopDriver(
            monkeypatch, AmrEnterpriseClient(slowlog), max_polls=20
        ).run()

        assert result.polls <= 1, f"kept polling after SIGTERM: {result.polls} polls"

    # FP-03 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B1: main()'s `except _Stop: save_state(state)` never runs because "
               "run_once already consumed the _Stop",
        strict=True,
    )
    def test_fp03_save_on_exit_runs_on_sigterm(self, enterprise, upload, monkeypatch):
        """main()'s shutdown handler must persist state before the process dies."""
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET k")
        upload.on_upload(raise_sigterm_once())

        result = MainLoopDriver(
            monkeypatch, AmrEnterpriseClient(slowlog), max_polls=20
        ).run()

        assert result.saved_on_exit, "save_state was never called from main()'s _Stop handler"

    # FP-04 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B1: a batch the service already accepted is recorded as failed, so "
               "the restart re-uploads it and Log Analytics gets duplicate rows",
        strict=True,
    )
    def test_fp04_accepted_batch_is_not_re_uploaded_after_sigterm(
        self, enterprise, upload, monkeypatch
    ):
        """A batch Log Analytics already ingested must never be sent twice.

        The hook fires *after* the service keeps the rows, so this is strictly the
        "server said yes, client never heard it" case.
        """
        slowlog = AmrSlowlog()
        for entry_id in range(1, 4):
            slowlog.add(entry_id, command=b"GET k%d" % entry_id)
        upload.on_upload(raise_sigterm_once(), when="after_accept")

        MainLoopDriver(monkeypatch, AmrEnterpriseClient(slowlog), max_polls=20).run()

        accepted = upload.accepted_ids
        assert accepted == sorted(set(accepted)), (
            f"rows ingested more than once by Log Analytics: {accepted}"
        )

    # FP-05 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B1: `except Exception` turns programming errors into an indefinite "
               "silent retry loop instead of surfacing them",
        strict=True,
    )
    @pytest.mark.parametrize(
        "failure",
        [
            KeyError("timestamp"),
            AttributeError("'dict' object has no attribute 'upload'"),
            TypeError("unsupported operand type"),
        ],
        ids=["KeyError", "AttributeError", "TypeError"],
    )
    def test_fp05_programming_errors_are_not_retried_forever(
        self, enterprise, upload, monkeypatch, failure
    ):
        """A bug in the exporter must not masquerade as a transient upload failure.

        Retrying a KeyError every POLL_INTERVAL forever produces a pod that looks
        healthy, logs an error line, and exports nothing.
        """
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET k")
        upload.fail_always(failure)

        result = MainLoopDriver(
            monkeypatch, AmrEnterpriseClient(slowlog), max_polls=15
        ).run()

        assert not result.budget_exhausted, (
            f"{type(failure).__name__} was swallowed and retried "
            f"{result.polls} times without surfacing"
        )
        assert isinstance(result.escaped, type(failure)), (
            f"expected {type(failure).__name__} to crash the process, "
            f"got {result.escaped!r}"
        )

    # FP-06 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B1: run_once's `except Exception` catches the _Stop sentinel instead "
               "of letting it reach main()'s shutdown handler",
        strict=True,
    )
    def test_fp06_stop_sentinel_propagates_out_of_run_once(self, enterprise, upload):
        """The narrow unit form of B1, independent of signal delivery timing.

        Whatever shape the fix takes — _Stop no longer subclassing Exception, or an
        explicit re-raise ahead of `except Exception` — a _Stop raised inside the
        upload must leave run_once.
        """
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET k")
        upload.fail_always(exporter._Stop())

        with pytest.raises(exporter._Stop):
            poll(AmrEnterpriseClient(slowlog), {})


# ─────────────────────────────────────────────────────────────────────────────
# B2 — fingerprint collision on deterministic workloads  (FP-07 ~ FP-10)
# ─────────────────────────────────────────────────────────────────────────────


class TestFingerprintCollision:
    """`_jsonl_append_indexes` decides "was there a reset?" by comparing content
    fingerprints of the overlapping ID range.  On Azure Managed Redis the available
    entropy is thin: start_time is second-granular, duration is an integer, and under
    the default Enterprise cluster policy redis-py leaves client_address / client_name
    absent while injecting a `complexity` field the fingerprint ignores.  A periodic
    task issuing the same slow command therefore produces byte-identical rows across
    generations."""

    # FP-07 ───────────────────────────────────────────────────────────────────
    def test_fp07_identical_amr_rows_across_generations_collide(self):
        """Document the entropy shortfall the collision rests on."""
        first = AmrSlowlog()
        first.add_burst([1], command=b"KEYS *", complexity=b"N=1")
        second = AmrSlowlog()
        second.add_burst([1], command=b"KEYS *", complexity=b"N=9")

        old_row = first.slowlog_get(1)[0]
        new_row = second.slowlog_get(1)[0]

        assert "client_address" not in old_row, (
            "AMR Enterprise SLOWLOG replies omit client_address; the fixture must "
            "not invent entropy the exporter will not have in production"
        )
        assert old_row["complexity"] != new_row["complexity"]
        assert exporter._entry_fingerprint(old_row) == exporter._entry_fingerprint(new_row), (
            "two rows from different generations must be shown to be indistinguishable "
            "for this scenario to be the real one"
        )

    # FP-08 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B2: every overlapping row collides, the reset goes undetected, and "
               "the whole new generation is skipped as already-backed-up",
        strict=True,
    )
    def test_fp08_enterprise_collision_keeps_new_generation_in_jsonl(
        self, enterprise, upload
    ):
        """A reset after a Log Analytics outage must not silently drop rows.

        Sequence: LA is down long enough for the backup cursor to climb past the new
        generation's IDs, then SLOWLOG resets and the same deterministic workload
        replays.  Every row that reaches Log Analytics must also reach the JSONL.
        """
        slowlog = AmrSlowlog()
        upload.fail_always(AzureError("Log Analytics unavailable"))

        state: dict = {}
        for generation in range(4):
            start = generation * 10 + 1
            slowlog.add_burst(range(start, start + 10), command=b"KEYS *", complexity=b"N=1")
            _, state = poll(AmrEnterpriseClient(slowlog), state)

        rows_before_reset = len(read_jsonl())
        assert read_state()["_jsonl"]["last_id"] == 40, "backlog must have raised the cursor"

        slowlog.reset()
        slowlog.add_burst(range(1, 41), command=b"KEYS *", complexity=b"N=1")
        upload.succeed()
        poll(AmrEnterpriseClient(slowlog), state)

        appended = len(read_jsonl()) - rows_before_reset
        assert appended == len(upload.accepted_rows), (
            f"Log Analytics received {len(upload.accepted_rows)} rows but only "
            f"{appended} reached the local JSONL"
        )

    # FP-09 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B2: the small-scale collision drops exactly the rows whose IDs the "
               "previous generation already used",
        strict=True,
    )
    def test_fp09_enterprise_collision_small_batch_loses_no_rows(self, enterprise, upload):
        """Three identical commands, reset, five identical commands → 8 JSONL rows."""
        slowlog = AmrSlowlog()
        slowlog.add_burst(range(1, 4), command=b"KEYS *", complexity=b"N=1")
        _, state = poll(AmrEnterpriseClient(slowlog), {})
        assert len(read_jsonl()) == 3

        slowlog.reset()
        slowlog.add_burst(range(1, 6), command=b"KEYS *", complexity=b"N=1")
        poll(AmrEnterpriseClient(slowlog), state)

        assert len(read_jsonl()) == 8, (
            f"expected 3 first-generation + 5 second-generation rows, got "
            f"{len(read_jsonl())}"
        )

    # FP-10 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B2: per-shard cursors are independent, so a collision on one shard "
               "drops that shard's generation while the other shard is unaffected",
        strict=True,
    )
    def test_fp10_oss_collision_is_isolated_to_the_resetting_shard(self, oss, upload):
        """OSS: shard-0 resets under a deterministic load, shard-1 keeps counting."""
        shard_0, shard_1 = AmrSlowlog(), AmrSlowlog()
        upload.fail_always(AzureError("Log Analytics unavailable"))

        state: dict = {}
        for generation in range(3):
            start = generation * 10 + 1
            shard_0.add_burst(range(start, start + 10), command=b"KEYS *", complexity=b"N=1")
            shard_1.add(100 + generation, command=b"GET other")
            _, state = poll(AmrOssClient({SHARD_0: shard_0, SHARD_1: shard_1}), state)

        rows_before_reset = len(read_jsonl())

        shard_0.reset()
        shard_0.add_burst(range(1, 21), command=b"KEYS *", complexity=b"N=1")
        shard_1.add(200, command=b"GET other")
        upload.succeed()
        poll(AmrOssClient({SHARD_0: shard_0, SHARD_1: shard_1}), state)

        appended = read_jsonl()[rows_before_reset:]
        shard_0_appended = [row for row in appended if row["node"] == SHARD_0]
        shard_0_uploaded = [
            row for row in upload.accepted_rows if row["Node"] == SHARD_0
        ]
        assert len(shard_0_appended) == len(shard_0_uploaded), (
            f"shard-0 uploaded {len(shard_0_uploaded)} rows to Log Analytics but "
            f"only {len(shard_0_appended)} reached the JSONL"
        )
        assert any(row["node"] == SHARD_1 for row in appended), (
            "shard-1's new row must still be appended"
        )


# ─────────────────────────────────────────────────────────────────────────────
# B3 — upgrading over a pre-fingerprint state file  (FP-11 ~ FP-12)
# ─────────────────────────────────────────────────────────────────────────────


class TestLegacyStateUpgrade:
    """Real upgrade sequence: the previous version is mid-outage with a pending
    `_jsonl` cursor on disk, the new image is deployed, and the pod restarts to retry
    the same batch.  That state file has no `fingerprints` key at all, which is not
    the same thing as having no history."""

    # FP-11 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B3: `not fingerprints` treats a legacy state file's missing key as a "
               "reset, so the already-backed-up batch is appended a second time",
        strict=True,
    )
    def test_fp11_enterprise_legacy_state_does_not_duplicate_jsonl(
        self, enterprise, upload
    ):
        """Enterprise: `{"_jsonl": {"last_id": 5}}` must be trusted by ID."""
        commands = [f"A{index}" for index in range(1, 6)]
        seed_backed_up_jsonl(commands)
        exporter.save_state({"_jsonl": {"last_id": 5}})

        slowlog = AmrSlowlog()
        for entry_id in range(1, 6):
            slowlog.add(entry_id, command=f"A{entry_id}".encode())

        poll_from_disk(AmrEnterpriseClient(slowlog))

        assert jsonl_commands() == commands, (
            f"already-backed-up rows were appended again: {jsonl_commands()}"
        )

    # FP-12 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="B3: the OSS per-node legacy state shape hits the same "
               "`not fingerprints` path and re-appends the batch",
        strict=True,
    )
    def test_fp12_oss_legacy_state_does_not_duplicate_jsonl(self, oss, upload):
        """OSS: `{"_jsonl": {"nodes": {...}}}` must be trusted by ID."""
        commands = [f"A{index}" for index in range(1, 6)]
        seed_backed_up_jsonl(commands, node=SHARD_0)
        exporter.save_state({"_jsonl": {"nodes": {SHARD_0: 5}}})

        shard = AmrSlowlog()
        for entry_id in range(1, 6):
            shard.add(entry_id, command=f"A{entry_id}".encode())

        poll_from_disk(AmrOssClient({SHARD_0: shard}))

        assert jsonl_commands() == commands, (
            f"already-backed-up rows were appended again: {jsonl_commands()}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Round-2 behaviour that must not regress again  (FP-13 ~ FP-20)
# ─────────────────────────────────────────────────────────────────────────────


class TestUploadFailureNonRegression:
    """These all pass on da5a997 and must keep passing after B1/B2/B3 are fixed.
    They are the guardrail against the pattern this component has repeated three
    times: a fix for the reported scenario that reintroduces an older one."""

    # FP-13 ───────────────────────────────────────────────────────────────────
    def test_fp13_enterprise_reset_above_old_cursor_keeps_every_row(
        self, enterprise, upload
    ):
        """SVHW-6's exact scenario: reset where the new generation outnumbers the
        old cursor value.  Distinguishable content, so no collision is involved."""
        slowlog = AmrSlowlog()
        for entry_id in range(1, 11):
            slowlog.add(entry_id, command=f"OLD{entry_id}".encode())
        upload.fail_always(AzureError("Log Analytics unavailable"))

        _, state = poll(AmrEnterpriseClient(slowlog), {})

        slowlog.reset()
        for entry_id in range(1, 13):
            slowlog.add(entry_id, command=f"NEW{entry_id}".encode())
        upload.succeed()
        poll(AmrEnterpriseClient(slowlog), state)

        commands = jsonl_commands()
        assert commands[:10] == [f"OLD{index}" for index in range(1, 11)]
        assert commands[10:] == [f"NEW{index}" for index in range(1, 13)], (
            "new-generation rows missing or misordered in the JSONL"
        )
        assert [row["Command"] for row in upload.accepted_rows] == [
            f"NEW{index}" for index in range(1, 13)
        ]
        assert read_state() == {"last_id": 12}

    # FP-14 ───────────────────────────────────────────────────────────────────
    def test_fp14_oss_reset_above_old_cursor_keeps_every_row(self, oss, upload):
        """Same scenario per shard, with independent per-node cursors."""
        shard = AmrSlowlog()
        for entry_id in range(1, 11):
            shard.add(entry_id, command=f"OLD{entry_id}".encode())
        upload.fail_always(AzureError("Log Analytics unavailable"))

        _, state = poll(AmrOssClient({SHARD_0: shard}), {})

        shard.reset()
        for entry_id in range(1, 13):
            shard.add(entry_id, command=f"NEW{entry_id}".encode())
        upload.succeed()
        poll(AmrOssClient({SHARD_0: shard}), state)

        commands = jsonl_commands()
        assert commands[10:] == [f"NEW{index}" for index in range(1, 13)]
        assert read_state() == {"nodes": {SHARD_0: 12}}

    # FP-15 ───────────────────────────────────────────────────────────────────
    @pytest.mark.parametrize(
        "failure",
        [
            ValueError("workload identity not configured"),
            SerializationError("bad payload"),
        ],
        ids=["ValueError", "SerializationError"],
    )
    def test_fp15_non_azure_upload_error_keeps_the_loop_alive(
        self, enterprise, upload, monkeypatch, failure
    ):
        """Credential and serialization failures are recoverable, not fatal.

        WorkloadIdentityCredential() raises a plain ValueError — not an AzureError —
        when AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_FEDERATED_TOKEN_FILE are
        missing, and it is constructed inside the upload boundary.
        """
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET key")
        upload.fail_always(failure)

        result = MainLoopDriver(
            monkeypatch, AmrEnterpriseClient(slowlog), max_polls=5
        ).run()

        assert result.budget_exhausted, (
            f"{type(failure).__name__} killed the poll loop; it must be retried"
        )
        assert not isinstance(failure, AzureError)

    # FP-16 ───────────────────────────────────────────────────────────────────
    @pytest.mark.parametrize(
        "failure",
        [
            ValueError("workload identity not configured"),
            SerializationError("bad payload"),
        ],
        ids=["ValueError", "SerializationError"],
    )
    def test_fp16_non_azure_upload_error_does_not_advance_the_cursor(
        self, enterprise, upload, failure
    ):
        """The durable cursor stays put so the batch is retried, not lost."""
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET key")
        upload.fail_always(failure)

        exported, _ = poll(AmrEnterpriseClient(slowlog), {})

        assert len(exported) == 1
        state = read_state()
        assert state["_jsonl"]["last_id"] == 1
        assert "last_id" not in state, "durable cursor advanced despite upload failure"

    # FP-17 ───────────────────────────────────────────────────────────────────
    def test_fp17_retry_after_single_failure_uploads_once_and_appends_once(
        self, enterprise, upload
    ):
        """The E2E-09 contract: one failure then success → 1 JSONL row, 1 upload."""
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"GET key")
        client = AmrEnterpriseClient(slowlog)
        upload.fail_next(1, AzureError("transient"))

        poll_from_disk(client)

        assert len(read_jsonl()) == 1
        pending = read_state()
        assert pending["_jsonl"]["last_id"] == 1
        assert set(pending["_jsonl"]["fingerprints"]) == {"1"}
        assert upload.accepted == []

        poll_from_disk(client)

        assert len(read_jsonl()) == 1, "retry appended the row a second time"
        assert read_state() == {"last_id": 1}
        assert len(upload.accepted_rows) == 1, "row uploaded more than once"

    # FP-18 ───────────────────────────────────────────────────────────────────
    def test_fp18_repeated_restarts_during_outage_do_not_duplicate_rows(
        self, enterprise, upload
    ):
        """Three restarts while Log Analytics is down must not grow the JSONL."""
        slowlog = AmrSlowlog()
        for entry_id in range(1, 4):
            slowlog.add(entry_id, command=f"CMD{entry_id}".encode())
        client = AmrEnterpriseClient(slowlog)
        upload.fail_always(AzureError("Log Analytics unavailable"))

        for _ in range(3):
            poll_from_disk(client)

        assert jsonl_commands() == ["CMD1", "CMD2", "CMD3"], (
            f"restarts duplicated rows: {jsonl_commands()}"
        )

        upload.succeed()
        poll_from_disk(client)

        assert jsonl_commands() == ["CMD1", "CMD2", "CMD3"]
        assert read_state() == {"last_id": 3}

    # FP-19 ───────────────────────────────────────────────────────────────────
    def test_fp19_batch_growth_during_outage_appends_only_the_delta(
        self, enterprise, upload
    ):
        """New rows arriving mid-outage are appended once, with no re-appends."""
        slowlog = AmrSlowlog()
        slowlog.add(1, command=b"CMD1")
        client = AmrEnterpriseClient(slowlog)
        upload.fail_always(AzureError("Log Analytics unavailable"))

        state: dict = {}
        _, state = poll(client, state)
        slowlog.add(2, command=b"CMD2")
        _, state = poll(client, state)
        slowlog.add(3, command=b"CMD3")
        _, state = poll(client, state)

        assert jsonl_commands() == ["CMD1", "CMD2", "CMD3"]

        upload.succeed()
        poll(client, state)

        assert jsonl_commands() == ["CMD1", "CMD2", "CMD3"]
        assert [row["Command"] for row in upload.accepted_rows] == [
            "CMD1",
            "CMD2",
            "CMD3",
        ]

    # FP-20 ───────────────────────────────────────────────────────────────────
    def test_fp20_oss_restart_during_outage_does_not_duplicate_per_shard(
        self, oss, upload
    ):
        """The E2E-18 contract across a restart, with two shards."""
        shard_0, shard_1 = AmrSlowlog(), AmrSlowlog()
        shard_0.add(1, command=b"A1")
        shard_1.add(4, command=b"B4")
        client = AmrOssClient({SHARD_0: shard_0, SHARD_1: shard_1})
        upload.fail_always(AzureError("Log Analytics unavailable"))

        poll_from_disk(client)
        assert len(read_jsonl()) == 2
        assert read_state()["_jsonl"]["nodes"] == {SHARD_0: 1, SHARD_1: 4}

        poll_from_disk(client)
        assert len(read_jsonl()) == 2, "restart duplicated rows"

        upload.succeed()
        poll_from_disk(client)

        assert len(read_jsonl()) == 2
        assert read_state() == {"nodes": {SHARD_0: 1, SHARD_1: 4}}
        assert len(upload.accepted_rows) == 2


# ─────────────────────────────────────────────────────────────────────────────
# State file growth  (FP-21)
# ─────────────────────────────────────────────────────────────────────────────


class TestStateGrowth:

    # FP-21 ───────────────────────────────────────────────────────────────────
    @pytest.mark.xfail(
        reason="non-blocking (round-3 suggestion 1): the fingerprint map is unbounded "
               "during an outage even though only SLOWLOG_BATCH_SIZE rows can ever "
               "be re-fetched",
        strict=True,
    )
    def test_fp21_fingerprint_map_is_bounded_by_batch_size(self, enterprise, upload):
        """A long outage must not grow the state file without bound.

        Only the newest SLOWLOG_BATCH_SIZE rows can ever come back from SLOWLOG GET,
        so retaining fingerprints beyond that cannot affect any future decision.
        """
        slowlog = AmrSlowlog()
        upload.fail_always(AzureError("Log Analytics unavailable"))

        state: dict = {}
        next_id = 1
        for _ in range(40):
            for _ in range(50):
                slowlog.add(next_id, command=b"GET x")
                next_id += 1
            _, state = poll(AmrEnterpriseClient(slowlog), state)

        fingerprints = read_state()["_jsonl"]["fingerprints"]
        assert len(fingerprints) <= exporter.SLOWLOG_BATCH_SIZE, (
            f"{len(fingerprints)} fingerprints retained for a "
            f"{exporter.SLOWLOG_BATCH_SIZE}-row batch cap "
            f"({Path(exporter.STATE_FILE).stat().st_size} bytes on disk)"
        )
