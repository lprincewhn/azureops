"""Reusable failure-injection harness for the slow query exporter.

The exporter's upload path is where every regression in this component has so far
been found, yet neither the E2E nor the live suite could express "the upload
failed" beyond a single always-raise flag.  This module provides the missing
seams so failure scenarios can be written as ordinary tests:

    InjectableUpload   programmable Log Analytics client — which call fails, with
                       which exception, and whether the server accepted the rows
                       before the client saw the error
    AmrSlowlog         SLOWLOG fake whose rows are produced by the *real*
                       redis-py parser from an Azure Managed Redis wire reply,
                       so the fields the exporter fingerprints are the fields
                       AMR actually returns
    MainLoopDriver     runs the real exporter.main() loop under a poll budget, so
                       "the polling loop survives" / "the process exits cleanly"
                       are assertable instead of assumed

Nothing here touches production code: the exporter is driven only through its
existing module globals (`_logs_client`, `connect`, the config vars) which are
already the documented test seams used by test_e2e.py.
"""

import signal
import sys

from redis._parsers.helpers import parse_slowlog_get

import exporter

# ── SLOWLOG fakes built on the real redis-py parser ──────────────────────────
#
# Azure Managed Redis runs Redis Enterprise, whose SLOWLOG reply carries a
# complexity element at index [3] and makes client_address / client_name
# conditional on the item having >= 7 elements.  Hand-written dicts hide that;
# routing through parse_slowlog_get keeps the fakes honest about which fields
# the exporter can actually rely on.


def amr_raw_item(
    entry_id: int,
    command: bytes = b"GET key",
    duration_us: int = 5_000,
    start_time: int = 1_700_000_000,
    complexity: bytes = b"N=1",
    client_address: bytes | None = None,
    client_name: bytes | None = None,
) -> list:
    """Build one raw SLOWLOG reply item in Azure Managed Redis (Enterprise) shape.

    Passing client_address/client_name produces the 7-element OSS-style item where
    redis-py also exposes those fields.  Omitting them — the AMR Enterprise default —
    produces the 5-element item where they are absent entirely.
    """
    item: list = [entry_id, start_time, duration_us, complexity, command.split(b" ")]
    if client_address is not None or client_name is not None:
        item.extend([client_address or b"", client_name or b""])
    return item


class AmrSlowlog:
    """A single node's SLOWLOG, newest entry first, parsed like real redis-py."""

    def __init__(self):
        self._raw: list[list] = []

    def add(self, *args, **kwargs) -> None:
        """Record one command; accepts the amr_raw_item signature."""
        self._raw.insert(0, amr_raw_item(*args, **kwargs))

    def add_burst(
        self,
        ids,
        command: bytes = b"KEYS *",
        duration_us: int = 5_000,
        start_time: int = 1_700_000_000,
        complexity: bytes = b"N=1",
    ) -> None:
        """Record a run of byte-identical commands inside a single wall-clock second.

        This is the deterministic periodic workload that makes SLOWLOG rows
        indistinguishable from each other: start_time has second granularity, the
        command text and duration repeat exactly, and complexity is the only
        element that varies between generations.
        """
        for entry_id in ids:
            self.add(
                entry_id,
                command=command,
                duration_us=duration_us,
                start_time=start_time,
                complexity=complexity,
            )

    def reset(self) -> None:
        """SLOWLOG RESET — rows cleared and the ID counter restarts from scratch."""
        self._raw.clear()

    def slowlog_get(self, count: int) -> list:
        return parse_slowlog_get(list(self._raw[:count]))


class AmrEnterpriseClient:
    """redis.Redis stand-in for enterprise cluster policy (single endpoint)."""

    def __init__(self, slowlog: AmrSlowlog):
        self._sl = slowlog

    def slowlog_get(self, count: int) -> list:
        return self._sl.slowlog_get(count)


class _Node:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port


class _NodeConn:
    def __init__(self, slowlog: AmrSlowlog):
        self.slowlog_get = slowlog.slowlog_get


class AmrOssClient:
    """RedisCluster stand-in for OSS cluster policy (per-shard SLOWLOG counters)."""

    def __init__(self, nodes: dict[str, AmrSlowlog]):
        self._nodes: dict[str, tuple] = {}
        for key, slowlog in nodes.items():
            host, port = key.rsplit(":", 1)
            self._nodes[key] = (_Node(host, int(port)), _NodeConn(slowlog))

    def get_primaries(self):
        return [node for node, _ in self._nodes.values()]

    def get_redis_connection(self, node):
        return self._nodes[f"{node.host}:{node.port}"][1]


# ── Programmable Log Analytics upload ────────────────────────────────────────


class InjectableUpload:
    """Log Analytics client stand-in with programmable failures.

    Installed as `exporter._logs_client`, the same seam test_e2e.py already uses.

    Failure selection (1-based call numbers):
        fail_always(exc)        every call raises
        fail_calls({1, 3}, exc) only those calls raise
        fail_next(n, exc)       the next n calls raise, then uploads succeed

    Partial / observed-vs-accepted distinction:
        accept_before_raise=True   rows land in `accepted` *before* the exception is
                                   raised — the service took the batch but the client
                                   never saw the 2xx.  This is what a SIGTERM inside
                                   the upload window looks like, and also what a
                                   chunked (>1MB) upload looks like when an early
                                   chunk is accepted and a later one fails.
        accept_rows=k              accept only the first k rows of the batch before
                                   raising, modelling a partially-ingested chunked
                                   upload.

    `on_upload(hook)` runs an arbitrary callable as `hook(rows, call_no)` during every
    upload — used to deliver a real signal mid-upload.  `when="after_accept"` runs it
    once the service has already kept the rows, which is the ordering that produces
    duplicate ingestion: the batch is in Log Analytics, but the client is interrupted
    before it ever observes the 2xx.

    `attempted` records what the exporter tried to send; `accepted` records what the
    service kept.  Duplicate-ingestion assertions read `accepted`.
    """

    def __init__(self):
        self.attempted: list[list[dict]] = []
        self.accepted: list[list[dict]] = []
        self._fail_calls: set[int] | None = None
        self._fail_from: int | None = None
        self._fail_until: int | None = None
        self._exc: BaseException | None = None
        self._accept_before_raise = False
        self._accept_rows: int | None = None
        self._hook = None
        self._hook_when = "before"

    # -- programming ---------------------------------------------------------

    def fail_always(self, exc: BaseException, **kw):
        self._exc = exc
        self._fail_from = 1
        self._fail_until = None
        self._configure(**kw)
        return self

    def fail_next(self, n: int, exc: BaseException, **kw):
        self._exc = exc
        self._fail_from = len(self.attempted) + 1
        self._fail_until = len(self.attempted) + n
        self._configure(**kw)
        return self

    def fail_calls(self, calls, exc: BaseException, **kw):
        self._exc = exc
        self._fail_calls = set(calls)
        self._configure(**kw)
        return self

    def succeed(self):
        """Stop failing — used to model recovery after an outage."""
        self._fail_calls = None
        self._fail_from = None
        self._fail_until = None
        self._exc = None
        return self

    def on_upload(self, hook, when: str = "before"):
        """Run hook(rows, call_no) on every upload.

        when="before"        at the top of upload(), before the service sees anything
        when="after_accept"  after the rows have been recorded as accepted
        """
        if when not in ("before", "after_accept"):
            raise ValueError(f"unknown hook position {when!r}")
        self._hook = hook
        self._hook_when = when
        return self

    def _configure(self, accept_before_raise: bool = False, accept_rows: int | None = None):
        self._accept_before_raise = accept_before_raise or accept_rows is not None
        self._accept_rows = accept_rows

    # -- the seam the exporter calls ----------------------------------------

    def upload(self, rule_id: str, stream_name: str, logs: list) -> None:
        rows = list(logs)
        call_no = len(self.attempted) + 1
        self.attempted.append(rows)

        if self._hook is not None and self._hook_when == "before":
            self._hook(rows, call_no)

        if not self._should_fail(call_no):
            self.accepted.append(rows)
            if self._hook is not None and self._hook_when == "after_accept":
                # The service has the batch; anything raised from here on is an
                # interruption the client suffers *after* successful ingestion.
                self._hook(rows, call_no)
            return

        if self._accept_before_raise:
            kept = rows if self._accept_rows is None else rows[: self._accept_rows]
            if kept:
                self.accepted.append(kept)
        raise self._exc

    def _should_fail(self, call_no: int) -> bool:
        if self._exc is None:
            return False
        if self._fail_calls is not None:
            return call_no in self._fail_calls
        if self._fail_from is None or call_no < self._fail_from:
            return False
        return self._fail_until is None or call_no <= self._fail_until

    # -- reading ------------------------------------------------------------

    @property
    def accepted_rows(self) -> list[dict]:
        return [row for batch in self.accepted for row in batch]

    @property
    def accepted_ids(self) -> list:
        return [row["SlowlogId"] for row in self.accepted_rows]

    @property
    def attempted_rows(self) -> list[dict]:
        return [row for batch in self.attempted for row in batch]

    @property
    def rows(self) -> list[dict]:
        """Alias for accepted_rows — the rows Log Analytics actually holds."""
        return self.accepted_rows

    @property
    def total(self) -> int:
        return len(self.accepted_rows)

    @property
    def calls(self) -> int:
        return len(self.attempted)


def raise_sigterm_once():
    """Upload hook that delivers a real SIGTERM during the first upload.

    signal.raise_signal is synchronous, so the handler main() installed runs
    inside upload() — exactly the production window where SIGTERM is most likely
    to land, since the upload is the slowest, network-bound part of a poll.
    """
    state = {"fired": False}

    def hook(rows, call_no):
        if not state["fired"]:
            state["fired"] = True
            signal.raise_signal(signal.SIGTERM)

    return hook


# ── Driving the real main() loop ─────────────────────────────────────────────


class PollBudgetExhausted(BaseException):
    """Raised from the patched sleep to bound main()'s infinite while loop.

    Deliberately a BaseException so it cannot be mistaken for a pollable error by
    main()'s `except _Stop` / run_once's `except Exception` handlers.
    """


class MainLoopResult:
    def __init__(self):
        self.polls = 0
        self.returned_gracefully = False
        self.budget_exhausted = False
        self.saves: list[dict] = []
        self.shutdown_saves: list[dict] = []
        self.escaped: BaseException | None = None

    @property
    def saved_on_exit(self) -> bool:
        """True when save_state ran from main()'s `except _Stop` shutdown path.

        Distinguished from run_once's own saves by call site, so a routine
        mid-poll save cannot be mistaken for the graceful-shutdown save.
        """
        return bool(self.shutdown_saves)


class MainLoopDriver:
    """Run the real exporter.main() with a bounded number of poll iterations.

    Everything patched is an existing exporter module global.  main() therefore
    executes its own signal registration, its own while loop, and its own
    `except _Stop: save_state(...)` shutdown path — which is what makes graceful
    shutdown and loop survival observable.
    """

    def __init__(self, monkeypatch, client, max_polls: int = 20):
        self.monkeypatch = monkeypatch
        self.client = client
        self.max_polls = max_polls
        self.result = MainLoopResult()
        self._saved_handlers: dict = {}

    def _install(self):
        mp = self.monkeypatch
        res = self.result

        mp.setattr(exporter, "connect", lambda: self.client)
        mp.setattr(exporter, "POLL_INTERVAL", 0)
        # _validate_config() reads these; give it a viable configuration.
        mp.setattr(exporter, "AMR_ACCESS_KEY", "test-key")
        mp.setattr(exporter, "DCE_ENDPOINT", "https://dce.test.ingest.monitor.azure.com")
        mp.setattr(exporter, "CLUSTERS_CONFIG_FILE", "")

        real_save = exporter.save_state

        def spy_save(state):
            snapshot = dict(state)
            res.saves.append(snapshot)
            # Attribute the save to its call site: main() only calls save_state from
            # its `except _Stop` shutdown handler, run_once from the poll body.
            caller = sys._getframe(1)
            if caller.f_code.co_name == "main":
                res.shutdown_saves.append(snapshot)
            real_save(state)

        mp.setattr(exporter, "save_state", spy_save)

        def bounded_sleep(_seconds):
            res.polls += 1
            if res.polls >= self.max_polls:
                res.budget_exhausted = True
                raise PollBudgetExhausted
        mp.setattr(exporter.time, "sleep", bounded_sleep)

        # main() installs its own SIGINT/SIGTERM handlers; run() restores ours.
        self._saved_handlers = {
            sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)
        }

    def run(self) -> MainLoopResult:
        """Run main(); returns once it exits, crashes, or the poll budget runs out.

        An exception escaping main() is recorded on `result.escaped` rather than
        propagated, so "this error must crash the process instead of being retried
        forever" is a plain assertion on the result.
        """
        self._install()
        try:
            exporter.main()
            self.result.returned_gracefully = True
        except PollBudgetExhausted:
            pass
        except BaseException as exc:  # noqa: BLE001 — the crash is the observation
            self.result.escaped = exc
        finally:
            for sig, handler in self._saved_handlers.items():
                signal.signal(sig, handler)
        return self.result
