#!/usr/bin/env python3
"""Isolated raw benchmark for response normalization and durable staging."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from uuid import UUID

CONTROL_COMMIT = "980e53b3a3815592c3bd0dab9e41e2b057e50724"
MODES = ("legacy_normalization_floor", "classic_legacy_cursor_durability", "candidate")
ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "BENCHMARK"
PICKLE_KEY = "benchmark-secret"
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
TRACKED_PATHS = (
    "src/nio/ingest/source.py",
    "src/nio/ingest/state.py",
    "src/nio/ingest/hydration.py",
    "src/nio/store/_sync_journal.py",
    "src/nio/store/_sync_journal_plan.py",
    "src/nio/store/_sync_journal_preflight.py",
    "src/nio/store/_sync_journal_rows.py",
    "src/nio/store/_sync_journal_values.py",
    "src/nio/store/sync_journal.py",
    "src/nio/store/sync_journal_schema.py",
)
SNAPSHOT_PATHS = (
    "scripts/benchmark_ingest_staging.py",
    *TRACKED_PATHS,
    "tests/ingest/staging_benchmark_test.py",
    "tests/ingest/run_staging_benchmark_corpus.py",
    "tests/ingest/source_journal_test.py",
    *(
        f"tests/data/ingest/{name}.json"
        for name in "canonical_1mib classic_continuation_1 classic_continuation_2 classic_steady_100_rooms_1000_events classic_sync sliding_continuation_1 sliding_continuation_2 sliding_steady_100_rooms_1000_events".split()
    ),
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _transport(raw: bytes) -> str:
    value = json.loads(raw)
    if type(value) is not dict:
        raise ValueError("fixture must contain a JSON object")
    if "next_batch" in value and "pos" not in value:
        return "classic"
    if "pos" in value and "next_batch" not in value:
        return "sliding"
    raise ValueError("fixture must be exactly one sync transport")


def _git(repo: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), *arguments], check=True, capture_output=True
    ).stdout


def _snapshot(repo: Path, candidate: bool, expected_commit=None):
    head = _git(repo, "rev-parse", "HEAD").decode().strip()
    tree = _git(repo, "rev-parse", "HEAD^{tree}").decode().strip()
    status = _git(
        repo, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"
    ).decode()
    expected = expected_commit if candidate else CONTROL_COMMIT
    if expected is None or head != expected:
        raise RuntimeError(f"target commit is {head}, expected {expected}")
    if status:
        raise RuntimeError("target worktree must be clean")
    payload: dict[str, Any] = {
        "kind": "candidate" if candidate else "fixed_control",
        "commit": head,
        "head_tree": tree,
        "clean": True,
        "tracked_diff_sha256": hashlib.sha256(b"").hexdigest(),
        "files": (
            {path: _sha256(repo / path) for path in SNAPSHOT_PATHS} if candidate else {}
        ),
        "excluded_paths": [],
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return {**payload, "snapshot_sha256": hashlib.sha256(encoded).hexdigest()}


def _verify_snapshot(repo: Path, candidate: bool, expected_commit, expected_digest):
    snapshot = _snapshot(repo, candidate, expected_commit)
    actual = snapshot["snapshot_sha256"]
    if expected_digest != actual and (candidate or expected_digest):
        raise RuntimeError("target snapshot does not match expected snapshot")
    return snapshot


def _manifest_stats(repo: Path, external: list[Path]) -> dict[str, tuple[Any, ...]]:
    tracked = _git(repo, "ls-files", "-z").decode().split("\0")
    paths = [*external, *(repo / path for path in tracked if path)]
    return {
        str(path.resolve()): (stat := path.stat())[1:7] + (stat.st_ctime_ns,)
        for path in paths
    }


def _rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _sql_shape(sql: str) -> str:
    normalized = " ".join(sql.split())
    upper = normalized.upper()
    if upper.startswith(("BEGIN", "COMMIT", "ROLLBACK", "PRAGMA")):
        return upper.split(maxsplit=1)[0]
    operation = upper.split(maxsplit=1)[0] if upper else "UNKNOWN"
    tables = [
        name
        for name in ("NioIngestMeta", "NioIngestSourceState", "NioIngestFrame")
        if name.upper() in upper
    ]
    return f"{operation} {'+'.join(tables) if tables else 'other'}"


def _significant(sql: str) -> bool:
    return (
        not sql.lstrip()
        .upper()
        .startswith(
            (
                "BEGIN",
                "COMMIT",
                "END",
                "ROLLBACK",
                "SAVEPOINT",
                "RELEASE",
                "PRAGMA",
            )
        )
    )


def _sample(
    phase: str,
    raw: bytes,
    wall_ns: int,
    cpu_ns: int,
    wal_bytes: int,
    trace: list[str],
    checks: dict[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "success": True,
        "wall_ns": wall_ns,
        "cpu_ns": cpu_ns,
        "peak_rss_bytes": _rss_bytes(),
        "wal_bytes": wal_bytes,
        "canonical_bytes": len(raw),
        "worker_pid": os.getpid(),
        "network_denied": getattr(_deny_network, "installed", False),
        "target_import_path": str(Path(sys.modules["nio"].__file__).resolve()),
        "sql_trace": [_sql_shape(sql) for sql in trace],
        "statement_count": sum(_significant(sql) for sql in trace),
        "transaction_count": sum(
            sql.lstrip().upper().startswith("BEGIN") for sql in trace
        ),
        "reopen_verification": checks,
        **extra,
    }


def calculate_statistics(
    samples: list[dict[str, Any]], expected_measured: int
) -> dict[str, int | float]:
    measured = [sample for sample in samples if sample["phase"] == "measured"]
    if len(measured) != expected_measured:
        raise ValueError("measured sample count does not match request")
    if not all(sample["success"] for sample in measured):
        raise ValueError("all measured samples must succeed")

    walls = sorted(sample["wall_ns"] for sample in measured)
    return {
        "measured_samples": len(measured),
        "median_wall_ns": statistics.median(sample["wall_ns"] for sample in measured),
        "p95_wall_ns": walls[(95 * len(walls) + 99) // 100 - 1],
        "median_cpu_ns": statistics.median(sample["cpu_ns"] for sample in measured),
        "peak_rss_bytes": max(sample["peak_rss_bytes"] for sample in measured),
        "max_wal_bytes": max(sample["wal_bytes"] for sample in measured),
    }


def _deny_network() -> None:
    def deny(event: str, _args: tuple[object, ...]) -> None:
        if (
            event
            in "socket.bind socket.connect socket.connect_ex socket.getaddrinfo socket.sendmsg socket.sendto".split()
        ):
            raise RuntimeError(f"network denied by benchmark: {event}")

    sys.addaudithook(deny)
    setattr(_deny_network, "installed", True)


def _native_response(raw: bytes, transport: str):
    from nio.responses import ErrorResponse, SlidingSyncResponse, SyncResponse

    response = (
        SyncResponse.from_dict(json.loads(raw))
        if transport == "classic"
        else SlidingSyncResponse.from_dict(json.loads(raw))
    )
    if isinstance(response, ErrorResponse):
        raise ValueError(f"native response parse failed: {response}")
    return response


def _source_config(transport: str):
    from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig

    if transport == "classic":
        return ClassicSourceConfig(30_000, b"{}")
    return SlidingSourceConfig(30_000, "benchmark", b"{}", b"{}", b"{}", 100)


def _adapter(stream_id, config):
    from nio.ingest.classic import ClassicSource
    from nio.ingest.sliding import SlidingSource

    if type(config).__name__ == "ClassicSourceConfig":
        return ClassicSource(stream_id, config, ACCOUNT_ID)
    return SlidingSource(stream_id, config, ACCOUNT_ID)


def _plan_proposal(journal, config):
    owner = journal.load_owner()
    prior = journal.load_source()
    source = _adapter(owner.stream_id, config)
    request = source.plan_request(prior, prior.next_request_id)
    if request is None:
        raise ValueError("benchmark source became inactive")
    return owner, prior, source, request


def _finish_proposal(planned, raw: bytes):
    from nio.ingest.ports import NetworkResult, StagedSourceResponse
    from nio.ingest.source import SourceResultKind
    from nio.ingest.state import SourceState, StagedFrame

    owner, prior, source, request = planned
    result = source.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            raw,
            None,
            None,
        ),
    )
    if result.kind is not SourceResultKind.FRAME or result.frame is None:
        raise ValueError(f"candidate normalization failed: {result.detail}")
    staged = StagedFrame(
        result.frame.frame_id,
        StagedSourceResponse(request, result.response_body, result.frame.source_sha256),
    )
    successor = SourceState(
        prior.source_epoch,
        prior.transport_kind,
        result.frame.candidate_cursor_json,
        request.request_id + 1,
        prior.active,
    )
    return owner, prior, source, successor, staged, result.frame


def _proposal(journal, config, raw: bytes):
    return _finish_proposal(_plan_proposal(journal, config), raw)


def _stage(journal, proposal) -> None:
    _owner, _prior, _source, successor, staged, _normalized = proposal
    journal.stage_source_response(
        source=successor,
        frame=staged,
    )


def _open_journal(path: Path, config, timeout_ms: int, observer=None):
    from nio.store.sync_journal import open_ingestion_store

    return open_ingestion_store(
        path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=config,
        pickle_key=PICKLE_KEY,
        database_name="benchmark.db",
        sqlite_busy_timeout_ms=timeout_ms,
        statement_observer=observer,
    )


def _candidate_sample(
    fixture: Path, setup: list[Path], timeout_ms: int, phase: str
) -> dict[str, Any]:
    from nio.ingest.source import renormalize_staged_frame

    raw = fixture.read_bytes()
    transport = _transport(raw)
    config = _source_config(transport)
    statements: list[str] = []
    with tempfile.TemporaryDirectory(prefix="nio-stage-candidate-") as directory:
        store_path = Path(directory)
        database_path = store_path / "benchmark.db"
        bootstrap = _open_journal(store_path, config, timeout_ms, statements.append)
        journal = bootstrap._journal
        for setup_path in setup:
            _stage(journal, _proposal(journal, config, setup_path.read_bytes()))
        before_source = journal.load_source()
        before_count = len(journal.list_frames(256))
        planned = _plan_proposal(journal, config)
        connection = journal._owner.database
        connection.execute_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        wal_path = Path(f"{database_path}-wal")
        baseline_wal = _file_size(wal_path)
        statements.clear()
        cpu_start = time.process_time_ns()
        wall_start = time.perf_counter_ns()
        proposal = _finish_proposal(planned, raw)
        _stage(journal, proposal)
        wall_ns = time.perf_counter_ns() - wall_start
        cpu_ns = time.process_time_ns() - cpu_start
        wal_bytes = max(0, _file_size(wal_path) - baseline_wal)
        trace = list(statements)
        owner, _prior, _source, successor, staged, normalized = proposal
        bootstrap.close()

        reopened = _open_journal(store_path, config, timeout_ms)
        try:
            loaded = reopened._journal.load_frame(staged.frame_id)
            loaded_source = reopened._journal.load_source()
            after_count = len(reopened._journal.list_frames(256))
            if loaded is None:
                raise ValueError("target frame missing after reopen")
            rebuilt = renormalize_staged_frame(
                _adapter(reopened._journal.load_owner().stream_id, config), loaded
            )
            checks = {
                "success": rebuilt == normalized and loaded_source == successor,
                "frame_identity": loaded.frame_id == staged.frame_id,
                "source_digest": loaded.response.source_sha256
                == staged.response.source_sha256,
                "canonical_body": loaded.response.response_body
                == staged.response.response_body,
                "cursor_before": json.loads(before_source.cursor_json),
                "cursor_after": json.loads(loaded_source.cursor_json),
                "frames_before": before_count,
                "frames_after": after_count,
            }
            if not checks["success"]:
                raise ValueError("candidate reopen verification mismatch")
        finally:
            reopened.close()

    return _sample(phase, raw, wall_ns, cpu_ns, wal_bytes, trace, checks)


def _floor_sample(fixture: Path, setup: list[Path], phase: str) -> dict[str, Any]:
    raw = fixture.read_bytes()
    transport = _transport(raw)
    for setup_path in setup:
        _native_response(setup_path.read_bytes(), transport)
    cpu_start = time.process_time_ns()
    wall_start = time.perf_counter_ns()
    response = _native_response(raw, transport)
    wall_ns = time.perf_counter_ns() - wall_start
    cpu_ns = time.process_time_ns() - cpu_start
    return _sample(
        phase,
        raw,
        wall_ns,
        cpu_ns,
        0,
        [],
        {"success": True, "response_type": type(response).__name__},
    )


def _classic_durability_sample(
    fixture: Path, setup: list[Path], timeout_ms: int, phase: str
) -> dict[str, Any]:
    from nio.crypto import OlmAccount
    from nio.store import SqliteStore

    raw = fixture.read_bytes()
    with tempfile.TemporaryDirectory(prefix="nio-stage-classic-") as directory:
        store = SqliteStore(ACCOUNT_ID, DEVICE_ID, directory, database_name="store.db")
        store.database.execute_sql(f"PRAGMA busy_timeout = {timeout_ms}")
        actual_timeout = store.database.execute_sql("PRAGMA busy_timeout").fetchone()[0]
        if actual_timeout != timeout_ms:
            raise ValueError("Classic durability busy timeout mismatch")
        store.save_account(OlmAccount())
        store.database.execute_sql("PRAGMA journal_mode = WAL")
        store.database.execute_sql("PRAGMA synchronous = NORMAL")
        for setup_path in setup:
            setup_response = _native_response(setup_path.read_bytes(), "classic")
            store.save_sync_token(setup_response.next_batch)
        store.database.execute_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        wal_path = Path(directory) / "store.db-wal"
        baseline_wal = _file_size(wal_path)
        statements: list[str] = []
        connection = store.database.connection()
        connection.set_trace_callback(statements.append)
        cpu_start = time.process_time_ns()
        wall_start = time.perf_counter_ns()
        response = _native_response(raw, "classic")
        store.save_sync_token(response.next_batch)
        wall_ns = time.perf_counter_ns() - wall_start
        cpu_ns = time.process_time_ns() - cpu_start
        wal_bytes = max(0, _file_size(wal_path) - baseline_wal)
        trace = list(statements)
        connection.set_trace_callback(None)
        store.database.close()
        reopened = SqliteStore(
            ACCOUNT_ID, DEVICE_ID, directory, database_name="store.db"
        )
        try:
            reopened.database.execute_sql(f"PRAGMA busy_timeout = {timeout_ms}")
            loaded = reopened.load_sync_token()
            checks = {"success": loaded == response.next_batch, "cursor": loaded}
        finally:
            reopened.database.close()
        if not checks["success"]:
            raise ValueError("classic cursor did not survive reopen")
    return _sample(
        phase,
        raw,
        wall_ns,
        cpu_ns,
        wal_bytes,
        trace,
        checks,
        busy_timeout_ms=actual_timeout,
    )


def _worker(args: argparse.Namespace) -> int:
    repo = args.repo.resolve()
    candidate = args.mode == "candidate"
    binding = args.expected_candidate_commit, args.expected_snapshot_sha256
    inputs = [args.fixture, *args.setup_fixture]
    _deny_network()
    expected_src = (repo / "src").resolve()
    imported = Path(__import__("nio").__file__).resolve()
    if not imported.is_relative_to(expected_src):
        raise RuntimeError(f"nio import escaped target repo: {imported}")
    snapshot = _verify_snapshot(repo, candidate, *binding)
    before_stats = _manifest_stats(repo, inputs)
    if args.mode == "candidate":
        sample = _candidate_sample(
            args.fixture.resolve(), args.setup_fixture, args.busy_timeout_ms, args.phase
        )
    elif args.mode == "legacy_normalization_floor":
        sample = _floor_sample(args.fixture.resolve(), args.setup_fixture, args.phase)
    else:
        sample = _classic_durability_sample(
            args.fixture.resolve(), args.setup_fixture, args.busy_timeout_ms, args.phase
        )
    after = _verify_snapshot(repo, candidate, *binding)
    if after != snapshot or _manifest_stats(repo, inputs) != before_stats:
        raise RuntimeError("target snapshot changed while sampling")
    sample["repo_head"] = snapshot["commit"]
    sample["snapshot_sha256"] = snapshot["snapshot_sha256"]
    print(json.dumps(sample, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0


def _environment(repo: Path, timeout_ms: int) -> dict[str, Any]:
    import peewee

    cpu_model = platform.processor()
    cpuinfo = Path("/proc/cpuinfo")
    if not cpu_model and cpuinfo.is_file():
        fields = {
            key.strip(): value.strip()
            for line in cpuinfo.read_text().split("\n\n", 1)[0].splitlines()
            if ":" in line
            for key, value in (line.split(":", 1),)
        }
        names = ("model name", "Hardware", "Processor")
        cpu_model = next((fields[name] for name in names if fields.get(name)), "")
        if not cpu_model:
            cpu_model = "; ".join(
                f"{name.removeprefix('CPU ').lower()}={fields[name].strip()}"
                for name in "CPU implementer,CPU architecture,CPU part,CPU revision".split(
                    ","
                )
                if name in fields
            )
    cpu_model = cpu_model or platform.machine()
    meminfo = Path("/proc/meminfo")
    memory = (
        next(
            (
                int(line.split()[1]) * 1024
                for line in meminfo.read_text().splitlines()
                if line.startswith("MemAvailable:")
            ),
            None,
        )
        if meminfo.is_file()
        else None
    )
    stat = os.statvfs(repo)
    fs_command = (
        ["stat", "-f", "%T", str(repo)]
        if sys.platform == "darwin"
        else ["findmnt", "-n", "-o", "FSTYPE", "-T", str(repo)]
    )
    filesystem_type = subprocess.run(
        fs_command, check=True, capture_output=True, text=True
    ).stdout.strip()
    return {
        "python": sys.version,
        "peewee": peewee.__version__,
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "cpu_model": cpu_model,
        "cpu_count": os.cpu_count(),
        "busy_timeout_ms": timeout_ms,
        "available_memory_bytes": memory,
        "filesystem_block_size": stat.f_frsize,
        "filesystem_available_bytes": stat.f_bavail * stat.f_frsize,
        "filesystem_type": filesystem_type,
    }


def _orchestrate(args: argparse.Namespace) -> int:
    repo = args.repo.resolve()
    fixture = args.fixture.resolve()
    setup = [path.resolve() for path in args.setup_fixture]
    raw = fixture.read_bytes()
    transport = _transport(raw)
    candidate = args.mode == "candidate"
    binding = args.expected_candidate_commit, args.expected_snapshot_sha256
    snapshot = _verify_snapshot(repo, candidate, *binding)
    base = {
        "schema_version": 1,
        "fixed_control_commit": CONTROL_COMMIT,
        "mode": args.mode,
        "fixture": {"path": str(fixture), "sha256": _sha256(fixture)},
        "setup_fixtures": [
            {"path": str(path), "sha256": _sha256(path)} for path in setup
        ],
        "environment": _environment(repo, args.busy_timeout_ms),
        "snapshot": snapshot,
    }
    if args.mode == "classic_legacy_cursor_durability" and transport == "sliding":
        report = {
            **base,
            "availability": "not_available",
            "reason": "fixed_base_has_no_durable_sliding_pos",
            "samples": [],
            "statistics": None,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        return 0

    samples: list[dict[str, Any]] = []
    for index in range(args.warmups + args.iterations):
        phase = "warmup" if index < args.warmups else "measured"
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--internal-worker",
            "--phase",
            phase,
            "--mode",
            args.mode,
            "--repo",
            str(repo),
            "--fixture",
            str(fixture),
            "--busy-timeout-ms",
            str(args.busy_timeout_ms),
            "--expected-snapshot-sha256",
            snapshot["snapshot_sha256"],
        ]
        if candidate:
            command.extend(
                (
                    "--expected-candidate-commit",
                    args.expected_candidate_commit,
                )
            )
        for path in setup:
            command.extend(("--setup-fixture", str(path)))
        environment = dict(os.environ)
        environment["PYTHONPATH"] = os.pathsep.join(
            [
                str(repo / "src"),
                environment.get("PYTHONPATH", ""),
            ]
        ).rstrip(os.pathsep)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        completed = subprocess.run(
            command,
            cwd=repo,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode:
            samples.append(
                {
                    "phase": phase,
                    "success": False,
                    "worker_pid": None,
                    "stdout": completed.stdout,
                    "error": completed.stderr,
                }
            )
        else:
            samples.append(json.loads(completed.stdout))
    final = _verify_snapshot(repo, candidate, *binding)
    if final != snapshot:
        raise RuntimeError("target snapshot changed while sampling")
    report = {**base, "availability": "available", "samples": samples}
    try:
        report["statistics"] = calculate_statistics(samples, args.iterations)
        exit_code = 0
    except ValueError as error:
        report["statistics"] = None
        report["error"] = str(error)
        exit_code = 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=MODES)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--setup-fixture", type=Path, action="append", default=[])
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--busy-timeout-ms", type=int, default=100)
    parser.add_argument("--expected-candidate-commit")
    parser.add_argument("--expected-snapshot-sha256")
    parser.add_argument("--output", type=Path)
    hidden = {"help": argparse.SUPPRESS}
    parser.add_argument("--internal-worker", action="store_true", **hidden)
    parser.add_argument("--phase", choices=("warmup", "measured"), **hidden)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.internal_worker:
        if args.phase is None:
            raise SystemExit("--phase is required for an internal worker")
        return _worker(args)
    if args.output is None:
        raise SystemExit("--output is required")
    if args.iterations <= 0 or args.warmups < 0 or args.busy_timeout_ms <= 0:
        raise SystemExit(
            "iterations and busy timeout must be positive; warmups nonnegative"
        )
    if args.mode is None or args.fixture is None:
        raise SystemExit("--mode and --fixture are required")
    if args.mode == "candidate" and not (
        args.expected_candidate_commit and args.expected_snapshot_sha256
    ):
        raise SystemExit("candidate mode requires expected commit and snapshot")
    return _orchestrate(args)


if __name__ == "__main__":
    raise SystemExit(main())
