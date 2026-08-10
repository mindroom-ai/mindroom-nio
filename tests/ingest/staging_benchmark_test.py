from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType
from uuid import UUID

import pytest

from nio.events.misc import BadEvent
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.model import TransportKind
from nio.ingest.ports import NetworkResult
from nio.ingest.sliding import SlidingCursor, SlidingRangeAckMode, SlidingSource
from nio.ingest.source import ClassicCursor, SourceResultKind, canonical_classic_cursor
from nio.ingest.state import SourceState
from nio.responses import ErrorResponse, SlidingSyncResponse, SyncResponse

ROOT = Path(__file__).parents[2]
DATA = ROOT / "tests/data/ingest"
SCRIPT = ROOT / "scripts/benchmark_ingest_staging.py"
CORPUS_RUNNER = ROOT / "tests/ingest/run_staging_benchmark_corpus.py"
CONTROL_COMMIT = "980e53b3a3815592c3bd0dab9e41e2b057e50724"
STREAM_ID = UUID("00000000-0000-0000-0000-000000000008")
CONNECTION_ID = UUID("00000000-0000-0000-0000-000000000009")
ACCOUNT_ID = "@alice:example.org"

FIXTURE_SHA256 = {
    "canonical_1mib.json": "7ad32b5ed6fe73ea24541c38a1b5527c4f85ad9c062078cc3dfa2e73357d6b63",
    "classic_continuation_1.json": "26fa70f4ddbbedd18b8f20e1651c88a5e88aaa4e9cb2aac8af20f8ceff21020d",
    "classic_continuation_2.json": "9ce3164bc8301086e5ea661a2f96fb388528f36f813ff74becd52c664c1b8039",
    "classic_steady_100_rooms_1000_events.json": "02bcdff97bde73aa936cc9368b73a805513b7d3a7d3ac6bbaf0f5e392c75059b",
    "classic_sync.json": "74f8a3fef570030ba1072274344577124a5cc51c2f8c5f6208e39b321dbc7982",
    "sliding_continuation_1.json": "6120478fd48bae09ea8dd4608e858b1c270a2af983334b5157897a6516942c55",
    "sliding_continuation_2.json": "54ef68249b049d2c1bed8e81cd3f10d8656d0e873b74e3e1b8cd1a1c2447d999",
    "sliding_steady_100_rooms_1000_events.json": "a8d7c413e184efc8f48f9b258df816f38ad7425d3231b017ebaca73aaf2163b3",
}
SNAPSHOT_FILES = {
    "scripts/benchmark_ingest_staging.py",
    "src/nio/ingest/state.py",
    "src/nio/store/_sync_journal.py",
    "src/nio/store/_sync_journal_rows.py",
    "tests/ingest/staging_benchmark_test.py",
    "tests/ingest/run_staging_benchmark_corpus.py",
    *(f"tests/data/ingest/{name}" for name in FIXTURE_SHA256),
}


def _load(name: str) -> tuple[bytes, dict[str, object]]:
    raw = (DATA / name).read_bytes()
    return raw, json.loads(raw)


def _module() -> ModuleType:
    assert SCRIPT.is_file(), "benchmark harness is not implemented"
    spec = importlib.util.spec_from_file_location("benchmark_ingest_staging", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _commit_candidate(path: Path) -> tuple[Path, str]:
    subprocess.run(
        ["git", "clone", "--quiet", "--no-hardlinks", str(ROOT), str(path)],
        check=True,
    )
    for relative in SNAPSHOT_FILES:
        target = path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    for command in (
        ("init",),
        ("config", "user.email", "benchmark@example.org"),
        ("config", "user.name", "Benchmark"),
        ("add", "."),
        ("commit", "-m", "frozen candidate"),
    ):
        subprocess.run(["git", *command], cwd=path, capture_output=True, check=True)
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    return path, commit


@pytest.fixture(scope="module")
def candidate_worktree(tmp_path_factory: pytest.TempPathFactory):
    repo, commit = _commit_candidate(
        tmp_path_factory.mktemp("benchmark-candidate") / "repo"
    )
    snapshot = _module()._snapshot(repo, candidate=True, expected_commit=commit)
    return repo, commit, snapshot["snapshot_sha256"]


@pytest.fixture(scope="module")
def control_worktree(tmp_path_factory: pytest.TempPathFactory):
    path = tmp_path_factory.mktemp("benchmark-control") / "repo"
    added = subprocess.run(
        ["git", "worktree", "add", "--detach", str(path), CONTROL_COMMIT],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert added.returncode == 0, added.stderr
    try:
        yield path
    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(path)],
            cwd=ROOT,
            check=True,
        )


def _source(transport: TransportKind):
    if transport is TransportKind.CLASSIC:
        return ClassicSource(
            STREAM_ID,
            ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}"),
            ACCOUNT_ID,
        )
    return SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            timeout_ms=30_000,
            connection_name="benchmark",
            lists_json=b"{}",
            room_subscriptions_json=b"{}",
            extensions_json=b"{}",
            all_rooms_page_size=100,
        ),
        ACCOUNT_ID,
    )


def _state(
    transport: TransportKind, cursor: str | None, request_id: int
) -> SourceState:
    if transport is TransportKind.CLASSIC:
        cursor_json = canonical_classic_cursor(ClassicCursor(cursor))
    else:
        from nio.ingest.sliding import canonical_sliding_cursor

        cursor_json = canonical_sliding_cursor(
            SlidingCursor(
                cursor,
                None if cursor is None else f"to-device-{cursor}",
                CONNECTION_ID,
                "benchmark",
                99 if cursor is None else 100,
                100,
                (
                    SlidingRangeAckMode.UNKNOWN
                    if cursor is None
                    else SlidingRangeAckMode.RESPONSE_BOUND
                ),
                cursor is not None,
            )
        )
    return SourceState(0, transport, cursor_json, request_id, True)


def _normalized(name: str, state: SourceState):
    raw, _ = _load(name)
    source = _source(state.transport_kind)
    request = source.plan_request(state, state.next_request_id)
    assert request is not None
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
    assert result.kind is SourceResultKind.FRAME
    assert result.frame is not None
    return result.frame


def _event_ids(response: SyncResponse | SlidingSyncResponse) -> set[str]:
    if isinstance(response, SyncResponse):
        events = [
            event
            for room in response.rooms.join.values()
            for event in room.timeline.events
        ]
    else:
        events = [event for room in response.rooms.values() for event in room.timeline]
    assert all(not isinstance(event, BadEvent) for event in events)
    return {event.source["event_id"] for event in events}


def test_fixture_bytes_hashes_and_canonical_round_trip_are_frozen() -> None:
    for name, expected_hash in FIXTURE_SHA256.items():
        raw, parsed = _load(name)
        assert hashlib.sha256(raw).hexdigest() == expected_hash
        if name != "classic_sync.json":
            assert not raw.endswith(b"\n")
            assert (
                json.dumps(
                    parsed,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode()
                == raw
            )
    assert len((DATA / "canonical_1mib.json").read_bytes()) == 1_048_576


def test_steady_fixtures_have_complete_native_events_without_fallbacks() -> None:
    classic_raw, classic_dict = _load("classic_steady_100_rooms_1000_events.json")
    sliding_raw, sliding_dict = _load("sliding_steady_100_rooms_1000_events.json")
    classic = SyncResponse.from_dict(json.loads(classic_raw))
    sliding = SlidingSyncResponse.from_dict(json.loads(sliding_raw))
    assert not isinstance(classic, ErrorResponse)
    assert not isinstance(sliding, ErrorResponse)
    assert len(classic.rooms.join) == len(sliding.rooms) == 100
    expected = {
        f"$event-{room:03d}-{event:02d}" for room in range(100) for event in range(10)
    }
    assert _event_ids(classic) == _event_ids(sliding) == expected
    assert (
        sum(
            len(room["timeline"]["events"])
            for room in classic_dict["rooms"]["join"].values()
        )
        == 1_000
    )
    assert (
        sum(len(room["timeline"]) for room in sliding_dict["rooms"].values()) == 1_000
    )


def test_exact_1mib_fixture_measures_complete_timeline_event_work() -> None:
    raw, parsed = _load("canonical_1mib.json")
    response = SyncResponse.from_dict(json.loads(raw))
    assert not isinstance(response, ErrorResponse)
    events = next(iter(response.rooms.join.values())).timeline.events
    assert (
        len(parsed["rooms"]["join"]["!exact:example.org"]["timeline"]["events"])
        == 4_200
    )
    assert len(events) == 4_200
    assert all(not isinstance(event, BadEvent) for event in events)
    assert {event.source["event_id"] for event in events} == {
        f"$exact-{index:04d}" for index in range(4_200)
    }


def test_classic_and_sliding_steady_transport_neutral_frames_match_exactly() -> None:
    classic = _normalized(
        "classic_steady_100_rooms_1000_events.json",
        _state(TransportKind.CLASSIC, "classic-pos-1", 1),
    )
    sliding = _normalized(
        "sliding_steady_100_rooms_1000_events.json",
        _state(TransportKind.SLIDING, "sliding-pos-1", 1),
    )
    projection = lambda frame: (
        frame.to_device_json,
        frame.device_list_delta_json,
        frame.one_time_key_counts_json,
        frame.unused_fallback_key_types_json,
        frame.room_segments,
        frame.ephemeral_json,
        frame.global_account_data_json,
        frame.presence_json,
    )
    assert projection(classic) == projection(sliding)


@pytest.mark.parametrize(
    ("name", "transport", "before", "after"),
    [
        ("classic_continuation_1.json", TransportKind.CLASSIC, None, "classic-pos-1"),
        (
            "classic_continuation_2.json",
            TransportKind.CLASSIC,
            "classic-pos-1",
            "classic-pos-2",
        ),
        ("sliding_continuation_1.json", TransportKind.SLIDING, None, "sliding-pos-1"),
        (
            "sliding_continuation_2.json",
            TransportKind.SLIDING,
            "sliding-pos-1",
            "sliding-pos-2",
        ),
    ],
)
def test_continuation_fixture_cursor_contract(
    name: str,
    transport: TransportKind,
    before: str | None,
    after: str,
) -> None:
    frame = _normalized(name, _state(transport, before, int(before is not None)))
    parsed_cursor = json.loads(frame.candidate_cursor_json)
    assert (
        parsed_cursor["next_batch" if transport is TransportKind.CLASSIC else "pos"]
        == after
    )


def test_statistics_exclude_warmups_use_even_median_and_nearest_rank_p95() -> None:
    benchmark = _module()
    samples = [
        {
            "phase": "warmup",
            "success": True,
            "wall_ns": 999,
            "cpu_ns": 999,
            "peak_rss_bytes": 999,
            "wal_bytes": 999,
        },
        *[
            {
                "phase": "measured",
                "success": True,
                "wall_ns": value,
                "cpu_ns": value + 1,
                "peak_rss_bytes": value + 2,
                "wal_bytes": value + 3,
            }
            for value in range(1, 31)
        ],
    ]
    stats = benchmark.calculate_statistics(samples, expected_measured=30)
    assert stats == {
        "measured_samples": 30,
        "median_wall_ns": 15.5,
        "p95_wall_ns": 29,
        "median_cpu_ns": 16.5,
        "peak_rss_bytes": 32,
        "max_wal_bytes": 33,
    }
    samples[-1] = {**samples[-1], "success": False, "error": "retained"}
    with pytest.raises(ValueError, match="all measured samples must succeed"):
        benchmark.calculate_statistics(samples, expected_measured=30)
    assert samples[-1]["error"] == "retained"


def test_candidate_snapshot_manifest_binds_exact_tree_and_worker_rejects_drift(
    candidate_worktree: tuple[Path, str, str],
) -> None:
    benchmark = _module()
    repo, commit, digest = candidate_worktree
    snapshot = benchmark._verify_snapshot(repo, True, commit, digest)
    assert snapshot["commit"] == commit
    assert snapshot["clean"] is True
    assert snapshot["tracked_diff_sha256"] == hashlib.sha256(b"").hexdigest()
    assert set(snapshot["files"]) == SNAPSHOT_FILES
    assert snapshot["excluded_paths"] == []
    payload = {
        key: value for key, value in snapshot.items() if key != "snapshot_sha256"
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    assert snapshot["snapshot_sha256"] == hashlib.sha256(encoded).hexdigest()

    rejected = subprocess.run(
        [
            sys.executable,
            str(repo / "scripts/benchmark_ingest_staging.py"),
            "--internal-worker",
            "--phase",
            "measured",
            "--mode",
            "candidate",
            "--repo",
            str(repo),
            "--fixture",
            str(repo / "tests/data/ingest/classic_continuation_1.json"),
            "--expected-candidate-commit",
            commit,
            "--expected-snapshot-sha256",
            "0" * 64,
        ],
        cwd=repo,
        env={
            **os.environ,
            "PYTHONPATH": str(repo / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        text=True,
        capture_output=True,
        check=False,
    )
    assert rejected.returncode != 0
    assert "snapshot" in rejected.stderr.lower()


def test_candidate_snapshot_accepts_clean_commit_and_rejects_unexpected_changes(
    tmp_path: Path,
) -> None:
    benchmark = _module()
    repo, commit = _commit_candidate(tmp_path / "candidate")
    clean = benchmark._snapshot(repo, candidate=True, expected_commit=commit)
    digest = clean["snapshot_sha256"]
    assert benchmark._verify_snapshot(repo, True, commit, digest) == clean
    assert clean["clean"] is True
    assert clean["tracked_diff_sha256"] == hashlib.sha256(b"").hexdigest()
    assert clean["files"] == {
        relative: hashlib.sha256((repo / relative).read_bytes()).hexdigest()
        for relative in SNAPSHOT_FILES
    }

    clone = tmp_path / "clean-clone"
    subprocess.run(["git", "clone", "--quiet", str(repo), str(clone)], check=True)
    assert not (clone / ".superpowers/sdd/.gitignore").exists()
    assert benchmark._verify_snapshot(clone, True, commit, digest) == clean

    fixture = clone / "tests/data/ingest/classic_continuation_1.json"
    fixture.write_bytes(fixture.read_bytes() + b" ")
    with pytest.raises(RuntimeError, match="clean|snapshot"):
        benchmark._verify_snapshot(clone, True, commit, digest)

    (repo / "unexpected.txt").write_text("committed drift")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", "unexpected"], cwd=repo, capture_output=True, check=True
    )
    with pytest.raises(RuntimeError, match="commit"):
        benchmark._verify_snapshot(repo, True, commit, digest)


def test_worker_rejects_manifest_mutate_and_restore_during_sample(
    candidate_worktree: tuple[Path, str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    benchmark = _module()
    repo, commit, digest = candidate_worktree
    target = repo / "tests/data/ingest/classic_continuation_1.json"
    fake_nio = ModuleType("nio")
    fake_nio.__file__ = str(repo / "src/nio/__init__.py")
    monkeypatch.setitem(sys.modules, "nio", fake_nio)
    monkeypatch.setattr(benchmark, "_deny_network", lambda: None)

    def mutate_and_restore(*_args):
        replacement = target.with_suffix(".replacement")
        replacement.write_bytes(target.read_bytes())
        os.replace(replacement, target)
        return {}

    monkeypatch.setattr(benchmark, "_candidate_sample", mutate_and_restore)
    args = argparse.Namespace(
        repo=repo,
        mode="candidate",
        expected_candidate_commit=commit,
        expected_snapshot_sha256=digest,
        fixture=target,
        setup_fixture=[],
        busy_timeout_ms=100,
        phase="measured",
    )
    with pytest.raises(RuntimeError, match="changed while sampling"):
        benchmark._worker(args)


def test_environment_has_real_cpu_filesystem_and_classic_busy_timeout() -> None:
    benchmark = _module()
    environment = benchmark._environment(ROOT, 100)
    cpuinfo = Path("/proc/cpuinfo").read_text().split("\n\n", 1)[0]
    cpu_fields = dict(
        (key.strip(), value.strip())
        for line in cpuinfo.splitlines()
        if ":" in line
        for key, value in (line.split(":", 1),)
    )
    for field in ("CPU implementer", "CPU architecture", "CPU part", "CPU revision"):
        assert (
            f"{field.removeprefix('CPU ').lower()}={cpu_fields[field]}"
            in environment["cpu_model"]
        )
    filesystem = subprocess.run(
        ["findmnt", "-n", "-o", "FSTYPE", "-T", str(ROOT)],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    assert environment["filesystem_type"] == filesystem == "ext4"
    assert environment["busy_timeout_ms"] == 100
    sample = benchmark._classic_durability_sample(
        DATA / "classic_continuation_1.json", [], 100, "measured"
    )
    assert sample["busy_timeout_ms"] == 100


def test_candidate_timer_includes_normalization_but_not_request_planning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark = _module()
    real_finish = benchmark._finish_proposal
    real_plan = benchmark._plan_proposal

    def slow_finish(planned, raw):
        time.sleep(0.02)
        return real_finish(planned, raw)

    def slow_plan(journal, config):
        time.sleep(0.02)
        return real_plan(journal, config)

    monkeypatch.setattr(benchmark, "_finish_proposal", slow_finish)
    monkeypatch.setattr(benchmark, "_plan_proposal", slow_plan)
    sample = benchmark._candidate_sample(
        DATA / "classic_continuation_1.json", [], 100, "measured"
    )
    assert 20_000_000 <= sample["wall_ns"] < 40_000_000


def test_classic_durability_records_real_redacted_sql() -> None:
    benchmark = _module()
    sample = benchmark._classic_durability_sample(
        DATA / "classic_continuation_1.json", [], 100, "measured"
    )
    assert sample["statement_count"] >= 1
    assert sample["transaction_count"] >= 0
    assert sample["sql_trace"]
    assert all("classic-pos-1" not in statement for statement in sample["sql_trace"])


def test_cli_runs_fresh_workers_and_emits_truthful_report(
    tmp_path: Path, candidate_worktree: tuple[Path, str, str]
) -> None:
    repo, commit, digest = candidate_worktree
    data = repo / "tests/data/ingest"
    output = tmp_path / "report.json"
    common = [
        sys.executable,
        str(repo / "scripts/benchmark_ingest_staging.py"),
        "--mode",
        "candidate",
        "--repo",
        str(repo),
        "--fixture",
        str(data / "classic_continuation_2.json"),
        "--setup-fixture",
        str(data / "classic_continuation_1.json"),
        "--iterations",
        "2",
        "--warmups",
        "1",
        "--busy-timeout-ms",
        "100",
        "--output",
        str(output),
    ]
    unbound = subprocess.run(
        common, cwd=repo, text=True, capture_output=True, check=False
    )
    assert unbound.returncode != 0
    completed = subprocess.run(
        [
            *common,
            "--expected-candidate-commit",
            commit,
            "--expected-snapshot-sha256",
            digest,
        ],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(output.read_bytes())
    assert report["schema_version"] == 1
    assert report["fixed_control_commit"] == CONTROL_COMMIT
    assert report["mode"] == "candidate"
    assert report["snapshot"]["snapshot_sha256"]
    assert report["snapshot"]["snapshot_sha256"] == digest
    assert report["snapshot"]["commit"] == commit
    assert report["snapshot"]["files"]
    assert report["environment"]["cpu_model"]
    assert report["environment"]["filesystem_type"]
    assert report["environment"]["busy_timeout_ms"] == 100
    assert report["fixture"]["sha256"] == FIXTURE_SHA256["classic_continuation_2.json"]
    assert report["setup_fixtures"] == [
        {
            "path": str((data / "classic_continuation_1.json").resolve()),
            "sha256": FIXTURE_SHA256["classic_continuation_1.json"],
        }
    ]
    assert [sample["phase"] for sample in report["samples"]] == [
        "warmup",
        "measured",
        "measured",
    ]
    assert len({sample["worker_pid"] for sample in report["samples"]}) == 3
    assert all(sample["success"] for sample in report["samples"])
    assert all(sample["network_denied"] for sample in report["samples"])
    assert all(
        sample["snapshot_sha256"] == report["snapshot"]["snapshot_sha256"]
        for sample in report["samples"]
    )
    assert all(sample["reopen_verification"]["success"] for sample in report["samples"])
    assert all(
        sample["target_import_path"].startswith(str(repo / "src"))
        for sample in report["samples"]
    )
    assert all(
        sample["canonical_bytes"]
        == (data / "classic_continuation_2.json").stat().st_size
        for sample in report["samples"]
    )
    assert report["statistics"]["measured_samples"] == 2
    assert (
        subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=repo,
            text=True,
            capture_output=True,
            check=True,
        ).stdout
        == ""
    )


def test_cli_rejects_invalid_mode_and_sliding_durability_claim(
    tmp_path: Path, control_worktree: Path
) -> None:
    common = [
        sys.executable,
        str(SCRIPT),
        "--repo",
        str(control_worktree),
        "--fixture",
        str(DATA / "sliding_continuation_1.json"),
        "--iterations",
        "2",
        "--warmups",
        "1",
        "--output",
        str(tmp_path / "report.json"),
    ]
    invalid = subprocess.run(
        [*common, "--mode", "control"], text=True, capture_output=True, check=False
    )
    assert invalid.returncode == 2
    unavailable = subprocess.run(
        [*common, "--mode", "classic_legacy_cursor_durability"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert unavailable.returncode == 0, unavailable.stderr
    report = json.loads((tmp_path / "report.json").read_bytes())
    assert report["availability"] == "not_available"
    assert report["reason"] == "fixed_base_has_no_durable_sliding_pos"
    assert report["samples"] == []
    assert report["snapshot"]["commit"] == CONTROL_COMMIT
    assert report["snapshot"]["clean"] is True
    assert report["snapshot"]["head_tree"]


def test_candidate_stage_uses_one_transaction_six_statements_and_three_dml(
    tmp_path: Path, candidate_worktree: tuple[Path, str, str]
) -> None:
    repo, commit, digest = candidate_worktree
    data = repo / "tests/data/ingest"
    observed = []
    for fixture in (
        "classic_continuation_2.json",
        "classic_steady_100_rooms_1000_events.json",
    ):
        output = tmp_path / f"{fixture}.report.json"
        completed = subprocess.run(
            [
                sys.executable,
                str(repo / "scripts/benchmark_ingest_staging.py"),
                "--mode",
                "candidate",
                "--repo",
                str(repo),
                "--fixture",
                str(data / fixture),
                "--setup-fixture",
                str(data / "classic_continuation_1.json"),
                "--expected-candidate-commit",
                commit,
                "--expected-snapshot-sha256",
                digest,
                "--iterations",
                "1",
                "--warmups",
                "0",
                "--output",
                str(output),
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        sample = json.loads(output.read_bytes())["samples"][0]
        dml = [
            statement
            for statement in sample["sql_trace"]
            if statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        ]
        assert sample["transaction_count"] == 1
        assert len(dml) == 3
        assert sample["statement_count"] <= 6
        observed.append(sample["statement_count"])
    assert observed == [6, 6]


def test_full_corpus_cli_generates_frozen_matrix_gates_and_diagnostics(
    tmp_path: Path,
    control_worktree: Path,
    candidate_worktree: tuple[Path, str, str],
) -> None:
    repo, commit, digest = candidate_worktree
    output = tmp_path / "full.json"
    markdown = tmp_path / "full.md"
    completed = subprocess.run(
        [
            sys.executable,
            str(repo / "tests/ingest/run_staging_benchmark_corpus.py"),
            "--repo",
            str(repo),
            "--control-repo",
            str(control_worktree),
            "--expected-candidate-commit",
            commit,
            "--expected-candidate-snapshot-sha256",
            digest,
            "--iterations",
            "2",
            "--warmups",
            "1",
            "--busy-timeout-ms",
            "100",
            "--output",
            str(output),
            "--markdown-output",
            str(markdown),
        ],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(output.read_bytes())
    assert report["schema_version"] == 1
    assert report["overall_passed"] is True
    assert report["fixed_control_commit"] == CONTROL_COMMIT
    assert report["candidate_commit"] == commit
    assert report["snapshots"]["candidate"]["snapshot_sha256"] == digest
    assert set(report["snapshots"]) == {"candidate", "fixed_control"}
    assert set(report["cases"]) == {
        "legacy_noncanonical_cold",
        "classic_continuation_1",
        "classic_continuation_2",
        "classic_steady_100_rooms_1000_events",
        "canonical_1mib",
        "sliding_continuation_1",
        "sliding_continuation_2",
        "sliding_steady_100_rooms_1000_events",
    }
    for case in report["cases"].values():
        assert case["gates"]["case_passed"] is True
        assert case["modes"]["candidate"]["statistics"]["measured_samples"] == 2
        assert (
            case["modes"]["legacy_normalization_floor"]["statistics"][
                "measured_samples"
            ]
            == 2
        )
        candidate_snapshot = report["snapshots"]["candidate"]["snapshot_sha256"]
        assert all(
            sample["snapshot_sha256"] == candidate_snapshot
            for sample in case["modes"]["candidate"]["samples"]
        )
    for name in (
        "sliding_continuation_1",
        "sliding_continuation_2",
        "sliding_steady_100_rooms_1000_events",
    ):
        durability = report["cases"][name]["modes"]["classic_legacy_cursor_durability"]
        assert durability["availability"] == "not_available"
        assert durability["reason"] == "fixed_base_has_no_durable_sliding_pos"
    classic = report["cases"]["classic_continuation_1"]["modes"][
        "classic_legacy_cursor_durability"
    ]
    assert classic["environment"]["busy_timeout_ms"] == 100
    assert all(sample["busy_timeout_ms"] == 100 for sample in classic["samples"])
    assert report["diagnostics"]["external_lock"]["passed"] is True
    assert report["diagnostics"]["full_capacity"]["passed"] is True
    assert report["diagnostics"]["overall_passed"] is True
    assert report["size_and_forecast"]["cumulative_production_net"] <= 6_240
    assert "Snapshot binding" in markdown.read_text()
    assert report["snapshots"]["candidate"]["snapshot_sha256"] in markdown.read_text()
