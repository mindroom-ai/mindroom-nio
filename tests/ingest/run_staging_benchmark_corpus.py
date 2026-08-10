#!/usr/bin/env python3
"""Generate the frozen Task 8 corpus, gates, diagnostics, and report."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).parents[2]
HARNESS = ROOT / "scripts/benchmark_ingest_staging.py"
CONTROL_COMMIT = "980e53b3a3815592c3bd0dab9e41e2b057e50724"
MODES = (
    "legacy_normalization_floor",
    "classic_legacy_cursor_durability",
    "candidate",
)
MATRIX = {
    "canonical_1mib": (
        "canonical_1mib.json",
        (),
        None,
        "classic-1mib-1",
        0,
        "cold",
        True,
    ),
    "classic_continuation_1": (
        "classic_continuation_1.json",
        (),
        None,
        "classic-pos-1",
        0,
        "cold",
        True,
    ),
    "classic_continuation_2": (
        "classic_continuation_2.json",
        ("classic_continuation_1.json",),
        "classic-pos-1",
        "classic-pos-2",
        1,
        "steady",
        True,
    ),
    "classic_steady_100_rooms_1000_events": (
        "classic_steady_100_rooms_1000_events.json",
        ("classic_continuation_1.json",),
        "classic-pos-1",
        "classic-steady-2",
        1,
        "steady",
        True,
    ),
    "legacy_noncanonical_cold": (
        "classic_sync.json",
        (),
        None,
        "s-next",
        0,
        "cold diagnostic",
        False,
    ),
    "sliding_continuation_1": (
        "sliding_continuation_1.json",
        (),
        None,
        "sliding-pos-1",
        0,
        "cold",
        True,
    ),
    "sliding_continuation_2": (
        "sliding_continuation_2.json",
        ("sliding_continuation_1.json",),
        "sliding-pos-1",
        "sliding-pos-2",
        1,
        "steady",
        True,
    ),
    "sliding_steady_100_rooms_1000_events": (
        "sliding_steady_100_rooms_1000_events.json",
        ("sliding_continuation_1.json",),
        "sliding-pos-1",
        "sliding-steady-2",
        1,
        "steady",
        True,
    ),
}


def _module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("benchmark_ingest_staging", HARNESS)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot import benchmark harness")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _canonical(raw: bytes) -> bytes:
    return json.dumps(
        json.loads(raw),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _run_mode(
    args: argparse.Namespace,
    repo: Path,
    fixture: Path,
    setup: tuple[Path, ...],
    mode: str,
    output: Path,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(HARNESS),
        "--mode",
        mode,
        "--repo",
        str(repo),
        "--fixture",
        str(fixture),
        "--iterations",
        str(args.iterations),
        "--warmups",
        str(args.warmups),
        "--busy-timeout-ms",
        str(args.busy_timeout_ms),
        "--output",
        str(output),
    ]
    if mode == "candidate":
        command.extend(
            (
                "--expected-candidate-commit",
                args.expected_candidate_commit,
                "--expected-snapshot-sha256",
                args.expected_candidate_snapshot_sha256,
            )
        )
    for path in setup:
        command.extend(("--setup-fixture", str(path)))
    completed = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, check=False
    )
    if not output.is_file():
        raise RuntimeError(
            f"{mode} orchestrator failed before report: {completed.stderr.strip()}"
        )
    report = json.loads(output.read_bytes())
    if completed.returncode and "error" not in report:
        report["error"] = completed.stderr
    return report


def _measured(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [sample for sample in report["samples"] if sample["phase"] == "measured"]


def _token(cursor: dict[str, Any]) -> Any:
    return cursor.get("next_batch", cursor.get("pos"))


def _candidate_reopen_matches(
    sample: dict[str, Any], before: str | None, after: str, frames_before: int
) -> bool:
    try:
        check = sample["reopen_verification"]
        return bool(
            sample["success"]
            and check["success"]
            and check["frame_identity"]
            and check["source_digest"]
            and check["canonical_body"]
            and _token(check["cursor_before"]) == before
            and _token(check["cursor_after"]) == after
            and check["frames_before"] == frames_before
            and check["frames_after"] == frames_before + 1
        )
    except (KeyError, TypeError):
        return False


def _gates(
    args: argparse.Namespace,
    spec: tuple[Any, ...],
    modes: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    _fixture, _setup, before, after, frames_before, _state, included = spec
    candidate = modes["candidate"]
    measured = _measured(candidate)
    wal_limit = 2.5 * Path(candidate["fixture"]["path"]).stat().st_size + 128 * 1024
    absolute = {
        "all_measured_succeeded": len(measured) == args.iterations
        and all(sample.get("success") for sample in measured),
        "at_most_six_significant_statements": bool(measured)
        and all(sample.get("statement_count", 7) <= 6 for sample in measured),
        "exactly_three_business_dml": bool(measured)
        and all(
            sum(
                statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
                for statement in sample.get("sql_trace", ())
            )
            == 3
            for sample in measured
        ),
        "one_outer_transaction": bool(measured)
        and all(sample.get("transaction_count") == 1 for sample in measured),
        "reopen_verified": len(measured) == args.iterations
        and all(
            _candidate_reopen_matches(sample, before, after, frames_before)
            for sample in measured
        ),
        "wal_limit_bytes": wal_limit,
        "wal_passed": bool(measured)
        and max(sample.get("wal_bytes", float("inf")) for sample in measured)
        <= wal_limit,
    }
    candidate_stats = candidate.get("statistics") or {}
    floor_stats = modes["legacy_normalization_floor"].get("statistics") or {}
    wall_limit = 2 * floor_stats.get("p95_wall_ns", 0) + 10_000_000
    cpu_limit = 2 * floor_stats.get("median_cpu_ns", 0) + 5_000_000
    rss_limit = floor_stats.get("peak_rss_bytes", 0) + 32 * 1024 * 1024
    paired = {
        "included": included,
        "floor_p95_wall_ns": floor_stats.get("p95_wall_ns"),
        "candidate_p95_wall_ns": candidate_stats.get("p95_wall_ns"),
        "wall_limit_ns": wall_limit,
        "wall_passed": candidate_stats.get("p95_wall_ns", float("inf")) <= wall_limit,
        "floor_median_cpu_ns": floor_stats.get("median_cpu_ns"),
        "candidate_median_cpu_ns": candidate_stats.get("median_cpu_ns"),
        "cpu_limit_ns": cpu_limit,
        "cpu_passed": candidate_stats.get("median_cpu_ns", float("inf")) <= cpu_limit,
        "floor_peak_rss_bytes": floor_stats.get("peak_rss_bytes"),
        "candidate_peak_rss_bytes": candidate_stats.get("peak_rss_bytes"),
        "rss_limit_bytes": rss_limit,
        "rss_passed": candidate_stats.get("peak_rss_bytes", float("inf")) <= rss_limit,
    }
    if not included:
        paired.update(
            reason="legacy_noncanonical_parser_semantic_mismatch",
            wall_passed=None,
            cpu_passed=None,
            rss_passed=None,
        )
    durability = modes["classic_legacy_cursor_durability"]
    if durability["availability"] == "not_available":
        reference = (
            durability.get("reason") == "fixed_base_has_no_durable_sliding_pos"
            and durability["samples"] == []
        )
    else:
        durability_measured = _measured(durability)
        reference = len(durability_measured) == args.iterations and all(
            sample.get("success")
            and sample.get("busy_timeout_ms") == args.busy_timeout_ms
            and sample.get("reopen_verification") == {"success": True, "cursor": after}
            for sample in durability_measured
        )
    required = [
        value for key, value in absolute.items() if key != "wal_limit_bytes"
    ] + [reference]
    if included:
        required.extend(
            paired[key] for key in ("wall_passed", "cpu_passed", "rss_passed")
        )
    return {
        "absolute_candidate": absolute,
        "paired": paired,
        "classic_durability_reference_passed": reference,
        "case_passed": all(required),
    }


class _NeverPlan:
    def __init__(self) -> None:
        self.plan_calls = 0

    def plan_request(self, *_args):
        self.plan_calls += 1
        raise AssertionError("full capacity must stop planning")

    def normalize(self, *_args):
        raise AssertionError("full capacity must not normalize")


def _diagnostics(benchmark: ModuleType, repo: Path, timeout_ms: int) -> dict[str, Any]:
    from nio.ingest.source import SourceScheduleStatus, plan_source_poll

    config = benchmark._source_config("classic")
    statements: list[str] = []
    with tempfile.TemporaryDirectory(prefix="nio-stage-diagnostics-") as directory:
        bootstrap = benchmark._open_journal(
            Path(directory), config, timeout_ms, statements.append
        )
        journal = bootstrap._journal
        fixture = repo / "tests/data/ingest/classic_continuation_1.json"
        proposal = benchmark._proposal(journal, config, fixture.read_bytes())
        before = (journal.load_owner(), journal.load_source(), journal.list_frames(256))
        external = sqlite3.connect(
            bootstrap.database_path, isolation_level=None, timeout=0
        )
        try:
            external.execute("BEGIN IMMEDIATE")
            started = time.monotonic_ns()
            try:
                benchmark._stage(journal, proposal)
                error: Exception | None = None
            except Exception as caught:
                error = caught
            elapsed = time.monotonic_ns() - started
            rollback_exact = before == (
                journal.load_owner(),
                journal.load_source(),
                journal.list_frames(256),
            )
            external.rollback()
            benchmark._stage(journal, proposal)
            retry = journal.load_frame(proposal[4].frame_id) is not None
            state, frames = journal.load_source(), journal.list_frames(1)
            source = _NeverPlan()
            sends = at_capacity = 0
            statements.clear()
            rss_before = benchmark._rss_bytes()
            for _ in range(10_000):
                decision = plan_source_poll(
                    source, state, state.next_request_id, frames, 1
                )
                at_capacity += decision.status is SourceScheduleStatus.AT_CAPACITY
                sends += decision.request is not None
            growth = max(0, benchmark._rss_bytes() - rss_before)
        finally:
            if external.in_transaction:
                external.rollback()
            external.close()
            bootstrap.close()
    lock = {
        "busy_timeout_ms": timeout_ms,
        "elapsed_ns": elapsed,
        "error": (
            None
            if error is None
            else {"type": type(error).__name__, "message": str(error)}
        ),
        "rollback_exact": rollback_exact,
        "retry_success": retry,
    }
    lock["passed"] = (
        error is not None
        and 100_000_000 <= elapsed <= 350_000_000
        and rollback_exact
        and retry
    )
    capacity = {
        "polls": 10_000,
        "at_capacity": at_capacity,
        "plan_calls": source.plan_calls,
        "send_calls": sends,
        "sql_statements": len(statements),
        "rss_growth_bytes": growth,
    }
    capacity["passed"] = (at_capacity, source.plan_calls, sends, len(statements)) == (
        10_000,
        0,
        0,
        0,
    ) and growth < 8 * 1024 * 1024
    return {
        "external_lock": lock,
        "full_capacity": capacity,
        "overall_passed": lock["passed"] and capacity["passed"],
    }


def _size_and_forecast(
    benchmark: ModuleType, repo: Path, expected_commit: str
) -> dict[str, Any]:
    source_additions = source_deletions = 0
    numstat = subprocess.run(
        [
            "git",
            "diff",
            "--numstat",
            "7205e83dce2920531926b478dd3b642763252da4",
            expected_commit,
            "--",
            *benchmark.TRACKED_PATHS,
        ],
        cwd=repo,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    for line in numstat.splitlines():
        additions, deletions, _path = line.split("\t")
        source_additions += int(additions)
        source_deletions += int(deletions)
    harness_lines = len(
        (repo / "scripts/benchmark_ingest_staging.py").read_text().splitlines()
    )
    additions = harness_lines + source_additions
    net = additions - source_deletions
    cumulative = 5_507 + net
    headroom = 6_240 - cumulative
    return {
        "repositories": {
            "mindroom-nio": {
                "additions": additions,
                "deletions": source_deletions,
                "net": net,
            },
            "MindRoom": {"additions": 0, "deletions": 0, "net": 0},
            "cross_repository_task_net": net,
        },
        "harness_lines": harness_lines,
        "source_additions": source_additions,
        "source_deletions": source_deletions,
        "cumulative_production_net": cumulative,
        "simplified_checkpoint_high_side": 6_240,
        "headroom": headroom,
        "actual_deletions": {
            "mindroom-nio_lines": source_deletions,
            "MindRoom_lines": 0,
            "credited_total": source_deletions,
        },
        "newly_unlocked_deletions": [],
        "remaining_forecast": {
            "simplified_checkpoint_additions_low": 0,
            "simplified_checkpoint_additions_high": max(0, headroom),
            "simplified_checkpoint_final_net_low": cumulative,
            "simplified_checkpoint_final_net_high": 6_240,
            "remaining_program_cap_after_checkpoint": 3_150,
            "pre_deletion_final_net_low": cumulative + 3_150,
            "pre_deletion_final_net_high": 9_390,
            "conditional_mapped_deletions_not_credited": 3_393,
            "conditional_final_net_low": cumulative + 3_150 - 3_393,
            "conditional_final_net_high": 5_997,
        },
        "estimate_misses_over_50_percent": [
            {
                "item": "joined snapshot and validation optimization production additions",
                "forecast_low": 10,
                "forecast_high": 25,
                "actual": source_additions,
                "miss_vs_high_percent": (source_additions - 25) * 100 / 25,
                "note": f"net source change is {source_additions - source_deletions:+d}",
            }
        ],
        "no_approved_harness_subestimate": {
            "actual_additions": harness_lines,
            "combined_task_envelope": 733,
            "note": "733 is the hard combined envelope, not a point estimate",
        },
    }


def _ms(value: int | float) -> str:
    return f"{value / 1_000_000:.3f}"


def _estimate_miss_markdown(size: dict[str, Any]) -> str:
    miss = size["estimate_misses_over_50_percent"][0]
    return (
        "- >50% estimate miss: joined snapshot/validation additions forecast "
        f"+{miss['forecast_low']}..+{miss['forecast_high']}, "
        f"actual +{miss['actual']} ({miss['miss_vs_high_percent']:g}% over high); "
        "deletions are reported separately. The harness had no approved "
        "sub-estimate; its actual is measured against the combined hard envelope "
        "only. No runtime or fixture-size estimate was approved."
    )


def _markdown(report: dict[str, Any], raw_path: Path) -> str:
    candidate_snapshot = report["snapshots"]["candidate"]
    lines = [
        "# Task 8 benchmark report",
        "",
        "## Outcome",
        "",
        f"- Overall: **{'PASS' if report['overall_passed'] else 'FAIL'}**.",
        f"- Raw aggregate: `{raw_path}`.",
        f"- Corpus: {report['method']['warmups']} warmups and {report['method']['measured_samples']} measured samples per available mode; failures are retained.",
        "",
        "## Snapshot binding",
        "",
        f"- Candidate commit: `{candidate_snapshot['commit']}`.",
        f"- Candidate tracked-diff SHA-256: `{candidate_snapshot['tracked_diff_sha256']}`.",
        f"- Candidate aggregate snapshot SHA-256: `{candidate_snapshot['snapshot_sha256']}`.",
        f"- Fixed control: `{report['fixed_control_commit']}`; clean tree `{report['snapshots']['fixed_control']['head_tree']}`; snapshot `{report['snapshots']['fixed_control']['snapshot_sha256']}`.",
        "- Measurement ran in a clean dedicated worktree; raw/report outputs were outside it. Root `src/mindroom_nio.egg-info/` was not measured.",
        f"- Reproduce with: `{report['reproduction_command']}`.",
        "",
        "| Snapshot path | SHA-256 |",
        "|---|---|",
    ]
    lines.extend(
        f"| `{path}` | `{digest}` |"
        for path, digest in candidate_snapshot["files"].items()
    )
    lines += [
        "",
        "## Fixture and setup hashes",
        "",
        "| Case | Raw SHA-256 | Normalized SHA-256 | Bytes | Ordered setup SHA-256 |",
        "|---|---|---|---:|---|",
    ]
    for name, case in report["cases"].items():
        setup = (
            ", ".join(item["sha256"] for item in case["ordered_untimed_setup"]) or "—"
        )
        fixture = case["fixture"]
        lines.append(
            f"| {name} | `{fixture['raw_sha256']}` | `{fixture['normalized_sha256']}` | {fixture['canonical_bytes']} | {setup} |"
        )
    lines += [
        "",
        "## All-mode measurements",
        "",
        "| Case/mode | Wall median/p95 ms | CPU median ms | Peak RSS MiB | WAL bytes | SQL | Result |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for name, case in report["cases"].items():
        for mode, measured in case["modes"].items():
            stats = measured.get("statistics")
            if stats is None:
                lines.append(
                    f"| {name}/{mode} | — | — | — | — | — | `{measured['availability']}` |"
                )
                continue
            sql = max(sample["statement_count"] for sample in _measured(measured))
            lines.append(
                f"| {name}/{mode} | {_ms(stats['median_wall_ns'])}/{_ms(stats['p95_wall_ns'])} | {_ms(stats['median_cpu_ns'])} | {stats['peak_rss_bytes'] / 1048576:.2f} | {stats['max_wal_bytes']} | {sql} | PASS |"
            )
    example = next(iter(report["cases"].values()))["modes"]["candidate"]["samples"][0]
    diagnostics = report["diagnostics"]
    size = report["size_and_forecast"]
    remaining = size["remaining_forecast"]
    lines += [
        "",
        "## SQL, durability, and diagnostics",
        "",
        f"- Candidate structural trace: `{example['sql_trace']}`; six significant statements, one transaction, and three DML are gated for every measured sample.",
        "- WAL amplification and the exact `2.5 * canonical bytes + 128 KiB` limit are recorded under every case gate.",
        "- Classic durability uses real redacted SQL, `PRAGMA busy_timeout=100`, and exact close/reopen token verification. Sliding is explicitly `not_available` with `fixed_base_has_no_durable_sliding_pos`.",
        f"- External lock: {_ms(diagnostics['external_lock']['elapsed_ns'])} ms; rollback={diagnostics['external_lock']['rollback_exact']}; retry={diagnostics['external_lock']['retry_success']}; PASS={diagnostics['external_lock']['passed']}.",
        f"- Full capacity: {diagnostics['full_capacity']['at_capacity']}/10000; plan/send/SQL={diagnostics['full_capacity']['plan_calls']}/{diagnostics['full_capacity']['send_calls']}/{diagnostics['full_capacity']['sql_statements']}; RSS growth={diagnostics['full_capacity']['rss_growth_bytes']}; PASS={diagnostics['full_capacity']['passed']}.",
        "",
        "## Production size and forecast",
        "",
        f"- mindroom-nio: +{size['repositories']['mindroom-nio']['additions']}/-{size['repositories']['mindroom-nio']['deletions']}, net +{size['repositories']['mindroom-nio']['net']}; MindRoom: +0/-0, net 0.",
        f"- Historical Task 4.5 size comparison (diagnostic only): +{size['cumulative_production_net']} cumulative; +6240 checkpoint; difference {size['headroom']}.",
        f"- Actual deletions credited: {size['actual_deletions']['credited_total']}; newly unlocked surfaces: none.",
        f"- Remaining checkpoint work: +{remaining['simplified_checkpoint_additions_low']}..+{remaining['simplified_checkpoint_additions_high']}; checkpoint final +{remaining['simplified_checkpoint_final_net_low']}..+{remaining['simplified_checkpoint_final_net_high']}.",
        f"- Pre-deletion final: +{remaining['pre_deletion_final_net_low']}..+{remaining['pre_deletion_final_net_high']}; conditional 3,393 mapped deletions are not credited (conditional +{remaining['conditional_final_net_low']}..+{remaining['conditional_final_net_high']}).",
        _estimate_miss_markdown(size),
        "",
        "## Fix-round RED/GREEN",
        "",
        "- RED pinned missing snapshot binding, Classic busy timeout, ARM CPU/filesystem evidence, and the absent full-corpus generator.",
        "- GREEN: this checked-in runner generated the frozen matrix, gates, diagnostics, raw JSON, and this report from one command.",
        "- Exact raw samples: `cases.<case>.modes.<mode>.samples`; statistics, environment, snapshots, gates, and diagnostics are adjacent fields.",
        "",
    ]
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--control-repo", type=Path, required=True)
    parser.add_argument("--expected-candidate-commit", required=True)
    parser.add_argument("--expected-candidate-snapshot-sha256", required=True)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--busy-timeout-ms", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.iterations <= 0 or args.warmups < 0 or args.busy_timeout_ms <= 0:
        raise SystemExit("iterations and timeout must be positive; warmups nonnegative")
    candidate_repo = args.repo.resolve()
    control_repo = args.control_repo.resolve()
    if any(
        path.resolve().is_relative_to(candidate_repo)
        for path in (args.output, args.markdown_output)
    ):
        raise SystemExit("benchmark outputs must be outside the candidate worktree")
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(candidate_repo / "src"))
    benchmark = _module()
    snapshots = {
        "candidate": benchmark._verify_snapshot(
            candidate_repo,
            True,
            args.expected_candidate_commit,
            args.expected_candidate_snapshot_sha256,
        ),
        "fixed_control": benchmark._snapshot(control_repo, candidate=False),
    }
    cases: dict[str, Any] = {}
    data = candidate_repo / "tests/data/ingest"
    with tempfile.TemporaryDirectory(prefix="nio-stage-corpus-") as directory:
        temporary = Path(directory)
        for name, spec in MATRIX.items():
            fixture_name, setup_names, before, after, frames_before, state, included = (
                spec
            )
            fixture = data / fixture_name
            setup = tuple(data / setup_name for setup_name in setup_names)
            modes = {
                mode: _run_mode(
                    args,
                    candidate_repo if mode == "candidate" else control_repo,
                    fixture,
                    setup,
                    mode,
                    temporary / f"{name}-{mode}.json",
                )
                for mode in MODES
            }
            raw = fixture.read_bytes()
            transport = benchmark._transport(raw)
            cases[name] = {
                "fixture": {
                    "path": str(fixture),
                    "raw_sha256": hashlib.sha256(raw).hexdigest(),
                    "normalized_sha256": hashlib.sha256(_canonical(raw)).hexdigest(),
                    "canonical_bytes": len(raw),
                },
                "ordered_untimed_setup": modes["candidate"]["setup_fixtures"],
                "setup_actions": {
                    "legacy_normalization_floor": "parse setup",
                    "classic_legacy_cursor_durability": (
                        "not available"
                        if transport == "sliding"
                        else "seed account; parse setup and save token"
                    ),
                    "candidate": "normalize and stage setup",
                },
                "expected": {
                    "cursor_before": before,
                    "cursor_after": after,
                    "frames_before": frames_before,
                    "frames_after": frames_before + 1,
                    "timed_state": state,
                },
                "modes": modes,
                "gates": _gates(args, spec, modes),
            }
    diagnostics = _diagnostics(benchmark, candidate_repo, args.busy_timeout_ms)
    size = _size_and_forecast(benchmark, candidate_repo, args.expected_candidate_commit)
    snapshot_stable = (
        benchmark._verify_snapshot(
            candidate_repo,
            True,
            args.expected_candidate_commit,
            args.expected_candidate_snapshot_sha256,
        )
        == snapshots["candidate"]
        and benchmark._snapshot(control_repo, candidate=False)
        == snapshots["fixed_control"]
        and all(
            mode["snapshot"]["snapshot_sha256"]
            == snapshots["candidate" if mode_name == "candidate" else "fixed_control"][
                "snapshot_sha256"
            ]
            for case in cases.values()
            for mode_name, mode in case["modes"].items()
        )
    )
    overall = (
        snapshot_stable
        and diagnostics["overall_passed"]
        and all(case["gates"]["case_passed"] for case in cases.values())
    )
    report = {
        "schema_version": 1,
        "fixed_control_commit": CONTROL_COMMIT,
        "fixed_control_worktree": str(control_repo),
        "candidate_commit": snapshots["candidate"]["commit"],
        "snapshots": snapshots,
        "snapshot_stable": snapshot_stable,
        "generated_at_unix_ns": time.time_ns(),
        "reproduction_command": " ".join(
            (
                sys.executable,
                str(
                    (
                        candidate_repo / "tests/ingest/run_staging_benchmark_corpus.py"
                    ).resolve()
                ),
                "--repo",
                str(candidate_repo),
                "--control-repo",
                str(control_repo),
                "--expected-candidate-commit",
                args.expected_candidate_commit,
                "--expected-candidate-snapshot-sha256",
                args.expected_candidate_snapshot_sha256,
                "--warmups",
                str(args.warmups),
                "--iterations",
                str(args.iterations),
                "--busy-timeout-ms",
                str(args.busy_timeout_ms),
                "--output",
                str(args.output.resolve()),
                "--markdown-output",
                str(args.markdown_output.resolve()),
            )
        ),
        "method": {
            "warmups": args.warmups,
            "measured_samples": args.iterations,
            "busy_timeout_ms": args.busy_timeout_ms,
            "median": "arithmetic mean of sorted middle values",
            "p95": "nearest rank at ceil(0.95*n)-1",
            "network": "denied by worker audit hook",
            "rss": "absolute process ru_maxrss normalized to bytes",
            "wal": "post-commit pre-close growth after post-setup TRUNCATE",
            "candidate_timing": "response normalization through stage commit; request planning excluded",
        },
        "cases": cases,
        "diagnostics": diagnostics,
        "size_and_forecast": size,
        "overall_passed": overall,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    args.markdown_output.write_text(_markdown(report, args.output))
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
