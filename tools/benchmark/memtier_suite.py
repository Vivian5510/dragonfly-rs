#!/usr/bin/env python3
"""
Dragonfly-rs memtier benchmark suite aligned with Dragonfly C++ repository conventions.

The Dragonfly C++ repo uses `memtier_benchmark` as its primary external throughput tool
(README benchmark section, FAQ, k8s benchmark job, and integration scripts). This suite
adopts the same tool and command-shape while adding reproducible multi-scenario orchestration.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Scenario:
    """One memtier workload scenario."""

    name: str
    ratio: str
    pipeline: int
    preferred_operation_hint: str


@dataclass
class ScenarioRunResult:
    """One concrete scenario execution sample."""

    scenario: str
    run_index: int
    throughput_ops: float
    p99_ms: float
    p999_ms: float
    avg_latency_ms: float
    operation: str
    duration_ms: float


@dataclass
class ScenarioAggregate:
    """Aggregated metrics over repeated runs for one scenario."""

    scenario: str
    runs: int
    throughput_ops_mean: float
    throughput_ops_median: float
    p99_ms_mean: float
    p99_ms_median: float
    avg_latency_ms_mean: float
    p999_ms_mean: float


DEFAULT_SCENARIOS: list[Scenario] = [
    # Mirrors Dragonfly README shape: independent SET and GET throughput views.
    Scenario("set", "1:0", 1, "set"),
    Scenario("get", "0:1", 1, "get"),
    # Mixed read-heavy traffic often used during practical sizing.
    Scenario("mixed", "1:10", 1, "get"),
    # Pipeline profile aligns with Dragonfly README pipeline note (`--pipeline=30`).
    Scenario("set_pipeline", "1:0", 30, "set"),
    Scenario("get_pipeline", "0:1", 30, "get"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dragonfly-rs memtier benchmark suite aligned with Dragonfly C++ conventions"
    )
    parser.add_argument(
        "--memtier-bin",
        default="memtier_benchmark",
        help="Path to memtier_benchmark executable",
    )
    parser.add_argument(
        "--server-bin",
        default="",
        help=(
            "Optional path to dfly-server binary. "
            "If set, the suite starts/stops the server automatically."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1", help="Target host")
    parser.add_argument("--port", type=int, default=6379, help="Target port")
    parser.add_argument("--threads", type=int, default=4, help="memtier --threads")
    parser.add_argument("--clients", type=int, default=20, help="memtier --clients")
    parser.add_argument(
        "--test-time",
        type=int,
        default=30,
        help="memtier --test-time in seconds. Set 0 to disable.",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=0,
        help="memtier --requests. Set >0 to use request-count mode.",
    )
    parser.add_argument("--data-size", type=int, default=256, help="memtier --data-size")
    parser.add_argument(
        "--key-maximum",
        type=int,
        default=5_000_000,
        help="memtier --key-maximum",
    )
    parser.add_argument(
        "--distinct-client-seed",
        action="store_true",
        default=True,
        help="Enable memtier --distinct-client-seed",
    )
    parser.add_argument(
        "--hide-histogram",
        action="store_true",
        default=True,
        help="Enable memtier --hide-histogram",
    )
    parser.add_argument("--repeats", type=int, default=3, help="Runs per scenario")
    parser.add_argument(
        "--scenarios",
        default="set,get,mixed,set_pipeline,get_pipeline",
        help="Comma-separated scenario names",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to write full benchmark results JSON",
    )
    parser.add_argument(
        "--baseline-json",
        default="",
        help="Optional baseline JSON path for regression gate checks",
    )
    parser.add_argument(
        "--min-throughput-ratio",
        type=float,
        default=95.0,
        help="Gate: current throughput must be >= baseline*ratio%%",
    )
    parser.add_argument(
        "--max-p99-delta-percent",
        type=float,
        default=5.0,
        help="Gate: current p99 increase against baseline must be <= this percent",
    )
    parser.add_argument(
        "--startup-timeout-sec",
        type=float,
        default=20.0,
        help="Timeout waiting for server startup when --server-bin is used",
    )
    return parser.parse_args()


def resolve_scenarios(selection: str) -> list[Scenario]:
    by_name = {scenario.name: scenario for scenario in DEFAULT_SCENARIOS}
    selected: list[Scenario] = []
    for name in [item.strip() for item in selection.split(",") if item.strip()]:
        scenario = by_name.get(name)
        if scenario is None:
            allowed = ", ".join(sorted(by_name))
            raise ValueError(f"unknown scenario '{name}', allowed: {allowed}")
        selected.append(scenario)
    if not selected:
        raise ValueError("no scenarios selected")
    return selected


def wait_for_tcp(host: str, port: int, timeout_sec: float) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"server did not become ready on {host}:{port} within {timeout_sec}s")


def choose_operation_name(all_stats: dict[str, Any], preferred_hint: str) -> str:
    candidates = [
        key
        for key, value in all_stats.items()
        if key != "Runtime" and isinstance(value, dict) and "Count" in value and "Ops/sec" in value
    ]
    if not candidates:
        raise ValueError("memtier JSON has no operation stats")
    preferred_hint = preferred_hint.lower()
    for candidate in candidates:
        if preferred_hint in candidate.lower():
            return candidate
    # Keep deterministic fallback if memtier naming differs by version.
    return sorted(candidates)[0]


def run_memtier_once(
    args: argparse.Namespace, scenario: Scenario, run_index: int, work_dir: Path
) -> ScenarioRunResult:
    json_path = work_dir / f"{scenario.name}_run{run_index}.json"
    command: list[str] = [
        args.memtier_bin,
        "--server",
        args.host,
        "--port",
        str(args.port),
        "--threads",
        str(args.threads),
        "--clients",
        str(args.clients),
        "--ratio",
        scenario.ratio,
        "--pipeline",
        str(scenario.pipeline),
        "--data-size",
        str(args.data_size),
        "--key-maximum",
        str(args.key_maximum),
        "--json-out-file",
        str(json_path),
    ]
    if args.test_time > 0:
        command.extend(["--test-time", str(args.test_time)])
    if args.requests > 0:
        command.extend(["--requests", str(args.requests)])
    if args.distinct_client_seed:
        command.append("--distinct-client-seed")
    if args.hide_histogram:
        command.append("--hide-histogram")

    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "memtier execution failed\n"
            f"command: {' '.join(command)}\n"
            f"exit code: {completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    with json_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    all_stats = payload["ALL STATS"]
    operation_name = choose_operation_name(all_stats, scenario.preferred_operation_hint)
    op_stats = all_stats[operation_name]
    runtime = all_stats["Runtime"]
    percentiles = op_stats.get("Percentile Latencies", {})

    return ScenarioRunResult(
        scenario=scenario.name,
        run_index=run_index,
        throughput_ops=float(op_stats["Ops/sec"]),
        p99_ms=float(percentiles.get("p99.00", 0.0)),
        p999_ms=float(percentiles.get("p99.90", 0.0)),
        avg_latency_ms=float(op_stats["Average Latency"]),
        operation=operation_name,
        duration_ms=float(runtime.get("Total duration", 0.0)),
    )


def aggregate_results(name: str, samples: list[ScenarioRunResult]) -> ScenarioAggregate:
    throughput = [item.throughput_ops for item in samples]
    p99 = [item.p99_ms for item in samples]
    avg = [item.avg_latency_ms for item in samples]
    p999 = [item.p999_ms for item in samples]
    return ScenarioAggregate(
        scenario=name,
        runs=len(samples),
        throughput_ops_mean=statistics.fmean(throughput),
        throughput_ops_median=statistics.median(throughput),
        p99_ms_mean=statistics.fmean(p99),
        p99_ms_median=statistics.median(p99),
        avg_latency_ms_mean=statistics.fmean(avg),
        p999_ms_mean=statistics.fmean(p999),
    )


def load_baseline(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def index_aggregates_by_name(aggregates: list[ScenarioAggregate]) -> dict[str, ScenarioAggregate]:
    return {item.scenario: item for item in aggregates}


def evaluate_gates(
    baseline: dict[str, Any],
    current_aggregates: list[ScenarioAggregate],
    min_throughput_ratio: float,
    max_p99_delta_percent: float,
) -> tuple[bool, list[str]]:
    baseline_items = {
        item["scenario"]: item for item in baseline.get("aggregates", []) if isinstance(item, dict)
    }
    ok = True
    messages: list[str] = []
    for current in current_aggregates:
        base = baseline_items.get(current.scenario)
        if base is None:
            messages.append(f"[WARN] baseline missing scenario '{current.scenario}', gate skipped")
            continue
        base_tp = float(base["throughput_ops_mean"])
        base_p99 = float(base["p99_ms_mean"])
        if base_tp <= 0:
            messages.append(f"[WARN] baseline throughput invalid for '{current.scenario}', gate skipped")
            continue

        throughput_ratio = (current.throughput_ops_mean / base_tp) * 100.0
        if base_p99 > 0:
            p99_delta = ((current.p99_ms_mean / base_p99) - 1.0) * 100.0
        else:
            p99_delta = 0.0

        tp_pass = throughput_ratio >= min_throughput_ratio
        p99_pass = p99_delta <= max_p99_delta_percent
        ok = ok and tp_pass and p99_pass
        messages.append(
            f"[GATE] {current.scenario}: throughput_ratio={throughput_ratio:.2f}% "
            f"(min {min_throughput_ratio:.2f}%) ; p99_delta={p99_delta:.2f}% "
            f"(max {max_p99_delta_percent:.2f}%) => {'PASS' if tp_pass and p99_pass else 'FAIL'}"
        )
    return ok, messages


def default_server_bin() -> str:
    target_dir = Path("target") / "release"
    candidate = target_dir / ("dfly-server.exe" if os.name == "nt" else "dfly-server")
    return str(candidate) if candidate.exists() else ""


def start_server(path: str) -> subprocess.Popen[str]:
    # Keep stdout/stderr attached to files so benchmark output stays readable in terminal.
    log_dir = Path(tempfile.mkdtemp(prefix="dfly_rs_server_logs_"))
    stdout_file = (log_dir / "stdout.log").open("w", encoding="utf-8")
    stderr_file = (log_dir / "stderr.log").open("w", encoding="utf-8")
    return subprocess.Popen(  # noqa: S603
        [path],
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
    )


def stop_server(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        else:
            process.terminate()
    except Exception:
        process.terminate()
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5.0)


def main() -> int:
    args = parse_args()
    scenarios = resolve_scenarios(args.scenarios)

    memtier_path = shutil.which(args.memtier_bin) if not Path(args.memtier_bin).exists() else args.memtier_bin
    if not memtier_path:
        print(f"memtier executable not found: {args.memtier_bin}", file=sys.stderr)
        return 2

    server_bin = args.server_bin or default_server_bin()
    process: subprocess.Popen[str] | None = None
    if server_bin:
        if not Path(server_bin).exists():
            print(f"server binary not found: {server_bin}", file=sys.stderr)
            return 2
        process = start_server(server_bin)
        try:
            wait_for_tcp(args.host, args.port, args.startup_timeout_sec)
        except Exception as error:
            stop_server(process)
            print(f"failed to start server: {error}", file=sys.stderr)
            return 2
    else:
        # If suite does not own server lifecycle, still verify connectivity early.
        try:
            wait_for_tcp(args.host, args.port, args.startup_timeout_sec)
        except Exception as error:
            print(f"target server not reachable: {error}", file=sys.stderr)
            return 2

    all_runs: list[ScenarioRunResult] = []
    aggregates: list[ScenarioAggregate] = []
    gate_ok = True
    gate_messages: list[str] = []
    try:
        with tempfile.TemporaryDirectory(prefix="dfly_rs_memtier_suite_") as temp_dir:
            work_dir = Path(temp_dir)
            for scenario in scenarios:
                samples: list[ScenarioRunResult] = []
                print(
                    f"[SCENARIO] {scenario.name}: ratio={scenario.ratio}, "
                    f"pipeline={scenario.pipeline}, repeats={args.repeats}"
                )
                for run_index in range(1, args.repeats + 1):
                    sample = run_memtier_once(args, scenario, run_index, work_dir)
                    samples.append(sample)
                    all_runs.append(sample)
                    print(
                        f"  run#{run_index}: throughput={sample.throughput_ops:.2f} ops/s, "
                        f"p99={sample.p99_ms:.3f} ms, op={sample.operation}"
                    )
                aggregate = aggregate_results(scenario.name, samples)
                aggregates.append(aggregate)
                print(
                    f"  aggregate: throughput_mean={aggregate.throughput_ops_mean:.2f} ops/s, "
                    f"throughput_median={aggregate.throughput_ops_median:.2f} ops/s, "
                    f"p99_mean={aggregate.p99_ms_mean:.3f} ms"
                )

        if args.baseline_json:
            baseline = load_baseline(Path(args.baseline_json))
            gate_ok, gate_messages = evaluate_gates(
                baseline,
                aggregates,
                args.min_throughput_ratio,
                args.max_p99_delta_percent,
            )
            for message in gate_messages:
                print(message)
    finally:
        if process is not None:
            stop_server(process)

    output_payload = {
        "meta": {
            "host": args.host,
            "port": args.port,
            "threads": args.threads,
            "clients": args.clients,
            "test_time": args.test_time,
            "requests": args.requests,
            "data_size": args.data_size,
            "key_maximum": args.key_maximum,
            "repeats": args.repeats,
            "scenarios": [scenario.name for scenario in scenarios],
        },
        "runs": [asdict(item) for item in all_runs],
        "aggregates": [asdict(item) for item in aggregates],
    }

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")
        print(f"[OUTPUT] wrote result json: {output_path}")

    return 0 if gate_ok else 3


if __name__ == "__main__":
    raise SystemExit(main())
