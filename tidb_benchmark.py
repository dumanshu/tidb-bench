#!/usr/bin/env python3
"""
Run sysbench benchmarks against the TiDB load-test stack.

Supports various workloads:
- oltp_read_write: Mixed read/write OLTP workload (default)
- oltp_read_only: Read-only queries
- oltp_write_only: Write-only operations
- oltp_point_select: Point selects (high concurrency)
"""

import argparse
import os
import subprocess
import sys
import textwrap
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "tidblt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_PORT = 4000
DEFAULT_DATABASE = "sbtest"

WORKLOAD_PROFILES = {
    "quick": {"tables": 4, "table_size": 10000, "threads": 16, "duration": 30},
    "light": {"tables": 8, "table_size": 50000, "threads": 32, "duration": 60},
    "medium": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 120},
    "heavy": {"tables": 32, "table_size": 500000, "threads": 128, "duration": 300},
    "stress": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 120, "multi_phase": "stress"},
    "scaling": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 120, "multi_phase": "scaling"},
}

MULTI_PHASE_PROFILES = {
    "stress": {
        "description": "CPU stress test: ramp-up, adaptive overload until QPS plateaus, then recover",
        "phases": [
            {"name": "warmup", "threads": 32, "duration": 30},
            {"name": "ramp_up", "threads": 64, "duration": 30},
            {"name": "overload", "threads": 128, "duration": 30, "adaptive": True, "thread_step": 32, "max_threads": 512, "plateau_threshold": 0.05},
            {"name": "recovery", "threads": 64, "duration": 30},
            {"name": "sustained", "threads": 48, "duration": 45},
        ],
        "total_duration": "adaptive",
    },
    "scaling": {
        "description": "Traffic scaling test: gradual ramp-up then ramp-down",
        "phases": [
            {"name": "baseline", "threads": 16, "duration": 30},
            {"name": "scale_up_1", "threads": 32, "duration": 30},
            {"name": "scale_up_2", "threads": 64, "duration": 30},
            {"name": "peak", "threads": 96, "duration": 30},
            {"name": "scale_down_1", "threads": 64, "duration": 30},
            {"name": "scale_down_2", "threads": 32, "duration": 30},
        ],
        "total_duration": 180,
    },
}

BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)


def ts():
    return datetime.now().strftime("[%Y-%m-%dT%H:%M:%S%z]")


def log(msg):
    print(f"{ts()} {msg}", flush=True)


def ec2_client(profile: Optional[str], region: str):
    session = boto3.session.Session(profile_name=profile, region_name=region)
    return session.client("ec2", region_name=region, config=BOTO_CONFIG)


def discover_tidb_host(region: str, profile: Optional[str], seed: str) -> str:
    """Discover the TiDB host instance public IP."""
    client = ec2_client(profile, region)
    stack = f"tidb-loadtest-{seed}"
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["pending", "running"]},
    ]
    resp = client.describe_instances(Filters=filters)
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {tag["Key"]: tag["Value"] for tag in inst.get("Tags", [])}
            role = tags.get("Role", "")
            if role == "host":
                ip = inst.get("PublicIpAddress")
                if ip:
                    return ip
    raise SystemExit("ERROR: Unable to discover TiDB host; specify --host explicitly.")


def ssh_run(host: str, script: str, key_path: Path, strict: bool = True):
    """Run a script on the remote host via SSH."""
    full = textwrap.dedent(script).lstrip()
    if strict:
        full = "set -euo pipefail\n" + full
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=30",
        "-i", str(key_path),
        f"ec2-user@{host}",
        "bash", "-s"
    ]
    subprocess.run(cmd, input=full, text=True, check=strict)


def ssh_capture(host: str, script: str, key_path: Path):
    """Run a script on the remote host and capture output."""
    full = textwrap.dedent(script).lstrip()
    full = "set -euo pipefail\n" + full
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=30",
        "-i", str(key_path),
        f"ec2-user@{host}",
        "bash", "-s"
    ]
    result = subprocess.run(cmd, input=full, text=True, capture_output=True)
    return result


def start_metrics_capture(
    host: str,
    key_path: Path,
    duration: int,
    output_dir: str = "/tmp/benchmark_metrics",
) -> threading.Thread:
    """Start background metrics capture (CPU, IOPS, memory) via SSH."""
    
    def capture_metrics():
        script = f"""
mkdir -p {output_dir}
DURATION={duration}

# CPU utilization per core
nohup mpstat -P ALL 1 $DURATION > {output_dir}/mpstat.log 2>&1 &

# Memory and swap
nohup vmstat 1 $DURATION > {output_dir}/vmstat.log 2>&1 &

# Disk I/O statistics (IOPS, throughput, latency)
nohup iostat -xdm 1 $DURATION > {output_dir}/iostat.log 2>&1 &

# Network statistics
nohup sar -n DEV 1 $DURATION > {output_dir}/network.log 2>&1 &

echo "Metrics capture started for $DURATION seconds"
"""
        ssh_run(host, script, key_path, strict=False)
    
    thread = threading.Thread(target=capture_metrics, daemon=True)
    thread.start()
    return thread


def collect_tidb_metrics(host: str, key_path: Path, port: int = DEFAULT_PORT) -> dict:
    """Collect TiDB-specific metrics via SQL queries."""
    script = f"""
mysql -h 127.0.0.1 -P {port} -u root -N -e "
SELECT 'qps', VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Questions';
SELECT 'uptime', VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Uptime';
SELECT 'connections', VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Threads_connected';
SELECT 'version', TIDB_VERSION();
" 2>/dev/null || echo "tidb_metrics_error"

kubectl get pods -n tidb-cluster -o wide 2>/dev/null | grep -E "^(NAME|basic-)" || true
"""
    result = ssh_capture(host, script, key_path)
    return {"stdout": result.stdout, "stderr": result.stderr}


def fetch_metrics_summary(host: str, key_path: Path, output_dir: str = "/tmp/benchmark_metrics"):
    """Fetch and summarize captured metrics after benchmark."""
    log("=== System Metrics Summary ===")
    
    script = f"""
echo "--- CPU Summary (mpstat) ---"
if [ -f {output_dir}/mpstat.log ]; then
    tail -20 {output_dir}/mpstat.log | grep -E "^[0-9]|^Average" | head -10
else
    echo "No mpstat data"
fi

echo ""
echo "--- Memory Summary (vmstat) ---"
if [ -f {output_dir}/vmstat.log ]; then
    head -2 {output_dir}/vmstat.log
    tail -5 {output_dir}/vmstat.log
else
    echo "No vmstat data"
fi

echo ""
echo "--- Disk I/O Summary (iostat) ---"
if [ -f {output_dir}/iostat.log ]; then
    tail -30 {output_dir}/iostat.log | grep -E "^Device|^nvme|^xvd|^sd" | tail -10
else
    echo "No iostat data"
fi

echo ""
echo "--- Network Summary (sar) ---"
if [ -f {output_dir}/network.log ]; then
    tail -20 {output_dir}/network.log | grep -v "^$" | tail -8
else
    echo "No network data"
fi
"""
    ssh_run(host, script, key_path, strict=False)


def fetch_tidb_cluster_metrics(host: str, key_path: Path, port: int = DEFAULT_PORT):
    """Fetch TiDB cluster health and region metrics."""
    log("=== TiDB Cluster Metrics ===")
    
    script = f"""
echo "--- Region Count ---"
mysql -h 127.0.0.1 -P {port} -u root -e "
SELECT COUNT(*) as total_regions FROM information_schema.tikv_region_status;
" 2>/dev/null || echo "Region query failed"

echo ""
echo "--- Leader Distribution ---"
mysql -h 127.0.0.1 -P {port} -u root -e "
SELECT store_id, COUNT(*) as leader_count 
FROM information_schema.tikv_region_status 
WHERE is_leader = 1 
GROUP BY store_id;
" 2>/dev/null || echo "Leader query failed"

echo ""
echo "--- Cluster Info ---"
mysql -h 127.0.0.1 -P {port} -u root -e "
SELECT TYPE, INSTANCE, STATUS_ADDRESS, VERSION 
FROM information_schema.cluster_info;
" 2>/dev/null || echo "Cluster info query failed"

echo ""
echo "--- Pod Status ---"
kubectl get pods -n tidb-cluster -o wide 2>/dev/null || true
"""
    ssh_run(host, script, key_path, strict=False)


def run_sysbench_prepare(
    host: str,
    key_path: Path,
    tables: int,
    table_size: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
):
    """Prepare sysbench tables."""
    log(f"Preparing sysbench tables: {tables} tables x {table_size:,} rows")
    
    estimated_row_size_bytes = 120
    total_rows = tables * table_size
    estimated_size_mb = (total_rows * estimated_row_size_bytes) / (1024 * 1024)
    log(f"  Estimated dataset size: {estimated_size_mb:,.1f} MB ({total_rows:,} total rows)")
    
    ssh_run(host, f"""
# Create database if not exists
mysql -h 127.0.0.1 -P {port} -u root -e "CREATE DATABASE IF NOT EXISTS {database};"

# Drop existing tables if any
for i in $(seq 1 {tables}); do
    mysql -h 127.0.0.1 -P {port} -u root -D {database} -e "DROP TABLE IF EXISTS sbtest$i;" 2>/dev/null || true
done

# Run sysbench prepare
sysbench oltp_read_write \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    --table-size={table_size} \\
    prepare

echo ""
echo "=== Dataset Size Report ==="
mysql -h 127.0.0.1 -P {port} -u root -e "
SELECT 
    table_schema AS db,
    COUNT(*) AS tables,
    SUM(table_rows) AS total_rows,
    ROUND(SUM(data_length) / 1024 / 1024, 2) AS data_mb,
    ROUND(SUM(index_length) / 1024 / 1024, 2) AS index_mb,
    ROUND((SUM(data_length) + SUM(index_length)) / 1024 / 1024, 2) AS total_mb
FROM information_schema.tables 
WHERE table_schema = '{database}'
GROUP BY table_schema;
"
""", key_path)


def run_sysbench_benchmark(
    host: str,
    key_path: Path,
    workload: str,
    tables: int,
    table_size: int,
    threads: int,
    duration: int,
    report_interval: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
    histogram: bool = True,
) -> dict:
    """Run sysbench benchmark and return parsed metrics."""
    log(f"Running sysbench {workload} benchmark")
    log(f"  Tables: {tables}, Table size: {table_size}")
    log(f"  Threads: {threads}, Duration: {duration}s")

    histogram_flag = "--histogram=on" if histogram else ""
    
    result = ssh_capture(host, f"""
sysbench {workload} \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    --table-size={table_size} \\
    --threads={threads} \\
    --time={duration} \\
    --report-interval={report_interval} \\
    {histogram_flag} \\
    run 2>&1
""", key_path)
    
    output = result.stdout
    print(output)
    
    return parse_sysbench_output(output, workload)


def parse_sysbench_output(output: str, workload: str) -> dict:
    """Parse sysbench output and extract detailed metrics."""
    metrics = {
        "workload": workload,
        "transactions": {},
        "queries": {},
        "latency": {},
        "throughput": {},
    }
    
    lines = output.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        
        if line.startswith("read:"):
            try:
                metrics["queries"]["read"] = int(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif line.startswith("write:"):
            try:
                metrics["queries"]["write"] = int(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif line.startswith("other:"):
            try:
                metrics["queries"]["other"] = int(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif line.startswith("total:"):
            try:
                metrics["queries"]["total"] = int(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        
        if "transactions:" in line and "per sec" in line:
            parts = line.split()
            for j, p in enumerate(parts):
                if "per" in p and j > 0:
                    try:
                        metrics["throughput"]["tps"] = float(parts[j-1].strip('('))
                        metrics["transactions"]["total"] = int(parts[1])
                    except (ValueError, IndexError):
                        pass
        
        if "queries:" in line and "per sec" in line:
            parts = line.split()
            for j, p in enumerate(parts):
                if "per" in p and j > 0:
                    try:
                        metrics["throughput"]["qps"] = float(parts[j-1].strip('('))
                    except (ValueError, IndexError):
                        pass
        
        if "min:" in line and "latency" not in line.lower():
            try:
                metrics["latency"]["min_ms"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif "avg:" in line:
            try:
                metrics["latency"]["avg_ms"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif "max:" in line:
            try:
                metrics["latency"]["max_ms"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif "95th percentile:" in line:
            try:
                metrics["latency"]["p95_ms"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
        elif "99th percentile:" in line:
            try:
                metrics["latency"]["p99_ms"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
    
    return metrics


def print_metrics_summary(metrics: dict):
    """Print a formatted summary of benchmark metrics."""
    log("")
    log("=" * 60)
    log(f"BENCHMARK RESULTS: {metrics.get('workload', 'unknown')}")
    log("=" * 60)
    
    tp = metrics.get("throughput", {})
    if tp:
        log(f"  Throughput:")
        log(f"    TPS: {tp.get('tps', 'N/A'):,.2f} transactions/sec")
        log(f"    QPS: {tp.get('qps', 'N/A'):,.2f} queries/sec")
    
    lat = metrics.get("latency", {})
    if lat:
        log(f"  Latency (ms):")
        log(f"    Min: {lat.get('min_ms', 'N/A')}")
        log(f"    Avg: {lat.get('avg_ms', 'N/A')}")
        log(f"    P95: {lat.get('p95_ms', 'N/A')}")
        log(f"    P99: {lat.get('p99_ms', 'N/A')}")
        log(f"    Max: {lat.get('max_ms', 'N/A')}")
    
    queries = metrics.get("queries", {})
    if queries:
        log(f"  Query Breakdown:")
        log(f"    Read:  {queries.get('read', 'N/A'):,}")
        log(f"    Write: {queries.get('write', 'N/A'):,}")
        log(f"    Other: {queries.get('other', 'N/A'):,}")
        log(f"    Total: {queries.get('total', 'N/A'):,}")
    
    log("=" * 60)


def run_adaptive_phase(
    host: str,
    key_path: Path,
    phase: dict,
    workload: str,
    tables: int,
    table_size: int,
    report_interval: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
) -> list:
    """Run adaptive overload phase: increase threads until QPS plateaus."""
    results = []
    start_threads = phase["threads"]
    thread_step = phase.get("thread_step", 32)
    max_threads = phase.get("max_threads", 512)
    plateau_threshold = phase.get("plateau_threshold", 0.05)  # 5% improvement threshold
    phase_duration = phase.get("duration", 30)
    
    current_threads = start_threads
    prev_qps = 0
    iteration = 0
    peak_qps = 0
    peak_threads = start_threads
    
    log(f"    Adaptive mode: starting at {start_threads} threads, step={thread_step}, max={max_threads}")
    log(f"    Will stop when QPS improvement < {plateau_threshold*100:.0f}% threshold")
    
    while current_threads <= max_threads:
        iteration += 1
        log(f"")
        log(f"    >> Iteration {iteration}: {current_threads} threads, {phase_duration}s")
        
        result = ssh_capture(host, f"""
sysbench {workload} \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    --table-size={table_size} \\
    --threads={current_threads} \\
    --time={phase_duration} \\
    --report-interval={report_interval} \\
    run 2>&1
""", key_path)
        
        output = result.stdout
        print(output)
        
        metrics = parse_sysbench_output(output, workload)
        metrics["phase"] = f"overload_iter{iteration}"
        metrics["threads"] = current_threads
        metrics["duration"] = phase_duration
        results.append(metrics)
        
        current_qps = metrics.get("throughput", {}).get("qps", 0)
        tp = metrics.get("throughput", {})
        lat = metrics.get("latency", {})
        
        # Track peak
        if current_qps > peak_qps:
            peak_qps = current_qps
            peak_threads = current_threads
        
        # Calculate improvement
        if prev_qps > 0:
            improvement = (current_qps - prev_qps) / prev_qps
            log(f"       QPS: {current_qps:,.1f} (prev: {prev_qps:,.1f}, change: {improvement*100:+.1f}%)")
            log(f"       P95: {lat.get('p95_ms', 'N/A')}ms, P99: {lat.get('p99_ms', 'N/A')}ms")
            
            # Check if we've plateaued (improvement below threshold or negative)
            if improvement < plateau_threshold:
                log(f"")
                log(f"    !! QPS plateau detected: improvement {improvement*100:.1f}% < {plateau_threshold*100:.0f}% threshold")
                log(f"    !! Peak QPS: {peak_qps:,.1f} at {peak_threads} threads")
                break
        else:
            log(f"       QPS: {current_qps:,.1f} (baseline)")
            log(f"       P95: {lat.get('p95_ms', 'N/A')}ms, P99: {lat.get('p99_ms', 'N/A')}ms")
        
        prev_qps = current_qps
        current_threads += thread_step
        
        # Brief pause between iterations to let system stabilize
        if current_threads <= max_threads:
            log(f"       Ramping up to {current_threads} threads...")
            time.sleep(3)
    
    if current_threads > max_threads:
        log(f"    !! Reached max threads ({max_threads}) without plateau")
        log(f"    !! Peak QPS: {peak_qps:,.1f} at {peak_threads} threads")
    
    return results


def run_multi_phase_benchmark(
    host: str,
    key_path: Path,
    profile_name: str,
    workload: str,
    tables: int,
    table_size: int,
    report_interval: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
) -> list:
    """Run multi-phase benchmark (stress or scaling profile)."""
    profile = MULTI_PHASE_PROFILES[profile_name]
    log("")
    log("=" * 70)
    log(f"MULTI-PHASE BENCHMARK: {profile_name.upper()}")
    log(f"  {profile['description']}")
    
    # Calculate total duration (may be adaptive)
    total_phases = len(profile['phases'])
    if profile['total_duration'] == "adaptive":
        estimated = sum(p.get('duration', 30) for p in profile['phases'])
        log(f"  Phases: {total_phases} (adaptive duration, min ~{estimated}s)")
    else:
        log(f"  Total duration: {profile['total_duration']}s ({total_phases} phases)")
    log("=" * 70)
    
    all_results = []
    
    for i, phase in enumerate(profile["phases"]):
        phase_name = phase["name"]
        threads = phase["threads"]
        duration = phase["duration"]
        is_adaptive = phase.get("adaptive", False)
        
        log("")
        log(f">>> PHASE {i+1}/{len(profile['phases'])}: {phase_name.upper()}")
        if is_adaptive:
            log(f"    Mode: ADAPTIVE (start={threads} threads, step={phase.get('thread_step', 32)}, max={phase.get('max_threads', 512)})")
        else:
            log(f"    Threads: {threads}, Duration: {duration}s")
        log("-" * 50)
        
        if is_adaptive:
            # Run adaptive overload phase
            phase_results = run_adaptive_phase(
                host, key_path, phase, workload,
                tables, table_size, report_interval,
                port, database
            )
            all_results.extend(phase_results)
        else:
            # Run standard fixed-duration phase
            result = ssh_capture(host, f"""
sysbench {workload} \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    --table-size={table_size} \\
    --threads={threads} \\
    --time={duration} \\
    --report-interval={report_interval} \\
    run 2>&1
""", key_path)
            
            output = result.stdout
            print(output)
            
            metrics = parse_sysbench_output(output, workload)
            metrics["phase"] = phase_name
            metrics["threads"] = threads
            metrics["duration"] = duration
            all_results.append(metrics)
            
            tp = metrics.get("throughput", {})
            lat = metrics.get("latency", {})
            log(f"    Phase Result: TPS={tp.get('tps', 0):,.1f} QPS={tp.get('qps', 0):,.1f} P95={lat.get('p95_ms', 'N/A')}ms")
    
    return all_results


def print_multi_phase_summary(profile_name: str, results: list):
    """Print summary of multi-phase benchmark results."""
    log("")
    log("=" * 80)
    log(f"MULTI-PHASE SUMMARY: {profile_name.upper()}")
    log("=" * 80)
    log(f"{'Phase':<15} {'Threads':>8} {'Duration':>10} {'TPS':>12} {'QPS':>12} {'P95(ms)':>10} {'P99(ms)':>10}")
    log("-" * 80)
    
    for r in results:
        phase = r.get("phase", "unknown")
        threads = r.get("threads", 0)
        duration = r.get("duration", 0)
        tp = r.get("throughput", {})
        lat = r.get("latency", {})
        tps = tp.get("tps", 0)
        qps = tp.get("qps", 0)
        p95 = lat.get("p95_ms", "N/A")
        p99 = lat.get("p99_ms", "N/A")
        
        p95_str = f"{p95:.2f}" if isinstance(p95, (int, float)) else str(p95)
        p99_str = f"{p99:.2f}" if isinstance(p99, (int, float)) else str(p99)
        
        log(f"{phase:<15} {threads:>8} {duration:>8}s {tps:>12,.1f} {qps:>12,.1f} {p95_str:>10} {p99_str:>10}")
    
    log("-" * 80)
    
    total_tps = sum(r.get("throughput", {}).get("tps", 0) * r.get("duration", 0) for r in results)
    total_duration = sum(r.get("duration", 0) for r in results)
    avg_tps = total_tps / total_duration if total_duration > 0 else 0
    
    peak_tps = max(r.get("throughput", {}).get("tps", 0) for r in results)
    peak_phase = next((r["phase"] for r in results if r.get("throughput", {}).get("tps", 0) == peak_tps), "N/A")
    
    log(f"Average TPS: {avg_tps:,.1f} | Peak TPS: {peak_tps:,.1f} (phase: {peak_phase})")
    log("=" * 80)


def run_sysbench_cleanup(
    host: str,
    key_path: Path,
    tables: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
):
    """Cleanup sysbench tables."""
    log("Cleaning up sysbench tables")
    ssh_run(host, f"""
sysbench oltp_read_write \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    cleanup
""", key_path, strict=False)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run sysbench benchmarks against TiDB load-test stack."
    )
    parser.add_argument(
        "--host",
        help="TiDB host IP (auto-discovered from EC2 tags if not specified)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"TiDB port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"AWS region for stack discovery (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--seed",
        default=DEFAULT_SEED,
        help=f"Stack seed used in the Project tag (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--aws-profile",
        default=DEFAULT_PROFILE,
        help=f"AWS profile (default: {DEFAULT_PROFILE})",
    )
    parser.add_argument(
        "--ssh-key",
        default=str(Path(__file__).resolve().with_name("tidb-load-test-key.pem")),
        help="Path to SSH private key",
    )

    # Workload options
    parser.add_argument(
        "--workload",
        choices=[
            "oltp_read_write",
            "oltp_read_only",
            "oltp_write_only",
            "oltp_point_select",
            "oltp_insert",
            "oltp_delete",
            "oltp_update_index",
            "oltp_update_non_index",
        ],
        default="oltp_read_write",
        help="Sysbench workload type (default: oltp_read_write)",
    )
    parser.add_argument(
        "--tables",
        type=int,
        default=16,
        help="Number of sysbench tables (default: 16)",
    )
    parser.add_argument(
        "--table-size",
        type=int,
        default=100000,
        help="Rows per table (default: 100000)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=64,
        help="Number of concurrent threads (default: 64)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Benchmark duration in seconds (default: 120)",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=10,
        help="Report interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--profile",
        choices=list(WORKLOAD_PROFILES.keys()),
        default="heavy",
        help="Workload profile: quick(30s), light(60s), medium(2m), heavy(5m, default), stress(4m multi-phase), scaling(4m multi-phase)",
    )

    # Actions
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Only prepare tables, don't run benchmark",
    )
    parser.add_argument(
        "--skip-prepare",
        action="store_true",
        help="Skip table preparation (use existing tables)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up tables after benchmark",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Only clean up tables",
    )
    parser.add_argument(
        "--capture-metrics",
        action="store_true",
        default=True,
        help="Capture system metrics (CPU, IOPS, memory) during benchmark. Default: enabled.",
    )
    parser.add_argument(
        "--no-metrics",
        action="store_true",
        help="Disable system metrics capture.",
    )
    parser.add_argument(
        "--show-tidb-metrics",
        action="store_true",
        default=True,
        help="Show TiDB cluster metrics (regions, leaders) after benchmark. Default: enabled.",
    )
    parser.add_argument(
        "--no-tidb-metrics",
        action="store_true",
        help="Disable TiDB cluster metrics display.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    key_path = Path(args.ssh_key).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")

    if args.host:
        host = args.host
    else:
        log("Discovering TiDB host from EC2 tags...")
        host = discover_tidb_host(args.region, args.aws_profile, args.seed)
    log(f"Using TiDB host: {host}")

    if args.cleanup_only:
        run_sysbench_cleanup(host, key_path, args.tables, args.port)
        log("Cleanup complete.")
        return

    tables = args.tables
    table_size = args.table_size
    threads = args.threads
    duration = args.duration
    
    if args.profile:
        profile = WORKLOAD_PROFILES[args.profile]
        tables = profile["tables"]
        table_size = profile["table_size"]
        threads = profile["threads"]
        duration = profile["duration"]
        multi_phase = profile.get("multi_phase")
        log(f"Using profile '{args.profile}': {tables} tables, {table_size} rows, {threads} threads, {duration}s")
        if multi_phase:
            log(f"  Multi-phase mode: {multi_phase}")
    else:
        multi_phase = None

    if not args.skip_prepare:
        run_sysbench_prepare(
            host, key_path,
            tables, table_size,
            args.port,
        )

    if args.prepare_only:
        log("Tables prepared. Skipping benchmark (--prepare-only).")
        return

    metrics_thread = None
    capture_metrics = args.capture_metrics and not args.no_metrics
    show_tidb_metrics = args.show_tidb_metrics and not args.no_tidb_metrics
    
    # Calculate total_duration for metrics capture (handle adaptive case)
    if multi_phase:
        mp_profile = MULTI_PHASE_PROFILES[multi_phase]
        if mp_profile["total_duration"] == "adaptive":
            # Estimate duration: sum of fixed phases + max adaptive iterations
            estimated = sum(p.get('duration', 30) for p in mp_profile['phases'])
            # Add buffer for potential adaptive iterations (up to 10 iterations)
            adaptive_phases = [p for p in mp_profile['phases'] if p.get('adaptive')]
            for ap in adaptive_phases:
                max_iters = (ap.get('max_threads', 512) - ap['threads']) // ap.get('thread_step', 32) + 1
                estimated += max_iters * ap.get('duration', 30)
            total_duration = estimated
        else:
            total_duration = mp_profile["total_duration"]
    else:
        total_duration = duration
    
    if capture_metrics:
        log("Starting background metrics capture (CPU, IOPS, memory)...")
        metrics_thread = start_metrics_capture(
            host, key_path, total_duration + 120  # Extra buffer for adaptive
        )

    if multi_phase:
        phase_results = run_multi_phase_benchmark(
            host, key_path,
            multi_phase,
            args.workload,
            tables, table_size,
            args.report_interval,
            args.port,
        )
        print_multi_phase_summary(multi_phase, phase_results)
    else:
        benchmark_metrics = run_sysbench_benchmark(
            host, key_path,
            args.workload,
            tables, table_size,
            threads, duration, args.report_interval,
            args.port,
        )
        print_metrics_summary(benchmark_metrics)

    if capture_metrics:
        log("Waiting for metrics capture to complete...")
        time.sleep(5)
        fetch_metrics_summary(host, key_path)

    if show_tidb_metrics:
        fetch_tidb_cluster_metrics(host, key_path, args.port)

    if args.cleanup:
        run_sysbench_cleanup(host, key_path, args.tables, args.port)

    log("Benchmark complete.")


if __name__ == "__main__":
    main()
