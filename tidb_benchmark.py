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
import re
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

# TiDB session variables for benchmarking
# These are safe for all workloads (read/write)
SESSION_VARS_BASE = (
    "SET SESSION tikv_client_read_timeout=5;"  # 5s timeout for TiKV reads
    "SET SESSION max_execution_time=10000;"    # 10s max query execution
)

# Additional session vars for read-only workloads
SESSION_VARS_READ_ONLY = (
    "SET SESSION tidb_replica_read='closest-replicas';"  # Read from nearest replica
)

# MySQL errors to ignore during benchmark (improves resilience)
# 1213: Deadlock, 1020: Record changed, 1205: Lock wait timeout, 1105: Deadline exceeded
MYSQL_IGNORE_ERRORS = "1213,1020,1205,1105"

# AWS cost estimates (us-east-1, on-demand pricing as of 2024)
# Sources: https://aws.amazon.com/ec2/pricing/on-demand/
AWS_COSTS = {
    # EC2 instances (hourly rates)
    "ec2": {
        "c7g.xlarge": {"hourly": 0.145, "vcpu": 4, "memory_gb": 8},
        "c7g.2xlarge": {"hourly": 0.29, "vcpu": 8, "memory_gb": 16},
        "c7g.4xlarge": {"hourly": 0.58, "vcpu": 16, "memory_gb": 32},
        "c7g.8xlarge": {"hourly": 1.16, "vcpu": 32, "memory_gb": 64},
        "m7g.xlarge": {"hourly": 0.163, "vcpu": 4, "memory_gb": 16},
        "m7g.2xlarge": {"hourly": 0.326, "vcpu": 8, "memory_gb": 32},
        "m7g.4xlarge": {"hourly": 0.652, "vcpu": 16, "memory_gb": 64},
        "r7g.2xlarge": {"hourly": 0.428, "vcpu": 8, "memory_gb": 64},
        "r7g.4xlarge": {"hourly": 0.856, "vcpu": 16, "memory_gb": 128},
    },
    # EBS storage
    "ebs": {
        "gp3_per_gb_month": 0.08,
        "gp3_iops_over_3000": 0.005,  # per IOPS/month over 3000 baseline
        "gp3_throughput_over_125": 0.04,  # per MB/s/month over 125 baseline
        "io2_per_gb_month": 0.125,
        "io2_per_iops_month": 0.065,
    },
    # Network transfer (per GB)
    "network": {
        "cross_az_per_gb": 0.01,  # $0.01/GB each direction
        "same_az_per_gb": 0.00,  # Free within same AZ
        "internet_out_first_10tb": 0.09,
        "internet_out_next_40tb": 0.085,
        "nat_gateway_per_gb": 0.045,
    },
    # S3 (if used for backups)
    "s3": {
        "standard_per_gb_month": 0.023,
        "put_per_1000": 0.005,
        "get_per_1000": 0.0004,
    },
}


class CostTracker:
    """Track and accumulate costs during benchmark run."""
    
    def __init__(self, region: str = "us-east-1"):
        self.region = region
        self.start_time = time.time()
        self.costs = {
            "client": {"compute": 0.0, "storage": 0.0, "network": 0.0},
            "server": {"compute": 0.0, "storage": 0.0, "network": 0.0},
        }
        self.metrics = {
            "total_queries": 0,
            "total_transactions": 0,
            "total_bytes_transferred": 0,
            "client_instance_type": None,
            "server_instance_type": None,
            "server_instance_count": 0,
            "ebs_gb": 0,
            "avg_qps": 0,
            "avg_tps": 0,
        }
    
    def set_infrastructure(self, client_type: str, server_type: str, 
                           server_count: int, ebs_gb: int = 600):
        """Set infrastructure details for cost calculation."""
        self.metrics["client_instance_type"] = client_type
        self.metrics["server_instance_type"] = server_type
        self.metrics["server_instance_count"] = server_count
        self.metrics["ebs_gb"] = ebs_gb
    
    def add_queries(self, query_count: int, transaction_count: int = 0,
                    avg_qps: float = 0, avg_tps: float = 0,
                    avg_row_size_bytes: int = 200):
        """Track query/transaction volume for cost and price-performance calculation."""
        self.metrics["total_queries"] += query_count
        self.metrics["total_transactions"] += transaction_count
        # Track throughput for price-performance
        if avg_qps > 0:
            self.metrics["avg_qps"] = avg_qps
        if avg_tps > 0:
            self.metrics["avg_tps"] = avg_tps
        # Estimate bytes: queries * avg response size
        self.metrics["total_bytes_transferred"] += query_count * avg_row_size_bytes
    
    def calculate_costs(self, duration_seconds: float) -> dict:
        """Calculate all costs for the given duration."""
        hours = duration_seconds / 3600
        
        # Client compute cost
        client_type = self.metrics.get("client_instance_type", "c7g.2xlarge")
        client_hourly = AWS_COSTS["ec2"].get(client_type, {}).get("hourly", 0.29)
        self.costs["client"]["compute"] = client_hourly * hours
        
        # Server compute cost (each pod on its own dedicated host)
        server_type = self.metrics.get("server_instance_type", "c7g.2xlarge")
        server_hourly = AWS_COSTS["ec2"].get(server_type, {}).get("hourly", 0.29)
        # Each TiKV/TiDB/PD pod runs on its own dedicated host
        server_count = self.metrics.get("server_instance_count", 1)
        self.costs["server"]["compute"] = server_hourly * hours * server_count
        
        # Each TiKV node has its own EBS volume; count TiKV nodes
        ebs_gb = self.metrics.get("ebs_gb", 600)
        tikv_nodes = max(1, self.metrics.get("server_instance_count", 1))
        monthly_ebs = ebs_gb * tikv_nodes * AWS_COSTS["ebs"]["gp3_per_gb_month"]
        self.costs["server"]["storage"] = monthly_ebs * (hours / 720)  # 720 hours/month
        
        # Network cost estimation
        # Cross-AZ traffic between TiDB/TiKV/PD nodes
        # Cross-AZ traffic would be for external clients
        bytes_gb = self.metrics.get("total_bytes_transferred", 0) / (1024**3)
        # Assume 50% of traffic would be cross-AZ in real deployment
        cross_az_gb = bytes_gb * 0.5
        self.costs["server"]["network"] = cross_az_gb * AWS_COSTS["network"]["cross_az_per_gb"] * 2  # bidirectional
        
        return self.costs
    
    def get_summary(self, duration_seconds: float) -> dict:
        """Get cost summary with totals."""
        self.calculate_costs(duration_seconds)
        
        client_total = sum(self.costs["client"].values())
        server_total = sum(self.costs["server"].values())
        
        return {
            "duration_seconds": duration_seconds,
            "duration_hours": duration_seconds / 3600,
            "client": {
                **self.costs["client"],
                "total": client_total,
            },
            "server": {
                **self.costs["server"],
                "total": server_total,
            },
            "grand_total": client_total + server_total,
            "hourly_rate": (client_total + server_total) / (duration_seconds / 3600) if duration_seconds > 0 else 0,
            "metrics": self.metrics,
        }
    
    def print_summary(self, duration_seconds: float):
        """Print formatted cost summary."""
        summary = self.get_summary(duration_seconds)
        hours = summary["duration_hours"]
        
        log("")
        log("=" * 70)
        log("COST SUMMARY")
        log("=" * 70)
        log(f"Duration: {duration_seconds:.0f}s ({hours:.3f} hours)")
        log("")
        log(f"{'Category':<20} {'Compute':>12} {'Storage':>12} {'Network':>12} {'Total':>12}")
        log("-" * 70)
        
        # Client costs
        c = summary["client"]
        log(f"{'Client (EC2)':<20} ${c['compute']:>11.4f} ${c['storage']:>11.4f} ${c['network']:>11.4f} ${c['total']:>11.4f}")
        
        # Server costs
        s = summary["server"]
        log(f"{'Server (EC2+EBS)':<20} ${s['compute']:>11.4f} ${s['storage']:>11.4f} ${s['network']:>11.4f} ${s['total']:>11.4f}")
        
        log("-" * 70)
        log(f"{'TOTAL':<20} ${c['compute']+s['compute']:>11.4f} ${c['storage']+s['storage']:>11.4f} ${c['network']+s['network']:>11.4f} ${summary['grand_total']:>11.4f}")
        log("")
        log(f"Effective hourly rate: ${summary['hourly_rate']:.2f}/hr")
        
        # Price-Performance Metrics
        avg_qps = self.metrics.get("avg_qps", 0)
        avg_tps = self.metrics.get("avg_tps", 0)
        monthly_cost = summary['hourly_rate'] * 720
        
        if avg_qps > 0 or avg_tps > 0:
            log("")
            log("--- Price-Performance Metrics ---")
            if avg_qps > 0:
                cost_per_million_queries = (monthly_cost / avg_qps / 3600 / 720) * 1_000_000
                log(f"Throughput: {avg_qps:,.1f} QPS")
                log(f"Cost per 1M queries: ${cost_per_million_queries:.4f}")
                log(f"Monthly $/QPS: ${monthly_cost / avg_qps:.4f}")
            if avg_tps > 0:
                cost_per_million_txns = (monthly_cost / avg_tps / 3600 / 720) * 1_000_000
                log(f"Throughput: {avg_tps:,.1f} TPS")
                log(f"Cost per 1M transactions: ${cost_per_million_txns:.4f}")
                log(f"Monthly $/TPS: ${monthly_cost / avg_tps:.4f}")
        
        # Monthly Projections
        log("")
        log("--- Monthly Cost Projections (730 hours) ---")
        log(f"Compute: ${(c['compute']+s['compute']) / hours * 730:.2f}")
        log(f"Storage: ${(c['storage']+s['storage']) / hours * 730:.2f}")
        log(f"Network: ${(c['network']+s['network']) / hours * 730:.2f}")
        log(f"TOTAL:   ${monthly_cost:.2f}/month")
        log("")
        log("Note: Costs are estimates based on us-east-1 on-demand pricing.")
        log("      Network costs assume 50% cross-AZ traffic in production.")
        log("      Actual costs may vary with reserved instances or savings plans.")
        log("=" * 70)

WORKLOAD_PROFILES = {
    "quick": {"tables": 4, "table_size": 10000, "threads": 16, "duration": 30, "disk_fill_pct": 30},
    "light": {"tables": 8, "table_size": 50000, "threads": 32, "duration": 60, "disk_fill_pct": 30},
    "medium": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 120, "disk_fill_pct": 30},
    "heavy": {"tables": 32, "table_size": 500000, "threads": 128, "duration": 300, "disk_fill_pct": 30},
    # Standard price-performance profile based on PingCAP's official benchmarks
    # Uses 16 tables x 100K rows (1.6M total), 64 threads, 5 minute duration
    # Suitable for comparing $/QPS and $/TPS across configurations
    "standard": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 300, "disk_fill_pct": 30},
    "stress": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 120, "multi_phase": "stress", "disk_fill_pct": 30},
    "scaling": {"tables": 16, "table_size": 100000, "threads": 64, "duration": 120, "multi_phase": "scaling", "disk_fill_pct": 30},
}

# Default EBS volume size in GB (root volume for kind cluster host)
DEFAULT_EBS_SIZE_GB = 600

# Average sysbench row size including indexes and TiDB overhead
# Empirically ~240 bytes per row on disk (data + indexes + MVCC overhead)
SYSBENCH_ROW_DISK_BYTES = 240

MULTI_PHASE_PROFILES = {
    "stress": {
        "description": "CPU stress test: ramp-up, push to breaking point, then recover",
        "phases": [
            {"name": "warmup", "threads": 32, "duration": 30},
            {"name": "ramp_up", "threads": 64, "duration": 30},
            {"name": "overload", "threads": 128, "duration": 30, "adaptive": True, "thread_step": 32, "max_threads": 512, "decay_threshold": 0.50, "latency_multiplier": 10},
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

# Global log file handle
_log_file = None


def ts():
    return datetime.now().strftime("[%Y-%m-%dT%H:%M:%S]")


def log(msg):
    """Log message to stdout and optionally to log file."""
    line = f"{ts()} {msg}"
    print(line, flush=True)
    if _log_file:
        _log_file.write(line + "\n")
        _log_file.flush()

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


def get_instance_info(region: str, profile: Optional[str], seed: str) -> dict:
    """Get instance details for cost estimation."""
    client = ec2_client(profile, region)
    stack = f"tidb-loadtest-{seed}"
    filters = [
        {"Name": "tag:Project", "Values": [stack]},
        {"Name": "instance-state-name", "Values": ["running"]},
    ]
    resp = client.describe_instances(Filters=filters)
    info = {"instances": [], "az": None}
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            tags = {tag["Key"]: tag["Value"] for tag in inst.get("Tags", [])}
            info["instances"].append({
                "id": inst.get("InstanceId"),
                "type": inst.get("InstanceType"),
                "az": inst.get("Placement", {}).get("AvailabilityZone"),
                "role": tags.get("Role", "unknown"),
            })
            if not info["az"]:
                info["az"] = inst.get("Placement", {}).get("AvailabilityZone")
    return info


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


def get_cluster_info(host: str, key_path: Path, port: int = DEFAULT_PORT) -> dict:
    """Get TiDB cluster configuration details."""
    script = f"""
mysql -h 127.0.0.1 -P {port} -u root -N -e "
SELECT 'tidb_version', TIDB_VERSION();
SELECT 'tidb_count', COUNT(*) FROM information_schema.cluster_info WHERE TYPE='tidb';
SELECT 'tikv_count', COUNT(*) FROM information_schema.cluster_info WHERE TYPE='tikv';
SELECT 'pd_count', COUNT(*) FROM information_schema.cluster_info WHERE TYPE='pd';
SELECT 'region_count', COUNT(*) FROM information_schema.tikv_region_status;
" 2>/dev/null || echo "cluster_info_error"

# Get pod resource requests
kubectl top pods -n tidb-cluster --no-headers 2>/dev/null | head -10 || echo "kubectl_error"
"""
    result = ssh_capture(host, script, key_path)
    
    info = {
        "tidb_version": "unknown",
        "tidb_count": 0,
        "tikv_count": 0,
        "pd_count": 0,
        "region_count": 0,
        "pods": [],
    }
    
    for line in result.stdout.split('\n'):
        parts = line.strip().split('\t')
        if len(parts) == 2:
            key, val = parts
            if key in info:
                try:
                    info[key] = int(val) if key.endswith('_count') else val
                except ValueError:
                    info[key] = val
        elif line and not line.startswith("kubectl_error") and not line.startswith("cluster_info_error"):
            # Parse kubectl top output: NAME CPU MEMORY
            pod_parts = line.split()
            if len(pod_parts) >= 3 and pod_parts[0].startswith("basic-"):
                info["pods"].append({
                    "name": pod_parts[0],
                    "cpu": pod_parts[1],
                    "memory": pod_parts[2],
                })
    
    return info


def print_cluster_summary(host: str, key_path: Path, region: str, profile: str, seed: str, port: int = DEFAULT_PORT):
    """Print cluster configuration summary with EBS details."""
    log("")
    log("=" * 80)
    log("CLUSTER CONFIGURATION")
    log("=" * 80)
    
    # Get instance info
    inst_info = get_instance_info(region, profile, seed)
    cluster_info = get_cluster_info(host, key_path, port)
    
    log(f"Region: {region} | Zone: {inst_info.get('az', 'unknown')}")
    log(f"TiDB Version: {cluster_info.get('tidb_version', 'unknown')}")
    log(f"Regions: {cluster_info.get('region_count', 'N/A')}")
    log("")
    
    # Determine instance type
    host_type = "c7g.4xlarge"  # Default (updated to larger instance)
    for inst in inst_info.get("instances", []):
        inst_type = inst.get("type")
        if inst_type:
            host_type = inst_type
            break
    
    host_specs = AWS_COSTS["ec2"].get(host_type, {"vcpu": 16, "memory_gb": 32})
    
    # Compute resources table
    log("--- Compute Resources ---")
    log(f"{'Component':<12} {'Count':>6} {'Instance':>14} {'vCPU':>6} {'Memory':>8}")
    log("-" * 52)
    log(f"{'TiDB':<12} {cluster_info.get('tidb_count', 2):>6} {host_type:>14} {host_specs.get('vcpu', 16):>6} {host_specs.get('memory_gb', 32):>6}GB")
    log(f"{'TiKV':<12} {cluster_info.get('tikv_count', 3):>6} {host_type:>14} {host_specs.get('vcpu', 16):>6} {host_specs.get('memory_gb', 32):>6}GB")
    log(f"{'PD':<12} {cluster_info.get('pd_count', 3):>6} {host_type:>14} {host_specs.get('vcpu', 16):>6} {host_specs.get('memory_gb', 32):>6}GB")
    log(f"{'Client (EC2)':<12} {'1':>6} {host_type:>14} {host_specs.get('vcpu', 16):>6} {host_specs.get('memory_gb', 32):>6}GB")
    server_count = (cluster_info.get('tidb_count', 2) +
                    cluster_info.get('tikv_count', 3) +
                    cluster_info.get('pd_count', 3))
    log(f"  Total server hosts: {server_count} (each pod on its own dedicated host)")
    log("")
    
    # EBS storage details (per TiKV node — each node has its own EBS volume)
    ebs_type = "gp3"
    ebs_size_gb = 600  # Default root volume size
    ebs_iops = 3000    # gp3 baseline IOPS
    ebs_throughput = 125  # gp3 baseline MB/s
    
    log("--- Storage Resources ---")
    log(f"{'Volume':<12} {'Type':>8} {'Size':>10} {'IOPS':>8} {'Throughput':>12}")
    log("-" * 56)
    log(f"{'Per-node EBS':<12} {ebs_type:>8} {ebs_size_gb:>8}GB {ebs_iops:>8} {ebs_throughput:>9}MB/s")
    
    # Calculate EBS costs
    ebs_monthly = ebs_size_gb * cluster_info.get('tikv_count', 3) * AWS_COSTS["ebs"]["gp3_per_gb_month"]
    log(f"{'':>12} EBS Cost: ${ebs_monthly:.2f}/month total ({cluster_info.get('tikv_count', 3)} nodes, ${ebs_monthly/730:.4f}/hr)")
    log("")
    
    # Network info
    log("--- Network Configuration ---")
    log(f"Cross-AZ transfer: ${AWS_COSTS['network']['cross_az_per_gb']:.3f}/GB (bidirectional)")
    log(f"Same-AZ transfer:  ${AWS_COSTS['network']['same_az_per_gb']:.3f}/GB (free)")
    log("")
    
    # Quick cost preview
    hourly_compute = AWS_COSTS["ec2"].get(host_type, {}).get("hourly", 0.58) * server_count
    hourly_storage = ebs_monthly / 730
    total_hourly = hourly_compute + hourly_storage
    log(f"--- Hourly Rate Preview ({server_count} server hosts + 1 client) ---")
    log(f"EC2 Compute: ${hourly_compute:.3f}/hr | EBS Storage: ${hourly_storage:.4f}/hr | Total: ${total_hourly:.3f}/hr")
    log("=" * 80)


def start_resource_monitor(host: str, key_path: Path, interval: int = 60, 
                          cost_tracker: Optional['CostTracker'] = None,
                          start_time: Optional[float] = None) -> threading.Thread:
    """Start background thread that logs resource utilization every interval."""
    stop_event = threading.Event()
    _start_time = start_time or time.time()
    
    def monitor_resources():
        iteration = 0
        while not stop_event.is_set():
            iteration += 1
            elapsed = time.time() - _start_time
            elapsed_min = elapsed / 60
            
            script = """
# Compact resource summary
echo "--- $(date +%H:%M:%S) Resource Snapshot ---"

# Client VM stats
CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1 2>/dev/null || echo "N/A")
MEM=$(free -m | awk 'NR==2{printf "%.0f%%", $3*100/$2}' 2>/dev/null || echo "N/A")
CONN=$(mysql -h 127.0.0.1 -P 4000 -u root -N -e "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Threads_connected';" 2>/dev/null || echo "N/A")
echo "Client: CPU=${CPU}% Mem=${MEM} Conn=${CONN}"

# Pod utilization (compact)
echo "Pods:"
kubectl top pods -n tidb-cluster --no-headers 2>/dev/null | awk '{printf "  %-20s CPU:%-6s Mem:%s\\n", $1, $2, $3}' | head -8 || echo "  N/A"
"""
            result = ssh_capture(host, script, key_path)
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    log(line)
            
            # Show cost accrual if tracker provided
            if cost_tracker:
                summary = cost_tracker.get_summary(elapsed)
                log(f"  Cost so far: ${summary['grand_total']:.4f} ({elapsed_min:.1f} min elapsed)")
            
            log("")
            stop_event.wait(interval)
    
    thread = threading.Thread(target=monitor_resources, daemon=True)
    thread.stop_event = stop_event
    thread.start()
    return thread


def stop_resource_monitor(thread: threading.Thread):
    """Stop the resource monitor thread."""
    if hasattr(thread, 'stop_event'):
        thread.stop_event.set()


def build_sysbench_cmd(
    workload: str,
    tables: int,
    table_size: int,
    threads: int,
    duration: int,
    report_interval: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
    skip_trx: bool = False,
) -> str:
    """Build sysbench command with proper flags."""
    cmd = f"""sysbench {workload} \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db={database} \\
    --tables={tables} \\
    --table-size={table_size} \\
    --threads={threads} \\
    --time={duration} \\
    --report-interval={report_interval} \\
    --percentile=99 \\
    --histogram \\
    --mysql-ignore-errors={MYSQL_IGNORE_ERRORS}"""
    
    if skip_trx:
        cmd += " \\\n    --skip_trx=true"
    
    cmd += " \\\n    run 2>&1"
    return cmd


# Regex to parse sysbench interval lines like:
# [ 10s ] thds: 16 tps: 355.96 qps: 7136.79 (r/w/o: 4998.23/1425.04/713.52) lat (ms,99%): 61.08 err/s: 0.00 reconn/s: 0.00
_INTERVAL_RE = re.compile(
    r'\[\s*(\d+)s\s*\].*?tps:\s*([\d.]+).*?qps:\s*([\d.]+)'
    r'.*?lat\s*\(ms,\d+%\):\s*([\d.]+).*?err/s:\s*([\d.]+)'
)


def parse_interval_line(line: str) -> dict:
    """Parse a sysbench interval line into structured data."""
    m = _INTERVAL_RE.match(line.strip())
    if not m:
        return None
    return {
        "elapsed_s": int(m.group(1)),
        "tps": float(m.group(2)),
        "qps": float(m.group(3)),
        "p99_ms": float(m.group(4)),
        "err_per_sec": float(m.group(5)),
    }


def fetch_resource_snapshot_compact(host: str, key_path: Path) -> str:
    """Fetch a compact resource snapshot from the remote host. Returns formatted string."""
    script = '''
# Client CPU
CPU=$(top -bn1 | grep "Cpu(s)" | awk '{printf "%.0f%%", $2+$4}' 2>/dev/null || echo "N/A")
MEM=$(free -m | awk 'NR==2{printf "%.0f%%", $3*100/$2}' 2>/dev/null || echo "N/A")
echo "CLIENT cpu=$CPU mem=$MEM"

# Per-node resource from TiDB CLUSTER_LOAD
mysql -h 127.0.0.1 -P 4000 -u root -N -e "
SELECT
    CONCAT('NODE ', TYPE, '-', SUBSTRING_INDEX(SUBSTRING_INDEX(INSTANCE, '.', 1), '-', -1),
           ' cpu=', ROUND((1 - MAX(CASE WHEN NAME='idle' AND DEVICE_NAME='usage' THEN VALUE ELSE NULL END)) * 100), '%',
           ' mem=', ROUND(MAX(CASE WHEN NAME='used-percent' AND DEVICE_TYPE='memory' THEN VALUE ELSE NULL END) * 100), '%')
FROM information_schema.CLUSTER_LOAD
WHERE (DEVICE_TYPE='cpu' AND DEVICE_NAME='usage') OR (DEVICE_TYPE='memory' AND DEVICE_NAME='virtual')
GROUP BY TYPE, INSTANCE
ORDER BY TYPE, INSTANCE;
" 2>/dev/null || echo "NODE N/A"
'''
    result = ssh_capture(host, script, key_path)
    return result.stdout.strip()


def get_disk_utilization(host: str, key_path: Path, port: int = DEFAULT_PORT,
                        ebs_size_gb: int = DEFAULT_EBS_SIZE_GB) -> dict:
    """Measure current disk utilization from TiDB store sizes and EBS volume.

    Returns dict with:
      ebs_total_gb: Total EBS volume size
      ebs_used_gb: Used space on EBS (from df)
      ebs_used_pct: EBS used percentage
      tikv_store_gb: Total TiKV store size (data on disk)
      tikv_store_pct: TiKV store as % of EBS total
      db_data_gb: Size of benchmark database data
    """
    script = f'''
# EBS volume usage from df (root filesystem)
DF_LINE=$(df -BG / | tail -1)
EBS_USED=$(echo "$DF_LINE" | awk '{{print $3}}' | tr -d 'G')
EBS_TOTAL=$(echo "$DF_LINE" | awk '{{print $2}}' | tr -d 'G')
EBS_PCT=$(echo "$DF_LINE" | awk '{{print $5}}' | tr -d '%')
echo "EBS_USED=$EBS_USED"
echo "EBS_TOTAL=$EBS_TOTAL"
echo "EBS_PCT=$EBS_PCT"

# TiKV store sizes from INFORMATION_SCHEMA
mysql -h 127.0.0.1 -P {port} -u root -N -e "
SELECT CONCAT('TIKV_STORE_BYTES=', IFNULL(SUM(AVAILABLE)+SUM(CAPACITY)-SUM(AVAILABLE), 0))
FROM INFORMATION_SCHEMA.TIKV_STORE_STATUS;
" 2>/dev/null || echo "TIKV_STORE_BYTES=0"

# Benchmark database size
mysql -h 127.0.0.1 -P {port} -u root -N -e "
SELECT CONCAT('DB_DATA_BYTES=', IFNULL(SUM(data_length + index_length), 0))
FROM information_schema.tables
WHERE table_schema = 'sbtest';
" 2>/dev/null || echo "DB_DATA_BYTES=0"
'''
    result = ssh_capture(host, script, key_path)
    info = {
        "ebs_total_gb": ebs_size_gb,
        "ebs_used_gb": 0,
        "ebs_used_pct": 0,
        "tikv_store_gb": 0,
        "tikv_store_pct": 0,
        "db_data_gb": 0,
    }
    for line in result.stdout.strip().split('\n'):
        line = line.strip()
        if line.startswith('EBS_USED='):
            try:
                info["ebs_used_gb"] = int(line.split('=')[1])
            except ValueError:
                pass
        elif line.startswith('EBS_TOTAL='):
            try:
                val = int(line.split('=')[1])
                if val > 0:
                    info["ebs_total_gb"] = val
            except ValueError:
                pass
        elif line.startswith('EBS_PCT='):
            try:
                info["ebs_used_pct"] = int(line.split('=')[1])
            except ValueError:
                pass
        elif line.startswith('TIKV_STORE_BYTES='):
            try:
                b = int(line.split('=')[1])
                info["tikv_store_gb"] = b / (1024 ** 3)
                info["tikv_store_pct"] = (b / (1024 ** 3)) / info["ebs_total_gb"] * 100
            except ValueError:
                pass
        elif line.startswith('DB_DATA_BYTES='):
            try:
                info["db_data_gb"] = int(line.split('=')[1]) / (1024 ** 3)
            except ValueError:
                pass
    return info


def calculate_bulk_load_params(
    target_disk_pct: float,
    ebs_total_gb: int,
    current_disk_used_gb: float,
    num_tables: int,
) -> dict:
    """Calculate how many rows per table to load to reach target disk utilization.

    Each TiKV pod runs on its own dedicated host with its own EBS volume.
    With replication factor 3 across 3 TiKV nodes, every region is stored
    on every node, so each node's disk grows by roughly the full dataset
    size.  We target disk fill on a single TiKV node's EBS.

    Args:
        target_disk_pct: Target disk utilization percentage (e.g. 30)
        ebs_total_gb: Per-node EBS volume size in GB
        current_disk_used_gb: Current disk usage in GB (OS + existing data)
        num_tables: Number of sysbench tables to create

    Returns:
        dict with target_data_gb, rows_per_table, estimated_total_gb
    """
    target_total_gb = ebs_total_gb * (target_disk_pct / 100.0)
    available_for_data_gb = max(0, target_total_gb - current_disk_used_gb)

    # With RF=3 across 3 TiKV nodes, each node stores ~100% of all data.
    # So loading N GB of sysbench data -> each TiKV disk grows by ~N GB.
    # No division by replica count needed.

    # Calculate rows needed
    total_rows = int(available_for_data_gb * (1024 ** 3) / SYSBENCH_ROW_DISK_BYTES)
    rows_per_table = max(10000, total_rows // num_tables)  # minimum 10K rows

    # Estimated disk usage after load (per TiKV node)
    estimated_disk_gb = (rows_per_table * num_tables * SYSBENCH_ROW_DISK_BYTES) / (1024 ** 3)
    estimated_total_gb = current_disk_used_gb + estimated_disk_gb

    return {
        "target_data_gb": available_for_data_gb,
        "rows_per_table": rows_per_table,
        "total_rows": rows_per_table * num_tables,
        "estimated_disk_gb": estimated_disk_gb,
        "estimated_total_gb": estimated_total_gb,
        "estimated_disk_pct": (estimated_total_gb / ebs_total_gb) * 100,
    }


def run_bulk_data_load(
    host: str,
    key_path: Path,
    target_disk_pct: float,
    num_tables: int,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
    ebs_size_gb: int = DEFAULT_EBS_SIZE_GB,
) -> dict:
    """Phase 1: Bulk-load data to reach target disk utilization.

    Measures current disk usage, calculates how many rows are needed to
    reach the target percentage, then runs sysbench prepare with the
    calculated table size. Verifies disk utilization after loading.

    Returns dict with load stats including actual disk utilization achieved.
    """
    log("")
    log("=" * 70)
    log("PHASE 1: BULK DATA LOAD")
    log(f"  Target disk utilization: {target_disk_pct}%")
    log("=" * 70)

    # Step 1: Measure current disk utilization
    log("")
    log("Measuring current disk utilization...")
    disk_before = get_disk_utilization(host, key_path, port, ebs_size_gb)
    log(f"  EBS volume: {disk_before['ebs_total_gb']}GB total, "
        f"{disk_before['ebs_used_gb']}GB used ({disk_before['ebs_used_pct']}%)")
    log(f"  TiKV stores: {disk_before['tikv_store_gb']:.1f}GB ({disk_before['tikv_store_pct']:.1f}% of EBS)")
    log(f"  Benchmark DB: {disk_before['db_data_gb']:.2f}GB")

    # Step 2: Calculate load parameters
    params = calculate_bulk_load_params(
        target_disk_pct=target_disk_pct,
        ebs_total_gb=disk_before['ebs_total_gb'],
        current_disk_used_gb=disk_before['ebs_used_gb'],
        num_tables=num_tables,
    )

    log("")
    log("Bulk load plan:")
    log(f"  Tables: {num_tables}")
    log(f"  Rows per table: {params['rows_per_table']:,}")
    log(f"  Total rows: {params['total_rows']:,}")
    log(f"  Estimated data on disk (per TiKV node): {params['estimated_disk_gb']:.1f}GB")
    log(f"  Estimated total disk: {params['estimated_total_gb']:.1f}GB ({params['estimated_disk_pct']:.1f}%)")
    log("")

    # Step 3: Clean any existing benchmark tables and load fresh data
    load_start = time.time()
    run_sysbench_prepare(
        host, key_path,
        num_tables, params['rows_per_table'],
        port, database,
    )
    load_duration = time.time() - load_start

    # Step 4: Wait for TiKV compaction to settle and verify disk utilization
    log("")
    log("Waiting 30s for TiKV compaction to settle...")
    time.sleep(30)

    disk_after = get_disk_utilization(host, key_path, port, ebs_size_gb)
    log("")
    log("Disk utilization after bulk load:")
    log(f"  EBS volume: {disk_after['ebs_total_gb']}GB total, "
        f"{disk_after['ebs_used_gb']}GB used ({disk_after['ebs_used_pct']}%)")
    log(f"  TiKV stores: {disk_after['tikv_store_gb']:.1f}GB ({disk_after['tikv_store_pct']:.1f}% of EBS)")
    log(f"  Benchmark DB: {disk_after['db_data_gb']:.2f}GB")
    log(f"  Load time: {load_duration:.0f}s ({load_duration/60:.1f}min)")
    log(f"  Load rate: {params['total_rows'] / load_duration:,.0f} rows/sec")
    log("=" * 70)

    return {
        "tables": num_tables,
        "rows_per_table": params['rows_per_table'],
        "total_rows": params['total_rows'],
        "disk_before": disk_before,
        "disk_after": disk_after,
        "load_duration_s": load_duration,
        "load_rate_rows_per_sec": params['total_rows'] / load_duration if load_duration > 0 else 0,
    }

def format_minute_report(minute: int, intervals: list, resource_text: str) -> str:
    """Format a per-minute summary combining performance + resources."""
    lines = []

    # Aggregate interval data for this minute
    if intervals:
        avg_tps = sum(i['tps'] for i in intervals) / len(intervals)
        avg_qps = sum(i['qps'] for i in intervals) / len(intervals)
        max_p99 = max(i['p99_ms'] for i in intervals)
        avg_p99 = sum(i['p99_ms'] for i in intervals) / len(intervals)
        total_errs = sum(i['err_per_sec'] for i in intervals)
        total_txns_est = sum(i['tps'] * 10 for i in intervals)  # each interval ~10s
        avail = 100.0
        if total_txns_est > 0 and total_errs > 0:
            err_count_est = total_errs * 10 * len(intervals) / len(intervals)
            avail = max(0, (1 - err_count_est / (total_txns_est + err_count_est)) * 100)
        lines.append(f"--- Minute {minute} Report ---")
        lines.append(f"  Perf:  TPS={avg_tps:,.1f}  QPS={avg_qps:,.1f}  P99={avg_p99:.1f}ms (max {max_p99:.1f}ms)  err/s={total_errs/len(intervals):.2f}  avail={avail:.2f}%")
    else:
        lines.append(f"--- Minute {minute} Report ---")
        lines.append(f"  Perf:  (no interval data)")

    # Parse resource text
    client_line = ""
    node_lines = []
    if resource_text:
        for rline in resource_text.split('\n'):
            rline = rline.strip()
            if rline.startswith('CLIENT '):
                parts = rline.split()
                client_line = ' '.join(parts[1:])
            elif rline.startswith('NODE '):
                # Format: NODE type-N cpu=X% mem=Y%
                node_info = rline[5:].strip()  # strip 'NODE '
                node_lines.append(node_info)

    res_str = f"  Rsrc:  VM=[{client_line}]"
    if node_lines:
        res_str += f"  Nodes=[{', '.join(node_lines)}]"
    lines.append(res_str)

    return '\n'.join(lines)


def ssh_stream(host: str, script: str, key_path: Path):
    """Run a script on the remote host, streaming stdout line by line."""
    full = "set -euo pipefail\n" + textwrap.dedent(script).lstrip()
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
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True
    )
    proc.stdin.write(full)
    proc.stdin.close()
    return proc


def run_sysbench_streaming(
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
) -> dict:
    """Run sysbench with streaming output and per-minute reporting.

    Streams sysbench output line-by-line, parses interval lines,
    and prints a consolidated per-minute report with performance
    and resource utilization.
    """
    skip_trx = workload in ("oltp_read_only", "oltp_point_select")
    cmd = build_sysbench_cmd(
        workload, tables, table_size, threads, duration,
        report_interval, port, database, skip_trx
    )

    proc = ssh_stream(host, cmd, key_path)

    full_output = []
    current_minute = 0
    minute_intervals = []  # intervals for current minute
    all_intervals = []

    for raw_line in proc.stdout:
        line = raw_line.rstrip('\n')
        full_output.append(line)

        # Check if this is an interval line
        iv = parse_interval_line(line)
        if iv:
            all_intervals.append(iv)
            elapsed_s = iv['elapsed_s']
            minute_of = (elapsed_s - 1) // 60 + 1  # which minute this belongs to

            if minute_of > current_minute:
                # Print report for the completed minute
                if current_minute > 0 and minute_intervals:
                    resource_text = fetch_resource_snapshot_compact(host, key_path)
                    report = format_minute_report(
                        current_minute, minute_intervals, resource_text
                    )
                    log(report)
                    log("")
                current_minute = minute_of
                minute_intervals = []

            minute_intervals.append(iv)
        else:
            # Print non-interval lines through log() so they go to log file too
            if line.strip():
                log(line)

    proc.wait()

    # Print final minute report if there are remaining intervals
    if minute_intervals:
        resource_text = fetch_resource_snapshot_compact(host, key_path)
        report = format_minute_report(
            current_minute, minute_intervals, resource_text
        )
        log(report)
        log("")

    full_text = '\n'.join(full_output)
    return parse_sysbench_output(full_text, workload)

def set_session_variables(host: str, key_path: Path, port: int, workload: str):
    """Set TiDB session variables for benchmarking."""
    session_vars = SESSION_VARS_BASE
    if workload in ("oltp_read_only", "oltp_point_select"):
        session_vars += SESSION_VARS_READ_ONLY
    
    log(f"Setting session variables for {workload}...")
    # Note: sysbench doesn't support --init_connection in stock version
    # Session vars are set per-connection by TiDB, we log what would be ideal
    log(f"  Recommended: {session_vars.replace(';', '; ')}")


def parse_sysbench_output(output: str, workload: str) -> dict:
    """Parse sysbench output and extract detailed metrics including errors."""
    metrics = {
        "workload": workload,
        "transactions": {},
        "queries": {},
        "latency": {},
        "throughput": {},
        "errors": {"ignored": 0, "per_sec": 0.0},
        "availability_pct": 100.0,
    }
    
    lines = output.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Query counts
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
        
        # Transactions and TPS
        if "transactions:" in line and "per sec" in line:
            parts = line.split()
            for j, p in enumerate(parts):
                if "per" in p and j > 0:
                    try:
                        metrics["throughput"]["tps"] = float(parts[j-1].strip('('))
                        metrics["transactions"]["total"] = int(parts[1])
                    except (ValueError, IndexError):
                        pass
        
        # QPS
        if "queries:" in line and "per sec" in line:
            parts = line.split()
            for j, p in enumerate(parts):
                if "per" in p and j > 0:
                    try:
                        metrics["throughput"]["qps"] = float(parts[j-1].strip('('))
                    except (ValueError, IndexError):
                        pass
        
        # Ignored errors - critical for availability calculation
        if "ignored errors:" in line:
            parts = line.split()
            try:
                # Format: "ignored errors:      15      (0.50 per sec.)"
                for j, p in enumerate(parts):
                    if p == "errors:":
                        metrics["errors"]["ignored"] = int(parts[j+1])
                    if "per" in p and j > 0:
                        metrics["errors"]["per_sec"] = float(parts[j-1].strip('('))
            except (ValueError, IndexError):
                pass
        
        # Latency metrics
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
        elif "99th percentile:" in line or ("percentile:" in line.lower() and "95th" not in line):
            # sysbench with --percentile=99 may output just "percentile:" or "99th percentile:"
            try:
                metrics["latency"]["p99_ms"] = float(line.split(":")[1].strip())
            except (ValueError, IndexError):
                pass
    
    # Calculate availability
    total_txns = metrics["transactions"].get("total", 0)
    ignored_errors = metrics["errors"].get("ignored", 0)
    if total_txns + ignored_errors > 0:
        metrics["availability_pct"] = (total_txns / (total_txns + ignored_errors)) * 100
    
    return metrics


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
) -> dict:
    """Run sysbench benchmark with per-minute reporting and return parsed metrics."""
    log(f"Running sysbench {workload} benchmark")
    log(f"  Tables: {tables}, Table size: {table_size:,}")
    log(f"  Threads: {threads}, Duration: {duration}s")
    log(f"  Error handling: ignoring errors {MYSQL_IGNORE_ERRORS}")
    
    skip_trx = workload in ("oltp_read_only", "oltp_point_select")
    if skip_trx:
        log(f"  Read-only mode: --skip_trx=true")
    
    return run_sysbench_streaming(
        host, key_path, workload, tables, table_size,
        threads, duration, report_interval, port, database,
    )


def print_workload_summary(workload: str, tables: int, table_size: int, 
                          threads: int, duration: int, profile_name: str = None):
    """Print a detailed summary of the workload that was executed."""
    log("")
    log("=" * 70)
    log("WORKLOAD SUMMARY")
    log("=" * 70)
    
    # Workload description
    workload_descriptions = {
        "oltp_read_write": {
            "name": "OLTP Read-Write (Mixed)",
            "description": "Simulates a typical OLTP application with mixed read/write operations",
            "operations": [
                "Point SELECT queries (primary key lookups)",
                "Range SELECT queries (secondary index scans)",
                "UPDATE statements (indexed columns)",
                "DELETE + INSERT pairs (maintaining table size)",
            ],
            "read_write_ratio": "70% reads / 30% writes",
            "transaction_type": "Multi-statement transactions with BEGIN/COMMIT",
        },
        "oltp_read_only": {
            "name": "OLTP Read-Only",
            "description": "Pure read workload simulating read-heavy applications",
            "operations": [
                "Point SELECT queries (primary key lookups)",
                "Range SELECT queries (10 rows per query)",
                "SUM aggregations on indexed columns",
                "ORDER BY with LIMIT queries",
                "DISTINCT SELECT queries",
            ],
            "read_write_ratio": "100% reads / 0% writes",
            "transaction_type": "Read-only transactions (--skip_trx enabled)",
        },
        "oltp_write_only": {
            "name": "OLTP Write-Only",
            "description": "Pure write workload for testing write throughput",
            "operations": [
                "UPDATE statements on indexed columns",
                "UPDATE statements on non-indexed columns",
                "DELETE statements",
                "INSERT statements",
            ],
            "read_write_ratio": "0% reads / 100% writes",
            "transaction_type": "Multi-statement write transactions",
        },
        "oltp_point_select": {
            "name": "OLTP Point Select",
            "description": "Simple primary key lookups - tests raw read latency",
            "operations": [
                "Single-row SELECT by primary key",
            ],
            "read_write_ratio": "100% reads / 0% writes",
            "transaction_type": "Simple queries (--skip_trx enabled)",
        },
    }
    
    wl_info = workload_descriptions.get(workload, {
        "name": workload,
        "description": f"Custom workload: {workload}",
        "operations": ["Custom operations"],
        "read_write_ratio": "Unknown",
        "transaction_type": "Standard transactions",
    })
    
    log(f"Workload: {wl_info['name']}")
    if profile_name:
        log(f"Profile:  {profile_name}")
    log(f"")
    log(f"Description: {wl_info['description']}")
    log(f"")
    
    # Dataset info
    total_rows = tables * table_size
    estimated_size_mb = total_rows * 120 / (1024 * 1024)  # ~120 bytes per row
    log(f"Dataset:")
    log(f"  Tables:     {tables}")
    log(f"  Rows/table: {table_size:,}")
    log(f"  Total rows: {total_rows:,}")
    log(f"  Est. size:  {estimated_size_mb:,.1f} MB")
    log(f"")
    
    # Concurrency info
    log(f"Concurrency:")
    log(f"  Threads:    {threads} concurrent connections")
    log(f"  Duration:   {duration} seconds")
    log(f"")
    
    # Operations
    log(f"Operations per transaction:")
    for op in wl_info['operations']:
        log(f"  • {op}")
    log(f"")
    log(f"Read/Write Ratio: {wl_info['read_write_ratio']}")
    log(f"Transaction Mode: {wl_info['transaction_type']}")
    log("=" * 70)

def print_metrics_summary(metrics: dict):
    """Print a formatted summary of benchmark metrics."""
    log("")
    log("=" * 70)
    log(f"BENCHMARK RESULTS: {metrics.get('workload', 'unknown')}")
    log("=" * 70)
    
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
    
    # Availability metrics
    errors = metrics.get("errors", {})
    avail = metrics.get("availability_pct", 100.0)
    log(f"  Availability:")
    log(f"    Success Rate: {avail:.3f}%")
    log(f"    Ignored Errors: {errors.get('ignored', 0):,} ({errors.get('per_sec', 0):.2f}/sec)")
    
    queries = metrics.get("queries", {})
    if queries:
        log(f"  Query Breakdown:")
        log(f"    Read:  {queries.get('read', 'N/A'):,}")
        log(f"    Write: {queries.get('write', 'N/A'):,}")
        log(f"    Other: {queries.get('other', 'N/A'):,}")
        log(f"    Total: {queries.get('total', 'N/A'):,}")
    
    log("=" * 70)


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
    """Run adaptive overload phase: increase threads until system breaks (50% QPS decay or 10x latency)."""
    results = []
    start_threads = phase["threads"]
    thread_step = phase.get("thread_step", 32)
    max_threads = phase.get("max_threads", 512)
    decay_threshold = phase.get("decay_threshold", 0.50)
    latency_multiplier = phase.get("latency_multiplier", 10)
    phase_duration = phase.get("duration", 30)
    
    current_threads = start_threads
    iteration = 0
    peak_qps = 0
    peak_threads = start_threads
    baseline_p95 = None
    
    skip_trx = workload in ("oltp_read_only", "oltp_point_select")
    
    log(f"    Adaptive mode: starting at {start_threads} threads, step={thread_step}, max={max_threads}")
    log(f"    Breaking conditions: QPS decay >= {decay_threshold*100:.0f}% OR latency >= {latency_multiplier}x baseline")
    
    while current_threads <= max_threads:
        iteration += 1
        log(f"")
        log(f"    >> Iteration {iteration}: {current_threads} threads, {phase_duration}s")
        
        cmd = build_sysbench_cmd(
            workload, tables, table_size, current_threads, phase_duration,
            report_interval, port, database, skip_trx
        )
        
        result = ssh_capture(host, cmd, key_path)
        output = result.stdout
        print(output)
        
        metrics = parse_sysbench_output(output, workload)
        metrics["phase"] = f"overload_iter{iteration}"
        metrics["threads"] = current_threads
        metrics["duration"] = phase_duration
        results.append(metrics)
        
        current_qps = metrics.get("throughput", {}).get("qps", 0)
        lat = metrics.get("latency", {})
        current_p95 = lat.get("p95_ms", 0)
        current_p99 = lat.get("p99_ms", 0)
        avail = metrics.get("availability_pct", 100.0)
        errors = metrics.get("errors", {}).get("ignored", 0)
        
        # Set baseline latency from first iteration
        if baseline_p95 is None and current_p95 > 0:
            baseline_p95 = current_p95
            log(f"       Baseline P95: {baseline_p95:.1f}ms")
        
        # Track peak QPS
        if current_qps > peak_qps:
            peak_qps = current_qps
            peak_threads = current_threads
        
        # Calculate metrics
        decay_from_peak = (peak_qps - current_qps) / peak_qps if peak_qps > 0 else 0
        latency_ratio = current_p95 / baseline_p95 if baseline_p95 and baseline_p95 > 0 else 1
        
        log(f"       QPS: {current_qps:,.1f} | Peak: {peak_qps:,.1f} @ {peak_threads} thr | Decay: {decay_from_peak*100:.1f}%")
        log(f"       P95: {current_p95:.1f}ms P99: {current_p99:.1f}ms | Ratio: {latency_ratio:.1f}x | Avail: {avail:.2f}% (err: {errors})")
        
        # Check breaking conditions
        broken = False
        break_reason = ""
        
        if decay_from_peak >= decay_threshold:
            broken = True
            break_reason = f"QPS decayed {decay_from_peak*100:.1f}% from peak (threshold: {decay_threshold*100:.0f}%)"
        elif latency_ratio >= latency_multiplier:
            broken = True
            break_reason = f"Latency {latency_ratio:.1f}x baseline (threshold: {latency_multiplier}x)"
        
        if broken:
            log(f"")
            log(f"    !! BREAKING POINT REACHED: {break_reason}")
            log(f"    !! Peak QPS: {peak_qps:,.1f} at {peak_threads} threads")
            log(f"    !! Final: {current_qps:,.1f} QPS, P95={current_p95:.1f}ms, P99={current_p99:.1f}ms @ {current_threads} threads")
            break
        
        current_threads += thread_step
        
        if current_threads <= max_threads:
            log(f"       Ramping up to {current_threads} threads...")
            time.sleep(3)
    
    if current_threads > max_threads:
        log(f"    !! Reached max threads ({max_threads}) without breaking")
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
    
    total_phases = len(profile['phases'])
    if profile['total_duration'] == "adaptive":
        estimated = sum(p.get('duration', 30) for p in profile['phases'])
        log(f"  Phases: {total_phases} (adaptive duration, min ~{estimated}s)")
    else:
        log(f"  Total duration: {profile['total_duration']}s ({total_phases} phases)")
    log(f"  Error handling: ignoring {MYSQL_IGNORE_ERRORS}")
    log("=" * 70)
    
    skip_trx = workload in ("oltp_read_only", "oltp_point_select")
    all_results = []
    
    for i, phase in enumerate(profile["phases"]):
        phase_name = phase["name"]
        threads = phase["threads"]
        duration = phase["duration"]
        is_adaptive = phase.get("adaptive", False)
        
        log("")
        log(f">>> PHASE {i+1}/{len(profile['phases'])}: {phase_name.upper()}")
        if is_adaptive:
            log(f"    Mode: ADAPTIVE (start={threads}, step={phase.get('thread_step', 32)}, max={phase.get('max_threads', 512)})")
        else:
            log(f"    Threads: {threads}, Duration: {duration}s")
        log("-" * 50)
        
        if is_adaptive:
            phase_results = run_adaptive_phase(
                host, key_path, phase, workload,
                tables, table_size, report_interval,
                port, database
            )
            all_results.extend(phase_results)
        else:
            metrics = run_sysbench_streaming(
                host, key_path, workload, tables, table_size,
                threads, duration, report_interval, port, database,
            )
            metrics["phase"] = phase_name
            metrics["threads"] = threads
            metrics["duration"] = duration
            all_results.append(metrics)
            
            tp = metrics.get("throughput", {})
            lat = metrics.get("latency", {})
            avail = metrics.get("availability_pct", 100.0)
            log(f"    Result: TPS={tp.get('tps', 0):,.1f} QPS={tp.get('qps', 0):,.1f} P95={lat.get('p95_ms', 'N/A')}ms P99={lat.get('p99_ms', 'N/A')}ms Avail={avail:.2f}%")
    
    return all_results


def print_multi_phase_summary(profile_name: str, results: list):
    """Print summary of multi-phase benchmark results."""
    log("")
    log("=" * 100)
    log(f"BENCHMARK SUMMARY: {profile_name.upper()}")
    log("=" * 100)
    log(f"{'Phase':<18} {'Thr':>5} {'Dur':>5} {'TPS':>10} {'QPS':>10} {'P95':>8} {'P99':>8} {'Avail':>8} {'Errors':>7}")
    log("-" * 100)
    
    total_errors = 0
    total_txns = 0
    
    for r in results:
        phase = r.get("phase", "unknown")
        threads = r.get("threads", 0)
        duration = r.get("duration", 0)
        tp = r.get("throughput", {})
        lat = r.get("latency", {})
        tps = tp.get("tps", 0)
        qps = tp.get("qps", 0)
        p95 = lat.get("p95_ms", 0)
        p99 = lat.get("p99_ms", 0)
        avail = r.get("availability_pct", 100.0)
        errors = r.get("errors", {}).get("ignored", 0)
        
        total_errors += errors
        total_txns += r.get("transactions", {}).get("total", 0)
        
        p95_str = f"{p95:.1f}" if isinstance(p95, (int, float)) and p95 > 0 else "N/A"
        p99_str = f"{p99:.1f}" if isinstance(p99, (int, float)) and p99 > 0 else "N/A"
        
        log(f"{phase:<18} {threads:>5} {duration:>4}s {tps:>10,.1f} {qps:>10,.1f} {p95_str:>8} {p99_str:>8} {avail:>7.2f}% {errors:>7}")
    
    log("-" * 100)
    
    # Aggregates
    total_duration = sum(r.get("duration", 0) for r in results)
    total_tps_weighted = sum(r.get("throughput", {}).get("tps", 0) * r.get("duration", 0) for r in results)
    avg_tps = total_tps_weighted / total_duration if total_duration > 0 else 0
    
    peak_tps = max((r.get("throughput", {}).get("tps", 0) for r in results), default=0)
    peak_phase = next((r["phase"] for r in results if r.get("throughput", {}).get("tps", 0) == peak_tps), "N/A")
    
    overall_avail = (total_txns / (total_txns + total_errors)) * 100 if (total_txns + total_errors) > 0 else 100.0
    
    log(f"Duration: {total_duration}s | Avg TPS: {avg_tps:,.1f} | Peak TPS: {peak_tps:,.1f} ({peak_phase})")
    log(f"Overall Availability: {overall_avail:.3f}% | Total Errors: {total_errors:,}")
    log("=" * 100)


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


def fetch_final_resource_snapshot(host: str, key_path: Path):
    """Fetch final resource utilization snapshot."""
    log("")
    log("=" * 70)
    log("FINAL RESOURCE UTILIZATION")
    log("=" * 70)
    
    script = '''
echo "--- Client VM ---"
echo "CPU: $(top -bn1 | grep "Cpu(s)" | awk '{printf "%.1f%% user, %.1f%% sys, %.1f%% idle", $2, $4, $8}')"
echo "Memory: $(free -h | awk 'NR==2{printf "%s used / %s total (%.1f%%)", $3, $2, $3*100/$2}')"
echo "Load: $(cat /proc/loadavg | awk '{print $1, $2, $3}')"
echo ""
echo "--- TiDB Cluster Nodes ---"
mysql -h 127.0.0.1 -P 4000 -u root -N -e "
SELECT
    CONCAT(UPPER(TYPE), '-', SUBSTRING_INDEX(SUBSTRING_INDEX(INSTANCE, '.', 1), '-', -1),
           '  CPU: ', ROUND((1 - MAX(CASE WHEN NAME='idle' AND DEVICE_NAME='usage' THEN VALUE ELSE NULL END)) * 100), '%',
           '  Mem: ', ROUND(MAX(CASE WHEN NAME='used-percent' AND DEVICE_TYPE='memory' THEN VALUE ELSE NULL END) * 100), '%')
FROM information_schema.CLUSTER_LOAD
WHERE (DEVICE_TYPE='cpu' AND DEVICE_NAME='usage') OR (DEVICE_TYPE='memory' AND DEVICE_NAME='virtual')
GROUP BY TYPE, INSTANCE
ORDER BY TYPE, INSTANCE;
" 2>/dev/null || echo "N/A"
'''
    result = ssh_capture(host, script, key_path)
    if result.stdout.strip():
        for line in result.stdout.strip().split('\n'):
            log(line)
    log("=" * 70)

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
        help="Workload profile: quick(30s), light(60s), medium(2m), heavy(5m, default), stress, scaling",
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
        "--no-resource-monitor",
        action="store_true",
        help="Disable per-minute resource monitoring during benchmark",
    )
    parser.add_argument(
        "--no-disk-fill",
        action="store_true",
        help="Skip Phase 1 bulk data load (use profile's table_size as-is instead of filling to disk target)",
    )
    parser.add_argument(
        "--disk-fill-pct",
        type=int,
        default=None,
        help="Override disk fill target percentage (default: from profile, typically 30%%)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output log file path (auto-generates if not specified, use 'none' to disable)",
    )

    return parser.parse_args()


def main():
    global _log_file
    args = parse_args()

    # Set up log file output
    log_path = None
    if args.output and args.output.lower() != 'none':
        log_path = Path(args.output).expanduser().resolve()
    elif args.output is None:  # Auto-generate log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = Path(f"tidb_benchmark_{args.profile}_{timestamp}.log").resolve()

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        _log_file = open(log_path, 'w')
        print(f"Logging output to: {log_path}")

    try:
        _run_benchmark(args)
    finally:
        if _log_file:
            _log_file.close()
            print(f"\nLog saved to: {log_path}")


def _run_benchmark(args):
    """Internal benchmark runner.

    Two-phase approach for every benchmark mode:
      Phase 1 (Bulk Load): Fill disk to target utilization (default 30%)
                           so benchmarks run against a realistic data volume.
      Phase 2 (Benchmark):  Run the actual workload (updates + reads) against
                           the pre-loaded data.

    Use --no-disk-fill to skip Phase 1 and use the profile's table_size as-is.
    """
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

    # Load profile settings
    tables = args.tables
    table_size = args.table_size
    threads = args.threads
    duration = args.duration
    disk_fill_pct = args.disk_fill_pct

    if args.profile:
        profile = WORKLOAD_PROFILES[args.profile]
        tables = profile["tables"]
        table_size = profile["table_size"]
        threads = profile["threads"]
        duration = profile["duration"]
        multi_phase = profile.get("multi_phase")
        if disk_fill_pct is None:
            disk_fill_pct = profile.get("disk_fill_pct", 30)
        log(f"Using profile '{args.profile}': {tables} tables, {threads} threads, {duration}s")
        if multi_phase:
            log(f"  Multi-phase mode: {multi_phase}")
    else:
        multi_phase = None
        if disk_fill_pct is None:
            disk_fill_pct = 30  # default

    # Print cluster summary and get instance info for cost tracking
    print_cluster_summary(host, key_path, args.region, args.aws_profile, args.seed, args.port)

    # Initialize cost tracker
    cost_tracker = CostTracker(region=args.region)
    inst_info = get_instance_info(args.region, args.aws_profile, args.seed)

    # Determine instance types
    client_type = "c7g.2xlarge"  # Default
    server_type = "c7g.2xlarge"  # Default
    for inst in inst_info.get("instances", []):
        if inst.get("type"):
            client_type = inst.get("type")
            server_type = inst.get("type")
            break

    # Get cluster info for pod count
    cluster_info = get_cluster_info(host, key_path, args.port)
    server_count = (cluster_info.get("tidb_count", 2) +
                    cluster_info.get("tikv_count", 3) +
                    cluster_info.get("pd_count", 3))

    cost_tracker.set_infrastructure(
        client_type=client_type,
        server_type=server_type,
        server_count=server_count,
        ebs_gb=DEFAULT_EBS_SIZE_GB,
    )

    benchmark_start_time = time.time()

    # ── PHASE 1: BULK DATA LOAD ──────────────────────────────────────────
    # Fill disk to target utilization so the benchmark runs against a
    # realistic data volume.  Writes in Phase 2 will be UPDATEs on this
    # pre-loaded data (not INSERTs into an empty database).
    load_stats = None
    if not args.no_disk_fill and not args.skip_prepare:
        load_stats = run_bulk_data_load(
            host, key_path,
            target_disk_pct=disk_fill_pct,
            num_tables=tables,
            port=args.port,
            ebs_size_gb=DEFAULT_EBS_SIZE_GB,
        )
        # Update table_size to match what was actually loaded
        table_size = load_stats["rows_per_table"]
        log(f"Phase 1 complete: {load_stats['total_rows']:,} rows loaded")
        log(f"  Actual disk: {load_stats['disk_after']['ebs_used_pct']}% of {load_stats['disk_after']['ebs_total_gb']}GB EBS")
    elif not args.skip_prepare:
        # Legacy mode: prepare with profile's table_size (small dataset, no disk fill)
        run_sysbench_prepare(
            host, key_path,
            tables, table_size,
            args.port,
        )

    if args.prepare_only:
        log("Tables prepared. Skipping benchmark (--prepare-only).")
        return

    # ── PHASE 2: BENCHMARK (updates + reads) ─────────────────────────────
    # The workload runs against the pre-loaded dataset:
    #   - oltp_read_write: mixed UPDATE + SELECT (the default / recommended)
    #   - oltp_read_only:  pure reads against the loaded data
    #   - oltp_write_only: pure UPDATE/DELETE/INSERT on existing rows
    #   - stress/scaling:  multi-phase with the loaded data
    log("")
    log("=" * 70)
    log("PHASE 2: BENCHMARK (updates + reads on loaded data)")
    log("=" * 70)

    # Print workload summary before running
    print_workload_summary(
        workload=args.workload,
        tables=tables,
        table_size=table_size,
        threads=threads,
        duration=duration,
        profile_name=args.profile,
    )

    # Print static monthly server hardware cost (excluding client and network)
    server_specs = AWS_COSTS["ec2"].get(server_type, {})
    server_hourly = server_specs.get("hourly", 0.29)
    tikv_count = cluster_info.get("tikv_count", 3)
    ebs_monthly = DEFAULT_EBS_SIZE_GB * tikv_count * AWS_COSTS["ebs"]["gp3_per_gb_month"]
    server_compute_monthly = server_hourly * server_count * 730  # each pod on its own host
    server_total_monthly = server_compute_monthly + ebs_monthly
    log("")
    log(f"Server Monthly Cost ({server_count} hosts): EC2=${server_compute_monthly:.0f} + EBS=${ebs_monthly:.0f} = ${server_total_monthly:.0f}/mo")
    log("")

    total_queries = 0
    total_transactions = 0
    avg_qps = 0
    avg_tps = 0
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
        # Sum up queries and calculate weighted average throughput
        total_duration = 0
        weighted_qps = 0
        weighted_tps = 0
        for r in phase_results:
            total_queries += r.get("queries", {}).get("total", 0)
            total_transactions += r.get("transactions", {}).get("total", 0)
            phase_dur = r.get("duration", 0)
            total_duration += phase_dur
            weighted_qps += r.get("throughput", {}).get("qps", 0) * phase_dur
            weighted_tps += r.get("throughput", {}).get("tps", 0) * phase_dur
        if total_duration > 0:
            avg_qps = weighted_qps / total_duration
            avg_tps = weighted_tps / total_duration
    else:
        benchmark_metrics = run_sysbench_benchmark(
            host, key_path,
            args.workload,
            tables, table_size,
            threads, duration, args.report_interval,
            args.port,
        )
        print_metrics_summary(benchmark_metrics)
        total_queries = benchmark_metrics.get("queries", {}).get("total", 0)
        total_transactions = benchmark_metrics.get("transactions", {}).get("total", 0)
        avg_qps = benchmark_metrics.get("throughput", {}).get("qps", 0)
        avg_tps = benchmark_metrics.get("throughput", {}).get("tps", 0)

    # Calculate benchmark duration
    benchmark_end_time = time.time()
    actual_duration = benchmark_end_time - benchmark_start_time

    # Track queries and throughput for cost and price-performance calculation
    cost_tracker.add_queries(
        query_count=total_queries,
        transaction_count=total_transactions,
        avg_qps=avg_qps,
        avg_tps=avg_tps,
        avg_row_size_bytes=200,
    )

    # Final resource snapshot + disk utilization
    fetch_final_resource_snapshot(host, key_path)
    if load_stats:
        disk_final = get_disk_utilization(host, key_path, args.port, DEFAULT_EBS_SIZE_GB)
        log("")
        log("--- Final Disk Utilization ---")
        log(f"  EBS: {disk_final['ebs_used_gb']}GB / {disk_final['ebs_total_gb']}GB ({disk_final['ebs_used_pct']}%)")
        log(f"  TiKV stores: {disk_final['tikv_store_gb']:.1f}GB ({disk_final['tikv_store_pct']:.1f}%)")
        log(f"  Benchmark DB: {disk_final['db_data_gb']:.2f}GB")

    # Print cost summary
    cost_tracker.print_summary(actual_duration)

    if args.cleanup:
        run_sysbench_cleanup(host, key_path, args.tables, args.port)

    log("Benchmark complete.")
if __name__ == "__main__":
    main()
