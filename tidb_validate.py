#!/usr/bin/env python3
"""
Validate TiDB load-test stack deployment and run health checks.
"""

import argparse
import os
import subprocess
import sys
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
DEFAULT_SEED = "tidblt-001"
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
DEFAULT_PORT = 4000

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


def discover_tidb_host(region: str, profile: Optional[str], seed: str) -> dict:
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
            if role in ("host", "client"):
                return {
                    "instance_id": inst["InstanceId"],
                    "public_ip": inst.get("PublicIpAddress"),
                    "private_ip": inst.get("PrivateIpAddress"),
                    "instance_type": inst.get("InstanceType"),
                    "state": inst.get("State", {}).get("Name"),
                    "availability_zone": inst.get("Placement", {}).get("AvailabilityZone"),
                }
    return None


def get_ebs_volumes(region: str, profile: Optional[str], instance_id: str) -> list:
    """Get EBS volume details for an EC2 instance."""
    client = ec2_client(profile, region)
    resp = client.describe_volumes(
        Filters=[{"Name": "attachment.instance-id", "Values": [instance_id]}]
    )
    volumes = []
    for vol in resp.get("Volumes", []):
        vol_info = {
            "volume_id": vol["VolumeId"],
            "size_gb": vol["Size"],
            "volume_type": vol["VolumeType"],
            "iops": vol.get("Iops"),
            "throughput": vol.get("Throughput"),
            "state": vol["State"],
        }
        for attach in vol.get("Attachments", []):
            if attach.get("InstanceId") == instance_id:
                vol_info["device"] = attach.get("Device")
        volumes.append(vol_info)
    return volumes


def ssh_capture(host: str, script: str, key_path: Path) -> subprocess.CompletedProcess:
    full = textwrap.dedent(script).lstrip()
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
    return subprocess.run(cmd, input=full, text=True, capture_output=True)


def check_ssh_connectivity(host: str, key_path: Path) -> bool:
    result = ssh_capture(host, "echo ok", key_path)
    return result.returncode == 0 and "ok" in result.stdout


def check_k3s(host: str, key_path: Path) -> bool:
    """Check if k3s is running (replaces Docker check)."""
    result = ssh_capture(host, "sudo systemctl is-active k3s >/dev/null 2>&1 && echo ok", key_path)
    return result.returncode == 0 and "ok" in result.stdout


def check_k3s_cluster(host: str, key_path: Path) -> bool:
    """Check if k3s cluster has nodes (replaces kind check)."""
    result = ssh_capture(host, "kubectl get nodes --no-headers 2>/dev/null | wc -l | xargs test 0 -lt && echo ok", key_path)
    return result.returncode == 0 and "ok" in result.stdout

def check_kubectl(host: str, key_path: Path) -> bool:
    result = ssh_capture(host, "kubectl cluster-info >/dev/null 2>&1 && echo ok", key_path)
    return result.returncode == 0 and "ok" in result.stdout


def get_tidb_pods(host: str, key_path: Path) -> str:
    result = ssh_capture(host, "kubectl get pods -n tidb-cluster -o wide 2>/dev/null", key_path)
    return result.stdout if result.returncode == 0 else ""


def get_tidb_cluster_status(host: str, key_path: Path) -> str:
    result = ssh_capture(host, "kubectl get tc -n tidb-cluster 2>/dev/null", key_path)
    return result.stdout if result.returncode == 0 else ""


def check_pd_ready(host: str, key_path: Path) -> bool:
    result = ssh_capture(host, """
kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=pd -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null
""", key_path)
    return result.stdout.strip() == "true"


def check_tikv_ready(host: str, key_path: Path) -> bool:
    result = ssh_capture(host, """
kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=tikv -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null
""", key_path)
    return result.stdout.strip() == "true"


def check_tidb_ready(host: str, key_path: Path) -> bool:
    result = ssh_capture(host, """
kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=tidb -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null
""", key_path)
    return result.stdout.strip() == "true"


def check_tidb_connection(host: str, key_path: Path, port: int) -> bool:
    result = ssh_capture(host, f"mysql -h 127.0.0.1 -P {port} -u root -e 'SELECT 1' 2>/dev/null", key_path)
    return result.returncode == 0


def get_tidb_version(host: str, key_path: Path, port: int) -> str:
    result = ssh_capture(host, f"mysql -h 127.0.0.1 -P {port} -u root -N -e 'SELECT version()' 2>/dev/null", key_path)
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def check_sysbench_installed(host: str, key_path: Path) -> bool:
    result = ssh_capture(host, "command -v sysbench >/dev/null 2>&1 && echo ok", key_path)
    return result.returncode == 0 and "ok" in result.stdout


def run_quick_benchmark(host: str, key_path: Path, port: int) -> dict:
    log("Running quick validation benchmark (10s)...")
    
    result = ssh_capture(host, f"""
mysql -h 127.0.0.1 -P {port} -u root -e "CREATE DATABASE IF NOT EXISTS sbtest_validate;"
mysql -h 127.0.0.1 -P {port} -u root -e "DROP TABLE IF EXISTS sbtest_validate.sbtest1;"

sysbench oltp_read_write \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db=sbtest_validate \\
    --tables=1 \\
    --table-size=10000 \\
    prepare 2>&1

sysbench oltp_read_write \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db=sbtest_validate \\
    --tables=1 \\
    --table-size=10000 \\
    --threads=8 \\
    --time=10 \\
    run 2>&1

sysbench oltp_read_write \\
    --mysql-host=127.0.0.1 \\
    --mysql-port={port} \\
    --mysql-user=root \\
    --mysql-db=sbtest_validate \\
    --tables=1 \\
    cleanup 2>&1

mysql -h 127.0.0.1 -P {port} -u root -e "DROP DATABASE IF EXISTS sbtest_validate;"
""", key_path)
    
    output = result.stdout
    metrics = {}
    
    for line in output.split('\n'):
        if 'transactions:' in line:
            parts = line.split()
            for i, p in enumerate(parts):
                if 'per' in p and i > 0:
                    try:
                        metrics['tps'] = float(parts[i-1].strip('('))
                    except (ValueError, IndexError):
                        pass
        if 'queries:' in line and 'per sec' in line:
            parts = line.split()
            for i, p in enumerate(parts):
                if 'per' in p and i > 0:
                    try:
                        metrics['qps'] = float(parts[i-1].strip('('))
                    except (ValueError, IndexError):
                        pass
        if '95th percentile:' in line:
            parts = line.split(':')
            if len(parts) > 1:
                try:
                    metrics['p95_latency_ms'] = float(parts[1].strip())
                except ValueError:
                    pass
    
    return metrics


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate TiDB load-test stack deployment."
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
    parser.add_argument(
        "--skip-benchmark",
        action="store_true",
        help="Skip quick validation benchmark",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed output",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    key_path = Path(args.ssh_key).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")
    
    log("=" * 60)
    log("TiDB Load Test Stack Validation")
    log("=" * 60)
    
    log("\n--- AWS Infrastructure ---")
    
    host_info = discover_tidb_host(args.region, args.aws_profile, args.seed)
    if not host_info:
        log("FAIL: No TiDB host instance found")
        if not args.host:
            raise SystemExit("Validation failed: no host found")
    else:
        log(f"  Instance ID: {host_info['instance_id']}")
        log(f"  Instance Type: {host_info['instance_type']}")
        log(f"  Availability Zone: {host_info.get('availability_zone', 'N/A')}")
        log(f"  State: {host_info['state']}")
        log(f"  Public IP: {host_info['public_ip']}")
        log(f"  Private IP: {host_info['private_ip']}")
        
        volumes = get_ebs_volumes(args.region, args.aws_profile, host_info['instance_id'])
        if volumes:
            log("\n--- EBS Storage ---")
            for vol in volumes:
                iops_str = f"{vol['iops']} IOPS" if vol.get('iops') else ""
                throughput_str = f"{vol['throughput']} MiB/s" if vol.get('throughput') else ""
                specs = ", ".join(filter(None, [iops_str, throughput_str]))
                log(f"  {vol.get('device', 'N/A')}: {vol['size_gb']}GB {vol['volume_type']}")
                if specs:
                    log(f"    Provisioned: {specs}")
    
    host = args.host or (host_info['public_ip'] if host_info else None)
    if not host:
        raise SystemExit("ERROR: No host IP available")
    
    log(f"\n--- Host Connectivity ({host}) ---")
    
    checks = []
    
    ssh_ok = check_ssh_connectivity(host, key_path)
    checks.append(("SSH Connectivity", ssh_ok))
    log(f"  SSH Connectivity: {'PASS' if ssh_ok else 'FAIL'}")
    
    if not ssh_ok:
        log("FAIL: Cannot connect to host via SSH")
        raise SystemExit("Validation failed: SSH connectivity")
    
    k3s_ok = check_k3s(host, key_path)
    checks.append(("k3s Service", k3s_ok))
    log(f"  k3s Service: {'PASS' if k3s_ok else 'FAIL'}")
    
    log("\n--- Kubernetes Cluster ---")
    
    cluster_ok = check_k3s_cluster(host, key_path)
    checks.append(("k3s Cluster", cluster_ok))
    log(f"  k3s Cluster: {'PASS' if cluster_ok else 'FAIL'}")
    
    kubectl_ok = check_kubectl(host, key_path)
    checks.append(("kubectl", kubectl_ok))
    log(f"  kubectl Access: {'PASS' if kubectl_ok else 'FAIL'}")
    
    if args.verbose and kubectl_ok:
        pods = get_tidb_pods(host, key_path)
        if pods:
            log("\n  Pods:")
            for line in pods.strip().split('\n'):
                log(f"    {line}")
    
    log("\n--- TiDB Components ---")
    
    pd_ok = check_pd_ready(host, key_path)
    checks.append(("PD Ready", pd_ok))
    log(f"  PD Ready: {'PASS' if pd_ok else 'FAIL'}")
    
    tikv_ok = check_tikv_ready(host, key_path)
    checks.append(("TiKV Ready", tikv_ok))
    log(f"  TiKV Ready: {'PASS' if tikv_ok else 'FAIL'}")
    
    tidb_ok = check_tidb_ready(host, key_path)
    checks.append(("TiDB Ready", tidb_ok))
    log(f"  TiDB Ready: {'PASS' if tidb_ok else 'FAIL'}")
    
    log("\n--- TiDB Database ---")
    
    conn_ok = check_tidb_connection(host, key_path, args.port)
    checks.append(("TiDB Connection", conn_ok))
    log(f"  MySQL Connection: {'PASS' if conn_ok else 'FAIL'}")
    
    if conn_ok:
        version = get_tidb_version(host, key_path, args.port)
        log(f"  TiDB Version: {version}")
    
    log("\n--- Tools ---")
    
    sysbench_ok = check_sysbench_installed(host, key_path)
    checks.append(("sysbench", sysbench_ok))
    log(f"  sysbench Installed: {'PASS' if sysbench_ok else 'FAIL'}")
    
    if not args.skip_benchmark and conn_ok and sysbench_ok:
        log("\n--- Quick Benchmark ---")
        metrics = run_quick_benchmark(host, key_path, args.port)
        if metrics:
            log(f"  TPS: {metrics.get('tps', 'N/A')}")
            log(f"  QPS: {metrics.get('qps', 'N/A')}")
            log(f"  P95 Latency: {metrics.get('p95_latency_ms', 'N/A')} ms")
    
    log("\n" + "=" * 60)
    log("Validation Summary")
    log("=" * 60)
    
    passed = sum(1 for _, ok in checks if ok)
    total = len(checks)
    
    for name, ok in checks:
        status = "PASS" if ok else "FAIL"
        log(f"  [{status}] {name}")
    
    log(f"\nResult: {passed}/{total} checks passed")
    
    if passed == total:
        log("\nStack is healthy and ready for benchmarking!")
        return 0
    else:
        log("\nSome checks failed. Review the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
