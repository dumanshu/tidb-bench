# TiDB Bench

Utilities for provisioning, validating, and benchmarking TiDB clusters using TiDB Operator on kind (Kubernetes in Docker). The scripts expect an AWS profile that can create EC2 resources and a pre-uploaded SSH key pair.

## Contents

- `tidb_setup.py` – provisions VPC + EC2 resources, installs dependencies (Docker, kind, Helm), deploys TiDB Operator and cluster, applies client-side tuning.
- `tidb_validate.py` – inspects the deployed cluster, runs health checks, prints SSH shortcuts and manual test commands.
- `tidb_benchmark.py` – runs sysbench benchmarks from the client host with resource monitoring, availability tracking, and cost estimates.

## Prerequisites

- Python 3.8+ with `boto3` available.
- AWS CLI v2 configured with a profile (e.g., `sandbox`) pointing at the target account.
- SSH key pair (`tidb-load-test-key`) uploaded to EC2.

### Generating and Uploading `tidb-load-test-key.pem`

```bash
# Generate PEM-formatted key locally
ssh-keygen -t rsa -b 4096 -m PEM -f tidb-load-test-key -N ""
chmod 400 tidb-load-test-key.pem

# Upload the public key to EC2 (imports as KeyPair)
aws ec2 import-key-pair \
  --key-name tidb-load-test-key \
  --public-key-material fileb://tidb-load-test-key.pub \
  --profile sandbox --region us-east-1
```

After importing, place `tidb-load-test-key.pem` in the repo root (or wherever you run the scripts) so SSH connections succeed.

## Provisioning the Stack

```bash
# Full deployment (default: 3 PD, 3 TiKV, 2 TiDB on c7g.2xlarge)
AWS_PROFILE=sandbox python3 tidb_setup.py

# With custom configuration
python3 tidb_setup.py \
  --aws-profile sandbox \
  --tidb-version v8.5.5 \
  --pd-replicas 3 \
  --tikv-replicas 3 \
  --tidb-replicas 2 \
  --instance-type c7g.4xlarge
```

Key flags:
- `--instance-type` controls the EC2 instance size (default: `c7g.4xlarge` for client)
- `--tidb-version` specifies TiDB version (default: `v8.5.5`)
- `--cleanup` to tear everything down

## Validating the Deployment

```bash
AWS_PROFILE=sandbox python3 tidb_validate.py

# Verbose output with pod details
python3 tidb_validate.py --aws-profile sandbox -v
```

The validator prints:
- Instance inventory + SSH shortcuts
- TiDB cluster status and pod health
- Manual benchmarking + connection commands
- Resource usage and cost estimates

## Running Benchmarks

```bash
# Standard benchmark (5 minutes, price-performance focused)
AWS_PROFILE=sandbox python3 tidb_benchmark.py --profile standard

# Quick smoke test (30 seconds)
python3 tidb_benchmark.py --aws-profile sandbox --profile quick

# Stress test (pushes to breaking point: 50% QPS decay or 10x latency)
python3 tidb_benchmark.py --aws-profile sandbox --profile stress

# Scaling test (traffic ramp-up and ramp-down)
python3 tidb_benchmark.py --aws-profile sandbox --profile scaling

# Custom workload
python3 tidb_benchmark.py \
  --aws-profile sandbox \
  --workload oltp_point_select \
  --tables 32 \
  --table-size 200000 \
  --threads 128 \
  --duration 300

# Skip preparation (use existing tables)
python3 tidb_benchmark.py --aws-profile sandbox --skip-prepare
```

## Workload Profiles

| Profile   | Tables | Rows/Table | Threads | Duration | Description |
|-----------|--------|------------|---------|----------|-------------|
| quick     | 4      | 10,000     | 16      | 30s      | Quick smoke test |
| light     | 8      | 50,000     | 32      | 60s      | Light validation |
| medium    | 16     | 100,000    | 64      | 2min     | Standard dev test |
| heavy     | 32     | 500,000    | 128     | 5min     | Load testing |
| **standard** | 16  | 100,000    | 64      | **5min** | Price-performance benchmark |
| stress    | 16     | 100,000    | adaptive| varies   | Breaking point test |
| scaling   | 16     | 100,000    | 16→96→32| 3min     | Scale up/down test |

### Multi-Phase Profiles

**Stress Profile** – Pushes TiDB until breaking point (50% QPS decay or 10x latency):
- Warmup → Ramp-up → Adaptive overload (keeps increasing until breaking) → Recovery → Sustained

**Scaling Profile** – Tests traffic scale-up and scale-down behavior:
- Baseline (16 threads) → Scale-up (32→64→96) → Peak → Scale-down (64→32)

## Benchmark Output

The benchmark provides comprehensive output including:

- **Workload Summary**: Description of operations, dataset size, concurrency
- **Per-Minute Resource Snapshots**: CPU/memory usage with cost accrual
- **Availability Tracking**: Success rate accounting for ignored transient errors
- **P99 Latency**: 99th percentile latency metrics
- **Cost Analysis**: Client vs server breakdown, price-performance metrics ($/QPS, $/TPS)
- **Monthly Projections**: Compute, storage, network cost estimates

Sample output sections:
```
======================================================================
WORKLOAD SUMMARY
======================================================================
Workload: OLTP Read-Write (Mixed)
Profile:  standard
Dataset:  16 tables × 100,000 rows = 1,600,000 total (~183 MB)
Threads:  64 concurrent connections for 300 seconds

======================================================================
BENCHMARK RESULTS: oltp_read_write
======================================================================
  Throughput:
    TPS: 357.24 transactions/sec
    QPS: 7,144.84 queries/sec
  Latency (ms):
    Min: 58.38 | Avg: 179.11 | P99: 292.6 | Max: 430.43
  Availability:
    Success Rate: 100.000%

======================================================================
COST SUMMARY
======================================================================
Monthly $/QPS: $0.0700
TOTAL: $499.89/month
======================================================================
```

## Available Workloads

- `oltp_read_write` (default) – Mixed read/write OLTP (70% reads / 30% writes)
- `oltp_read_only` – Read-only queries (auto-enables `--skip_trx`)
- `oltp_write_only` – Write-only operations
- `oltp_point_select` – Point selects (auto-enables `--skip_trx`)
- `oltp_insert` – Insert operations
- `oltp_delete` – Delete operations
- `oltp_update_index` – Index updates
- `oltp_update_non_index` – Non-index updates

## Architecture

```
EC2 Instance (c7g.4xlarge, ARM Graviton)
├── Docker
└── kind (Kubernetes in Docker)
    └── tidb-bench cluster
        ├── tidb-admin namespace
        │   └── TiDB Operator
        └── tidb-cluster namespace
            ├── PD x3 (Placement Driver)
            ├── TiKV x3 (Distributed Storage)
            └── TiDB x2 (SQL Layer)
```

## Session Variables

The benchmark automatically sets these TiDB session variables for optimal performance:
- `tikv_client_read_timeout=5` – 5 second read timeout
- `max_execution_time=10000` – 10 second max execution time

## Client-Side Tuning

The setup script automatically applies:
- **File descriptors**: 1,000,000 (soft/hard)
- **Network tuning**: TCP buffer sizes, connection backlog, port range
- **Kernel parameters**: perf profiling enabled for flamegraphs

## Manual Test Snippets

```bash
# SSH to the host
ssh -i tidb-load-test-key.pem ec2-user@<PUBLIC_IP>

# Connect to TiDB
mysql -h 127.0.0.1 -P 4000 -u root

# Check cluster status
kubectl get tc -n tidb-cluster
kubectl get pods -n tidb-cluster

# Manual sysbench run
sysbench oltp_read_write \
  --mysql-host=127.0.0.1 \
  --mysql-port=4000 \
  --mysql-user=root \
  --mysql-db=sbtest \
  --tables=16 \
  --table-size=100000 \
  --threads=64 \
  --time=300 \
  --report-interval=10 \
  --percentile=99 \
  run
```

## Cleanup

```bash
# Tear down all AWS resources
python3 tidb_setup.py --cleanup --aws-profile sandbox
```

## References

- [PingCAP TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [How Flipkart Scales Over 1M QPS with TiDB](https://www.pingcap.com/blog/how-flipkart-scales-over-1m-qps-with-zero-downtime-maintenance/)

