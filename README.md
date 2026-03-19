# TiDB Bench

Utilities for provisioning, validating, and benchmarking TiDB clusters on bare EC2 with k3s and TiDB Operator. Supports **multi-AZ deployments** (3 availability zones by default) and **TiCDC replication** benchmarking with lag measurement.

## Contents

- `tidb_setup.py` -- provisions VPC + EC2 resources across multiple AZs, installs k3s, deploys TiDB Operator and cluster(s), applies client-side tuning.
- `tidb_validate.py` -- inspects the deployed cluster, runs health checks, prints SSH shortcuts and manual test commands.
- `tidb_benchmark.py` -- runs sysbench benchmarks from the client host with resource monitoring, availability tracking, cost estimates, and optional TiCDC replication lag measurement.

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

## Architecture

### Multi-AZ Deployment (Default)

Each component runs on a **dedicated EC2 instance** (one pod per VM) spread across 3 availability zones. This gives each TiKV raft group 1 leader + 2 replicas in separate AZs for true fault tolerance.

```
VPC 10.43.0.0/16
|
+-- Subnet us-east-1a (10.43.1.0/24)
|   +-- EC2 (PD-1)          -- k3s agent, label: tidb.pingcap.com/pd
|   +-- EC2 (TiKV-1)        -- k3s agent, label: tidb.pingcap.com/tikv
|   +-- EC2 (TiDB-1)        -- k3s agent, label: tidb.pingcap.com/tidb
|
+-- Subnet us-east-1b (10.43.2.0/24)
|   +-- EC2 (PD-2)          -- k3s agent, label: tidb.pingcap.com/pd
|   +-- EC2 (TiKV-2)        -- k3s agent, label: tidb.pingcap.com/tikv
|
+-- Subnet us-east-1c (10.43.3.0/24)
|   +-- EC2 (PD-3)          -- k3s agent, label: tidb.pingcap.com/pd
|   +-- EC2 (TiKV-3)        -- k3s agent, label: tidb.pingcap.com/tikv
|   +-- EC2 (TiDB-2)        -- k3s agent, label: tidb.pingcap.com/tidb
|
+-- EC2 (Client)             -- k3s server, runs sysbench
|
+-- k3s Cluster
    +-- tidb-admin namespace
    |   +-- TiDB Operator (Helm)
    +-- tidb-cluster namespace
        +-- PD x3    (topologySpreadConstraints: 1 per AZ)
        +-- TiKV x3  (topologySpreadConstraints: 1 per AZ)
        +-- TiDB x2  (spread across AZs)
```

Each TiKV pod consumes the entire EC2 VM. PD location labels are set to `topology.kubernetes.io/zone` and `kubernetes.io/hostname`, and TiDB Operator auto-propagates zone labels from the underlying k8s nodes to TiKV store labels.

### TiCDC Replication Mode

When `--ticdc` is specified, the setup deploys a **second TiDB cluster** (`downstream`) and configures TiCDC to replicate from the upstream cluster in real time using the [new TiCDC architecture](https://docs.pingcap.com/tidb/stable/ticdc-architecture/).

```
VPC 10.43.0.0/16
|
+-- Upstream Cluster (tidb-cluster namespace)
|   +-- PD x3, TiKV x3, TiDB x2
|   +-- TiCDC x3  (newarch=true, dedicated EC2 nodes)
|       |
|       +-- changefeed --> mysql sink
|
+-- Downstream Cluster (tidb-downstream namespace)
|   +-- PD x3, TiKV x3, TiDB x1
|   +-- NodePort 30401
|
+-- Client EC2
    +-- sysbench --> upstream (NodePort 30400)
    +-- lag tracker --> reads heartbeat from downstream
```

TiCDC is configured with:
- New architecture mode (`newarch = true`)
- Changefeed tuning: `worker-count=32`, `max-txn-row=512`, `mounter.worker-num=32`
- Cross-node scheduling: `scheduler.enable-table-across-nodes=true`

## Provisioning the Stack

### Standard Multi-AZ Deployment

```bash
# Full deployment (default: 3 PD, 3 TiKV, 2 TiDB across 3 AZs)
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

### With TiCDC Replication

```bash
# Deploy upstream + downstream + TiCDC
python3 tidb_setup.py \
  --aws-profile sandbox \
  --ticdc \
  --ticdc-replicas 3 \
  --downstream-tikv-replicas 3

# Custom downstream sizing
python3 tidb_setup.py \
  --aws-profile sandbox \
  --ticdc \
  --downstream-pd-replicas 3 \
  --downstream-tikv-replicas 3 \
  --downstream-tidb-replicas 1
```

### Key Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--instance-type` | `c7g.4xlarge` | EC2 instance size for client |
| `--tidb-version` | `v8.5.5` | TiDB version |
| `--multi-az` | `true` | Deploy across 3 AZs (disable with `--no-multi-az`) |
| `--ticdc` | `false` | Enable TiCDC replication mode |
| `--ticdc-replicas` | `1` | Number of TiCDC instances |
| `--downstream-pd-replicas` | `3` | PD replicas in downstream cluster |
| `--downstream-tikv-replicas` | `3` | TiKV replicas in downstream cluster |
| `--downstream-tidb-replicas` | `1` | TiDB replicas in downstream cluster |
| `--cleanup` | -- | Tear down all AWS resources |

## Validating the Deployment

```bash
AWS_PROFILE=sandbox python3 tidb_validate.py

# Verbose output with pod details
python3 tidb_validate.py --aws-profile sandbox -v
```

The validator prints:
- Instance inventory + SSH shortcuts
- TiDB cluster status and pod health
- TiCDC status and changefeed health (when deployed)
- Downstream cluster readiness (when deployed)
- Manual benchmarking + connection commands
- Resource usage and cost estimates

## Running Benchmarks

### Standard Benchmark

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

### TiCDC Replication Benchmark

When `--ticdc` is passed, the benchmark additionally:
1. Inserts heartbeat rows into the upstream cluster
2. Polls the downstream cluster for those heartbeats
3. Computes and records replication lag at 1-second intervals throughout the run
4. Reports lag statistics (min, avg, p50, p99, max) alongside benchmark results

```bash
# Benchmark with TiCDC lag measurement
python3 tidb_benchmark.py \
  --aws-profile sandbox \
  --profile standard \
  --ticdc \
  --downstream-host <DOWNSTREAM_PUBLIC_IP> \
  --downstream-port 30401

# With custom upstream connection
python3 tidb_benchmark.py \
  --aws-profile sandbox \
  --ticdc \
  --upstream-host <UPSTREAM_PUBLIC_IP> \
  --upstream-port 30400 \
  --downstream-host <DOWNSTREAM_PUBLIC_IP> \
  --downstream-port 30401
```

The TiCDC benchmark output includes a replication lag section:

```
======================================================================
TICDC REPLICATION LAG
======================================================================
  Samples:      300
  Min Lag:      45.2 ms
  Avg Lag:      127.8 ms
  P50 Lag:      112.3 ms
  P99 Lag:      289.1 ms
  Max Lag:      412.5 ms
  Lag Stddev:   67.4 ms
======================================================================
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
| scaling   | 16     | 100,000    | 16->96->32| 3min   | Scale up/down test |

### Multi-Phase Profiles

**Stress Profile** -- Pushes TiDB until breaking point (50% QPS decay or 10x latency):
- Warmup -> Ramp-up -> Adaptive overload (keeps increasing until breaking) -> Recovery -> Sustained

**Scaling Profile** -- Tests traffic scale-up and scale-down behavior:
- Baseline (16 threads) -> Scale-up (32->64->96) -> Peak -> Scale-down (64->32)

## Available Workloads

- `oltp_read_write` (default) -- Mixed read/write OLTP (70% reads / 30% writes)
- `oltp_read_only` -- Read-only queries (auto-enables `--skip_trx`)
- `oltp_write_only` -- Write-only operations
- `oltp_point_select` -- Point selects (auto-enables `--skip_trx`)
- `oltp_insert` -- Insert operations
- `oltp_delete` -- Delete operations
- `oltp_update_index` -- Index updates
- `oltp_update_non_index` -- Non-index updates

## Benchmark Output

The benchmark provides comprehensive output including:

- **Workload Summary**: Description of operations, dataset size, concurrency
- **Per-Minute Resource Snapshots**: CPU/memory usage with cost accrual
- **Availability Tracking**: Success rate accounting for ignored transient errors
- **P99 Latency**: 99th percentile latency metrics
- **Cost Analysis**: Client vs server breakdown, price-performance metrics ($/QPS, $/TPS)
- **Monthly Projections**: Compute, storage, network cost estimates
- **TiCDC Replication Lag** (when `--ticdc`): Min/avg/p50/p99/max lag with standard deviation

Sample output:
```
======================================================================
WORKLOAD SUMMARY
======================================================================
Workload: OLTP Read-Write (Mixed)
Profile:  standard
Dataset:  16 tables x 100,000 rows = 1,600,000 total (~183 MB)
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

## Session Variables

The benchmark automatically sets these TiDB session variables for optimal performance:
- `tikv_client_read_timeout=5` -- 5 second read timeout
- `max_execution_time=10000` -- 10 second max execution time

## Client-Side Tuning

The setup script automatically applies:
- **File descriptors**: 1,000,000 (soft/hard)
- **Network tuning**: TCP buffer sizes, connection backlog, port range
- **Kernel parameters**: perf profiling enabled for flamegraphs

## Manual Test Snippets

```bash
# SSH to the client host
ssh -i tidb-load-test-key.pem ec2-user@<CLIENT_PUBLIC_IP>

# Connect to upstream TiDB
mysql -h 127.0.0.1 -P 30400 -u root

# Connect to downstream TiDB (TiCDC mode)
mysql -h 127.0.0.1 -P 30401 -u root

# Check cluster status
kubectl get tc -n tidb-cluster
kubectl get pods -n tidb-cluster

# Check TiCDC status
kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=ticdc

# Check changefeed status
kubectl exec -n tidb-cluster $(kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=ticdc -o jsonpath='{.items[0].metadata.name}') -- \
  /cdc cli changefeed list --server=http://127.0.0.1:8301

# Check downstream cluster
kubectl get tc -n tidb-downstream
kubectl get pods -n tidb-downstream

# Manual sysbench run
sysbench oltp_read_write \
  --mysql-host=127.0.0.1 \
  --mysql-port=30400 \
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
- [TiCDC Architecture (New)](https://docs.pingcap.com/tidb/stable/ticdc-architecture/)
- [TiCDC Deployment on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable/deploy-ticdc/)
- [How Flipkart Scales Over 1M QPS with TiDB](https://www.pingcap.com/blog/how-flipkart-scales-over-1m-qps-with-zero-downtime-maintenance/)
