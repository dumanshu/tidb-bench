# TiDB Benchmark Toolkit

AWS infrastructure provisioning and sysbench benchmarking for TiDB clusters using TiDB Operator on kind (Kubernetes in Docker).

## Prerequisites

- Python 3.8+
- AWS CLI configured with appropriate credentials
- SSH key pair for EC2 access
- boto3 (`pip install boto3`)

## Quick Start

```bash
# 1. Generate SSH key (if not already done)
ssh-keygen -t rsa -b 4096 -f tidb-load-test-key.pem -N ""

# 2. Import your SSH key to AWS
aws ec2 import-key-pair --key-name tidb-load-test-key \
    --public-key-material fileb://tidb-load-test-key.pem.pub \
    --profile sandbox

# 3. Provision infrastructure and deploy TiDB
python3 tidb_setup.py --aws-profile sandbox

# 4. Validate deployment
python3 tidb_validate.py --aws-profile sandbox

# 5. Run benchmarks
python3 tidb_benchmark.py --aws-profile sandbox

# 6. Cleanup when done
python3 tidb_setup.py --cleanup --aws-profile sandbox
```

## Scripts

### tidb_setup.py

Provisions AWS infrastructure and bootstraps a TiDB cluster:

- Creates VPC, subnet, security group, and EC2 instance
- Installs Docker, kubectl, Helm, kind
- Deploys TiDB Operator and TiDB cluster (3 PD, 3 TiKV, 2 TiDB by default)
- Installs MySQL client and sysbench
- Applies client-side tuning (file descriptors, sysctl, network)

```bash
# Full deployment with multi-AZ (default)
python3 tidb_setup.py --aws-profile sandbox

# With custom configuration
python3 tidb_setup.py \
    --aws-profile sandbox \
    --tidb-version v8.5.5 \
    --pd-replicas 3 \
    --tikv-replicas 3 \
    --tidb-replicas 2 \
    --instance-type c7g.2xlarge

# Cleanup all resources
python3 tidb_setup.py --cleanup --aws-profile sandbox
```

### tidb_validate.py

Validates deployment and runs health checks:

```bash
# Basic validation
python3 tidb_validate.py --aws-profile sandbox

# Verbose output with pod details
python3 tidb_validate.py --aws-profile sandbox -v
```

### tidb_benchmark.py

Runs sysbench benchmarks against TiDB with workload profiles:

```bash
# Default benchmark (heavy profile: 32 tables, 500k rows, 128 threads, 5min)
python3 tidb_benchmark.py --aws-profile sandbox

# Quick test
python3 tidb_benchmark.py --aws-profile sandbox --profile quick

# Stress test (multi-phase: warmup → ramp-up → overload → recovery → sustained)
python3 tidb_benchmark.py --aws-profile sandbox --profile stress

# Scaling test (multi-phase: baseline → scale-up → peak → scale-down)
python3 tidb_benchmark.py --aws-profile sandbox --profile scaling

# Custom workload
python3 tidb_benchmark.py \
    --aws-profile sandbox \
    --workload oltp_point_select \
    --tables 32 \
    --table-size 200000 \
    --threads 128 \
    --duration 300

# Prepare tables only
python3 tidb_benchmark.py --aws-profile sandbox --prepare-only

# Skip preparation (use existing tables)
python3 tidb_benchmark.py --aws-profile sandbox --skip-prepare
```

## Workload Profiles

| Profile | Tables | Rows/Table | Total Rows | Est. Size | Threads | Duration | Description |
|---------|--------|------------|------------|-----------|---------|----------|-------------|
| quick | 4 | 10,000 | 40,000 | ~5 MB | 16 | 30s | Quick validation |
| light | 8 | 50,000 | 400,000 | ~46 MB | 32 | 60s | Light load |
| medium | 16 | 100,000 | 1,600,000 | ~183 MB | 64 | 2min | Medium load |
| **heavy** | 32 | 500,000 | 16,000,000 | ~1.8 GB | 128 | 5min | Heavy load (default) |
| stress | 16 | 100,000 | 1,600,000 | ~183 MB | varies | 3min | Multi-phase stress test |
| scaling | 16 | 100,000 | 1,600,000 | ~183 MB | varies | 3min | Multi-phase scaling test |

### Multi-Phase Profiles

**Stress Profile** - Tests TiDB's behavior under CPU overload and recovery:
- Warmup (32 threads, 30s)
- Ramp-up (64 threads, 30s)
- Overload (128 threads, 45s) - pushes CPU to high utilization
- Recovery (64 threads, 30s)
- Sustained (48 threads, 45s) - stable ~60% utilization

**Scaling Profile** - Tests traffic scale-up and scale-down behavior:
- Baseline (16 threads, 30s)
- Scale-up phases (32 → 64 → 96 threads)
- Peak (96 threads, 30s)
- Scale-down phases (64 → 32 threads)

## Available Workloads

- `oltp_read_write` (default) - Mixed read/write OLTP
- `oltp_read_only` - Read-only queries
- `oltp_write_only` - Write-only operations
- `oltp_point_select` - Point selects (high concurrency)
- `oltp_insert` - Insert operations
- `oltp_delete` - Delete operations
- `oltp_update_index` - Index updates
- `oltp_update_non_index` - Non-index updates

## Architecture

```
EC2 Instance (c7g.2xlarge, ARM Graviton)
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

## Client-Side Tuning

The setup script automatically applies:

- **File descriptors**: 1,000,000 (soft/hard)
- **Network tuning**: TCP buffer sizes, connection backlog, port range
- **Kernel parameters**: perf profiling enabled for flamegraphs

## Connecting to TiDB

```bash
# SSH to the host
ssh -i tidb-load-test-key.pem ec2-user@<PUBLIC_IP>

# Connect to TiDB
mysql -h 127.0.0.1 -P 4000 -u root

# Check cluster status
kubectl get tc -n tidb-cluster
kubectl get pods -n tidb-cluster
```

## Environment Variables

- `AWS_PROFILE` - AWS profile name (default: sandbox)
- `TIDB_VERSION` - TiDB version (default: v8.5.5)
- `TIDB_OPERATOR_VERSION` - Operator version (default: v1.6.5)
- `OWNER` - Owner tag value for resources

## References

- [PingCAP TiDB Documentation](https://docs.pingcap.com/tidb/stable)
- [TiDB Operator on Kubernetes](https://docs.pingcap.com/tidb-in-kubernetes/stable)
- [How Flipkart Scales Over 1M QPS with TiDB](https://www.pingcap.com/blog/how-flipkart-scales-over-1m-qps-with-zero-downtime-maintenance/)
