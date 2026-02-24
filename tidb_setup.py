#!/usr/bin/env python3
"""
TiDB Load Test Stack Provisioner

Provisions AWS infrastructure and bootstraps a TiDB cluster using TiDB Operator on kind.
Based on the valkey-bench pattern for consistency.

Architecture:
- 1 EC2 instance (bastion/k8s host) in public subnet
  - Runs kind (Kubernetes in Docker)
  - TiDB Operator manages the TiDB cluster
  - sysbench runs benchmarks
"""

import argparse
import boto3
import botocore
from botocore.config import Config
import ipaddress
import os
import shutil
import subprocess
import textwrap
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

REGION = "us-east-1"
AWS_PROFILE = os.environ.get("AWS_PROFILE", "sandbox")
SEED = "tidblt-001"
STACK = f"tidb-loadtest-{SEED}"
OWNER = os.environ.get("OWNER", "")
CLEANUP_LOG_PATH = Path("cleanup.log")
LOG_TO_FILE = None
BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "adaptive"},
    connect_timeout=15,
    read_timeout=60,
)

# Default instance type for single-node deployment
INSTANCE_TYPE = "c7g.2xlarge"

# PingCAP recommended production instance types (AWS)
# https://docs.pingcap.com/tidb/stable/hardware-and-software-requirements
PRODUCTION_INSTANCE_TYPES = {
    "pd": "c7g.xlarge",      # 4 vCPU, 8GB - metadata/scheduling
    "tidb": "c7g.4xlarge",   # 16 vCPU, 32GB - compute optimized for SQL
    "tikv": "m7g.4xlarge",   # 16 vCPU, 64GB - memory optimized for storage
}

# Cost-optimized instance types for testing/benchmarking
BENCHMARK_INSTANCE_TYPES = {
    "pd": "c7g.large",       # 2 vCPU, 4GB
    "tidb": "c7g.2xlarge",   # 8 vCPU, 16GB
    "tikv": "m7g.2xlarge",   # 8 vCPU, 32GB
}

# EBS gp3 storage recommendations from PingCAP
# https://docs.pingcap.com/tidb-in-kubernetes/stable/configure-storage-class
EBS_CONFIG = {
    "tikv": {
        "type": "gp3",
        "iops": 4000,
        "throughput": 400,  # MiB/s
        "size_gb": 500,
    },
    "pd": {
        "type": "gp3",
        "iops": 3000,
        "throughput": 125,  # MiB/s
        "size_gb": 50,
    },
}

# Multi-AZ configuration
AVAILABILITY_ZONES = ["us-east-1a", "us-east-1b", "us-east-1c"]

AMI_OVERRIDE = os.environ.get("TIDB_AMI_ID")
AMI_SSM_PARAM = os.environ.get(
    "TIDB_AMI_PARAM",
    "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
)
_RESOLVED_AMI_ID = None

KEY_NAME = "tidb-load-test-key"
DEFAULT_SSH_KEY_PATH = Path(__file__).resolve().with_name("tidb-load-test-key.pem")

VPC_CIDR = "10.43.0.0/16"
PUB_CIDR = "10.43.1.0/24"

TIDB_PORT = 4000
SSH_PORT = 22

TIDB_OPERATOR_VERSION = os.environ.get("TIDB_OPERATOR_VERSION", "v1.6.5")
TIDB_VERSION = os.environ.get("TIDB_VERSION", "v8.5.5")
KIND_VERSION = os.environ.get("KIND_VERSION", "v0.24.0")
HELM_VERSION = os.environ.get("HELM_VERSION", "v3.16.3")
KUBECTL_VERSION = os.environ.get("KUBECTL_VERSION", "v1.31.0")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Provision TiDB load test stack using TiDB Operator on kind."
    )
    parser.add_argument("--region", default=REGION, help="AWS region (default: us-east-1)")
    parser.add_argument("--seed", default=SEED, help="Unique seed used in stack name.")
    parser.add_argument("--owner", default=os.environ.get("OWNER", ""), help="Owner tag value.")
    parser.add_argument(
        "--ssh-private-key-path",
        dest="ssh_key_path",
        default=str(DEFAULT_SSH_KEY_PATH),
        help=f"Path to SSH private key (.pem) (default: {DEFAULT_SSH_KEY_PATH}).",
    )
    parser.add_argument("--ssh-cidr", help="CIDR allowed for SSH (default: detected public IP /32).")
    parser.add_argument("--aws-profile", help="AWS named profile (default: sandbox or $AWS_PROFILE).")
    parser.add_argument("--skip-bootstrap", action="store_true", help="Provision infrastructure only.")
    parser.add_argument("--cleanup", action="store_true", help="Tear down stack resources.")
    parser.add_argument(
        "--instance-type",
        default=INSTANCE_TYPE,
        help=f"EC2 instance type (default: {INSTANCE_TYPE}).",
    )
    parser.add_argument(
        "--tidb-version",
        default=TIDB_VERSION,
        help=f"TiDB version to deploy (default: {TIDB_VERSION}).",
    )
    parser.add_argument(
        "--pd-replicas",
        type=int,
        default=3,
        help="Number of PD replicas (default: 3 for multi-AZ).",
    )
    parser.add_argument(
        "--tikv-replicas",
        type=int,
        default=3,
        help="Number of TiKV replicas (default: 3 for multi-AZ).",
    )
    parser.add_argument(
        "--tidb-replicas",
        type=int,
        default=2,
        help="Number of TiDB replicas (default: 2 for HA).",
    )
    parser.add_argument(
        "--multi-az",
        action="store_true",
        default=True,
        help="Deploy across 3 AZs for HA (1 node per AZ minimum). Default: enabled.",
    )
    parser.add_argument(
        "--single-az",
        action="store_true",
        help="Disable multi-AZ deployment (single zone, sets replicas to 1).",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        default=True,
        help="Use PingCAP recommended production instance types. Default: enabled.",
    )
    parser.add_argument(
        "--benchmark-mode",
        action="store_true",
        help="Use cost-optimized instance types for benchmarking.",
    )
    parser.add_argument(
        "--client-zone",
        default="us-east-1a",
        help="AZ for client VM; leaders prefer this zone (default: us-east-1a).",
    )
    return parser


def configure_runtime(*, region=None, seed=None, owner=None, aws_profile=None):
    global REGION, SEED, STACK, AWS_PROFILE, OWNER
    if region:
        REGION = region
    if seed:
        SEED = seed
    if owner is not None:
        OWNER = owner
    if aws_profile:
        AWS_PROFILE = aws_profile
    STACK = f"tidb-loadtest-{SEED}"


def configure_from_args(args):
    configure_runtime(
        region=args.region,
        seed=args.seed,
        owner=args.owner if args.owner != "" else OWNER,
        aws_profile=args.aws_profile,
    )


def ts():
    return datetime.now().strftime("[%Y-%m-%dT%H:%M:%S%z]")


def log(msg):
    global LOG_TO_FILE
    line = f"{ts()} {msg}"
    print(line, flush=True)
    if LOG_TO_FILE:
        try:
            LOG_TO_FILE.parent.mkdir(parents=True, exist_ok=True)
            with LOG_TO_FILE.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass


def need_cmd(cmd):
    if shutil.which(cmd):
        return
    raise SystemExit(f"ERROR: Required command '{cmd}' not found in PATH.")


def my_public_cidr():
    ip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
    ipaddress.ip_address(ip)
    return f"{ip}/32"


def aws_session():
    try:
        return boto3.session.Session(profile_name=AWS_PROFILE, region_name=REGION)
    except botocore.exceptions.ProfileNotFound:
        raise SystemExit(f"ERROR: AWS profile '{AWS_PROFILE}' not found.")


def ec2():
    return aws_session().client("ec2", region_name=REGION, config=BOTO_CONFIG)


def ssm():
    return aws_session().client("ssm", region_name=REGION, config=BOTO_CONFIG)


def resolved_ami_id():
    global _RESOLVED_AMI_ID
    if AMI_OVERRIDE:
        return AMI_OVERRIDE
    if _RESOLVED_AMI_ID:
        return _RESOLVED_AMI_ID
    try:
        param = ssm().get_parameter(Name=AMI_SSM_PARAM)
        value = param["Parameter"]["Value"]
        _RESOLVED_AMI_ID = value
        log(f"Using Amazon Linux 2023 AMI via {AMI_SSM_PARAM}: {value}")
        return value
    except botocore.exceptions.ClientError as exc:
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        raise SystemExit(
            f"ERROR: Unable to resolve AMI from SSM ({AMI_SSM_PARAM}): {msg}. Set TIDB_AMI_ID to override."
        )


def ensure_keypair_accessible():
    try:
        ec2().describe_key_pairs(KeyNames=[KEY_NAME])
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidKeyPair.NotFound":
            raise SystemExit(
                f"ERROR: KeyPair '{KEY_NAME}' not found in EC2. "
                "Create it with: aws ec2 import-key-pair --key-name tidb-load-test-key "
                "--public-key-material fileb://tidb-load-test-key.pub --profile sandbox"
            )
        raise


@dataclass
class InstanceInfo:
    role: str
    instance_id: str
    public_ip: str
    private_ip: str


@dataclass
class BootstrapContext:
    ssh_key_path: Path
    self_cidr: str
    host: InstanceInfo
    tidb_version: str
    pd_replicas: int
    tikv_replicas: int
    tidb_replicas: int
    multi_az: bool = False
    production: bool = False
    client_zone: str = "us-east-1a"


def describe_instance(iid):
    resp = ec2().describe_instances(InstanceIds=[iid])
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst
    raise RuntimeError(f"Instance {iid} not found.")


def ssh_base_cmd(host: InstanceInfo, ctx: BootstrapContext):
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=30",
        "-i", str(ctx.ssh_key_path),
        f"ec2-user@{host.public_ip}",
        "bash", "-s"
    ]
    return cmd


def ssh_run(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    subprocess.run(cmd, input=full_script, text=True, check=True)


def ssh_capture(host: InstanceInfo, script: str, ctx: BootstrapContext, strict=True):
    full_script = textwrap.dedent(script).lstrip()
    if strict:
        full_script = "set -euo pipefail\n" + full_script
    cmd = ssh_base_cmd(host, ctx)
    result = subprocess.run(cmd, input=full_script, text=True, capture_output=True)
    if strict:
        result.check_returncode()
    return result


def tags_common():
    tags = [{"Key": "Project", "Value": STACK}]
    if OWNER:
        tags.append({"Key": "Owner", "Value": OWNER})
    return tags


def get_vpcs():
    resp = ec2().describe_vpcs(
        Filters=[{"Name": "tag:Project", "Values": [STACK]}]
    )
    return resp.get("Vpcs", [])


def ensure_vpc():
    v = get_vpcs()
    if v:
        vpc_id = v[0]["VpcId"]
        log(f"REUSED  vpc: {vpc_id}")
        return vpc_id
    resp = ec2().create_vpc(CidrBlock=VPC_CIDR, TagSpecifications=[{
        "ResourceType": "vpc",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{STACK}-vpc"}]
    }])
    vpc_id = resp["Vpc"]["VpcId"]
    log(f"CREATED vpc: {vpc_id}")
    try:
        ec2().modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        ec2().modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    except botocore.exceptions.ClientError:
        pass
    return vpc_id


def get_igw_for_vpc(vpc_id):
    resp = ec2().describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )
    igws = resp.get("InternetGateways", [])
    return igws[0]["InternetGatewayId"] if igws else None


def ensure_igw(vpc_id):
    igw = get_igw_for_vpc(vpc_id)
    if igw:
        log(f"REUSED  igw (attached): {igw}")
        return igw
    resp = ec2().create_internet_gateway(TagSpecifications=[{
        "ResourceType": "internet-gateway",
        "Tags": tags_common() + [{"Key": "Name", "Value": f"{STACK}-igw"}]
    }])
    igw_id = resp["InternetGateway"]["InternetGatewayId"]
    ec2().attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    log(f"CREATED igw: {igw_id}")
    return igw_id


def find_subnet(vpc_id, name, cidr):
    resp = ec2().describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )
    subnets = resp.get("Subnets", [])
    for sn in subnets:
        tags = {t["Key"]: t["Value"] for t in sn.get("Tags", [])}
        if tags.get("Project") == STACK and tags.get("Name") == name:
            return sn["SubnetId"], tags
    for sn in subnets:
        if sn.get("CidrBlock") == cidr:
            return sn["SubnetId"], {t["Key"]: t["Value"] for t in sn.get("Tags", [])}
    return None, None


def ensure_subnet(vpc_id, name, cidr, public=False):
    sn, tags = find_subnet(vpc_id, name, cidr)
    if sn:
        log(f"REUSED  subnet: {sn}")
        return sn
    resp = ec2().create_subnet(
        VpcId=vpc_id,
        CidrBlock=cidr,
        AvailabilityZone=f"{REGION}a",
        TagSpecifications=[{
            "ResourceType": "subnet",
            "Tags": tags_common() + [{"Key": "Name", "Value": name}]
        }],
    )
    subnet_id = resp["Subnet"]["SubnetId"]
    if public:
        ec2().modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    log(f"CREATED subnet: {subnet_id}")
    return subnet_id


def find_rtb_by_name(name):
    resp = ec2().describe_route_tables(
        Filters=[{"Name": "tag:Name", "Values": [name]}, {"Name": "tag:Project", "Values": [STACK]}]
    )
    rtbs = resp.get("RouteTables", [])
    return rtbs[0]["RouteTableId"] if rtbs else None


def ensure_public_rtb(vpc_id, igw_id, public_subnet_id):
    name = f"{STACK}-rtb-public"
    rtb = find_rtb_by_name(name)
    if not rtb:
        resp = ec2().create_route_table(
            VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "route-table",
                "Tags": tags_common() + [{"Key": "Name", "Value": name}]
            }]
        )
        rtb = resp["RouteTable"]["RouteTableId"]
        ec2().create_route(RouteTableId=rtb, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
        log(f"CREATED rtb public: {rtb}")
    else:
        log(f"REUSED  rtb public: {rtb}")
    try:
        ec2().associate_route_table(RouteTableId=rtb, SubnetId=public_subnet_id)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "Resource.AlreadyAssociated":
            raise
    return rtb


def ensure_sg(vpc_id, name, description):
    resp = ec2().describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [name]},
                 {"Name": "vpc-id", "Values": [vpc_id]}]
    )
    sgs = resp.get("SecurityGroups", [])
    if sgs:
        sg_id = sgs[0]["GroupId"]
        log(f"REUSED  sg {name}: {sg_id}")
    else:
        description = description[:200]
        resp = ec2().create_security_group(
            GroupName=name, Description=description, VpcId=vpc_id,
            TagSpecifications=[{
                "ResourceType": "security-group",
                "Tags": tags_common() + [{"Key": "Name", "Value": name}]
            }]
        )
        sg_id = resp["GroupId"]
        log(f"CREATED sg {name}: {sg_id}")
    return sg_id


def ensure_ingress_tcp_cidr(sg, port, cidr):
    try:
        ec2().authorize_security_group_ingress(
            GroupId=sg, IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": port, "ToPort": port,
                "IpRanges": [{"CidrIp": cidr}]
            }]
        )
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            return False
        raise


def refresh_ssh_rule(sg_id, cidr):
    if not cidr:
        return
    resp = ec2().describe_security_groups(GroupIds=[sg_id])
    perms = resp["SecurityGroups"][0].get("IpPermissions", [])
    to_revoke = []
    need_authorize = True
    for p in perms:
        if p.get("IpProtocol") == "tcp" and p.get("FromPort") == SSH_PORT and p.get("ToPort") == SSH_PORT:
            for r in p.get("IpRanges", []):
                ip = r.get("CidrIp")
                if ip == cidr:
                    need_authorize = False
                else:
                    to_revoke.append(ip)
    if to_revoke:
        try:
            ec2().revoke_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[{
                    "IpProtocol": "tcp",
                    "FromPort": SSH_PORT,
                    "ToPort": SSH_PORT,
                    "IpRanges": [{"CidrIp": ip} for ip in to_revoke]
                }]
            )
        except botocore.exceptions.ClientError:
            pass
    if need_authorize:
        ensure_ingress_tcp_cidr(sg_id, SSH_PORT, cidr)


def find_instance_id_by_name(name):
    resp = ec2().describe_instances(
        Filters=[{"Name": "tag:Name", "Values": [name]},
                 {"Name": "tag:Project", "Values": [STACK]},
                 {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}]
    )
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            return inst["InstanceId"]
    return None


def wait_running(iid):
    ec2().get_waiter("instance_running").wait(InstanceIds=[iid])


def ensure_instance(name, role, itype, subnet_id, sg_id, root_volume_size=100):
    iid = find_instance_id_by_name(name)
    if iid:
        log(f"REUSED  instance {role}: {iid}")
        return iid
    ensure_keypair_accessible()
    ni = [{
        "DeviceIndex": 0,
        "SubnetId": subnet_id,
        "AssociatePublicIpAddress": True,
        "Groups": [sg_id]
    }]
    block_devices = [{
        "DeviceName": "/dev/xvda",
        "Ebs": {
            "VolumeSize": root_volume_size,
            "VolumeType": "gp3",
            "DeleteOnTermination": True
        }
    }]
    resp = ec2().run_instances(
        ImageId=resolved_ami_id(),
        InstanceType=itype,
        KeyName=KEY_NAME,
        NetworkInterfaces=ni,
        BlockDeviceMappings=block_devices,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": tags_common() + [
                {"Key": "Name", "Value": name},
                {"Key": "Role", "Value": role}
            ]
        }],
        MinCount=1, MaxCount=1
    )
    iid = resp["Instances"][0]["InstanceId"]
    log(f"CREATED instance {role}: {iid}")
    wait_running(iid)
    return iid


def install_docker(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing Docker on {host.role}")
    ssh_run(host, """
sudo dnf -y update || true
sudo dnf -y install docker git jq htop sysstat mtr || true

sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user

sudo tee /etc/sysctl.d/99-k8s.conf >/dev/null <<'EOF'
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward = 1
fs.inotify.max_user_watches = 524288
fs.inotify.max_user_instances = 512
EOF

sudo tee /etc/sysctl.d/99-tidb-bench.conf >/dev/null <<'EOF'
# File descriptor limits
fs.file-max = 1000000
fs.nr_open = 1000000

# Network tuning for high-throughput benchmarks
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 1024 65535

# Perf profiling (for flamegraphs)
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
EOF

sudo tee /etc/security/limits.d/99-tidb-bench.conf >/dev/null <<'EOF'
* soft nofile 1000000
* hard nofile 1000000
* soft nproc 65535
* hard nproc 65535
root soft nofile 1000000
root hard nofile 1000000
EOF

sudo sysctl --system || true
""", ctx)


def install_kubectl(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing kubectl on {host.role}")
    ssh_run(host, f"""
if command -v kubectl >/dev/null 2>&1; then
    echo "kubectl already installed"
    exit 0
fi

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    ARCH="arm64"
elif [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
fi

curl -LO "https://dl.k8s.io/release/{KUBECTL_VERSION}/bin/linux/$ARCH/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/
kubectl version --client || true
""", ctx)


def install_helm(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing Helm on {host.role}")
    ssh_run(host, f"""
if command -v helm >/dev/null 2>&1; then
    echo "Helm already installed"
    exit 0
fi

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    ARCH="arm64"
elif [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
fi

curl -fsSL https://get.helm.sh/helm-{HELM_VERSION}-linux-$ARCH.tar.gz | tar xz
sudo mv linux-$ARCH/helm /usr/local/bin/
rm -rf linux-$ARCH
helm version || true
""", ctx)


def install_kind(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing kind on {host.role}")
    ssh_run(host, f"""
if command -v kind >/dev/null 2>&1; then
    echo "kind already installed"
    exit 0
fi

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
    ARCH="arm64"
elif [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
fi

curl -Lo ./kind https://kind.sigs.k8s.io/dl/{KIND_VERSION}/kind-linux-$ARCH
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind
kind version || true
""", ctx)


def create_kind_cluster(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Creating kind cluster on {host.role}")
    ssh_run(host, """
if kind get clusters 2>/dev/null | grep -q tidb-bench; then
    echo "kind cluster 'tidb-bench' already exists"
    exit 0
fi

cat <<'EOF' > /tmp/kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 30400
    hostPort: 4000
    protocol: TCP
  - containerPort: 30080
    hostPort: 10080
    protocol: TCP
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
EOF

kind create cluster --name tidb-bench --config /tmp/kind-config.yaml --wait 5m
kubectl cluster-info --context kind-tidb-bench
""", ctx)


def install_tidb_operator(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing TiDB Operator {TIDB_OPERATOR_VERSION} on {host.role}")
    ssh_run(host, f"""
kubectl create -f https://raw.githubusercontent.com/pingcap/tidb-operator/{TIDB_OPERATOR_VERSION}/manifests/crd.yaml || true

helm repo add pingcap https://charts.pingcap.com/ || true
helm repo update

kubectl create namespace tidb-admin || true

if helm status tidb-operator -n tidb-admin >/dev/null 2>&1; then
    echo "TiDB Operator already installed"
else
    helm install --namespace tidb-admin tidb-operator pingcap/tidb-operator --version {TIDB_OPERATOR_VERSION} --wait
fi

kubectl get pods --namespace tidb-admin -l app.kubernetes.io/instance=tidb-operator
""", ctx)


def deploy_tidb_cluster(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Deploying TiDB cluster {ctx.tidb_version} on {host.role}")
    
    pd_config = "{}"
    tikv_config = """
      storage:
        reserve-space: "0MB"
      rocksdb:
        max-open-files: 256
      raftdb:
        max-open-files: 256"""
    
    if ctx.multi_az:
        pd_config = """
      replication:
        location-labels:
          - zone
          - rack
          - host
        max-replicas: 3
      enable-placement-rules: true"""
        tikv_config = """
      storage:
        reserve-space: "0MB"
      rocksdb:
        max-open-files: 256
      raftdb:
        max-open-files: 256
      server:
        labels:
          zone: "zone1"
          rack: "rack1"
          host: "host1" """
    
    ssh_run(host, f"""
kubectl create namespace tidb-cluster || true

cat <<'EOF' > /tmp/tidb-cluster.yaml
apiVersion: pingcap.com/v1alpha1
kind: TidbCluster
metadata:
  name: basic
  namespace: tidb-cluster
spec:
  version: {ctx.tidb_version}
  timezone: UTC
  pvReclaimPolicy: Retain
  enableDynamicConfiguration: true
  configUpdateStrategy: RollingUpdate
  discovery: {{}}
  helper:
    image: alpine:3.16.0
  pd:
    baseImage: pingcap/pd
    maxFailoverCount: 0
    replicas: {ctx.pd_replicas}
    requests:
      storage: "10Gi"
    config: {pd_config}
  tikv:
    baseImage: pingcap/tikv
    maxFailoverCount: 0
    evictLeaderTimeout: 1m
    replicas: {ctx.tikv_replicas}
    requests:
      storage: "50Gi"
    config: {tikv_config}
  tidb:
    baseImage: pingcap/tidb
    maxFailoverCount: 0
    replicas: {ctx.tidb_replicas}
    service:
      type: NodePort
    config: {{}}
EOF

kubectl apply -f /tmp/tidb-cluster.yaml -n tidb-cluster

echo "Waiting for TiDB service to be created..."
for i in $(seq 1 30); do
    if kubectl get svc basic-tidb -n tidb-cluster >/dev/null 2>&1; then
        kubectl patch svc basic-tidb -n tidb-cluster --type='json' -p='[{{"op": "replace", "path": "/spec/ports/0/nodePort", "value": 30400}}]' || true
        break
    fi
    sleep 2
done
""", ctx)


def wait_for_tidb_ready(host: InstanceInfo, ctx: BootstrapContext):
    log("Waiting for TiDB cluster to be ready...")
    ssh_run(host, """
echo "Waiting for TiDB pods to be ready (this may take 5-10 minutes)..."

for i in $(seq 1 60); do
    READY=$(kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=tidb -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
    if [ "$READY" = "true" ]; then
        echo "TiDB is ready!"
        break
    fi
    echo "Waiting for TiDB... (attempt $i/60)"
    kubectl get pods -n tidb-cluster 2>/dev/null || true
    sleep 10
done

kubectl get pods -n tidb-cluster
kubectl get svc -n tidb-cluster

# Ensure NodePort is set correctly (operator may have reverted it)
kubectl patch svc basic-tidb -n tidb-cluster --type='json' -p='[{"op": "replace", "path": "/spec/ports/0/nodePort", "value": 30400}]' 2>/dev/null || true
echo "TiDB NodePort set to 30400 (mapped to host port 4000)"
""", ctx, strict=False)


def configure_leader_zone_affinity(host: InstanceInfo, ctx: BootstrapContext):
    """Configure placement rules to prefer leaders in the client zone."""
    if not ctx.multi_az:
        log("Skipping leader zone affinity (single-AZ deployment)")
        return
    
    log(f"Configuring leader zone affinity for zone: {ctx.client_zone}")
    zone_short = ctx.client_zone.split("-")[-1]
    
    ssh_run(host, f"""
for i in $(seq 1 30); do
    if mysql -h 127.0.0.1 -P {TIDB_PORT} -u root -e "SELECT 1" 2>/dev/null; then
        break
    fi
    echo "Waiting for TiDB connection... (attempt $i/30)"
    sleep 5
done

mysql -h 127.0.0.1 -P {TIDB_PORT} -u root -e "
-- Create placement policy preferring leaders in client zone
CREATE PLACEMENT POLICY IF NOT EXISTS local_leader 
  LEADER_CONSTRAINTS='[+zone={zone_short}]'
  FOLLOWER_CONSTRAINTS='{{\\"+zone={zone_short}\\": 1}}';

-- Show placement policies
SHOW PLACEMENT;
" 2>/dev/null || echo "Note: Placement rules require TiDB 5.2+ and enabled placement-rules"
""", ctx, strict=False)


def install_mysql_client(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing MySQL client on {host.role}")
    ssh_run(host, """
if command -v mysql >/dev/null 2>&1; then
    echo "MySQL client already installed"
    exit 0
fi
sudo dnf -y install mariadb105 || sudo dnf -y install mysql || true
""", ctx)


def install_sysbench(host: InstanceInfo, ctx: BootstrapContext):
    log(f"Installing sysbench on {host.role}")
    ssh_run(host, """
if command -v sysbench >/dev/null 2>&1; then
    echo "sysbench already installed"
    exit 0
fi

# Try package manager first
if sudo dnf -y install sysbench 2>/dev/null; then
    exit 0
fi

# Build from source - need to handle Amazon Linux 2023 which uses MariaDB
sudo dnf -y install mariadb-connector-c-devel libaio-devel automake libtool make gcc gcc-c++ || true

# Create symlinks for MySQL compatibility (AL2023 uses mariadb paths)
if [ -d /usr/include/mariadb ] && [ ! -f /usr/include/mysql.h ]; then
    sudo ln -sf /usr/include/mariadb/mysql.h /usr/include/mysql.h
    sudo ln -sf /usr/include/mariadb/mysqld_error.h /usr/include/mysqld_error.h 2>/dev/null || true
    sudo ln -sf /usr/include/mariadb/errmsg.h /usr/include/errmsg.h 2>/dev/null || true
fi

cd /tmp
rm -rf sysbench
git clone https://github.com/akopytov/sysbench.git
cd sysbench
./autogen.sh

# Configure with explicit MariaDB paths for Amazon Linux 2023
if [ -d /usr/include/mariadb ]; then
    export CPPFLAGS="-I/usr/include/mariadb"
    ./configure --with-mysql-includes=/usr/include/mariadb --with-mysql-libs=/usr/lib64/mariadb
else
    ./configure --with-mysql
fi

make -j $(nproc)
sudo make install
cd /tmp && rm -rf sysbench

# Verify installation
sysbench --version
""", ctx)


def create_sysbench_database(host: InstanceInfo, ctx: BootstrapContext):
    log("Creating sysbench database...")
    ssh_run(host, f"""
for i in $(seq 1 30); do
    if mysql -h 127.0.0.1 -P {TIDB_PORT} -u root -e "SELECT 1" 2>/dev/null; then
        break
    fi
    echo "Waiting for TiDB connection... (attempt $i/30)"
    sleep 5
done

mysql -h 127.0.0.1 -P {TIDB_PORT} -u root -e "CREATE DATABASE IF NOT EXISTS sbtest;"
mysql -h 127.0.0.1 -P {TIDB_PORT} -u root -e "SHOW DATABASES;"
""", ctx)


def terminate_stack_instances():
    name = f"{STACK}-host"
    iid = find_instance_id_by_name(name)
    if iid:
        log(f"TERMINATING instance: {iid}")
        try:
            ec2().terminate_instances(InstanceIds=[iid])
            waiter = ec2().get_waiter("instance_terminated")
            waiter.wait(InstanceIds=[iid])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidInstanceID.NotFound":
                raise


def delete_stack_security_groups():
    resp = ec2().describe_security_groups(
        Filters=[{"Name": "tag:Project", "Values": [STACK]}]
    )
    groups = [sg for sg in resp.get("SecurityGroups", []) if sg.get("GroupName") != "default"]
    for sg in groups:
        sg_id = sg["GroupId"]
        name = sg.get("GroupName", sg_id)
        desc = ec2().describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
        ingress = desc.get("IpPermissions", [])
        if ingress:
            try:
                ec2().revoke_security_group_ingress(GroupId=sg_id, IpPermissions=ingress)
            except botocore.exceptions.ClientError:
                pass
        try:
            log(f"DELETING security group {name}: {sg_id}")
            ec2().delete_security_group(GroupId=sg_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("InvalidGroup.NotFound", "DependencyViolation"):
                raise


def delete_route_table_by_name(name):
    rtb = find_rtb_by_name(name)
    if not rtb:
        return
    log(f"DELETING route table {name}: {rtb}")
    desc = ec2().describe_route_tables(RouteTableIds=[rtb])["RouteTables"][0]
    for assoc in desc.get("Associations", []):
        if not assoc.get("Main"):
            try:
                ec2().disassociate_route_table(AssociationId=assoc["RouteTableAssociationId"])
            except botocore.exceptions.ClientError:
                pass
    try:
        ec2().delete_route_table(RouteTableId=rtb)
    except botocore.exceptions.ClientError:
        pass


def delete_stack_subnets(vpc_id):
    resp = ec2().describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "tag:Project", "Values": [STACK]}]
    )
    for subnet in resp.get("Subnets", []):
        subnet_id = subnet["SubnetId"]
        log(f"DELETING subnet: {subnet_id}")
        try:
            ec2().delete_subnet(SubnetId=subnet_id)
        except botocore.exceptions.ClientError:
            pass


def delete_stack_igw_and_vpc(vpc_id):
    igw = get_igw_for_vpc(vpc_id)
    if igw:
        log(f"DETACHING internet gateway: {igw}")
        try:
            ec2().detach_internet_gateway(InternetGatewayId=igw, VpcId=vpc_id)
        except botocore.exceptions.ClientError:
            pass
        log(f"DELETING internet gateway: {igw}")
        try:
            ec2().delete_internet_gateway(InternetGatewayId=igw)
        except botocore.exceptions.ClientError:
            pass
    log(f"DELETING VPC: {vpc_id}")
    for _ in range(12):
        try:
            ec2().delete_vpc(VpcId=vpc_id)
            return
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "InvalidVpcID.NotFound":
                return
            if e.response["Error"]["Code"] == "DependencyViolation":
                time.sleep(5)
                continue
            raise


def cleanup_stack():
    log(f"Cleanup requested for stack: {STACK} in {REGION}")
    vpcs = get_vpcs()
    if not vpcs:
        log("No tagged VPC found; nothing to clean up.")
        return
    terminate_stack_instances()
    delete_stack_security_groups()
    delete_route_table_by_name(f"{STACK}-rtb-public")
    for vpc in vpcs:
        delete_stack_subnets(vpc["VpcId"])
        delete_stack_igw_and_vpc(vpc["VpcId"])
    log("Cleanup complete.")


def main():
    parser = parse_args()
    args = parser.parse_args()
    configure_from_args(args)

    if args.cleanup:
        cleanup_stack()
        return

    key_path = Path(args.ssh_key_path).expanduser().resolve()
    if not key_path.exists():
        raise SystemExit(f"ERROR: SSH key not found: {key_path}")
    need_cmd("ssh")

    self_cidr = args.ssh_cidr or my_public_cidr()
    log(f"Using SSH CIDR: {self_cidr}")
    log(f"Using TiDB version: {args.tidb_version}")
    log(f"Using instance type: {args.instance_type}")
    if args.multi_az:
        log(f"Multi-AZ deployment enabled (client zone: {args.client_zone})")
    if args.production:
        log("Using PingCAP production instance type recommendations")

    log("=== Provisioning Network ===")
    vpc_id = ensure_vpc()
    igw_id = ensure_igw(vpc_id)
    public_subnet = ensure_subnet(vpc_id, f"{STACK}-public", PUB_CIDR, public=True)
    ensure_public_rtb(vpc_id, igw_id, public_subnet)

    log("=== Provisioning Security Group ===")
    sg_host = ensure_sg(vpc_id, f"{STACK}-host", "TiDB load test host")
    refresh_ssh_rule(sg_host, self_cidr)
    ensure_ingress_tcp_cidr(sg_host, TIDB_PORT, self_cidr)

    log("=== Provisioning Instance ===")
    host_id = ensure_instance(f"{STACK}-host", "host", args.instance_type, public_subnet, sg_host)

    log("=== Gathering Instance Information ===")
    time.sleep(10)
    inst = describe_instance(host_id)
    host = InstanceInfo(
        role="host",
        instance_id=host_id,
        public_ip=inst.get("PublicIpAddress"),
        private_ip=inst.get("PrivateIpAddress"),
    )
    log(f"  host: public={host.public_ip} private={host.private_ip}")

    if args.skip_bootstrap:
        log("Skipping bootstrap (--skip-bootstrap). Infrastructure ready.")
        log(f"SSH: ssh -i {key_path} ec2-user@{host.public_ip}")
        return

    multi_az = args.multi_az and not args.single_az
    pd_replicas = args.pd_replicas
    tikv_replicas = args.tikv_replicas
    tidb_replicas = args.tidb_replicas
    
    if args.single_az:
        pd_replicas = 1
        tikv_replicas = 1
        tidb_replicas = 1
        log("Single-AZ mode: using 1 replica per component")
    else:
        log(f"Multi-AZ mode: PD={pd_replicas}, TiKV={tikv_replicas}, TiDB={tidb_replicas}")

    ctx = BootstrapContext(
        ssh_key_path=key_path,
        self_cidr=self_cidr,
        host=host,
        tidb_version=args.tidb_version,
        pd_replicas=pd_replicas,
        tikv_replicas=tikv_replicas,
        tidb_replicas=tidb_replicas,
        multi_az=multi_az,
        production=args.production and not args.benchmark_mode,
        client_zone=args.client_zone,
    )

    log("=== Waiting for instance to be ready ===")
    for attempt in range(30):
        result = ssh_capture(host, "echo ready", ctx, strict=False)
        if result.returncode == 0:
            break
        log(f"Instance not ready yet (attempt {attempt + 1}/30)...")
        time.sleep(10)

    log("=== Installing Prerequisites ===")
    install_docker(host, ctx)
    install_kubectl(host, ctx)
    install_helm(host, ctx)
    install_kind(host, ctx)

    log("=== Creating kind Cluster ===")
    create_kind_cluster(host, ctx)

    log("=== Installing TiDB Operator ===")
    install_tidb_operator(host, ctx)

    log("=== Deploying TiDB Cluster ===")
    deploy_tidb_cluster(host, ctx)
    wait_for_tidb_ready(host, ctx)

    log("=== Installing Client Tools ===")
    install_mysql_client(host, ctx)
    install_sysbench(host, ctx)
    create_sysbench_database(host, ctx)

    if ctx.multi_az:
        log("=== Configuring Leader Zone Affinity ===")
        configure_leader_zone_affinity(host, ctx)

    log("=== Deployment Complete ===")
    log(f"")
    log(f"SSH to host: ssh -i {key_path} ec2-user@{host.public_ip}")
    log(f"")
    log(f"TiDB is accessible at: {host.public_ip}:{TIDB_PORT}")
    log(f"")
    log("From the host, connect to TiDB:")
    log(f"  mysql -h 127.0.0.1 -P {TIDB_PORT} -u root")
    log("")
    log("To run sysbench prepare:")
    log(f"  sysbench oltp_read_write --mysql-host=127.0.0.1 --mysql-port={TIDB_PORT} --mysql-user=root --mysql-db=sbtest --tables=16 --table-size=100000 prepare")
    log("")
    log("To run sysbench benchmark:")
    log(f"  sysbench oltp_read_write --mysql-host=127.0.0.1 --mysql-port={TIDB_PORT} --mysql-user=root --mysql-db=sbtest --tables=16 --table-size=100000 --threads=64 --time=120 run")
    log("")
    log("To check cluster status:")
    log("  kubectl get pods -n tidb-cluster")
    log("  kubectl get tc -n tidb-cluster")


if __name__ == "__main__":
    main()
