#!/usr/bin/env python3
"""
TiDB Load Test Stack Provisioner

Provisions AWS infrastructure and bootstraps a TiDB cluster using TiDB Operator
on a multi-node k3s cluster across dedicated EC2 instances.

Architecture:
- 1 client EC2 (k3s server / control plane + sysbench)
- 3 PD EC2 instances (k3s agents, labeled for PD pods)
- 3 TiKV EC2 instances (k3s agents, labeled for TiKV pods)
- 2 TiDB EC2 instances (k3s agents, labeled for TiDB pods)
- All nodes in a single VPC/subnet with self-referencing SG for intra-cluster traffic
"""

import argparse
import base64
import boto3
import botocore
from botocore.config import Config
import ipaddress
import json
import os
import shutil
import subprocess
import textwrap
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

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

# Default instance type for client VM (sysbench runner)
# c7g.4xlarge provides 16 vCPU, 32GB RAM - enough headroom for sysbench client
CLIENT_INSTANCE_TYPE = "c7g.4xlarge"

# PingCAP recommended production instance types (AWS)
# https://docs.pingcap.com/tidb/stable/hardware-and-software-requirements
PRODUCTION_INSTANCE_TYPES = {
    "pd": "c7g.xlarge",      # 4 vCPU, 8GB - metadata/scheduling
    "tidb": "c7g.4xlarge",   # 16 vCPU, 32GB - compute optimized for SQL
    "tikv": "m7g.4xlarge",   # 16 vCPU, 64GB - memory optimized for storage
    "client": "c7g.4xlarge", # 16 vCPU, 32GB - benchmark client
}

# Cost-optimized instance types for testing/benchmarking
BENCHMARK_INSTANCE_TYPES = {
    "pd": "c7g.large",       # 2 vCPU, 4GB
    "tidb": "c7g.2xlarge",   # 8 vCPU, 16GB
    "tikv": "m7g.2xlarge",   # 8 vCPU, 32GB
    "client": "c7g.2xlarge", # 8 vCPU, 16GB
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

# Root volume sizes per role (GB)
ROOT_VOLUME_SIZES = {
    "client": 100,
    "pd": 50,
    "tikv": 500,
    "tidb": 50,
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

# k3s ports for inter-node communication
K3S_API_PORT = 6443
K3S_FLANNEL_PORT = 8472  # UDP VXLAN
KUBELET_PORT = 10250

TIDB_OPERATOR_VERSION = os.environ.get("TIDB_OPERATOR_VERSION", "v1.6.5")
TIDB_VERSION = os.environ.get("TIDB_VERSION", "v8.5.5")
HELM_VERSION = os.environ.get("HELM_VERSION", "v3.16.3")
KUBECTL_VERSION = os.environ.get("KUBECTL_VERSION", "v1.31.0")

# All roles that get their own EC2 instances
COMPONENT_ROLES = ["pd", "tikv", "tidb"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Provision TiDB load test stack on multi-node k3s cluster."
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


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class InstanceInfo:
    role: str
    instance_id: str
    public_ip: str
    private_ip: str
    availability_zone: str = ""


@dataclass
class BootstrapContext:
    ssh_key_path: Path
    self_cidr: str
    client: InstanceInfo
    pd_nodes: List[InstanceInfo] = field(default_factory=list)
    tikv_nodes: List[InstanceInfo] = field(default_factory=list)
    tidb_nodes: List[InstanceInfo] = field(default_factory=list)
    tidb_version: str = TIDB_VERSION
    pd_replicas: int = 3
    tikv_replicas: int = 3
    tidb_replicas: int = 2
    multi_az: bool = False
    production: bool = False
    client_zone: str = "us-east-1a"

    @property
    def host(self) -> InstanceInfo:
        """Backward compatibility: client is the main host for SSH operations."""
        return self.client

    @property
    def all_agent_nodes(self) -> List[InstanceInfo]:
        """All k3s agent nodes (everything except client/server)."""
        return self.pd_nodes + self.tikv_nodes + self.tidb_nodes

    @property
    def all_nodes(self) -> List[InstanceInfo]:
        """All nodes including client."""
        return [self.client] + self.all_agent_nodes


# ---------------------------------------------------------------------------
# EC2 / Instance helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# VPC / Network
# ---------------------------------------------------------------------------

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


def ensure_subnet(vpc_id, name, cidr, az=None, public=False):
    """Create or reuse a subnet. If az is None, defaults to {REGION}a."""
    sn, tags = find_subnet(vpc_id, name, cidr)
    if sn:
        log(f"REUSED  subnet: {sn}")
        return sn
    availability_zone = az or f"{REGION}a"
    resp = ec2().create_subnet(
        VpcId=vpc_id,
        CidrBlock=cidr,
        AvailabilityZone=availability_zone,
        TagSpecifications=[{
            "ResourceType": "subnet",
            "Tags": tags_common() + [{"Key": "Name", "Value": name}]
        }],
    )
    subnet_id = resp["Subnet"]["SubnetId"]
    if public:
        ec2().modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    log(f"CREATED subnet: {subnet_id} (AZ: {availability_zone})")
    return subnet_id


def find_rtb_by_name(name):
    resp = ec2().describe_route_tables(
        Filters=[{"Name": "tag:Name", "Values": [name]}, {"Name": "tag:Project", "Values": [STACK]}]
    )
    rtbs = resp.get("RouteTables", [])
    return rtbs[0]["RouteTableId"] if rtbs else None


def ensure_public_rtb(vpc_id, igw_id, subnet_ids):
    """Create/reuse a public route table and associate it with all given subnets."""
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
    if isinstance(subnet_ids, str):
        subnet_ids = [subnet_ids]
    for subnet_id in subnet_ids:
        try:
            ec2().associate_route_table(RouteTableId=rtb, SubnetId=subnet_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "Resource.AlreadyAssociated":
                raise
    return rtb


# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------

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


def ensure_intra_cluster_rules(sg_id):
    """Allow all traffic between instances in the same security group.

    This is needed for k3s API (6443), kubelet (10250), flannel VXLAN (8472/UDP),
    TiDB/TiKV/PD inter-node communication, and CoreDNS.
    """
    try:
        ec2().authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "-1",  # All traffic
                "UserIdGroupPairs": [{"GroupId": sg_id}]
            }]
        )
        log(f"CREATED intra-cluster SG rule (self-referencing): {sg_id}")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            log(f"REUSED  intra-cluster SG rule: {sg_id}")
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


# ---------------------------------------------------------------------------
# EC2 Instances
# ---------------------------------------------------------------------------

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


def find_all_stack_instances():
    """Find all running/pending instances in this stack."""
    resp = ec2().describe_instances(
        Filters=[{"Name": "tag:Project", "Values": [STACK]},
                 {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}]
    )
    instances = []
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
            instances.append({
                "id": inst["InstanceId"],
                "name": tags.get("Name", ""),
                "role": tags.get("Role", "unknown"),
                "state": inst["State"]["Name"],
            })
    return instances


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


def instance_info_from_id(iid, role) -> InstanceInfo:
    """Build InstanceInfo from an instance ID after it's running."""
    inst = describe_instance(iid)
    return InstanceInfo(
        role=role,
        instance_id=iid,
        public_ip=inst.get("PublicIpAddress", ""),
        private_ip=inst.get("PrivateIpAddress", ""),
        availability_zone=inst.get("Placement", {}).get("AvailabilityZone", ""),
    )


def provision_role_instances(role, count, instance_types, subnet_id, sg_id, production):
    """Provision `count` EC2 instances for a given role. Returns list of instance IDs."""
    types = PRODUCTION_INSTANCE_TYPES if production else BENCHMARK_INSTANCE_TYPES
    itype = types.get(role, instance_types.get(role, CLIENT_INSTANCE_TYPE))
    vol_size = ROOT_VOLUME_SIZES.get(role, 100)
    ids = []
    for i in range(1, count + 1):
        name = f"{STACK}-{role}-{i}"
        iid = ensure_instance(name, role, itype, subnet_id, sg_id, vol_size)
        ids.append(iid)
    return ids


# ---------------------------------------------------------------------------
# Software Installation (shared across nodes)
# ---------------------------------------------------------------------------

def install_base_packages(host: InstanceInfo, ctx: BootstrapContext):
    """Install base OS packages and tune sysctl on any node."""
    log(f"Installing base packages on {host.role} ({host.public_ip})")
    ssh_run(host, """
sudo dnf -y update || true
sudo dnf -y install jq htop sysstat mtr || true

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


# ---------------------------------------------------------------------------
# k3s Installation (replaces kind + Docker)
# ---------------------------------------------------------------------------

def install_k3s_server(client: InstanceInfo, ctx: BootstrapContext):
    """Install k3s in server (control-plane) mode on the client node.

    Disables traefik, servicelb, and local-storage (not needed for TiDB).
    Taints the control-plane node so TiDB pods don't schedule on it.
    """
    log(f"Installing k3s server on client ({client.public_ip})")
    ssh_run(client, f"""
# Check if k3s is already running
if sudo systemctl is-active k3s >/dev/null 2>&1; then
    echo "k3s server already running"
    sudo k3s kubectl get nodes || true
    exit 0
fi

curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --disable=traefik,servicelb,local-storage \
  --tls-san={client.private_ip} \
  --tls-san={client.public_ip} \
  --node-taint=node-role.kubernetes.io/control-plane:NoSchedule \
  --write-kubeconfig-mode=644" sh -

# Wait for k3s to be ready
for i in $(seq 1 30); do
    if sudo k3s kubectl get nodes >/dev/null 2>&1; then
        echo "k3s server ready"
        break
    fi
    echo "Waiting for k3s server... (attempt $i/30)"
    sleep 5
done

# Set up kubeconfig for ec2-user
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
chmod 600 ~/.kube/config

# Verify
kubectl get nodes
echo "k3s server installed successfully"
""", ctx)


def get_k3s_token(client: InstanceInfo, ctx: BootstrapContext) -> str:
    """Retrieve the k3s node-token from the server node."""
    result = ssh_capture(client, """
sudo cat /var/lib/rancher/k3s/server/node-token
""", ctx)
    return result.stdout.strip()


def install_k3s_agent(agent: InstanceInfo, server_ip: str, token: str, ctx: BootstrapContext):
    """Install k3s agent on a worker node and join it to the cluster."""
    log(f"Joining k3s agent {agent.role} ({agent.public_ip}) to server {server_ip}")
    ssh_run(agent, f"""
# Check if k3s-agent is already running
if sudo systemctl is-active k3s-agent >/dev/null 2>&1; then
    echo "k3s agent already running on {agent.role}"
    exit 0
fi

curl -sfL https://get.k3s.io | K3S_URL="https://{server_ip}:6443" \
  K3S_TOKEN="{token}" sh -

# Wait for agent to register
for i in $(seq 1 20); do
    if sudo systemctl is-active k3s-agent >/dev/null 2>&1; then
        echo "k3s agent started on {agent.role}"
        break
    fi
    echo "Waiting for k3s agent... (attempt $i/20)"
    sleep 5
done
""", ctx)


def label_and_taint_nodes(ctx: BootstrapContext):
    """Label and taint all agent nodes from the k3s server.

    This ensures TiDB Operator schedules each component to the correct node.
    """
    log("Labeling and tainting k3s agent nodes for TiDB scheduling")

    # Wait for all agents to register
    total_agents = len(ctx.all_agent_nodes)
    ssh_run(ctx.client, f"""
echo "Waiting for {total_agents} agent nodes to register..."
for i in $(seq 1 60); do
    COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l)
    # Total = agents + 1 server
    if [ "$COUNT" -ge "{total_agents + 1}" ]; then
        echo "All {total_agents + 1} nodes registered"
        break
    fi
    echo "Nodes registered: $COUNT/{total_agents + 1} (attempt $i/60)"
    sleep 10
done
kubectl get nodes -o wide
""", ctx)

    # Label and taint each node by matching on private IP (k3s uses the IP as node name)
    for node in ctx.pd_nodes:
        _label_taint_node(ctx, node.private_ip, "pd")
    for node in ctx.tikv_nodes:
        _label_taint_node(ctx, node.private_ip, "tikv")
    for node in ctx.tidb_nodes:
        _label_taint_node(ctx, node.private_ip, "tidb")


def _label_taint_node(ctx: BootstrapContext, node_ip: str, component: str):
    """Label and taint a single k3s node by its IP address."""
    ssh_run(ctx.client, f"""
# Find the node name by IP
NODE_NAME=$(kubectl get nodes -o jsonpath='{{.items[?(@.status.addresses[?(@.type=="InternalIP")].address=="{node_ip}")].metadata.name}}')
if [ -z "$NODE_NAME" ]; then
    echo "WARNING: Could not find node with IP {node_ip}, trying hostname-based lookup..."
    NODE_NAME=$(kubectl get nodes -o wide --no-headers | grep "{node_ip}" | awk '{{print $1}}')
fi
if [ -z "$NODE_NAME" ]; then
    echo "ERROR: Could not find node with IP {node_ip}"
    exit 1
fi

echo "Labeling node $NODE_NAME as {component} (IP: {node_ip})"
kubectl label node "$NODE_NAME" tidb.pingcap.com/{component}="" --overwrite
kubectl taint node "$NODE_NAME" tidb.pingcap.com/{component}:NoSchedule --overwrite 2>/dev/null || \
    kubectl taint node "$NODE_NAME" tidb.pingcap.com/{component}=:NoSchedule 2>/dev/null || true

# Also add a zone label based on the node's AZ for topology-aware scheduling
kubectl label node "$NODE_NAME" topology.kubernetes.io/zone=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone 2>/dev/null || echo "unknown") --overwrite 2>/dev/null || true
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Helm + TiDB Operator
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# TiDB Cluster Deployment (with nodeSelector + tolerations for multi-node)
# ---------------------------------------------------------------------------

def deploy_tidb_cluster(host: InstanceInfo, ctx: BootstrapContext):
    """Deploy TiDB cluster with nodeSelector/tolerations for dedicated hosts."""
    log(f"Deploying TiDB cluster {ctx.tidb_version} with multi-node scheduling")

    pd_config_block = ""
    tikv_extra_config = ""

    if ctx.multi_az:
        pd_config_block = (
            "      replication:\n"
            "        location-labels:\n"
            "          - zone\n"
            "          - host\n"
            "        max-replicas: 3\n"
            "      enable-placement-rules: true"
        )
        tikv_extra_config = (
            "      server:\n"
            "        labels:\n"
            "          zone: \"auto\"\n"
            "          host: \"auto\""
        )

    # Build the YAML as a plain Python string (no f-string for the YAML body)
    # to avoid brace-escaping issues with YAML/JSON syntax
    yaml_lines = []
    yaml_lines.append('apiVersion: pingcap.com/v1alpha1')
    yaml_lines.append('kind: TidbCluster')
    yaml_lines.append('metadata:')
    yaml_lines.append('  name: basic')
    yaml_lines.append('  namespace: tidb-cluster')
    yaml_lines.append('spec:')
    yaml_lines.append(f'  version: {ctx.tidb_version}')
    yaml_lines.append('  timezone: UTC')
    yaml_lines.append('  pvReclaimPolicy: Retain')
    yaml_lines.append('  enableDynamicConfiguration: true')
    yaml_lines.append('  configUpdateStrategy: RollingUpdate')
    yaml_lines.append('  discovery: {}')
    yaml_lines.append('  helper:')
    yaml_lines.append('    image: alpine:3.16.0')
    # PD
    yaml_lines.append('  pd:')
    yaml_lines.append('    baseImage: pingcap/pd')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append(f'    replicas: {ctx.pd_replicas}')
    yaml_lines.append('    requests:')
    yaml_lines.append('      storage: "10Gi"')
    if pd_config_block:
        yaml_lines.append('    config: |')
        yaml_lines.append(pd_config_block)
    else:
        yaml_lines.append('    config: {}')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/pd: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/pd')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('    affinity:')
    yaml_lines.append('      podAntiAffinity:')
    yaml_lines.append('        requiredDuringSchedulingIgnoredDuringExecution:')
    yaml_lines.append('          - labelSelector:')
    yaml_lines.append('              matchExpressions:')
    yaml_lines.append('                - key: app.kubernetes.io/component')
    yaml_lines.append('                  operator: In')
    yaml_lines.append('                  values:')
    yaml_lines.append('                    - pd')
    yaml_lines.append('            topologyKey: kubernetes.io/hostname')
    # TiKV
    yaml_lines.append('  tikv:')
    yaml_lines.append('    baseImage: pingcap/tikv')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append('    evictLeaderTimeout: 1m')
    yaml_lines.append(f'    replicas: {ctx.tikv_replicas}')
    yaml_lines.append('    requests:')
    yaml_lines.append('      storage: "50Gi"')
    yaml_lines.append('    config: |')
    yaml_lines.append('      storage:')
    yaml_lines.append('        reserve-space: "0MB"')
    yaml_lines.append('      rocksdb:')
    yaml_lines.append('        max-open-files: 256')
    yaml_lines.append('      raftdb:')
    yaml_lines.append('        max-open-files: 256')
    if tikv_extra_config:
        yaml_lines.append(tikv_extra_config)
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/tikv: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/tikv')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('    affinity:')
    yaml_lines.append('      podAntiAffinity:')
    yaml_lines.append('        requiredDuringSchedulingIgnoredDuringExecution:')
    yaml_lines.append('          - labelSelector:')
    yaml_lines.append('              matchExpressions:')
    yaml_lines.append('                - key: app.kubernetes.io/component')
    yaml_lines.append('                  operator: In')
    yaml_lines.append('                  values:')
    yaml_lines.append('                    - tikv')
    yaml_lines.append('            topologyKey: kubernetes.io/hostname')
    # TiDB
    yaml_lines.append('  tidb:')
    yaml_lines.append('    baseImage: pingcap/tidb')
    yaml_lines.append('    maxFailoverCount: 0')
    yaml_lines.append(f'    replicas: {ctx.tidb_replicas}')
    yaml_lines.append('    service:')
    yaml_lines.append('      type: NodePort')
    yaml_lines.append('    config: {}')
    yaml_lines.append('    nodeSelector:')
    yaml_lines.append('      tidb.pingcap.com/tidb: ""')
    yaml_lines.append('    tolerations:')
    yaml_lines.append('      - key: tidb.pingcap.com/tidb')
    yaml_lines.append('        operator: Exists')
    yaml_lines.append('        effect: NoSchedule')
    yaml_lines.append('    affinity:')
    yaml_lines.append('      podAntiAffinity:')
    yaml_lines.append('        preferredDuringSchedulingIgnoredDuringExecution:')
    yaml_lines.append('          - weight: 100')
    yaml_lines.append('            podAffinityTerm:')
    yaml_lines.append('              labelSelector:')
    yaml_lines.append('                matchExpressions:')
    yaml_lines.append('                  - key: app.kubernetes.io/component')
    yaml_lines.append('                    operator: In')
    yaml_lines.append('                    values:')
    yaml_lines.append('                      - tidb')
    yaml_lines.append('              topologyKey: kubernetes.io/hostname')

    yaml_str = '\n'.join(yaml_lines)

    # Write YAML via base64 to avoid shell quoting issues
    yaml_b64 = base64.b64encode(yaml_str.encode()).decode()

    ssh_run(host, f"""
kubectl create namespace tidb-cluster || true

echo '{yaml_b64}' | base64 -d > /tmp/tidb-cluster.yaml
kubectl apply -f /tmp/tidb-cluster.yaml -n tidb-cluster

echo "Waiting for TiDB service to be created..."
for i in $(seq 1 30); do
    if kubectl get svc basic-tidb -n tidb-cluster >/dev/null 2>&1; then
        kubectl patch svc basic-tidb -n tidb-cluster --type='json' \\
            -p='[{{"op": "replace", "path": "/spec/ports/0/nodePort", "value": 30400}}]' || true
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
    READY=$(kubectl get pods -n tidb-cluster -l app.kubernetes.io/component=tidb \
        -o jsonpath='{.items[0].status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
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
kubectl patch svc basic-tidb -n tidb-cluster --type='json' \
    -p='[{"op": "replace", "path": "/spec/ports/0/nodePort", "value": 30400}]' 2>/dev/null || true
echo "TiDB NodePort set to 30400"
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Leader zone affinity
# ---------------------------------------------------------------------------

def configure_leader_zone_affinity(host: InstanceInfo, ctx: BootstrapContext):
    """Configure placement rules to prefer leaders in the client zone."""
    if not ctx.multi_az:
        log("Skipping leader zone affinity (single-AZ deployment)")
        return

    log(f"Configuring leader zone affinity for zone: {ctx.client_zone}")
    zone_short = ctx.client_zone.split("-")[-1]

    # Find TiDB node IP to connect through NodePort
    tidb_ip = ctx.tidb_nodes[0].private_ip if ctx.tidb_nodes else "127.0.0.1"

    ssh_run(host, f"""
# Get the TiDB NodePort service cluster IP or use node IP
TIDB_HOST=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.clusterIP}}' 2>/dev/null || echo "")
TIDB_PORT=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.ports[0].port}}' 2>/dev/null || echo "4000")

if [ -z "$TIDB_HOST" ]; then
    TIDB_HOST="{tidb_ip}"
    TIDB_PORT="30400"
fi

for i in $(seq 1 30); do
    if mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SELECT 1" 2>/dev/null; then
        break
    fi
    echo "Waiting for TiDB connection... (attempt $i/30)"
    sleep 5
done

mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "
-- Create placement policy preferring leaders in client zone
CREATE PLACEMENT POLICY IF NOT EXISTS local_leader
  LEADER_CONSTRAINTS='[+zone={zone_short}]'
  FOLLOWER_CONSTRAINTS='{{\\"+zone={zone_short}\\": 1}}';

-- Show placement policies
SHOW PLACEMENT;
" 2>/dev/null || echo "Note: Placement rules require TiDB 5.2+ and enabled placement-rules"
""", ctx, strict=False)


# ---------------------------------------------------------------------------
# Client Tools (only on client node)
# ---------------------------------------------------------------------------

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
    """Create the sysbench database via the TiDB service in k8s."""
    log("Creating sysbench database...")

    # Get the TiDB NodePort IP (use first TiDB node IP with NodePort 30400)
    tidb_ip = ctx.tidb_nodes[0].private_ip if ctx.tidb_nodes else "127.0.0.1"

    ssh_run(host, f"""
# Determine TiDB connection endpoint
TIDB_HOST=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.clusterIP}}' 2>/dev/null || echo "")
TIDB_PORT=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.ports[0].port}}' 2>/dev/null || echo "4000")

if [ -z "$TIDB_HOST" ]; then
    TIDB_HOST="{tidb_ip}"
    TIDB_PORT="30400"
fi

for i in $(seq 1 30); do
    if mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SELECT 1" 2>/dev/null; then
        break
    fi
    echo "Waiting for TiDB connection... (attempt $i/30)"
    sleep 5
done

mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS sbtest;"
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SHOW DATABASES;"

# Disable resource control to avoid error 8249 (Unknown resource group 'default')
# Resource control is not needed for benchmarking and causes sysbench FATAL errors
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u root -e "SET GLOBAL tidb_enable_resource_control = OFF;" 2>/dev/null || true
echo "Resource control disabled"
""", ctx)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def terminate_stack_instances():
    """Terminate all EC2 instances belonging to this stack."""
    instances = find_all_stack_instances()
    if not instances:
        log("No stack instances found to terminate.")
        return

    ids_to_terminate = [inst["id"] for inst in instances if inst["state"] not in ("terminated", "shutting-down")]
    if not ids_to_terminate:
        log("All instances already terminated.")
        return

    for inst in instances:
        if inst["id"] in ids_to_terminate:
            log(f"TERMINATING instance {inst['role']} ({inst['name']}): {inst['id']}")

    try:
        ec2().terminate_instances(InstanceIds=ids_to_terminate)
        waiter = ec2().get_waiter("instance_terminated")
        waiter.wait(InstanceIds=ids_to_terminate)
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


# ---------------------------------------------------------------------------
# Wait for SSH on multiple nodes
# ---------------------------------------------------------------------------

def wait_for_ssh(node: InstanceInfo, ctx: BootstrapContext, max_attempts=30):
    """Wait for SSH to become available on a node."""
    for attempt in range(max_attempts):
        result = ssh_capture(node, "echo ready", ctx, strict=False)
        if result.returncode == 0:
            return True
        if attempt < max_attempts - 1:
            log(f"  {node.role} ({node.public_ip}) not ready yet (attempt {attempt + 1}/{max_attempts})...")
            time.sleep(10)
    log(f"  WARNING: {node.role} ({node.public_ip}) did not become reachable after {max_attempts} attempts")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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

    multi_az = args.multi_az and not args.single_az
    production = args.production and not args.benchmark_mode

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

    types = PRODUCTION_INSTANCE_TYPES if production else BENCHMARK_INSTANCE_TYPES
    log(f"Instance types: PD={types['pd']}, TiKV={types['tikv']}, TiDB={types['tidb']}, Client={types['client']}")

    # -----------------------------------------------------------------------
    # Network
    # -----------------------------------------------------------------------
    log("=== Provisioning Network ===")
    vpc_id = ensure_vpc()
    igw_id = ensure_igw(vpc_id)
    # Single subnet (all nodes in same subnet for simplicity & minimal latency)
    public_subnet = ensure_subnet(vpc_id, f"{STACK}-public", PUB_CIDR, az=args.client_zone, public=True)
    ensure_public_rtb(vpc_id, igw_id, public_subnet)

    # -----------------------------------------------------------------------
    # Security Group
    # -----------------------------------------------------------------------
    log("=== Provisioning Security Group ===")
    sg = ensure_sg(vpc_id, f"{STACK}-cluster", "TiDB load test cluster (multi-node)")
    refresh_ssh_rule(sg, self_cidr)
    ensure_ingress_tcp_cidr(sg, TIDB_PORT, self_cidr)
    ensure_intra_cluster_rules(sg)  # All traffic within the cluster

    # -----------------------------------------------------------------------
    # EC2 Instances
    # -----------------------------------------------------------------------
    log("=== Provisioning EC2 Instances ===")
    client_type = types["client"]
    client_vol = ROOT_VOLUME_SIZES["client"]
    client_id = ensure_instance(f"{STACK}-client", "client", client_type, public_subnet, sg, client_vol)

    pd_ids = provision_role_instances("pd", pd_replicas, types, public_subnet, sg, production)
    tikv_ids = provision_role_instances("tikv", tikv_replicas, types, public_subnet, sg, production)
    tidb_ids = provision_role_instances("tidb", tidb_replicas, types, public_subnet, sg, production)

    # -----------------------------------------------------------------------
    # Gather Instance Information
    # -----------------------------------------------------------------------
    log("=== Gathering Instance Information ===")
    time.sleep(10)  # Allow public IPs to propagate

    client = instance_info_from_id(client_id, "client")
    pd_nodes = [instance_info_from_id(iid, f"pd-{i+1}") for i, iid in enumerate(pd_ids)]
    tikv_nodes = [instance_info_from_id(iid, f"tikv-{i+1}") for i, iid in enumerate(tikv_ids)]
    tidb_nodes = [instance_info_from_id(iid, f"tidb-{i+1}") for i, iid in enumerate(tidb_ids)]

    log(f"  client:  public={client.public_ip} private={client.private_ip}")
    for n in pd_nodes:
        log(f"  {n.role}: public={n.public_ip} private={n.private_ip}")
    for n in tikv_nodes:
        log(f"  {n.role}: public={n.public_ip} private={n.private_ip}")
    for n in tidb_nodes:
        log(f"  {n.role}: public={n.public_ip} private={n.private_ip}")

    total_instances = 1 + len(pd_nodes) + len(tikv_nodes) + len(tidb_nodes)
    log(f"  Total instances: {total_instances}")

    if args.skip_bootstrap:
        log("Skipping bootstrap (--skip-bootstrap). Infrastructure ready.")
        log(f"SSH to client: ssh -i {key_path} ec2-user@{client.public_ip}")
        return

    # -----------------------------------------------------------------------
    # Build context
    # -----------------------------------------------------------------------
    ctx = BootstrapContext(
        ssh_key_path=key_path,
        self_cidr=self_cidr,
        client=client,
        pd_nodes=pd_nodes,
        tikv_nodes=tikv_nodes,
        tidb_nodes=tidb_nodes,
        tidb_version=args.tidb_version,
        pd_replicas=pd_replicas,
        tikv_replicas=tikv_replicas,
        tidb_replicas=tidb_replicas,
        multi_az=multi_az,
        production=production,
        client_zone=args.client_zone,
    )

    # -----------------------------------------------------------------------
    # Wait for SSH on all nodes
    # -----------------------------------------------------------------------
    log("=== Waiting for SSH on all nodes ===")
    for node in ctx.all_nodes:
        wait_for_ssh(node, ctx)

    # -----------------------------------------------------------------------
    # Install base packages on all nodes
    # -----------------------------------------------------------------------
    log("=== Installing Base Packages ===")
    for node in ctx.all_nodes:
        install_base_packages(node, ctx)

    # -----------------------------------------------------------------------
    # k3s Cluster Setup
    # -----------------------------------------------------------------------
    log("=== Setting up k3s Cluster ===")
    install_k3s_server(client, ctx)
    k3s_token = get_k3s_token(client, ctx)
    log(f"k3s token retrieved (length={len(k3s_token)})")

    log("=== Joining k3s Agent Nodes ===")
    for node in ctx.all_agent_nodes:
        install_k3s_agent(node, client.private_ip, k3s_token, ctx)

    log("=== Labeling and Tainting Nodes ===")
    label_and_taint_nodes(ctx)

    # -----------------------------------------------------------------------
    # TiDB Operator + Cluster
    # -----------------------------------------------------------------------
    log("=== Installing Helm ===")
    install_helm(client, ctx)

    log("=== Installing TiDB Operator ===")
    install_tidb_operator(client, ctx)

    log("=== Deploying TiDB Cluster ===")
    deploy_tidb_cluster(client, ctx)
    wait_for_tidb_ready(client, ctx)

    # -----------------------------------------------------------------------
    # Client Tools
    # -----------------------------------------------------------------------
    log("=== Installing Client Tools ===")
    install_mysql_client(client, ctx)
    install_sysbench(client, ctx)
    create_sysbench_database(client, ctx)

    if ctx.multi_az:
        log("=== Configuring Leader Zone Affinity ===")
        configure_leader_zone_affinity(client, ctx)

    # -----------------------------------------------------------------------
    # Done
    # -----------------------------------------------------------------------
    log("=== Deployment Complete ===")
    log("")
    log(f"SSH to client: ssh -i {key_path} ec2-user@{client.public_ip}")
    log("")

    # Determine TiDB endpoint for the user
    tidb_node_ip = tidb_nodes[0].public_ip if tidb_nodes else client.public_ip
    log(f"TiDB accessible via NodePort: {tidb_node_ip}:30400")
    log("")
    log("From the client host, connect to TiDB:")
    log(f"  TIDB_HOST=$(kubectl get svc basic-tidb -n tidb-cluster -o jsonpath='{{.spec.clusterIP}}')")
    log(f"  mysql -h $TIDB_HOST -P 4000 -u root")
    log("")
    log("To check cluster status:")
    log("  kubectl get nodes -o wide")
    log("  kubectl get pods -n tidb-cluster -o wide")
    log("  kubectl get tc -n tidb-cluster")


if __name__ == "__main__":
    main()
