"""Deploy ChargePulse to a single EC2 instance (AWS Academy sandbox friendly).

  python deploy/ec2_deploy.py                    # create/reuse infra, upload code, compose up
  python deploy/ec2_deploy.py --instance-type t3.xlarge   # where the sandbox allows it
  python deploy/ec2_deploy.py --code-only        # redeploy code to the existing instance
  python deploy/ec2_deploy.py --status           # show instance, URL and container status

Uses the default AWS credential chain (e.g. ~/.aws/credentials written by
run-aws.cmd). It is idempotent: everything is found again by its Name tag.

* Security Group: SSH (22) and Grafana (3000) from YOUR public IP /32 only.
* IAM: the instance runs with LabInstanceProfile, so no AWS keys go on the
  host; containers reach the instance profile through IMDSv2 (hop limit 2).
* Elastic IP (if the sandbox allows it), so the address survives stop/start.
* The remote .env is generated once (random passwords) and never overwritten;
  a copy is kept at ~/.chargepulse/ec2.env so you know the Grafana password.
"""

from __future__ import annotations

import argparse
import io
import os
import secrets
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REPO = Path(__file__).resolve().parent.parent
NAME = "chargepulse"
KEY_PATH = Path.home() / ".ssh" / f"{NAME}-key.pem"
LOCAL_ENV_COPY = Path.home() / ".chargepulse" / "ec2.env"
AMI_PARAM = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
REMOTE_DIR = "ChargePulse"
PACKAGE = [
    "docker-compose.yml", ".env.example", ".dockerignore", ".gitignore", "README.md",
    "requirements.txt", "chargepulse_common", "config", "producer", "streaming", "batch",
    "serving", "grafana", "spark", "tests", "docs", "deploy",
]
# Memory sizing for instances below 8 GB RAM.
SMALL_MEMORY = {
    "KAFKA_HEAP_OPTS": '"-Xms256m -Xmx384m"',
    "SPEED_DRIVER_MEMORY": "640m",
    "BATCH_DRIVER_MEMORY": "640m",
}
SMALL_TYPES = ("nano", "micro", "small", "medium", "large")


def log(msg: str) -> None:
    print(f"[deploy] {msg}", flush=True)


# --------------------------------------------------------------- AWS infra
class Infra:
    def __init__(self, region: str):
        self.ec2 = boto3.client("ec2", region_name=region)
        self.ssm = boto3.client("ssm", region_name=region)

    def key_pair(self) -> str:
        name = f"{NAME}-key"
        existing = self.ec2.describe_key_pairs(Filters=[{"Name": "key-name", "Values": [name]}])["KeyPairs"]
        if existing and KEY_PATH.exists():
            return name
        if existing:  # key exists in AWS but we lost the private half
            self.ec2.delete_key_pair(KeyName=name)
        material = self.ec2.create_key_pair(KeyName=name, KeyType="ed25519")["KeyMaterial"]
        KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
        KEY_PATH.write_bytes(material.encode())  # LF line endings, or ssh says "invalid format"
        if os.name == "nt":  # OpenSSH on Windows refuses keys readable by others
            user = os.environ.get("USERNAME", "")
            subprocess.run(["icacls", str(KEY_PATH), "/inheritance:r", "/grant:r", f"{user}:R"],
                           check=True, capture_output=True)
        else:
            KEY_PATH.chmod(0o600)
        log(f"created key pair {name}, private key saved to {KEY_PATH}")
        return name

    def security_group(self, my_ip: str) -> str:
        vpc = self.ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
        name = f"{NAME}-sg"
        groups = self.ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [name]}, {"Name": "vpc-id", "Values": [vpc]}]
        )["SecurityGroups"]
        if groups:
            sg = groups[0]["GroupId"]
        else:
            sg = self.ec2.create_security_group(
                GroupName=name, VpcId=vpc,
                Description="ChargePulse: SSH and Grafana from the team IP only",
                TagSpecifications=[{"ResourceType": "security-group", "Tags": [{"Key": "Name", "Value": name}]}],
            )["GroupId"]
            log(f"created security group {sg}")
        # Replace any old rules so only the current /32 is allowed (IP changes at home).
        current = self.ec2.describe_security_groups(GroupIds=[sg])["SecurityGroups"][0]["IpPermissions"]
        if current:
            self.ec2.revoke_security_group_ingress(GroupId=sg, IpPermissions=current)
        self.ec2.authorize_security_group_ingress(
            GroupId=sg,
            IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": port, "ToPort": port,
                 "IpRanges": [{"CidrIp": f"{my_ip}/32", "Description": desc}]}
                for port, desc in ((22, "SSH"), (3000, "Grafana"))
            ],
        )
        log(f"security group {sg}: ports 22, 3000 open to {my_ip}/32 only")
        return sg

    def find_instance(self) -> dict | None:
        reservations = self.ec2.describe_instances(Filters=[
            {"Name": "tag:Name", "Values": [NAME]},
            {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
        ])["Reservations"]
        instances = [i for r in reservations for i in r["Instances"]]
        return instances[0] if instances else None

    def instance(self, instance_type: str, key: str, sg: str, disk_gb: int) -> dict:
        found = self.find_instance()
        if found:
            if found["State"]["Name"] in ("stopped", "stopping"):
                self.ec2.get_waiter("instance_stopped").wait(InstanceIds=[found["InstanceId"]])
                self.ec2.start_instances(InstanceIds=[found["InstanceId"]])
                log(f"starting existing instance {found['InstanceId']}")
            else:
                log(f"reusing instance {found['InstanceId']} ({found['InstanceType']})")
        else:
            ami = self.ssm.get_parameter(Name=AMI_PARAM)["Parameter"]["Value"]
            found = self.ec2.run_instances(
                ImageId=ami, InstanceType=instance_type, MinCount=1, MaxCount=1,
                KeyName=key, SecurityGroupIds=[sg],
                IamInstanceProfile={"Name": "LabInstanceProfile"},
                MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled",
                                 "HttpPutResponseHopLimit": 2},
                BlockDeviceMappings=[{"DeviceName": "/dev/sda1",
                                      "Ebs": {"VolumeSize": disk_gb, "VolumeType": "gp3",
                                              "DeleteOnTermination": True}}],
                UserData=(REPO / "deploy" / "ec2_user_data.sh").read_text(),
                TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": NAME}]}],
            )["Instances"][0]
            log(f"launched {found['InstanceId']} ({instance_type}, Ubuntu 24.04, {disk_gb} GB gp3)")
        self.ec2.get_waiter("instance_running").wait(InstanceIds=[found["InstanceId"]])
        return self.ec2.describe_instances(InstanceIds=[found["InstanceId"]])["Reservations"][0]["Instances"][0]

    def elastic_ip(self, instance: dict) -> str:
        addresses = self.ec2.describe_addresses(Filters=[{"Name": "tag:Name", "Values": [NAME]}])["Addresses"]
        try:
            if addresses:
                allocation = addresses[0]["AllocationId"]
            else:
                allocation = self.ec2.allocate_address(
                    Domain="vpc",
                    TagSpecifications=[{"ResourceType": "elastic-ip", "Tags": [{"Key": "Name", "Value": NAME}]}],
                )["AllocationId"]
                log("allocated an Elastic IP")
            if not addresses or addresses[0].get("InstanceId") != instance["InstanceId"]:
                self.ec2.associate_address(AllocationId=allocation, InstanceId=instance["InstanceId"])
            return self.ec2.describe_addresses(AllocationIds=[allocation])["Addresses"][0]["PublicIp"]
        except ClientError as exc:
            log(f"Elastic IP not available in this sandbox ({exc.response['Error']['Code']}); "
                "using the instance public IP (changes on stop/start)")
            return instance["PublicIpAddress"]


# ------------------------------------------------------------------ remote
def ssh_base(ip: str) -> list[str]:
    return ["ssh", "-i", str(KEY_PATH), "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30", f"ubuntu@{ip}"]


def ssh(ip: str, command: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(ssh_base(ip) + [command], check=check, text=True,
                          capture_output=capture)


def scp(ip: str, local: Path, remote: str) -> None:
    subprocess.run(["scp", "-i", str(KEY_PATH), "-o", "StrictHostKeyChecking=accept-new",
                    str(local), f"ubuntu@{ip}:{remote}"], check=True)


def wait_for_host(ip: str, timeout: int = 900) -> None:
    log("waiting for SSH and first-boot setup (Docker install, swap)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = ssh(ip, "test -f /var/lib/cloud/chargepulse-ready && docker compose version", check=False, capture=True)
        if r.returncode == 0:
            log(f"host ready: {r.stdout.strip()}")
            return
        time.sleep(15)
    raise TimeoutError("instance did not finish first-boot setup in time")


def build_package() -> Path:
    out = Path.home() / ".chargepulse" / "chargepulse-src.tar.gz"
    out.parent.mkdir(parents=True, exist_ok=True)

    def skip(info: tarfile.TarInfo):
        parts = Path(info.name).parts
        if "__pycache__" in parts or info.name.endswith((".pyc", ".pem")):
            return None
        return info

    with tarfile.open(out, "w:gz") as tar:
        for item in PACKAGE:
            tar.add(REPO / item, arcname=item, filter=skip)
    return out


def make_env(bucket: str, region: str, instance_type: str) -> str:
    values = {
        "POSTGRES_PASSWORD": secrets.token_urlsafe(18),
        "GRAFANA_ADMIN_PASSWORD": secrets.token_urlsafe(12),
        "S3_BUCKET": bucket,
        "AWS_DEFAULT_REGION": region,
        "S3_ENDPOINT_URL": "",
        "S3_CREATE_BUCKET": "true",
        "AWS_ACCESS_KEY_ID": "",
        "AWS_SECRET_ACCESS_KEY": "",
        "AWS_SESSION_TOKEN": "",
        "COMPOSE_PROFILES": "",
    }
    if instance_type.split(".")[-1] in SMALL_TYPES:
        values.update(SMALL_MEMORY)
    lines = []
    for line in (REPO / ".env.example").read_text().splitlines():
        key = line.split("=", 1)[0]
        if "=" in line and not line.startswith("#") and key in values:
            lines.append(f"{key}={values.pop(key)}")
        else:
            lines.append(line)
    lines += [f"{k}={v}" for k, v in values.items()]
    return "\n".join(["# Generated by deploy/ec2_deploy.py (instance profile, no AWS keys)"] + lines) + "\n"


def deploy_code(ip: str, bucket: str, region: str, instance_type: str) -> None:
    package = build_package()
    log(f"uploading code ({package.stat().st_size // 1024} KB)")
    scp(ip, package, "chargepulse-src.tar.gz")
    ssh(ip, f"mkdir -p {REMOTE_DIR} && tar xzf chargepulse-src.tar.gz -C {REMOTE_DIR} && rm chargepulse-src.tar.gz")

    has_env = ssh(ip, f"test -f {REMOTE_DIR}/.env", check=False).returncode == 0
    if not has_env:
        env_text = make_env(bucket, region, instance_type)
        LOCAL_ENV_COPY.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_ENV_COPY.write_bytes(env_text.encode())  # LF only; CRs would leak into values
        scp(ip, LOCAL_ENV_COPY, f"{REMOTE_DIR}/.env")
        ssh(ip, f"chmod 600 {REMOTE_DIR}/.env")
        log(f"wrote remote .env (copy with the Grafana password: {LOCAL_ENV_COPY})")
    else:
        log("remote .env already exists; kept as is")

    log("docker compose up -d --build on the instance (first build takes several minutes)...")
    ssh(ip, f"cd {REMOTE_DIR} && docker compose up -d --build --remove-orphans > ~/compose-up.log 2>&1; "
            "status=$?; tail -n 15 ~/compose-up.log; exit $status")
    ssh(ip, f"cd {REMOTE_DIR} && docker compose ps --format 'table {{{{.Service}}}}\\t{{{{.Status}}}}'")


def my_public_ip() -> str:
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as r:
        return r.read().decode().strip()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    p.add_argument("--instance-type", default="t3.small",
                   help="t3.xlarge per the guideline; t3.small is the largest the Data Engineering sandbox allows")
    p.add_argument("--disk-gb", type=int, default=30)
    p.add_argument("--bucket", default=None, help="S3 bucket (default: chargepulse-<account>-ec2)")
    p.add_argument("--code-only", action="store_true", help="skip infra changes, redeploy code")
    p.add_argument("--status", action="store_true")
    args = p.parse_args(argv)

    account = boto3.client("sts", region_name=args.region).get_caller_identity()["Account"]
    bucket = args.bucket or f"chargepulse-{account}-ec2"
    infra = Infra(args.region)

    if args.status or args.code_only:
        instance = infra.find_instance()
        if not instance:
            log("no ChargePulse instance found; run without --status/--code-only first")
            return 1
        ip = infra.elastic_ip(instance) if instance["State"]["Name"] == "running" else None
        log(f"instance {instance['InstanceId']} {instance['InstanceType']} {instance['State']['Name']} ip={ip}")
        if args.status:
            if ip:
                ssh(ip, f"cd {REMOTE_DIR} && docker compose ps --format 'table {{{{.Service}}}}\\t{{{{.Status}}}}'",
                    check=False)
                log(f"Grafana: http://{ip}:3000")
            return 0
        deploy_code(ip, bucket, args.region, instance["InstanceType"])
        log(f"done. Grafana: http://{ip}:3000")
        return 0

    my_ip = my_public_ip()
    key = infra.key_pair()
    sg = infra.security_group(my_ip)
    instance = infra.instance(args.instance_type, key, sg, args.disk_gb)
    ip = infra.elastic_ip(instance)
    log(f"instance {instance['InstanceId']} at {ip}")
    wait_for_host(ip)
    deploy_code(ip, bucket, args.region, instance["InstanceType"])
    log(f"done. Grafana: http://{ip}:3000  (user admin, password in {LOCAL_ENV_COPY})")
    log(f"SSH: ssh -i {KEY_PATH} ubuntu@{ip}")
    log("Stop the instance when you are not working (EC2 console -> Instance state -> Stop).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
