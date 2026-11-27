#!/bin/bash
# Cloud-init for the ChargePulse EC2 host (Ubuntu 24.04). Runs once at first boot.
set -euxo pipefail

# Swap: lets the stack fit on small instances (t3.small = 2 GB RAM).
if [ ! -f /swapfile ]; then
  fallocate -l 4G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
echo 'vm.swappiness=20' > /etc/sysctl.d/99-chargepulse.conf
sysctl --system

# Docker Engine + Compose v2 + buildx from the Ubuntu archive.
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose-v2 docker-buildx git
systemctl enable --now docker
usermod -aG docker ubuntu

touch /var/lib/cloud/chargepulse-ready
