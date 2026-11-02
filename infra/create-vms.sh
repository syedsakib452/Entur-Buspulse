#!/usr/bin/env bash
# Creates the security group and the three k3s VMs in OpenStack.
# Run on a machine with the openstack CLI and your login loaded:
#   set -a; source ~/openstack.env; set +a
#   ./infra/create-vms.sh
# Safe to re-run: existing resources are skipped.
set -euo pipefail

NETWORK="${NETWORK:-dat515-network}"
KEY="${KEY:-buspulse}"
FLAVOR="${FLAVOR:-m1.medium}"
IMAGE="${IMAGE:-Ubuntu-24.04}"
SG="k3s-cluster"
UIS_NET="172.16.0.0/16"
NODES=(k3s-master k3s-worker1 k3s-worker2)

if ! openstack security group show "$SG" >/dev/null 2>&1; then
  echo "Creating security group $SG"
  openstack security group create "$SG" --description "BusPulse k3s nodes" >/dev/null
  # SSH and the Kubernetes API from the UiS network (incl. the jump host)
  openstack security group rule create "$SG" --protocol tcp --dst-port 22   --remote-ip "$UIS_NET" >/dev/null
  openstack security group rule create "$SG" --protocol tcp --dst-port 6443 --remote-ip "$UIS_NET" >/dev/null
  # Everything between the cluster nodes (k3s needs several ports)
  openstack security group rule create "$SG" --protocol any --remote-group "$SG" >/dev/null
else
  echo "Security group $SG already exists"
fi

for name in "${NODES[@]}"; do
  if openstack server show "$name" >/dev/null 2>&1; then
    echo "VM $name already exists"
    continue
  fi
  echo "Creating VM $name"
  openstack server create "$name" --flavor "$FLAVOR" --image "$IMAGE" \
    --network "$NETWORK" --key-name "$KEY" --security-group "$SG" --wait >/dev/null
done

openstack server list --name 'k3s-' -c Name -c Status -c Networks
