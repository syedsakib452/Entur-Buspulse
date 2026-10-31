#!/usr/bin/env bash
# Removes the three k3s VMs and their security group. Data in Swift is NOT touched.
#   ./infra/destroy-vms.sh
set -euo pipefail

read -r -p "Delete k3s-master, k3s-worker1, k3s-worker2 and security group k3s-cluster? [y/N] " answer
[ "$answer" = "y" ] || { echo "Cancelled"; exit 0; }

for name in k3s-master k3s-worker1 k3s-worker2; do
  openstack server delete "$name" --wait 2>/dev/null && echo "Deleted $name" || echo "$name not found"
done
openstack security group delete k3s-cluster 2>/dev/null && echo "Deleted security group" || echo "Security group not found"
