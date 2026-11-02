#!/usr/bin/env bash
# Installs k3s on the three VMs and sets up kubectl on this machine.
# Needs: SSH access to the VMs with the names below (see infra/README.md),
#        and the openstack CLI with your login loaded (to look up the master IP).
#   ./infra/install-k3s.sh
set -euo pipefail

MASTER=k3s-master
WORKERS=(k3s-worker1 k3s-worker2)

MASTER_IP=$(openstack server show "$MASTER" -f value -c addresses | grep -oE '[0-9]+(\.[0-9]+){3}' | head -1)
echo "Master IP: $MASTER_IP"

echo "Syncing clocks with UiS's time server (public time servers are blocked)"
for n in "$MASTER" "${WORKERS[@]}"; do
  ssh "$n" 'sudo mkdir -p /etc/systemd/timesyncd.conf.d &&
    printf "[Time]\nNTP=ntp.ux.uis.no\n" | sudo tee /etc/systemd/timesyncd.conf.d/uis.conf >/dev/null &&
    sudo systemctl restart systemd-timesyncd'
done

echo "Installing k3s server on $MASTER"
ssh "$MASTER" 'curl -sfL https://get.k3s.io | sh -s - server --write-kubeconfig-mode 644'
TOKEN=$(ssh "$MASTER" 'sudo cat /var/lib/rancher/k3s/server/node-token')

for w in "${WORKERS[@]}"; do
  echo "Joining $w"
  ssh "$w" "curl -sfL https://get.k3s.io | K3S_URL=https://$MASTER_IP:6443 K3S_TOKEN=$TOKEN sh -"
done

echo "Setting up kubectl on this machine"
mkdir -p ~/.kube
scp "$MASTER:/etc/rancher/k3s/k3s.yaml" ~/.kube/config
sed -i "s/127.0.0.1/$MASTER_IP/" ~/.kube/config
chmod 600 ~/.kube/config

echo "Waiting for all nodes to be Ready"
kubectl wait --for=condition=Ready node --all --timeout=180s
kubectl get nodes
