# Cluster setup (k3s on UiS OpenStack)

Three Ubuntu 24.04 VMs (`m1.medium`: 2 CPU, 4 GB RAM) on `dat515-network`:

| VM | Role |
|---|---|
| `k3s-master` | Control plane |
| `k3s-worker1` | Worker |
| `k3s-worker2` | Worker |

Security group `k3s-cluster`: SSH (22) and the Kubernetes API (6443) from the UiS network
(`172.16.0.0/16`, includes the jump host), and all traffic between the three nodes. Everything else is blocked.

## Prerequisites

Run from a machine on the UiS network (we use `budpulse1`) with:

```bash
sudo apt install -y python3-openstackclient
sudo snap install kubectl --classic
set -a; source ~/openstack.env; set +a   # OpenStack login (OS_... lines), not in Git
```

SSH access to the VMs by name, in `~/.ssh/config` (IPs from `openstack server list`):

```
Host k3s-master
  HostName <master ip>
Host k3s-worker1
  HostName <worker1 ip>
Host k3s-worker2
  HostName <worker2 ip>
Host k3s-*
  User ubuntu
  IdentityFile ~/.ssh/buspulse.pem
```

## Create

```bash
./infra/create-vms.sh     # security group + 3 VMs (skips what already exists)
# add the IPs to ~/.ssh/config, then:
./infra/install-k3s.sh    # k3s on all nodes + kubectl on this machine
```

## Verify

```bash
kubectl get nodes         # 3 nodes, all Ready
```

## Remove

```bash
./infra/destroy-vms.sh    # deletes the VMs and the security group; Swift data is kept
```
