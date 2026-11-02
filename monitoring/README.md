# Monitoring: Prometheus, Grafana, Loki

Everything runs in the namespace `monitoring` and covers the whole cluster (all nodes and pods).

| Part | Installed with | What it does |
|---|---|---|
| Prometheus + Alertmanager | `kube-prometheus-stack` chart, `prometheus-grafana-values.yaml` | Collects metrics (CPU, memory, restarts, ...) every 30 s, keeps 7 days; sends alerts |
| Grafana | same chart | Dashboards and log search |
| Loki | `loki` chart, `loki-values.yaml` | Stores all pod logs for 7 days, also across pod restarts and replacements |
| Alloy | `alloy` chart, `alloy-values.yaml` | Reads every pod's logs and sends them to Loki |

Sized for three 2 CPU / 4 GB VMs: about 1.2 GB of memory requested in total.
Prometheus, Loki and Grafana keep their data on disks created by k3s (local-path).

All commands run on a machine with `kubectl` access (we use `budpulse1`), from the repo root.

## Install

```bash
# Helm and the chart repositories (once)
sudo snap install helm --classic
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# Namespace and Grafana admin login (password typed, not shown, not saved in history)
kubectl create namespace monitoring
read -rsp "Grafana admin password: " GPW; echo
kubectl create secret generic grafana-admin -n monitoring \
  --from-literal=admin-user=admin --from-literal=admin-password="$GPW"
unset GPW

# The three charts
helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring \
  -f monitoring/prometheus-grafana-values.yaml
helm install loki  grafana/loki  -n monitoring -f monitoring/loki-values.yaml
helm install alloy grafana/alloy -n monitoring -f monitoring/alloy-values.yaml

kubectl get pods -n monitoring -w      # wait until everything is Running
helm list -n monitoring                # write the chart versions down below
```

Installed versions: _(fill in from `helm list -n monitoring`)_

## Open Grafana

Grafana is only reachable inside the cluster. Forward it to your PC in two steps:

```bash
# 1. on budpulse1: forward Grafana to port 3000 there (keep this running)
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80

# 2. on your PC: tunnel your port 3000 to budpulse1's port 3000 (keep this running)
ssh -N -L 3000:localhost:3000 buspulse
```

Then open http://localhost:3000 and log in as `admin` with the password from above.

## Check that it works

- **Metrics:** Dashboards → *Kubernetes / Compute Resources / Namespace (Pods)* → namespace `buspulse`:
  CPU and memory of the collectors.
- **Logs:** Explore → data source *Loki* → query `{namespace="buspulse"}`; errors only:
  `{namespace="buspulse"} |= "ERROR"`.

## Change settings

Edit a values file, then upgrade that chart, e.g.:

```bash
helm upgrade loki grafana/loki -n monitoring -f monitoring/loki-values.yaml
```

## Remove

```bash
helm uninstall alloy loki monitoring -n monitoring
kubectl delete namespace monitoring    # also deletes the stored metrics, logs and the Grafana login
```
