# Monitoring: Prometheus, Grafana, Loki, alerts

Everything runs in the namespace `monitoring` and covers the whole cluster (all nodes and pods).

| Part | Installed with | What it does |
|---|---|---|
| Prometheus + Alertmanager | `kube-prometheus-stack` chart, `prometheus-grafana-values.yaml` | Collects metrics every 30 s, keeps 7 days; sends alerts to Discord |
| Grafana | same chart | Dashboards and log search |
| Loki | `loki` chart, `loki-values.yaml` | Stores all pod logs for 7 days, also across pod restarts and replacements |
| Alloy | `alloy` chart, `alloy-values.yaml` | Reads every pod's logs and sends them to Loki |
| BusPulse metrics | `buspulse-podmonitor.yaml` | Collects the collectors' own numbers (`common/metrics.py`, port 8000) |
| BusPulse alerts | `buspulse-alerts.yaml` | **No new data in Swift for 5 min**, no successful round for 10 min, Swift uploads failing for 10 min, fewer than 3 collectors running |
| BusPulse dashboard | `buspulse-dashboard.yaml` | Grafana dashboard **BusPulse / Data collection** (loaded automatically) |

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

# Namespace and Secrets (typed, not shown, not saved in shell history, never in Git)
kubectl create namespace monitoring
read -rsp "Grafana admin password: " GPW; echo
kubectl create secret generic grafana-admin -n monitoring \
  --from-literal=admin-user=admin --from-literal=admin-password="$GPW"
unset GPW
# Discord: channel settings -> Integrations -> Webhooks -> New Webhook -> Copy Webhook URL
read -rsp "Discord webhook URL: " DWH; echo
kubectl create secret generic discord-webhook -n monitoring --from-literal=url="$DWH"
unset DWH

# The three charts, with pinned versions so a reinstall gives the same result
helm install monitoring prometheus-community/kube-prometheus-stack --version 92.17.0 \
  -n monitoring -f monitoring/prometheus-grafana-values.yaml
helm install loki  grafana/loki  --version 7.5.0  -n monitoring -f monitoring/loki-values.yaml
helm install alloy grafana/alloy --version 1.15.0 -n monitoring -f monitoring/alloy-values.yaml
kubectl get pods -n monitoring -w      # wait until everything is Running

# BusPulse metrics, alerts and dashboard
kubectl apply -f monitoring/buspulse-podmonitor.yaml -f monitoring/buspulse-alerts.yaml \
  -f monitoring/buspulse-dashboard.yaml
```

## Open Grafana

Grafana is only reachable inside the cluster. Forward it to your PC in two steps:

```bash
# 1. on budpulse1: forward Grafana to port 3000 there (keep this running)
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80

# 2. on your PC: tunnel your port 3000 to budpulse1's port 3000 (keep this running;
#    "buspulse" is the SSH name for budpulse1 in your ~/.ssh/config)
ssh -N -L 3000:localhost:3000 buspulse
```

Then open http://localhost:3000 and log in as `admin` with the password from above.

## Check that it works

- **Collectors reachable:** Explore → Prometheus → `up{namespace="buspulse"}`: three series with value 1.
- **Data collection:** Dashboards → **BusPulse / Data collection**: time since data last reached
  Swift, uploads, buffer, failed rounds, restarts, and errors and buffer events from the logs.
- **Alerts:** Alerting → Alert rules → `buspulse-collectors`: four rules, normally *Normal* (green).
- **Logs:** Explore → Loki → `{namespace="buspulse"}`; errors only: `{namespace="buspulse"} |= "ERROR"`.

## Alerts to Discord

Alertmanager posts firing alerts to our Discord channel, and again when they are resolved.
Besides our four rules, the chart's built-in rules cover crash-looping pods, failed jobs (e.g. the
daily static timetable) and node problems. Not sent: the chart's always-firing test alerts
(`Watchdog`, `InfoInhibitor`) and `KubeAPIErrorBudgetBurn`, which fires and resolves on its own on k3s.

Test (a message arrives after about 30 s, "resolved" about 5 min later):

```bash
kubectl exec -n monitoring alertmanager-monitoring-kube-prometheus-alertmanager-0 -c alertmanager -- \
  amtool alert add BusPulseTest severity=warning feed=test --alertmanager.url=http://localhost:9093
```

The alerts found one real problem so far: the VMs' clocks were not synchronised, because UiS
blocks public time servers. Fixed in `infra/install-k3s.sh`.

## Change settings

Edit a file, commit it, `git pull` on budpulse1, then apply it (GitHub Actions does not deploy this):

```bash
helm upgrade monitoring prometheus-community/kube-prometheus-stack --version 92.17.0 \
  -n monitoring -f monitoring/prometheus-grafana-values.yaml
helm upgrade loki  grafana/loki  --version 7.5.0  -n monitoring -f monitoring/loki-values.yaml
helm upgrade alloy grafana/alloy --version 1.15.0 -n monitoring -f monitoring/alloy-values.yaml
kubectl apply -f monitoring/buspulse-alerts.yaml    # same for the other buspulse-*.yaml files
```

## Remove

```bash
kubectl delete -f monitoring/buspulse-podmonitor.yaml -f monitoring/buspulse-alerts.yaml \
  -f monitoring/buspulse-dashboard.yaml
helm uninstall alloy loki monitoring -n monitoring
kubectl delete namespace monitoring    # also deletes stored metrics, logs and the Secrets
```
