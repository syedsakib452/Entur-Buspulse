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

# Discord webhook for alerts (Discord: channel settings -> Integrations -> Webhooks -> Copy URL).
# The URL works like a password, so it lives only in this Secret, never in Git.
read -rsp "Discord webhook URL: " DWH; echo
kubectl create secret generic discord-webhook -n monitoring --from-literal=url="$DWH"
unset DWH

# The three charts, with pinned versions so a reinstall gives the same result
helm install monitoring prometheus-community/kube-prometheus-stack --version 92.17.0 \
  -n monitoring -f monitoring/prometheus-grafana-values.yaml
helm install loki  grafana/loki  --version 7.5.0  -n monitoring -f monitoring/loki-values.yaml
helm install alloy grafana/alloy --version 1.15.0 -n monitoring -f monitoring/alloy-values.yaml

kubectl get pods -n monitoring -w      # wait until everything is Running
helm list -n monitoring                # check the versions below
```

Installed versions (chart / app): kube-prometheus-stack 92.17.0 / v0.94.2,
loki 7.5.0 / 3.6.16, alloy 1.15.0 / v1.21.3.

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

Edit a values file, commit it, `git pull` on budpulse1, then upgrade that chart with the
same version (GitHub Actions does not deploy this):

```bash
helm upgrade monitoring prometheus-community/kube-prometheus-stack --version 92.17.0 \
  -n monitoring -f monitoring/prometheus-grafana-values.yaml
helm upgrade loki  grafana/loki  --version 7.5.0  -n monitoring -f monitoring/loki-values.yaml
helm upgrade alloy grafana/alloy --version 1.15.0 -n monitoring -f monitoring/alloy-values.yaml
```

## Remove

```bash
helm uninstall alloy loki monitoring -n monitoring
kubectl delete namespace monitoring    # also deletes the stored metrics, logs and the Grafana login
```

## Collector metrics, alerts and dashboard

The collectors report their own numbers at `:8000/metrics` (see `common/metrics.py`): snapshots
stored (to Swift, to the buffer, skipped), when data last reached Swift, files waiting in the
buffer, whether Swift uploads are failing, and failed rounds.

| File | What it does |
|---|---|
| `buspulse-podmonitor.yaml` | Tells Prometheus to collect those numbers every 30 s |
| `buspulse-alerts.yaml` | Alerts: **no new data in Swift for 5 min**, no successful round for 10 min, Swift uploads failing for 10 min, fewer than 3 collectors running |
| `buspulse-dashboard.yaml` | Grafana dashboard **BusPulse / Data collection** (loaded automatically) |

```bash
kubectl apply -f monitoring/buspulse-podmonitor.yaml -f monitoring/buspulse-alerts.yaml -f monitoring/buspulse-dashboard.yaml
```

Check:
- Grafana → Explore → Prometheus → `up{namespace="buspulse"}`: three series with value 1.
- Dashboards → **BusPulse / Data collection**.
- Alerting → Alert rules → `buspulse-collectors`: four rules, normally *Normal* (green).

The chart's built-in rules also alert on crash-looping pods, failed jobs (e.g. the daily
static timetable) and node problems such as unsynchronised clocks.

## Alerts to Discord

Alertmanager posts every firing alert to our Discord channel, and again when it is resolved
(settings in `prometheus-grafana-values.yaml`, webhook URL from the Secret `discord-webhook`).
The chart's always-firing test alert (`Watchdog`) is not sent.

Send a test alert (arrives in Discord after about 30 s, resolved after about 5 min):

```bash
kubectl exec -n monitoring alertmanager-monitoring-kube-prometheus-alertmanager-0 -c alertmanager -- \
  amtool alert add BusPulseTest severity=warning feed=test --alertmanager.url=http://localhost:9093
```

Found this way: the VMs' clocks were not synchronised, because UiS blocks public time servers.
`infra/install-k3s.sh` now points every VM to UiS's own server, `ntp.ux.uis.no`.