"""Checks for the monitoring setup in monitoring/: PodMonitor, alert rules and Grafana dashboard.

They only read the files, so no cluster is needed:  pytest monitoring/tests -v
"""
import json
import re
from pathlib import Path

import yaml

MON = Path(__file__).resolve().parents[1]
REPO = MON.parent
K8S = REPO / "k8s"
NAMESPACE = "buspulse"


def load_all(path):
    with open(path, encoding="utf-8") as f:
        return [d for d in yaml.safe_load_all(f) if d]


def collector_deployments():
    kust = yaml.safe_load((K8S / "kustomization.yaml").read_text(encoding="utf-8"))
    return [o for name in kust["resources"] for o in load_all(K8S / name) if o["kind"] == "Deployment"]


def known_metrics():
    """Metric names common/metrics.py exports (Prometheus adds _total to counters)."""
    src = (REPO / "common" / "metrics.py").read_text(encoding="utf-8")
    names = {n + "_total" for n in re.findall(r'Counter\(\s*"(\w+)"', src)}
    names |= set(re.findall(r'Gauge\(\s*"(\w+)"', src))
    return names


def test_podmonitor_scrapes_the_collector_pods():
    pm = load_all(MON / "buspulse-podmonitor.yaml")[0]
    assert NAMESPACE in pm["spec"]["namespaceSelector"]["matchNames"]
    selector = pm["spec"]["selector"]["matchLabels"]
    ports = {e["port"] for e in pm["spec"]["podMetricsEndpoints"]}
    deployments = collector_deployments()
    assert deployments, "no collector Deployments in k8s/"
    for d in deployments:
        tpl, name = d["spec"]["template"], d["metadata"]["name"]
        assert selector.items() <= tpl["metadata"]["labels"].items(), f"{name}: not scraped by Prometheus"
        pod_ports = {p.get("name") for c in tpl["spec"]["containers"] for p in c.get("ports", [])}
        assert ports <= pod_ports, f"{name}: has no port named {ports - pod_ports}"


def test_alert_rules_are_complete():
    rules = [r for g in load_all(MON / "buspulse-alerts.yaml")[0]["spec"]["groups"] for r in g["rules"]]
    names = {r["alert"] for r in rules}
    assert "BusPulseNoNewData" in names, "the main data-loss alert is missing"
    for r in rules:
        assert r.get("expr"), f"{r['alert']}: no expression"
        assert r.get("labels", {}).get("severity") in {"critical", "warning", "info"}, f"{r['alert']}: no severity"
        assert r.get("annotations", {}).get("summary"), f"{r['alert']}: no summary for the Discord message"


def test_alerts_and_dashboard_only_use_metrics_the_collectors_export():
    exported = known_metrics()
    text = "".join((MON / f).read_text(encoding="utf-8") for f in ("buspulse-alerts.yaml", "buspulse-dashboard.yaml"))
    used = set(re.findall(r"\bbuspulse_[a-z_]+\b", text))
    assert used, "no BusPulse metrics used in monitoring/"
    assert used <= exported, f"metrics that common/metrics.py does not export: {sorted(used - exported)}"


def test_grafana_dashboard_is_loaded_automatically_and_valid_json():
    cm = load_all(MON / "buspulse-dashboard.yaml")[0]
    assert cm["metadata"]["labels"].get("grafana_dashboard") == "1", "Grafana's sidecar would not pick it up"
    for key, raw in cm["data"].items():
        dash = json.loads(raw)
        assert dash.get("uid") and dash.get("title"), f"{key}: no uid/title"
        assert dash.get("panels"), f"{key}: no panels"


def test_helm_values_files_are_valid_yaml():
    for name in ("prometheus-grafana-values.yaml", "loki-values.yaml", "alloy-values.yaml"):
        assert isinstance(yaml.safe_load((MON / name).read_text(encoding="utf-8")), dict), f"{name}: not valid YAML"
