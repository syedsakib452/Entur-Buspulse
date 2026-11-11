"""Checks for the Kubernetes manifests in k8s/ (what `kubectl apply -k k8s/` deploys).

They only read the YAML files, so no cluster is needed:  pytest k8s/tests -v
"""
import re
from pathlib import Path

import yaml

K8S = Path(__file__).resolve().parents[1]
NAMESPACE = "buspulse"
CONFIGMAP, SECRET = "collector-settings", "openstack-credentials"


def load_all(path):
    with open(path, encoding="utf-8") as f:
        return [d for d in yaml.safe_load_all(f) if d]


def kustomization():
    return yaml.safe_load((K8S / "kustomization.yaml").read_text(encoding="utf-8"))


def deployed_objects():
    """Every object `kubectl apply -k k8s/` creates, as (file name, object)."""
    return [(name, obj) for name in kustomization()["resources"] for obj in load_all(K8S / name)]


def workloads():
    """(name, kind, pod spec) for every Deployment and CronJob."""
    out = []
    for _, obj in deployed_objects():
        if obj["kind"] == "Deployment":
            out.append((obj["metadata"]["name"], obj["kind"], obj["spec"]["template"]["spec"]))
        elif obj["kind"] == "CronJob":
            out.append((obj["metadata"]["name"], obj["kind"], obj["spec"]["jobTemplate"]["spec"]["template"]["spec"]))
    return out


def deployments():
    return [o for _, o in deployed_objects() if o["kind"] == "Deployment"]


def containers():
    return [(name, c) for name, _, spec in workloads() for c in spec["containers"]]


def test_every_manifest_is_valid_kubernetes_yaml():
    for path in sorted(K8S.glob("*.yaml")):
        for obj in load_all(path):
            keys = ("apiVersion", "kind") if obj.get("kind") == "Kustomization" else ("apiVersion", "kind", "metadata")
            for key in keys:
                assert key in obj, f"{path.name}: missing {key}"


def test_kustomization_lists_every_manifest_and_nothing_missing():
    listed = set(kustomization()["resources"])
    on_disk = {p.name for p in K8S.glob("*.yaml")} - {"kustomization.yaml", "secret.example.yaml"}
    assert listed == on_disk, f"not deployed: {on_disk - listed}; listed but missing: {listed - on_disk}"


def test_secret_template_is_never_deployed():
    assert "secret.example.yaml" not in kustomization()["resources"]


def test_all_objects_are_in_the_buspulse_namespace():
    for name, obj in deployed_objects():
        if obj["kind"] == "Namespace":
            assert obj["metadata"]["name"] == NAMESPACE
        else:
            assert obj["metadata"].get("namespace") == NAMESPACE, f"{name}: {obj['kind']} outside {NAMESPACE}"


def test_image_tag_is_pinned_to_a_commit():
    """A pod restart must never pull code that was not deliberately deployed."""
    images = kustomization()["images"]
    assert images, "kustomization.yaml sets no image version"
    for img in images:
        assert re.fullmatch(r"sha-[0-9a-f]{7,40}", str(img["newTag"])), f"{img['name']} pinned to {img['newTag']!r}"


def test_every_container_uses_the_pinned_image():
    pinned = {img["name"] for img in kustomization()["images"]}
    for name, c in containers():
        assert c["image"].split(":")[0] in pinned, f"{name}: image {c['image']} is not pinned by kustomization.yaml"


def test_every_container_has_cpu_and_memory_requests_and_limits():
    for name, c in containers():
        res = c.get("resources", {})
        for part in ("requests", "limits"):
            for kind in ("cpu", "memory"):
                assert kind in res.get(part, {}), f"{name}: no {kind} {part}"


def test_pods_run_as_non_root_without_extra_privileges():
    for name, _, spec in workloads():
        assert spec.get("securityContext", {}).get("runAsNonRoot") is True, f"{name}: may run as root"
        for c in spec["containers"]:
            sc = c.get("securityContext", {})
            assert sc.get("allowPrivilegeEscalation") is False, f"{name}: privilege escalation allowed"
            assert "ALL" in sc.get("capabilities", {}).get("drop", []), f"{name}: Linux capabilities not dropped"


def test_collectors_get_settings_and_swift_credentials_from_config_objects():
    configmaps = {o["metadata"]["name"] for _, o in deployed_objects() if o["kind"] == "ConfigMap"}
    assert CONFIGMAP in configmaps
    for name, c in containers():
        refs = c.get("envFrom", [])
        assert {"configMapRef": {"name": CONFIGMAP}} in refs, f"{name}: no {CONFIGMAP}"
        assert {"secretRef": {"name": SECRET}} in refs, f"{name}: no {SECRET}"
        for env in c.get("env", []):
            assert not env["name"].startswith("OS_"), f"{name}: OpenStack login written into the manifest"


def test_long_running_collectors_are_single_writers_with_a_liveness_probe():
    assert deployments(), "no Deployments found"
    for d in deployments():
        name = d["metadata"]["name"]
        assert d["spec"]["replicas"] == 1, f"{name}: more than one writer"
        assert d["spec"]["strategy"]["type"] == "Recreate", f"{name}: rolling update would run two copies"
        for c in d["spec"]["template"]["spec"]["containers"]:
            assert "livenessProbe" in c, f"{name}: a stuck collector would never be restarted"


def test_deployment_selector_matches_its_pod_labels():
    for d in deployments():
        selector = d["spec"]["selector"]["matchLabels"]
        labels = d["spec"]["template"]["metadata"]["labels"]
        assert selector.items() <= labels.items(), f"{d['metadata']['name']}: selector does not match the pods"


def test_buffer_volume_is_mounted_and_size_limited():
    for d in deployments():
        spec = d["spec"]["template"]["spec"]
        vols = {v["name"]: v for v in spec.get("volumes", [])}
        assert "buffer" in vols and "emptyDir" in vols["buffer"], f"{d['metadata']['name']}: no buffer volume"
        assert vols["buffer"]["emptyDir"].get("sizeLimit"), "buffer can fill the node's disk"
        mounts = [m for c in spec["containers"] for m in c.get("volumeMounts", [])]
        assert any(m["name"] == "buffer" for m in mounts)


def test_cronjobs_cannot_overlap_or_hang():
    jobs = [o for _, o in deployed_objects() if o["kind"] == "CronJob"]
    assert jobs, "no CronJob found (static timetable)"
    for j in jobs:
        name, spec = j["metadata"]["name"], j["spec"]
        assert len(spec["schedule"].split()) == 5, f"{name}: bad cron schedule {spec['schedule']!r}"
        assert spec.get("concurrencyPolicy") == "Forbid", f"{name}: two runs can overlap"
        job = spec["jobTemplate"]["spec"]
        assert job.get("activeDeadlineSeconds"), f"{name}: a stuck run is never stopped"
        assert job.get("backoffLimit") is not None, f"{name}: no retry limit"


def test_secret_template_holds_only_placeholders():
    secret = load_all(K8S / "secret.example.yaml")[0]
    for key, value in secret.get("stringData", {}).items():
        if "SECRET" in key or "PASSWORD" in key or key.endswith("_ID"):
            assert value.startswith("<") and value.endswith(">"), f"secret.example.yaml: {key} looks real"
