"""Checks for the GitHub Actions workflows in .github/workflows/.
(pytest skips folders starting with a dot, so these tests live here.)

    pytest tests/test_ci_workflows.py -v
"""
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO / ".github" / "workflows"


def workflows():
    paths = sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml"))
    return [(p.name, yaml.safe_load(p.read_text(encoding="utf-8"))) for p in paths]


def triggers(wf):
    on = wf.get("on", wf.get(True))       # YAML 1.1 reads the key `on` as True
    return {on} if isinstance(on, str) else set(on or {})


def test_workflows_are_valid_yaml_with_jobs():
    assert workflows(), "no workflows found"
    for name, wf in workflows():
        assert triggers(wf), f"{name}: no trigger"
        assert wf.get("jobs"), f"{name}: no jobs"
        for job_name, job in wf["jobs"].items():
            assert job.get("runs-on"), f"{name}/{job_name}: no runs-on"


def test_pull_requests_never_push_images():
    for name, wf in workflows():
        if "pull_request" not in triggers(wf):
            continue
        for job_name, job in wf["jobs"].items():
            for step in job.get("steps", []):
                if str(step.get("uses", "")).startswith("docker/build-push-action"):
                    push = str(step.get("with", {}).get(
                        "push", "false")).lower()
                    assert push != "true", f"{name}/{job_name}: pull requests push images"


@pytest.mark.xfail(reason="known issue: build.yml runs pull requests on the self-hosted runner (budpulse1). "
                          "Remove its pull_request trigger, then delete this marker.", strict=False)
def test_self_hosted_runners_never_run_pull_requests():
    """The repository is public: a pull request from a fork must not run code on our VMs."""
    offenders = [f"{name}/{job_name}" for name, wf in workflows()
                 if triggers(wf) & {"pull_request", "pull_request_target"}
                 for job_name, job in wf["jobs"].items() if "self-hosted" in str(job.get("runs-on"))]
    assert not offenders, f"self-hosted jobs run for pull requests: {offenders}"


def test_workflows_only_get_the_permissions_they_need():
    for name, wf in workflows():
        for job_name, job in wf["jobs"].items():
            perms = job.get("permissions", wf.get("permissions"))
            assert perms is not None, f"{name}/{job_name}: default (broad) token permissions"
            assert perms != "write-all", f"{name}/{job_name}: write-all permissions"


def test_collector_image_is_built_where_kubernetes_pulls_it_from():
    wf = (WORKFLOWS / "build-image.yml").read_text(encoding="utf-8")
    assert "images: ghcr.io/${{ github.repository }}" in wf
    assert "type=sha" in wf, "no sha-<commit> tag to pin in k8s/kustomization.yaml"
    kust = yaml.safe_load(
        (REPO / "k8s" / "kustomization.yaml").read_text(encoding="utf-8"))
    for img in kust["images"]:
        assert img["name"].startswith(
            "ghcr.io/") and img["name"].endswith("/entur-buspulse")


def test_layer_images_are_built_from_existing_dockerfiles():
    wf = dict(workflows())["build-layer-images.yml"]
    for entry in wf["jobs"]["build"]["strategy"]["matrix"]["include"]:
        assert (REPO / entry["dockerfile"]
                ).exists(), f"{entry['layer']}: {entry['dockerfile']} is missing"
