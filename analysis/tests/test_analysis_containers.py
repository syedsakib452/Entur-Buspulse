"""Checks for the batch layers' containers: Dockerfiles, analysis/compose.yaml and the VM pipeline script.

    pytest analysis/tests -v
"""
import subprocess
from pathlib import Path

import pytest
import yaml

ANALYSIS = Path(__file__).resolve().parents[1]
LAYERS = {"bronze": "bronze-layer/Dockerfile", "silver": "silver-layer/Dockerfile", "gold": "gold/Dockerfile"}


def lines(path, name):
    return [l.strip().split(None, 1)[1] for l in (ANALYSIS / path).read_text(encoding="utf-8").splitlines()
            if l.strip().upper().startswith(name + " ")]


@pytest.mark.parametrize("dockerfile", LAYERS.values())
def test_base_image_has_a_fixed_version(dockerfile):
    for image in lines(dockerfile, "FROM"):
        image = image.split()[0]
        assert ":" in image and not image.endswith(":latest"), f"{dockerfile}: unpinned base image {image}"


@pytest.mark.parametrize("dockerfile", LAYERS.values())
def test_layer_runs_as_non_root(dockerfile):
    users = lines(dockerfile, "USER")
    assert users and users[-1] not in {"root", "0"}, f"{dockerfile}: container runs as root"


def test_compose_builds_each_layer_from_its_own_dockerfile():
    services = yaml.safe_load((ANALYSIS / "compose.yaml").read_text(encoding="utf-8"))["services"]
    assert set(services) == set(LAYERS), f"compose services {set(services)} != layers {set(LAYERS)}"
    for layer, dockerfile in LAYERS.items():
        assert services[layer]["build"]["dockerfile"] == dockerfile, f"{layer}: wrong Dockerfile"
        assert (ANALYSIS / dockerfile).exists(), f"{dockerfile} is missing"


def test_layers_run_one_at_a_time_and_share_the_tables_folder():
    services = yaml.safe_load((ANALYSIS / "compose.yaml").read_text(encoding="utf-8"))["services"]
    for name, svc in services.items():
        assert svc.get("profiles"), f"{name}: would start with every `docker compose up`"
        assert "depends_on" not in svc, f"{name}: layers must stay independent"
        assert any(v.endswith(":/tables") for v in svc["volumes"]), f"{name}: does not mount the tables folder"


def test_spark_has_a_memory_and_cpu_limit():
    silver = yaml.safe_load((ANALYSIS / "compose.yaml").read_text(encoding="utf-8"))["services"]["silver"]
    assert silver.get("mem_limit") and silver.get("cpus"), "silver (Spark) can use all of the VM"


def test_no_openstack_login_in_compose():
    services = yaml.safe_load((ANALYSIS / "compose.yaml").read_text(encoding="utf-8"))["services"]
    for name, svc in services.items():
        for key, value in (svc.get("environment") or {}).items():
            if key.startswith("OS_"):
                assert str(value).startswith("${"), f"{name}: {key} is written into compose.yaml"


def test_pipeline_script_stops_on_errors_and_never_overlaps():
    text = (ANALYSIS / "scripts" / "run-pipeline.sh").read_text(encoding="utf-8")
    assert "set -euo pipefail" in text, "keeps going after a failed command"
    assert "flock" in text, "two nightly runs could overlap"
    try:   # checked as Linux sees it (a Windows checkout has CRLF line ends; git stores LF)
        # bytes, not text: on Windows, text mode would turn the line ends back into CRLF
        result = subprocess.run(["bash", "-n"], input=text.replace("\r\n", "\n").encode(), capture_output=True)
    except FileNotFoundError:
        pytest.skip("bash not available")
    assert result.returncode == 0, f"syntax error\n{result.stderr.decode(errors='replace')}"
