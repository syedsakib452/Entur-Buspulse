"""Checks for the collectors' container setup: Dockerfile and compose.yaml (local/VM development).

    pytest tests/test_collector_container.py -v
"""
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]


def test_base_images_have_a_fixed_version():
    for dockerfile in ("Dockerfile", "uploader/Dockerfile"):
        for line in (REPO / dockerfile).read_text(encoding="utf-8").splitlines():
            if line.strip().upper().startswith("FROM "):
                image = line.split()[1]
                assert ":" in image and not image.endswith(":latest"), f"{dockerfile}: unpinned base image {image}"


def test_image_contains_only_the_collector_code():
    copies = [l for l in (REPO / "Dockerfile").read_text(encoding="utf-8").splitlines() if l.startswith("COPY")]
    assert copies and not any(c.split()[1] == "." for c in copies), "the whole repository is copied into the image"


def test_one_restarting_service_per_collector():
    services = yaml.safe_load((REPO / "compose.yaml").read_text(encoding="utf-8"))["services"]
    collectors = {n: s for n, s in services.items() if "jobs" not in s.get("profiles", [])}
    modules = {p.stem for p in (REPO / "collectors").glob("*.py") if not p.stem.startswith("_")}
    for name, svc in collectors.items():
        assert svc.get("restart") in {"always", "unless-stopped"}, f"compose service {name} is not restarted"
        assert svc["command"].removeprefix("collectors.") in modules, f"{name}: runs unknown module {svc['command']}"


def test_openstack_login_is_read_from_outside_the_repository():
    uploader = yaml.safe_load((REPO / "compose.yaml").read_text(encoding="utf-8"))["services"]["uploader"]
    assert str(uploader["env_file"]).startswith("${HOME}"), "the login file must live outside the repo"
