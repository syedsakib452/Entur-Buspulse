"""Checks for the dashboard's container image (dashboard/Dockerfile).

    pytest dashboard/tests -v
"""
from pathlib import Path

DASHBOARD = Path(__file__).resolve().parents[1]
DOCKERFILE = (DASHBOARD / "Dockerfile").read_text(encoding="utf-8")


def instructions(name):
    return [l.strip().split(None, 1)[1] for l in DOCKERFILE.splitlines() if l.strip().upper().startswith(name + " ")]


def test_base_image_has_a_fixed_version():
    for image in instructions("FROM"):
        image = image.split()[0]
        assert ":" in image and not image.endswith(":latest"), f"unpinned base image {image}"


def test_runs_as_non_root():
    users = instructions("USER")
    assert users and users[-1] not in {"root", "0"}, "the dashboard container runs as root"


def test_has_a_health_check():
    assert instructions("HEALTHCHECK"), "Docker cannot tell when the dashboard has hung"


def test_gold_data_is_mounted_not_built_in():
    """Gold changes every night, so it is mounted at run time, never copied into the image."""
    assert "/data/gold" in DOCKERFILE
    assert not any("tables" in c for c in instructions("COPY")), "gold tables are copied into the image"


def test_listens_on_all_interfaces_on_port_8501():
    cmd = instructions("CMD")[-1]
    assert "--server.address=0.0.0.0" in cmd, "only reachable from inside the container"
    assert "8501" in cmd and instructions("EXPOSE") == ["8501"]


def test_static_road_map_is_in_the_image():
    assert (DASHBOARD / "map" / "road_segments.parquet").exists(), "dashboard/map/road_segments.parquet is missing"
