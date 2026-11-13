"""Repository-wide checks: no secrets or data files in git.

    pytest tests/test_repo_security.py -v
"""
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SECRET_FILES = re.compile(
    r"(\.env|openrc\.sh|clouds\.ya?ml|\.pem|id_rsa|id_ed25519|kubeconfig|k3s\.yaml)$")
SECRET_TEXT = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----|"
                         r"OS_(APPLICATION_CREDENTIAL_SECRET|PASSWORD)[ \t]*[=:][ \t]*['\"]?[A-Za-z0-9_\-]{12,}")   # value on the same line
BINARY = {".parquet", ".duckdb", ".zip", ".png", ".jpg", ".gz"}


def tracked_files():
    try:
        out = subprocess.run(["git", "ls-files"], cwd=REPO,
                             capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    return [REPO / p for p in out.splitlines()]


def test_env_files_are_git_ignored():
    assert "*.env" in (REPO / ".gitignore").read_text(encoding="utf-8").split()


def test_no_secrets_are_committed():
    found = []
    for path in tracked_files():
        rel = path.relative_to(REPO).as_posix()
        if SECRET_FILES.search(rel):
            found.append(rel)
            continue
        if path.suffix in BINARY or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if SECRET_TEXT.search(text):
            found.append(rel)
    assert not found, f"possible secrets in git: {found}"


# Data committed on purpose:
#   dashboard/map/               static road map, built into the dashboard image
#   tables/parquet_2026-10-24/   one day of sample data, so the dashboard and notebooks work right after cloning
ALLOWED_DATA = ("dashboard/map/", "tables/parquet_2026-10-24/")


def test_no_data_files_are_committed():
    data = []
    for path in tracked_files():
        rel = path.relative_to(REPO).as_posix()
        if path.suffix in {".parquet", ".duckdb"} and not rel.startswith(ALLOWED_DATA):
            data.append(rel)
    assert not data, f"{len(data)} data files tracked in git outside {ALLOWED_DATA}, e.g. {data[:3]}"
