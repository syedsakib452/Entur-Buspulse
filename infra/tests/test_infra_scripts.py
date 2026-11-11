"""Checks for the OpenStack/k3s scripts in infra/ (they are not run, only checked).

    pytest infra/tests -v
"""
import subprocess
from pathlib import Path

import pytest

INFRA = Path(__file__).resolve().parents[1]
SCRIPTS = ["create-vms.sh", "destroy-vms.sh", "install-k3s.sh"]


def bash_syntax_error(text):
    """None if bash can parse the script, else the error. Checked as Linux sees it
    (a Windows checkout has CRLF line ends; git stores LF)."""
    try:
        # bytes, not text: on Windows, text mode would turn the line ends back into CRLF
        result = subprocess.run(["bash", "-n"], input=text.replace("\r\n", "\n").encode(), capture_output=True)
    except FileNotFoundError:
        pytest.skip("bash not available")
    return result.stderr.decode(errors="replace") if result.returncode else None


@pytest.mark.parametrize("script", SCRIPTS)
def test_script_stops_on_errors_and_parses(script):
    text = (INFRA / script).read_text(encoding="utf-8")
    assert text.startswith("#!/usr/bin/env bash"), f"{script}: no bash shebang"
    assert "set -euo pipefail" in text, f"{script}: keeps going after a failed command"
    assert bash_syntax_error(text) is None, f"{script}: syntax error\n{bash_syntax_error(text)}"


def test_create_and_destroy_handle_the_same_vms():
    """destroy-vms.sh must remove every VM that create-vms.sh makes (no forgotten VMs that cost money)."""
    create = (INFRA / "create-vms.sh").read_text(encoding="utf-8")
    destroy = (INFRA / "destroy-vms.sh").read_text(encoding="utf-8")
    for vm in ("k3s-master", "k3s-worker1", "k3s-worker2"):
        assert vm in create, f"create-vms.sh does not create {vm}"
        assert vm in destroy, f"destroy-vms.sh does not remove {vm}"


def test_scripts_contain_no_openstack_login():
    for script in SCRIPTS:
        text = (INFRA / script).read_text(encoding="utf-8")
        for secret in ("OS_PASSWORD=", "OS_APPLICATION_CREDENTIAL_SECRET="):
            assert secret not in text, f"{script}: OpenStack login written into the script"
