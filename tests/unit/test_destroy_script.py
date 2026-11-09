"""Runs scripts/destroy.sh against stub `aws` and `cdk` executables, so nothing touches AWS."""
import os
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"

AWS_STUB = """#!/usr/bin/env bash
case "$*" in
  *"sts get-caller-identity"*) echo 111111111111 ;;
  *"cloudformation describe-stacks"*)
    name="$(echo "$*" | sed -E 's/.*--stack-name ([^ ]+).*/\\1/')"
    if [[ -n "${STUB_MISSING:-}" && "$name" == *"$STUB_MISSING"* ]]; then exit 255; fi
    if [[ -e "$STUB_STATE/$name" ]]; then exit 255; fi
    echo sg-stub ;;
esac
"""
CDK_STUB = """#!/usr/bin/env bash
echo "CDK $*"
[[ "$1" == "destroy" ]] && touch "$STUB_STATE/$2"
exit 0
"""


@pytest.fixture
def run(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    for name, body in (("aws", AWS_STUB), ("cdk", CDK_STUB)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)

    def _run(*args, script="destroy.sh", missing=""):
        env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "VIRTUAL_ENV": "x", "STUB_MISSING": missing,
               "STUB_STATE": str(state_dir)}
        done = subprocess.run(
            [str(SCRIPTS / script), "--profile", "stub", *args], env=env, capture_output=True, text=True
        )
        return done.returncode, [l for l in done.stdout.splitlines() if l.startswith("CDK ")], done.stdout + done.stderr

    return _run


def test_cluster_stage_destroys_only_the_cluster_stack(run):
    code, cdk_calls, _ = run("--stage", "cluster")
    assert code == 0
    assert [c.split()[2] for c in cdk_calls] == ["EmrSdpClusterStack"]


def test_keep_platform_is_an_alias_for_the_cluster_stage(run):
    _, cdk_calls, _ = run("--keep-platform")
    assert [c.split()[2] for c in cdk_calls] == ["EmrSdpClusterStack"]


def test_wrapper_script_runs_the_cluster_stage(run):
    _, cdk_calls, _ = run(script="destroy_cluster.sh")
    assert [c.split()[2] for c in cdk_calls] == ["EmrSdpClusterStack"]


def test_default_destroys_cluster_then_platform(run):
    _, cdk_calls, _ = run()
    assert [c.split()[2] for c in cdk_calls] == ["EmrSdpClusterStack", "EmrSdpPlatformStack"]


def test_platform_stage_refuses_while_the_cluster_stack_exists(run):
    code, cdk_calls, output = run("--stage", "platform")
    assert code != 0 and cdk_calls == []
    assert "--stage cluster first" in output


def test_missing_cluster_stack_is_not_an_error(run):
    code, cdk_calls, output = run("--stage", "cluster", missing="ClusterStack")
    assert code == 0 and cdk_calls == []
    assert "nothing to do" in output


def test_unknown_stage_is_rejected_before_any_aws_call(run):
    code, cdk_calls, output = run("--stage", "bogus")
    assert code != 0 and cdk_calls == []
    assert "must be cluster, platform or all" in output
