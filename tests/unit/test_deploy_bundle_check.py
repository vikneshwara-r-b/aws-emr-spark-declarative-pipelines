"""scripts/deploy.sh --stage cluster must refuse to launch a cluster whose S3 bundle differs from the local build.
Uses stub aws/cdk executables, so nothing touches AWS."""
import os
import subprocess
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[2] / "scripts" / "deploy.sh"
AWS_STUB = """#!/usr/bin/env bash
case "$*" in
  *"sts get-caller-identity"*) echo 111111111111 ;;
  *"cloudformation describe-stacks"*) echo stub-value ;;
  *"s3api head-object"*) ;;
  *"s3 cp"*".sha256 -"*) echo "$STUB_REMOTE_SUM  spark-4.3.0-bundle.tar.gz" ;;
esac
"""
CDK_STUB = '#!/usr/bin/env bash\necho "CDK $*"\n'


@pytest.fixture
def deploy(tmp_path):
    bin_dir, bundle_dir = tmp_path / "bin", tmp_path / "bundle"
    bin_dir.mkdir(); bundle_dir.mkdir()
    for name, body in (("aws", AWS_STUB), ("cdk", CDK_STUB)):
        (bin_dir / name).write_text(body); (bin_dir / name).chmod(0o755)

    def _run(local_sum, remote_sum, **extra_env):
        if local_sum is not None:
            (bundle_dir / "spark-4.3.0-bundle.tar.gz.sha256").write_text(f"{local_sum}  spark-4.3.0-bundle.tar.gz\n")
        env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "VIRTUAL_ENV": "x",
               "BUNDLE_DIR": str(bundle_dir), "STUB_REMOTE_SUM": remote_sum, **extra_env}
        done = subprocess.run([str(DEPLOY), "--profile", "stub", "--stage", "cluster"], env=env,
                              capture_output=True, text=True)
        return done.returncode, done.stdout + done.stderr

    return _run


def test_matching_bundle_deploys(deploy):
    code, out = deploy("abc123", "abc123")
    assert code == 0 and "CDK deploy EmrSdpClusterStack" in out


def test_stale_bundle_in_s3_blocks_the_cluster_deploy(deploy):
    code, out = deploy("newhash", "oldhash")
    assert code != 0 and "CDK deploy" not in out
    assert "not your latest local build" in out and "build_bundle.sh" in out


def test_missing_remote_checksum_blocks_the_deploy(deploy):
    code, out = deploy("newhash", "")
    assert code != 0 and "CDK deploy" not in out


def test_override_allows_a_stale_bundle(deploy):
    code, out = deploy("newhash", "oldhash", ALLOW_STALE_BUNDLE="1")
    assert code == 0 and "CDK deploy EmrSdpClusterStack" in out


def test_no_local_build_skips_the_check(deploy, tmp_path):
    code, out = deploy(None, "oldhash")
    assert code == 0 and "CDK deploy EmrSdpClusterStack" in out
