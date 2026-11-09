"""scripts/run_pipeline.sh --upload-reference-data: initial / incremental / all, run against a stub `aws`."""
import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "run_pipeline.sh"
AWS_STUB = """#!/usr/bin/env bash
echo "aws $*" >> "$STUB_LOG"
case "$*" in
  *"sts get-caller-identity"*) echo 111111111111 ;;
  *"cloudformation describe-stacks"*) echo stub-value ;;
  *"emr add-steps"*) echo s-STUB ;;
  *"emr describe-step"*) echo COMPLETED ;;
esac
"""
DST = "s3://stub-value/f1-pipeline/reference-data/circuit_track_type"


@pytest.fixture
def run(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "aws").write_text(AWS_STUB)
    (bin_dir / "aws").chmod(0o755)
    log = tmp_path / "aws.log"

    def _run(*args):
        env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STUB_LOG": str(log)}
        done = subprocess.run(
            [str(SCRIPT), "--profile", "stub", "--mode", "dry-run", *args], env=env, capture_output=True, text=True
        )
        calls = log.read_text().splitlines() if log.exists() else []
        return done.returncode, [c for c in calls if "reference" in c or "csv" in c], done.stdout + done.stderr

    return _run


def test_initial_uploads_only_the_initial_file(run):
    code, calls, _ = run("--upload-reference-data", "initial")
    assert code == 0
    assert len(calls) == 1 and calls[0].startswith("aws s3 cp ") and calls[0].endswith(f"initial_load.csv {DST}/initial_load.csv --only-show-errors")


def test_incremental_uploads_only_the_incremental_file(run):
    code, calls, _ = run("--upload-reference-data", "incremental")
    assert code == 0
    assert len(calls) == 1 and "incremental_load.csv" in calls[0] and "initial_load.csv" not in calls[0]


@pytest.mark.parametrize("args", [("--upload-reference-data", "all"), ("--upload-reference-data",)])
def test_all_and_the_bare_flag_sync_the_folder(run, args):
    code, calls, _ = run(*args)
    assert code == 0 and len(calls) == 1 and calls[0].startswith("aws s3 sync "), args


def test_no_flag_uploads_nothing(run):
    code, calls, _ = run()
    assert code == 0 and calls == []


def test_flag_before_other_options_does_not_swallow_them(run):
    code, calls, output = run("--upload-reference-data", "--wait")
    assert code == 0 and len(calls) == 1 and calls[0].startswith("aws s3 sync ")
