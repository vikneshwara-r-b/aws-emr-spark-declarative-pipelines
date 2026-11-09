"""Runs bootstrap/run_pipeline_step.sh end to end against a stand-in Spark install and a stub `aws`.

The point: spark.f1.* run settings must end up in the spec's `configuration:` block (the only place the
pipeline flows can read them), not only on the spark-submit command line."""
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from scripts.package_pipeline import render_package

REPO = Path(__file__).resolve().parents[2]
STEP_SCRIPT = REPO / "bootstrap" / "run_pipeline_step.sh"

SPARK_SUBMIT = "#!/usr/bin/env bash\necho 'Welcome ... version 4.3.0'\n"
SPARK_PIPELINES = """#!/usr/bin/env bash
echo "SPARK_PIPELINES_ARGS: $*"
echo "SPARK_CONF_DIR=$SPARK_CONF_DIR"
echo "--- spark-defaults.conf used:"; cat "$SPARK_CONF_DIR/spark-defaults.conf"
echo "--- spec used:"; cat spark-pipeline.yml
"""
AWS_STUB = """#!/usr/bin/env bash
# only `aws s3 sync <src> <dst>` is used by the step script: serve the prepared package
if [[ "$1 $2" == "s3 sync" ]]; then cp -R "$STUB_PACKAGE"/. "$4"/; fi
"""


@pytest.fixture
def run_step(tmp_path):
    package = render_package(
        bucket="b", prefix="p", database="d", api_base_url="https://api.jolpi.ca/ergast/f1",
        src_dir=REPO / "pipeline" / "f1", out_dir=tmp_path / "package",
    )
    spark_home = tmp_path / "spark"
    (spark_home / "bin").mkdir(parents=True)
    (spark_home / "conf").mkdir()
    (spark_home / "conf" / "spark-defaults.conf").write_text((REPO / "docker/conf/spark-defaults.conf").read_text())
    for name, body in (("spark-submit", SPARK_SUBMIT), ("spark-pipelines", SPARK_PIPELINES)):
        (spark_home / "bin" / name).write_text(body)
        (spark_home / "bin" / name).chmod(0o755)
    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    (stub_bin / "aws").write_text(AWS_STUB)
    (stub_bin / "aws").chmod(0o755)

    script = tmp_path / "step.sh"
    text = STEP_SCRIPT.read_text()
    text = text.replace("/opt/spark-sdp/venv/bin/python", sys.executable).replace("/opt/spark-sdp", str(spark_home))
    text = text.replace("mktemp -d /tmp/sdp-pipeline", f"mktemp -d {tmp_path}/sdp-pipeline")
    script.write_text(text)

    def _run(mode, *extra):
        env = {**os.environ, "PATH": f"{stub_bin}:{os.environ['PATH']}", "STUB_PACKAGE": str(package)}
        done = subprocess.run(["bash", str(script), "s3://b/p/package", mode, *extra], env=env,
                              capture_output=True, text=True)
        out = done.stdout + done.stderr
        spec_text = out.split("--- spec used:", 1)[1] if "--- spec used:" in out else ""
        spec = yaml.safe_load(spec_text) if spec_text else {}
        return done.returncode, out, spec

    return _run


def test_dry_run_puts_validate_mode_in_the_spec_not_on_the_command_line(run_step):
    code, out, spec = run_step("dry-run")
    assert code == 0, out
    assert spec["configuration"]["spark.f1.job.mode"] == "VALIDATE"
    assert "SPARK_PIPELINES_ARGS: dry-run --spec spark-pipeline.yml" in out
    assert "--conf" not in out.split("SPARK_PIPELINES_ARGS:")[1].splitlines()[0]


def test_run_puts_season_and_round_in_the_spec_as_strings(run_step):
    code, out, spec = run_step("run", "--conf", "spark.f1.season.year=2024", "--conf", "spark.f1.season.round=3")
    assert code == 0, out
    assert spec["configuration"]["spark.f1.season.year"] == "2024"
    assert spec["configuration"]["spark.f1.season.round"] == "3"
    assert "spark.f1.job.mode" not in spec["configuration"]
    assert "SPARK_PIPELINES_ARGS: run --spec spark-pipeline.yml" in out


def test_existing_manifest_settings_survive_the_rewrite(run_step):
    _, _, spec = run_step("run", "--conf", "spark.f1.season.year=2024", "--conf", "spark.f1.season.round=1")
    assert spec["configuration"]["spark.f1.api.base_url"] == "https://api.jolpi.ca/ergast/f1"
    assert spec["configuration"]["spark.sql.catalog.glue_catalog"].endswith("SparkCatalog")
    assert spec["catalog"] == "glue_catalog" and spec["database"] == "d"
    assert [lib["glob"]["include"] for lib in spec["libraries"]] == ["_sys_path_bootstrap.py", "transformations/**"]


def test_other_arguments_are_passed_through_to_spark_pipelines(run_step):
    code, out, _ = run_step("run", "--conf", "spark.f1.season.year=2024", "--full-refresh-all",
                            "--conf", "spark.sql.shuffle.partitions=8")
    assert code == 0, out
    args_line = out.split("SPARK_PIPELINES_ARGS:")[1].splitlines()[0]
    assert "--full-refresh-all" in args_line and "--conf spark.sql.shuffle.partitions=8" in args_line
    assert "spark.f1.season.year" not in args_line


def test_pipeline_runs_with_a_conf_that_has_no_master(run_step):
    _, out, _ = run_step("run", "--conf", "spark.f1.season.year=2024")
    used = out.split("--- spark-defaults.conf used:")[1].split("--- spec used:")[0]
    assert "spark.master" not in used and "spark.submit.deployMode" not in used
    assert "spark.driver.memory" in used
