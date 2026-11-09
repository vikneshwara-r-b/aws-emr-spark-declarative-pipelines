"""EMR runs bootstrap actions as the unprivileged `hadoop` user, so writes outside /tmp and the
home directory must go through sudo. A first cluster failed with "tar: spark-sdp: Cannot mkdir:
Permission denied" when the script assumed root."""
import re
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "bootstrap" / "install_spark.sh"
PRIVILEGED_TARGETS = ("/opt", "/etc/", "/usr/local/")


def _commands():
    for line in SCRIPT.read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            yield stripped


def test_every_command_touching_system_paths_uses_sudo():
    offenders = [
        cmd
        for cmd in _commands()
        if any(target in cmd for target in PRIVILEGED_TARGETS)
        and re.match(r"(tar|chmod|ln|tee|cp|mv|mkdir|rm)\b", cmd)
    ]
    assert offenders == []


def test_sudo_is_skipped_only_when_already_root():
    text = SCRIPT.read_text()
    assert '[[ "$(id -u)" -eq 0 ]] || SUDO="sudo"' in text


BOOTSTRAP_DIR = Path(__file__).resolve().parents[2] / "bootstrap"


def test_step_scripts_pin_spark_home_so_emr_spark_is_not_used():
    # EMR step environments set SPARK_HOME=/usr/lib/spark; without pinning, spark-submit runs
    # EMR's Spark 4.0.2 ("Running Spark version 4.0.2-amzn-0") and pyspark.pipelines is missing.
    for name in ("run_spark_script_step.sh", "run_pipeline_step.sh"):
        text = (BOOTSTRAP_DIR / name).read_text()
        assert "export SPARK_HOME=/opt/spark-sdp" in text, name
        if name == "run_pipeline_step.sh":
            # pipelines run with a private copy of the 4.3 conf (without spark.master), derived from it
            assert 'cp -R "${SPARK_HOME}/conf"' in text and 'export SPARK_CONF_DIR="${SDP_CONF_DIR}"' in text
        else:
            assert "export SPARK_CONF_DIR=/opt/spark-sdp/conf" in text, name
        assert "unset PYTHONPATH" in text, name
        assert "spark-sdp-" not in text.replace("# ", ""), name


def test_pipeline_step_refuses_to_run_on_the_wrong_spark_version():
    assert "version 4\\.3" in (BOOTSTRAP_DIR / "run_pipeline_step.sh").read_text()


def test_installed_wrappers_pin_spark_home():
    text = SCRIPT.read_text()
    assert "export SPARK_HOME=/opt/spark-sdp" in text
    assert "ln -sf" not in text


def test_pipeline_step_uses_the_bundled_python_not_the_system_one():
    # bin/spark-pipelines defaults PYSPARK_PYTHON to python3 (3.9 on AL2023), which cannot import PySpark 4.3.
    text = (BOOTSTRAP_DIR / "run_pipeline_step.sh").read_text()
    assert "export PYSPARK_PYTHON=/opt/spark-sdp/venv/bin/python" in text
    assert "export PYSPARK_PYTHON=/opt/spark-sdp/venv/bin/python" in SCRIPT.read_text()


def test_pipeline_conf_has_no_master_because_spark_pipelines_adds_remote_local():
    import subprocess

    defaults = Path(__file__).resolve().parents[2] / "docker" / "conf" / "spark-defaults.conf"
    assert "spark.master" in defaults.read_text()  # plain spark-submit steps still run on YARN
    sed_args = [a for a in re.findall(r"-e '([^']+)'", (BOOTSTRAP_DIR / "run_pipeline_step.sh").read_text())]
    assert sed_args, "the pipeline step must strip spark.master / spark.submit.deployMode"
    filtered = subprocess.run(
        ["sed", *[x for a in sed_args for x in ("-e", a.replace("\\.", "\\."))], str(defaults)],
        capture_output=True, text=True, check=True,
    ).stdout
    assert "spark.master" not in filtered and "spark.submit.deployMode" not in filtered
    assert "spark.hadoop.fs.s3.impl" in filtered


def test_pipeline_step_gives_the_local_connect_server_more_memory():
    assert "spark.driver.memory" in (BOOTSTRAP_DIR / "run_pipeline_step.sh").read_text()
