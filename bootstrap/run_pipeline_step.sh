#!/usr/bin/env bash
# Run as an EMR step (via command-runner.jar + bash) on the primary node.
# Usage: run_pipeline_step.sh <s3-pipeline-package-uri> <dry-run|run> [extra spark-pipelines args...]
#
# Pulls the rendered pipeline package from S3 and runs it with Spark 4.3's spark-pipelines
# CLI in YARN client mode. Extra args (e.g. --conf spark.f1.season.year=2024,
# --full-refresh-all) are passed through.
set -euo pipefail

PACKAGE_URI="${1:?package s3 uri}"
MODE="${2:?dry-run|run}"
shift 2

# `--conf spark.f1.*=value` arguments are NOT delivered to the pipeline flows: spark-submit applies --conf to
# its own process, while the flows read spark.conf from the Spark Connect session, which spark-pipelines builds
# only from the spec's `configuration:` block. So collect them here and write them into the spec below.
# Every other argument is passed through to spark-pipelines unchanged.
F1_CONF=()
PASSTHROUGH=()
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--conf" && "${2:-}" == spark.f1.* ]]; then
    F1_CONF+=("$2"); shift 2
  else
    PASSTHROUGH+=("$1"); shift
  fi
done
# dry-run still executes the flow bodies; tell the bronze flow not to hit the live API.
if [[ "${MODE}" == "dry-run" ]]; then F1_CONF+=("spark.f1.job.mode=VALIDATE"); fi

WORKDIR="$(mktemp -d /tmp/sdp-pipeline.XXXXXX)"
aws s3 sync "${PACKAGE_URI}" "${WORKDIR}" --only-show-errors
cd "${WORKDIR}"

# The EMR step environment points at EMR's own Spark (SPARK_HOME=/usr/lib/spark, its PYTHONPATH and conf
# dir), and spark-submit honours an existing SPARK_HOME -- so without this it silently runs EMR's
# Spark 4.0.2 instead of 4.3. Pin everything to the 4.3 install.
unset PYTHONPATH PYTHONHOME SPARK_CONF_DIR SPARK_DIST_CLASSPATH SPARK_EXTRA_CLASSPATH
export SPARK_HOME=/opt/spark-sdp

# bin/spark-pipelines runs "$PYSPARK_PYTHON" to locate its CLI before spark-env.sh is read, and falls back to
# the system python3 (3.9 on AL2023) which cannot import PySpark 4.3. Point it at the bundled Python 3.11.
export PYSPARK_PYTHON=/opt/spark-sdp/venv/bin/python

# spark-pipelines always adds `--remote local` (Spark Connect), and spark-submit rejects --remote together with
# a master or deploy mode -- including ones that come from spark-defaults.conf. So run with a private copy of the
# conf that has no spark.master / spark.submit.deployMode. The pipeline then runs in a local Spark Connect server
# on the primary node (the data volume here is small); give that JVM more memory than the 1g default.
SDP_CONF_DIR="${WORKDIR}/spark-conf"
cp -R "${SPARK_HOME}/conf" "${SDP_CONF_DIR}"
sed -e '/^spark\.master[[:space:]]/d' -e '/^spark\.submit\.deployMode[[:space:]]/d' \
  "${SPARK_HOME}/conf/spark-defaults.conf" > "${SDP_CONF_DIR}/spark-defaults.conf"
echo "spark.driver.memory                   4g" >> "${SDP_CONF_DIR}/spark-defaults.conf"
export SPARK_CONF_DIR="${SDP_CONF_DIR}"

version_line="$("${SPARK_HOME}/bin/spark-submit" --version 2>&1 | grep -m1 -i 'version')"
echo "Spark: ${version_line}"
if ! grep -q 'version 4\.3' <<<"${version_line}"; then
  echo "error: expected Spark 4.3 but got: ${version_line}" >&2
  exit 1
fi

# Write the collected settings into this run's copy of the spec (the package in S3 is not modified).
"${PYSPARK_PYTHON}" - spark-pipeline.yml "${F1_CONF[@]+"${F1_CONF[@]}"}" <<'PY'
import sys

import yaml

spec_path, settings = sys.argv[1], sys.argv[2:]
with open(spec_path) as f:
    spec = yaml.safe_load(f)
configuration = spec.setdefault("configuration", {})
for setting in settings:
    key, value = setting.split("=", 1)
    configuration[key] = str(value)
with open(spec_path, "w") as f:
    yaml.safe_dump(spec, f, sort_keys=False)
print("Run settings added to the pipeline spec:", ", ".join(settings) or "(none)")
PY

if [[ "${MODE}" == "dry-run" ]]; then
  exec "${SPARK_HOME}/bin/spark-pipelines" dry-run --spec spark-pipeline.yml "${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}"
fi
exec "${SPARK_HOME}/bin/spark-pipelines" run --spec spark-pipeline.yml "${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}"
