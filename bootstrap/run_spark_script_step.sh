#!/usr/bin/env bash
# EMR step helper: runs a PySpark script from S3 with Spark 4.3.
# Usage: run_spark_script_step.sh <s3-script-uri> [script args...]
set -euo pipefail

SCRIPT_URI="${1:?script s3 uri}"
shift
LOCAL="/tmp/$(basename "${SCRIPT_URI}")"
aws s3 cp "${SCRIPT_URI}" "${LOCAL}" --only-show-errors
# The EMR step environment points at EMR's own Spark (SPARK_HOME=/usr/lib/spark, its PYTHONPATH and conf
# dir), and spark-submit honours an existing SPARK_HOME -- so without this it silently runs EMR's
# Spark 4.0.2 instead of 4.3. Pin everything to the 4.3 install.
unset PYTHONPATH PYTHONHOME SPARK_CONF_DIR SPARK_DIST_CLASSPATH SPARK_EXTRA_CLASSPATH
export SPARK_HOME=/opt/spark-sdp
export SPARK_CONF_DIR=/opt/spark-sdp/conf
exec "${SPARK_HOME}/bin/spark-submit" "${LOCAL}" "$@"
