#!/usr/bin/env bash
# EMR bootstrap action. Runs on every node, before applications are installed, as the
# unprivileged `hadoop` user (not root) -- so everything that writes outside /tmp and the
# home directory goes through sudo (hadoop has passwordless sudo on EMR).
# Usage: install_spark.sh s3://bucket/path/spark-<version>-bundle.tar.gz
#
# Unpacks the Spark 4.3 bundle to /opt/spark-sdp alongside (not over) EMR's own Spark, so
# EMR's daemons keep working and the stock Spark stays available for comparison.
set -euo pipefail

BUNDLE_URI="${1:?usage: install_spark.sh <s3-uri-of-bundle>}"
TMP_TARBALL=/tmp/spark-bundle.tar.gz

# Empty when already root (e.g. when testing the script by hand as root).
SUDO=""
[[ "$(id -u)" -eq 0 ]] || SUDO="sudo"

token="$(curl -fsS -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')"
region="$(curl -fsS -H "X-aws-ec2-metadata-token: ${token}" http://169.254.169.254/latest/meta-data/placement/region)"

echo "Downloading ${BUNDLE_URI}"
aws s3 cp "${BUNDLE_URI}" "${TMP_TARBALL}" --region "${region}" --only-show-errors
$SUDO tar -xzf "${TMP_TARBALL}" -C /opt
rm -f "${TMP_TARBALL}"
$SUDO chmod -R a+rX /opt/spark-sdp

# Convenience environment for interactive shells and EMR steps.
$SUDO tee /etc/profile.d/spark-sdp.sh >/dev/null <<'PROFILE'
export SPARK_SDP_HOME=/opt/spark-sdp
export PATH="$SPARK_SDP_HOME/bin:$PATH"
PROFILE
$SUDO chmod 644 /etc/profile.d/spark-sdp.sh

# Explicit entry points that don't collide with EMR's Spark on PATH. They are wrappers, not plain
# symlinks: EMR's own environment sets SPARK_HOME/PYTHONPATH to its Spark 4.0.x, which spark-submit
# would otherwise honour, so each wrapper pins everything to the 4.3 install first.
for tool in submit sql pipelines; do
  $SUDO tee "/usr/local/bin/spark-sdp-${tool}" >/dev/null <<WRAPPER
#!/usr/bin/env bash
unset PYTHONPATH PYTHONHOME SPARK_CONF_DIR SPARK_DIST_CLASSPATH SPARK_EXTRA_CLASSPATH
export SPARK_HOME=/opt/spark-sdp
export SPARK_CONF_DIR=/opt/spark-sdp/conf
export PYSPARK_PYTHON=/opt/spark-sdp/venv/bin/python
exec /opt/spark-sdp/bin/spark-${tool} "\$@"
WRAPPER
  $SUDO chmod 755 "/usr/local/bin/spark-sdp-${tool}"
done

echo "Spark 4.3 installed at /opt/spark-sdp"
