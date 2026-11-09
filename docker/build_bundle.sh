#!/usr/bin/env bash
# Runs INSIDE the builder image. Produces $OUT_DIR/spark-$SPARK_VERSION-bundle.tar.gz:
#   spark-sdp/           Hadoop-free Spark distribution (bin/, jars/, python/ ...)
#     jars/iceberg-*.jar  Iceberg runtime + AWS bundle
#     venv/               Python 3.11 venv with the SDP / Spark Connect dependencies
#     conf/               spark-env.sh + spark-defaults.conf for running on EMR YARN
set -euo pipefail

: "${SPARK_VERSION:?}" "${ICEBERG_VERSION:?}" "${ICEBERG_SPARK_SERIES:?}" "${INSTALL_DIR:?}" "${OUT_DIR:?}"
ICEBERG_REPO="${ICEBERG_REPO:-https://repo1.maven.org/maven2}"
APACHE_BASE="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}"
CDN_BASE="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}"
TARBALL="spark-${SPARK_VERSION}-bin-without-hadoop.tgz"
MAVEN="${ICEBERG_REPO:-https://repo1.maven.org/maven2}/org/apache/iceberg"
WORK="$(mktemp -d)"

# -# prints a progress bar (the 470 MB tarball otherwise looks hung); retries cover flaky mirrors.
fetch() { curl -fL --retry 5 --retry-delay 5 --connect-timeout 20 -# -o "$2" "$1"; }

echo "==> Downloading ${TARBALL} (~470 MB)"
# The CDN is fast but only keeps current releases; archive.apache.org keeps all but is throttled.
fetch "${CDN_BASE}/${TARBALL}" "${WORK}/${TARBALL}" || {
  echo "CDN download failed, falling back to archive.apache.org"
  fetch "${APACHE_BASE}/${TARBALL}" "${WORK}/${TARBALL}"
}
echo "==> Verifying checksum (taken from archive.apache.org)"
fetch "${APACHE_BASE}/${TARBALL}.sha512" "${WORK}/${TARBALL}.sha512"
(cd "${WORK}" && sha512sum -c "${TARBALL}.sha512")

echo "==> Installing Spark to ${INSTALL_DIR}"
rm -rf "${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"
tar -xzf "${WORK}/${TARBALL}" -C "${INSTALL_DIR}" --strip-components=1

echo "==> Adding Iceberg ${ICEBERG_VERSION} (spark ${ICEBERG_SPARK_SERIES} runtime) jars from ${ICEBERG_REPO}"
# A timestamped snapshot such as 1.13.0-20261108.001937-52 lives in the 1.13.0-SNAPSHOT directory.
if [[ "${ICEBERG_VERSION}" =~ ^(.+)-[0-9]{8}\.[0-9]{6}-[0-9]+$ ]]; then
  ICEBERG_DIR="${BASH_REMATCH[1]}-SNAPSHOT"
else
  ICEBERG_DIR="${ICEBERG_VERSION}"
fi
for artifact in "iceberg-spark-runtime-${ICEBERG_SPARK_SERIES}_2.13" "iceberg-aws-bundle"; do
  jar="${artifact}-${ICEBERG_VERSION}.jar"
  url="${MAVEN}/${artifact}/${ICEBERG_DIR}/${jar}"
  fetch "${url}" "${INSTALL_DIR}/jars/${jar}"
  fetch "${url}.sha1" "${WORK}/${jar}.sha1"
  echo "$(cut -d' ' -f1 "${WORK}/${jar}.sha1")  ${INSTALL_DIR}/jars/${jar}" | sha1sum -c -
done

echo "==> Building Python venv"
python3.11 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --no-cache-dir --upgrade pip
"${INSTALL_DIR}/venv/bin/pip" install --no-cache-dir -r /opt/builder/requirements-spark.txt
# Make the PySpark that ships in the Spark tarball importable (also inside YARN executors).
SITE="$("${INSTALL_DIR}/venv/bin/python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
PY4J_ZIP="$(ls "${INSTALL_DIR}"/python/lib/py4j-*-src.zip)"
printf '%s\n%s\n' "${INSTALL_DIR}/python" "${PY4J_ZIP}" > "${SITE}/spark-sdp.pth"
"${INSTALL_DIR}/venv/bin/pip" freeze > "${INSTALL_DIR}/requirements.lock"

echo "==> Installing conf"
cp /opt/builder/conf/spark-env.sh /opt/builder/conf/spark-defaults.conf "${INSTALL_DIR}/conf/"

echo "==> Smoke test (no JVM): PySpark version and Auto CDC API"
"${INSTALL_DIR}/venv/bin/python" - <<PY
import inspect
import pyspark
from pyspark import pipelines as dp
assert pyspark.__version__.startswith("${SPARK_VERSION%.*}"), pyspark.__version__
print("pyspark", pyspark.__version__)
print("create_auto_cdc_flow", inspect.signature(dp.create_auto_cdc_flow))
PY
ls "${INSTALL_DIR}"/jars/spark-connect_2.13-*.jar "${INSTALL_DIR}/bin/spark-pipelines"

# The Iceberg runtime in this bundle was built for an older Spark minor than the Spark in it (see the
# Dockerfile). Prove they link before the 700 MB bundle is packaged and uploaded. The Hadoop-free Spark needs
# Hadoop classes to start, so the client jars are downloaded to the scratch dir only (the EMR nodes have their own).
# Set SKIP_JVM_SMOKE=1 to bypass if this check itself misbehaves.
if [[ -z "${SKIP_JVM_SMOKE:-}" ]]; then
  echo "==> Smoke test (JVM): Iceberg runtime links against Spark ${SPARK_VERSION}"
  for jar in hadoop-client-api hadoop-client-runtime; do
    fetch "https://repo1.maven.org/maven2/org/apache/hadoop/${jar}/3.4.2/${jar}-3.4.2.jar" "${WORK}/${jar}.jar"
  done
  # slf4j-api is also missing from a Hadoop-free Spark (on a node it comes from `hadoop classpath`); same version as the
  # slf4j bridges that ship in Spark's jars/ (jul-to-slf4j-2.0.17, log4j-slf4j2-impl).
  fetch "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar" "${WORK}/slf4j-api.jar"
  SPARK_HOME="${INSTALL_DIR}" SPARK_CONF_DIR="${WORK}/no-conf" \
    SPARK_DIST_CLASSPATH="${WORK}/hadoop-client-api.jar:${WORK}/hadoop-client-runtime.jar:${WORK}/slf4j-api.jar" \
    PYSPARK_PYTHON="${INSTALL_DIR}/venv/bin/python" \
    "${INSTALL_DIR}/venv/bin/python" /opt/builder/smoke_iceberg.py "${WORK}/iceberg-warehouse"
fi

echo "==> Packaging"
mkdir -p "${OUT_DIR}"
BUNDLE="${OUT_DIR}/spark-${SPARK_VERSION}-bundle.tar.gz"
tar -czf "${BUNDLE}" -C "$(dirname "${INSTALL_DIR}")" "$(basename "${INSTALL_DIR}")"
(cd "${OUT_DIR}" && sha256sum "$(basename "${BUNDLE}")" > "$(basename "${BUNDLE}").sha256")
ls -lh "${BUNDLE}"
