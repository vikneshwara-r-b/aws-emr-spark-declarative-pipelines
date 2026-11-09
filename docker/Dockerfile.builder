# Builder image: produces the Spark 4.3 bundle that the EMR bootstrap action installs.
# Based on Amazon Linux 2023 so the Python venv it builds matches the EMR nodes
# (emr-spark-8.0.0 runs AL2023 with Python 3.11). The image is pushed to the ECR repo
# that the platform stack creates; run it with a volume on /out to get the bundle.
FROM public.ecr.aws/amazonlinux/amazonlinux:2023

# java: only for the build-time linkage smoke test (the EMR nodes have their own Java 17)
RUN dnf install -y python3.11 python3.11-pip tar gzip findutils which java-17-amazon-corretto-headless \
    && dnf clean all

WORKDIR /opt/builder
COPY docker/build_bundle.sh docker/requirements-spark.txt docker/smoke_iceberg.py ./
COPY docker/conf ./conf
RUN chmod +x build_bundle.sh

# Spark 4.3 is used for Auto CDC SCD Type 2. Iceberg has no Spark 4.3 runtime yet (no iceberg-spark-runtime-4.3
# on Maven Central or the Apache snapshots repo; PR apache/iceberg#18749 is open), and no released 4.2 runtime
# either (1.12.0 only ships 4.0/4.1; the 4.1 one fails on Spark 4.2 with IncompatibleClassChangeError).
# So this bundle pairs Spark 4.3 with the Spark 4.2 snapshot runtime. Apache publishes it as a snapshot only,
# so pin one exact timestamped build: snapshots are mutable and eventually purged. build_bundle.sh runs a
# linkage smoke test (Iceberg table, MERGE INTO, view) to catch an incompatibility before anything is uploaded.
# When a runtime built for Spark 4.3 exists, set ICEBERG_SPARK_SERIES=4.3 and its ICEBERG_VERSION.
ENV SPARK_VERSION=4.3.0 \
    ICEBERG_VERSION=1.13.0-20261108.001937-52 \
    ICEBERG_SPARK_SERIES=4.2 \
    ICEBERG_REPO=https://repository.apache.org/content/repositories/snapshots \
    INSTALL_DIR=/opt/spark-sdp \
    OUT_DIR=/out

ENTRYPOINT ["/opt/builder/build_bundle.sh"]
