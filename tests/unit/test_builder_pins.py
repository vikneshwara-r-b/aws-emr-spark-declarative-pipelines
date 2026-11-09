"""The bundle must use an Iceberg runtime built for Spark 4.2 or newer: the 4.1 runtime fails on Spark 4.2 with
IncompatibleClassChangeError (org.apache.iceberg.spark.SparkView can not implement ...catalog.View). Spark 4.3 has
no Iceberg runtime of its own yet, so it runs the 4.2 one; build_bundle.sh smoke-tests that pairing."""
import re
from pathlib import Path

DOCKER = Path(__file__).resolve().parents[2] / "docker"


def _minor(version):
    return tuple(int(p) for p in version.split("."))


def test_iceberg_runtime_is_not_older_than_spark_4_2_and_not_newer_than_spark():
    dockerfile = (DOCKER / "Dockerfile.builder").read_text()
    spark = re.search(r"SPARK_VERSION=(\d+\.\d+)\.", dockerfile).group(1)
    series = re.search(r"ICEBERG_SPARK_SERIES=(\S+)", dockerfile).group(1)
    assert (4, 2) <= _minor(series) <= _minor(spark)


def test_a_runtime_older_than_spark_is_smoke_tested_in_the_builder():
    dockerfile = (DOCKER / "Dockerfile.builder").read_text()
    script = (DOCKER / "build_bundle.sh").read_text()
    spark = re.search(r"SPARK_VERSION=(\d+\.\d+)\.", dockerfile).group(1)
    series = re.search(r"ICEBERG_SPARK_SERIES=(\S+)", dockerfile).group(1)
    if series != spark:
        assert "smoke_iceberg.py" in script and "smoke_iceberg.py" in dockerfile
        assert "MERGE INTO" in (DOCKER / "smoke_iceberg.py").read_text()


def test_snapshot_builds_are_pinned_to_one_timestamped_version():
    dockerfile = (DOCKER / "Dockerfile.builder").read_text()
    version = re.search(r"ICEBERG_VERSION=(\S+)", dockerfile).group(1)
    if "snapshots" in dockerfile:
        assert re.fullmatch(r".+-\d{8}\.\d{6}-\d+", version), version


def test_bundle_script_verifies_the_iceberg_jars():
    script = (DOCKER / "build_bundle.sh").read_text()
    assert "sha1sum -c" in script
    assert "-SNAPSHOT" in script
