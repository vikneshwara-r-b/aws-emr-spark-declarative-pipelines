"""Confirms the cluster really runs Spark 4.3 (not EMR's stock 4.0.x), Java 17+, Python 3.11,
that the Iceberg Glue catalog is reachable, and that the Auto CDC API is present.

Runs as an EMR step: scripts/run_spark_script.sh validation/env_check.py --bucket B --prefix P --database D
Writes s3://<bucket>/<prefix>/validation/results/env.json
"""
import argparse
import inspect
import json
import sys

import boto3
from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("env-check")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args.bucket}/{args.prefix}/warehouse")
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true")
        .getOrCreate()
    )
    from pyspark import pipelines as dp

    namespaces = [r[0] for r in spark.sql("SHOW NAMESPACES IN glue_catalog").collect()]
    result = {
        "spark_version": spark.version,
        "java_version": spark._jvm.System.getProperty("java.version"),
        "python_version": sys.version.split()[0],
        "master": spark.sparkContext.master,
        "glue_catalog_namespaces": namespaces,
        "database_visible": args.database in namespaces,
        "auto_cdc_signature": str(inspect.signature(dp.create_auto_cdc_flow)),
    }
    print(json.dumps(result, indent=2))
    boto3.client("s3").put_object(
        Bucket=args.bucket,
        Key=f"{args.prefix}/validation/results/env.json",
        Body=json.dumps(result).encode(),
    )
    spark.stop()


if __name__ == "__main__":
    main()
