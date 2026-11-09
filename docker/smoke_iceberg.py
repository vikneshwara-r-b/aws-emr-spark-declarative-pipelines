"""Linkage smoke test run by build_bundle.sh inside the builder image, before the bundle is packaged.

The bundle pairs Spark 4.3 with an Iceberg runtime built for Spark 4.2 (no 4.3 runtime exists yet). An
incompatible pairing fails the moment the catalog class loads (Iceberg 4.1 on Spark 4.2 threw
IncompatibleClassChangeError: SparkView can not implement View) or on the first row-level operation, so
one local table with MERGE INTO and DELETE, on a Hadoop catalog (no AWS), finds it in minutes and for free.

Usage: smoke_iceberg.py <warehouse-dir>
"""
import sys

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[1]")
    .appName("iceberg-linkage-smoke")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.catalog.t", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.t.type", "hadoop")
    .config("spark.sql.catalog.t.warehouse", sys.argv[1])
    .getOrCreate()
)
print("spark", spark.version)
spark.sql("CREATE NAMESPACE t.db")
spark.sql("CREATE TABLE t.db.x (id INT, v STRING) USING iceberg")
spark.sql("INSERT INTO t.db.x VALUES (1, 'a'), (2, 'b'), (4, 'd')")
spark.sql(
    "MERGE INTO t.db.x USING (SELECT 2 AS id, 'B' AS v UNION ALL SELECT 3, 'c') s ON x.id = s.id "
    "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT *"
)
spark.sql("DELETE FROM t.db.x WHERE id = 4")
rows = sorted((r.id, r.v) for r in spark.table("t.db.x").collect())
assert rows == [(1, "a"), (2, "B"), (3, "c")], rows
print("iceberg runtime links against this Spark: MERGE INTO and DELETE ok", rows)
spark.stop()
