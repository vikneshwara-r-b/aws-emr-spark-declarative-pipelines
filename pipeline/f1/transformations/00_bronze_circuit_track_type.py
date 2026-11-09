# f1_pipeline_lib is made importable by _sys_path_bootstrap.py -- see
# 01_bronze_raw.py for the full rationale.
from f1_pipeline_lib.schema import CIRCUIT_TRACK_TYPE_SCHEMA

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, TimestampType

# spark.-prefixed per this codebase's established convention -- Spark's
# --conf parser silently drops non-"spark."-prefixed keys (SPARK-7037).
REFERENCE_PATH_CONF = "spark.f1.reference.circuit_track_type.path"

# Explicit output schema, matching the pattern used for the raw_* tables
# (01_bronze_raw.py): without it, the dependency analyzer has nothing to
# resolve this table against on a brand-new deployment with zero prior
# successful RUNs, and raises [TABLE_OR_VIEW_NOT_FOUND].
_CIRCUIT_TRACK_TYPE_OUTPUT_SCHEMA = StructType(
    list(CIRCUIT_TRACK_TYPE_SCHEMA.fields)
    + [StructField("refreshed_at", TimestampType(), False)]
)

# Second, independent, non-API data source: a static circuit track-type
# (street/permanent/hybrid) reference CSV, deployed declaratively to S3 by the
# uploaded to S3 by scripts/run_pipeline.sh --upload-reference-data (not fetched or
# staged at run time, and not part of the pipeline package).
# Ingested as a streaming table (not a materialized view) to demonstrate
# genuine incremental processing: SDP's checkpoint only reads new files added
# to this directory since the last run. The directory must already hold at least
# one CSV before the first run (run_pipeline.sh --upload-reference-data initial),
# otherwise the streaming read fails with [PATH_NOT_FOUND].
dp.create_streaming_table(
    "circuit_track_type",
    comment=(
        "Incremental circuit track-type reference ingestion (street / "
        "permanent / hybrid) from flat CSV files in S3. Independent, "
        "non-API source -- proves SDP auto-infers a separate DAG branch "
        "purely from the table reference used in 03_curated.sql, with no "
        "orchestration config anywhere."
    ),
    schema=_CIRCUIT_TRACK_TYPE_OUTPUT_SCHEMA,
)


@dp.append_flow(target="circuit_track_type")
def ingest_circuit_track_type():
    spark = SparkSession.active()
    reference_dir = spark.conf.get(REFERENCE_PATH_CONF)  # required -- set in spark-pipeline.yml
    parsed = (
        spark.readStream.schema(CIRCUIT_TRACK_TYPE_SCHEMA)
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .format("csv")
        .load(reference_dir)
    )
    return parsed.select("*", F.current_timestamp().alias("refreshed_at"))
