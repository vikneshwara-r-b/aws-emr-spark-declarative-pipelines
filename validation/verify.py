"""Data checks on the curated (Auto CDC) tables after a pipeline run.

Two kinds of table. The facts are SCD1 (one row per key). dim_drivers, dim_teams and
dim_circuits are SCD2 (one row per key *version*, bounded by __START_AT / __END_AT; the current version
has __END_AT NULL). For SCD2 tables "rows" counts current versions, "history_rows" all versions, and
"duplicate_keys" counts keys that do not have exactly one current version.

For each curated table it records: row count, duplicate-key count (exactly one row / one current version
per key), a content hash (of the current rows), and a parity count against a plain "latest row per key" computed over
the prepared tables with window functions -- i.e. what Auto CDC should have produced. Parity compares
every column the two sides share. refreshed_at (the bronze load time, which a re-run legitimately moves; no curated table keeps it) is never compared. The
content hash also ignores Auto CDC's hidden __spark_autocdc_metadata column, so a re-run of the same race
must leave it unchanged.

It also checks, for SCD1 tables, that each row's __spark_autocdc_metadata.upsertSequence
equals the sequence value of the latest source row for its key. For SCD2 tables it checks the version chain instead: versions of a
key are contiguous (each __END_AT equals the next __START_AT, only the last is open) and the first version
starts at the earliest source sequence.

It also records a few data-sanity numbers (typed values parsed from the API strings).

Runs as an EMR step:
  scripts/run_spark_script.sh validation/verify.py --label r1 --bucket B --prefix P --database D
Writes s3://<bucket>/<prefix>/validation/results/<label>.json
"""
import argparse
import json

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

AUDIT_COLUMNS = {"refreshed_at"}

# curated table -> (prepared source table, key columns, ordering of "latest" within a key)
EXPECTED = {
    "dim_drivers": ("race_drivers", ["driver_Id"], ["race_Date", "refreshed_at"]),
    "dim_teams": ("race_teams", ["team_id"], ["race_Date", "refreshed_at"]),
    "fact_race_results": ("race_results_unnested", ["race_Id", "driver_Id"], ["refreshed_at"]),
    "fact_qualifying": ("race_qualifying_unnested", ["race_Id", "driver_Id"], ["refreshed_at"]),
    "fact_pit_stops": ("race_pit_stops", ["race_Id", "driver_Id", "stop"], ["refreshed_at"]),
}
KEYS = {**{t: k for t, (_, k, _) in EXPECTED.items()}, "dim_circuits": ["circuit_Id"]}
# curated table -> column the flow sequences by (its value must end up in __spark_autocdc_metadata.upsertSequence)
SEQUENCE_BY = {
    "dim_drivers": "race_Date",
    "dim_teams": "race_Date",
    "dim_circuits": "race_Date",
    "fact_race_results": "refreshed_at",
    "fact_qualifying": "refreshed_at",
    "fact_pit_stops": "refreshed_at",
}
# SCD2 dimension -> prepared table holding its source events (sequenced by SEQUENCE_BY)
SCD2_SOURCES = {"dim_drivers": "race_drivers", "dim_teams": "race_teams", "dim_circuits": "race_circuits"}
START, END = "__START_AT", "__END_AT"
GOLD_TABLES = [
    "driver_performance_summary",
    "team_performance_summary",
    "circuit_summary",
    "qualifying_vs_race_summary",
    "pit_stop_summary",
]


def latest_per_key(df: DataFrame, keys: list[str], order: list[str]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc() for c in order])
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def expected_dim_circuits(spark: SparkSession, db: str) -> DataFrame:
    """Latest circuit row, enriched with the latest track_type (the stream-static join in 03_curated.py)."""
    circuits = latest_per_key(spark.table(f"{db}.race_circuits"), ["circuit_Id"], ["race_Date", "refreshed_at"])
    track_types = latest_per_key(spark.table(f"{db}.circuit_track_type"), ["circuit_Id"], ["refreshed_at"])
    return circuits.join(
        track_types.select(F.col("circuit_Id").alias("tt_circuit_Id"), "track_type"),
        circuits["circuit_Id"] == F.col("tt_circuit_Id"),
        "left",
    ).drop("tt_circuit_Id")


def content_columns(df: DataFrame) -> list[str]:
    return sorted(c for c in df.columns if c not in AUDIT_COLUMNS and not c.startswith("__"))


def content_hash(df: DataFrame) -> str:
    row = df.select(F.sum(F.xxhash64(*[F.col(c) for c in content_columns(df)]).cast("decimal(38,0)"))).first()
    return str(row[0])


def parity_mismatches(actual: DataFrame, expected: DataFrame) -> int:
    columns = [c for c in content_columns(expected) if c in actual.columns]
    a, e = actual.select(*columns), expected.select(*columns)
    return a.exceptAll(e).count() + e.exceptAll(a).count()


def upsert_sequence_mismatches(actual: DataFrame, expected: DataFrame, keys: list[str], sequence_by: str) -> int:
    """Keys whose __spark_autocdc_metadata.upsertSequence differs from the sequence value of the latest source row
    (both cast to timestamp, as the dimensions sequence by a date and the facts by a timestamp)."""
    e = expected.select(*keys, F.col(sequence_by).cast("timestamp").alias("seq"))
    a = actual.select(*keys, F.col("__spark_autocdc_metadata.upsertSequence").cast("timestamp").alias("seq"))
    return a.exceptAll(e).count() + e.exceptAll(a).count()


def scd2_stats(actual: DataFrame, source: DataFrame, keys: list[str], sequence_by: str) -> dict:
    """Version-chain checks for an SCD2 table (see the module docstring)."""
    open_versions = F.sum(F.when(F.col(END).isNull(), 1).otherwise(0))
    keys_without_one_current = actual.groupBy(*keys).agg(open_versions.alias("n")).where("n != 1").count()

    window = Window.partitionBy(*keys).orderBy(F.col(START))
    chain = actual.select(*keys, START, END, F.lead(F.col(START)).over(window).alias("next_start"))
    closed_but_broken = F.col(END).isNotNull() & (
        F.col("next_start").isNull() | (F.col(END) != F.col("next_start")) | (F.col(START) >= F.col(END))
    )
    open_but_not_last = F.col(END).isNull() & F.col("next_start").isNotNull()
    broken_links = chain.where(closed_but_broken | open_but_not_last).count()

    first_start = actual.groupBy(*keys).agg(F.min(F.col(START).cast("timestamp")).alias("seq"))
    first_event = source.groupBy(*keys).agg(F.min(F.col(sequence_by).cast("timestamp")).alias("seq"))
    return {
        "history_rows": actual.count(),
        "duplicate_keys": keys_without_one_current,
        "broken_version_links": broken_links,
        "first_start_mismatch_keys": first_start.exceptAll(first_event).count() + first_event.exceptAll(first_start).count(),
    }


def scd2_history(spark: SparkSession, db: str, limit: int = 40) -> dict:
    """Entities with more than one version, per SCD2 dimension: for dim_drivers the driver's team over time.
    (Only dim_drivers carries an attribute that really changes in the Jolpica history: transfers between teams.)"""
    table, key, attribute = "dim_drivers", "driver_Id", "team_id"
    frame = spark.table(f"{db}.{table}")
    multi = frame.groupBy(key).count().where("count > 1").select(key)
    rows = (
        frame.join(multi, key)
        .select(key, attribute, F.col(START).cast("string").alias("start"), F.col(END).cast("string").alias("end"))
        .orderBy(key, START)
        .collect()
    )
    versions: dict = {}
    for r in rows:
        versions.setdefault(r[key], []).append({"value": r[attribute], "start": r["start"], "end": r["end"]})
    return {table: dict(list(versions.items())[:limit])}


def sanity(spark: SparkSession, db: str) -> dict:
    results = spark.table(f"{db}.fact_race_results")
    qualifying = spark.table(f"{db}.fact_qualifying")
    pit_stops = spark.table(f"{db}.fact_pit_stops")
    winners = results.where("final_position = 1")
    qualifiers = qualifying.count()
    return {
        "races": results.select("race_Id").distinct().count(),
        "results_missing_driver_or_team": results.where("driver_Id IS NULL OR team_Id IS NULL").count(),
        "winners_without_race_time": winners.where("race_time_ms IS NULL").count(),
        "winners": winners.count(),
        "qualifying_q1_coverage": (
            qualifying.where("q1_ms IS NOT NULL").count() / qualifiers if qualifiers else 0.0
        ),
        "pit_stops_without_duration": pit_stops.where("duration_ms IS NULL").count(),
        "track_type_rows": spark.table(f"{db}.circuit_track_type").count(),
        "circuits_without_track_type": (
            spark.table(f"{db}.dim_circuits").where("__END_AT IS NULL AND track_type IS NULL").count()
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    db = f"glue_catalog.{args.database}"

    spark = (
        SparkSession.builder.appName(f"verify-{args.label}")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args.bucket}/{args.prefix}/warehouse")
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true")
        .getOrCreate()
    )

    tables = {}
    for table, keys in KEYS.items():
        actual = spark.table(f"{db}.{table}")
        if table == "dim_circuits":
            expected = expected_dim_circuits(spark, db)
        else:
            source, key_columns, order = EXPECTED[table]
            expected = latest_per_key(spark.table(f"{db}.{source}"), key_columns, order)
        if table in SCD2_SOURCES:
            current = actual.where(f"{END} IS NULL")
            tables[table] = {
                "rows": current.count(),
                "parity_mismatch_rows": parity_mismatches(current, expected),
                "content_hash": content_hash(current),
                **scd2_stats(actual, spark.table(f"{db}.{SCD2_SOURCES[table]}"), keys, SEQUENCE_BY[table]),
            }
            continue
        tables[table] = {
            "rows": actual.count(),
            "duplicate_keys": actual.groupBy(*keys).count().where("count > 1").count(),
            "parity_mismatch_rows": parity_mismatches(actual, expected),
            "upsert_sequence_mismatch_rows": upsert_sequence_mismatches(actual, expected, keys, SEQUENCE_BY[table]),
            "content_hash": content_hash(actual),
        }

    gold = {}
    for table in GOLD_TABLES:
        gold[table] = spark.table(f"{db}.{table}").count() if spark.catalog.tableExists(f"{db}.{table}") else None

    result = {
        "label": args.label,
        "spark_version": spark.version,
        "tables": tables,
        "gold_rows": gold,
        "sanity": sanity(spark, db),
        "scd2_history": scd2_history(spark, db),
    }
    print(json.dumps(result, indent=2))
    boto3.client("s3").put_object(
        Bucket=args.bucket,
        Key=f"{args.prefix}/validation/results/{args.label}.json",
        Body=json.dumps(result).encode(),
    )
    spark.stop()


if __name__ == "__main__":
    main()
