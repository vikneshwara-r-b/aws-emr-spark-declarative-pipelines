# Curated (silver) zone, built with Spark 4.3's Auto CDC (dp.create_auto_cdc_flow):
#   * SCD Type 2 (full history) for the three dimensions that describe an entity whose attributes can change:
#     dim_drivers, dim_teams, dim_circuits. Every change to a tracked attribute closes the current version and
#     opens a new one; Auto CDC adds the version bounds __START_AT / __END_AT (the current version has
#     __END_AT NULL). The gold zone reads the current versions only (the *_current views in 04_gold.sql).
#   * SCD Type 1 (latest state only) for the facts: a corrected result overwrites the old row.
# SCD Type 2 needs Spark 4.3; Spark 4.2 supports stored_as_scd_type=1 only.
#
# Each target must be declared with create_streaming_table() before the flow that writes it. These
# targets deliberately declare NO schema: Auto CDC adds a hidden column (__spark_autocdc_metadata, a
# struct of deleteSequence and upsertSequence timestamps) to its target, so a declared schema is
# rejected with USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE. Spark infers it from the flow.
#
# Sequencing: dimensions use race_Date (a later race wins; re-ingesting the same race is a tie with
# identical content), facts use refreshed_at (a re-run overwrites the earlier load): the bronze load time that
# 02_bronze_prepared.py carries on the fact sources. It only orders events, so every target drops it
# (except_column_list); Auto CDC keeps the sequence it used in the hidden __spark_autocdc_metadata.upsertSequence
# (checked by verify.py). SCD2 tables mark when a version began in __START_AT.
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _auto_cdc(target: str, source: str, keys: list[str], sequence_by: str, comment: str, scd_type: int = 1) -> None:
    dp.create_streaming_table(target, comment=comment)
    options = {"except_column_list": ["refreshed_at"]}
    if scd_type == 2:
        # The sequence column (race_Date) moves with every race, so it must not open a new version by itself:
        # a version changes only when a business attribute changes.
        options["track_history_except_column_list"] = [sequence_by]
    dp.create_auto_cdc_flow(
        target=target,
        source=source,
        keys=keys,
        sequence_by=F.col(sequence_by),
        stored_as_scd_type=scd_type,
        **options,
    )


# --- dimensions straight from the prepared tables ----------------------------------------------

_auto_cdc(
    "dim_drivers",
    "race_drivers",
    ["driver_Id"],
    "race_Date",
    "Driver history (Auto CDC SCD2, sequenced by race_Date); the current version has __END_AT NULL.",
    scd_type=2,
)
_auto_cdc(
    "dim_teams",
    "race_teams",
    ["team_id"],
    "race_Date",
    "Constructor history (Auto CDC SCD2, sequenced by race_Date); the current version has __END_AT NULL.",
    scd_type=2,
)


# --- dim_circuits: enriched with track_type from the independent circuit_track_type reference ---
# Stream (race_circuits) LEFT JOIN static snapshot (circuit_track_type): LEFT so a missing/mismatched
# circuit_Id surfaces as track_type = NULL instead of dropping the circuit. The static side is
# de-duplicated to the latest row per circuit (circuit_track_type is append-only). Because this is a
# stream-static join, a track_type change is picked up the next time that circuit's row streams
# through -- load the reference CSVs before the first race run (scripts/run_pipeline.sh
# --upload-reference-data).
@dp.temporary_view(name="circuits_enriched")
def circuits_enriched():
    spark = SparkSession.active()
    circuits = spark.readStream.table("race_circuits")
    track_types = (
        spark.read.table("circuit_track_type")
        .select(
            "circuit_Id",
            "track_type",
            F.row_number()
            .over(Window.partitionBy("circuit_Id").orderBy(F.col("refreshed_at").desc()))
            .alias("_rn"),
        )
        .where("_rn = 1")
        .select(F.col("circuit_Id").alias("tt_circuit_Id"), "track_type")
    )
    return circuits.join(
        track_types, circuits["circuit_Id"] == track_types["tt_circuit_Id"], "left"
    ).select(
        "circuit_Id",
        "name",
        "locality",
        "country",
        "track_type",
        "lat",
        "lon",
        "race_Date",
        "refreshed_at",
    )


_auto_cdc(
    "dim_circuits",
    "circuits_enriched",
    ["circuit_Id"],
    "race_Date",
    "Circuit history incl. track_type (Auto CDC SCD2, sequenced by race_Date); the current version has __END_AT NULL.",
    scd_type=2,
)


# --- facts ---------------------------------------------------------------------------------------

_auto_cdc(
    "fact_race_results",
    "race_results_unnested",
    ["race_Id", "driver_Id"],
    "refreshed_at",
    "Latest result per (race, driver) (Auto CDC SCD1, sequenced by refreshed_at).",
)
_auto_cdc(
    "fact_qualifying",
    "race_qualifying_unnested",
    ["race_Id", "driver_Id"],
    "refreshed_at",
    "Latest qualifying result per (race, driver) (Auto CDC SCD1, sequenced by refreshed_at).",
)
_auto_cdc(
    "fact_pit_stops",
    "race_pit_stops",
    ["race_Id", "driver_Id", "stop"],
    "refreshed_at",
    "Latest pit stop per (race, driver, stop number) (Auto CDC SCD1, sequenced by refreshed_at).",
)
