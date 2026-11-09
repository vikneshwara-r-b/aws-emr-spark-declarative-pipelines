from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

# Prepared zone: the raw Jolpica payloads (all strings, nested Races[] arrays) flattened and typed,
# one streaming table per grain. Every derived column is expressed via .select(...), never
# .withColumn(...), per AWS's documented SDP guidance: a downstream dataset reading an upstream
# pipeline table via .table(...) and then applying .withColumn(...) can cause SDP to miss the
# dependency on the second and subsequent runs (one-run-lag stale reads).
#
# ANSI mode is on (Spark 4), so a plain cast of a malformed string RAISES. Everything parsed from the
# API goes through try_cast / try_to_date or a regex-guarded expression, so bad or missing values
# become NULL instead of failing the run. No .distinct(): it compiles to a Deduplicate node that
# forces the server-side analyzer to resolve the upstream plan at registration time
# ([TABLE_OR_VIEW_NOT_FOUND] on a first run); the curated Auto CDC flows resolve duplicates anyway.
#
# These helpers live here (not in f1_pipeline_lib) because they use pyspark.pipelines graph state at
# import and are only ever called from flows in this file.


# --- parsing helpers -------------------------------------------------------------------------

_HMS = r"^(\d+):(\d{2}):(\d{2}\.\d+)$"  # 1:33:56.736
_MS = r"^(\d+):(\d{2}\.\d+)$"  # 1:24.350
_S = r"^(\d+\.\d+)$"  # 23.456


def _try_int(column: Column) -> Column:
    return column.try_cast("int")


def _try_double(column: Column) -> Column:
    return column.try_cast("double")


def _duration_ms(text: Column) -> Column:
    """'1:24.350' / '1:33:56.736' / '23.456' -> milliseconds (bigint); anything else (empty, '+22.457',
    NULL) -> NULL. The regexes guarantee the pieces are numeric, so the plain casts cannot raise."""

    def part(pattern: str, group: int) -> Column:
        return F.regexp_extract(text, pattern, group).cast("double")

    return (
        F.round(
            F.when(text.rlike(_HMS), part(_HMS, 1) * 3_600_000 + part(_HMS, 2) * 60_000 + part(_HMS, 3) * 1_000)
            .when(text.rlike(_MS), part(_MS, 1) * 60_000 + part(_MS, 2) * 1_000)
            .when(text.rlike(_S), part(_S, 1) * 1_000)
        )
        .cast("bigint")
    )


# --- shared building blocks ------------------------------------------------------------------


def _races(raw: DataFrame) -> DataFrame:
    """One row per race in the payload (a run fetches exactly one) with the race struct kept for field access."""
    return raw.select(F.explode(F.col("MRData.RaceTable.Races")).alias("race"), "refreshed_at")


def _race_columns() -> list[Column]:
    """Race-level columns shared by every table. race_Id is derived: Jolpica has no race id."""
    return [
        F.concat(F.col("race.season"), F.lit("_"), F.lpad(F.col("race.round"), 2, "0")).alias("race_Id"),
        _try_int(F.col("race.season")).alias("season_year"),
        _try_int(F.col("race.round")).alias("race_Round"),
        F.col("race.raceName").alias("race_Name"),
        F.try_to_date(F.col("race.date"), "yyyy-MM-dd").alias("race_Date"),
        F.col("race.time").alias("race_Time"),
        F.col("race.Circuit.circuitId").alias("circuit_Id"),
    ]


def _results_stream() -> DataFrame:
    """One row per (race, result row): the race struct plus the result struct as `r`."""
    races = _races(SparkSession.active().readStream.table("raw_results"))
    return races.select("race", "refreshed_at", F.explode(F.col("race.Results")).alias("r"))


# --- declared outputs ------------------------------------------------------------------------

dp.create_streaming_table(
    "race_results_unnested",
    comment="One row per (race, driver): typed race result.",
    schema=(
        "race_Id string,season_year integer,race_Round integer,race_Name string,race_Date date,"
        "race_Time string,circuit_Id string,driver_Id string,team_Id string,car_number integer,"
        "grid integer,final_position integer,position_text string,points double,laps integer,"
        "status string,race_time_ms bigint,race_time_text string,fastest_lap_rank integer,"
        "fastest_lap_number integer,fastest_lap_time_ms bigint,fastest_lap_avg_speed double,"
        "refreshed_at timestamp"
    ),
)
dp.create_streaming_table(
    "race_drivers",
    comment="One row per (race, driver) occurrence, not deduped.",
    schema=(
        "driver_Id string,given_name string,family_name string,code string,nationality string,"
        "birthday date,number integer,team_id string,race_Date date,refreshed_at timestamp"
    ),
)
dp.create_streaming_table(
    "race_teams",
    comment="One row per (race, constructor) occurrence, not deduped.",
    schema="team_id string,name string,nationality string,race_Date date,refreshed_at timestamp",
)
dp.create_streaming_table(
    "race_circuits",
    comment="One row per race (a race has exactly one circuit).",
    schema=(
        "circuit_Id string,name string,locality string,country string,lat double,lon double,"
        "race_Date date,refreshed_at timestamp"
    ),
)
dp.create_streaming_table(
    "race_qualifying_unnested",
    comment="One row per (race, driver): typed qualifying result.",
    schema=(
        "race_Id string,season_year integer,race_Round integer,race_Date date,driver_Id string,"
        "team_Id string,car_number integer,qual_position integer,q1_ms bigint,q2_ms bigint,"
        "q3_ms bigint,refreshed_at timestamp"
    ),
)
dp.create_streaming_table(
    "race_pit_stops",
    comment="One row per (race, driver, stop).",
    schema=(
        "race_Id string,season_year integer,race_Round integer,race_Date date,driver_Id string,"
        "stop integer,lap integer,stop_time string,duration_ms bigint,duration_text string,"
        "refreshed_at timestamp"
    ),
)


# --- results-derived flows -------------------------------------------------------------------


@dp.append_flow(target="race_results_unnested")
def ingest_race_results_unnested():
    return _results_stream().select(
        *_race_columns(),
        F.col("r.Driver.driverId").alias("driver_Id"),
        F.col("r.Constructor.constructorId").alias("team_Id"),
        _try_int(F.col("r.number")).alias("car_number"),
        _try_int(F.col("r.grid")).alias("grid"),
        _try_int(F.col("r.position")).alias("final_position"),
        F.col("r.positionText").alias("position_text"),
        _try_double(F.col("r.points")).alias("points"),
        _try_int(F.col("r.laps")).alias("laps"),
        F.col("r.status").alias("status"),
        F.col("r.Time.millis").try_cast("bigint").alias("race_time_ms"),
        F.col("r.Time.time").alias("race_time_text"),
        _try_int(F.col("r.FastestLap.rank")).alias("fastest_lap_rank"),
        _try_int(F.col("r.FastestLap.lap")).alias("fastest_lap_number"),
        _duration_ms(F.col("r.FastestLap.Time.time")).alias("fastest_lap_time_ms"),
        _try_double(F.col("r.FastestLap.AverageSpeed.speed")).alias("fastest_lap_avg_speed"),
        "refreshed_at",
    )


@dp.append_flow(target="race_drivers")
def ingest_race_drivers():
    # One row per (race, driver) occurrence -- not deduped; dim_drivers resolves the versions.
    # team_id is the constructor the driver raced for in this race: the one driver attribute that really
    # changes in the API's history (transfers), so it is what gives dim_drivers (SCD2) real versions.
    # dateOfBirth is documented as YYYY-MM-DD; try_to_date keeps an odd value from failing the run.
    return _results_stream().select(
        F.col("r.Driver.driverId").alias("driver_Id"),
        F.col("r.Driver.givenName").alias("given_name"),
        F.col("r.Driver.familyName").alias("family_name"),
        F.col("r.Driver.code").alias("code"),
        F.col("r.Driver.nationality").alias("nationality"),
        F.try_to_date(F.col("r.Driver.dateOfBirth"), "yyyy-MM-dd").alias("birthday"),
        _try_int(F.col("r.Driver.permanentNumber")).alias("number"),
        F.col("r.Constructor.constructorId").alias("team_id"),
        F.try_to_date(F.col("race.date"), "yyyy-MM-dd").alias("race_Date"),
        "refreshed_at",
    )


@dp.append_flow(target="race_teams")
def ingest_race_teams():
    # Lowercase "team_id" for the key (Auto CDC key in dim_teams); Spark's case-insensitive identifier
    # resolution means joins against team_Id elsewhere still work.
    return _results_stream().select(
        F.col("r.Constructor.constructorId").alias("team_id"),
        F.col("r.Constructor.name").alias("name"),
        F.col("r.Constructor.nationality").alias("nationality"),
        F.try_to_date(F.col("race.date"), "yyyy-MM-dd").alias("race_Date"),
        "refreshed_at",
    )


@dp.append_flow(target="race_circuits")
def ingest_race_circuits():
    # One row per race: read the races without exploding the results.
    return _races(SparkSession.active().readStream.table("raw_results")).select(
        F.col("race.Circuit.circuitId").alias("circuit_Id"),
        F.col("race.Circuit.circuitName").alias("name"),
        F.col("race.Circuit.Location.locality").alias("locality"),
        F.col("race.Circuit.Location.country").alias("country"),
        _try_double(F.col("race.Circuit.Location.lat")).alias("lat"),
        _try_double(F.col("race.Circuit.Location.long")).alias("lon"),
        F.try_to_date(F.col("race.date"), "yyyy-MM-dd").alias("race_Date"),
        "refreshed_at",
    )


# --- qualifying and pit stops ----------------------------------------------------------------


@dp.append_flow(target="race_qualifying_unnested")
def ingest_race_qualifying():
    races = _races(SparkSession.active().readStream.table("raw_qualifying"))
    return races.select(
        "race", "refreshed_at", F.explode(F.col("race.QualifyingResults")).alias("q")
    ).select(
        *_race_columns(),
        F.col("q.Driver.driverId").alias("driver_Id"),
        F.col("q.Constructor.constructorId").alias("team_Id"),
        _try_int(F.col("q.number")).alias("car_number"),
        _try_int(F.col("q.position")).alias("qual_position"),
        _duration_ms(F.col("q.Q1")).alias("q1_ms"),
        _duration_ms(F.col("q.Q2")).alias("q2_ms"),
        _duration_ms(F.col("q.Q3")).alias("q3_ms"),
        "refreshed_at",
    ).select(
        "race_Id", "season_year", "race_Round", "race_Date", "driver_Id", "team_Id", "car_number",
        "qual_position", "q1_ms", "q2_ms", "q3_ms", "refreshed_at",
    )


@dp.append_flow(target="race_pit_stops")
def ingest_race_pit_stops():
    races = _races(SparkSession.active().readStream.table("raw_pitstops"))
    return races.select(
        "race", "refreshed_at", F.explode(F.col("race.PitStops")).alias("p")
    ).select(
        *_race_columns(),
        F.col("p.driverId").alias("driver_Id"),
        _try_int(F.col("p.stop")).alias("stop"),
        _try_int(F.col("p.lap")).alias("lap"),
        F.col("p.time").alias("stop_time"),
        _duration_ms(F.col("p.duration")).alias("duration_ms"),
        F.col("p.duration").alias("duration_text"),
        "refreshed_at",
    ).select(
        "race_Id", "season_year", "race_Round", "race_Date", "driver_Id", "stop", "lap",
        "stop_time", "duration_ms", "duration_text", "refreshed_at",
    )
