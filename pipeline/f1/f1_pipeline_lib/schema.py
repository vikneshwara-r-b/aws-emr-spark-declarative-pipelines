from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# Raw-zone schemas for the Jolpica-F1 API (https://api.jolpi.ca/ergast/f1, the Ergast successor).
# Jolpica returns every value as a JSON string (numbers, dates and times included), so every leaf
# here is a StringType; typing happens in the prepared zone (02_bronze_prepared.py). Fields that the
# API documents as optional (Q2/Q3, FastestLap, Time, ...) are simply absent from some rows: the JSON
# reader turns them into NULL.
#
# Built via explicit StructType/StructField construction rather than
# StructType.fromDDL(...): fromDDL requires an active SparkContext at call time
# (it parses the DDL string through the JVM), which makes it fail at plain module
# import if nothing has created a SparkSession yet. Building the schema directly
# as Python objects has no such dependency, so this module is safe to import
# standalone (e.g. in tests, or if the SDP runtime imports transformation files
# before/without an active session).


def _strings(*names):
    return [StructField(name, StringType(), True) for name in names]


def _struct(*fields):
    return StructType(list(fields))


def _nested(name, data_type):
    return StructField(name, data_type, True)


_LOCATION = _struct(*_strings("lat", "long", "locality", "country"))
_CIRCUIT = _struct(*_strings("circuitId", "url", "circuitName"), _nested("Location", _LOCATION))
_DRIVER = _struct(
    *_strings(
        "driverId", "permanentNumber", "code", "url", "givenName", "familyName", "dateOfBirth", "nationality"
    )
)
_CONSTRUCTOR = _struct(*_strings("constructorId", "url", "name", "nationality"))

_RACE_TIME = _struct(*_strings("millis", "time"))
_FASTEST_LAP = _struct(
    *_strings("rank", "lap"),
    _nested("Time", _struct(*_strings("time"))),
    _nested("AverageSpeed", _struct(*_strings("units", "speed"))),
)
_RESULT = _struct(
    *_strings("number", "position", "positionText", "points"),
    _nested("Driver", _DRIVER),
    _nested("Constructor", _CONSTRUCTOR),
    *_strings("grid", "laps", "status"),
    _nested("Time", _RACE_TIME),
    _nested("FastestLap", _FASTEST_LAP),
)
_QUALIFYING_RESULT = _struct(
    *_strings("number", "position"),
    _nested("Driver", _DRIVER),
    _nested("Constructor", _CONSTRUCTOR),
    *_strings("Q1", "Q2", "Q3"),
)
_PIT_STOP = _struct(*_strings("driverId", "lap", "stop", "time", "duration"))


def _raw_schema(list_field: str, item_schema: StructType) -> StructType:
    """Envelope shared by all three endpoints: MRData.RaceTable.Races[], each race carrying the
    endpoint-specific list (Results / QualifyingResults / PitStops)."""
    race = _struct(
        *_strings("season", "round", "url", "raceName"),
        _nested("Circuit", _CIRCUIT),
        *_strings("date", "time"),
        _nested(list_field, ArrayType(item_schema)),
    )
    race_table = _struct(*_strings("season", "round"), _nested("Races", ArrayType(race)))
    return _struct(
        _nested(
            "MRData",
            _struct(*_strings("series", "limit", "offset", "total"), _nested("RaceTable", race_table)),
        )
    )


RAW_RESULTS_SCHEMA = _raw_schema("Results", _RESULT)
RAW_QUALIFYING_SCHEMA = _raw_schema("QualifyingResults", _QUALIFYING_RESULT)
RAW_PITSTOPS_SCHEMA = _raw_schema("PitStops", _PIT_STOP)

# Second, independent reference source (flat-file, not API): circuit track-type
# classification (street / permanent / hybrid), not present anywhere in the Jolpica
# payloads. Joins on circuit_Id -- the same key race_circuits/dim_circuits
# already carry.
#
# nullable=True on both fields (not False): confirmed locally that Spark's CSV
# reader always produces nullable columns regardless of the declared schema's
# nullability (same reason every raw field is nullable=True) --
# declaring these non-nullable caused a real Glue RUN failure, since Iceberg's
# streaming writer rejects a batch whose actual (nullable) schema doesn't match
# the table's declared (required) schema:
# IllegalArgumentException: Cannot write incompatible dataset to table with
# schema ... circuit_Id should be required, but is optional.
CIRCUIT_TRACK_TYPE_SCHEMA = StructType(
    [
        StructField("circuit_Id", StringType(), True),
        StructField("track_type", StringType(), True),
    ]
)
