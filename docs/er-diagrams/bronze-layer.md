# Bronze layer -- ER diagram

Source: `pipeline/f1/transformations/00_bronze_circuit_track_type.py`, `01_bronze_raw.py`, `02_bronze_prepared.py`.

Raw = one streaming table per Jolpica endpoint (payload as returned, every value a string, plus lineage
columns), read straight from the API by a PySpark Python Data Source (`f1_api`, no files staged in S3). Prepared = the same data flattened and typed, one table per grain; `race_Id` is derived as
`<season>_<round, 2 digits>` because the API has no race id. `circuit_track_type` is deliberately drawn
with no relationship to the raw tables: it is the second, independent, non-API source (static CSVs) and
only joins the graph in the curated layer, via `dim_circuits`.

Every raw and prepared table (and `circuit_track_type`) carries `refreshed_at`, the time the row was loaded. The curated facts
use it as their Auto CDC sequence (a re-run of a race overwrites the earlier load) and then drop it, so it is not visible in the
curated or gold tables.

```mermaid
erDiagram
    RAW_RESULTS {
        struct MRData "RaceTable.Races[].Results[]"
        string source_url
        string season_requested
        string round_requested
        timestamp refreshed_at
    }
    RAW_QUALIFYING {
        struct MRData "RaceTable.Races[].QualifyingResults[]"
        string source_url
        string season_requested
        string round_requested
        timestamp refreshed_at
    }
    RAW_PITSTOPS {
        struct MRData "RaceTable.Races[].PitStops[]"
        string source_url
        string season_requested
        string round_requested
        timestamp refreshed_at
    }

    CIRCUIT_TRACK_TYPE {
        string circuit_Id PK
        string track_type
        timestamp refreshed_at
    }

    RACE_RESULTS_UNNESTED {
        string race_Id PK
        string driver_Id PK
        int season_year
        int race_Round
        string race_Name
        date race_Date
        string race_Time
        string circuit_Id
        string team_Id
        int car_number
        int grid
        int final_position
        string position_text
        double points
        int laps
        string status
        bigint race_time_ms
        string race_time_text
        int fastest_lap_rank
        int fastest_lap_number
        bigint fastest_lap_time_ms
        double fastest_lap_avg_speed
        timestamp refreshed_at
    }
    RACE_DRIVERS {
        string driver_Id
        string given_name
        string family_name
        string code
        string nationality
        date birthday
        int number
        string team_id
        date race_Date
        timestamp refreshed_at
    }
    RACE_TEAMS {
        string team_id
        string name
        string nationality
        date race_Date
        timestamp refreshed_at
    }
    RACE_CIRCUITS {
        string circuit_Id
        string name
        string locality
        string country
        double lat
        double lon
        date race_Date
        timestamp refreshed_at
    }
    RACE_QUALIFYING_UNNESTED {
        string race_Id PK
        string driver_Id PK
        int season_year
        int race_Round
        date race_Date
        string team_Id
        int car_number
        int qual_position
        bigint q1_ms
        bigint q2_ms
        bigint q3_ms
        timestamp refreshed_at
    }
    RACE_PIT_STOPS {
        string race_Id PK
        string driver_Id PK
        int stop PK
        int season_year
        int race_Round
        date race_Date
        int lap
        string stop_time
        bigint duration_ms
        string duration_text
        timestamp refreshed_at
    }

    RAW_RESULTS ||--o{ RACE_RESULTS_UNNESTED : "explodes Races[].Results[] into"
    RAW_RESULTS ||--o{ RACE_DRIVERS : "extracts driver from"
    RAW_RESULTS ||--o{ RACE_TEAMS : "extracts constructor from"
    RAW_RESULTS ||--o{ RACE_CIRCUITS : "extracts circuit from"
    RAW_QUALIFYING ||--o{ RACE_QUALIFYING_UNNESTED : "explodes QualifyingResults[] into"
    RAW_PITSTOPS ||--o{ RACE_PIT_STOPS : "explodes PitStops[] into"
```

Parsing rules (all through `try_cast` / regex-guarded expressions, so a bad value becomes NULL instead
of failing the run, because ANSI mode is on): lap and qualifying times (`1:24.350`, `1:33:56.736`,
`23.456`) become milliseconds; `+gap` strings and `R`/`D` become NULL; `race_time_ms` comes from the
API's `Time.millis`.
