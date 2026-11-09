# Curated layer -- ER diagram

Source: `pipeline/f1/transformations/03_curated.py`.

All six tables are built with Spark 4.3's Auto CDC (`dp.create_auto_cdc_flow`):

- `dim_drivers`, `dim_teams`, `dim_circuits` are **SCD Type 2** (`stored_as_scd_type=2`): one row per *version* of an entity.
  Auto CDC adds `__START_AT` and `__END_AT`; the current version has `__END_AT IS NULL`. A new version opens only when a tracked
  attribute changes (`track_history_except_column_list=[race_Date]`; the sequence column itself is not tracked). The gold zone reads
  `dim_*_current` views (open versions only). Primary key below is the entity key; a key appears once per version.
- The three facts are **SCD Type 1** (latest state only, `stored_as_scd_type=1`).

Each target is declared with no schema because Auto CDC adds hidden columns (`__spark_autocdc_metadata` with `deleteSequence` and
`upsertSequence`; for SCD2 also the version bounds) to it. Sequencing: dimensions by `race_Date` (a later race wins),
the facts by `refreshed_at`, the bronze load time (a re-run overwrites the earlier load). `refreshed_at` only orders
events, so no curated table keeps it; Auto CDC records the sequence it used in the hidden `__spark_autocdc_metadata.upsertSequence`.
The SCD2 tables mark when a version began in `__START_AT`.

`dim_circuits` carries `track_type`, joined in from the second, independent bronze source
`circuit_track_type` (stream-static join, left join: a circuit without a classification keeps
`track_type = NULL`). Load the reference CSVs before the first race run.

```mermaid
erDiagram
    DIM_DRIVERS {
        string driver_Id PK "SCD2: one row per version"
        string given_name
        string family_name
        string code
        string nationality
        date birthday
        int number
        string team_id "tracked: new version on a transfer"
        date race_Date
        timestamp __START_AT
        timestamp __END_AT
    }
    DIM_TEAMS {
        string team_id PK
        string name
        string nationality
        date race_Date
        timestamp __START_AT
        timestamp __END_AT
    }
    DIM_CIRCUITS {
        string circuit_Id PK
        string name
        string locality
        string country
        string track_type
        double lat
        double lon
        date race_Date
        timestamp __START_AT
        timestamp __END_AT
    }
    FACT_RACE_RESULTS {
        string race_Id PK
        string driver_Id PK
        string team_Id FK
        string circuit_Id FK
        int season_year
        int grid
        int final_position
        double points
        int laps
        string status
        bigint race_time_ms
        bigint fastest_lap_time_ms
        double fastest_lap_avg_speed
    }
    FACT_QUALIFYING {
        string race_Id PK
        string driver_Id PK
        string team_Id FK
        int qual_position
        bigint q1_ms
        bigint q2_ms
        bigint q3_ms
    }
    FACT_PIT_STOPS {
        string race_Id PK
        string driver_Id PK
        int stop PK
        int lap
        bigint duration_ms
    }

    DIM_DRIVERS ||--o{ FACT_RACE_RESULTS : "drove"
    DIM_TEAMS ||--o{ FACT_RACE_RESULTS : "entered by"
    DIM_CIRCUITS ||--o{ FACT_RACE_RESULTS : "held at"
    DIM_DRIVERS ||--o{ FACT_QUALIFYING : "set by"
    DIM_DRIVERS ||--o{ FACT_PIT_STOPS : "made by"
```

(Only the main columns are drawn; `fact_race_results` also carries `race_Round`, `race_Name`, `race_Date`,
`race_Time`, `position_text`, `race_time_text`, `fastest_lap_rank`, `fastest_lap_number`, `car_number`.)
