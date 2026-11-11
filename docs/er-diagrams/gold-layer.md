# Gold (modelled) layer -- ER diagram

Source: `pipeline/f1/transformations/04_gold.sql`.

Five denormalized materialized views, no foreign keys between them, each aggregated straight from the
curated Auto CDC tables. The three dimensions are SCD Type 2 (one row per version), so the views read them through the
`dim_drivers_current`, `dim_teams_current` and `dim_circuits_current` temporary views (open versions only, `__END_AT IS NULL`);
joining the history rows would multiply every fact row by the number of versions. Here `refreshed_at` is the time the view was
computed (`current_timestamp()`), not the bronze load time. `circuit_summary` carries `track_type` from `dim_circuits`, proving the second
bronze source reaches gold; its fastest lap is computed from the ingested races (Jolpica has no circuit lap
record). `pit_stop_summary` excludes stops over 60 s as red-flag outliers; `positions_gained` is
`qual_position - final_position` (positive = finished ahead of the qualifying position).

```mermaid
erDiagram
    DRIVER_PERFORMANCE_SUMMARY {
        int season_year
        string driver_Id
        string driver_name
        string team_Id
        string team_name
        long races_entered
        long wins
        long podiums
        double total_points
        double avg_finish_position
        int best_finish_position
        timestamp refreshed_at
    }
    TEAM_PERFORMANCE_SUMMARY {
        int season_year
        string team_Id
        string team_name
        long races_entered
        long wins
        long podiums
        double total_points
        long drivers_used
        timestamp refreshed_at
    }
    CIRCUIT_SUMMARY {
        string circuit_Id
        string circuit_name
        string locality
        string country
        string track_type
        double lat
        double lon
        long total_races_held
        date first_race_date
        date last_race_date
        bigint fastest_lap_time_ms
        string fastest_lap_driver_id
        timestamp refreshed_at
    }
    QUALIFYING_VS_RACE_SUMMARY {
        int season_year
        string driver_Id
        string driver_name
        long races_entered
        long poles
        double avg_qualifying_position
        double avg_finish_position
        double avg_positions_gained
        timestamp refreshed_at
    }
    PIT_STOP_SUMMARY {
        int season_year
        string team_Id
        string team_name
        long stops
        long drivers
        double avg_duration_ms
        bigint best_duration_ms
        timestamp refreshed_at
    }
```
