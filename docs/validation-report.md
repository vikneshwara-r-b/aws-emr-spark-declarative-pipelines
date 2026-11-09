# Validation report: Spark 4.3 Auto CDC (SCD1 and SCD2) on EMR

Races ingested: season 2024, rounds 1,2, then round 2 again.
Result: **PASS** (160/160 checks passed)

| Check | Result | Detail |
|---|---|---|
| Spark version is 4.3.x | PASS | 4.3.0 |
| Java is 17+ | PASS | 17.0.21 |
| Python is 3.11+ | PASS | 3.11.18 |
| Runs on YARN | PASS | yarn |
| Glue database visible via Iceberg catalog | PASS | ['default', 'f1_sdp_db'] |
| create_auto_cdc_flow available | PASS | (target: str, source: str, keys: Union[List[str], List[pyspark.sql.column.Column]], sequence_by: Union[str, pyspark.sql.column.Column], apply_as_deletes: Union[str, pyspark.sql.column.Column, NoneType] = None, column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, except_column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, stored_as_scd_type: Optional[Literal[1, 2, '1', '2']] = None, name: Optional[str] = None, *, track_history_column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, track_history_except_column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, spark_conf: Optional[Dict[str, str]] = None) -> None |
| Auto CDC SCD2 history tracking available | PASS | (target: str, source: str, keys: Union[List[str], List[pyspark.sql.column.Column]], sequence_by: Union[str, pyspark.sql.column.Column], apply_as_deletes: Union[str, pyspark.sql.column.Column, NoneType] = None, column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, except_column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, stored_as_scd_type: Optional[Literal[1, 2, '1', '2']] = None, name: Optional[str] = None, *, track_history_column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, track_history_except_column_list: Union[List[str], List[pyspark.sql.column.Column], NoneType] = None, spark_conf: Optional[Dict[str, str]] = None) -> None |
| [r1] dim_drivers: has rows | PASS | 20 |
| [r1] dim_drivers: one current version per key | PASS | 0 keys without exactly one current version |
| [r1] dim_drivers: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r1] dim_drivers: version chain is contiguous | PASS | 0 broken links, 20 versions |
| [r1] dim_drivers: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r1] dim_teams: has rows | PASS | 10 |
| [r1] dim_teams: one current version per key | PASS | 0 keys without exactly one current version |
| [r1] dim_teams: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r1] dim_teams: version chain is contiguous | PASS | 0 broken links, 10 versions |
| [r1] dim_teams: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r1] dim_circuits: has rows | PASS | 1 |
| [r1] dim_circuits: one current version per key | PASS | 0 keys without exactly one current version |
| [r1] dim_circuits: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r1] dim_circuits: version chain is contiguous | PASS | 0 broken links, 1 versions |
| [r1] dim_circuits: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r1] fact_race_results: has rows | PASS | 20 |
| [r1] fact_race_results: one row per key | PASS | 0 keys without exactly one row |
| [r1] fact_race_results: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r1] fact_race_results: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r1] fact_qualifying: has rows | PASS | 20 |
| [r1] fact_qualifying: one row per key | PASS | 0 keys without exactly one row |
| [r1] fact_qualifying: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r1] fact_qualifying: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r1] fact_pit_stops: has rows | PASS | 43 |
| [r1] fact_pit_stops: one row per key | PASS | 0 keys without exactly one row |
| [r1] fact_pit_stops: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r1] fact_pit_stops: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2] dim_drivers: has rows | PASS | 21 |
| [r2] dim_drivers: one current version per key | PASS | 0 keys without exactly one current version |
| [r2] dim_drivers: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r2] dim_drivers: version chain is contiguous | PASS | 0 broken links, 21 versions |
| [r2] dim_drivers: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r2] dim_teams: has rows | PASS | 10 |
| [r2] dim_teams: one current version per key | PASS | 0 keys without exactly one current version |
| [r2] dim_teams: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r2] dim_teams: version chain is contiguous | PASS | 0 broken links, 10 versions |
| [r2] dim_teams: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r2] dim_circuits: has rows | PASS | 2 |
| [r2] dim_circuits: one current version per key | PASS | 0 keys without exactly one current version |
| [r2] dim_circuits: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r2] dim_circuits: version chain is contiguous | PASS | 0 broken links, 2 versions |
| [r2] dim_circuits: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r2] fact_race_results: has rows | PASS | 40 |
| [r2] fact_race_results: one row per key | PASS | 0 keys without exactly one row |
| [r2] fact_race_results: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r2] fact_race_results: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2] fact_qualifying: has rows | PASS | 40 |
| [r2] fact_qualifying: one row per key | PASS | 0 keys without exactly one row |
| [r2] fact_qualifying: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r2] fact_qualifying: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2] fact_pit_stops: has rows | PASS | 62 |
| [r2] fact_pit_stops: one row per key | PASS | 0 keys without exactly one row |
| [r2] fact_pit_stops: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r2] fact_pit_stops: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2_rerun] dim_drivers: has rows | PASS | 21 |
| [r2_rerun] dim_drivers: one current version per key | PASS | 0 keys without exactly one current version |
| [r2_rerun] dim_drivers: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r2_rerun] dim_drivers: version chain is contiguous | PASS | 0 broken links, 21 versions |
| [r2_rerun] dim_drivers: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r2_rerun] dim_teams: has rows | PASS | 10 |
| [r2_rerun] dim_teams: one current version per key | PASS | 0 keys without exactly one current version |
| [r2_rerun] dim_teams: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r2_rerun] dim_teams: version chain is contiguous | PASS | 0 broken links, 10 versions |
| [r2_rerun] dim_teams: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r2_rerun] dim_circuits: has rows | PASS | 2 |
| [r2_rerun] dim_circuits: one current version per key | PASS | 0 keys without exactly one current version |
| [r2_rerun] dim_circuits: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [r2_rerun] dim_circuits: version chain is contiguous | PASS | 0 broken links, 2 versions |
| [r2_rerun] dim_circuits: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [r2_rerun] fact_race_results: has rows | PASS | 40 |
| [r2_rerun] fact_race_results: one row per key | PASS | 0 keys without exactly one row |
| [r2_rerun] fact_race_results: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r2_rerun] fact_race_results: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2_rerun] fact_qualifying: has rows | PASS | 40 |
| [r2_rerun] fact_qualifying: one row per key | PASS | 0 keys without exactly one row |
| [r2_rerun] fact_qualifying: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r2_rerun] fact_qualifying: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2_rerun] fact_pit_stops: has rows | PASS | 62 |
| [r2_rerun] fact_pit_stops: one row per key | PASS | 0 keys without exactly one row |
| [r2_rerun] fact_pit_stops: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [r2_rerun] fact_pit_stops: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [history] dim_drivers: has rows | PASS | 27 |
| [history] dim_drivers: one current version per key | PASS | 0 keys without exactly one current version |
| [history] dim_drivers: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [history] dim_drivers: version chain is contiguous | PASS | 0 broken links, 35 versions |
| [history] dim_drivers: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [history] dim_teams: has rows | PASS | 10 |
| [history] dim_teams: one current version per key | PASS | 0 keys without exactly one current version |
| [history] dim_teams: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [history] dim_teams: version chain is contiguous | PASS | 0 broken links, 10 versions |
| [history] dim_teams: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [history] dim_circuits: has rows | PASS | 5 |
| [history] dim_circuits: one current version per key | PASS | 0 keys without exactly one current version |
| [history] dim_circuits: parity with ROW_NUMBER() logic (current versions) | PASS | 0 mismatching rows |
| [history] dim_circuits: version chain is contiguous | PASS | 0 broken links, 5 versions |
| [history] dim_circuits: first version starts at the earliest source sequence | PASS | 0 mismatching keys |
| [history] fact_race_results: has rows | PASS | 100 |
| [history] fact_race_results: one row per key | PASS | 0 keys without exactly one row |
| [history] fact_race_results: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [history] fact_race_results: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [history] fact_qualifying: has rows | PASS | 100 |
| [history] fact_qualifying: one row per key | PASS | 0 keys without exactly one row |
| [history] fact_qualifying: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [history] fact_qualifying: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [history] fact_pit_stops: has rows | PASS | 193 |
| [history] fact_pit_stops: one row per key | PASS | 0 keys without exactly one row |
| [history] fact_pit_stops: parity with ROW_NUMBER() logic | PASS | 0 mismatching rows |
| [history] fact_pit_stops: upsertSequence matches the latest source sequence | PASS | 0 mismatching keys |
| [r2] fact_race_results grew (new race upserted) | PASS | 20 -> 40 |
| [r2 -> rerun] dim_drivers: re-ingesting the same race is idempotent | PASS | rows 21 -> 21 |
| [r2 -> rerun] dim_teams: re-ingesting the same race is idempotent | PASS | rows 10 -> 10 |
| [r2 -> rerun] dim_circuits: re-ingesting the same race is idempotent | PASS | rows 2 -> 2 |
| [r2 -> rerun] fact_race_results: re-ingesting the same race is idempotent | PASS | rows 40 -> 40 |
| [r2 -> rerun] fact_qualifying: re-ingesting the same race is idempotent | PASS | rows 40 -> 40 |
| [r2 -> rerun] fact_pit_stops: re-ingesting the same race is idempotent | PASS | rows 62 -> 62 |
| gold driver_performance_summary populated | PASS | 21 |
| gold team_performance_summary populated | PASS | 10 |
| gold circuit_summary populated | PASS | 2 |
| gold qualifying_vs_race_summary populated | PASS | 21 |
| gold pit_stop_summary populated | PASS | 10 |
| [r1] fact_race_results covers every ingested race | PASS | 1 (expected 1) |
| [r1] every result has a driver and a constructor | PASS | 0 |
| [r1] every winner has a race time | PASS | 0 of 1 winners without time |
| [r1] at least 90% of qualifiers have a Q1 time | PASS | 100% |
| [r1] every pit stop has a parsed duration | PASS | 0 |
| [r1] every circuit has a track_type | PASS | 0 without |
| [r2] fact_race_results covers every ingested race | PASS | 2 (expected 2) |
| [r2] every result has a driver and a constructor | PASS | 0 |
| [r2] every winner has a race time | PASS | 0 of 2 winners without time |
| [r2] at least 90% of qualifiers have a Q1 time | PASS | 98% |
| [r2] every pit stop has a parsed duration | PASS | 0 |
| [r2] every circuit has a track_type | PASS | 0 without |
| [r2_rerun] fact_race_results covers every ingested race | PASS | 2 (expected 2) |
| [r2_rerun] every result has a driver and a constructor | PASS | 0 |
| [r2_rerun] every winner has a race time | PASS | 0 of 2 winners without time |
| [r2_rerun] at least 90% of qualifiers have a Q1 time | PASS | 98% |
| [r2_rerun] every pit stop has a parsed duration | PASS | 0 |
| [r2_rerun] every circuit has a track_type | PASS | 0 without |
| [history] fact_race_results covers every ingested race | PASS | 5 (expected 5) |
| [history] every result has a driver and a constructor | PASS | 0 |
| [history] every winner has a race time | PASS | 0 of 5 winners without time |
| [history] at least 90% of qualifiers have a Q1 time | PASS | 98% |
| [history] every pit stop has a parsed duration | PASS | 0 |
| [history] every circuit has a track_type | PASS | 0 without |
| [r2] incremental reference file was ingested (circuit_track_type grew) | PASS | 23 -> 25 rows |
| [r2 -> rerun] re-running does not reload reference data | PASS | 25 -> 25 rows |
| [history] SCD2: drivers with more than one version exist | PASS | 7 drivers |
| [history] SCD2: hamilton has versions mercedes -> ferrari, closed in order, last one current | PASS | mercedes[2024-03-02..2025-03-16] -> ferrari[2025-03-16..None] |
| [history] SCD2: sainz has versions ferrari -> williams, closed in order, last one current | PASS | ferrari[2024-03-02..2025-03-16] -> williams[2025-03-16..None] |
| [history] SCD2: tsunoda has versions rb -> red_bull, closed in order, last one current | PASS | rb[2024-03-02..2025-04-06] -> red_bull[2025-04-06..None] |
| [history] SCD2: lawson has versions rb -> red_bull -> rb, closed in order, last one current | PASS | rb[2024-12-08..2025-03-16] -> red_bull[2025-03-16..2025-04-06] -> rb[2025-04-06..None] |
| [history] SCD2: ocon has versions alpine -> haas, closed in order, last one current | PASS | alpine[2024-03-02..2025-03-16] -> haas[2025-03-16..None] |
| [history] SCD2: every other multi-version driver is a transfer too (consecutive versions differ) | PASS | 2 other drivers with history |
