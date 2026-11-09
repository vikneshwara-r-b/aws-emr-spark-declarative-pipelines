"""Static checks on the pipeline definitions. SDP's graph APIs only work inside a real pipeline run, so
these parse the source instead of importing it; Spark behaviour is verified on the cluster by
validation/verify.py."""
import ast
import re
from pathlib import Path

import pytest

TRANSFORMATIONS = Path(__file__).resolve().parents[2] / "pipeline" / "f1" / "transformations"
RAW = TRANSFORMATIONS / "01_bronze_raw.py"
PREPARED = TRANSFORMATIONS / "02_bronze_prepared.py"
CURATED = TRANSFORMATIONS / "03_curated.py"
GOLD = TRANSFORMATIONS / "04_gold.sql"

# target -> (source dataset, keys, sequence_by)
EXPECTED_TARGETS = {
    "dim_drivers": ("race_drivers", ["driver_Id"], "race_Date"),
    "dim_teams": ("race_teams", ["team_id"], "race_Date"),
    "dim_circuits": ("circuits_enriched", ["circuit_Id"], "race_Date"),
    "fact_race_results": ("race_results_unnested", ["race_Id", "driver_Id"], "refreshed_at"),
    "fact_qualifying": ("race_qualifying_unnested", ["race_Id", "driver_Id"], "refreshed_at"),
    "fact_pit_stops": ("race_pit_stops", ["race_Id", "driver_Id", "stop"], "refreshed_at"),
}
PREPARED_TABLES = {
    "race_results_unnested", "race_drivers", "race_teams", "race_circuits",
    "race_qualifying_unnested", "race_pit_stops",
}
RAW_TABLES = {"raw_results", "raw_qualifying", "raw_pitstops"}


def _calls(path, func_name):
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", getattr(node.func, "id", "")) == func_name:
            yield node


def _literal(node):
    return ast.literal_eval(node)


def _auto_cdc_targets():
    """Calls of the module's own _auto_cdc(target, source, keys, sequence_by, comment) helper."""
    return {
        _literal(c.args[0]): (_literal(c.args[1]), _literal(c.args[2]), _literal(c.args[3]))
        for c in _calls(CURATED, "_auto_cdc")
    }


def test_all_six_curated_tables_are_auto_cdc_targets_with_the_expected_keys_and_sequence():
    assert _auto_cdc_targets() == EXPECTED_TARGETS


SCD2_TARGETS = {"dim_drivers", "dim_teams", "dim_circuits"}


def _scd_types():
    """target -> SCD type, from the optional scd_type argument (6th positional or keyword) of _auto_cdc."""
    types = {}
    for call in _calls(CURATED, "_auto_cdc"):
        given = {kw.arg: _literal(kw.value) for kw in call.keywords}
        types[_literal(call.args[0])] = given.get("scd_type", 1)
    return types


def test_dimensions_that_can_change_keep_history_with_scd2_and_the_rest_are_scd1():
    types = _scd_types()
    assert {t for t, scd in types.items() if scd == 2} == SCD2_TARGETS
    assert {t for t, scd in types.items() if scd == 1} == set(EXPECTED_TARGETS) - SCD2_TARGETS


def test_scd2_history_is_not_opened_by_the_sequence_column_alone():
    # race_Date moves with every race; if it were tracked, each race would open a new version of every entity.
    (flow,) = _calls(CURATED, "create_auto_cdc_flow")
    kws = {kw.arg: kw.value for kw in flow.keywords}
    assert "stored_as_scd_type" in kws
    source = CURATED.read_text()
    assert 'options["track_history_except_column_list"] = [sequence_by]' in source
    assert "scd_type == 2" in source


def test_auto_cdc_helper_declares_no_schema():
    # A declared schema fails with USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE (hidden Auto CDC columns).
    (table,) = _calls(CURATED, "create_streaming_table")
    assert "schema" not in {kw.arg for kw in table.keywords}


def test_every_target_drops_refreshed_at_and_the_facts_sequence_by_it():
    # refreshed_at only orders events; Auto CDC keeps the sequence in the hidden __spark_autocdc_metadata
    assert '{"except_column_list": ["refreshed_at"]}' in CURATED.read_text()
    assert {t: seq for t, (_, _, seq) in EXPECTED_TARGETS.items() if t.startswith("fact_")} == dict.fromkeys(
        ("fact_race_results", "fact_qualifying", "fact_pit_stops"), "refreshed_at"
    )
    # every prepared table carries the bronze load time under that name, so the fact sources can sequence by it
    assert "load_date_time" not in PREPARED.read_text() and "refreshed_at timestamp" in PREPARED.read_text()


def test_scd1_facts_read_the_prepared_tables_directly_so_the_flows_wait_for_them():
    # A temporary view over readStream.table(...) as an Auto CDC source is not recognised as a dependency: the
    # fact flows then ran at the start of the update, before the prepared tables were filled, and lagged a run behind.
    for table in ("fact_race_results", "fact_qualifying", "fact_pit_stops"):
        assert EXPECTED_TARGETS[table][0] in PREPARED_TABLES


def test_every_auto_cdc_source_exists_upstream():
    sources = {source for source, _, _ in EXPECTED_TARGETS.values()}
    views = {_literal(kw.value) for c in _calls(CURATED, "temporary_view") for kw in c.keywords if kw.arg == "name" and isinstance(kw.value, ast.Constant)}
    assert sources <= PREPARED_TABLES | views


def test_prepared_zone_declares_every_table_the_curated_zone_reads():
    declared = {_literal(c.args[0]) for c in _calls(PREPARED, "create_streaming_table")}
    flows = {kw.value.value for c in _calls(PREPARED, "append_flow") for kw in c.keywords if kw.arg == "target"}
    assert declared == PREPARED_TABLES == flows


def test_raw_zone_defines_the_three_tables_and_the_prepared_zone_reads_only_those():
    raw_defined = set(re.findall(r'^\s+"(raw_[a-z]+)",$', RAW.read_text(), flags=re.M))
    raw_read = set(re.findall(r'readStream\.table\("(raw_[a-z]+)"\)', PREPARED.read_text()))
    assert raw_defined == RAW_TABLES
    assert raw_read == RAW_TABLES


def test_race_drivers_carry_the_team_so_dim_drivers_has_real_history():
    # The only driver attribute that changes in the Jolpica history is the team (transfers); SCD2 tracks every
    # column except the sequence column, so team_id in race_drivers is what opens new dim_drivers versions.
    text = PREPARED.read_text()
    assert "birthday date,number integer,team_id string,race_Date date" in text
    assert 'F.col("r.Constructor.constructorId").alias("team_id")' in text


def test_gold_views_read_only_curated_tables_and_define_five_views():
    sql = GOLD.read_text()
    views = re.findall(r"CREATE MATERIALIZED VIEW (\w+)", sql)
    assert views == [
        "driver_performance_summary", "team_performance_summary", "circuit_summary",
        "qualifying_vs_race_summary", "pit_stop_summary",
    ]
    referenced = set(re.findall(r"\b(?:FROM|JOIN)\s+(\w+)", sql))
    current_views = {f"{t}_current" for t in SCD2_TARGETS}
    assert referenced <= set(EXPECTED_TARGETS) | current_views, referenced


def test_scd2_current_views_filter_to_open_versions():
    sql = GOLD.read_text()
    for table in SCD2_TARGETS:
        assert f"CREATE TEMPORARY VIEW {table}_current AS SELECT * FROM {table} WHERE __END_AT IS NULL;" in sql
        assert f"JOIN {table} " not in sql and f"FROM {table} " not in sql.replace(
            f"FROM {table} WHERE __END_AT IS NULL", ""
        )


def test_no_leftover_f1api_dev_references_in_pipeline_code():
    for path in (TRANSFORMATIONS.parent).rglob("*"):
        if path.is_file() and path.suffix in {".py", ".sql", ".yml"}:
            text = path.read_text()
            assert "f1api.dev" not in text.lower() and "raw_race" not in text, path


def test_job_mode_conf_is_the_f1_one():
    bronze = RAW.read_text()
    assert "spark.glue.sdp" not in bronze
    assert 'JOB_MODE_CONF = "spark.f1.job.mode"' in bronze


def test_each_raw_flow_has_a_distinct_explicit_name():
    # the default flow name is the function name, identical for the three factory-made flows
    assert 'name=f"ingest_{table}"' in RAW.read_text()


@pytest.mark.parametrize("path", [RAW, PREPARED, CURATED])
def test_transformations_parse(path):
    ast.parse(path.read_text())
