import json

import pytest

from validation import report

TABLES = report.TABLES


def _snapshot(label, rows=5, dup=0, parity=0, seq=0, broken=0, first=0, hash_="1", gold=3, races=None, **sanity_overrides):
    sanity = {
        "races": races if races is not None else {"r1": 1, "history": 5}.get(label, 2),
        "results_missing_driver_or_team": 0,
        "winners": races if races is not None else {"r1": 1, "history": 5}.get(label, 2),
        "winners_without_race_time": 0,
        "qualifying_q1_coverage": 1.0,
        "pit_stops_without_duration": 0,
        "circuits_without_track_type": 0,
        "track_type_rows": {"r1": 23}.get(label, 25),
        **sanity_overrides,
    }
    return {
        "label": label,
        "spark_version": "4.3.0",
        "tables": {
            t: (
                {"rows": rows, "duplicate_keys": dup, "parity_mismatch_rows": parity, "history_rows": rows + 1,
                 "broken_version_links": broken, "first_start_mismatch_keys": first, "content_hash": hash_}
                if t in report.SCD2_TABLES
                else {"rows": rows, "duplicate_keys": dup, "parity_mismatch_rows": parity,
                      "upsert_sequence_mismatch_rows": seq, "content_hash": hash_}
            )
            for t in TABLES
        },
        "gold_rows": {view: gold for view in report.GOLD_VIEWS},
        "sanity": sanity,
    }


def _write(tmp_path, r1, r2, rerun, env=None):
    env = env or {
        "spark_version": "4.3.0",
        "java_version": "17.0.12",
        "python_version": "3.11.9",
        "master": "yarn",
        "database_visible": True,
        "glue_catalog_namespaces": ["f1_sdp_db"],
        "auto_cdc_signature": "(target, source, keys, sequence_by, stored_as_scd_type=None, *, track_history_except_column_list=None)",
    }
    for label, data in {"env": env, "r1": r1, "r2": r2, "r2_rerun": rerun}.items():
        (tmp_path / f"{label}.json").write_text(json.dumps(data))


def test_all_checks_pass_for_healthy_results(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    assert all(ok for _, ok, _ in report.evaluate(tmp_path))


@pytest.mark.parametrize(
    "r2_kwargs, expected_failure",
    [
        ({"dup": 1}, "one row per key"),
        ({"parity": 2}, "parity"),
        ({"seq": 1}, "upsertSequence"),
    ],
)
def test_detects_duplicates_and_parity_mismatches(tmp_path, r2_kwargs, expected_failure):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6, **r2_kwargs), _snapshot("r2_rerun", rows=6))
    failed = [name for name, ok, _ in report.evaluate(tmp_path) if not ok]
    assert any(expected_failure in name for name in failed)


def test_rerun_that_changes_content_fails_idempotence(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6, hash_="2"))
    assert any("idempotent" in name for name, ok, _ in report.evaluate(tmp_path) if not ok)


def test_stock_emr_spark_is_rejected(tmp_path):
    env = {
        "spark_version": "4.0.2-amzn-0", "java_version": "17.0.1", "python_version": "3.11.9", "master": "yarn",
        "database_visible": True, "glue_catalog_namespaces": [], "auto_cdc_signature": "()",
    }
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6), env=env)
    failed = [name for name, ok, _ in report.evaluate(tmp_path) if not ok]
    assert "Spark version is 4.3.x" in failed


def test_six_curated_tables_and_five_gold_views_are_checked(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    names = [name for name, _, _ in report.evaluate(tmp_path)]
    assert len(report.TABLES) == 6 and len(report.GOLD_VIEWS) == 5
    for table in report.TABLES:
        assert any(f"{table}: one " in n and " per key" in n for n in names), table
    for view in report.GOLD_VIEWS:
        assert f"gold {view} populated" in names


def test_missing_gold_view_fails(tmp_path):
    rerun = _snapshot("r2_rerun", rows=6)
    rerun["gold_rows"]["pit_stop_summary"] = None
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), rerun)
    assert any("pit_stop_summary" in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


@pytest.mark.parametrize(
    "override, expected_failure",
    [
        ({"winners_without_race_time": 1}, "winner has a race time"),
        ({"qualifying_q1_coverage": 0.5}, "Q1 time"),
        ({"pit_stops_without_duration": 3}, "parsed duration"),
        ({"results_missing_driver_or_team": 2}, "driver and a constructor"),
    ],
)
def test_data_sanity_failures_are_reported(tmp_path, override, expected_failure):
    _write(tmp_path, _snapshot("r1", rows=3, **override), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    assert any(expected_failure in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


def test_wrong_number_of_races_in_the_results_fails(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3, races=2), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    assert any("covers every ingested race" in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


def test_scd2_tables_get_version_chain_checks_and_scd1_tables_get_the_upsert_sequence_check(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    names = [name for name, _, _ in report.evaluate(tmp_path)]
    for table in report.TABLES:
        chain = any(f"{table}: version chain is contiguous" in n for n in names)
        upsert = any(f"{table}: upsertSequence matches" in n for n in names)
        assert chain == (table in report.SCD2_TABLES) and upsert == (table not in report.SCD2_TABLES), table


@pytest.mark.parametrize(
    "kwargs, expected_failure",
    [({"broken": 1}, "version chain"), ({"first": 2}, "earliest source sequence"), ({"seq": 1}, "upsertSequence")],
)
def test_version_chain_and_sequence_failures_are_reported(tmp_path, kwargs, expected_failure):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6, **kwargs), _snapshot("r2_rerun", rows=6))
    failed = [n for n, ok, _ in report.evaluate(tmp_path) if not ok]
    assert any(expected_failure in n for n in failed), failed


def test_rerun_that_adds_a_version_fails_idempotence(tmp_path):
    rerun = _snapshot("r2_rerun", rows=6)
    rerun["tables"]["dim_teams"]["history_rows"] += 1
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), rerun)
    assert any("dim_teams" in n and "idempotent" in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


@pytest.mark.parametrize(
    "r1_kwargs, r2_kwargs, expected_failure",
    [
        ({"track_type_rows": 25}, {}, "incremental reference file was ingested"),
        ({}, {"circuits_without_track_type": 1}, "every circuit has a track_type"),
    ],
)
def test_reference_data_loads_are_checked(tmp_path, r1_kwargs, r2_kwargs, expected_failure):
    _write(tmp_path, _snapshot("r1", rows=3, **r1_kwargs), _snapshot("r2", rows=6, **r2_kwargs), _snapshot("r2_rerun", rows=6))
    assert any(expected_failure in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


def test_rerun_that_reloads_reference_data_fails(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6, track_type_rows=30))
    assert any("does not reload reference data" in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


# --- history phase: real driver transfers ------------------------------------------------------------------


def _v(value, start, end):
    return {"value": value, "start": start, "end": end}


def _history(**overrides):
    snap = _snapshot("history", rows=8)
    snap["scd2_history"] = {
        "dim_drivers": {
            "hamilton": [_v("mercedes", "2024-03-02", "2025-03-16"), _v("ferrari", "2025-03-16", None)],
            "sainz": [_v("ferrari", "2024-03-02", "2025-03-16"), _v("williams", "2025-03-16", None)],
            "tsunoda": [_v("rb", "2024-03-02", "2025-04-06"), _v("red_bull", "2025-04-06", None)],
            "lawson": [_v("rb", "2024-12-08", "2025-03-16"), _v("red_bull", "2025-03-16", "2025-04-06"), _v("rb", "2025-04-06", None)],
            "ocon": [_v("alpine", "2024-03-02", "2025-03-16"), _v("haas", "2025-03-16", None)],
            "bearman": [_v("ferrari", "2024-03-09", "2025-03-16"), _v("haas", "2025-03-16", None)],
        }
    }
    snap["scd2_history"]["dim_drivers"].update(overrides)
    return snap


def _write_with_history(tmp_path, history):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    (tmp_path / "history.json").write_text(json.dumps(history))


def test_history_phase_is_optional(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    assert not any(n.startswith("[history]") for n, _, _ in report.evaluate(tmp_path))


def test_real_transfers_pass(tmp_path):
    _write_with_history(tmp_path, _history())
    checks = [(n, ok) for n, ok, _ in report.evaluate(tmp_path) if n.startswith("[history]")]
    assert len(checks) > 8 and all(ok for _, ok in checks), [c for c in checks if not c[1]]


def test_history_snapshot_gets_the_same_table_checks_as_the_other_snapshots(tmp_path):
    _write_with_history(tmp_path, _history())
    names = [n for n, _, _ in report.evaluate(tmp_path)]
    assert any(n.startswith("[history] dim_drivers: version chain") for n in names)
    assert any("[history] fact_race_results covers every ingested race" in n for n in names)


@pytest.mark.parametrize(
    "overrides, expected_failure",
    [
        ({"hamilton": [_v("mercedes", "2024-03-02", None)]}, "hamilton has versions"),
        ({"sainz": [_v("ferrari", "2024-03-02", "2025-03-16"), _v("williams", "2025-03-17", None)]}, "sainz has versions"),
        ({"tsunoda": [_v("rb", "2024-03-02", "2025-04-06"), _v("red_bull", "2025-04-06", "2025-05-01")]}, "tsunoda has versions"),
        ({"bearman": [_v("haas", "2024-03-09", "2025-03-16"), _v("haas", "2025-03-16", None)]}, "every other multi-version driver"),
    ],
)
def test_wrong_history_is_reported(tmp_path, overrides, expected_failure):
    _write_with_history(tmp_path, _history(**overrides))
    assert any(expected_failure in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


def test_no_history_at_all_fails(tmp_path):
    history = _history()
    history["scd2_history"] = {"dim_drivers": {}}
    _write_with_history(tmp_path, history)
    assert any("drivers with more than one version exist" in n for n, ok, _ in report.evaluate(tmp_path) if not ok)


def test_missing_env_json_fails_one_check_instead_of_crashing(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    (tmp_path / "env.json").unlink()
    failed = [n for n, ok, _ in report.evaluate(tmp_path) if not ok]
    assert failed == ["Environment check ran (env.json present)"]


def test_skip_env_leaves_the_environment_checks_out(tmp_path):
    _write(tmp_path, _snapshot("r1", rows=3), _snapshot("r2", rows=6), _snapshot("r2_rerun", rows=6))
    (tmp_path / "env.json").unlink()
    checks = report.evaluate(tmp_path, skip_env=True)
    assert all(ok for _, ok, _ in checks)
    assert not any("Spark version" in n or "env.json" in n for n, _, _ in checks)
