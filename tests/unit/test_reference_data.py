import csv
from pathlib import Path

CIRCUIT_TRACK_TYPE_DIR = (
    Path(__file__).resolve().parent.parent.parent / "reference_data" / "circuit_track_type"
)

# Two files, demonstrating the streaming table's incremental semantics:
# initial_load.csv ships at first deploy; incremental_load.csv is picked up by
# a later run without reprocessing the first (see README's "Multiple data
# sources, zero orchestration" section).
INITIAL_LOAD_CSV = CIRCUIT_TRACK_TYPE_DIR / "initial_load.csv"
INCREMENTAL_LOAD_CSV = CIRCUIT_TRACK_TYPE_DIR / "incremental_load.csv"

VALID_TRACK_TYPES = {"street", "permanent", "hybrid"}


def _rows(csv_path):
    with csv_path.open(newline="") as f:
        return list(csv.DictReader(f))


def _all_rows():
    return _rows(INITIAL_LOAD_CSV) + _rows(INCREMENTAL_LOAD_CSV)


def test_circuit_track_type_csvs_header_and_nonempty():
    for csv_path in (INITIAL_LOAD_CSV, INCREMENTAL_LOAD_CSV):
        rows = _rows(csv_path)
        assert rows, f"{csv_path.name} must not be empty"
        assert set(rows[0].keys()) == {"circuit_Id", "track_type"}


def test_circuit_track_type_csvs_have_no_duplicate_circuit_ids_combined():
    # A duplicate circuit_Id -- within one file or across both -- would fan-out
    # the LEFT JOIN in dim_circuits (03_curated.sql), breaking its
    # one-row-per-circuit grain. The curated-layer dedup picks the latest
    # refreshed_at per circuit_Id, but the two files themselves should still
    # never claim the same circuit twice.
    circuit_ids = [r["circuit_Id"] for r in _all_rows()]
    assert len(circuit_ids) == len(set(circuit_ids))


def test_circuit_track_type_csvs_have_no_blank_values():
    for r in _all_rows():
        assert r["circuit_Id"].strip()
        assert r["track_type"].strip()


def test_circuit_track_type_csvs_values_are_valid_enum():
    for r in _all_rows():
        assert r["track_type"] in VALID_TRACK_TYPES
