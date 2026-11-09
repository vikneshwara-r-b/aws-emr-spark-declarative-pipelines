"""The raw schemas must cover every field Jolpica actually returns (the JSON reader silently drops
anything not in the schema) and every leaf must be a string (the API returns all values as strings)."""
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark.sql.types")
from pyspark.sql.types import ArrayType, StringType, StructType  # noqa: E402

from f1_pipeline_lib import schema  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "jolpica"
CASES = [
    ("results", schema.RAW_RESULTS_SCHEMA),
    ("qualifying", schema.RAW_QUALIFYING_SCHEMA),
    ("pitstops", schema.RAW_PITSTOPS_SCHEMA),
]


def schema_paths(data_type, prefix=""):
    """All field paths in a schema; arrays are transparent (their element's fields appear under the array's name)."""
    if isinstance(data_type, ArrayType):
        yield from schema_paths(data_type.elementType, prefix)
    elif isinstance(data_type, StructType):
        for field in data_type.fields:
            path = f"{prefix}.{field.name}" if prefix else field.name
            yield path, field.dataType
            yield from schema_paths(field.dataType, path)


def json_paths(value, prefix=""):
    if isinstance(value, list):
        for item in value:
            yield from json_paths(item, prefix)
    elif isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else key
            yield path
            yield from json_paths(child, path)


@pytest.mark.parametrize("endpoint, raw_schema", CASES)
def test_every_field_in_the_live_response_is_in_the_schema(endpoint, raw_schema):
    response = json.loads((FIXTURES / f"2024_1_{endpoint}.json").read_text())
    known = {path for path, _ in schema_paths(raw_schema)}
    missing = {p for p in set(json_paths(response)) if p not in known}
    # keys that exist in the response but that we deliberately do not read
    assert missing <= {"MRData.xmlns", "MRData.url"}, f"fields silently dropped by the reader: {sorted(missing)}"


@pytest.mark.parametrize("endpoint, raw_schema", CASES)
def test_every_leaf_is_a_string(endpoint, raw_schema):
    leaves = [(p, t) for p, t in schema_paths(raw_schema) if not isinstance(t, (StructType, ArrayType))]
    assert leaves and all(isinstance(t, StringType) for _, t in leaves)


def test_each_endpoint_schema_has_its_own_list_under_races():
    names = {
        "results": "Results",
        "qualifying": "QualifyingResults",
        "pitstops": "PitStops",
    }
    for endpoint, raw_schema in CASES:
        paths = {p for p, _ in schema_paths(raw_schema)}
        assert f"MRData.RaceTable.Races.{names[endpoint]}" in paths
        others = set(names.values()) - {names[endpoint]}
        assert not any(p.endswith(f".{o}") for p in paths for o in others)
