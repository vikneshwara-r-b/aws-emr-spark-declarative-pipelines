import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

from f1_pipeline_lib import f1_source as extract
from f1_pipeline_lib.f1_source import fetch_endpoint

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "jolpica"
BASE_URL = "https://api.jolpi.ca/ergast/f1"


def fixture(endpoint):
    return json.loads((FIXTURES / f"2024_1_{endpoint}.json").read_text())


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status_code, body, headers or {}

    def json(self):
        return copy.deepcopy(self._body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} error", response=self)


class FakeSession:
    """Serves queued responses and records every call."""

    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def no_sleep():
    with patch.object(extract.time, "sleep") as sleep:
        yield sleep


def paged(endpoint, total, page_size=100):
    """Builds the pages Jolpica would return for `total` rows of an endpoint, by repeating the fixture row."""
    key = extract.ENDPOINT_LIST_KEYS[endpoint]
    base = fixture(endpoint)
    row = base["MRData"]["RaceTable"]["Races"][0][key][0]
    pages = []
    for offset in range(0, total, page_size):
        page = copy.deepcopy(base)
        page["MRData"]["total"] = str(total)
        page["MRData"]["offset"] = str(offset)
        page["MRData"]["RaceTable"]["Races"][0][key] = [
            {**row, "number": str(offset + i)} for i in range(min(page_size, total - offset))
        ]
        pages.append(FakeResponse(200, page))
    return pages


# --- fetch_endpoint ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "endpoint, list_key, expected_rows",
    [("results", "Results", 20), ("qualifying", "QualifyingResults", 20), ("pitstops", "PitStops", 43)],
)
def test_single_page_returns_the_api_envelope_as_one_json_line(endpoint, list_key, expected_rows):
    session = FakeSession(FakeResponse(200, fixture(endpoint)))
    text = fetch_endpoint(BASE_URL, "2024", "1", endpoint, session=session)

    assert "\n" not in text  # Spark's JSON reader needs one document per line
    race = json.loads(text)["MRData"]["RaceTable"]["Races"][0]
    assert len(race[list_key]) == expected_rows
    assert race["season"] == "2024" and race["round"] == "1"


def test_request_uses_the_documented_url_page_size_and_user_agent():
    session = FakeSession(FakeResponse(200, fixture("results")))
    fetch_endpoint(BASE_URL + "/", "2024", "1", "results", user_agent="my-agent/9", session=session)

    (call,) = session.calls
    assert call["url"] == f"{BASE_URL}/2024/1/results.json"
    assert call["params"] == {"limit": 100, "offset": 0}
    assert call["headers"] == {"User-Agent": "my-agent/9"}
    assert call["timeout"] == extract.REQUEST_TIMEOUT_SECONDS


def test_default_user_agent_identifies_the_project():
    session = FakeSession(FakeResponse(200, fixture("results")))
    fetch_endpoint(BASE_URL, "2024", "1", "results", session=session)
    assert session.calls[0]["headers"]["User-Agent"] == extract.DEFAULT_USER_AGENT


def test_pagination_merges_every_page_into_one_race(no_sleep):
    session = FakeSession(*paged("pitstops", 250))
    text = fetch_endpoint(BASE_URL, "2024", "1", "pitstops", session=session)

    assert [c["params"]["offset"] for c in session.calls] == [0, 100, 200]
    stops = json.loads(text)["MRData"]["RaceTable"]["Races"][0]["PitStops"]
    assert [s["driverId"] for s in stops][:1] == [fixture("pitstops")["MRData"]["RaceTable"]["Races"][0]["PitStops"][0]["driverId"]]
    assert len(stops) == 250
    assert no_sleep.call_count == 2  # polite pause between pages, none after the last


def test_exact_multiple_of_page_size_stops_without_an_extra_call():
    session = FakeSession(*paged("pitstops", 200))
    fetch_endpoint(BASE_URL, "2024", "1", "pitstops", session=session)
    assert len(session.calls) == 2


def test_empty_later_page_stops_pagination():
    pages = paged("pitstops", 150)
    empty = copy.deepcopy(pages[0]._body)
    empty["MRData"]["RaceTable"]["Races"] = []
    session = FakeSession(pages[0], FakeResponse(200, empty))
    text = fetch_endpoint(BASE_URL, "2024", "1", "pitstops", session=session)
    assert len(session.calls) == 2
    assert len(json.loads(text)["MRData"]["RaceTable"]["Races"][0]["PitStops"]) == 100


# --- rate limiting and errors ------------------------------------------------------------------


def test_429_is_retried_with_backoff_then_succeeds(no_sleep):
    session = FakeSession(FakeResponse(429), FakeResponse(429), FakeResponse(200, fixture("results")))
    text = fetch_endpoint(BASE_URL, "2024", "1", "results", session=session)

    assert len(session.calls) == 3
    assert [c.args[0] for c in no_sleep.call_args_list] == [2, 4]  # exponential backoff
    assert len(json.loads(text)["MRData"]["RaceTable"]["Races"][0]["Results"]) == 20


def test_retry_after_header_is_honoured(no_sleep):
    session = FakeSession(FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, fixture("results")))
    fetch_endpoint(BASE_URL, "2024", "1", "results", session=session)
    assert no_sleep.call_args_list[0].args[0] == 7.0


def test_persistent_rate_limiting_raises_after_the_last_attempt():
    session = FakeSession(*[FakeResponse(429) for _ in range(extract.MAX_ATTEMPTS)])
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_endpoint(BASE_URL, "2024", "1", "results", session=session)
    assert len(session.calls) == extract.MAX_ATTEMPTS


def test_server_errors_are_retried():
    session = FakeSession(FakeResponse(503), FakeResponse(200, fixture("results")))
    fetch_endpoint(BASE_URL, "2024", "1", "results", session=session)
    assert len(session.calls) == 2


def test_client_errors_other_than_429_fail_immediately():
    session = FakeSession(FakeResponse(402), FakeResponse(200, fixture("results")))
    with pytest.raises(requests.exceptions.HTTPError, match="402"):
        fetch_endpoint(BASE_URL, "2024", "1", "results", session=session)
    assert len(session.calls) == 1


def test_connection_errors_propagate():
    class Boom:
        def get(self, *args, **kwargs):
            raise requests.exceptions.ConnectionError("boom")

    with pytest.raises(requests.exceptions.ConnectionError):
        fetch_endpoint(BASE_URL, "2024", "1", "results", session=Boom())


@pytest.mark.parametrize("season, round_", [(None, "1"), ("2024", None), ("", ""), (None, None)])
def test_season_and_round_are_required(season, round_):
    with pytest.raises(ValueError, match="season"):
        fetch_endpoint(BASE_URL, season, round_, "results", session=FakeSession())


def test_unknown_endpoint_is_rejected():
    with pytest.raises(ValueError, match="unknown endpoint"):
        fetch_endpoint(BASE_URL, "2024", "1", "laps", session=FakeSession())


def test_a_race_without_results_raises_instead_of_staging_an_empty_document():
    empty = fixture("results")
    empty["MRData"]["RaceTable"]["Races"] = []
    with pytest.raises(ValueError, match="no results"):
        fetch_endpoint(BASE_URL, "2099", "1", "results", session=FakeSession(FakeResponse(200, empty)))


def test_empty_pit_stops_are_tolerated():
    empty = fixture("pitstops")
    empty["MRData"]["RaceTable"]["Races"] = []
    text = fetch_endpoint(BASE_URL, "2024", "1", "pitstops", session=FakeSession(FakeResponse(200, empty)))
    assert json.loads(text)["MRData"]["RaceTable"]["Races"] == []


# --- the Python data source ----------------------------------------------------------------------


from f1_pipeline_lib.f1_source import F1ApiDataSource, F1ApiStreamReader, to_row  # noqa: E402
from f1_pipeline_lib.schema import RAW_PITSTOPS_SCHEMA, RAW_QUALIFYING_SCHEMA, RAW_RESULTS_SCHEMA  # noqa: E402

SCHEMAS = {"results": RAW_RESULTS_SCHEMA, "qualifying": RAW_QUALIFYING_SCHEMA, "pitstops": RAW_PITSTOPS_SCHEMA}


def reader(run_id="run-1", endpoint="results", **overrides):
    options = {"baseUrl": BASE_URL, "endpoint": endpoint, "season": "2024", "round": "1", "runId": run_id, **overrides}
    options = {k: v for k, v in options.items() if v is not None}
    return F1ApiStreamReader(SCHEMAS[endpoint], options)


@pytest.fixture
def api():
    """Replaces the HTTP fetch: returns the fixture for the endpoint and records the calls."""
    calls = []

    def fake_fetch(base_url, season, round_, endpoint, user_agent=extract.DEFAULT_USER_AGENT, session=None):
        calls.append((base_url, season, round_, endpoint, user_agent))
        return json.dumps(fixture(endpoint))

    with patch.object(extract, "fetch_endpoint", side_effect=fake_fetch):
        yield calls


def test_data_source_is_named_f1_api_and_builds_the_stream_reader_with_its_options():
    source = F1ApiDataSource(options={"season": "2024"})
    assert F1ApiDataSource.name() == "f1_api"
    built = source.simpleStreamReader(RAW_RESULTS_SCHEMA)
    assert isinstance(built, F1ApiStreamReader) and built.options == {"season": "2024"}


def test_the_raw_schema_must_be_passed_explicitly():
    with pytest.raises(NotImplementedError, match="schema"):
        F1ApiDataSource(options={}).schema()


def test_first_read_of_a_run_fetches_the_race_once_and_returns_one_row(api):
    r = reader()
    rows, end = r.read(r.initialOffset())

    rows = list(rows)
    assert len(rows) == 1
    assert api == [(BASE_URL, "2024", "1", "results", extract.DEFAULT_USER_AGENT)]
    assert end == {"run": "run-1", "endpoint": "results", "season": "2024", "round": "1"}
    assert json.loads(json.dumps(end)) == end  # offsets must survive the checkpoint's JSON


def test_second_read_in_the_same_run_returns_nothing_and_does_not_call_the_api(api):
    r = reader()
    _, end = r.read(r.initialOffset())
    rows, end_again = r.read(end)

    assert list(rows) == [] and end_again == end
    assert len(api) == 1


def test_a_new_run_reads_again_even_for_the_same_race(api):
    _, end = reader(run_id="run-1").read({"run": ""})
    rows, new_end = reader(run_id="run-2").read(end)

    assert len(list(rows)) == 1 and new_end["run"] == "run-2"
    assert len(api) == 2


def test_replay_reads_the_race_recorded_in_the_offset_not_the_current_options(api):
    r = reader(run_id="run-9", season="2025", round="9")
    rows = list(r.readBetweenOffsets({"run": ""}, {"run": "run-1", "endpoint": "qualifying", "season": "2024", "round": "3"}))

    assert len(rows) == 1
    assert api[0][1:4] == ("2024", "3", "qualifying")


def test_a_missing_run_id_is_an_error():
    with pytest.raises(ValueError, match="runId"):
        reader(run_id=None).read({"run": ""})


def test_api_errors_fail_the_read_instead_of_returning_an_empty_batch():
    with patch.object(extract, "fetch_endpoint", side_effect=requests.exceptions.HTTPError("503")):
        with pytest.raises(requests.exceptions.HTTPError):
            reader().read({"run": ""})


def test_missing_season_and_round_options_fail_in_the_fetch():
    # no mock: the real fetch_endpoint rejects the missing settings before any HTTP call
    with pytest.raises(ValueError, match="season"):
        reader(season=None, round=None).read({"run": ""})


def test_options_reach_the_fetch(api):
    reader(userAgent="my-agent/1", baseUrl="http://x/f1").read({"run": ""})
    assert api[0][0] == "http://x/f1" and api[0][4] == "my-agent/1"


@pytest.mark.parametrize("endpoint", ["results", "qualifying", "pitstops"])
def test_row_follows_the_top_level_schema_and_nests_the_payload_as_a_dict(endpoint):
    row = to_row(fixture(endpoint), SCHEMAS[endpoint])
    assert row == (fixture(endpoint)["MRData"],)


@pytest.mark.parametrize("endpoint", ["results", "qualifying", "pitstops"])
def test_row_converts_with_sparks_own_python_data_source_conversion(endpoint):
    # The exact conversion the Python worker applies to rows a data source returns: a dict must be accepted for
    # structs and every payload field must fit the all-string raw schema.
    pytest.importorskip("pyarrow")
    from pyspark.sql.conversion import LocalDataToArrowConversion

    table = LocalDataToArrowConversion.convert([to_row(fixture(endpoint), SCHEMAS[endpoint])], SCHEMAS[endpoint], False)
    assert table.num_rows == 1
    race = table.to_pylist()[0]["MRData"]["RaceTable"]["Races"][0]
    assert race["season"] == "2024" and race["Circuit"]["circuitId"] == "bahrain"


def test_module_is_self_contained_so_it_can_be_pickled_by_value():
    # The Python workers get this module's classes by value, so at import time it may only need the standard
    # library, requests and pyspark -- never another module of this project.
    import ast

    tree = ast.parse(Path(extract.__file__).read_text())
    imported = {
        (n.module if isinstance(n, ast.ImportFrom) else a.name).split(".")[0]
        for n in ast.walk(tree)
        if isinstance(n, (ast.Import, ast.ImportFrom))
        for a in (n.names if isinstance(n, ast.Import) else [n])
    }
    assert "f1_pipeline_lib" not in imported
    assert imported <= {"__future__", "json", "sys", "time", "typing", "requests", "pyspark"}
