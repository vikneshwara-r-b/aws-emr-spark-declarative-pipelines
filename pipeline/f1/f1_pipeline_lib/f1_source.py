"""A PySpark Python Data Source that reads the Jolpica-F1 API directly.

    spark.dataSource.register(F1ApiDataSource)
    spark.readStream.format("f1_api").schema(RAW_RESULTS_SCHEMA)
         .option("baseUrl", ...).option("endpoint", "results").option("season", "2024").option("round", "1")
         .option("runId", RUN_ID).load()

Each pipeline run reads ONE race (season, round) of one endpoint, once, as a single row holding the API's own
JSON envelope (MRData ...), and then reports "no more data". Nothing is staged in S3.

How "once per run" works (SimpleDataSourceStreamReader): the stream offset is {"run": <runId>, ...}. A new run has a
new runId, so the stored offset differs and read() fetches and returns an offset carrying the new runId; the next
read() in the same run sees its own runId and returns nothing, which ends the (available-now) update. If a batch has
to be replayed after a failure, readBetweenOffsets() re-reads the (season, round) recorded in the end offset, not the
current options.

This module is pickled BY VALUE and shipped to the Python workers (register_data_source below), so it must stay
self-contained: only the standard library, requests and pyspark at import time, no other module of this project.
Python data source classes must be picklable and libraries are imported inside methods (Apache Spark docs).
"""
from __future__ import annotations

import json
import sys
import time
from typing import Any, Iterator, Tuple

import requests
from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import StructType

REQUEST_TIMEOUT_SECONDS = 30
PAGE_SIZE = 100  # Jolpica's documented maximum for `limit`
PAUSE_BETWEEN_CALLS_SECONDS = 0.3  # stay well under the 4 requests/second burst limit
MAX_ATTEMPTS = 5
DEFAULT_USER_AGENT = "aws-emr-spark-declarative-pipelines/1.0"

# endpoint -> key of the per-race list inside MRData.RaceTable.Races[0]
ENDPOINT_LIST_KEYS = {
    "results": "Results",
    "qualifying": "QualifyingResults",
    "pitstops": "PitStops",
}


def _get_json(url: str, params: dict, user_agent: str, session) -> dict:
    """GET with retries for rate limiting (429) and server errors (5xx); other 4xx raise immediately."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = session.get(
            url, params=params, headers={"User-Agent": user_agent}, timeout=REQUEST_TIMEOUT_SECONDS
        )
        retryable = response.status_code == 429 or response.status_code >= 500
        if not retryable:
            response.raise_for_status()
            return response.json()
        if attempt == MAX_ATTEMPTS:
            response.raise_for_status()
        retry_after = (response.headers or {}).get("Retry-After")
        delay = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else 2**attempt
        time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def fetch_endpoint(
    base_url: str,
    season: str | None,
    round_: str | None,
    endpoint: str,
    user_agent: str = DEFAULT_USER_AGENT,
    session=requests,
) -> str:
    """Fetch one Jolpica endpoint for one race, following pagination, and return a single-line JSON
    document in the API's own envelope (MRData.RaceTable.Races[0] holding the merged list).

    One (season, round) per call, by design -- "one run = one race". Jolpica pages by leaf row
    (`limit` max 100, `total` in the envelope), so a race with more than 100 rows needs several
    calls; the per-race list (Results / QualifyingResults / PitStops) is concatenated across pages.
    There is no quarantine layer: a missing conf, network failure or non-retryable HTTP error raises
    and fails the run (re-run it).
    """
    if not season or not round_:
        raise ValueError("spark.f1.season.year and spark.f1.season.round are both required settings")
    if endpoint not in ENDPOINT_LIST_KEYS:
        raise ValueError(f"unknown endpoint '{endpoint}', expected one of {sorted(ENDPOINT_LIST_KEYS)}")

    list_key = ENDPOINT_LIST_KEYS[endpoint]
    url = f"{base_url.rstrip('/')}/{season}/{round_}/{endpoint}.json"

    merged: dict | None = None
    offset = 0
    while True:
        page = _get_json(url, {"limit": PAGE_SIZE, "offset": offset}, user_agent, session)
        mr_data = page["MRData"]
        races = mr_data.get("RaceTable", {}).get("Races", [])
        if merged is None:
            if not races and endpoint == "results":
                raise ValueError(f"no results returned for season {season} round {round_} ({url}); has the race been run?")
            merged = page
        elif races:
            merged["MRData"]["RaceTable"]["Races"][0].setdefault(list_key, []).extend(
                races[0].get(list_key, [])
            )

        offset += PAGE_SIZE
        if not races or offset >= int(mr_data.get("total", 0)):
            break
        time.sleep(PAUSE_BETWEEN_CALLS_SECONDS)

    return json.dumps(merged, separators=(",", ":"))


# --- the data source -----------------------------------------------------------------------------------


def to_row(payload: dict, schema: StructType) -> Tuple[Any, ...]:
    """One row for the data source's top-level schema. Nested structs/arrays stay dicts/lists: Spark's Python data
    source conversion fills a struct from a dict by field name (missing -> NULL, unknown keys ignored)."""
    return tuple(payload.get(field.name) for field in schema.fields)


class F1ApiStreamReader(SimpleDataSourceStreamReader):
    """Reads one race of one endpoint per pipeline run (see the module docstring)."""

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options

    def initialOffset(self) -> dict:
        return {"run": ""}

    def _fetch_row(self, endpoint: str, season: str, round_: str) -> Tuple[Any, ...]:
        payload = json.loads(
            fetch_endpoint(
                self.options.get("baseUrl", DEFAULT_BASE_URL),
                season,
                round_,
                endpoint,
                self.options.get("userAgent", DEFAULT_USER_AGENT),
            )
        )
        return to_row(payload, self.schema)

    def read(self, start: dict) -> Tuple[Iterator[Tuple[Any, ...]], dict]:
        run_id = self.options.get("runId")
        if not run_id:
            raise ValueError("the f1_api data source needs a runId option (one value per pipeline run)")
        if start.get("run") == run_id:
            return iter([]), start  # already read in this run: nothing more, the update can finish
        endpoint = self.options.get("endpoint", "")
        season, round_ = self.options.get("season"), self.options.get("round")
        row = self._fetch_row(endpoint, season, round_)  # raises on a missing setting, network or HTTP error
        end = {"run": run_id, "endpoint": endpoint, "season": season, "round": round_}
        return iter([row]), end

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple[Any, ...]]:
        # Replay of a batch that was planned but not committed: re-read what the offset says, not the current options.
        return iter([self._fetch_row(end["endpoint"], end["season"], end["round"])])

    def commit(self, end: dict) -> None:
        pass


DEFAULT_BASE_URL = "https://api.jolpi.ca/ergast/f1"


class F1ApiDataSource(DataSource):
    """format("f1_api"). Options: baseUrl, endpoint (results|qualifying|pitstops), season, round, userAgent, runId."""

    @classmethod
    def name(cls) -> str:
        return "f1_api"

    def schema(self) -> str | StructType:
        raise NotImplementedError("pass the raw schema with .schema(...) when reading from f1_api")

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        return F1ApiStreamReader(schema, self.options)


def register_data_source(spark) -> None:
    """Registers F1ApiDataSource on the session, pickled by value so Python workers need no copy of this project."""
    from pyspark import cloudpickle

    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    spark.dataSource.register(F1ApiDataSource)
