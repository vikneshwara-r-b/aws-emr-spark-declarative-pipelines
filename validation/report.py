"""Evaluates the result files that env_check.py / verify.py wrote, prints a pass/fail table
and writes a Markdown report. Plain Python (no Spark): runs on the laptop.

  python3 validation/report.py <results-dir> <report.md> [--season 2024 --rounds 1,2]
Exit code 1 if any check fails.
"""
import argparse
import json
import sys
from pathlib import Path

TABLES = [
    "dim_drivers",
    "dim_teams",
    "dim_circuits",
    "fact_race_results",
    "fact_qualifying",
    "fact_pit_stops",
]
SCD2_TABLES = ["dim_drivers", "dim_teams", "dim_circuits"]  # history kept; the rest are SCD1
GOLD_VIEWS = [
    "driver_performance_summary",
    "team_performance_summary",
    "circuit_summary",
    "qualifying_vs_race_summary",
    "pit_stop_summary",
]


def load(results: Path, label: str) -> dict:
    return json.loads((results / f"{label}.json").read_text())


def evaluate(results: Path, skip_env: bool = False) -> list[tuple[str, bool, str]]:
    checks: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append((name, bool(ok), detail))

    # env.json comes from env_check.py, which needs a running cluster; skip_env leaves its checks out
    # (the report then says so) instead of failing when the cluster is already gone
    if not skip_env:
        if not (results / "env.json").exists():
            check("Environment check ran (env.json present)", False, "run validation/env_check.py on the cluster, then sync the results")
        else:
            env = load(results, "env")
            check("Spark version is 4.3.x", env["spark_version"].startswith("4.3."), env["spark_version"])
            check("Java is 17+", int(env["java_version"].split(".")[0]) >= 17, env["java_version"])
            check("Python is 3.11+", tuple(map(int, env["python_version"].split(".")[:2])) >= (3, 11), env["python_version"])
            check("Runs on YARN", env["master"] == "yarn", env["master"])
            check("Glue database visible via Iceberg catalog", env["database_visible"], str(env["glue_catalog_namespaces"]))
            check("create_auto_cdc_flow available", "stored_as_scd_type" in env["auto_cdc_signature"], env["auto_cdc_signature"])
            check("Auto CDC SCD2 history tracking available", "track_history_except_column_list" in env["auto_cdc_signature"], env["auto_cdc_signature"])

    r1, r2, rerun = (load(results, label) for label in ("r1", "r2", "r2_rerun"))
    # optional last phase: more races, chosen for real driver transfers (see README); absent -> not checked
    history = load(results, "history") if (results / "history.json").exists() else None
    for snap in (r1, r2, rerun) + ((history,) if history else ()):
        label = snap["label"]
        for table in TABLES:
            t = snap["tables"][table]
            scd2 = table in SCD2_TABLES
            check(f"[{label}] {table}: has rows", t["rows"] > 0, str(t["rows"]))
            check(
                f"[{label}] {table}: " + ("one current version per key" if scd2 else "one row per key"),
                t["duplicate_keys"] == 0,
                f"{t['duplicate_keys']} keys without exactly one " + ("current version" if scd2 else "row"),
            )
            check(
                f"[{label}] {table}: parity with ROW_NUMBER() logic" + (" (current versions)" if scd2 else ""),
                t["parity_mismatch_rows"] == 0,
                f"{t['parity_mismatch_rows']} mismatching rows",
            )
            if scd2:
                check(
                    f"[{label}] {table}: version chain is contiguous",
                    t["broken_version_links"] == 0,
                    f"{t['broken_version_links']} broken links, {t['history_rows']} versions",
                )
                check(
                    f"[{label}] {table}: first version starts at the earliest source sequence",
                    t["first_start_mismatch_keys"] == 0,
                    f"{t['first_start_mismatch_keys']} mismatching keys",
                )
            else:
                check(
                    f"[{label}] {table}: upsertSequence matches the latest source sequence",
                    t["upsert_sequence_mismatch_rows"] == 0,
                    f"{t['upsert_sequence_mismatch_rows']} mismatching keys",
                )
    check(
        "[r2] fact_race_results grew (new race upserted)",
        r2["tables"]["fact_race_results"]["rows"] > r1["tables"]["fact_race_results"]["rows"],
        f"{r1['tables']['fact_race_results']['rows']} -> {r2['tables']['fact_race_results']['rows']}",
    )
    for table in TABLES:
        check(
            f"[r2 -> rerun] {table}: re-ingesting the same race is idempotent",
            r2["tables"][table]["content_hash"] == rerun["tables"][table]["content_hash"]
            and r2["tables"][table]["rows"] == rerun["tables"][table]["rows"]
            and r2["tables"][table].get("history_rows") == rerun["tables"][table].get("history_rows"),
            f"rows {r2['tables'][table]['rows']} -> {rerun['tables'][table]['rows']}",
        )
    for view in GOLD_VIEWS:
        rows = rerun["gold_rows"].get(view)
        check(f"gold {view} populated", bool(rows), str(rows))

    # data sanity: values typed out of the API's strings, and a result for every ingested race
    for snap, expected_races in ((r1, 1), (r2, 2), (rerun, 2)) + (((history, HISTORY_RACE_COUNT),) if history else ()):
        label, san = snap["label"], snap["sanity"]
        check(f"[{label}] fact_race_results covers every ingested race", san["races"] == expected_races, f"{san['races']} (expected {expected_races})")
        check(f"[{label}] every result has a driver and a constructor", san["results_missing_driver_or_team"] == 0, str(san["results_missing_driver_or_team"]))
        check(f"[{label}] every winner has a race time", san["winners"] > 0 and san["winners_without_race_time"] == 0, f"{san['winners_without_race_time']} of {san['winners']} winners without time")
        check(f"[{label}] at least 90% of qualifiers have a Q1 time", san["qualifying_q1_coverage"] >= 0.9, f"{san['qualifying_q1_coverage']:.0%}")
        check(f"[{label}] every pit stop has a parsed duration", san["pit_stops_without_duration"] == 0, str(san["pit_stops_without_duration"]))
        check(f"[{label}] every circuit has a track_type", san["circuits_without_track_type"] == 0, f"{san['circuits_without_track_type']} without")
    # reference data is loaded in two steps: the initial file before race 1, the incremental one before race 2
    check(
        "[r2] incremental reference file was ingested (circuit_track_type grew)",
        r2["sanity"]["track_type_rows"] > r1["sanity"]["track_type_rows"],
        f"{r1['sanity']['track_type_rows']} -> {r2['sanity']['track_type_rows']} rows",
    )
    check(
        "[r2 -> rerun] re-running does not reload reference data",
        rerun["sanity"]["track_type_rows"] == r2["sanity"]["track_type_rows"],
        f"{r2['sanity']['track_type_rows']} -> {rerun['sanity']['track_type_rows']} rows",
    )
    if history:
        check_history(history, check)
    return checks


# the races of the history phase in validate.sh (2024 R1, R2 and R24, 2025 R1 and R3), i.e. the races ingested by then
HISTORY_RACE_COUNT = 5
# driver -> team over the ingested races, from the Jolpica API (chronological): real transfers, nothing simulated
EXPECTED_TEAM_HISTORY = {
    "hamilton": ["mercedes", "ferrari"],
    "sainz": ["ferrari", "williams"],
    "tsunoda": ["rb", "red_bull"],
    "lawson": ["rb", "red_bull", "rb"],
    "ocon": ["alpine", "haas"],
}


def check_history(history: dict, check) -> None:
    """SCD2 with real data: drivers who changed team must have one version per team stint, in a closed chain."""
    drivers = history.get("scd2_history", {}).get("dim_drivers", {})
    check("[history] SCD2: drivers with more than one version exist", len(drivers) > 0, f"{len(drivers)} drivers")
    for driver, teams in EXPECTED_TEAM_HISTORY.items():
        versions = drivers.get(driver, [])
        chained = all(a["end"] == b["start"] for a, b in zip(versions, versions[1:])) and (
            not versions or versions[-1]["end"] is None
        )
        check(
            f"[history] SCD2: {driver} has versions {' -> '.join(teams)}, closed in order, last one current",
            [v["value"] for v in versions] == teams and chained,
            " -> ".join(f"{v['value']}[{v['start']}..{v['end']}]" for v in versions) or "no versions",
        )
    one_stint = [d for d in drivers if d not in EXPECTED_TEAM_HISTORY]
    check(
        "[history] SCD2: every other multi-version driver is a transfer too (consecutive versions differ)",
        all(all(a["value"] != b["value"] for a, b in zip(drivers[d], drivers[d][1:])) for d in one_stint),
        f"{len(one_stint)} other drivers with history",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--season", default="")
    parser.add_argument("--rounds", default="")
    parser.add_argument("--skip-env", action="store_true", help="leave out the env.json checks (cluster already destroyed)")
    args = parser.parse_args()

    needed = ["r1", "r2", "r2_rerun"]
    missing = [f"{label}.json" for label in needed if not (args.results_dir / f"{label}.json").exists()]
    if missing:
        sys.exit(f"missing result files in {args.results_dir}: {', '.join(missing)} (run verify.py on the cluster, then sync the results)")
    checks = evaluate(args.results_dir, skip_env=args.skip_env)
    failed = [c for c in checks if not c[1]]
    lines = [
        "# Validation report: Spark 4.3 Auto CDC (SCD1 and SCD2) on EMR",
        "",
        f"Races ingested: season {args.season}, rounds {args.rounds}, then round "
        f"{args.rounds.split(',')[-1] if args.rounds else ''} again.",
        f"Result: **{'PASS' if not failed else 'FAIL'}** ({len(checks) - len(failed)}/{len(checks)} checks passed)",
        *(["", "Environment checks were skipped (`--skip-env`): Spark/Java/YARN versions were not verified in this report."] if args.skip_env else []),
        "",
        "| Check | Result | Detail |",
        "|---|---|---|",
    ]
    for name, ok, detail in checks:
        lines.append(f"| {name} | {'PASS' if ok else 'FAIL'} | {detail} |")
    args.report.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
