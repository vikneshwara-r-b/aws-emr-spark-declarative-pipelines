#!/usr/bin/env bash
# End-to-end validation of Spark 4.3 + Auto CDC (SCD1 and SCD2) on the deployed EMR cluster.
#
#   scripts/validate.sh --profile <profile> [--region <region>] [--season 2024] [--rounds 1,2]
#
# Steps: environment check -> dry-run -> race 1 -> checks -> race 2 -> checks ->
# race 2 again (idempotence) -> checks -> history races (real driver transfers, for SCD2) -> checks ->
# report (docs/validation-report.md).
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SEASON=2024 ROUNDS="1,2" COMMON=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --season) SEASON="${2:?}"; shift 2 ;;
    --rounds) ROUNDS="${2:?}"; shift 2 ;;
    -p|--profile|-r|--region) COMMON+=("$1" "$2"); shift 2 ;;
    *) echo "error: unrecognized argument: $1" >&2; exit 1 ;;
  esac
done
parse_common_args "${COMMON[@]+"${COMMON[@]}"}"
load_deployment
require_cluster
IFS=',' read -r R1 R2 <<<"$ROUNDS"
PROFILE_ARGS=(--profile "$AWS_PROFILE" --region "$AWS_REGION")
SPARK_ARGS=(--bucket "$BUCKET" --prefix "$PREFIX" --database "$DATABASE")
RESULTS="$PROJECT_ROOT/build/validation-results"
rm -rf "$RESULTS"; mkdir -p "$RESULTS"

run() { "$PROJECT_ROOT/scripts/run_pipeline.sh" "${PROFILE_ARGS[@]}" "$@" --wait >/dev/null; }
verify() { "$PROJECT_ROOT/scripts/run_spark_script.sh" "${PROFILE_ARGS[@]}" "$PROJECT_ROOT/validation/verify.py" --label "$1" "${SPARK_ARGS[@]}"; }

echo "==> Environment check"
"$PROJECT_ROOT/scripts/run_spark_script.sh" "${PROFILE_ARGS[@]}" "$PROJECT_ROOT/validation/env_check.py" "${SPARK_ARGS[@]}"
echo "==> Dry run"
run --mode dry-run --upload-reference-data initial
echo "==> Race $SEASON/$R1"
run --mode run --season "$SEASON" --round "$R1"; verify r1
echo "==> Race $SEASON/$R2 (loads the incremental reference file first)"
run --mode run --season "$SEASON" --round "$R2" --upload-reference-data incremental; verify r2
echo "==> Race $SEASON/$R2 again (idempotence)"
run --mode run --season "$SEASON" --round "$R2"; verify r2_rerun

# SCD2 needs real changes to show history. Jolpica's dimension attributes never change, but drivers change
# team: these races (2024 R24, 2025 R1, 2025 R3) contain the transfers Hamilton to Ferrari, Sainz to Williams,
# Tsunoda to Red Bull, Lawson back and forth, Ocon to Haas. They are ingested in date order. Set HISTORY=0 to skip.
if [[ "${HISTORY:-1}" != "0" ]]; then
  for race in "2024 24" "2025 1" "2025 3"; do
    read -r hist_season hist_round <<<"$race"
    echo "==> History race $hist_season/$hist_round"
    run --mode run --season "$hist_season" --round "$hist_round"
  done
  verify history
fi

aws s3 sync "s3://$BUCKET/$PREFIX/validation/results/" "$RESULTS/" --only-show-errors
python3 -I "$PROJECT_ROOT/validation/report.py" "$RESULTS" "$PROJECT_ROOT/docs/validation-report.md" \
  --season "$SEASON" --rounds "$ROUNDS"
