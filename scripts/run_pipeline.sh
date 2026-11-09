#!/usr/bin/env bash
# Run the F1 SDP pipeline on the EMR cluster as an EMR step, for one (season, round).
#
#   scripts/run_pipeline.sh --profile <profile> [--region <region>] \
#     --season <year> --round <round> [--mode dry-run|run] \
#     [--full-refresh <dataset>|--full-refresh-all] [--upload-reference-data [initial|incremental|all]] [--wait]
#
# One run = one race. --mode dry-run validates the graph without fetching or writing data.
# --upload-reference-data uploads the circuit_track_type reference CSVs to S3 first. The streaming table
# only reads files added since its last run, so the two files can be loaded in two steps:
#   initial      reference_data/circuit_track_type/initial_load.csv      (before the first race run)
#   incremental  reference_data/circuit_track_type/incremental_load.csv  (before a later run: picked up on its own)
#   all          both files (default when no value is given)
# Uploading never deletes anything already in S3.
# Prints the EMR step id on stdout (last line) so callers can wait on / look up logs.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

SEASON="" ROUND="" MODE="run" FULL_REFRESH="" WAIT=false UPLOAD_REF="" COMMON=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --season) SEASON="${2:?}"; shift 2 ;;
    --round) ROUND="${2:?}"; shift 2 ;;
    --mode) MODE="${2:?}"; shift 2 ;;
    --full-refresh) FULL_REFRESH="--full-refresh ${2:?}"; shift 2 ;;
    --full-refresh-all) FULL_REFRESH="--full-refresh-all"; shift ;;
    --upload-reference-data)
      UPLOAD_REF=all; shift
      if [[ "${1:-}" =~ ^(initial|incremental|all)$ ]]; then UPLOAD_REF="$1"; shift; fi ;;
    --wait) WAIT=true; shift ;;
    -p|--profile|-r|--region) COMMON+=("$1" "$2"); shift 2 ;;
    *) echo "error: unrecognized argument: $1" >&2; exit 1 ;;
  esac
done
parse_common_args "${COMMON[@]+"${COMMON[@]}"}" >&2
case "$MODE" in dry-run|run) ;; *) echo "error: --mode must be dry-run or run" >&2; exit 1 ;; esac
[[ "$MODE" == "dry-run" || ( -n "$SEASON" && -n "$ROUND" ) ]] || { echo "error: --season and --round are required for --mode run." >&2; exit 1; }
[[ -z "$FULL_REFRESH" || "$MODE" == "run" ]] || { echo "error: --full-refresh needs --mode run." >&2; exit 1; }
load_deployment
require_cluster

PKG_LOCAL="$PROJECT_ROOT/build/package"
PKG_URI="s3://$BUCKET/$PREFIX/package"
python3 -I "$PROJECT_ROOT/scripts/package_pipeline.py" --bucket "$BUCKET" --prefix "$PREFIX" \
  --database "$DATABASE" --src "$PROJECT_ROOT/pipeline/f1" --out "$PKG_LOCAL" >&2
aws s3 sync "$PKG_LOCAL" "$PKG_URI/" --delete --only-show-errors >&2

REF_SRC="$PROJECT_ROOT/reference_data/circuit_track_type"
REF_DST="s3://$BUCKET/$PREFIX/reference-data/circuit_track_type"
case "$UPLOAD_REF" in
  "") ;;
  all) aws s3 sync "$REF_SRC/" "$REF_DST/" --exclude '.DS_Store' --only-show-errors >&2 ;;
  initial|incremental)
    aws s3 cp "$REF_SRC/${UPLOAD_REF}_load.csv" "$REF_DST/${UPLOAD_REF}_load.csv" --only-show-errors >&2
    echo "Uploaded reference data: ${UPLOAD_REF}_load.csv" >&2 ;;
esac

CONF=()
[[ "$MODE" == "run" ]] && CONF+=(--conf "spark.f1.season.year=$SEASON" --conf "spark.f1.season.round=$ROUND")
CMD="aws s3 cp s3://$BUCKET/$PREFIX/bootstrap/run_pipeline_step.sh /tmp/run_pipeline_step.sh --only-show-errors && bash /tmp/run_pipeline_step.sh $PKG_URI $MODE ${CONF[*]-} $FULL_REFRESH"
STEP_ID="$(add_emr_step "f1-sdp-$MODE${SEASON:+-$SEASON-$ROUND}" "$CMD")"
echo "Submitted EMR step $STEP_ID" >&2
$WAIT && wait_for_step "$STEP_ID" >&2
echo "$STEP_ID"
