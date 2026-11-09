#!/usr/bin/env bash
# Upload a PySpark script from validation/ and run it as an EMR step with Spark 4.3.
#
#   scripts/run_spark_script.sh --profile <profile> <script.py> [script args...]
# Waits for the step and exits non-zero if it fails.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

COMMON=() REST=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile|-r|--region) COMMON+=("$1" "$2"); shift 2 ;;
    *) REST+=("$1"); shift ;;
  esac
done
parse_common_args "${COMMON[@]+"${COMMON[@]}"}" >&2
load_deployment
require_cluster

SCRIPT="${REST[0]:?script path}"
ARGS=("${REST[@]:1}")
NAME="$(basename "$SCRIPT")"
aws s3 cp "$SCRIPT" "s3://$BUCKET/$PREFIX/validation/scripts/$NAME" --only-show-errors >&2
CMD="aws s3 cp s3://$BUCKET/$PREFIX/bootstrap/run_spark_script_step.sh /tmp/run_spark_script_step.sh --only-show-errors && bash /tmp/run_spark_script_step.sh s3://$BUCKET/$PREFIX/validation/scripts/$NAME ${ARGS[*]-}"
STEP_ID="$(add_emr_step "script-$NAME" "$CMD")"
echo "Submitted EMR step $STEP_ID" >&2
wait_for_step "$STEP_ID" >&2
