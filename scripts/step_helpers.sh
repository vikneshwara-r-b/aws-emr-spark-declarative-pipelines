#!/usr/bin/env bash
# Helpers for running the validation steps by hand (bash or zsh). Source it, don't run it:
#
#   P=<your-aws-profile>; R=us-east-1
#   source scripts/step_helpers.sh
#   init_env
#
# Then use: step_log, show_result and the variables init_env sets (BUCKET, PREFIX, DATABASE, CLUSTER_ID).
# Steps themselves are submitted with scripts/run_pipeline.sh and scripts/run_spark_script.sh.

: "${P:?set P to your AWS profile name first}"
: "${R:=us-east-1}"

stack_out() {
  aws cloudformation describe-stacks --stack-name "$1" --profile "$P" --region "$R" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" --output text
}

init_env() {
  BUCKET="$(stack_out EmrSdpPlatformStack BucketName)"
  DATABASE="$(stack_out EmrSdpPlatformStack GlueDatabase)"
  CLUSTER_ID="$(stack_out EmrSdpClusterStack ClusterId)"
  PREFIX=f1-pipeline
  export BUCKET DATABASE CLUSTER_ID PREFIX
  echo "bucket=$BUCKET database=$DATABASE cluster=$CLUSTER_ID"
}

# step_log STEP_ID [stderr|stdout] [LINES]: prints the tail of a step log. EMR copies logs to S3 a few
# minutes after a step ends, so it may say "does not exist" at first.
step_log() {
  aws s3 cp "s3://$BUCKET/$PREFIX/emr-logs/$CLUSTER_ID/steps/$1/${2:-stderr}.gz" - --profile "$P" --region "$R" | gunzip | tail -n "${3:-60}"
}

# show_result LABEL: prints the verification summary for r1, r2 or r2_rerun (rows, duplicate keys, parity and upsertSequence mismatches, SCD2 versions).
show_result() {
  aws s3 cp "s3://$BUCKET/$PREFIX/validation/results/$1.json" - --profile "$P" --region "$R" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("spark", d["spark_version"], "| label", d["label"])
print("%-20s %8s %10s %8s %8s %8s" % ("table", "rows", "dup_keys", "parity", "upseq", "versions"))
for t, v in d["tables"].items():
    # SCD1 tables report upseq (upsertSequence mismatches); SCD2 tables report versions (history rows) and broken links.
    print("%-20s %8s %10s %8s %8s %8s" % (t, v["rows"], v["duplicate_keys"], v["parity_mismatch_rows"],
          v.get("upsert_sequence_mismatch_rows", "-"), v.get("history_rows", "-")))
    if v.get("broken_version_links") or v.get("first_start_mismatch_keys"):
        print("   !! %s: %s broken version links, %s first-start mismatches" % (t, v.get("broken_version_links"), v.get("first_start_mismatch_keys")))
print("gold rows:", d["gold_rows"])
'
}
