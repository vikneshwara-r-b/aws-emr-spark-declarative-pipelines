#!/usr/bin/env bash
# Shared helpers for the scripts in this folder. Source this, don't run it.

PLATFORM_STACK="EmrSdpPlatformStack"
CLUSTER_STACK="EmrSdpClusterStack"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Reads a key from infra/cdk.json "context" (overridable via CDK_CONTEXT_<KEY> env var).
cdk_context() {
  python3 -I - "$PROJECT_ROOT/infra/cdk.json" "$1" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["context"][sys.argv[2]])
PY
}

# Parses --profile/-p <name> and --region/-r <name>; falls back to AWS_PROFILE / AWS_REGION /
# the cdk.json region. Leaves any other arguments in REMAINING_ARGS.
parse_common_args() {
  REMAINING_ARGS=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -p|--profile) AWS_PROFILE="${2:?--profile needs a value}"; shift 2 ;;
      -r|--region)  AWS_REGION="${2:?--region needs a value}"; shift 2 ;;
      *) REMAINING_ARGS+=("$1"); shift ;;
    esac
  done

  if [[ -z "${AWS_PROFILE:-}" ]]; then
    echo "error: no AWS profile. Pass --profile <your-aws-profile> or export AWS_PROFILE." >&2
    exit 1
  fi
  export AWS_PROFILE
  AWS_REGION="${AWS_REGION:-$(cdk_context region)}"
  export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

  CDK_DEFAULT_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)" || {
    echo "error: could not authenticate with profile '$AWS_PROFILE'" >&2
    exit 1
  }
  export CDK_DEFAULT_ACCOUNT CDK_DEFAULT_REGION="$AWS_REGION"
  echo "Using profile=$AWS_PROFILE account=$CDK_DEFAULT_ACCOUNT region=$AWS_REGION"
}

# stack_output <stack> <OutputKey>: prints the output value, or nothing if absent.
stack_output() {
  aws cloudformation describe-stacks --stack-name "$1" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue" --output text 2>/dev/null | grep -v '^None$' || true
}

# Loads BUCKET / PREFIX / DATABASE / CLUSTER_ID (cluster optional) from deployed stacks.
load_deployment() {
  BUCKET="$(stack_output "$PLATFORM_STACK" BucketName)"
  [[ -n "$BUCKET" ]] || { echo "error: $PLATFORM_STACK is not deployed (run scripts/deploy.sh --stage platform)." >&2; exit 1; }
  PREFIX="$(cdk_context prefix)"
  DATABASE="$(stack_output "$PLATFORM_STACK" GlueDatabase)"
  CLUSTER_ID="$(stack_output "$CLUSTER_STACK" ClusterId)"
}

require_cluster() {
  [[ -n "${CLUSTER_ID:-}" ]] || { echo "error: $CLUSTER_STACK is not deployed (run scripts/deploy.sh --stage cluster)." >&2; exit 1; }
}

# add_emr_step <name> <shell command run on the primary node>; prints the step id.
add_emr_step() {
  local steps
  steps="$(python3 -I - "$1" "$2" <<'PY'
import json, sys
print(json.dumps([{
    # The --steps JSON of the aws cli is flat (Jar and Args at the top level). The nested
    # HadoopJarStep form is the raw API shape and the cli rejects it.
    "Type": "CUSTOM_JAR",
    "Name": sys.argv[1],
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": ["bash", "-c", sys.argv[2]],
}]))
PY
)"
  aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "$steps" --query 'StepIds[0]' --output text
}

# wait_for_step <step-id>: blocks until the step finishes; non-zero exit unless COMPLETED.
wait_for_step() {
  local state
  while true; do
    state="$(aws emr describe-step --cluster-id "$CLUSTER_ID" --step-id "$1" --query 'Step.Status.State' --output text)"
    case "$state" in
      COMPLETED) echo "step $1: COMPLETED"; return 0 ;;
      FAILED|CANCELLED|INTERRUPTED)
        echo "step $1: $state" >&2
        aws emr describe-step --cluster-id "$CLUSTER_ID" --step-id "$1" \
          --query 'Step.Status.FailureDetails' --output json >&2 || true
        echo "logs: s3://$BUCKET/$PREFIX/emr-logs/$CLUSTER_ID/steps/$1/" >&2
        return 1 ;;
      *) sleep 20 ;;
    esac
  done
}

activate_venv() {
  if [[ -z "${VIRTUAL_ENV:-}" && -f "$PROJECT_ROOT/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.venv/bin/activate"
  fi
}
