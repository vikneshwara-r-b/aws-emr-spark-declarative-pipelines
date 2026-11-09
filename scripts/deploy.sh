#!/usr/bin/env bash
# Deploy the CDK stacks.
#
#   scripts/deploy.sh --profile <profile> [--region <region>] [--stage platform|cluster|all] \
#     [--bootstrap] [-- <extra cdk args>]
#
# Typical order (the cluster's bootstrap action needs the Spark bundle in S3 first):
#   1. scripts/deploy.sh   --profile P --stage platform
#   2. scripts/build_and_push_image.sh --profile P     (builder image -> ECR)
#   3. scripts/build_bundle.sh         --profile P     (Spark 4.3 bundle -> S3)
#   4. scripts/deploy.sh   --profile P --stage cluster
#
# --bootstrap runs `cdk bootstrap` first (once per account/region; it is also done
# automatically when the CDKToolkit stack is missing). Extra args after `--` go to
# `cdk deploy` untouched, e.g. -- -c core_market=ON_DEMAND
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

STAGE="all"
DO_BOOTSTRAP=false
OWN_ARGS=()
CDK_EXTRA=()
SEEN_SEP=false
for a in "$@"; do
  if ! $SEEN_SEP && [[ "$a" == "--" ]]; then SEEN_SEP=true; continue; fi
  if $SEEN_SEP; then CDK_EXTRA+=("$a"); else OWN_ARGS+=("$a"); fi
done
set -- "${OWN_ARGS[@]+"${OWN_ARGS[@]}"}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) STAGE="${2:?--stage needs a value}"; shift 2 ;;
    --bootstrap) DO_BOOTSTRAP=true; shift ;;
    -p|--profile|-r|--region) COMMON+=("$1" "$2"); shift 2 ;;
    *) echo "error: unrecognized argument: $1" >&2; exit 1 ;;
  esac
done
parse_common_args "${COMMON[@]+"${COMMON[@]}"}"
case "$STAGE" in platform|cluster|all) ;; *) echo "error: --stage must be platform, cluster or all" >&2; exit 1 ;; esac

activate_venv
cd "$PROJECT_ROOT/infra"

if $DO_BOOTSTRAP || ! aws cloudformation describe-stacks --stack-name CDKToolkit >/dev/null 2>&1; then
  echo "==> cdk bootstrap aws://$CDK_DEFAULT_ACCOUNT/$AWS_REGION"
  cdk bootstrap "aws://$CDK_DEFAULT_ACCOUNT/$AWS_REGION" --profile "$AWS_PROFILE"
fi

if [[ "$STAGE" == "platform" || "$STAGE" == "all" ]]; then
  cdk deploy "$PLATFORM_STACK" --profile "$AWS_PROFILE" --require-approval never "${CDK_EXTRA[@]+"${CDK_EXTRA[@]}"}"
fi

if [[ "$STAGE" == "cluster" || "$STAGE" == "all" ]]; then
  load_deployment
  BUNDLE_KEY="$PREFIX/bundles/spark-$(cdk_context spark_version)-bundle.tar.gz"
  aws s3api head-object --bucket "$BUCKET" --key "$BUNDLE_KEY" >/dev/null 2>&1 || {
    echo "error: s3://$BUCKET/$BUNDLE_KEY not found. Run scripts/build_bundle.sh first." >&2
    exit 1
  }
  # The bootstrap action installs whatever bundle is in S3. A cluster launched with a stale one costs a
  # 15 minute cycle (a bundle with the wrong Iceberg runtime fails with IncompatibleClassChangeError),
  # so refuse when the S3 copy differs from the latest local build. Set ALLOW_STALE_BUNDLE=1 to bypass.
  LOCAL_SUM_FILE="${BUNDLE_DIR:-$PROJECT_ROOT/build/bundle}/$(basename "$BUNDLE_KEY").sha256"
  if [[ -f "$LOCAL_SUM_FILE" && -z "${ALLOW_STALE_BUNDLE:-}" ]]; then
    local_sum="$(cut -d' ' -f1 "$LOCAL_SUM_FILE")"
    remote_sum="$(aws s3 cp "s3://$BUCKET/$BUNDLE_KEY.sha256" - 2>/dev/null | cut -d' ' -f1 || true)"
    if [[ "$remote_sum" != "$local_sum" ]]; then
      echo "error: the bundle in S3 is not your latest local build." >&2
      echo "       S3 sha256:   ${remote_sum:-<none>}" >&2
      echo "       local sha256: $local_sum" >&2
      echo "       Run: scripts/build_bundle.sh --profile $AWS_PROFILE --skip-build   (or set ALLOW_STALE_BUNDLE=1)" >&2
      exit 1
    fi
  fi
  cdk deploy "$CLUSTER_STACK" --profile "$AWS_PROFILE" --require-approval never "${CDK_EXTRA[@]+"${CDK_EXTRA[@]}"}"
  echo "Cluster id: $(stack_output "$CLUSTER_STACK" ClusterId)"
fi
