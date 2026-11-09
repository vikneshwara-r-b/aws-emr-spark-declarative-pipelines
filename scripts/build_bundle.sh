#!/usr/bin/env bash
# Run the builder image to produce the Spark 4.3 bundle and upload it to S3, where the EMR
# bootstrap action picks it up. Uses the image from ECR (pulling it if not present
# locally), or a local image tagged emr-sdp-builder with --local.
#
#   scripts/build_bundle.sh --profile <profile> [--region <region>] [--local | --skip-build]
#
# --skip-build uploads an already-built build/bundle/spark-<version>-bundle.tar.gz (e.g. from
# an earlier or interrupted run) without running Docker. A fresh build downloads ~470 MB of
# Spark plus Python packages and takes roughly 5-10 minutes.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

LOCAL=false
SKIP_BUILD=false
ARGS=()
for a in "$@"; do
  case "$a" in
    --local) LOCAL=true ;;
    --skip-build) SKIP_BUILD=true ;;
    *) ARGS+=("$a") ;;
  esac
done
parse_common_args "${ARGS[@]+"${ARGS[@]}"}"
load_deployment

SPARK_VERSION="$(cdk_context spark_version)"
OUT_DIR="$PROJECT_ROOT/build/bundle"
mkdir -p "$OUT_DIR"

if $SKIP_BUILD; then
  [[ -f "$OUT_DIR/spark-$SPARK_VERSION-bundle.tar.gz" && -f "$OUT_DIR/spark-$SPARK_VERSION-bundle.tar.gz.sha256" ]] || {
    echo "error: no built bundle in $OUT_DIR (run without --skip-build first)." >&2
    exit 1
  }
  echo "Skipping build, uploading existing bundle"
elif $LOCAL; then
  IMAGE="emr-sdp-builder:local"
  docker build --platform linux/amd64 -f "$PROJECT_ROOT/docker/Dockerfile.builder" -t "$IMAGE" "$PROJECT_ROOT"
else
  REPO_URI="$(stack_output "$PLATFORM_STACK" EcrRepositoryUri)"
  IMAGE="$REPO_URI:spark-$SPARK_VERSION"
  aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO_URI%%/*}"
  docker pull --platform linux/amd64 "$IMAGE"
fi

$SKIP_BUILD || docker run --rm --platform linux/amd64 -e SPARK_VERSION="$SPARK_VERSION" -e SKIP_JVM_SMOKE="${SKIP_JVM_SMOKE:-}" -v "$OUT_DIR:/out" "$IMAGE"

BUNDLE="$OUT_DIR/spark-$SPARK_VERSION-bundle.tar.gz"
DEST="s3://$BUCKET/$PREFIX/bundles/$(basename "$BUNDLE")"
aws s3 cp "$BUNDLE" "$DEST"
aws s3 cp "$BUNDLE.sha256" "$DEST.sha256"
echo "Bundle uploaded to $DEST"
