#!/usr/bin/env bash
# Build the Docker builder image (docker/Dockerfile.builder) and push it to the ECR repo
# created by the platform stack.
#
#   scripts/build_and_push_image.sh --profile <profile> [--region <region>]
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
parse_common_args "$@"
load_deployment

REPO_URI="$(stack_output "$PLATFORM_STACK" EcrRepositoryUri)"
SPARK_VERSION="$(cdk_context spark_version)"
REGISTRY="${REPO_URI%%/*}"

aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY"
# Always linux/amd64: the bundle runs on x86_64 EMR nodes whatever the local machine is.
docker build --platform linux/amd64 \
  --build-arg SPARK_VERSION="$SPARK_VERSION" \
  -f "$PROJECT_ROOT/docker/Dockerfile.builder" \
  -t "$REPO_URI:spark-$SPARK_VERSION" -t "$REPO_URI:latest" "$PROJECT_ROOT"
docker push "$REPO_URI:spark-$SPARK_VERSION"
docker push "$REPO_URI:latest"
echo "Pushed $REPO_URI:spark-$SPARK_VERSION"
