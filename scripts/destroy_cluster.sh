#!/usr/bin/env bash
# Destroy only the EMR cluster stack, keeping the platform (VPC, NAT, S3, ECR, Glue tables).
#
#   scripts/destroy_cluster.sh --profile <profile> [--region <region>]
exec "$(dirname "${BASH_SOURCE[0]}")/destroy.sh" --stage cluster "$@"
