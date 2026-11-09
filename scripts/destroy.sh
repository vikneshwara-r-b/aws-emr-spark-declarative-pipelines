#!/usr/bin/env bash
# Destroy the CDK stacks. Mirrors deploy.sh --stage.
#
#   scripts/destroy.sh --profile <profile> [--region <region>] [--stage cluster|platform|all]
#
#   --stage cluster    only the EMR cluster stack (stops the EMR bill; keeps VPC, NAT, S3, ECR, Glue tables)
#   --stage platform   only the platform stack; refuses while the cluster stack still exists
#   --stage all        cluster stack, then platform stack (default; stops all billing)
#   --keep-platform    old spelling of --stage cluster
#
# For the cluster stage alone there is also scripts/destroy_cluster.sh.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

STAGE="all"
COMMON=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) STAGE="${2:?--stage needs a value}"; shift 2 ;;
    --keep-platform) STAGE="cluster"; shift ;;
    -p|--profile|-r|--region) COMMON+=("$1" "$2"); shift 2 ;;
    *) echo "error: unrecognized argument: $1" >&2; exit 1 ;;
  esac
done
case "$STAGE" in cluster|platform|all) ;; *) echo "error: --stage must be cluster, platform or all" >&2; exit 1 ;; esac
parse_common_args "${COMMON[@]+"${COMMON[@]}"}"

stack_exists() { aws cloudformation describe-stacks --stack-name "$1" >/dev/null 2>&1; }

# EMR adds its own security-group rules at runtime (tcp/udp/icmp rules in which the primary and
# core groups reference each other). CloudFormation doesn't know about them, so each group still
# "has a dependent object" and the platform stack fails with DELETE_FAILED. Revoke every
# group-to-group rule in the EMR groups before deleting them.
revoke_group_references() {
  local sg rule_ids
  for sg in "$@"; do
    [[ -n "$sg" ]] || continue
    for direction in false true; do
      rule_ids="$(aws ec2 describe-security-group-rules --filters "Name=group-id,Values=$sg" \
        --query "SecurityGroupRules[?ReferencedGroupInfo!=\`null\` && IsEgress==\`$direction\`].SecurityGroupRuleId" \
        --output text 2>/dev/null || true)"
      [[ -n "$rule_ids" && "$rule_ids" != "None" ]] || continue
      echo "Revoking group-to-group rules in $sg (egress=$direction): $rule_ids"
      if [[ "$direction" == "true" ]]; then
        # shellcheck disable=SC2086
        aws ec2 revoke-security-group-egress --group-id "$sg" --security-group-rule-ids $rule_ids >/dev/null
      else
        # shellcheck disable=SC2086
        aws ec2 revoke-security-group-ingress --group-id "$sg" --security-group-rule-ids $rule_ids >/dev/null
      fi
    done
  done
}

activate_venv
cd "$PROJECT_ROOT/infra"

if [[ "$STAGE" == "cluster" || "$STAGE" == "all" ]]; then
  if stack_exists "$CLUSTER_STACK"; then
    # Deleting the stack terminates the EMR cluster. Works whether the cluster is running or was
    # already terminated by hand (which leaves the stack looking CREATE_COMPLETE).
    cdk destroy "$CLUSTER_STACK" --profile "$AWS_PROFILE" --force
  else
    echo "$CLUSTER_STACK does not exist, nothing to do for the cluster stage."
  fi
fi

if [[ "$STAGE" == "platform" || "$STAGE" == "all" ]]; then
  if stack_exists "$CLUSTER_STACK"; then
    echo "error: $CLUSTER_STACK still exists and depends on the platform stack. Run with --stage cluster first." >&2
    exit 1
  fi
  if stack_exists "$PLATFORM_STACK"; then
    revoke_group_references \
      "$(stack_output "$PLATFORM_STACK" PrimarySecurityGroupId)" \
      "$(stack_output "$PLATFORM_STACK" CoreSecurityGroupId)" \
      "$(stack_output "$PLATFORM_STACK" ServiceAccessSecurityGroupId)"
    cdk destroy "$PLATFORM_STACK" --profile "$AWS_PROFILE" --force
  else
    echo "$PLATFORM_STACK does not exist, nothing to do for the platform stage."
  fi
fi
