#!/usr/bin/env bash
# Milestone 7: full teardown of one MediSlot environment (everything except the IAM bootstrap stack, which holds the
# pipeline's own permissions). Afterwards run the "MediSlot CI and Deployment" workflow to rebuild it from scratch.
# The RDS instance has DeletionPolicy Snapshot, so a final snapshot is kept. SSM secrets are kept (the deploy reuses
# them; delete them too with DELETE_SECRETS=true for a truly blank start - the deploy recreates them).
# Usage: ENVIRONMENT=dev AWS_REGION=ap-south-1 CONFIRM=DELETE-dev bash scripts/ops/teardown.sh
set -euo pipefail
ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${AWS_REGION:-ap-south-1}"
[[ "${CONFIRM:-}" == "DELETE-${ENVIRONMENT}" ]] || { echo "Set CONFIRM=DELETE-${ENVIRONMENT} to tear the environment down."; exit 1; }
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"

empty_bucket() {
  local b="$1"
  aws s3api head-bucket --bucket "$b" --region "$REGION" 2>/dev/null || return 0
  echo "emptying s3://$b (all versions)"
  python3 - "$b" "$REGION" <<'PY'
import sys, boto3
bucket = boto3.resource("s3", region_name=sys.argv[2]).Bucket(sys.argv[1])
bucket.object_versions.delete()
PY
}

delete_stack() {
  local s="medislot-${ENVIRONMENT}-$1"
  aws cloudformation describe-stacks --stack-name "$s" --region "$REGION" >/dev/null 2>&1 || { echo "skip $s"; return 0; }
  echo "deleting $s"
  aws cloudformation delete-stack --stack-name "$s" --region "$REGION"
  aws cloudformation wait stack-delete-complete --stack-name "$s" --region "$REGION"
}

delete_stack dashboard
delete_stack observability
delete_stack auth
empty_bucket "medislot-${ENVIRONMENT}-artifacts-${ACCOUNT}"
empty_bucket "medislot-${ENVIRONMENT}-doctor-photos-${ACCOUNT}-${REGION}"
delete_stack data
delete_stack network
if [[ "${DELETE_SECRETS:-false}" == "true" ]]; then
  for p in db/password auth/jwt-secret web/session-secret; do
    aws ssm delete-parameter --name "/medislot/${ENVIRONMENT}/$p" --region "$REGION" 2>/dev/null || true
  done
fi
echo "Environment ${ENVIRONMENT} torn down. Re-run the deploy workflow to rebuild it from scratch."
