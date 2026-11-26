#!/usr/bin/env bash
# Milestone 7: drift detection on every MediSlot stack. Exits 1 if any stack is not IN_SYNC.
# Usage: ENVIRONMENT=dev AWS_REGION=ap-south-1 bash scripts/ops/drift_check.sh
set -euo pipefail
ENVIRONMENT="${ENVIRONMENT:-dev}"
REGION="${AWS_REGION:-ap-south-1}"
STACKS="iam network data auth observability dashboard"
failed=0
for s in $STACKS; do
  stack="medislot-${ENVIRONMENT}-${s}"
  if ! aws cloudformation describe-stacks --stack-name "$stack" --region "$REGION" >/dev/null 2>&1; then
    echo "skip   $stack (not deployed)"; continue
  fi
  id="$(aws cloudformation detect-stack-drift --stack-name "$stack" --region "$REGION" --query StackDriftDetectionId --output text)"
  for _ in $(seq 1 60); do
    st="$(aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id "$id" --region "$REGION" \
          --query DetectionStatus --output text)"
    [[ "$st" == "DETECTION_IN_PROGRESS" ]] || break
    sleep 5
  done
  drift="$(aws cloudformation describe-stack-drift-detection-status --stack-drift-detection-id "$id" --region "$REGION" \
           --query StackDriftStatus --output text)"
  printf '%-8s %s\n' "$drift" "$stack"
  if [[ "$drift" != "IN_SYNC" ]]; then
    failed=1
    aws cloudformation describe-stack-resource-drifts --stack-name "$stack" --region "$REGION" \
      --stack-resource-drift-status-filters MODIFIED DELETED \
      --query 'StackResourceDrifts[*].[LogicalResourceId,ResourceType,StackResourceDriftStatus]' --output table
  fi
done
if [[ "$failed" == "1" ]]; then
  echo "::error::At least one stack has drifted. Undo the console change or put it into the template, then redeploy."
  exit 1
fi
echo "All MediSlot stacks are IN_SYNC."
