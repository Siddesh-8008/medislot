#!/usr/bin/env bash
# Builds every Lambda zip exactly the way .github/workflows/deploy.yaml does, and checks each one is usable.
# Used by CI (ci.yaml "Lambda package builds"). Nothing is uploaded.
set -euo pipefail

cd "$(dirname "$0")/.."
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

FUNCTIONS="authentication authorizer rds_check doctor_domain slot_materialiser patient_domain doctor_discovery"
MAX_BYTES=$((50 * 1024 * 1024))

for fn in $FUNCTIONS; do
  [[ -f "lambdas/${fn}/lambda_function.py" ]] || { echo "::error::lambdas/${fn}/lambda_function.py is missing"; exit 1; }
  [[ -f "lambdas/${fn}/requirements.txt" ]]   || { echo "::error::lambdas/${fn}/requirements.txt is missing"; exit 1; }
  tmp="$(mktemp -d)"
  cp -R "lambdas/${fn}/." "$tmp/"
  cp lambdas/common.py "$tmp/common.py"
  pip install --quiet -r "lambdas/${fn}/requirements.txt" -t "$tmp"
  (cd "$tmp" && zip -qr "$OUT/${fn}.zip" .)
  rm -rf "$tmp"
  python - "$OUT/${fn}.zip" "$MAX_BYTES" <<'PY'
import sys, zipfile, os
path, limit = sys.argv[1], int(sys.argv[2])
names = zipfile.ZipFile(path).namelist()
for needed in ("lambda_function.py", "common.py"):
    assert needed in names, f"{os.path.basename(path)} is missing {needed}"
size = os.path.getsize(path)
assert size < limit, f"{os.path.basename(path)} is {size} bytes (limit {limit})"
print(f"ok  {os.path.basename(path)}  {size/1024:.0f} KiB")
PY
done
echo "All ${FUNCTIONS// /, } packages built."
