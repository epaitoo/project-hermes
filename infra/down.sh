#!/usr/bin/env bash
# infra/down.sh
set -euo pipefail

cd "$(dirname "$0")"

if [[ -f .session_start ]]; then
  elapsed=$(( ($(date +%s) - $(cat .session_start)) / 60 ))
  printf 'Session ran %d minutes, roughly $%.2f\n' "$elapsed" "$(echo "$elapsed * 0.0003" | bc -l)"
  rm .session_start
fi

echo "Removing PVCs so the CSI driver releases their volumes..."
kubectl delete pvc -l app=hermes-broker --ignore-not-found 2>/dev/null || true

terraform destroy -auto-approve

echo "Destroyed. Checking for leftover resources:"
doctl kubernetes cluster list --no-header || true
doctl compute volume list --no-header || true
echo "(If anything is listed above, it is still billing. Delete it.)"