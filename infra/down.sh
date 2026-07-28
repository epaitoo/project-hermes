#!/usr/bin/env bash
# infra/down.sh
set -euo pipefail

cd "$(dirname "$0")"

if [[ -f .session_start ]]; then
  elapsed=$(( ($(date +%s) - $(cat .session_start)) / 60 ))
  printf 'Session ran %d minutes, roughly $%.2f\n' "$elapsed" "$(echo "$elapsed * 0.0003" | bc -l)"
  rm .session_start
fi

terraform destroy -auto-approve
echo "Destroyed. Verify at https://cloud.digitalocean.com/kubernetes/clusters"