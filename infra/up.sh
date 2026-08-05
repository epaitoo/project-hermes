#!/usr/bin/env bash
# infra/up.sh
set -euo pipefail

cd "$(dirname "$0")"

terraform apply -auto-approve
doctl kubernetes cluster kubeconfig save "$(terraform output -raw cluster_name)"

date +%s > .session_start
echo "Cluster up. Remember: ./down.sh when you're finished."
kubectl get nodes

kubectl apply -f ../k8s/
kubectl get pods