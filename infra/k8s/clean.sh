#!/usr/bin/env bash
# k8s/clean.sh - remove Hermes workloads and their storage
set -euo pipefail

kubectl delete statefulset hermes-broker --ignore-not-found
kubectl delete service hermes-broker hermes-broker-headless --ignore-not-found
kubectl delete pvc -l app=hermes-broker --ignore-not-found

echo "Remaining PVCs (should be empty):"
kubectl get pvc --no-headers 2>/dev/null || echo "  none"