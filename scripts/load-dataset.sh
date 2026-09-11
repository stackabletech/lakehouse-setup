#!/usr/bin/env bash
#
# Puts the test dataset into the object store.
#
# `data/customers.csv` is a real file rather than YAML so it can be edited and
# diffed like data. This turns it into the ConfigMap that testing/04-dataset.yaml
# mounts, then applies that Job.
#
# Usage:  ./scripts/load-dataset.sh [namespace]     (default: lakehouse)
#
# Requires: the object store and the read-write credentials Secret to exist -
#           that is testing/00-minio.yaml and manifests/02-s3.yaml.
#
# Re-running replaces the ConfigMap and re-runs the upload. The previous Job is
# deleted first because a completed Job's pod template is immutable.

set -euo pipefail

NAMESPACE="${1:-lakehouse}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kubectl create configmap lakehouse-dataset \
  --from-file="$ROOT/data/customers.csv" \
  --namespace "$NAMESPACE" \
  --dry-run=client -o yaml \
  | kubectl apply -f -

kubectl delete job dataset-upload -n "$NAMESPACE" --ignore-not-found --wait
# Same CHANGEME-NAMESPACE substitution scripts/apply.sh does; see the comment
# block there for why the object store is addressed by its fully qualified name.
sed "s/CHANGEME-NAMESPACE/$NAMESPACE/g" "$ROOT/testing/04-dataset.yaml" \
  | kubectl apply -n "$NAMESPACE" -f -

echo "dataset upload job submitted; follow it with:"
echo "  kubectl logs -n $NAMESPACE job/dataset-upload -f"
