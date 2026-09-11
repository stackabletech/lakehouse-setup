#!/usr/bin/env bash
#
# Runs the end-to-end verification. It takes several minutes: the DAG it
# triggers submits a real Spark job, which downloads the Iceberg libraries from
# Maven Central before it does any work.
#
# Usage:  ./scripts/smoke-test.sh [namespace]      (default: lakehouse)
#
# The checks run in a throwaway pod inside the cluster rather than on the host,
# because everything they talk to is a cluster-internal Service. The script
# itself is scripts/smoke_test.py; it is piped in on stdin so there is no image
# to build and no ConfigMap to keep in sync.
#
# Exit status is 0 only if every check passed.
#
# NOTE - the test is not read-only. It unpauses the `lakehouse_ingest` DAG and
#        triggers a run, which rewrites `lakehouse.raw.customers`. The Spark job
#        replaces partitions rather than appending, so running it repeatedly
#        leaves the same 2000 rows.

set -euo pipefail

NAMESPACE="${1:-lakehouse}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/resolve-config.sh"

# NiFi only answers on a hostname its certificate covers, and how it is reached
# depends on the topology - see manifests/70-nifi.yaml. Both cases are resolved
# out here, where kubectl is available.
if [ "$NIFI_LISTENER_CLASS" = "external-unstable" ]; then
  # Direct NodePort: the port is assigned by the listener-operator, so read it.
  NIFI_PORT="$(kubectl get listener nifi-node -n "$NAMESPACE" \
    -o jsonpath='{.status.nodePorts.https}')"
  if [ -z "$NIFI_PORT" ]; then
    echo "NiFi's Listener has no NodePort yet - is nifi-node-default-0 running?" >&2
    exit 1
  fi
  NIFI_URL="https://$EXTERNAL_HOST:$NIFI_PORT"
else
  # Behind a load balancer, at its public hostname.
  NIFI_URL="https://$NIFI_HOSTNAME"
fi

echo "Running the smoke test in namespace $NAMESPACE"
echo "  keycloak: $KEYCLOAK_URL"
echo "  nifi:     $NIFI_URL"
echo "This takes a few minutes - the Spark job resolves its dependencies first."

kubectl run smoke-test \
  --namespace "$NAMESPACE" \
  --image=docker.io/library/python:3.12-slim \
  --restart=Never \
  --rm --stdin --quiet \
  --env "NAMESPACE=$NAMESPACE" \
  --env "KEYCLOAK_URL=$KEYCLOAK_URL" \
  --env "NIFI_URL=$NIFI_URL" \
  --command -- python3 -u - < "$ROOT/scripts/smoke_test.py"
