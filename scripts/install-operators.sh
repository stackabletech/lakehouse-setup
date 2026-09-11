#!/usr/bin/env bash
#
# Installs the Stackable Data Platform operators with Helm.
#
# Every operator goes into the namespace `stackable-operators` from the official
# OCI registry. `helm upgrade --install` is idempotent, so re-running is safe.
#
# Usage:  ./scripts/install-operators.sh
#
# Requires: helm 3, and a kubectl context pointing at the target cluster with
#           permission to create cluster-scoped resources (CRDs, ClusterRoles).
#
# NOTE: all operators in one Stackable release must be on the same version.
#       Mixing versions is not supported and fails in ways that look like
#       product bugs.
#
# NOTE: Helm and OperatorHub installations are mutually exclusive. An operator
#       installed through OperatorHub cannot be upgraded with Helm, and the
#       other way round. Pick one path and stay on it.

set -euo pipefail

SDP_VERSION="${SDP_VERSION:-26.7.0}"
OPERATOR_NAMESPACE="stackable-operators"

# Platform operators (required by everything) followed by the product operators
# this deployment uses.
OPERATORS=(
  commons-operator
  secret-operator
  listener-operator
  hive-operator
  opa-operator
  trino-operator
  spark-k8s-operator
  airflow-operator
  superset-operator
  nifi-operator
)

echo "Installing Stackable Data Platform ${SDP_VERSION} operators into ${OPERATOR_NAMESPACE}"
echo

for operator in "${OPERATORS[@]}"; do
  echo ">> ${operator}"
  helm upgrade --install "${operator}" \
    "oci://oci.stackable.tech/sdp-charts/${operator}" \
    --version "${SDP_VERSION}" \
    --namespace "${OPERATOR_NAMESPACE}" \
    --create-namespace \
    --wait
done

echo
echo "Done. Check with:  kubectl get pods -n ${OPERATOR_NAMESPACE}"
