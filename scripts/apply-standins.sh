#!/usr/bin/env bash
#
# Brings up ONLY the stand-ins, in a namespace of their own, so the platform can
# be installed against them as if they were pre-existing infrastructure.
#
# Usage:  ./scripts/apply-standins.sh [namespace]      (default: lakehouse-infra)
#
# ── Why this exists ───────────────────────────────────────────────────────────
# scripts/apply.sh with LOCAL_STANDINS=true brings the stand-ins and the platform
# up together in one namespace, which exercises the manifests but not the path a
# real install takes: with LOCAL_STANDINS=false the platform refuses to start
# until an object store, a PostgreSQL server and a git repository exist somewhere
# else. This script creates that "somewhere else".
#
# The intended shape is two namespaces:
#
#   ./scripts/apply-standins.sh lakehouse-infra
#   ... set LOCAL_STANDINS=false in config.env and follow this script's output ...
#   ./scripts/apply.sh lakehouse
#
# Everything crosses the namespace boundary by fully qualified Service name,
# which is what the platform CA issues certificates for anyway - so the split
# needs no TLS concessions and is a fair test rather than a rigged one.
#
# ── What it applies, and why each one ─────────────────────────────────────────
#   manifests/03-truststore.yaml   the platform CA as a ConfigMap. MinIO's init
#                                  job and the git push job both mount it.
#   manifests/02-s3.yaml           applied here for its two credential Secrets,
#                                  which the dataset upload needs. The two
#                                  S3Connection objects come along unused.
#   testing/00-minio.yaml          the object store, TLS, two scoped users
#   testing/01-postgres.yaml       one server, the four databases
#   testing/02-forgejo.yaml        the git server and its private repository
#   scripts/load-dags.sh           pushes dags/ into that repository
#   scripts/load-dataset.sh        puts data/customers.csv in the object store
#   testing/05-nodeports.yaml      the MinIO console and the Forgejo UI
#
# NOTE - `CHANGEME-NAMESPACE` in dags/ resolves to THIS namespace, not the one
#   the platform goes into. The only thing it names there is the object store
#   endpoint, which does live here. The SparkApplication itself is created by
#   Airflow in its own namespace.

set -euo pipefail

NAMESPACE="${1:-lakehouse-infra}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# This namespace holds the stand-ins themselves, so they go up regardless of
# what config.env says about the platform install. resolve-config.sh lets a
# caller-set value win over the file.
LOCAL_STANDINS=true

. "$ROOT/scripts/resolve-config.sh"

echo "stand-ins into namespace: $NAMESPACE"
echo

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# The placeholders the files below actually carry. Deliberately a short list
# rather than a copy of render() in apply.sh: if one of these files grows a
# placeholder this does not know about, the check after the substitution catches
# it instead of applying it to the cluster verbatim.
render() {
  sed -e "s|CHANGEME-NAMESPACE|$NAMESPACE|g" \
      -e "s|CHANGEME-FORGEJO-URL|$FORGEJO_URL|g" \
      -e "s|CHANGEME-S3-CA-VOLUME|$S3_CA_VOLUME|g" \
      -e "s|CHANGEME-S3-CA|$S3_CA|g" "$1"
}

apply() {
  echo ">> $(basename "$1")"
  rendered="$(render "$1")"
  if printf '%s' "$rendered" | grep -q CHANGEME; then
    echo >&2
    echo "$1 still contains a placeholder after substitution:" >&2
    printf '%s' "$rendered" | grep -n CHANGEME | grep -Ev '^[0-9]+:[[:space:]]*#' >&2
    echo >&2
    echo "Add it to render() in this script, or to config.env." >&2
    exit 1
  fi
  printf '%s' "$rendered" | kubectl apply -n "$NAMESPACE" --server-side --force-conflicts -f -
}

for job in minio-init dataset-upload dags-git-push; do
  kubectl delete job "$job" -n "$NAMESPACE" --ignore-not-found --wait >/dev/null 2>&1 || true
done

apply "$ROOT/manifests/03-truststore.yaml"
apply "$ROOT/manifests/02-s3.yaml"
apply "$ROOT/testing/00-minio.yaml"
apply "$ROOT/testing/01-postgres.yaml"
apply "$ROOT/testing/02-forgejo.yaml"

# The two Jobs below talk to MinIO and Forgejo rather than merely being
# scheduled alongside them, so they need the pods serving first.
echo
echo "waiting for the object store and the git server..."
kubectl wait --for=condition=available deploy/minio deploy/forgejo \
  -n "$NAMESPACE" --timeout=300s

echo
"$ROOT/scripts/load-dags.sh" "$NAMESPACE"
echo
"$ROOT/scripts/load-dataset.sh" "$NAMESPACE"

# The MinIO console and the Forgejo UI, on fixed ports. A convenience for
# looking at the stand-ins by hand, and the only step here that can fail for a
# reason that does not matter: 31090 and 31091 are cluster-wide, so a previous
# namespace that has not finished terminating still holds them. Everything the
# platform talks to is a Service name, so carry on either way.
echo
if ! apply "$ROOT/testing/05-nodeports.yaml"; then
  echo
  echo "WARNING: the MinIO console and Forgejo UI NodePorts were not created." >&2
  echo "Usually 31090/31091 are still held by a namespace that is terminating." >&2
  echo "Nothing else depends on them; re-run this script later to get them." >&2
fi

cat <<EOF

──────────────────────────────────────────────────────────────────────────────
Stand-ins are up in namespace '$NAMESPACE'. To install the platform against
them, make these changes and run apply.sh in a namespace of its own.

config.env
  LOCAL_STANDINS=false
  DAGS_GIT_REPO=https://forgejo.$NAMESPACE.svc.cluster.local:3000/lakehouse/airflow-dags.git
  DAGS_GIT_TLS="{verification: {server: {caCert: {secretClass: tls}}}}"
  S3_ENDPOINT=https://minio.$NAMESPACE.svc.cluster.local:9000

  S3_ENDPOINT has to match the host below: it is the object store as Airflow's
  boto3 client takes it, and its default assumes the object store shares the
  namespace being installed into, which here it does not. apply.sh stops if the
  two disagree.

  Leave S3_CA and KEYCLOAK_CA alone: these stand-ins serve the platform's own
  CA, which is what both already default to.

manifests/02-s3.yaml            both S3Connection objects
  host: minio.$NAMESPACE.svc.cluster.local
  port: 9000                    unchanged
  region: unchanged

  Leave the two credential Secrets as they are - the users the object store was
  initialised with are the ones already written there.

manifests/30-hive-metastore.yaml, 51-airflow.yaml, 60-superset.yaml
manifests/00-keycloak.yaml
  host: postgres.$NAMESPACE.svc.cluster.local

examples/external-credentials.yaml
  keycloak / keycloak-db-password        superset / superset-db-password
  hive     / hive-db-password            lakehouse / lakehouse-git-password  (git)
  airflow  / airflow-db-password

Then:
  kubectl create namespace lakehouse
  kubectl apply -n lakehouse -f examples/external-credentials.yaml
  ./scripts/apply.sh lakehouse
  kubectl apply -n lakehouse -f examples/nodeports.yaml
  ./scripts/smoke-test.sh lakehouse

Tear down with:
  kubectl delete namespace lakehouse $NAMESPACE
──────────────────────────────────────────────────────────────────────────────
EOF
