#!/usr/bin/env bash
#
# Brings the whole setup up in one namespace, in the order the numbered filenames
# define: credentials and connections before the components that use them, Trino
# catalogs before the Trino cluster, the object store before anything writing to
# it.
#
# Usage:  ./scripts/apply.sh [namespace]        (default: lakehouse)
#
# Requires kubectl pointing at the target cluster and the operators installed
# (scripts/install-operators.sh). The namespace is created if absent. Re-running
# is safe: every step is an apply or an idempotent Job.
#
# ── With and without the stand-ins ────────────────────────────────────────────
# testing/ holds local stand-ins for the object store, the databases and the git
# server. LOCAL_STANDINS in config.env decides whether they are deployed, and
# every step that depends on them is guarded by it, so one script serves both a
# laptop and a cluster with real infrastructure.
#
# With LOCAL_STANDINS=false, three things have to be in place first and the
# script stops with the reason if they are not:
#   - the five Secrets in examples/external-credentials.yaml, applied,
#   - DAGS_GIT_REPO in config.env,
#   - manifests/02-s3.yaml and each metadataDatabase `host`, pointing at your
#     object store and PostgreSQL.
#
# External access is never applied from manifests/: examples/loadbalancer/ and
# examples/nodeports.yaml are alternatives and the platform cannot pick one. The
# local rig applies the NodePorts because the smoke test's browser logins need
# them.
#
# ── Substitutions ─────────────────────────────────────────────────────────────
# The manifests carry placeholders for everything deployment-specific. They are
# resolved from config.env and substituted on the way to the cluster, so the
# files on disk stay readable and re-appliable. `render()` below is the
# authoritative list; applying a manifest by hand means substituting by hand.
#
#   CHANGEME-NAMESPACE          needed wherever one component reaches another
#                               over TLS: the platform CA issues certificates for
#                               `<service>.<namespace>.svc.cluster.local` and no
#                               shorter name.
#   CHANGEME-KEYCLOAK-HOSTNAME  }  one address for browsers and products alike,
#   CHANGEME-KEYCLOAK-PORT      }  split because an AuthenticationClass takes the
#   CHANGEME-KEYCLOAK-URL       }  parts and Keycloak wants a whole origin.
#   CHANGEME-KEYCLOAK-CA        which CA signed the certificate at that address.
#   CHANGEME-KEYCLOAK-BACKCHANNEL-DYNAMIC  request-derived backchannel endpoints.
#   CHANGEME-S3-CA              }  the object store's CA, as a SecretClass and as
#   CHANGEME-S3-CA-VOLUME       }  a volume source for Airflow's boto3 client.
#   CHANGEME-S3-ENDPOINT        the object store as a whole URL, for that same
#                               client. Checked against 02-s3.yaml below.
#   CHANGEME-AIRFLOW-URL        Airflow's external origin; its OIDC redirect is
#                               derived from it.
#   CHANGEME-DAGS-GIT-REPO      }  the DAG repository, branch, subdirectory, and
#   CHANGEME-DAGS-GIT-BRANCH    }  the CA that signed the git server's
#   CHANGEME-DAGS-GIT-FOLDER    }  certificate.
#   CHANGEME-DAGS-GIT-TLS       }
#   CHANGEME-DAGS-GIT-CA-VOLUME the podOverride giving that CA volume a week-long
#                               certificate. Empty when there is no such volume -
#                               see scripts/resolve-config.sh.
#   CHANGEME-FORGEJO-URL        the local git server. Stand-ins only; derived.
#   CHANGEME-NIFI-HOSTNAME      NiFi refuses a Host header it does not know.
#   CHANGEME-NIFI-LISTENER-CLASS  differs between NodePort and load balancer.
#
# ── Why --server-side ─────────────────────────────────────────────────────────
# Client-side apply treats an explicit `null` as "remove this field" and the API
# server then reapplies the schema default, silently ignoring any manifest that
# turns a defaulted feature off. Server-side apply preserves it.

set -euo pipefail

NAMESPACE="${1:-lakehouse}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

. "$ROOT/scripts/resolve-config.sh"

# Only over what this script applies. dags/ carries placeholders too, but those
# files are committed to the DAG repository rather than applied here, so an
# unfilled one is a broken Spark job later rather than a reason to refuse the
# platform now. It is reported at the end instead.
if [ "$LOCAL_STANDINS" = true ]; then
  "$ROOT/scripts/check-config.sh" manifests testing
else
  "$ROOT/scripts/check-config.sh" manifests examples/external-credentials.yaml
fi
echo

echo "namespace:      $NAMESPACE"
echo "keycloak:       $KEYCLOAK_URL"
echo "airflow:        $AIRFLOW_URL"
echo "nifi:           $NIFI_HOSTNAME  (listener: $NIFI_LISTENER_CLASS)"
echo "dags:           $DAGS_GIT_REPO  (branch: $DAGS_GIT_BRANCH, folder: $DAGS_GIT_FOLDER)"
echo

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Credentials for the infrastructure this package does not deploy. Checked rather
# than applied, because they belong to the environment. A missing one otherwise
# surfaces much later, as a pod stuck in ContainerCreating naming a Secret but
# not the file it should have come from.
if [ "$LOCAL_STANDINS" != true ]; then
  missing=()
  for secret in keycloak-postgres-credentials hive-postgres-credentials \
                airflow-postgres-credentials superset-postgres-credentials \
                airflow-git-credentials; do
    kubectl get secret "$secret" -n "$NAMESPACE" >/dev/null 2>&1 || missing+=("$secret")
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "Missing in namespace $NAMESPACE: ${missing[*]}" >&2
    echo >&2
    echo "Fill in examples/external-credentials.yaml and apply it first:" >&2
    echo "  kubectl apply -n $NAMESPACE -f examples/external-credentials.yaml" >&2
    exit 1
  fi
fi

render() {
  sed -e "s|CHANGEME-NAMESPACE|$NAMESPACE|g" \
      -e "s|CHANGEME-KEYCLOAK-HOSTNAME|$KEYCLOAK_HOSTNAME|g" \
      -e "s|CHANGEME-KEYCLOAK-PORT|$KEYCLOAK_PORT|g" \
      -e "s|CHANGEME-KEYCLOAK-URL|$KEYCLOAK_URL|g" \
      -e "s|CHANGEME-KEYCLOAK-CA|$KEYCLOAK_CA|g" \
      -e "s|CHANGEME-KEYCLOAK-BACKCHANNEL-DYNAMIC|$KEYCLOAK_BACKCHANNEL_DYNAMIC|g" \
      -e "s|CHANGEME-S3-CA-VOLUME|$S3_CA_VOLUME|g" \
      -e "s|CHANGEME-S3-ENDPOINT|$S3_ENDPOINT|g" \
      -e "s|CHANGEME-S3-CA|$S3_CA|g" \
      -e "s|CHANGEME-AIRFLOW-URL|$AIRFLOW_URL|g" \
      -e "s|CHANGEME-DAGS-GIT-REPO|$DAGS_GIT_REPO|g" \
      -e "s|CHANGEME-DAGS-GIT-BRANCH|$DAGS_GIT_BRANCH|g" \
      -e "s|CHANGEME-DAGS-GIT-FOLDER|$DAGS_GIT_FOLDER|g" \
      -e "s|CHANGEME-DAGS-GIT-TLS|$DAGS_GIT_TLS|g" \
      -e "s|CHANGEME-DAGS-GIT-CA-VOLUME|$DAGS_GIT_CA_VOLUME|g" \
      -e "s|CHANGEME-FORGEJO-URL|$FORGEJO_URL|g" \
      -e "s|CHANGEME-NIFI-HOSTNAME|$NIFI_HOSTNAME|g" \
      -e "s|CHANGEME-NIFI-LISTENER-CLASS|$NIFI_LISTENER_CLASS|g" \
      -e "s|CHANGEME-EXTERNAL-HOST|$EXTERNAL_HOST|g" "$1"
}

# The object store is named in two shapes: `host` and `port` on the S3Connection
# objects, and a whole URL in Airflow's boto3 connection, which reads no
# S3Connection at all. Nothing in Kubernetes relates the two and a disagreement
# is silent - the task succeeds, only its log is never written. Checked after
# rendering, because both sides carry placeholders until then.
s3_from_manifest="$(render "$ROOT/manifests/02-s3.yaml" \
  | awk '/^  host:/ {h=$2} /^  port:/ && h != "" {print h ":" $2; h=""}' | sort -u)"
if [ "$s3_from_manifest" != "${S3_ENDPOINT#*://}" ]; then
  echo "The object store is named twice and the two do not agree:" >&2
  echo >&2
  echo "  manifests/02-s3.yaml   $(echo "$s3_from_manifest" | tr '\n' ' ')" >&2
  echo "  S3_ENDPOINT            ${S3_ENDPOINT#*://}" >&2
  echo >&2
  echo "Both S3Connection objects must share one host and port, and S3_ENDPOINT" >&2
  echo "in config.env must be that same endpoint as a URL. It is what Airflow" >&2
  echo "writes its task logs through; a mismatch loses them silently." >&2
  exit 1
fi

apply() {
  echo ">> $(basename "$1")"
  render "$1" | kubectl apply -n "$NAMESPACE" --server-side --force-conflicts -f -
}

# A completed Job's pod template is immutable, so re-applying one fails. All four
# are idempotent, so deleting them first simply re-runs them.
for job in minio-init superset-trino-connection dataset-upload dags-git-push; do
  kubectl delete job "$job" -n "$NAMESPACE" --ignore-not-found --wait >/dev/null 2>&1 || true
done

# The CA ConfigMap comes first: the object store's init job already mounts it.
echo "--- the platform CA ---"
apply "$ROOT/manifests/03-truststore.yaml"

# Stand-ins next: object store and databases. Everything else needs them.
if [ "$LOCAL_STANDINS" = true ]; then
  echo
  echo "--- testing/ (local stand-ins) ---"
  for manifest in "$ROOT"/testing/0[012]-*.yaml; do
    apply "$manifest"
  done

  # The DAGs before Airflow rather than after it: its git-sync sidecar clones at
  # startup, and a repository that does not exist yet fails the clone rather than
  # waiting for it.
  echo
  echo "--- the DAGs, into the local git server ---"
  "$ROOT/scripts/load-dags.sh" "$NAMESPACE"
fi

echo
echo "--- manifests/ (the platform) ---"
for manifest in "$ROOT"/manifests/*.yaml; do
  # Already applied above, before the stand-ins that need it.
  [ "$manifest" = "$ROOT/manifests/03-truststore.yaml" ] && continue
  apply "$manifest"
done

# The dataset needs the read-write credentials from manifests/02-s3.yaml, which
# is why it comes after the platform rather than with the rest of testing/.
if [ "$LOCAL_STANDINS" = true ]; then
  echo
  echo "--- the test dataset ---"
  "$ROOT/scripts/load-dataset.sh" "$NAMESPACE"

  # The smoke test completes real browser logins, and the OIDC redirect URIs
  # those follow are built from the ports below.
  echo
  echo "--- external access (NodePorts) ---"
  apply "$ROOT/examples/nodeports.yaml"
  apply "$ROOT/testing/05-nodeports.yaml"
fi

echo
echo "Applied. Watch the rollout with:"
echo "  kubectl get pods -n $NAMESPACE -w"
echo
if [ "$LOCAL_STANDINS" != true ]; then
  echo "Nothing here is reachable from outside the cluster yet. Apply one of:"
  echo "  examples/loadbalancer/    one hostname per product behind an ingress"
  echo "  examples/nodeports.yaml   fixed NodePorts on the node addresses"
  echo
  # Not fatal, and deliberately last: the platform is up either way, but a DAG
  # committed with these still in it submits a Spark job pointing at nothing.
  if grep -rq "CHANGEME" "$ROOT/dags" 2>/dev/null; then
    echo "Before committing dags/ to $DAGS_GIT_REPO, substitute the placeholders"
    echo "still in it - nothing here renders those files:"
    grep -rn "CHANGEME" "$ROOT/dags" \
      | grep -Ev '^[^:]+:[0-9]+:[[:space:]]*#' | sed "s|$ROOT/|  |"
    echo
  fi
fi
echo "Then:"
echo "  ./scripts/access.sh $NAMESPACE      URLs and accounts"
echo "  ./scripts/smoke-test.sh $NAMESPACE  end-to-end verification"
