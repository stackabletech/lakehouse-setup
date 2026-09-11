#!/usr/bin/env bash
#
# Puts the DAGs into the local git server.
#
# `dags/` holds real Python and YAML rather than a ConfigMap, so it can be
# edited, diffed and linted like source. This turns that directory into the
# ConfigMap that testing/03-dags-git.yaml mounts, and applies the Job that
# creates the repository in Forgejo and pushes the files into it.
#
# Usage:  ./scripts/load-dags.sh [namespace]     (default: lakehouse)
#
# Requires: the git server and the platform CA to exist - that is
#           testing/02-forgejo.yaml and manifests/03-truststore.yaml.
#
# Re-running replaces the ConfigMap and re-pushes. The previous Job is deleted
# first because a completed Job's pod template is immutable.
#
# `dags/` has to stay flat: a ConfigMap has no directories, so a subdirectory
# would be dropped here without a word. A DAG package that needs one belongs in
# a real repository, which is the point of git-sync - set DAGS_GIT_REPO and
# this script stops being involved at all.
#
# This waits for the push to finish, unlike scripts/load-dataset.sh. Airflow's
# git-sync sidecar needs the repository to exist by the time the pods start:
# a missing repository is reported as `remote: Repository not found`, which is
# indistinguishable from an authentication failure, and git-sync eventually
# gives up and takes the pod NotReady.

set -euo pipefail

NAMESPACE="${1:-lakehouse}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The DAGs carry the same CHANGEME-NAMESPACE placeholder the manifests do - the
# SparkApplication in them addresses the object store by its fully qualified
# name. Substituting has to happen before the ConfigMap is built, so the files
# are rendered into a temporary directory first.
RENDERED="$(mktemp -d)"
trap 'rm -rf "$RENDERED"' EXIT
for dag in "$ROOT"/dags/*; do
  # Regular files only. Importing or linting a DAG locally leaves a
  # `__pycache__` directory next to it, and `sed` on a directory is a hard
  # error that would stop the whole install.
  [ -f "$dag" ] || continue
  sed "s/CHANGEME-NAMESPACE/$NAMESPACE/g" "$dag" > "$RENDERED/$(basename "$dag")"
done

kubectl create configmap airflow-dags \
  --from-file="$RENDERED" \
  --namespace "$NAMESPACE" \
  --dry-run=client -o yaml \
  | kubectl apply -f -

kubectl delete job dags-git-push -n "$NAMESPACE" --ignore-not-found --wait
sed "s/CHANGEME-NAMESPACE/$NAMESPACE/g" "$ROOT/testing/03-dags-git.yaml" \
  | kubectl apply -n "$NAMESPACE" -f -

echo "waiting for the push to complete..."
# The Job waits for Forgejo itself, and Forgejo waits for its volumes, so the
# timeout has to cover a cold start of both.
kubectl wait --for=condition=complete job/dags-git-push \
  -n "$NAMESPACE" --timeout=300s
kubectl logs -n "$NAMESPACE" job/dags-git-push | sed 's/^/  /'
