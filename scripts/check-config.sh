#!/usr/bin/env bash
#
# Reports every placeholder that has not been filled in yet.
# Reads files only; changes nothing.
#
# Usage:  ./scripts/check-config.sh [path ...]
#
# Without arguments it checks manifests/ and dags/, plus testing/ when
# LOCAL_STANDINS is true and examples/external-credentials.yaml when it is not -
# the Secrets there are required in that case rather than optional.
#
# The rest of examples/ is checked only on request, because those files are
# applied by hand and an unfilled placeholder in one must not block an install
# that never uses it:
#
#   ./scripts/check-config.sh manifests examples
#
# Comment lines are skipped: each file's header names its placeholders on
# purpose, and those names stay after the values are filled in.
#
# Placeholders scripts/apply.sh resolves are skipped too, but only under the
# paths it renders. Everywhere else the same placeholder is a real finding.
# dags/ is the case that matters: those files are committed to the DAG
# repository and read from the checkout, so they never pass through `render()`
# and a CHANGEME left in one reaches the cluster verbatim.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "$ROOT/config.env" ]; then
  # shellcheck source=../config.env
  . "$ROOT/config.env"
fi
LOCAL_STANDINS="${LOCAL_STANDINS:-true}"

# Exactly what `render()` in scripts/apply.sh substitutes. Keep the two in step.
RESOLVED='CHANGEME-(NAMESPACE|EXTERNAL-HOST|KEYCLOAK-(HOSTNAME|PORT|URL|CA|BACKCHANNEL-DYNAMIC)|AIRFLOW-URL|S3-(CA(-VOLUME)?|ENDPOINT)|NIFI-(HOSTNAME|LISTENER-CLASS)|DAGS-GIT-(REPO|BRANCH|FOLDER|TLS|CA-VOLUME)|FORGEJO-URL)'

TARGETS=()
for target in "$@"; do
  # Accept both a path relative to where you stand and a name like `examples`,
  # which is resolved against the package root.
  if [ -e "$target" ]; then
    TARGETS+=("$target")
  else
    TARGETS+=("$ROOT/$target")
  fi
done
if [ "${#TARGETS[@]}" -eq 0 ]; then
  TARGETS=("$ROOT/manifests" "$ROOT/dags")
  if [ "$LOCAL_STANDINS" = true ]; then
    TARGETS+=("$ROOT/testing")
  else
    TARGETS+=("$ROOT/examples/external-credentials.yaml")
  fi
fi

# Whether a path is substituted on the way to the cluster, which decides whether
# a placeholder apply.sh knows about is a finding or a normal state of the file.
# dags/ is rendered only by load-dags.sh, on the way into the stand-in git
# server; against a real repository it is committed verbatim.
rendered() {
  case "${1#"$ROOT"/}" in
    manifests|manifests/*|testing|testing/*) return 0 ;;
    dags|dags/*) [ "$LOCAL_STANDINS" = true ] ;;
    *) return 1 ;;
  esac
}

RENDERED=()
VERBATIM=()
for target in "${TARGETS[@]}"; do
  if rendered "$target"; then RENDERED+=("$target"); else VERBATIM+=("$target"); fi
done

echo "Checking for unfilled placeholders in: ${TARGETS[*]}"

# grep prints "<file>:<line>:<content>"; -H keeps the filename there when the
# target is a single file, which the comment filter depends on.
scan() {
  grep -rHn "CHANGEME" "$@" | grep -Ev '^[^:]+:[0-9]+:[[:space:]]*#' || true
}

remaining=""
if [ "${#RENDERED[@]}" -gt 0 ]; then
  remaining=$(scan "${RENDERED[@]}" | grep -Ev "$RESOLVED" || true)
fi

byhand=""
if [ "${#VERBATIM[@]}" -gt 0 ]; then
  byhand=$(scan "${VERBATIM[@]}")
fi

if [ -n "$remaining" ]; then
  echo
  echo "$remaining"
  echo
  echo "^ These placeholders still need a value."
fi

if [ -n "$byhand" ]; then
  echo
  echo "$byhand"
  echo
  echo "^ These are in files scripts/apply.sh does not render - it never reads"
  echo "  them. Substitute them by hand before applying or committing the file."
fi

if [ -n "$remaining" ] || [ -n "$byhand" ]; then
  exit 1
fi

echo "No placeholders left."
