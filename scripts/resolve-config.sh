#!/usr/bin/env bash
#
# Resolves config.env into the values the other scripts use. Sourced, not run:
#
#   . "$ROOT/scripts/resolve-config.sh"
#
# Expects $ROOT to be the package root. apply.sh and access.sh need the same
# answers, so the fallback logic lives here rather than twice.
#
# Every value config.env leaves unset falls back to a minikube default, which is
# why an untouched checkout comes up locally with no editing. Those defaults
# assume the stand-ins in testing/; where a fallback makes no sense without them,
# the value is required instead - see DAGS_GIT_REPO.

# A value the caller set wins over config.env, which sourcing would otherwise
# overwrite - scripts/apply-standins.sh depends on that.
_standins_override="${LOCAL_STANDINS:-}"

# Sourced with `if`, not `&&`: under `set -e` a compound with `&&` swallows a
# failure inside config.env, leaving defaults in place instead of stopping.
if [ -f "$ROOT/config.env" ]; then
  # shellcheck source=../config.env
  . "$ROOT/config.env"
fi

# Whether to deploy the stand-ins in testing/ alongside the platform. See the
# top of config.env.
LOCAL_STANDINS="${_standins_override:-${LOCAL_STANDINS:-true}}"
unset _standins_override

# Several defaults below are built from the namespace, so it comes first.
# scripts/apply.sh sets it from its argument.
NAMESPACE="${NAMESPACE:-lakehouse}"

# The address every per-product hostname falls back to. On minikube that is the
# node. On any other cluster there is no such default, so either EXTERNAL_HOST or
# every per-product hostname has to be set.
EXTERNAL_HOST="${EXTERNAL_HOST:-}"
if [ -z "$EXTERNAL_HOST" ] && command -v minikube >/dev/null 2>&1; then
  EXTERNAL_HOST="$(minikube ip)"
fi

if [ -z "$EXTERNAL_HOST" ]; then
  unresolved=""
  for name in KEYCLOAK_HOSTNAME AIRFLOW_URL TRINO_HOSTNAME SUPERSET_HOSTNAME \
              NIFI_HOSTNAME; do
    eval "value=\${$name:-}"
    [ -z "$value" ] && unresolved="$unresolved  $name
"
  done
  if [ -n "$unresolved" ]; then
    echo "These have no value and no default, because EXTERNAL_HOST is unset" >&2
    echo "and this is not a minikube cluster:" >&2
    echo >&2
    printf '%s' "$unresolved" >&2
    echo >&2
    echo "Set EXTERNAL_HOST in config.env to the address the products are" >&2
    echo "reached on - every one of the above then defaults to it - or set each" >&2
    echo "of them explicitly. See the top of config.env." >&2
    exit 1
  fi
fi

KEYCLOAK_HOSTNAME="${KEYCLOAK_HOSTNAME:-$EXTERNAL_HOST}"
KEYCLOAK_PORT="${KEYCLOAK_PORT:-31443}"
AIRFLOW_URL="${AIRFLOW_URL:-http://$EXTERNAL_HOST:31081}"
TRINO_HOSTNAME="${TRINO_HOSTNAME:-$EXTERNAL_HOST:31080}"
SUPERSET_HOSTNAME="${SUPERSET_HOSTNAME:-$EXTERNAL_HOST:31082}"
NIFI_HOSTNAME="${NIFI_HOSTNAME:-$EXTERNAL_HOST}"
NIFI_LISTENER_CLASS="${NIFI_LISTENER_CLASS:-external-unstable}"
KEYCLOAK_BACKCHANNEL_DYNAMIC="${KEYCLOAK_BACKCHANNEL_DYNAMIC:-true}"

# The local git server is a stand-in and has no entry in config.env. This is the
# address a browser reaches it on, which Forgejo builds its links from.
FORGEJO_URL="${FORGEJO_URL:-https://$EXTERNAL_HOST:31091/}"

# The object store's CA, and the same trust material as a mountable volume for
# Airflow's boto3 client. Two variables because a SecretClass and a volume source
# are different shapes, not because they are different CAs. YAML flow style, so
# each substitutes as a single line.
S3_CA="${S3_CA:-caCert: {secretClass: tls\}}"
S3_CA_VOLUME="${S3_CA_VOLUME:-configMap: {name: cluster-internal-ca\}}"
KEYCLOAK_CA="${KEYCLOAK_CA:-caCert: {secretClass: tls\}}"

# The same object store once more, as a whole URL: Airflow writes task logs with
# boto3, which reads a connection string rather than the S3Connection. The
# default matches an object store in the namespace being installed into.
# scripts/apply.sh checks the two agree - a mismatch is silent, the task succeeds
# and only its log is lost.
S3_ENDPOINT="${S3_ENDPOINT:-https://minio.$NAMESPACE.svc.cluster.local:9000}"

# Where Airflow reads its DAGs from. With the stand-ins it defaults to the
# Forgejo of testing/02-forgejo.yaml, by fully qualified name because the
# platform CA issues certificates for nothing shorter. Without them there is no
# sensible default: git-sync exits after repeated fetch failures, so a wrong
# value brings Airflow up NotReady with the reason three containers deep.
if [ "$LOCAL_STANDINS" != true ] && [ -z "${DAGS_GIT_REPO:-}" ]; then
  echo "DAGS_GIT_REPO is not set in config.env, and LOCAL_STANDINS is false so" >&2
  echo "the stand-in git server that would provide a default is not deployed." >&2
  echo "Set it to your DAG repository, and DAGS_GIT_TLS to match its scheme." >&2
  exit 1
fi
DAGS_GIT_REPO="${DAGS_GIT_REPO:-https://forgejo.$NAMESPACE.svc.cluster.local:3000/lakehouse/airflow-dags.git}"
DAGS_GIT_BRANCH="${DAGS_GIT_BRANCH:-main}"
DAGS_GIT_FOLDER="${DAGS_GIT_FOLDER:-dags}"

# The whole value of the AirflowCluster's `tls:` field. Written with an `if`
# rather than `${VAR:-default}` because the default's nested braces are
# unreadable once escaped inside a parameter expansion.
if [ -z "${DAGS_GIT_TLS:-}" ]; then
  DAGS_GIT_TLS='{verification: {server: {caCert: {secretClass: tls}}}}'
fi

# The one podOverrides entry in 51-airflow.yaml that cannot be written inline. It
# gives the CA volume git-sync verifies against a week-long certificate, and that
# volume exists only while the line above names a SecretClass. Under `webPki` or
# plain `http://` the operator creates no such volume, and a podOverride naming
# one it did not create leaves the operator retrying an invalid StatefulSet
# forever - so the placeholder resolves to nothing and the line disappears.
#
# `ca-cert-0` is indexed per CA: one dagsGitSync entry means index 0.
case "$DAGS_GIT_TLS" in
  *secretClass*)
    DAGS_GIT_CA_VOLUME="- {name: ca-cert-0, ephemeral: {volumeClaimTemplate: {metadata: {annotations: {secrets.stackable.tech/backend.autotls.cert.lifetime: 7d}}, spec: {}}}}"
    ;;
  *)
    DAGS_GIT_CA_VOLUME=""
    ;;
esac

# Keycloak's issuer claim carries no port when it is the default one, and the
# operators drop it from the URLs they build too. An explicit :443 would produce
# an issuer mismatch that only surfaces at login.
if [ "$KEYCLOAK_PORT" = "443" ]; then
  KEYCLOAK_URL="https://$KEYCLOAK_HOSTNAME"
else
  KEYCLOAK_URL="https://$KEYCLOAK_HOSTNAME:$KEYCLOAK_PORT"
fi
