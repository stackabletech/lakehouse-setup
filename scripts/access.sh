#!/usr/bin/env bash
#
# Prints where the user interfaces are and which accounts reach them.
#
# Usage:  ./scripts/access.sh [namespace]      (default: lakehouse)
#
# Reads the cluster only; changes nothing.

set -euo pipefail

NAMESPACE="${1:-lakehouse}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/resolve-config.sh"
HOST="$EXTERNAL_HOST"

# With a direct NodePort the port is assigned by the listener-operator rather
# than fixed, because NiFi needs the node address inside its certificate. Behind
# a load balancer it is just the public hostname. See manifests/70-nifi.yaml.
if [ "$NIFI_LISTENER_CLASS" = "external-unstable" ]; then
  NIFI_PORT="$(kubectl get listener nifi-node -n "$NAMESPACE" \
    -o jsonpath='{.status.nodePorts.https}' 2>/dev/null)"
  NIFI_URL="https://$HOST:${NIFI_PORT:-<not ready>}"
else
  NIFI_URL="https://$NIFI_HOSTNAME"
fi

# The object store and the git server are stand-ins, worth printing only while
# they are deployed. Against real infrastructure both belong to whoever runs it.
STANDIN_LINES=""
CERT_WARNING="Trino, NiFi and Keycloak serve certificates from the platform's"
if [ "$LOCAL_STANDINS" = true ]; then
  STANDIN_LINES="  MinIO       https://$HOST:31090     admin / adminadmin
  Forgejo     https://$HOST:31091     lakehouse / lakehouse-git-password"
  CERT_WARNING="Trino, NiFi, Keycloak, MinIO and Forgejo serve certificates from the platform's"
fi

# How a DAG gets deployed differs: locally a script force-pushes dags/ into the
# stand-in repository, against a real git server a commit is the whole story.
if [ "$LOCAL_STANDINS" = true ]; then
  DAG_DEPLOY="Committing there deploys a DAG; scripts/load-dags.sh re-pushes the
contents of dags/ over whatever is in it."
else
  DAG_DEPLOY="Committing there deploys a DAG - that is the whole procedure. A new
commit is live one sync period later, plus the dag processor's own interval."
fi

# The dashboard exists only while the demo data case is applied.
DEMO_LINES=""
if [ "$DEMO_DATA" = true ]; then
  DEMO_LINES="
Demo data: \`lakehouse.demo.orders\`, 20 000 synthetic webshop orders, and the
Superset dashboard \"Webshop orders\" at
  http://$HOST:31082/superset/dashboard/webshop-orders/
Every number on it is the EMEA-only, hashed view - the reasoning is in the
header of demo/20-superset-dashboard.yaml. demo/WALKTHROUGH.md is the
presenter's script.
"
fi

cat <<EOF
Namespace: $NAMESPACE
Node:      $HOST

  Keycloak    $KEYCLOAK_URL     admin / admin  (the 'master' realm)
  Trino       https://$HOST:31080
  Airflow     $AIRFLOW_URL
  Superset    http://$HOST:31082      local login at /login/dblogin
  NiFi        $NIFI_URL/nifi
$STANDIN_LINES

Airflow reads its DAGs from the repository at
  $DAGS_GIT_REPO
on branch $DAGS_GIT_BRANCH, folder $DAGS_GIT_FOLDER. $DAG_DEPLOY
$DEMO_LINES
$CERT_WARNING
internal CA, which browsers do not know. The warning is expected. NiFi's port is
not fixed - it comes from its Listener, and it changes if that Listener is
re-created.

Realm accounts (realm 'sdp'), all with the password equal to the username:

  bob     /admins     Airflow Admin, Superset Admin, NiFi full access,
                      unmasked lakehouse data
  alice   /analysts   Airflow User, Superset Gamma + SQL Lab, NiFi read-only,
                      EMEA rows only, full_name hidden, customer_id hashed,
                      email masked
  carol   (no group)  authenticates everywhere and sees nothing. This is a test
                      case, not an oversight.

Bootstrap accounts that do NOT go through Keycloak:

  Airflow   admin / airflow-admin-password
  Superset  admin / superset-admin-password     at /login/dblogin

Current pod state:
EOF

kubectl get pods -n "$NAMESPACE" 2>/dev/null \
  | sed 's/^/  /' \
  || echo "  (namespace $NAMESPACE not found)"
