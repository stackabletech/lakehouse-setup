# Secured lakehouse on the Stackable Data Platform

A deployment package for a secured lakehouse on the Stackable Data Platform
(SDP) 26.7.0, installed and verified end to end on a local minikube cluster.

Apache Trino queries Apache Iceberg tables on an object store, an Apache Hive
metastore is the catalog, Open Policy Agent decides who may read what, Apache
Spark loads data, Apache Airflow orchestrates the loads, Apache Superset serves
dashboards and SQL Lab, Apache NiFi is there for dataflow, and Keycloak is the
single place where a person's identity and group membership are defined.

## Try it on minikube

An untouched checkout comes up locally with no editing: every value in
`config.env` falls back to a minikube default, and `testing/` stands in for the
object store, the databases and the git server.

```sh
minikube start --cpus 8 --memory 24g
./scripts/install-operators.sh      # once per cluster
./scripts/apply.sh lakehouse
./scripts/access.sh lakehouse       # URLs and accounts
./scripts/smoke-test.sh lakehouse   # 32 checks, end to end
```

Expect five to ten minutes on a cold cluster, most of it pulling images.

## What is in it

```
manifests/                          the platform, applied in filename order
  00-keycloak.yaml                  identity provider, realm, users, clients
  01-authentication.yaml            the shared AuthenticationClass, client secrets
  02-s3.yaml                        object store connections, read-only and read-write
  03-truststore.yaml                the platform CA, as a ConfigMap
  20-opa.yaml                       authorization engine, group lookup, audit log
  21-opa-trino-regorules.yaml       the Trino policy engine    (generic, no changes)
  22-opa-trino-policies.yaml        who may read which rows and columns
  23-opa-superset-policies.yaml     Keycloak group to Superset role
  24-opa-nifi-policies.yaml         who may read and change flows
  30-hive-metastore.yaml            the Iceberg catalog
  31-trino-catalogs.yaml            read-only and read-write catalogs, plus tpch
  32-trino.yaml                     the query engine, resource groups
  40-spark-jobs.yaml                the PySpark load script
  50-airflow-rbac.yaml              permission for Airflow to submit Spark jobs
  51-airflow.yaml                   orchestration, Keycloak login, DAGs from git,
                                    logs to S3
  60-superset.yaml                  dashboards and SQL Lab
  61-superset-trino-connection.yaml registers the Trino connection
  70-nifi.yaml                      dataflow

testing/                            local stand-ins for infrastructure you have
  00-minio.yaml                     object store, two scoped users, TLS
  01-postgres.yaml                  one server, four databases
  02-forgejo.yaml                   git server, private repo, TLS
  03-dags-git.yaml                  pushes dags/ into that repository
  04-dataset.yaml                   uploads the test dataset
  05-nodeports.yaml                 the MinIO console and the Forgejo UI

dags/                               what Airflow syncs from git
  spark_ingest_dag.py               the load DAG
  spark_job.yaml                    the SparkApplication it submits

data/
  customers.csv                     the test dataset, 2000 synthetic rows
  generate.py                       regenerates it, seeded

examples/                           applied by hand, each optional in its own way
  external-credentials.yaml         the PostgreSQL and git Secrets the platform
                                    references and does not define
  nodeports.yaml                    external access on node addresses
  loadbalancer/                     external access behind a load balancer
    ingress.yaml                    one Ingress per product, verified annotations
    truststore-secret.yaml          the platform CA, as the Secret nginx wants
  external-ca.yaml                  trust anchor for CAs the platform did not issue
  spark-iceberg-job.yaml            template for your own secured Spark jobs

config.env                          every environment-specific value, one place

scripts/
  install-operators.sh              installs the operators with Helm, once per cluster
  apply.sh <namespace>              brings the whole thing up
  apply-standins.sh <namespace>     only testing/, in a namespace of its own
  resolve-config.sh                 reads config.env, applies the defaults
  check-config.sh                   lists placeholders still unfilled  (read-only)
  load-dags.sh                      pushes dags/ into the git server
  load-dataset.sh                   puts data/customers.csv in the object store
  access.sh <namespace>             URLs and accounts                  (read-only)
  smoke-test.sh <namespace>         end-to-end verification
  smoke_test.py                     what smoke-test.sh runs in the cluster
```

## What a real cluster needs

The platform is `manifests/`. Three things come from your environment, and
`testing/` only stands in for them while `LOCAL_STANDINS` is `true`:

| | What it is for | Where you point it |
|---|---|---|
| **An S3-compatible object store** | the Iceberg data, and Airflow's task logs | `manifests/02-s3.yaml`, and `S3_ENDPOINT` in `config.env` |
| **A PostgreSQL server, four empty databases** | `keycloak`, `hive`, `airflow`, `superset` | the `host` of each `metadataDatabase`, and `manifests/00-keycloak.yaml` |
| **A git repository for the DAGs** | Airflow reads its DAGs only from git | `DAGS_GIT_*` in `config.env` |

Plus a Kubernetes cluster you can create CRDs and ClusterRoles in, `kubectl`,
`helm` 3, and roughly **20 GiB of memory and 7 CPU** of schedulable room. See
"Resource footprint" below.

The object store must serve **TLS**. Trino 469 and later refuse an S3 connection
without it, and the refusal happens inside the operator: the `TrinoCluster` never
reconciles and no pod is ever created, which looks like a stuck cluster rather
than a configuration error.

Keycloak is part of the platform rather than a stand-in, but it runs in
`start-dev` mode here. Production uses `start`, which enforces hostname and TLS
strictness - expect to revisit `KC_HOSTNAME` there. It also wants a real DNS name
and a certificate of its own.

## Installing on a real cluster

### 1. Install the operators

Once per cluster, idempotent.

```sh
./scripts/install-operators.sh
```

Ten operators into the `stackable-operators` namespace from `oci.stackable.tech`,
all on SDP 26.7.0. All operators in one SDP release must be on the same version;
mixing them fails in ways that look like product bugs. Helm and OperatorHub
installations are mutually exclusive - pick one and stay on it.

### 2. Fill in the credentials for your own infrastructure

Four manifests reference Secrets this package deliberately does not define,
because the platform should not own a credential to something outside it.

```sh
kubectl create namespace lakehouse
kubectl apply -n lakehouse -f examples/external-credentials.yaml
```

Five Secrets: the PostgreSQL accounts for `keycloak`, `hive`, `airflow` and
`superset`, and the account or token git-sync authenticates the DAG repository
with. `apply.sh` stops with the list if one is missing, rather than letting the
products come up and hang on a mount.

The four databases must exist and be empty on the first run. The operators create
the schema; they do not create the database.

### 3. Point the package at your environment

Everything deployment-specific lives in `config.env`, and `apply.sh` substitutes
it into the manifests as it applies - so the files on disk stay readable and
re-appliable, and there is no build output. At minimum:

```sh
LOCAL_STANDINS=false
EXTERNAL_HOST=lakehouse.example.com
KEYCLOAK_HOSTNAME=keycloak.example.com
KEYCLOAK_PORT=443
DAGS_GIT_REPO=https://gitlab.example.com/data-platform/airflow-dags.git
DAGS_GIT_TLS="{verification: {server: {caCert: {webPki: {}}}}}"
AIRFLOW_URL=https://airflow.example.com
S3_ENDPOINT=https://s3.example.com:9000
```

`./scripts/check-config.sh` lists anything still unfilled, reading files only, and
`apply.sh` refuses to run while it does.

### 4. Commit the DAGs

See "Deploying a DAG" below. `dags/spark_job.yaml` is **not** substituted by
`apply.sh` - it never passes through it. Replace `CHANGEME-NAMESPACE` and the
object store endpoint by hand before committing.

### 5. Bring it up

```sh
./scripts/apply.sh lakehouse
kubectl get pods -n lakehouse -w
```

The numbered filename order is the apply order, and it matters: credentials and
connections before the components that use them, Trino catalogs before the Trino
cluster.

### 6. Expose the user interfaces

Nothing in `manifests/` reaches outside the cluster. The two ways to change that
are alternatives - never apply both.

```sh
kubectl apply -n lakehouse -f examples/loadbalancer/    # the normal case
kubectl apply -n lakehouse -f examples/nodeports.yaml   # no ingress controller
```

Read "Behind a load balancer" below before the first one: it needs its own
`config.env` settings, and seven things from the load balancer itself that are
not in the manifests.

The NodePorts are fixed at 31080, 31081 and 31082 for Trino, Airflow and
Superset. Keycloak's own NodePort is in `00-keycloak.yaml` and exists either way,
because `KC_HOSTNAME` has to agree with it. NiFi is reachable either way too, on a
port the listener-operator assigns; `access.sh` reads it.

### 7. Find your way in

```sh
./scripts/access.sh lakehouse
```

URLs, realm accounts and bootstrap accounts, reading the cluster for the parts
assigned at runtime.

## Testing against infrastructure that already exists

`./scripts/apply-standins.sh lakehouse-infra` brings up only `testing/`, in a
namespace of its own, so the platform can then be installed against it with
`LOCAL_STANDINS=false` exactly as it would be against real infrastructure. That
exercises the path `apply.sh lakehouse` on its own does not: the required
Secrets, the required `DAGS_GIT_REPO`, and an object store outside the namespace
being installed into. The script prints the settings to change.

## The smoke test

```sh
./scripts/smoke-test.sh lakehouse
```

32 checks, non-zero exit if any fails, in a throwaway pod inside the cluster
because everything it talks to is a cluster-internal Service. In order:

- Trino answers and OPA is deciding.
- The Airflow DAG is registered from a path under `/stackable/app/git-0/`, which
  is what proves it arrived through git-sync rather than from a volume left over
  from an earlier install.
- The DAG runs, submitting a Spark job that reads the CSV from the object store
  and writes an Iceberg table through the metastore. That one step covers
  Airflow, Spark, the object store and the metastore at once.
- Trino reads back the table Spark wrote, which proves the two are on the same
  catalog rather than on two that happen to agree.
- The policies restrict: `bob` sees everything, `alice` gets EMEA rows with
  `full_name` refused, `customer_id` hashed and `email` masked, `carol` is denied.
  Their tokens come from Keycloak, so this covers Keycloak too.
- The NiFi policy answers correctly for the same three users, including the
  `/proxy` grant that node-to-node traffic needs.
- Superset holds a working connection to Trino.
- All three UIs complete a real browser login: the full authorization-code flow
  against Keycloak, ending in the roles and permissions OPA assigned. For NiFi
  this is also the only check covering the product honouring the policy rather
  than the policy answering in isolation.

It is not read-only: it triggers the ingest DAG, which rewrites
`lakehouse.raw.customers`. The Spark job replaces partitions rather than
appending, so repeated runs leave the same 2000 rows. Each run leaves a
`SparkApplication` object behind and nothing collects those, so
`kubectl delete sparkapplication --all -n lakehouse` is worth running
occasionally.

## Deploying a DAG

Airflow reads its DAGs from a git repository through git-sync. There is no
ConfigMap of DAGs and no volume to fill: the operator clones the repository into
an emptyDir and points `AIRFLOW__CORE__DAGS_FOLDER` at the checkout. Committing
to the repository is the entire deployment procedure, and a new commit is live one
sync period (20 s) later, plus however long Airflow's dag processor takes to
notice the file.

`dags/` holds a worked example of the load path: a DAG that submits a
`SparkApplication` which reads a CSV from the object store and writes an Iceberg
table through the metastore. Commit both files into the repository
`DAGS_GIT_REPO` names, under `DAGS_GIT_FOLDER`, or replace them with your own.
Because they are real Python and YAML rather than a ConfigMap, they can be
edited, diffed and linted like source.

Locally the repository is the private `airflow-dags` repository in the Forgejo of
`testing/02-forgejo.yaml`, and `scripts/load-dags.sh` force-pushes `dags/` into
it. Re-running `apply.sh` re-pushes and overwrites, so Forgejo is not the place
to keep an edit.

## Who sees what

Three accounts ship in the realm, password equal to the username. They exist to
make the authorization model visible rather than to be useful:

| | Group | Trino | Airflow | Superset | NiFi |
|---|---|---|---|---|---|
| `bob` | `/admins` | everything, unmasked | Admin | Admin | full access |
| `alice` | `/analysts` | EMEA rows only, `full_name` unreadable, `customer_id` hashed, `email` masked | User | Gamma + SQL Lab | read-only |
| `carol` | none | nothing | Public (sees nothing) | Public (sees nothing) | nothing |

`carol` is the case worth checking after any policy change. Membership of the
realm alone must grant no access anywhere, and it is easy to break that by adding
a rule with a wider pattern than intended.

Two static Trino credentials exist for clients that cannot complete a browser
login: `data-import` for the load path, and `superset` for Superset's single
connection. Airflow and Superset each also have a bootstrap admin account that
does not go through Keycloak - `scripts/access.sh` prints both.

## Behind a load balancer

The package is written for this: one hostname per product, the load balancer
terminating the browser's TLS and opening a new TLS connection to the pods.
Everything environment-specific is in `config.env`, and the four products that
need to know they are behind a proxy are already configured for it
(`http-server.process-forwarded` on Trino, `ENABLE_PROXY_FIX` on Airflow and
Superset, `KC_PROXY_HEADERS` on Keycloak). Those settings are inert when nothing
is in front, so the same manifests serve both topologies.

**What to change.** In `config.env`:

```sh
KEYCLOAK_HOSTNAME=keycloak.example.com
KEYCLOAK_PORT=443
KEYCLOAK_CA="webPki: {}"              # the load balancer's certificate, not the platform's
S3_CA="webPki: {}"                    # or an external CA - see "Certificates" above
KEYCLOAK_BACKCHANNEL_DYNAMIC=false
AIRFLOW_URL=https://airflow.example.com
NIFI_HOSTNAME=nifi.example.com
NIFI_LISTENER_CLASS=cluster-internal
TRINO_HOSTNAME=trino.example.com
SUPERSET_HOSTNAME=superset.example.com
```

Then apply `examples/loadbalancer/` instead of `examples/nodeports.yaml`, and
register the five public URLs as redirect URIs on their Keycloak clients in place
of the `*` the shipped realm uses.

## License

Apache License 2.0. See [LICENSE](LICENSE).
