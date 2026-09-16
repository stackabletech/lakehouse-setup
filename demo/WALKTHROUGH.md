# Walking through the demo

A presenter's path through the demo environment: what to open, whom to log in
as, what to type, and the one thing to say at each step. About 20 minutes.
Every step was walked on a minikube install of this package; the one exception
is marked.

The story is one sentence: **a person's group in Keycloak decides what they see
in every product, and the products enforce it, not the dashboard.**

## Before you start

- The install is up and both demo Jobs have completed:

  ```sh
  kubectl get jobs -n lakehouse
  ./scripts/access.sh lakehouse
  ```

  The second command prints every URL and account below with the real host
  filled in. `HOST` in this document is that node address. NiFi's port is
  assigned at runtime, so read it from that output rather than from here.

- Three personas, password equal to the username:

  | | Group | Role in the story |
  |---|---|---|
  | `bob` | `/admins` | sees everything |
  | `alice` | `/analysts` | sees EMEA, with identifiers hashed |
  | `carol` | none | authenticates everywhere, sees nothing |

- **Use one private browser window per persona.** Keycloak keeps a single
  sign-on session per browser. Logging out of Superset and logging in again
  brings the same user straight back, which looks like the login form ignoring
  the new name. Three private windows, one per persona, avoids that for the
  whole session.

- Every product serves a certificate from the platform's own CA. Browsers warn
  once per host. Click through; that is expected, not a fault.

- Start the Airflow step (step 6) early if time is short: its Spark job takes
  three to five minutes on the first run.

## 1. Keycloak, where identity lives (2 min)

Open `https://HOST:31443/admin/`, log in as `admin` / `admin`, and switch the
realm selector from `master` to `sdp`.

- **Groups**: `admins` and `analysts`. That is the entire authorization model as
  far as the identity provider is concerned.
- **Users**: `bob` is in `admins`, `alice` in `analysts`, `carol` in no group.

Say: nothing here says what a group may do. The products ask OPA, OPA asks
Keycloak who is in which group, and the policy files decide the rest. Changing
what analysts see is a policy change, not a Keycloak change.

## 2. The dashboard, as bob (3 min)

Open `http://HOST:31082`, choose **Sign in with Keycloak**, log in as `bob`.
Open **Dashboards**, then **Webshop orders**.

What is on it, and it is the same on every install because the data is seeded:

| Tile | Shows |
|---|---|
| Orders | 9 920 |
| Revenue | about 2.73 million |
| Monthly revenue by region | one colour: EMEA |
| Revenue by category | electronics ahead of sports |
| Top customers | `sha256:...` strings, not ids |

Say: this is the *restricted* view, on purpose. Superset talks to Trino as one
technical user for everybody, and the policy row-filters and hashes for that
user. So the dashboard is safe to show to anyone who can open it, and the
administrator is not special here. The whole table has 20 000 orders across
three regions; step 5 shows them.

## 3. The same dashboard, as alice (2 min)

In a second private window, log in to Superset as `alice`. Open **Dashboards**.

She sees **Webshop orders** too, with identical numbers. Her menu is shorter:
no settings, no dataset management. The user menu at the top right lists her
roles: `Gamma` and `sql_lab`.

Say: those roles were not assigned in Superset. OPA mapped her Keycloak group to
them at login, and it does so on every login. An administrator who gives her
more in the Superset UI loses that change the next time she logs in.

## 4. SQL Lab, as alice (3 min)

Still as alice: **SQL**, then **SQL Lab**. Database `lakehouse`, schema `demo`.

```sql
SELECT region, count(*) FROM demo.orders GROUP BY 1
```

One row: `EMEA`, `9920`. She did not filter; the policy did.

```sql
SELECT customer_id, amount FROM demo.orders ORDER BY amount DESC LIMIT 5
```

Every `customer_id` is a `sha256:...` string. Say: the hash is stable, so she
can still join and group on it. She just cannot learn who it is.

```sql
SELECT * FROM raw.customers LIMIT 5
```

Superset refuses before Trino is asked: *You need access to the following
tables: lakehouse.raw.customers*. Say: two layers. Superset grants analysts the
demo dataset and nothing else. Underneath, Trino's policy would have masked this
table anyway. Switch to bob's window and run the same query as the Superset
administrator: Trino answers *Access Denied: Cannot select from columns
[full_name, ...]*. The administrator of the tool cannot get past the policy
either, because the tool itself is the restricted identity.

## 5. The unfiltered view, as bob in Trino (3 min)

This needs a Trino client on the presenter's machine. The CLI:

```sh
trino --server https://HOST:31080 --insecure --external-authentication
```

It prints a URL; open it, log in as `bob`, return to the terminal.

```sql
SELECT region, count(*), round(sum(amount)) FROM lakehouse.demo.orders GROUP BY 1;
```

| region | orders | revenue |
|---|---|---|
| AMER | 5 970 | 1 699 282 |
| APAC | 4 110 | 1 097 014 |
| EMEA | 9 920 | 2 734 735 |

All 20 000 rows, 5.53 million in total, plain customer ids. Repeat the login as
`alice` and run the same query: one row, EMEA, and `SELECT customer_id FROM
lakehouse.demo.orders LIMIT 1` comes back hashed. Say: same table, same query
engine, same catalog. The only thing that changed is who asked.

The Trino web UI at `https://HOST:31080/ui` also takes the Keycloak login and
shows the query history, including Superset's queries running as `superset`.

*Not walked here:* the CLI's browser login. The smoke test proves the same
three outcomes with tokens obtained from Keycloak directly, and any client that
supports Trino's OAuth 2.0 authentication, DBeaver included, follows the same
flow.

## 6. Where the data comes from: Airflow and Spark (4 min, start early)

Open `http://HOST:31081`, **Login with Keycloak**, log in as `bob`.

- Open the DAG **lakehouse_ingest**. Its **Code** tab shows the DAG as it is in
  git: the file path starts with `/stackable/app/git-0/`, which is the git-sync
  checkout. Say: DAGs are deployed by committing to a repository. There is no
  upload and no volume to fill.
- **Trigger** it. In a terminal:

  ```sh
  kubectl get pods -n lakehouse -w
  ```

  A pod named `airflow-ingest-...` appears, then a Spark driver and two
  executors. First run: three to five minutes, most of it downloading the
  Iceberg libraries. The job reads `customers.csv` from the object store and
  writes the Iceberg table `lakehouse.raw.customers`, 2 000 rows.

Say: this is the trusted load path. Airflow submits with its own service
account; Spark writes with the platform's read-write object store identity;
neither goes through Trino or the policy. That is why analysts get a read-only
object store identity, so a write from their side fails at the storage layer
whatever the policy would say.

If asked: the demo `orders` table did not come this way. A Job loaded it through
Trino as the technical user, so that the demo does not depend on the DAG having
run. Real data takes the Spark path.

Optional, local installs only: the MinIO console at `https://HOST:31090`
(`admin` / `adminadmin`). Bucket `raw` holds `customers/customers.csv`, the
landing zone. Bucket `lakehouse` holds `warehouse/demo.db/orders-.../data/
region=EMEA/...parquet`. Say: an Iceberg table is files in a bucket plus
metadata in the catalog. Nothing is locked inside a database.

## 7. NiFi (2 min)

URL from `access.sh`, path `/nifi`. Log in through Keycloak.

- `bob`: the canvas opens and a processor can be dragged onto it.
- `alice`: the canvas opens, the toolbar is inert. She can look, not change.
- `carol`: NiFi itself refuses her with *insufficient permissions*.

Say: NiFi asks OPA for every action. The same three groups, the same policy
engine, a different vocabulary.

## 8. carol, everywhere (2 min)

In a third private window, log in as `carol`:

- Superset: the login succeeds and the dashboard list is empty. No SQL Lab.
- Airflow: the login succeeds and the DAG list is empty.
- NiFi: refused, as above.
- Trino: `Access Denied: Cannot access catalog lakehouse`.

Say: she is a valid user of the realm. Membership of the company is not access
to the data. Nothing is granted by default, and a mistake in a policy shows up
here first, which is why she exists.

## 9. The finale: promote carol (3 min)

In Keycloak: **Users**, `carol`, **Groups**, **Join Group**, `analysts`.

Back in carol's window, log out of Superset and log in again. Now she has
`Gamma` and `sql_lab`, the dashboard is listed, and SQL Lab answers the
queries from step 4 with EMEA rows and hashes. Trino and NiFi ask OPA per
request and OPA caches group lookups briefly, so both follow within about a
minute without a new login. Airflow assigns roles at login, so she logs in
again there.

Say: one change, in one place, and every product agreed. That is the point of
the setup.

**Undo it afterwards**: remove `carol` from `analysts` in Keycloak. The realm is
imported once, at Keycloak's first start, so re-applying the manifests does
not reset her.

## Resetting between demos

```sh
./scripts/apply.sh lakehouse                              # reloads the orders table and rebuilds the dashboard
kubectl delete sparkapplication --all -n lakehouse        # the DAG leaves one object per run behind
```

The rebuild overwrites any edit made to the demo dataset, its charts or the
dashboard in the Superset UI, which is what makes it a reset. Copy a chart
before changing it during a demo.

## Things that go wrong

| Symptom | Cause |
|---|---|
| Logging in as alice lands you in bob's session | Keycloak single sign-on. Use a private window per persona, or sign out at `https://HOST:31443/realms/sdp/account`. |
| The dashboard is empty or the Jobs are still running | On a fresh install the load Job waits for Trino and the dashboard Job waits for the load. `kubectl logs -n lakehouse job/demo-orders-load -f`. |
| NiFi's URL from last time does not answer | Its port is assigned by the listener and changes when the Listener is recreated. Re-run `access.sh`. |
| The Spark job sits in `Pending` | Memory on the node. `kubectl describe node | grep -A6 "Allocated resources"`. |
| Trino refuses the connection on the first query of the day | Certificates in the platform last seven days; a product restarts at expiry. Wait for the pods to be back. |
