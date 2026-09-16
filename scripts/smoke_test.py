#!/usr/bin/env python3
"""End-to-end verification of the lakehouse setup.

Runs INSIDE the cluster - scripts/smoke-test.sh puts it there. Everything it
talks to is addressed by Service name, and nothing is exposed to the host.

What it checks, in order, because each step depends on the one before:

  1. Trino answers, and OPA is deciding. The `tpch` catalog separates "Trino is
     broken" from "the lakehouse is broken".
  2. Airflow runs the DAG, which submits the Spark job, which reads the CSV from
     the object store and writes an Iceberg table through the Hive metastore.
     This one step exercises Airflow, Spark, the object store and the metastore.
     The DAG's own file path proves it arrived through git-sync rather than
     from a volume.
  3. Trino reads that table back - so the catalog Spark wrote and the catalog
     Trino reads are genuinely the same one.
  4. The OPA policies actually restrict: `alice` gets EMEA rows with masked
     columns, `bob` gets everything, `carol` gets nothing. Tokens come from
     Keycloak by password grant, so this covers Keycloak too.
  5. The NiFi policy answers correctly for the same three users.
  6. Superset holds a working connection to Trino.
  7. Real browser logins work, for all three products that have a UI: the whole
     authorization-code flow against Keycloak, ending in the roles and
     permissions OPA assigned. These are the checks that exercise the redirect
     chain rather than an API token, and for NiFi it is the only check that
     covers the product's own use of the policy rather than the policy alone.
  8. With DEMO_DATA set (scripts/smoke-test.sh passes it through): the demo
     table is loaded, its policy restricts the same way, the dashboard is
     published and queries through the shared connection, and an analyst can
     open it.

TLS: this harness does not verify certificates. The products do - that they work
at all is the proof. Verifying here would mean mounting the CA into a throwaway
pod for no additional signal.
"""

import base64
import html
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Imported by name rather than as `http.cookiejar`: the helper below is called
# `http`, and that would shadow the package.
from http.cookiejar import CookieJar

NAMESPACE = os.environ["NAMESPACE"]

TRINO = "https://trino-coordinator:8443"
AIRFLOW = "http://airflow-webserver:8080"
SUPERSET = "http://superset-node:8088"
OPA = "http://opa-server:8081"
# The pod's own name under the headless Service. NiFi's Jetty rejects any Host
# its certificate does not cover, answering `HTTP ERROR 400 Invalid SNI` - which
# reads like TLS and is really a hostname mismatch. The certificate covers the
# headless name, the metrics name and the node address, but NOT the role Service
# name `nifi-node`, so that is the one address that does not work.
NIFI = f"https://nifi-node-default-0.nifi-node-default-headless.{NAMESPACE}.svc.cluster.local:8443"
# The externally reachable NiFi, looked up by smoke-test.sh. Its login flow
# builds absolute URLs from the address it is reached on, so the browser check
# below has to use this one rather than the in-cluster name.
NIFI_EXTERNAL = os.environ["NIFI_URL"]
KEYCLOAK = f"{os.environ['KEYCLOAK_URL']}/realms/sdp"

TRINO_TECHNICAL_PASSWORD = "data-import-password"
AIRFLOW_ADMIN_PASSWORD = "airflow-admin-password"
SUPERSET_ADMIN_PASSWORD = "superset-admin-password"

SSL_CONTEXT = ssl._create_unverified_context()

failures = []


def check(name, condition, detail=""):
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}" + (f"\n          {detail}" if detail else ""))
        failures.append(name)


def http(method, url, headers=None, body=None, form=None):
    """Returns (status, parsed-or-text). Never raises on an HTTP error status."""
    headers = dict(headers or {})
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, context=SSL_CONTEXT) as response:
            raw = response.read()
            try:
                return response.status, json.loads(raw or b"{}")
            except ValueError:
                return response.status, raw.decode(errors="replace")
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            return error.code, json.loads(raw or b"{}")
        except ValueError:
            return error.code, raw.decode(errors="replace")


def trino(query, user="data-import", password=None, token=None):
    """Run a query. Returns (columns, rows) on success, or ("ERROR", message)."""
    headers = {"X-Trino-User": user, "X-Trino-Source": "smoke-test"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        secret = password if password is not None else TRINO_TECHNICAL_PASSWORD
        credentials = base64.b64encode(f"{user}:{secret}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"

    url = f"{TRINO}/v1/statement"
    body, rows, columns = query.encode(), [], None
    while True:
        request = urllib.request.Request(
            url, data=body, headers=headers, method="POST" if body else "GET"
        )
        with urllib.request.urlopen(request, context=SSL_CONTEXT) as response:
            document = json.loads(response.read())
        if "error" in document:
            return "ERROR", document["error"].get("message", "")
        columns = document.get("columns") or columns
        rows += document.get("data") or []
        if not document.get("nextUri"):
            return [c["name"] for c in columns] if columns else [], rows
        url, body = document["nextUri"], None
        time.sleep(0.2)


def keycloak_token(username, password):
    """Password grant against the realm. Needs directAccessGrants on the client."""
    status, document = http(
        "POST",
        f"{KEYCLOAK}/protocol/openid-connect/token",
        form={
            "grant_type": "password",
            "client_id": "trino",
            "client_secret": "trino-client-secret",
            "username": username,
            "password": password,
            "scope": "openid",
        },
    )
    if status != 200:
        raise SystemExit(f"could not get a token for {username}: {status} {document}")
    return document["access_token"]


def keycloak_form(start_url):
    """Start an OIDC flow and return (browser, the Keycloak login form action).

    The opener carries a cookie jar and follows redirects, which is what makes
    it behave like a browser: the product redirects to Keycloak, Keycloak serves
    its login form, and after the form is submitted the redirect back through
    the product's callback sets the session cookie on this same opener.
    """
    browser = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(CookieJar()),
        urllib.request.HTTPSHandler(context=SSL_CONTEXT),
    )
    # Keycloak varies its login page by User-Agent; ask for the browser one.
    browser.addheaders = [("User-Agent", "Mozilla/5.0")]
    try:
        page = browser.open(start_url).read().decode()
    except urllib.error.HTTPError:
        return browser, None
    form = re.search(r'action="([^"]+)"', page)
    return browser, (form.group(1).replace("&amp;", "&") if form else None)


def submit_keycloak_form(browser, action, username, password):
    """Post credentials to Keycloak. True if the redirect chain came back."""
    body = urllib.parse.urlencode(
        {"username": username, "password": password, "credentialId": ""}
    ).encode()
    try:
        browser.open(urllib.request.Request(action, data=body), timeout=60)
        return True
    except urllib.error.HTTPError:
        return False


# ── 1. Trino and OPA ────────────────────────────────────────────────────────
print("\nTrino and OPA")

columns, rows = trino("SHOW CATALOGS")
catalogs = {row[0] for row in rows} if columns != "ERROR" else set()
check(
    "the technical user sees the read and write catalogs",
    {"lakehouse", "lakehousewrite"} <= catalogs,
    f"got {sorted(catalogs)}",
)

columns, rows = trino("SELECT count(*) FROM tpch.tiny.nation")
check("tpch is queryable (Trino itself is healthy)", rows == [[25]], f"got {rows}")


# ── 2. Airflow runs the DAG, which runs the Spark job ───────────────────────
print("\nAirflow to Spark to Iceberg")

DAG_ID = "lakehouse_ingest"
# Retried, because the webserver is the pod most likely to still be coming up:
# a refused connection here would otherwise end the run with a stack trace
# instead of a failed check.
for attempt in range(30):
    try:
        status, document = http(
            "POST",
            f"{AIRFLOW}/auth/token",
            body={"username": "admin", "password": AIRFLOW_ADMIN_PASSWORD},
        )
        if status in (200, 201):
            break
    except urllib.error.URLError as error:
        status, document = None, error
    print(f"  waiting for the Airflow webserver ({document})")
    time.sleep(10)
if status not in (200, 201):
    raise SystemExit(f"could not authenticate against Airflow: {status} {document}")
airflow_auth = {"Authorization": f"Bearer {document['access_token']}"}

# A DAG is created paused, and triggering a paused DAG creates a run that never
# starts. Unpausing first is what makes the trigger below take effect.
deadline = time.time() + 300
while time.time() < deadline:
    status, _ = http(
        "PATCH",
        f"{AIRFLOW}/api/v2/dags/{DAG_ID}?update_mask=is_paused",
        headers=airflow_auth,
        body={"is_paused": False},
    )
    if status == 200:
        break
    print(f"  waiting for the dag processor to pick up {DAG_ID}")
    time.sleep(10)
check("the DAG is registered and unpaused", status == 200, f"got {status}")

# Where Airflow parsed the DAG from. git-sync clones into
# /stackable/app/git-<n>/current/<gitFolder>, so this is what separates "the
# DAG is there" from "the DAG got there the way it is supposed to". A DAG left
# over from an earlier ConfigMap-mounted install would pass every other check
# in this section.
status, document = http("GET", f"{AIRFLOW}/api/v2/dags/{DAG_ID}", headers=airflow_auth)
fileloc = document.get("fileloc", "") if isinstance(document, dict) else ""
check(
    "the DAG came from git-sync, not from a volume",
    fileloc.startswith("/stackable/app/git-"),
    f"got {fileloc!r}",
)

run_id = f"smoke-test-{int(time.time())}"
status, document = http(
    "POST",
    f"{AIRFLOW}/api/v2/dags/{DAG_ID}/dagRuns",
    headers=airflow_auth,
    body={"dag_run_id": run_id, "logical_date": None},
)
check("the DAG run was accepted", status in (200, 201), f"got {status} {document}")

state, deadline = None, time.time() + 1500
while time.time() < deadline:
    status, document = http(
        "GET", f"{AIRFLOW}/api/v2/dags/{DAG_ID}/dagRuns/{run_id}", headers=airflow_auth
    )
    state = document.get("state") if isinstance(document, dict) else None
    if state in ("success", "failed"):
        break
    print(f"  dag run {run_id} is {state}")
    time.sleep(20)
check("the DAG run succeeded", state == "success", f"ended in state {state}")


# ── 3. Trino reads what Spark wrote ─────────────────────────────────────────
print("\nThe Iceberg table, through the same metastore")

TABLE = "lakehouse.raw.customers"
columns, rows = trino(f"SELECT count(*) FROM {TABLE}")
total = rows[0][0] if columns != "ERROR" and rows else None
check("the technical user reads the whole table", total == 2000, f"got {total}")

columns, rows = trino(f"SELECT count(*) FROM {TABLE} WHERE region = 'EMEA'")
emea = rows[0][0] if columns != "ERROR" and rows else None
check("the row-filter column has EMEA rows to filter to", bool(emea), f"got {emea}")


# ── 4. The OPA policies restrict what people see ────────────────────────────
print("\nTrino authorization, as real Keycloak users")

bob = keycloak_token("bob", "bob")
alice = keycloak_token("alice", "alice")
carol = keycloak_token("carol", "carol")

columns, rows = trino(f"SELECT count(*) FROM {TABLE}", user="bob", token=bob)
check(
    "/admins sees every row",
    columns != "ERROR" and rows == [[total]],
    f"got {rows if columns != 'ERROR' else rows}",
)

columns, rows = trino(
    f"SELECT full_name FROM {TABLE} LIMIT 1", user="bob", token=bob
)
check("/admins reads full_name unmasked", columns != "ERROR", f"got {rows}")

columns, rows = trino(f"SELECT count(*) FROM {TABLE}", user="alice", token=alice)
check(
    "/analysts is restricted to the EMEA rows",
    columns != "ERROR" and rows == [[emea]],
    f"expected {emea}, got {rows}",
)

columns, rows = trino(
    f"SELECT full_name FROM {TABLE} LIMIT 1", user="alice", token=alice
)
check(
    "/analysts cannot select the hidden column at all",
    columns == "ERROR" and "denied" in str(rows).lower(),
    f"got {columns} {rows}",
)

columns, rows = trino(
    f"SELECT customer_id, email FROM {TABLE} LIMIT 1", user="alice", token=alice
)
masked = rows[0] if columns != "ERROR" and rows else ["", ""]
check(
    "/analysts gets customer_id hashed",
    str(masked[0]).startswith("sha256:"),
    f"got {masked[0]!r}",
)
check(
    "/analysts gets email partially masked",
    "---@" in str(masked[1]),
    f"got {masked[1]!r}",
)

columns, rows = trino(f"SELECT count(*) FROM {TABLE}", user="carol", token=carol)
check(
    "a user in no group is denied outright",
    columns == "ERROR",
    f"got {columns} {rows}",
)


# ── 5. The NiFi policy ──────────────────────────────────────────────────────
print("\nNiFi authorization policy")


def nifi_decision(identity, action):
    status, document = http(
        "POST",
        f"{OPA}/v1/data/nifi/allow",
        body={
            "input": {
                "action": {"name": action},
                "resource": {"id": "/process-groups/root", "name": "NiFi Flow"},
                "identity": {"name": identity, "groups": []},
            }
        },
    )
    return document.get("result", {}) if status == 200 else {"error": document}


check("/admins may write", nifi_decision("bob", "write").get("allowed") is True)
check("/analysts may read", nifi_decision("alice", "read").get("allowed") is True)
check("/analysts may not write", nifi_decision("alice", "write").get("allowed") is False)
check("a user in no group may not read", nifi_decision("carol", "read").get("allowed") is False)
check(
    "a node certificate identity may use /proxy",
    http(
        "POST",
        f"{OPA}/v1/data/nifi/allow",
        body={
            "input": {
                "action": {"name": "write"},
                "resource": {"id": "/proxy", "name": "Proxy"},
                "identity": {"name": "CN=nifi-node-default-0", "groups": []},
            }
        },
    )[1].get("result", {}).get("allowed")
    is True,
)

status, _ = http("GET", f"{NIFI}/nifi-api/flow/current-user")
check("NiFi is serving and refuses an unauthenticated request", status == 401, f"got {status}")


# ── 6. Superset's connection to Trino ───────────────────────────────────────
print("\nSuperset")

superset_auth = None
status, document = http(
    "POST",
    f"{SUPERSET}/api/v1/security/login",
    body={
        "username": "admin",
        "password": SUPERSET_ADMIN_PASSWORD,
        "provider": "db",
        "refresh": True,
    },
)
if status != 200:
    check("Superset accepts the bootstrap account", False, f"got {status} {document}")
else:
    superset_auth = {"Authorization": f"Bearer {document['access_token']}"}
    status, document = http(
        "GET", f"{SUPERSET}/api/v1/database/", headers=superset_auth
    )
    databases = {d["database_name"]: d["id"] for d in document.get("result", [])}
    check(
        "the lakehouse connection is registered",
        "lakehouse" in databases,
        f"got {sorted(databases)}",
    )
    if "lakehouse" in databases:
        # Listing schemas makes Superset open the connection and query Trino,
        # so this fails if the driver, the credentials or the CA are wrong.
        status, document = http(
            "GET",
            f"{SUPERSET}/api/v1/database/{databases['lakehouse']}/schemas/",
            headers=superset_auth,
        )
        schemas = set(document.get("result", [])) if status == 200 else set()
        check(
            "Superset can query Trino through it",
            "raw" in schemas,
            f"got {status} {sorted(schemas)}",
        )


# ── 7. The browser login flow, end to end ───────────────────────────────────
print("\nSuperset login through Keycloak")


def oidc_login(username, password):
    """Complete the authorization-code flow the way a browser would.

    Returns the set of Superset role names the user ends up with, and the
    logged-in browser, which the demo checks reuse.

    The roles come out of the `data-bootstrap` blob the welcome page embeds for
    its frontend, not from `/api/v1/me/` - that endpoint does not report roles
    in Superset 6.1, and a user on the `Public` role may not call it at all.
    """
    browser, form = keycloak_form(f"{SUPERSET}/login/keycloak")
    if form is None:
        return "no Keycloak login form", browser
    submit_keycloak_form(browser, form, username, password)
    try:
        page = browser.open(f"{SUPERSET}/superset/welcome/").read().decode()
    except urllib.error.HTTPError as error:
        return f"welcome page returned HTTP {error.code}", browser
    blob = re.search(r'data-bootstrap="([^"]+)"', page)
    if not blob:
        return "no bootstrap data on the welcome page", browser
    document = json.loads(html.unescape(blob.group(1)))
    return set(document.get("user", {}).get("roles") or {}), browser


bob_roles, _ = oidc_login("bob", "bob")
check(
    "/admins logs in and OPA grants Admin",
    bob_roles == {"Admin"},
    f"got {bob_roles}",
)
alice_roles, alice_browser = oidc_login("alice", "alice")
check(
    "/analysts logs in and OPA grants Gamma and sql_lab",
    alice_roles == {"Gamma", "sql_lab"},
    f"got {alice_roles}",
)
# The interesting case: the login succeeds, and the user lands on a role that
# carries no permissions. Superset returns HTTP 500 for a user with no roles at
# all, so `Public` rather than an empty set is the correct outcome.
carol_roles, _ = oidc_login("carol", "carol")
check(
    "a user in no group logs in and lands on Public with nothing",
    carol_roles == {"Public"},
    f"got {carol_roles}",
)


# The Airflow UI. Roles land in Airflow's own user table, which the API does not
# expose, so what is checked here is that the redirect chain completes - the
# mapping itself is asserted through Trino and Superset above.
print("\nAirflow login through Keycloak")

for username in ("bob", "alice", "carol"):
    browser, form = keycloak_form(f"{AIRFLOW}/auth/login/keycloak")
    check(
        f"{username} completes the Airflow login flow",
        form is not None and submit_keycloak_form(browser, form, username, username),
        "the flow did not come back to Airflow",
    )

# NiFi, where the answer is a permission rather than a role. This is the check
# that proves NiFi asks OPA and honours what it says.
print("\nNiFi login through Keycloak")


def nifi_permissions(username, password):
    """Log in and return NiFi's own view of what this identity may do."""
    browser, form = keycloak_form(
        f"{NIFI_EXTERNAL}/nifi-api/oauth2/authorization/consumer"
    )
    if form is None:
        return "no Keycloak form"
    submit_keycloak_form(browser, form, username, password)
    try:
        document = json.loads(browser.open(f"{NIFI_EXTERNAL}/nifi-api/flow/current-user").read())
    except urllib.error.HTTPError as error:
        return f"HTTP {error.code}"
    return document


bob_view = nifi_permissions("bob", "bob")
check(
    "/admins is identified as itself and may read and write",
    isinstance(bob_view, dict)
    and bob_view.get("identity") == "bob"
    and bob_view["provenancePermissions"] == {"canRead": True, "canWrite": True},
    f"got {bob_view}",
)
alice_view = nifi_permissions("alice", "alice")
check(
    "/analysts may read but not write",
    isinstance(alice_view, dict)
    and alice_view["provenancePermissions"] == {"canRead": True, "canWrite": False},
    f"got {alice_view}",
)
carol_view = nifi_permissions("carol", "carol")
check(
    "a user in no group is refused by NiFi itself",
    carol_view == "HTTP 403",
    f"got {carol_view}",
)


# ── 8. The demo data case ───────────────────────────────────────────────────
# Only while demo/ is applied. Its two Jobs wait for the platform themselves, so
# on a fresh install they can still be running when this test starts - which is
# why the first check waits for the table to fill rather than reading it once.
if os.environ.get("DEMO_DATA", "true") == "true":
    print("\nDemo data")

    DEMO_TABLE = "lakehouse.demo.orders"
    DEMO_ROWS = 20000
    demo_total, deadline = None, time.time() + 900
    while time.time() < deadline:
        columns, rows = trino(f"SELECT count(*) FROM {DEMO_TABLE}")
        demo_total = rows[0][0] if columns != "ERROR" and rows else None
        if demo_total == DEMO_ROWS:
            break
        print(f"  waiting for the demo load ({rows if columns == 'ERROR' else demo_total} so far)")
        time.sleep(15)
    check(
        "the load Job filled the demo table",
        demo_total == DEMO_ROWS,
        f"expected {DEMO_ROWS} rows, got {demo_total}",
    )

    columns, rows = trino(f"SELECT count(*) FROM {DEMO_TABLE} WHERE region = 'EMEA'")
    demo_emea = rows[0][0] if columns != "ERROR" and rows else None

    columns, rows = trino(f"SELECT count(*) FROM {DEMO_TABLE}", user="bob", token=bob)
    check("/admins sees every order", columns != "ERROR" and rows == [[demo_total]], f"got {rows}")

    columns, rows = trino(
        f"SELECT count(*), min(customer_id) FROM {DEMO_TABLE}", user="alice", token=alice
    )
    restricted = rows[0] if columns != "ERROR" and rows else [None, ""]
    check(
        "/analysts sees EMEA orders with customer_id hashed",
        restricted[0] == demo_emea and str(restricted[1]).startswith("sha256:"),
        f"expected {demo_emea} rows, got {restricted}",
    )

    columns, rows = trino(f"SELECT count(*) FROM {DEMO_TABLE}", user="carol", token=carol)
    check("a user in no group is denied the demo table", columns == "ERROR", f"got {rows}")

    if superset_auth:
        status, document = http(
            "GET", f"{SUPERSET}/api/v1/dashboard/webshop-orders", headers=superset_auth
        )
        dashboard = document.get("result", {}) if status == 200 else {}
        check(
            "the demo dashboard is published",
            dashboard.get("published") is True,
            f"got {status} {document if status != 200 else dashboard.get('published')}",
        )
        status, document = http(
            "GET", f"{SUPERSET}/api/v1/dashboard/webshop-orders/charts", headers=superset_auth
        )
        charts = document.get("result", []) if status == 200 else []
        check("with its five charts", len(charts) == 5, f"got {[c.get('slice_name') for c in charts]}")

        # The dataset queried through Superset's own connection. What comes back
        # is what a dashboard shows, and it has to be the restricted view.
        query = urllib.parse.quote(
            "(filters:!((col:schema,opr:eq,value:demo),(col:table_name,opr:eq,value:orders)))"
        )
        status, document = http("GET", f"{SUPERSET}/api/v1/dataset/?q={query}", headers=superset_auth)
        datasets = document.get("result", []) if status == 200 else []
        regions = None
        if datasets:
            status, document = http(
                "POST",
                f"{SUPERSET}/api/v1/chart/data",
                headers=superset_auth,
                body={
                    "datasource": {"id": datasets[0]["id"], "type": "table"},
                    "queries": [{"columns": ["region"], "metrics": ["count"], "row_limit": 10}],
                    "result_format": "json",
                    "result_type": "full",
                },
            )
            if status == 200:
                regions = sorted(row["region"] for row in document["result"][0]["data"])
        check(
            "the dataset answers through the shared connection, EMEA only",
            regions == ["EMEA"],
            f"got {status} {regions if regions is not None else document}",
        )

    # The dashboard Job grants Gamma access to the dataset; this is what that
    # grant is for. The browser is the one alice logged in with above.
    try:
        listing = json.loads(alice_browser.open(f"{SUPERSET}/api/v1/dashboard/").read())
        visible = [d.get("slug") for d in listing.get("result", [])]
    except (urllib.error.HTTPError, ValueError) as error:
        visible = str(error)
    check(
        "/analysts can see the demo dashboard",
        "webshop-orders" in visible,
        f"alice sees {visible}",
    )


# ── verdict ─────────────────────────────────────────────────────────────────
print()
if failures:
    print(f"{len(failures)} check(s) failed:")
    for name in failures:
        print(f"  - {name}")
    sys.exit(1)
print("all checks passed")
