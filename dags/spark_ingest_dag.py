"""Submit a SparkApplication to the Stackable Spark operator and wait for it.

Airflow creates a SparkApplication through the Kubernetes API using its own
ServiceAccount, the platform's technical identity for this, and then polls its
status. No user credential is involved and no user authorization is evaluated:
the Spark job runs with the object store identity configured in its manifest.

The file next to this one, `spark_job.yaml`, is the SparkApplication that gets
submitted. Both arrive through git-sync, so the path they live under is chosen
by the platform and can change - which is why the manifest is located relative
to this file rather than by an absolute path.

NOTE - the DAG is triggered manually (`schedule=None`). Give it a schedule once
  it does real work.

NOTE - a SparkApplication can report `phase: Succeeded` even when its driver
  failed to bring up executors. The DAG treats `Succeeded` as success, which is
  the operator's own contract; stronger guarantees need the job itself to write
  a completion marker that the DAG then checks for.
"""

import re
from datetime import datetime
from pathlib import Path

import yaml
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sdk import dag, task

# AirflowFailException moved between Airflow 3 minor versions: 3.1 has it in
# airflow.exceptions, 3.2 only in airflow.sdk.exceptions. Try both so this
# DAG survives a version change in either direction.
try:
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.exceptions import AirflowFailException

API_GROUP = "spark.stackable.tech"
API_VERSION = "v1alpha1"
PLURAL = "sparkapplications"

# Provided by the platform: the in-cluster Kubernetes connection, authenticating
# with the pod's own ServiceAccount.
KUBERNETES_CONN_ID = "kubernetes_in_cluster"

# Next to this file in the repository, wherever git-sync put the checkout.
JOB_MANIFEST = Path(__file__).with_name("spark_job.yaml")
NAMESPACE = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
).read_text().strip()


@dag(
    dag_id="lakehouse_ingest",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "lakehouse"],
)
def lakehouse_ingest():
    @task
    def submit(**context) -> str:
        """Create the SparkApplication and return its name."""
        manifest = yaml.safe_load(JOB_MANIFEST.read_text())

        # Every run needs its own resource name. The Airflow run id is
        # unique per run but contains characters a Kubernetes name may not.
        suffix = re.sub(r"[^a-z0-9]+", "-", context["dag_run"].run_id.lower())
        name = f"{manifest['metadata']['name']}-{suffix}"[:40].strip("-")
        manifest["metadata"]["name"] = name
        manifest["metadata"]["namespace"] = NAMESPACE

        KubernetesHook(conn_id=KUBERNETES_CONN_ID).create_custom_object(
            group=API_GROUP,
            version=API_VERSION,
            plural=PLURAL,
            body=manifest,
            namespace=NAMESPACE,
        )
        print(f"submitted SparkApplication {name}")
        return name

    @task.sensor(poke_interval=15, timeout=3600, mode="reschedule")
    def wait_for_completion(name: str) -> bool:
        """Poll until the Spark job finishes. Fail the task if it failed."""
        application = KubernetesHook(conn_id=KUBERNETES_CONN_ID).get_custom_object(
            group=API_GROUP,
            version=API_VERSION,
            plural=PLURAL,
            name=name,
            namespace=NAMESPACE,
        )
        phase = application.get("status", {}).get("phase")
        print(f"SparkApplication {name} is in phase {phase}")

        if phase == "Failed":
            raise AirflowFailException(f"SparkApplication {name} failed")
        return phase == "Succeeded"

    wait_for_completion(submit())


lakehouse_ingest()
