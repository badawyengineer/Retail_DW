"""
orchestration/retail_dwh_dag.py

Apache Airflow DAG that runs the pipeline end to end: 00_setup is a one-time
step (excluded from the scheduled DAG — see the note below), then
01 -> 02 -> 03 -> validation_tests, each as a Databricks notebook task.

This uses the Databricks provider for Airflow (DatabricksNotebookOperator),
which submits each notebook as a job run on your Databricks workspace via
a Databricks connection configured in Airflow (Admin -> Connections ->
databricks_default) rather than Airflow executing any Spark code itself.

Not tested against a live Airflow instance in this repo — no Airflow
deployment was available to verify against. The DAG structure and operator
usage are correct as written; confirm your own `databricks_conn_id`,
`existing_cluster_id`/job-cluster config, and notebook paths (adjust
NOTEBOOK_BASE_PATH below to wherever this repo actually lives once
imported into your Databricks workspace) before scheduling it for real.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator

NOTEBOOK_BASE_PATH = "/Workspace/Repos/<your-workspace-user>/Retail_DW"

# Adjust to an existing cluster id, or swap existing_cluster_id for
# job_cluster_key + a job_cluster spec if you'd rather Airflow spin up a
# fresh job cluster per run instead of reusing a standing one.
DATABRICKS_CLUSTER_ID = "<your-cluster-id>"

default_args = {
    "owner": "retail_dwh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["<your-alert-email>@example.com"],
}

with DAG(
    dag_id="retail_dwh_medallion_pipeline",
    description="Bronze -> Silver -> Gold -> validation for the Retail DWH project",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retail_dwh", "databricks", "medallion"],
) as dag:

    bronze = DatabricksNotebookOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/notebooks/01_bronze_ingestion/01_bronze_ingestion",
        source="WORKSPACE",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
    )

    silver = DatabricksNotebookOperator(
        task_id="silver_transformation",
        databricks_conn_id="databricks_default",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/notebooks/02_silver_transformation/02_silver_transformation",
        source="WORKSPACE",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
    )

    gold = DatabricksNotebookOperator(
        task_id="gold_views",
        databricks_conn_id="databricks_default",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/notebooks/03_gold_views/03_gold_views",
        source="WORKSPACE",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
    )

    validate = DatabricksNotebookOperator(
        task_id="validation_tests",
        databricks_conn_id="databricks_default",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/tests/validation_tests",
        source="WORKSPACE",
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
    )

    # 00_setup is deliberately not part of this scheduled DAG — it creates
    # the catalog/schema/volume, which you run once by hand when setting
    # up a new workspace, not on every daily run.
    bronze >> silver >> gold >> validate
