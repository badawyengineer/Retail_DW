# Getting this repo running in Databricks Free Edition

The project doc (section 4.1) specifies Databricks Free Edition with Unity
Catalog, serverless compute, notebooks, and a Volume — no cluster and no
Databricks CLI/Asset Bundle setup required for the core pipeline.
Everything below is doable from the Workspace UI.

Transform logic in `/notebooks` is PySpark. `/dlt_pipeline` is an
alternative Delta Live Tables implementation of the same logic — pick one
path or the other, not both against the same tables (see that folder's
notebook for why).

## 1. Load the notebooks into the workspace

Two options:

- **Repos (recommended if submitting via GitHub):** Workspace → Repos →
  Add Repo → paste this repo's GitHub URL. Databricks clones it and every
  `.py` notebook shows up ready to open — no manual import needed.
- **Manual import:** Workspace → your folder → Import → select each `.py`
  file under `/notebooks/00_setup`, `/notebooks/01_bronze_ingestion`,
  `/notebooks/02_silver_transformation`, `/notebooks/03_gold_views`,
  `/exploration`, and `/tests/validation_tests.py`. Databricks recognizes
  the `# Databricks notebook source` header and `# COMMAND ----------`
  cell markers automatically, so each file opens as a proper multi-cell
  notebook, not a flat script.

## 2. Run order — core pipeline

1. Open `00_setup` and run all cells — creates the `retail_dwh` catalog,
   the `bronze`/`silver`/`gold` schemas, and the `raw_files` volume.
2. Go to Catalog → `retail_dwh` → `bronze` → `raw_files` (or Catalog
   Explorer's Volume UI) and upload the six CSVs from `/datasets`.
3. (optional) Run `exploration/data_profiling` — the findings behind every
   Silver rule; worth re-running against any new batch of source data.
4. Run `01_bronze_ingestion`, then `02_silver_transformation`, then
   `03_gold_views`, each with "Run All."
5. Run `tests/validation_tests.py` and confirm every check cell comes back
   empty.

Gold is views again (`03_gold_views`), so unlike the pandas revision of
this project, you don't need to manually re-run Gold after a Silver
reload — it reflects the latest Silver data automatically.

## 3. Optional: Delta Live Tables path

Instead of steps 4–5 above, you can run `dlt_pipeline/dlt_pipeline.py` as
a DLT Pipeline: Workflows → Delta Live Tables → Create Pipeline → point
the source at that file, target catalog/schema `retail_dwh`. DLT manages
run order itself (based on the `dlt.read()` dependencies in the code) and
gives you a built-in event log and data-quality dashboard from the
`@dlt.expect` constraints — no `metrics_logger` calls needed on this path.

## 4. Optional: orchestration with Airflow

`orchestration/retail_dwh_dag.py` is an Airflow DAG that chains
01 → 02 → 03 → validation as Databricks job runs, on a daily schedule with
retries and failure email alerts. Requires a separate Airflow instance
with the `apache-airflow-providers-databricks` package installed and a
`databricks_default` connection configured (Admin → Connections) pointing
at your workspace. Not tested against a live Airflow instance — confirm
`NOTEBOOK_BASE_PATH` and `DATABRICKS_CLUSTER_ID` in the file match your
actual workspace before scheduling it.

## 5. Optional: streaming extension

`streaming_extension/kafka_sales_ingestion.py` is what real-time Bronze
ingestion for `sales_details` would look like if it came from a Kafka
topic instead of a CSV export. **Not required by the current data
source** (it's static CSVs) — this is an architecture demonstration, kept
deliberately separate from the core pipeline. Only relevant if you
actually have a Kafka cluster to point it at.

## 6. Optional: observability

Every core-pipeline notebook calls `observability/metrics_logger.log_event()`
after each load/check, printing a structured JSON line that lands in the
cluster's driver logs. `observability/cribl_edge_source.yml` is an example
Cribl Edge Source config for collecting those lines centrally. See
`observability/README.md` for what this gives you and its honest
limitations (untested against a live Cribl instance).

## 7. Compute

No cluster to create for the core pipeline — attach each notebook to
**Serverless** compute (the default in Free Edition) rather than a
classic cluster. The Airflow and Kafka paths, if you use them, will need
their own cluster/job-cluster configuration — see the comments in those
files.
