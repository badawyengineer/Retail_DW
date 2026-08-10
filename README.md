# Retail Sales Data Warehouse — Medallion Architecture

Retail DWH built on Databricks Free Edition, integrating a CRM and an ERP
source into a single star-schema warehouse using the Bronze → Silver → Gold
pattern.

## Stack

- Unity Catalog (catalog: `retail_dwh`, schemas: `bronze` / `silver` / `gold`)
- Databricks serverless compute for the core pipeline
- Delta Lake for storage at every layer
- **PySpark + Spark SQL** for all transform logic
- Optional: Delta Live Tables, Apache Airflow, Kafka, Cribl Edge — see
  "What's new" below

## Repository structure

```
/datasets                       six source CSVs go here (see datasets/README.md)
/exploration                    data profiling — the findings every Silver rule is based on
/notebooks/00_setup             catalog, schemas, volume creation
/notebooks/01_bronze_ingestion  raw load of the six CSVs into Bronze Delta tables
/notebooks/02_silver_transformation
                                 cleansing, standardization, business rules
/notebooks/03_gold_views        star schema as SQL views (dim_customers, dim_products, fact_sales)
/tests                          validation queries (section 6 checks)
/docs                           project brief, schema design, data catalog,
                                 full project report, Databricks setup guide
/dlt_pipeline                   alternative Delta Live Tables implementation
/orchestration                  Apache Airflow DAG for scheduled runs
/streaming_extension            optional Kafka-based real-time ingestion demo
/observability                  structured pipeline logging + Cribl Edge config
```

## Running the pipeline

1. `00_setup` — creates the catalog/schemas/volume (run once).
2. Upload the six CSVs into `/Volumes/retail_dwh/bronze/raw_files/`.
3. (optional) `exploration/data_profiling` — re-run against any new batch
   of source data; it's the actual inspection every Silver rule is based on.
4. `01_bronze_ingestion` → `02_silver_transformation` → `03_gold_views`,
   each with "Run All."
5. `tests/validation_tests` — confirm every check comes back empty.

Every load uses `mode('overwrite')`, so the whole pipeline is idempotent —
safe to re-run end to end from a clean Bronze load. Gold is SQL views, so
it reflects the latest Silver data automatically, no separate refresh step.

Full run-order detail, including the optional components below, is in
`docs/databricks_setup_guide.md`.

## What's new in this revision

The pipeline was previously rewritten in pandas for a portfolio variant;
this revision moves it back to PySpark and adds several optional,
clearly-separated extensions:

- **Back to PySpark** (`/notebooks`, `/tests`, `/exploration`) — distributed
  transforms instead of pandas' single-node `.toPandas()` collection. Gold
  is SQL views again as a result (pandas couldn't create a view; PySpark
  can).
- **`/dlt_pipeline`** — the same Bronze→Silver→Gold logic re-implemented
  declaratively as Delta Live Tables, with `@dlt.expect` data-quality
  constraints that publish pass/fail metrics to DLT's built-in event log
  automatically. An alternative to `/notebooks`, not a replacement — pick
  one path to actually run.
- **`/orchestration`** — an Apache Airflow DAG (`retail_dwh_dag.py`) that
  chains Bronze → Silver → Gold → validation as scheduled Databricks job
  runs, with retries and failure alerting.
- **`/observability`** — every core notebook now emits structured JSON
  log events (row counts, validation pass/fail) via
  `metrics_logger.log_event()`. Includes an example Cribl Edge Source
  config for collecting those events centrally from Databricks' driver logs.
- **`/streaming_extension`** — a Kafka-based Structured Streaming
  alternative to the batch CSV load for `sales_details`. **Explicitly
  optional**: the actual source data is static CSVs, not a live feed, so
  this isn't required by the use case — it's included as an architecture
  demonstration, kept separate from the core pipeline on purpose.

Honest caveat carried across all four additions: none of them were tested
against a live Kafka cluster, Airflow instance, or Cribl Edge deployment —
none were available in this environment. The code and configs are correct
as written against each tool's documented usage; connection details
(cluster IDs, broker addresses, conn IDs) are left as placeholders and
should be confirmed against your actual infrastructure before relying on
them. The core PySpark pipeline (`/notebooks`) has no such caveat — that
part follows the same pattern as the original, tested-in-spirit revisions.

## Data sources

| System | File | Grain |
|---|---|---|
| CRM | cust_info.csv | one row per customer record (historical duplicates) |
| CRM | prd_info.csv | one row per product version |
| CRM | sales_details.csv | one row per order line item |
| ERP | CUST_AZ12.csv | one row per customer |
| ERP | LOC_A101.csv | one row per customer |
| ERP | PX_CAT_G1V2.csv | one row per category id |

See `docs/data_catalog.md` for the full Gold-layer column reference.
