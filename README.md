# Retail Sales Data Warehouse — Medallion Architecture

Retail DWH built on Databricks Free Edition, integrating a CRM and an ERP
source into a single star-schema warehouse using the Bronze → Silver → Gold
pattern.

## Stack

- Unity Catalog (catalog: `retail_dwh`, schemas: `bronze` / `silver` / `gold`)
- Databricks serverless compute, no cluster management
- Delta Lake for storage at every layer
- **pandas** for all transform logic; Spark is used only where it's
  structurally required — Unity Catalog operations (`00_setup`) and the
  final Delta write on each notebook (`spark.createDataFrame(df).write...`),
  since Delta itself is a Spark-native format

## Repository structure

```
/datasets                       six source CSVs go here (see datasets/README.md)
/exploration                    data profiling — the findings every Silver rule is based on
/notebooks/00_setup             catalog, schemas, volume creation
/notebooks/01_bronze_ingestion  raw load of the six CSVs into Bronze Delta tables
/notebooks/02_silver_transformation
                                 cleansing, standardization, business rules
/notebooks/03_gold_tables       star schema (dim_customers, dim_products, fact_sales)
/tests                          validation queries (section 6 checks)
/docs                           project brief, schema design, data catalog,
                                 full project report, Databricks setup guide
```

## Running the pipeline

1. `00_setup` — creates the catalog/schemas/volume (run once).
2. Upload the six CSVs into `/Volumes/retail_dwh/bronze/raw_files/`.
3. `01_bronze_ingestion` — pandas reads the raw files off the Volume, lands
   them as Bronze Delta tables.
4. (optional but recommended) `exploration/data_profiling` — re-run this
   against any new batch of source data; it's the actual inspection every
   Silver rule below is based on, so a new batch can surface findings the
   current rules don't cover yet.
5. `02_silver_transformation` — pulls each Bronze table into pandas with
   `.toPandas()`, applies the cleansing rules, writes Silver Delta tables.
6. `03_gold_tables` — merges the Silver tables in pandas into the star
   schema, writes the three Gold Delta tables.
7. `tests/validation_tests` — re-runs the section 6 checks against the
   result, in pandas.

Every load uses `mode('overwrite')`, so the whole pipeline is idempotent —
safe to re-run end to end from a clean Bronze load.

**Note on Gold:** this is a change from a pure-PySpark design, where Gold
was three `CREATE OR REPLACE VIEW` statements that stayed live automatically.
A pandas DataFrame can't define a database view, so Gold here is physical
tables — which means `03_gold_tables` needs to be re-run after every Silver
refresh to stay current. See the comment at the top of that notebook.

**Note on scale:** `.toPandas()` collects a full table onto the driver node.
Fine for a dataset this size; if source volume grows significantly, revisit
whether pandas is still the right choice for Silver/Gold, or use Spark's
pandas API (`pyspark.pandas`) to keep the same pandas-style code distributed.

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
