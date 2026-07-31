# Retail Sales Data Warehouse — Medallion Architecture

Retail DWH built on Databricks Free Edition, integrating a CRM and an ERP
source into a single star-schema warehouse using the Bronze → Silver → Gold
pattern.

## Stack

- Unity Catalog (catalog: `retail_dwh`, schemas: `bronze` / `silver` / `gold`)
- Databricks serverless compute, no cluster management
- Delta Lake for Bronze/Silver, Spark SQL views for Gold
- PySpark + Spark SQL notebooks

## Repository structure

```
/datasets                       six source CSVs go here (see datasets/README.md)
/notebooks/00_setup             catalog, schemas, volume creation
/notebooks/01_bronze_ingestion  raw load of the six CSVs into Bronze Delta tables
/notebooks/02_silver_transformation
                                 cleansing, standardization, business rules
/notebooks/03_gold_views        star schema (dim_customers, dim_products, fact_sales)
/tests                          validation queries (section 6 checks)
/docs                           project brief, data catalog, Databricks setup guide
/window_functions_practice      unrelated SQL exercise (al_noor_trading schema/seed)
```

`/window_functions_practice` is a separate SQL practice set (window
functions: RANK, LAG/LEAD, running totals, etc.) — it isn't part of the
medallion pipeline, just bundled in the same submission for convenience.

## Running the pipeline

1. `00_setup` — creates the catalog/schemas/volume (run once).
2. Upload the six CSVs into `/Volumes/retail_dwh/bronze/raw_files/`.
3. `01_bronze_ingestion` — lands the raw files as Bronze Delta tables.
4. `02_silver_transformation` — reads Bronze, applies the cleansing rules,
   writes Silver Delta tables.
5. `03_gold_views` — creates/replaces the three Gold views.
6. `tests/validation_tests` — re-runs the section 6 checks against the
   result.

Every load uses `mode('overwrite')`, so the whole pipeline is idempotent —
safe to re-run end to end from a clean Bronze load.

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
