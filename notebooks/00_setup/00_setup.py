# Databricks notebook source
# MAGIC %md
# MAGIC ### 00_setup
# MAGIC Creates the Unity Catalog objects the rest of the pipeline depends on:
# MAGIC a dedicated catalog, the three medallion schemas, and a volume that acts
# MAGIC as the landing zone for the raw CRM/ERP files. Run this once per workspace
# MAGIC before touching the ingestion notebooks.

# COMMAND ----------

CATALOG = "retail_dwh"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# Volume for the raw source files. Upload cust_info.csv, prd_info.csv,
# sales_details.csv, CUST_AZ12.csv, LOC_A101.csv and PX_CAT_G1V2.csv here
# through the workspace UI (or Databricks CLI) before running 01_bronze_ingestion.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected volume path once the files are uploaded:
# MAGIC `/Volumes/retail_dwh/bronze/raw_files/`
