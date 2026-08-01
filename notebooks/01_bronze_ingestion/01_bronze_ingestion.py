# Databricks notebook source
# MAGIC %md
# MAGIC ### 01_bronze_ingestion
# MAGIC Loads the six source CSVs from the Volume as-is into managed Delta tables.
# MAGIC No business logic here — the only job of this layer is a reliable,
# MAGIC untransformed copy of the source.
# MAGIC
# MAGIC Reads and holds the data with **pandas**, since a Volume path is just a
# MAGIC regular filesystem path on the driver (`/Volumes/...`) and these files
# MAGIC are small enough for a single node. Delta itself is a Spark-native
# MAGIC format, so the only place Spark shows up is the final write — each
# MAGIC pandas DataFrame gets handed to `spark.createDataFrame()` right before
# MAGIC `saveAsTable()`.

# COMMAND ----------

import pandas as pd

CATALOG = "retail_dwh"
RAW_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

# read everything as strings at this stage — type-casting and validation
# belong in Silver, not here
READ_OPTS = {"dtype": str, "keep_default_na": False}

def load_to_bronze(csv_name, table_name):
    pdf = pd.read_csv(f"{RAW_PATH}/{csv_name}", **READ_OPTS)
    sdf = spark.createDataFrame(pdf)
    sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.{table_name}")
    print(f"{table_name:<20} {len(pdf):>8} rows")
    return pdf

# COMMAND ----------

# MAGIC %md
# MAGIC #### CRM source

# COMMAND ----------

crm_cust_info = load_to_bronze("cust_info.csv", "crm_cust_info")

# COMMAND ----------

crm_prd_info = load_to_bronze("prd_info.csv", "crm_prd_info")

# COMMAND ----------

crm_sales_details = load_to_bronze("sales_details.csv", "crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ERP source

# COMMAND ----------

erp_cust_az12 = load_to_bronze("CUST_AZ12.csv", "erp_cust_az12")

# COMMAND ----------

erp_loc_a101 = load_to_bronze("LOC_A101.csv", "erp_loc_a101")

# COMMAND ----------

erp_px_cat_g1v2 = load_to_bronze("PX_CAT_G1V2.csv", "erp_px_cat_g1v2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sanity check — Bronze row counts vs. source files
# MAGIC Run this after every load; each count should match the corresponding
# MAGIC CSV exactly (see section 6.1 of the project doc).

# COMMAND ----------

for tbl in ("crm_cust_info", "crm_prd_info", "crm_sales_details",
            "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"):
    n = spark.table(f"{CATALOG}.bronze.{tbl}").count()
    print(f"{tbl:<20} {n:>8} rows")
