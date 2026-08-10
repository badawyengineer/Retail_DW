# Databricks notebook source
# MAGIC %md
# MAGIC ### 01_bronze_ingestion
# MAGIC Loads the six source CSVs from the Volume as-is into managed Delta tables.
# MAGIC No business logic here — the only job of this layer is a reliable,
# MAGIC untransformed copy of the source, with a row count printed after every
# MAGIC write so the notebook run history doubles as the load audit log.
# MAGIC
# MAGIC PySpark throughout — reads are distributed rather than pulled onto a
# MAGIC single driver, which matters once source volume grows past what pandas
# MAGIC comfortably holds.

# COMMAND ----------

from observability.metrics_logger import log_event

CATALOG = "retail_dwh"
RAW_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

# read everything as strings at this stage — type-casting and validation
# belong in Silver, not here
READ_OPTS = {"header": "true", "inferSchema": "false"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### CRM source

# COMMAND ----------

crm_cust_info = spark.read.options(**READ_OPTS).csv(f"{RAW_PATH}/cust_info.csv")
(crm_cust_info.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.crm_cust_info"))
n = crm_cust_info.count()
print("crm_cust_info rows:", n)
log_event("bronze_load", table="crm_cust_info", row_count=n)

# COMMAND ----------

crm_prd_info = spark.read.options(**READ_OPTS).csv(f"{RAW_PATH}/prd_info.csv")
(crm_prd_info.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.crm_prd_info"))
n = crm_prd_info.count()
print("crm_prd_info rows:", n)
log_event("bronze_load", table="crm_prd_info", row_count=n)

# COMMAND ----------

crm_sales_details = spark.read.options(**READ_OPTS).csv(f"{RAW_PATH}/sales_details.csv")
(crm_sales_details.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.crm_sales_details"))
n = crm_sales_details.count()
print("crm_sales_details rows:", n)
log_event("bronze_load", table="crm_sales_details", row_count=n)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ERP source

# COMMAND ----------

erp_cust_az12 = spark.read.options(**READ_OPTS).csv(f"{RAW_PATH}/CUST_AZ12.csv")
(erp_cust_az12.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.erp_cust_az12"))
n = erp_cust_az12.count()
print("erp_cust_az12 rows:", n)
log_event("bronze_load", table="erp_cust_az12", row_count=n)

# COMMAND ----------

erp_loc_a101 = spark.read.options(**READ_OPTS).csv(f"{RAW_PATH}/LOC_A101.csv")
(erp_loc_a101.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.erp_loc_a101"))
n = erp_loc_a101.count()
print("erp_loc_a101 rows:", n)
log_event("bronze_load", table="erp_loc_a101", row_count=n)

# COMMAND ----------

erp_px_cat_g1v2 = spark.read.options(**READ_OPTS).csv(f"{RAW_PATH}/PX_CAT_G1V2.csv")
(erp_px_cat_g1v2.write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.bronze.erp_px_cat_g1v2"))
n = erp_px_cat_g1v2.count()
print("erp_px_cat_g1v2 rows:", n)
log_event("bronze_load", table="erp_px_cat_g1v2", row_count=n)

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
