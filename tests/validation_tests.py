# Databricks notebook source
# MAGIC %md
# MAGIC ### Validation Tests
# MAGIC Runs the Bronze / Silver / Gold checks from section 6 of the project
# MAGIC doc. Each cell should return zero rows (or an empty result) when the
# MAGIC pipeline is healthy — any output here means a rule broke somewhere
# MAGIC upstream. Every check also emits a structured log event so pass/fail
# MAGIC history is queryable over time, not just visible in the current run.

# COMMAND ----------

from observability.metrics_logger import log_event

CATALOG = "retail_dwh"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md #### 6.1 Bronze — row counts vs. source

# COMMAND ----------

for tbl in ("crm_cust_info", "crm_prd_info", "crm_sales_details",
            "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"):
    n = spark.table(f"bronze.{tbl}").count()
    print(tbl, n)
    log_event("validation_check", layer="bronze", table=tbl, row_count=n)

# COMMAND ----------

# MAGIC %md #### 6.2 Silver checks

# COMMAND ----------

dupe_cust = spark.sql("""
SELECT cst_id, COUNT(*) c FROM silver.crm_cust_info
GROUP BY cst_id HAVING COUNT(*) > 1
""")
display(dupe_cust)
log_event("validation_check", check="duplicate_cst_id", failing_rows=dupe_cust.count())

dupe_prd = spark.sql("""
SELECT prd_key, COUNT(*) c FROM silver.crm_prd_info
GROUP BY prd_key HAVING COUNT(*) > 1
""")
display(dupe_prd)
log_event("validation_check", check="duplicate_prd_key", failing_rows=dupe_prd.count())

# COMMAND ----------

# categorical columns limited to the approved value set
display(spark.sql("SELECT DISTINCT cst_gndr FROM silver.crm_cust_info"))
display(spark.sql("SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info"))
display(spark.sql("SELECT DISTINCT prd_line FROM silver.crm_prd_info"))
display(spark.sql("SELECT DISTINCT cntry FROM silver.erp_loc_a101"))

# COMMAND ----------

bad_dates = spark.sql("""
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt
""")
display(bad_dates)
log_event("validation_check", check="date_ordering", failing_rows=bad_dates.count())

# COMMAND ----------

bad_sales = spark.sql("""
SELECT * FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_quantity <= 0
   OR sls_price <= 0
""")
display(bad_sales)
log_event("validation_check", check="sales_reconciliation", failing_rows=bad_sales.count())

# COMMAND ----------

orphan_cust = spark.sql("""
SELECT s.sls_cust_id FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL
""")
display(orphan_cust)
log_event("validation_check", check="orphan_customer_fk", failing_rows=orphan_cust.count())

orphan_prd = spark.sql("""
SELECT s.sls_prd_key FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL
""")
display(orphan_prd)
log_event("validation_check", check="orphan_product_fk", failing_rows=orphan_prd.count())

# COMMAND ----------

# MAGIC %md #### 6.3 Gold checks

# COMMAND ----------

silver_rows = spark.table("silver.crm_sales_details").count()
gold_rows = spark.table("gold.fact_sales").count()
print("silver.crm_sales_details:", silver_rows, "| gold.fact_sales:", gold_rows)
log_event("validation_check", check="silver_gold_row_parity",
          silver_rows=silver_rows, gold_rows=gold_rows, passed=(silver_rows == gold_rows))
assert silver_rows == gold_rows, "row count mismatch between silver and gold fact"

# COMMAND ----------

missing_keys = spark.sql("""
SELECT * FROM gold.fact_sales
WHERE customer_key IS NULL OR product_key IS NULL
""")
display(missing_keys)
log_event("validation_check", check="missing_surrogate_keys", failing_rows=missing_keys.count())

# COMMAND ----------

display(spark.sql("""
SELECT dc.country, SUM(fs.sales_amount) AS total_sales
FROM gold.fact_sales fs
JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY total_sales DESC
"""))
