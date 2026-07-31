# Databricks notebook source
# MAGIC %md
# MAGIC ### Validation Tests
# MAGIC Runs the Bronze / Silver / Gold checks from section 6 of the project
# MAGIC doc. Each cell should return zero rows (or an empty result) when the
# MAGIC pipeline is healthy — any output here means a rule broke somewhere
# MAGIC upstream.

# COMMAND ----------

CATALOG = "retail_dwh"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md #### 6.1 Bronze — row counts vs. source
# MAGIC (compare against the counts printed by 01_bronze_ingestion / the raw
# MAGIC CSV row counts; Bronze itself does no filtering so this is a manual
# MAGIC cross-check rather than a query.)

# COMMAND ----------

for tbl in ("crm_cust_info", "crm_prd_info", "crm_sales_details",
            "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"):
    print(tbl, spark.table(f"bronze.{tbl}").count())

# COMMAND ----------

# MAGIC %md #### 6.2 Silver checks

# COMMAND ----------

# no duplicate customer / product keys
display(spark.sql("""
SELECT cst_id, COUNT(*) c FROM silver.crm_cust_info
GROUP BY cst_id HAVING COUNT(*) > 1
"""))

display(spark.sql("""
SELECT prd_key, COUNT(*) c FROM silver.crm_prd_info
GROUP BY prd_key HAVING COUNT(*) > 1
"""))

# COMMAND ----------

# categorical columns limited to the approved value set
display(spark.sql("SELECT DISTINCT cst_gndr FROM silver.crm_cust_info"))
display(spark.sql("SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info"))
display(spark.sql("SELECT DISTINCT prd_line FROM silver.crm_prd_info"))
display(spark.sql("SELECT DISTINCT cntry FROM silver.erp_loc_a101"))

# COMMAND ----------

# date columns: no leftover placeholder integers, no invalid values
display(spark.sql("""
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_due_dt OR sls_order_dt > sls_ship_dt
"""))

# COMMAND ----------

# sls_sales == quantity * price, both positive
display(spark.sql("""
SELECT * FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_quantity <= 0
   OR sls_price <= 0
"""))

# COMMAND ----------

# referential integrity: every sales customer/product exists in the dimension source
display(spark.sql("""
SELECT s.sls_cust_id FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL
"""))

display(spark.sql("""
SELECT s.sls_prd_key FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL
"""))

# COMMAND ----------

# MAGIC %md #### 6.3 Gold checks

# COMMAND ----------

silver_rows = spark.table("silver.crm_sales_details").count()
gold_rows = spark.table("gold.fact_sales").count()
print("silver.crm_sales_details:", silver_rows, "| gold.fact_sales:", gold_rows)
assert silver_rows == gold_rows, "row count mismatch between silver and gold fact"

# COMMAND ----------

# no unexpected NULL surrogate keys
display(spark.sql("""
SELECT * FROM gold.fact_sales
WHERE customer_key IS NULL OR product_key IS NULL
"""))

# COMMAND ----------

# spot-check: total sales by country, matched against a manual silver-side rollup
display(spark.sql("""
SELECT dc.country, SUM(fs.sales_amount) AS total_sales
FROM gold.fact_sales fs
JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dc.country
ORDER BY total_sales DESC
"""))
