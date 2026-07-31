# Databricks notebook source
# MAGIC %md
# MAGIC ### 03_gold_views
# MAGIC Builds the star schema as Spark SQL views on top of Silver — no physical
# MAGIC tables, no separate load step, so Gold always reflects the current
# MAGIC Silver state.

# COMMAND ----------

CATALOG = "retail_dwh"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gold.dim_customers
# MAGIC CRM is the authoritative source for identity; ERP fills in birthdate,
# MAGIC a fallback gender, and country. CRM gender wins on conflict.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id                              AS customer_id,
    ci.cst_key                             AS customer_number,
    ci.cst_firstname                       AS first_name,
    ci.cst_lastname                        AS last_name,
    la.cntry                               AS country,
    ci.cst_marital_status                  AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END                                     AS gender,
    ca.bdate                                AS birthdate,
    ci.cst_create_date                      AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la  ON ci.cst_key = la.cid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gold.dim_products
# MAGIC Current-state only — `prd_end_dt IS NULL` filters out superseded
# MAGIC product versions.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id                                                 AS product_id,
    pi.prd_key                                                AS product_number,
    pi.prd_nm                                                 AS product_name,
    pi.cat_id                                                 AS category_id,
    pc.cat                                                     AS category,
    pc.subcat                                                  AS subcategory,
    pc.maintenance                                             AS maintenance,
    pi.prd_cost                                                AS cost,
    pi.prd_line                                                AS product_line,
    pi.prd_start_dt                                            AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gold.fact_sales
# MAGIC Same grain as the Silver sales table, re-pointed to the Gold surrogate
# MAGIC keys.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num   AS order_number,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt  AS order_date,
    sd.sls_ship_dt   AS ship_date,
    sd.sls_due_dt    AS due_date,
    sd.sls_sales     AS sales_amount,
    sd.sls_quantity  AS quantity,
    sd.sls_price     AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products  dp ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers dc ON sd.sls_cust_id  = dc.customer_id
""")

# COMMAND ----------

display(spark.sql("SELECT * FROM gold.dim_customers LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM gold.fact_sales LIMIT 10"))
