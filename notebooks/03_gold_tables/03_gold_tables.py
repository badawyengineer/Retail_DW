# Databricks notebook source
# MAGIC %md
# MAGIC ### 03_gold_tables
# MAGIC Builds the star schema in pandas by merging the Silver tables, then
# MAGIC writes each result as a Delta table.
# MAGIC
# MAGIC **Tradeoff worth knowing about:** the original PySpark design made
# MAGIC Gold a set of `CREATE OR REPLACE VIEW` statements, so Gold always
# MAGIC reflected the latest Silver data with no separate load step. A pandas
# MAGIC DataFrame is just an in-memory object — it can't define a database
# MAGIC view — so this version writes physical tables instead. That means
# MAGIC **this notebook has to be re-run after every Silver refresh** to keep
# MAGIC Gold current; it's no longer automatic.

# COMMAND ----------

import pandas as pd

CATALOG = "retail_dwh"

def read_silver(table):
    return spark.table(f"{CATALOG}.silver.{table}").toPandas()

def write_gold(pdf, table):
    spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gold.dim_customers
# MAGIC CRM is the authoritative source for identity; ERP fills in birthdate,
# MAGIC a fallback gender, and country. CRM gender wins on conflict.

# COMMAND ----------

cust_info = read_silver("crm_cust_info")
cust_az12 = read_silver("erp_cust_az12")
loc_a101 = read_silver("erp_loc_a101")

dim_customers = (
    cust_info
    .merge(cust_az12, left_on="cst_key", right_on="cid", how="left")
    .merge(loc_a101, left_on="cst_key", right_on="cid", how="left", suffixes=("", "_loc"))
    .sort_values("cst_id")
    .reset_index(drop=True)
)

dim_customers["gender"] = dim_customers["cst_gndr"].where(
    dim_customers["cst_gndr"] != "n/a", dim_customers["gen"].fillna("n/a")
)

dim_customers = dim_customers.assign(customer_key=dim_customers.index + 1)[[
    "customer_key", "cst_id", "cst_key", "cst_firstname", "cst_lastname",
    "cntry", "cst_marital_status", "gender", "bdate", "cst_create_date",
]].rename(columns={
    "cst_id": "customer_id",
    "cst_key": "customer_number",
    "cst_firstname": "first_name",
    "cst_lastname": "last_name",
    "cntry": "country",
    "cst_marital_status": "marital_status",
    "bdate": "birthdate",
    "cst_create_date": "create_date",
})

write_gold(dim_customers, "dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gold.dim_products
# MAGIC Current-state only — `prd_end_dt` null filters out superseded product
# MAGIC versions.

# COMMAND ----------

prd_info = read_silver("crm_prd_info")
px_cat = read_silver("erp_px_cat_g1v2")

dim_products = (
    prd_info[prd_info["prd_end_dt"].isna()]
    .merge(px_cat, left_on="cat_id", right_on="id", how="left")
    .sort_values(["prd_start_dt", "prd_key"])
    .reset_index(drop=True)
)

dim_products = dim_products.assign(product_key=dim_products.index + 1)[[
    "product_key", "prd_id", "prd_key", "prd_nm", "cat_id",
    "cat", "subcat", "maintenance", "prd_cost", "prd_line", "prd_start_dt",
]].rename(columns={
    "prd_id": "product_id",
    "prd_key": "product_number",
    "prd_nm": "product_name",
    "cat_id": "category_id",
    "cat": "category",
    "subcat": "subcategory",
    "prd_cost": "cost",
    "prd_line": "product_line",
    "prd_start_dt": "start_date",
})

write_gold(dim_products, "dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gold.fact_sales
# MAGIC Same grain as the Silver sales table, re-pointed to the Gold surrogate
# MAGIC keys via merges against the two dimension tables just built.

# COMMAND ----------

sales_details = read_silver("crm_sales_details")

fact_sales = (
    sales_details
    .merge(dim_products[["product_key", "product_number"]],
           left_on="sls_prd_key", right_on="product_number", how="left")
    .merge(dim_customers[["customer_key", "customer_id"]],
           left_on="sls_cust_id", right_on="customer_id", how="left")
)[[
    "sls_ord_num", "product_key", "customer_key", "sls_order_dt",
    "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price",
]].rename(columns={
    "sls_ord_num": "order_number",
    "sls_order_dt": "order_date",
    "sls_ship_dt": "ship_date",
    "sls_due_dt": "due_date",
    "sls_sales": "sales_amount",
    "sls_quantity": "quantity",
    "sls_price": "price",
})

write_gold(fact_sales, "fact_sales")

# COMMAND ----------

display(spark.table(f"{CATALOG}.gold.dim_customers").limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.gold.fact_sales").limit(10))
