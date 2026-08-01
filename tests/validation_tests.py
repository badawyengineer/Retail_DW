# Databricks notebook source
# MAGIC %md
# MAGIC ### Validation Tests
# MAGIC Runs the Bronze / Silver / Gold checks from section 6 of the project
# MAGIC doc. Each cell should print an empty/zero result when the pipeline is
# MAGIC healthy — any rows returned here mean a rule broke somewhere upstream.
# MAGIC
# MAGIC Same as the rest of the pipeline: each Delta table is pulled down with
# MAGIC `.toPandas()` and checked with pandas rather than Spark SQL.

# COMMAND ----------

import pandas as pd

CATALOG = "retail_dwh"

# COMMAND ----------

# MAGIC %md #### 6.1 Bronze — row counts vs. source
# MAGIC (compare against the counts printed by 01_bronze_ingestion / the raw
# MAGIC CSV row counts; Bronze itself does no filtering so this is a manual
# MAGIC cross-check rather than a rule.)

# COMMAND ----------

for tbl in ("crm_cust_info", "crm_prd_info", "crm_sales_details",
            "erp_cust_az12", "erp_loc_a101", "erp_px_cat_g1v2"):
    print(tbl, spark.table(f"{CATALOG}.bronze.{tbl}").count())

# COMMAND ----------

# MAGIC %md #### 6.2 Silver checks

# COMMAND ----------

silver_cust = spark.table(f"{CATALOG}.silver.crm_cust_info").toPandas()
silver_prd = spark.table(f"{CATALOG}.silver.crm_prd_info").toPandas()
silver_sales = spark.table(f"{CATALOG}.silver.crm_sales_details").toPandas()
silver_loc = spark.table(f"{CATALOG}.silver.erp_loc_a101").toPandas()

# no duplicate customer / product keys
print("duplicate cst_id:", silver_cust["cst_id"].duplicated().sum())
print("duplicate prd_key:", silver_prd["prd_key"].duplicated().sum())

# COMMAND ----------

# categorical columns limited to the approved value set
print("cst_gndr values:", silver_cust["cst_gndr"].unique().tolist())
print("cst_marital_status values:", silver_cust["cst_marital_status"].unique().tolist())
print("prd_line values:", silver_prd["prd_line"].unique().tolist())
print("cntry values:", silver_loc["cntry"].unique().tolist())

# COMMAND ----------

# date columns: order_dt shouldn't be after ship_dt or due_dt
bad_dates = silver_sales[
    (silver_sales["sls_order_dt"] > silver_sales["sls_due_dt"])
    | (silver_sales["sls_order_dt"] > silver_sales["sls_ship_dt"])
]
print("rows with order_dt after ship/due dt:", len(bad_dates))

# COMMAND ----------

# sls_sales == quantity * price, both positive
bad_sales = silver_sales[
    (silver_sales["sls_sales"] != silver_sales["sls_quantity"] * silver_sales["sls_price"])
    | (silver_sales["sls_quantity"] <= 0)
    | (silver_sales["sls_price"] <= 0)
]
print("rows failing sales = quantity * price:", len(bad_sales))

# COMMAND ----------

# referential integrity: every sales customer/product exists in the dimension source
orphan_customers = silver_sales[~silver_sales["sls_cust_id"].isin(silver_cust["cst_id"])]
print("sales rows with unknown customer:", len(orphan_customers))

orphan_products = silver_sales[~silver_sales["sls_prd_key"].isin(silver_prd["prd_key"])]
print("sales rows with unknown product:", len(orphan_products))

# COMMAND ----------

# MAGIC %md #### 6.3 Gold checks

# COMMAND ----------

gold_fact = spark.table(f"{CATALOG}.gold.fact_sales").toPandas()

print("silver.crm_sales_details:", len(silver_sales), "| gold.fact_sales:", len(gold_fact))
assert len(silver_sales) == len(gold_fact), "row count mismatch between silver and gold fact"

# COMMAND ----------

# no unexpected NULL surrogate keys
missing_keys = gold_fact[gold_fact["customer_key"].isna() | gold_fact["product_key"].isna()]
print("fact_sales rows with missing surrogate keys:", len(missing_keys))

# COMMAND ----------

# spot-check: total sales by country
gold_cust = spark.table(f"{CATALOG}.gold.dim_customers").toPandas()
by_country = (
    gold_fact.merge(gold_cust[["customer_key", "country"]], on="customer_key", how="left")
    .groupby("country")["sales_amount"].sum()
    .sort_values(ascending=False)
)
print(by_country)
