# Databricks notebook source
# MAGIC %md
# MAGIC ### exploration / data_profiling
# MAGIC Run this against Bronze **before** writing any Silver logic. Every rule
# MAGIC in `02_silver_transformation` traces back to a finding in this
# MAGIC notebook — nothing in Silver is guessed, it's a response to something
# MAGIC found here. Re-run this whenever new source data lands, since a rule
# MAGIC written against one batch can go stale against the next.
# MAGIC
# MAGIC pandas throughout, same as the rest of the pipeline.

# COMMAND ----------

import pandas as pd

CATALOG = "retail_dwh"

def read_bronze(table):
    return spark.table(f"{CATALOG}.bronze.{table}").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_cust_info
# MAGIC **Finding:** `cst_id` has duplicate values — the same customer appears
# MAGIC multiple times with different `cst_create_date`s, i.e. this table holds
# MAGIC a change history, not one row per customer. **Rule:** keep only the
# MAGIC latest row per `cst_id`.
# MAGIC
# MAGIC **Finding:** `cst_gndr` / `cst_marital_status` use single-letter codes,
# MAGIC inconsistently cased, sometimes padded with whitespace. **Rule:**
# MAGIC trim + uppercase + map to a fixed value set, default `'n/a'` for
# MAGIC anything unrecognized.

# COMMAND ----------

df = read_bronze("crm_cust_info")

print("duplicate cst_id count:", df["cst_id"].duplicated().sum())
print("cst_gndr raw distinct values:", df["cst_gndr"].unique().tolist())
print("cst_marital_status raw distinct values:", df["cst_marital_status"].unique().tolist())
print("rows with blank/null cst_id:", (df["cst_id"].isna() | (df["cst_id"] == "")).sum())

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_prd_info
# MAGIC **Finding:** `prd_key` is a compound key — the first 5 characters are a
# MAGIC category id in a different delimiter style (`-`) than the ERP category
# MAGIC table uses (`_`). **Rule:** split it, normalize the delimiter.
# MAGIC
# MAGIC **Finding:** `prd_end_dt` from the source doesn't reliably reflect when
# MAGIC a product version actually ended — some rows have it null even though
# MAGIC a newer version of the same `prd_key` exists with a later
# MAGIC `prd_start_dt`. **Rule:** recompute `prd_end_dt` as one day before the
# MAGIC next version's `prd_start_dt`, rather than trusting the source column.
# MAGIC
# MAGIC **Finding:** `prd_cost` has missing values. **Rule:** default to 0
# MAGIC rather than dropping the row.

# COMMAND ----------

df = read_bronze("crm_prd_info")

print("prd_key sample (first 5 chars vs rest):",
      df["prd_key"].str[:5].unique()[:5].tolist())
print("prd_cost nulls:", pd.to_numeric(df["prd_cost"], errors="coerce").isna().sum())
print("prd_line raw distinct values:", df["prd_line"].unique().tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_sales_details
# MAGIC **Finding:** date columns (`sls_order_dt`, `sls_ship_dt`, `sls_due_dt`)
# MAGIC are stored as `YYYYMMDD` integers, and some are `0` (placeholder for
# MAGIC "no date") rather than null. **Rule:** treat `0` and any non-8-digit
# MAGIC value as null, then parse the rest as real dates.
# MAGIC
# MAGIC **Finding:** `sls_sales` doesn't always equal `sls_quantity * sls_price`
# MAGIC — sometimes it's null, negative, or just arithmetically wrong.
# MAGIC **Finding:** `sls_price` is sometimes null or non-positive despite
# MAGIC `sls_sales` and `sls_quantity` both being present. **Rule:** derive
# MAGIC whichever of the two is bad from the other two columns.

# COMMAND ----------

df = read_bronze("crm_sales_details")

print("sls_order_dt == '0':", (df["sls_order_dt"].astype(str) == "0").sum())
qty = pd.to_numeric(df["sls_quantity"], errors="coerce")
price = pd.to_numeric(df["sls_price"], errors="coerce")
sales = pd.to_numeric(df["sls_sales"], errors="coerce")
print("rows where sales != quantity * price:", (sales != qty * price).sum())
print("rows with null/non-positive price:", (price.isna() | (price <= 0)).sum())

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_cust_az12
# MAGIC **Finding:** `cid` sometimes carries a `NAS` prefix that CRM's
# MAGIC `cst_key` doesn't have, which breaks the join. **Rule:** strip it.
# MAGIC
# MAGIC **Finding:** a handful of `bdate` values are in the future.
# MAGIC **Rule:** null those out rather than keep an impossible birthdate.

# COMMAND ----------

df = read_bronze("erp_cust_az12")

print("cid with NAS prefix:", df["cid"].str.startswith("NAS").sum())
bdate = pd.to_datetime(df["bdate"], errors="coerce")
print("bdate values in the future:", (bdate > pd.Timestamp.today()).sum())
print("gen raw distinct values:", df["gen"].unique().tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_loc_a101
# MAGIC **Finding:** `cid` has hyphens that CRM's `cst_key` doesn't, which
# MAGIC breaks the join. **Rule:** strip them.
# MAGIC
# MAGIC **Finding:** `cntry` has multiple spellings for the same country
# MAGIC (`US`/`USA`, `DE`, blanks). **Rule:** map to one canonical name per
# MAGIC country.

# COMMAND ----------

df = read_bronze("erp_loc_a101")

print("cntry raw distinct values:", df["cntry"].unique().tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_px_cat_g1v2
# MAGIC **Finding:** clean already — no nulls, no inconsistent casing beyond
# MAGIC stray whitespace. **Rule:** trim only, no mapping logic needed.

# COMMAND ----------

df = read_bronze("erp_px_cat_g1v2")

print("nulls per column:")
print(df.isna().sum())
