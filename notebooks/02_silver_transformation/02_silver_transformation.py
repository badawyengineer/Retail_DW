# Databricks notebook source
# MAGIC %md
# MAGIC ### 02_silver_transformation
# MAGIC Reads every Bronze table untouched, applies the cleansing/standardization
# MAGIC rules that came out of the profiling pass (project doc, section 4.4),
# MAGIC and writes each result as a Silver Delta table. Every table gains a
# MAGIC `dwh_create_date` column marking when the row was loaded into Silver.
# MAGIC
# MAGIC Transform logic is all **pandas** — each Bronze Delta table is pulled
# MAGIC into a pandas DataFrame with `.toPandas()`, cleaned, then handed back to
# MAGIC Spark only for the final `saveAsTable()` write. `.toPandas()` collects
# MAGIC the full table onto the driver, which is fine for a dataset this size,
# MAGIC but is the tradeoff worth knowing about vs. the distributed PySpark
# MAGIC version.

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, date

CATALOG = "retail_dwh"

def read_bronze(table):
    return spark.table(f"{CATALOG}.bronze.{table}").toPandas()

def write_silver(pdf, table):
    pdf = pdf.copy()
    pdf["dwh_create_date"] = datetime.now()
    spark.createDataFrame(pdf).write.mode("overwrite").saveAsTable(f"{CATALOG}.silver.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_cust_info
# MAGIC Drop rows with no `cst_id`, keep only the latest snapshot per customer,
# MAGIC trim names, and map the short codes to readable values.

# COMMAND ----------

df = read_bronze("crm_cust_info")

df["cst_create_date"] = pd.to_datetime(df["cst_create_date"], errors="coerce")

crm_cust_info_silver = (
    df[df["cst_id"].notna() & (df["cst_id"] != "")]
    .sort_values("cst_create_date", ascending=False)
    .drop_duplicates(subset="cst_id", keep="first")
    .copy()
)

crm_cust_info_silver["cst_firstname"] = crm_cust_info_silver["cst_firstname"].str.strip()
crm_cust_info_silver["cst_lastname"] = crm_cust_info_silver["cst_lastname"].str.strip()

crm_cust_info_silver["cst_marital_status"] = (
    crm_cust_info_silver["cst_marital_status"].str.strip().str.upper()
    .map({"S": "Single", "M": "Married"})
    .fillna("n/a")
)

crm_cust_info_silver["cst_gndr"] = (
    crm_cust_info_silver["cst_gndr"].str.strip().str.upper()
    .map({"M": "Male", "F": "Female"})
    .fillna("n/a")
)

write_silver(crm_cust_info_silver, "crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_prd_info
# MAGIC Split the compound `prd_key` into a category id and a shortened product
# MAGIC key, fill missing costs, map product-line codes, and recompute
# MAGIC `prd_end_dt` from each product's version history instead of trusting it.

# COMMAND ----------

df = read_bronze("crm_prd_info")

crm_prd_info_silver = df.copy()
crm_prd_info_silver["cat_id"] = crm_prd_info_silver["prd_key"].str[:5].str.replace("-", "_", regex=False)
crm_prd_info_silver["prd_key"] = crm_prd_info_silver["prd_key"].str[6:]

crm_prd_info_silver["prd_cost"] = pd.to_numeric(crm_prd_info_silver["prd_cost"], errors="coerce").fillna(0)

crm_prd_info_silver["prd_line"] = (
    crm_prd_info_silver["prd_line"].str.strip().str.upper()
    .map({"R": "Road", "M": "Mountain", "S": "Other Sales", "T": "Touring"})
    .fillna("n/a")
)

crm_prd_info_silver["prd_start_dt"] = pd.to_datetime(crm_prd_info_silver["prd_start_dt"], errors="coerce")

# recompute end date as (next version's start date - 1 day), per product key
crm_prd_info_silver = crm_prd_info_silver.sort_values(["prd_key", "prd_start_dt"])
crm_prd_info_silver["prd_end_dt"] = (
    crm_prd_info_silver.groupby("prd_key")["prd_start_dt"].shift(-1) - pd.Timedelta(days=1)
)

write_silver(crm_prd_info_silver, "crm_prd_info")

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_sales_details
# MAGIC Convert the integer-encoded dates, recompute `sls_sales` wherever it's
# MAGIC inconsistent with quantity × price, and back-fill `sls_price` from
# MAGIC sales ÷ quantity when it's missing or non-positive.

# COMMAND ----------

df = read_bronze("crm_sales_details")

def parse_yyyymmdd(series):
    s = series.astype(str)
    s = s.where((s != "0") & (s.str.len() == 8), other=np.nan)
    return pd.to_datetime(s, format="%Y%m%d", errors="coerce")

crm_sales_details_silver = df.copy()
crm_sales_details_silver["sls_order_dt"] = parse_yyyymmdd(crm_sales_details_silver["sls_order_dt"])
crm_sales_details_silver["sls_ship_dt"] = parse_yyyymmdd(crm_sales_details_silver["sls_ship_dt"])
crm_sales_details_silver["sls_due_dt"] = parse_yyyymmdd(crm_sales_details_silver["sls_due_dt"])

crm_sales_details_silver["sls_quantity"] = pd.to_numeric(crm_sales_details_silver["sls_quantity"], errors="coerce")
crm_sales_details_silver["sls_price"] = pd.to_numeric(crm_sales_details_silver["sls_price"], errors="coerce")
crm_sales_details_silver["sls_sales"] = pd.to_numeric(crm_sales_details_silver["sls_sales"], errors="coerce")

needs_price = crm_sales_details_silver["sls_price"].isna() | (crm_sales_details_silver["sls_price"] <= 0)
safe_qty = crm_sales_details_silver["sls_quantity"].replace(0, np.nan)
crm_sales_details_silver.loc[needs_price, "sls_price"] = (
    crm_sales_details_silver["sls_sales"] / safe_qty
)[needs_price]

expected_sales = crm_sales_details_silver["sls_quantity"] * crm_sales_details_silver["sls_price"].abs()
needs_sales = (
    crm_sales_details_silver["sls_sales"].isna()
    | (crm_sales_details_silver["sls_sales"] < 0)
    | (crm_sales_details_silver["sls_sales"] != expected_sales)
)
crm_sales_details_silver.loc[needs_sales, "sls_sales"] = expected_sales[needs_sales]

write_silver(crm_sales_details_silver, "crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_cust_az12
# MAGIC Strip the `NAS` prefix so `cid` lines up with CRM's `cst_key`, null out
# MAGIC birthdates in the future, and standardize gender.

# COMMAND ----------

df = read_bronze("erp_cust_az12")

erp_cust_az12_silver = df.copy()
mask_nas = erp_cust_az12_silver["cid"].str.startswith("NAS")
erp_cust_az12_silver.loc[mask_nas, "cid"] = erp_cust_az12_silver.loc[mask_nas, "cid"].str[3:]

erp_cust_az12_silver["bdate"] = pd.to_datetime(erp_cust_az12_silver["bdate"], errors="coerce")
future_bdate = erp_cust_az12_silver["bdate"] > pd.Timestamp(date.today())
erp_cust_az12_silver.loc[future_bdate, "bdate"] = pd.NaT

erp_cust_az12_silver["gen"] = (
    erp_cust_az12_silver["gen"].str.strip().str.upper()
    .map({"M": "Male", "MALE": "Male", "F": "Female", "FEMALE": "Female"})
    .fillna("n/a")
)

write_silver(erp_cust_az12_silver, "erp_cust_az12")

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_loc_a101
# MAGIC Remove hyphens from `cid` so it joins to the CRM key, and collapse the
# MAGIC country field down to one consistent value per country.

# COMMAND ----------

df = read_bronze("erp_loc_a101")

erp_loc_a101_silver = df.copy()
erp_loc_a101_silver["cid"] = erp_loc_a101_silver["cid"].str.replace("-", "", regex=False)
erp_loc_a101_silver["cntry"] = erp_loc_a101_silver["cntry"].str.strip()

erp_loc_a101_silver["cntry"] = np.select(
    [
        erp_loc_a101_silver["cntry"].str.upper().isin(["US", "USA"]),
        erp_loc_a101_silver["cntry"].str.upper() == "DE",
        (erp_loc_a101_silver["cntry"] == "") | erp_loc_a101_silver["cntry"].isna(),
    ],
    ["United States", "Germany", "n/a"],
    default=erp_loc_a101_silver["cntry"],
)

write_silver(erp_loc_a101_silver, "erp_loc_a101")

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_px_cat_g1v2
# MAGIC Profiling found no structural issues here — trim and null-safety checks
# MAGIC only, no corrective logic needed.

# COMMAND ----------

df = read_bronze("erp_px_cat_g1v2")

erp_px_cat_g1v2_silver = df.copy()
for col in ("cat", "subcat", "maintenance"):
    erp_px_cat_g1v2_silver[col] = erp_px_cat_g1v2_silver[col].str.strip()

write_silver(erp_px_cat_g1v2_silver, "erp_px_cat_g1v2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation — re-run the profiling checks against Silver

# COMMAND ----------

silver_cust = spark.table(f"{CATALOG}.silver.crm_cust_info").toPandas()
dupe_customers = silver_cust["cst_id"].duplicated().sum()
print("duplicate cst_id in silver:", dupe_customers)

silver_sales = spark.table(f"{CATALOG}.silver.crm_sales_details").toPandas()
bad_sales = (silver_sales["sls_sales"] != silver_sales["sls_quantity"] * silver_sales["sls_price"]).sum()
print("rows where sls_sales != quantity * price:", bad_sales)

print("cst_gndr distinct values:", silver_cust["cst_gndr"].unique().tolist())
