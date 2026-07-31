# Databricks notebook source
# MAGIC %md
# MAGIC ### 02_silver_transformation
# MAGIC Reads every Bronze table untouched, applies the cleansing/standardization
# MAGIC rules that came out of the profiling pass (project doc, section 4.4),
# MAGIC and writes each result as a Silver Delta table. Every table gains a
# MAGIC `dwh_create_date` column marking when the row was loaded into Silver.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "retail_dwh"

def read_bronze(table):
    return spark.table(f"{CATALOG}.bronze.{table}")

def write_silver(df, table):
    (df.withColumn("dwh_create_date", F.current_timestamp())
       .write.mode("overwrite")
       .saveAsTable(f"{CATALOG}.silver.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_cust_info
# MAGIC Drop rows with no `cst_id`, keep only the latest snapshot per customer,
# MAGIC trim names, and map the short codes to readable values.

# COMMAND ----------

src = read_bronze("crm_cust_info")

w = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())

crm_cust_info_silver = (
    src
    .filter(F.col("cst_id").isNotNull())
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("cst_firstname", F.trim("cst_firstname"))
    .withColumn("cst_lastname", F.trim("cst_lastname"))
    .withColumn(
        "cst_marital_status",
        F.when(F.upper(F.trim("cst_marital_status")) == "S", "Single")
         .when(F.upper(F.trim("cst_marital_status")) == "M", "Married")
         .otherwise("n/a")
    )
    .withColumn(
        "cst_gndr",
        F.when(F.upper(F.trim("cst_gndr")) == "M", "Male")
         .when(F.upper(F.trim("cst_gndr")) == "F", "Female")
         .otherwise("n/a")
    )
)

write_silver(crm_cust_info_silver, "crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_prd_info
# MAGIC Split the compound `prd_key` into a category id and a shortened product
# MAGIC key, fill missing costs, map product-line codes, and recompute
# MAGIC `prd_end_dt` from each product's version history instead of trusting it.

# COMMAND ----------

src = read_bronze("crm_prd_info")

w = Window.partitionBy("prd_key").orderBy("prd_start_dt")

crm_prd_info_silver = (
    src
    .withColumn("cat_id", F.regexp_replace(F.substring("prd_key", 1, 5), "-", "_"))
    .withColumn("prd_key", F.expr("substring(prd_key, 7, length(prd_key))"))
    .withColumn("prd_cost", F.coalesce(F.col("prd_cost").cast("decimal(10,2)"), F.lit(0)))
    .withColumn(
        "prd_line",
        F.when(F.upper(F.trim("prd_line")) == "R", "Road")
         .when(F.upper(F.trim("prd_line")) == "M", "Mountain")
         .when(F.upper(F.trim("prd_line")) == "S", "Other Sales")
         .when(F.upper(F.trim("prd_line")) == "T", "Touring")
         .otherwise("n/a")
    )
    .withColumn("prd_start_dt", F.col("prd_start_dt").cast("date"))
    .withColumn(
        "prd_end_dt",
        F.date_sub(F.lead("prd_start_dt").over(w), 1)
    )
)

write_silver(crm_prd_info_silver, "crm_prd_info")

# COMMAND ----------

# MAGIC %md
# MAGIC #### crm_sales_details
# MAGIC Convert the integer-encoded dates, recompute `sls_sales` wherever it's
# MAGIC inconsistent with quantity × price, and back-fill `sls_price` from
# MAGIC sales ÷ quantity when it's missing or non-positive.

# COMMAND ----------

src = read_bronze("crm_sales_details")

def parse_yyyymmdd(col):
    c = F.col(col).cast("string")
    return F.when(
        (c == "0") | (F.length(c) != 8), None
    ).otherwise(F.to_date(c, "yyyyMMdd"))

crm_sales_details_silver = (
    src
    .withColumn("sls_order_dt", parse_yyyymmdd("sls_order_dt"))
    .withColumn("sls_ship_dt", parse_yyyymmdd("sls_ship_dt"))
    .withColumn("sls_due_dt", parse_yyyymmdd("sls_due_dt"))
    .withColumn("sls_quantity", F.col("sls_quantity").cast("int"))
    .withColumn("sls_price", F.col("sls_price").cast("decimal(10,2)"))
    .withColumn("sls_sales", F.col("sls_sales").cast("decimal(10,2)"))
    .withColumn(
        "sls_price",
        F.when(
            F.col("sls_price").isNull() | (F.col("sls_price") <= 0),
            F.col("sls_sales") / F.when(F.col("sls_quantity") == 0, None).otherwise(F.col("sls_quantity"))
        ).otherwise(F.col("sls_price"))
    )
    .withColumn(
        "sls_sales",
        F.when(
            F.col("sls_sales").isNull()
            | (F.col("sls_sales") < 0)
            | (F.col("sls_sales") != F.col("sls_quantity") * F.abs(F.col("sls_price"))),
            F.col("sls_quantity") * F.abs(F.col("sls_price"))
        ).otherwise(F.col("sls_sales"))
    )
)

write_silver(crm_sales_details_silver, "crm_sales_details")

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_cust_az12
# MAGIC Strip the `NAS` prefix so `cid` lines up with CRM's `cst_key`, null out
# MAGIC birthdates in the future, and standardize gender.

# COMMAND ----------

src = read_bronze("erp_cust_az12")

erp_cust_az12_silver = (
    src
    .withColumn(
        "cid",
        F.when(F.col("cid").startswith("NAS"), F.expr("substring(cid, 4, length(cid))"))
         .otherwise(F.col("cid"))
    )
    .withColumn("bdate", F.col("bdate").cast("date"))
    .withColumn("bdate", F.when(F.col("bdate") > F.current_date(), None).otherwise(F.col("bdate")))
    .withColumn(
        "gen",
        F.when(F.upper(F.trim("gen")).isin("M", "MALE"), "Male")
         .when(F.upper(F.trim("gen")).isin("F", "FEMALE"), "Female")
         .otherwise("n/a")
    )
)

write_silver(erp_cust_az12_silver, "erp_cust_az12")

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_loc_a101
# MAGIC Remove hyphens from `cid` so it joins to the CRM key, and collapse the
# MAGIC country field down to one consistent value per country.

# COMMAND ----------

src = read_bronze("erp_loc_a101")

erp_loc_a101_silver = (
    src
    .withColumn("cid", F.regexp_replace("cid", "-", ""))
    .withColumn("cntry", F.trim("cntry"))
    .withColumn(
        "cntry",
        F.when(F.upper(F.col("cntry")).isin("US", "USA"), "United States")
         .when(F.upper(F.col("cntry")) == "DE", "Germany")
         .when((F.col("cntry") == "") | F.col("cntry").isNull(), "n/a")
         .otherwise(F.col("cntry"))
    )
)

write_silver(erp_loc_a101_silver, "erp_loc_a101")

# COMMAND ----------

# MAGIC %md
# MAGIC #### erp_px_cat_g1v2
# MAGIC Profiling found no structural issues here — trim and null-safety checks
# MAGIC only, no corrective logic needed.

# COMMAND ----------

src = read_bronze("erp_px_cat_g1v2")

erp_px_cat_g1v2_silver = (
    src
    .withColumn("cat", F.trim("cat"))
    .withColumn("subcat", F.trim("subcat"))
    .withColumn("maintenance", F.trim("maintenance"))
)

write_silver(erp_px_cat_g1v2_silver, "erp_px_cat_g1v2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation — re-run the profiling checks against Silver

# COMMAND ----------

dupe_customers = (spark.table(f"{CATALOG}.silver.crm_cust_info")
                   .groupBy("cst_id").count().filter("count > 1").count())
print("duplicate cst_id in silver:", dupe_customers)

bad_sales = (spark.table(f"{CATALOG}.silver.crm_sales_details")
             .filter("sls_sales != sls_quantity * sls_price").count())
print("rows where sls_sales != quantity * price:", bad_sales)

genders = [r.cst_gndr for r in
           spark.table(f"{CATALOG}.silver.crm_cust_info").select("cst_gndr").distinct().collect()]
print("cst_gndr distinct values:", genders)
