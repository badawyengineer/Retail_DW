# Databricks notebook source
# MAGIC %md
# MAGIC ### dlt_pipeline
# MAGIC A **Delta Live Tables** (DLT) implementation of the same Bronze →
# MAGIC Silver → Gold logic as `/notebooks`, written declaratively instead of
# MAGIC as manually-run notebooks.
# MAGIC
# MAGIC **This is an alternative implementation, not a replacement.** The
# MAGIC `/notebooks` path is plain PySpark you run yourself, step by step —
# MAGIC good for learning and for a portfolio review where someone wants to
# MAGIC read the logic top to bottom. This file is what you'd actually run in
# MAGIC production: DLT manages the run order, retries failed writes, and —
# MAGIC relevant to the observability ask — gives you a built-in event log and
# MAGIC data-quality dashboard for free, without needing metrics_logger.py at
# MAGIC all for this path. Pick one or the other; running both against the
# MAGIC same tables isn't useful.
# MAGIC
# MAGIC To run: create a DLT Pipeline in the Databricks UI, point it at this
# MAGIC file as the source, target catalog/schema `retail_dwh`/`silver`
# MAGIC (DLT manages Bronze internally as a streaming table unless configured
# MAGIC otherwise — see the note on `dlt.read` below).

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "retail_dwh"
RAW_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze — same "no logic" rule as the notebook version

# COMMAND ----------

def make_bronze_table(name, filename):
    @dlt.table(
        name=f"bronze_{name}",
        comment=f"Raw, untransformed load of {filename}.",
    )
    def _bronze():
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(f"{RAW_PATH}/{filename}")
        )
    return _bronze

make_bronze_table("crm_cust_info", "cust_info.csv")
make_bronze_table("crm_prd_info", "prd_info.csv")
make_bronze_table("crm_sales_details", "sales_details.csv")
make_bronze_table("erp_cust_az12", "CUST_AZ12.csv")
make_bronze_table("erp_loc_a101", "LOC_A101.csv")
make_bronze_table("erp_px_cat_g1v2", "PX_CAT_G1V2.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver — same cleaning rules, now with DLT `expect` constraints
# MAGIC
# MAGIC This is the part that's genuinely better than the plain-notebook
# MAGIC version for observability: `expect_or_drop` doesn't just clean data,
# MAGIC it publishes a pass/fail metric for that specific rule to DLT's event
# MAGIC log automatically — visible in the pipeline UI without writing any
# MAGIC logging code.

# COMMAND ----------

@dlt.table(name="silver_crm_cust_info")
@dlt.expect_or_drop("valid_cst_id", "cst_id IS NOT NULL")
def silver_crm_cust_info():
    src = dlt.read("bronze_crm_cust_info")
    w = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
    return (
        src.withColumn("rn", F.row_number().over(w))
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
        .withColumn("dwh_create_date", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(name="silver_crm_prd_info")
@dlt.expect("reasonable_cost", "prd_cost >= 0")
def silver_crm_prd_info():
    src = dlt.read("bronze_crm_prd_info")
    w = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    return (
        src.withColumn("cat_id", F.regexp_replace(F.substring("prd_key", 1, 5), "-", "_"))
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
        .withColumn("prd_end_dt", F.date_sub(F.lead("prd_start_dt").over(w), 1))
        .withColumn("dwh_create_date", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(name="silver_crm_sales_details")
@dlt.expect_or_drop("positive_quantity", "sls_quantity > 0")
@dlt.expect("sales_reconciled", "sls_sales = sls_quantity * sls_price")
def silver_crm_sales_details():
    src = dlt.read("bronze_crm_sales_details")

    def parse_yyyymmdd(col):
        c = F.col(col).cast("string")
        return F.when((c == "0") | (F.length(c) != 8), None).otherwise(F.to_date(c, "yyyyMMdd"))

    df = (
        src.withColumn("sls_order_dt", parse_yyyymmdd("sls_order_dt"))
        .withColumn("sls_ship_dt", parse_yyyymmdd("sls_ship_dt"))
        .withColumn("sls_due_dt", parse_yyyymmdd("sls_due_dt"))
        .withColumn("sls_quantity", F.col("sls_quantity").cast("int"))
        .withColumn("sls_price", F.col("sls_price").cast("decimal(10,2)"))
        .withColumn("sls_sales", F.col("sls_sales").cast("decimal(10,2)"))
    )
    df = df.withColumn(
        "sls_price",
        F.when(
            F.col("sls_price").isNull() | (F.col("sls_price") <= 0),
            F.col("sls_sales") / F.when(F.col("sls_quantity") == 0, None).otherwise(F.col("sls_quantity"))
        ).otherwise(F.col("sls_price"))
    )
    df = df.withColumn(
        "sls_sales",
        F.when(
            F.col("sls_sales").isNull()
            | (F.col("sls_sales") < 0)
            | (F.col("sls_sales") != F.col("sls_quantity") * F.abs(F.col("sls_price"))),
            F.col("sls_quantity") * F.abs(F.col("sls_price"))
        ).otherwise(F.col("sls_sales"))
    )
    return df.withColumn("dwh_create_date", F.current_timestamp())

# COMMAND ----------

@dlt.table(name="silver_erp_cust_az12")
def silver_erp_cust_az12():
    src = dlt.read("bronze_erp_cust_az12")
    return (
        src.withColumn(
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
        .withColumn("dwh_create_date", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(name="silver_erp_loc_a101")
def silver_erp_loc_a101():
    src = dlt.read("bronze_erp_loc_a101")
    return (
        src.withColumn("cid", F.regexp_replace("cid", "-", ""))
        .withColumn("cntry", F.trim("cntry"))
        .withColumn(
            "cntry",
            F.when(F.upper(F.col("cntry")).isin("US", "USA"), "United States")
             .when(F.upper(F.col("cntry")) == "DE", "Germany")
             .when((F.col("cntry") == "") | F.col("cntry").isNull(), "n/a")
             .otherwise(F.col("cntry"))
        )
        .withColumn("dwh_create_date", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(name="silver_erp_px_cat_g1v2")
def silver_erp_px_cat_g1v2():
    src = dlt.read("bronze_erp_px_cat_g1v2")
    return (
        src.withColumn("cat", F.trim("cat"))
        .withColumn("subcat", F.trim("subcat"))
        .withColumn("maintenance", F.trim("maintenance"))
        .withColumn("dwh_create_date", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold — star schema, same shape as `03_gold_views`

# COMMAND ----------

@dlt.table(name="gold_dim_customers")
def gold_dim_customers():
    ci = dlt.read("silver_crm_cust_info")
    ca = dlt.read("silver_erp_cust_az12")
    la = dlt.read("silver_erp_loc_a101")
    joined = (
        ci.join(ca, ci.cst_key == ca.cid, "left")
        .join(la, ci.cst_key == la.cid, "left")
    )
    return joined.select(
        F.row_number().over(Window.orderBy("cst_id")).alias("customer_key"),
        ci.cst_id.alias("customer_id"),
        ci.cst_key.alias("customer_number"),
        ci.cst_firstname.alias("first_name"),
        ci.cst_lastname.alias("last_name"),
        la.cntry.alias("country"),
        ci.cst_marital_status.alias("marital_status"),
        F.when(ci.cst_gndr != "n/a", ci.cst_gndr).otherwise(F.coalesce(ca.gen, F.lit("n/a"))).alias("gender"),
        ca.bdate.alias("birthdate"),
        ci.cst_create_date.alias("create_date"),
    )

# COMMAND ----------

@dlt.table(name="gold_dim_products")
def gold_dim_products():
    pi = dlt.read("silver_crm_prd_info").filter("prd_end_dt IS NULL")
    pc = dlt.read("silver_erp_px_cat_g1v2")
    joined = pi.join(pc, pi.cat_id == pc.id, "left")
    return joined.select(
        F.row_number().over(Window.orderBy("prd_start_dt", "prd_key")).alias("product_key"),
        pi.prd_id.alias("product_id"),
        pi.prd_key.alias("product_number"),
        pi.prd_nm.alias("product_name"),
        pi.cat_id.alias("category_id"),
        pc.cat.alias("category"),
        pc.subcat.alias("subcategory"),
        pc.maintenance,
        pi.prd_cost.alias("cost"),
        pi.prd_line.alias("product_line"),
        pi.prd_start_dt.alias("start_date"),
    )

# COMMAND ----------

@dlt.table(name="gold_fact_sales")
@dlt.expect("has_customer_key", "customer_key IS NOT NULL")
@dlt.expect("has_product_key", "product_key IS NOT NULL")
def gold_fact_sales():
    sd = dlt.read("silver_crm_sales_details")
    dp = dlt.read("gold_dim_products")
    dc = dlt.read("gold_dim_customers")
    joined = (
        sd.join(dp, sd.sls_prd_key == dp.product_number, "left")
        .join(dc, sd.sls_cust_id == dc.customer_id, "left")
    )
    return joined.select(
        sd.sls_ord_num.alias("order_number"),
        dp.product_key,
        dc.customer_key,
        sd.sls_order_dt.alias("order_date"),
        sd.sls_ship_dt.alias("ship_date"),
        sd.sls_due_dt.alias("due_date"),
        sd.sls_sales.alias("sales_amount"),
        sd.sls_quantity.alias("quantity"),
        sd.sls_price.alias("price"),
    )
