# Databricks notebook source
# MAGIC %md
# MAGIC ### streaming_extension / kafka_sales_ingestion
# MAGIC
# MAGIC **Read this note before the code.** The actual source data for this
# MAGIC project is six static CSV exports — there's no live feed, so Kafka
# MAGIC isn't required by the use case as it stands. This notebook exists as
# MAGIC an *optional, extended* architecture: what Bronze ingestion for
# MAGIC `sales_details` would look like if the CRM started emitting sales
# MAGIC events onto a Kafka topic in real time instead of a nightly CSV
# MAGIC export. It's kept separate from `/notebooks` on purpose — it doesn't
# MAGIC replace `01_bronze_ingestion`, it's a demonstration of the pattern.
# MAGIC
# MAGIC If you don't have a Kafka cluster to point this at, that's fine —
# MAGIC nothing else in the pipeline depends on this notebook running.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from observability.metrics_logger import log_event

CATALOG = "retail_dwh"

# Fill these in for your actual Kafka cluster — left as placeholders since
# no broker is available in this environment to verify connectivity against.
KAFKA_BOOTSTRAP_SERVERS = "<your-kafka-bootstrap-servers>:9092"
KAFKA_TOPIC = "crm.sales_details"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema
# MAGIC Same shape as `sales_details.csv`, since this is meant to be a
# MAGIC drop-in real-time alternative to the batch file — same columns land
# MAGIC in the same Bronze table either way.

# COMMAND ----------

sales_event_schema = StructType([
    StructField("sls_ord_num", StringType()),
    StructField("sls_prd_key", StringType()),
    StructField("sls_cust_id", StringType()),
    StructField("sls_order_dt", StringType()),
    StructField("sls_ship_dt", StringType()),
    StructField("sls_due_dt", StringType()),
    StructField("sls_sales", DoubleType()),
    StructField("sls_quantity", IntegerType()),
    StructField("sls_price", DoubleType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the stream
# MAGIC Structured Streaming, not a one-off batch read — this is the actual
# MAGIC difference from `01_bronze_ingestion`: this runs continuously.

# COMMAND ----------

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_stream = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), sales_event_schema).alias("data"))
    .select("data.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Bronze
# MAGIC Appends into the same `bronze.crm_sales_details` table `01_bronze_ingestion`
# MAGIC writes with `overwrite` — in a real deployment you'd pick one ingestion
# MAGIC path or the other for this table, not run both against it at once.
# MAGIC A checkpoint location makes this restart-safe: if the stream stops and
# MAGIC restarts, it resumes from the last processed offset instead of
# MAGIC reprocessing or dropping events.

# COMMAND ----------

query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/bronze/checkpoints/sales_details_kafka")
    .trigger(processingTime="1 minute")
    .toTable(f"{CATALOG}.bronze.crm_sales_details")
)

log_event("kafka_stream_started", topic=KAFKA_TOPIC, table="crm_sales_details")

# query.awaitTermination()  # uncomment when running this as a standing job, not interactively
