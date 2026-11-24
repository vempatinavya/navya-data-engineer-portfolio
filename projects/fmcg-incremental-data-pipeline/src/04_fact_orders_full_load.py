# Databricks notebook source
# Initial/full load for order facts. Use for bootstrap or controlled backfill.

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")
dbutils.widgets.text("s3_bucket", "amzn-childdataset", "S3 bucket")
dbutils.widgets.text("landing_folder", "landing", "Landing folder")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
s3_bucket = dbutils.widgets.get("s3_bucket")
landing_folder = dbutils.widgets.get("landing_folder")
bronze_schema, silver_schema, gold_schema = "bronze", "silver", "gold"

landing_path = f"s3://{s3_bucket}/{landing_folder}/"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# Read all CSV files for the full-load batch.
df = (
    spark.read.options(inferSchema=True, header=True)
    .csv(f"{landing_path}*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_path", "_metadata.file_size")
)

# Standardize customer identifier and date.
df = df.withColumn(
    "customer_id",
    F.when(F.col("customer_id").cast("string").rlike(r"^[0-9]+$"),
           F.col("customer_id").cast("string"))
     .otherwise(F.lit("999999"))
)
df = df.withColumn(
    "order_placement_date",
    F.regexp_replace("order_placement_date", r"^[A-Za-z]+,\s*", "")
)
df = df.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy-MM-dd"),
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy")
    )
)
df = df.dropDuplicates([
    "order_id", "order_placement_date", "customer_id", "product_id", "order_qty"
]).withColumn("product_id", F.col("product_id").cast("string"))

# Enrich with product surrogate/business key.
products = spark.table(f"{catalog}.{silver_schema}.products").select(
    "product_id", "product_code"
)
df_silver = df.join(products, on="product_id", how="inner")

if not spark.catalog.tableExists(silver_table):
    (df_silver.write.format("delta")
     .option("mergeSchema", "true")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite")
     .saveAsTable(silver_table))
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    (silver_delta.alias("target")
     .merge(
         df_silver.alias("source"),
         """target.order_placement_date = source.order_placement_date
         AND target.order_id = source.order_id
         AND target.product_code = source.product_code
         AND target.customer_id = source.customer_id"""
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

# GOLD child fact
df_gold = spark.table(silver_table).select(
    "order_id",
    F.col("order_placement_date").alias("date"),
    F.col("customer_id").alias("customer_code"),
    "product_code", "product_id",
    F.col("order_qty").alias("sold_quantity")
)
if not spark.catalog.tableExists(gold_table):
    (df_gold.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .option("mergeSchema", "true")
     .mode("overwrite")
     .saveAsTable(gold_table))
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    (gold_delta.alias("target")
     .merge(
         df_gold.alias("source"),
         """target.date = source.date
         AND target.order_id = source.order_id
         AND target.product_code = source.product_code
         AND target.customer_code = source.customer_code"""
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

# Aggregate child daily facts to the parent monthly grain.
child = spark.table(gold_table).select(
    "date", "product_code", "customer_code", "sold_quantity"
)
monthly = (
    child.withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)

parent = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
(parent.alias("target")
 .merge(
     monthly.alias("source"),
     "target.date = source.date AND target.product_code = source.product_code AND target.customer_code = source.customer_code"
 )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
