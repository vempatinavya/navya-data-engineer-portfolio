# Databricks notebook source
# Incremental order ingestion:
# landing files -> Bronze append -> staging -> Silver MERGE -> Gold MERGE
# -> recompute impacted parent months.

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")
dbutils.widgets.text("s3_bucket", "amzn-childdataset", "S3 bucket")
dbutils.widgets.text("landing_folder", "landing", "Landing folder")
dbutils.widgets.text("processed_folder", "processed", "Processed folder")
dbutils.widgets.text("move_files_after_success", "false", "Move files after success")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
s3_bucket = dbutils.widgets.get("s3_bucket")
landing_folder = dbutils.widgets.get("landing_folder")
processed_folder = dbutils.widgets.get("processed_folder")
move_files_after_success = dbutils.widgets.get("move_files_after_success").lower() == "true"

bronze_schema, silver_schema, gold_schema = "bronze", "silver", "gold"
landing_path = f"s3://{s3_bucket}/{landing_folder}/"
processed_path = f"s3://{s3_bucket}/{processed_folder}/"

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"
bronze_staging = f"{catalog}.{bronze_schema}.staging_{data_source}"
silver_staging = f"{catalog}.{silver_schema}.staging_{data_source}"

# BRONZE: append the newly arrived CSV batch.
df = (
    spark.read.options(header=True, inferSchema=True)
    .csv(f"{landing_path}*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)
if df.limit(1).count() == 0:
    raise ValueError(f"No input rows found under {landing_path}")

(df.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("append")
 .saveAsTable(bronze_table))

# Isolate this run's batch in a staging table.
(df.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(bronze_staging))

# SILVER transformations
orders = spark.table(bronze_staging).filter(F.col("order_qty").isNotNull())
orders = orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").cast("string").rlike(r"^[0-9]+$"),
           F.col("customer_id").cast("string"))
     .otherwise(F.lit("999999"))
)
orders = orders.withColumn(
    "order_placement_date",
    F.regexp_replace("order_placement_date", r"^[A-Za-z]+,\s*", "")
)
orders = orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
        F.try_to_date("order_placement_date", "yyyy-MM-dd")
    )
)
orders = orders.dropDuplicates([
    "order_id", "order_placement_date", "customer_id", "product_id", "order_qty"
]).withColumn("product_id", F.col("product_id").cast("string"))

products = spark.table(f"{catalog}.{silver_schema}.products").select(
    "product_id", "product_code"
)
joined = orders.join(products, on="product_id", how="inner").select(
    orders["*"], products["product_code"]
)

if not spark.catalog.tableExists(silver_table):
    (joined.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .option("mergeSchema", "true")
     .mode("overwrite")
     .saveAsTable(silver_table))
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    source = (
        joined
        .withColumnRenamed("read_timestamp", "current_timestamp")
        .withColumnRenamed("file_name", "file_path")
    )
    (silver_delta.alias("target")
     .merge(
         source.alias("source"),
         """target.order_placement_date = source.order_placement_date
         AND target.order_id = source.order_id
         AND target.product_code = source.product_code
         AND target.customer_id = source.customer_id"""
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

# Store this run's transformed rows separately for downstream calculations.
(joined.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(silver_staging))

# GOLD child fact: only the current batch is merged.
incremental_gold = spark.table(silver_staging).select(
    "order_id",
    F.col("order_placement_date").alias("date"),
    F.col("customer_id").alias("customer_code"),
    "product_code", "product_id",
    F.col("order_qty").alias("sold_quantity")
)
if not spark.catalog.tableExists(gold_table):
    (incremental_gold.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .option("mergeSchema", "true")
     .mode("overwrite")
     .saveAsTable(gold_table))
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    (gold_delta.alias("target")
     .merge(
         incremental_gold.alias("source"),
         """target.date = source.date
         AND target.order_id = source.order_id
         AND target.product_code = source.product_code
         AND target.customer_code = source.customer_code"""
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

# Recompute complete monthly totals for only the months touched by this batch.
impacted_months = (
    incremental_gold
    .select(F.trunc("date", "MM").alias("month_start"))
    .distinct()
)
all_child_rows = spark.table(gold_table).select(
    "date", "product_code", "customer_code", "sold_quantity"
)
monthly_recalculation = (
    all_child_rows.join(
        impacted_months,
        F.trunc(all_child_rows["date"], "MM") == impacted_months["month_start"],
        "inner"
    )
    .groupBy(
        impacted_months["month_start"].alias("date"),
        "product_code", "customer_code"
    )
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
)

parent = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
(parent.alias("target")
 .merge(
     monthly_recalculation.alias("source"),
     "target.date = source.date AND target.product_code = source.product_code AND target.customer_code = source.customer_code"
 )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

# File movement is opt-in. For production, add a manifest / idempotency strategy,
# handle partial failures and verify the destination before moving source files.
if move_files_after_success:
    for file_info in dbutils.fs.ls(landing_path):
        if not file_info.isDir():
            dbutils.fs.mv(
                file_info.path,
                f"{processed_path.rstrip('/')}/{file_info.name}",
                True
            )

# Staging cleanup can be scheduled after successful validation / audit.
# spark.sql(f"DROP TABLE IF EXISTS {bronze_staging}")
# spark.sql(f"DROP TABLE IF EXISTS {silver_staging}")
