# Databricks notebook source
# Gross price pipeline: parse and validate prices, enrich with product code,
# select latest non-zero annual price and merge into the parent dimension.

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")
dbutils.widgets.text("s3_bucket", "amzn-childdataset", "S3 bucket")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
s3_bucket = dbutils.widgets.get("s3_bucket")
bronze_schema, silver_schema, gold_schema = "bronze", "silver", "gold"
base_path = f"s3://{s3_bucket}/{data_source}.csv"

# BRONZE
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)
(df.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}"))

# SILVER: normalize month formats and price values.
df = spark.table(f"{catalog}.{bronze_schema}.{data_source}")
df = df.withColumn(
    "month",
    F.coalesce(
        F.try_to_date("month", "yyyy/MM/dd"),
        F.try_to_date("month", "dd/MM/yyyy"),
        F.try_to_date("month", "yyyy-MM-dd"),
        F.try_to_date("month", "dd-MM-yyyy")
    )
)
# Keep only numeric values; normalize negative prices to positive values.
df = df.withColumn(
    "gross_price",
    F.when(
        F.col("gross_price").cast("string").rlike(r"^-?\d+(\.\d+)?$"),
        F.abs(F.col("gross_price").cast("double"))
    ).otherwise(F.lit(0.0))
)

products = spark.table(f"{catalog}.{silver_schema}.products").select(
    "product_id", "product_code"
)
df_silver = (
    df.join(products, on="product_id", how="inner")
      .select(
          "product_id", "product_code", "month", "gross_price",
          "read_timestamp", "file_name", "file_size"
      )
)
(df_silver.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .option("mergeSchema", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}"))

# GOLD child price table
df_gold = spark.table(f"{catalog}.{silver_schema}.{data_source}").select(
    "product_code", "month", "gross_price"
)
(df_gold.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}"))

# Choose the most recent non-zero price for each product and year.
price_df = (
    spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}")
    .withColumn("year", F.year("month"))
    .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)
window_spec = (
    Window.partitionBy("product_code", "year")
    .orderBy(F.col("is_zero").asc(), F.col("month").desc())
)
latest_price = (
    price_df.withColumn("rnk", F.row_number().over(window_spec))
    .filter(F.col("rnk") == 1)
    .select(
        "product_code",
        F.col("gross_price").alias("price_inr"),
        F.col("year").cast("string").alias("year")
    )
)

# MERGE key includes year because this output contains annual prices.
parent = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_gross_price")
(parent.alias("target")
 .merge(
     latest_price.alias("source"),
     "target.product_code = source.product_code AND target.year = source.year"
 )
 .whenMatchedUpdate(set={
     "price_inr": "source.price_inr"
 })
 .whenNotMatchedInsert(values={
     "product_code": "source.product_code",
     "price_inr": "source.price_inr",
     "year": "source.year"
 })
 .execute())
