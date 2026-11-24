# Databricks notebook source
# Product dimension pipeline: S3 -> Bronze -> Silver -> Gold -> parent MERGE.

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")
dbutils.widgets.text("s3_bucket", "amzn-childdataset", "S3 bucket")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
s3_bucket = dbutils.widgets.get("s3_bucket")

bronze_schema, silver_schema, gold_schema = "bronze", "silver", "gold"
base_path = f"s3://{s3_bucket}/{data_source}.csv"

# BRONZE
df_bronze = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(base_path)
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

(df_bronze.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}"))

# SILVER: deduplicate product IDs and standardize text.
df_silver = spark.table(f"{catalog}.{bronze_schema}.{data_source}")
df_silver = df_silver.dropDuplicates(["product_id"])

df_silver = (
    df_silver
    .withColumn(
        "category",
        F.when(F.col("category").isNull(), None)
         .otherwise(F.initcap(F.col("category")))
    )
    .withColumn("category", F.regexp_replace("category", "Protien", "Protein"))
    .withColumn("product_name", F.regexp_replace("product_name", "Protien", "Protein"))
    .withColumn(
        "division",
        F.when(F.col("category").isin("Energy Bars", "Protein Bars"), "Nutrition Bars")
         .when(F.col("category") == "Granola & Cereals", "Breakfast Foods")
         .when(F.col("category") == "Recovery Dairy", "Dairy & Recovery")
         .when(F.col("category") == "Healthy Snacks", "Healthy Snacks")
         .when(F.col("category") == "Electrolyte Mix", "Hydration & Electrolytes")
         .otherwise("Other")
    )
    .withColumn("variant", F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1))
    .withColumn("product_code", F.sha2(F.col("product_name").cast("string"), 256))
    .withColumn(
        "product_id",
        F.when(F.col("product_id").cast("string").rlike(r"^[0-9]+$"),
               F.col("product_id").cast("string"))
         .otherwise(F.lit("999999"))
    )
    .withColumnRenamed("product_name", "product")
    .select(
        "product_code", "division", "category", "product", "variant",
        "product_id", "read_timestamp", "file_name", "file_size"
    )
)

(df_silver.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .option("mergeSchema", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}"))

# GOLD child dimension
df_gold = df_silver.select(
    "product_code", "product_id", "division", "category", "product", "variant"
)
(df_gold.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}"))

# MERGE into parent dimension
parent = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_products")
child = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

(parent.alias("target")
 .merge(child.alias("source"), "target.product_code = source.product_code")
 .whenMatchedUpdate(set={
     "division": "source.division",
     "category": "source.category",
     "product": "source.product",
     "variant": "source.variant"
 })
 .whenNotMatchedInsert(values={
     "product_code": "source.product_code",
     "division": "source.division",
     "category": "source.category",
     "product": "source.product",
     "variant": "source.variant"
 })
 .execute())
