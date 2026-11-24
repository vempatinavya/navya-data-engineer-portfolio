# Databricks notebook source
# Customer dimension pipeline: S3 -> Bronze -> Silver -> Gold -> parent MERGE.

from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")
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
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .select("*", "_metadata.file_name", "_metadata.file_size")
)
(df.write.format("delta")
 .option("delta.enableChangeDataFeed", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}"))

# SILVER
df = spark.table(f"{catalog}.{bronze_schema}.{data_source}")
df = df.withColumn("customer_name", F.trim("customer_name"))

name_fixes = {
    "PowerSnack hub": "PowerSnack Hub",
    "MacroBite superfoods": "MacroBite Superfoods",
    "SprintX nutrition": "SprintX Nutrition",
    "ZenAthlete foods": "ZenAthlete Foods",
    "Peak Performance store": "Peak Performance Store",
    "Peak performance Store": "Peak Performance Store"
}
df = df.replace(name_fixes, subset=["customer_name"])
df = df.dropDuplicates(["customer_id"])

city_fixes = {
    "Hyderabadd": "Hyderabad",
    " Hyderbad": "Hyderabad",
    "NewDheli": "New Delhi",
    "NewDelhi": "New Delhi",
    "NewDelhee": "New Delhi",
    "Bengaluruu": "Bengaluru",
    "Bengalore": "Bengaluru"
}
valid_cities = ["Hyderabad", "New Delhi", "Bengaluru"]
df = df.replace(city_fixes, subset=["city"])
df = df.withColumn(
    "city",
    F.when(F.col("city").isNull(), None)
     .when(F.col("city").isin(valid_cities), F.col("city"))
     .otherwise(None)
)

# Explicit correction map from the supplied notebook.
customer_city_fixes = {
    789521: "Hyderabad",
    789421: "Hyderabad",
    789603: "Hyderabad",
    789403: "New Delhi",
    789420: "Bengaluru"
}
fix_df = spark.createDataFrame(
    [(str(k), v) for k, v in customer_city_fixes.items()],
    ["customer_id", "fixed_city"]
)
original_cols = df.columns
df = (
    df.join(fix_df, on="customer_id", how="left")
      .withColumn("city", F.coalesce(F.col("city"), F.col("fixed_city")))
      .select(*original_cols)
)

df = (
    df
    .withColumn(
        "customer",
        F.concat_ws("-", F.col("customer_name"), F.coalesce(F.col("city"), F.lit("unknown")))
    )
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

(df.write.format("delta")
 .option("mergeSchema", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}"))

# GOLD child dimension
df_gold = spark.table(f"{catalog}.{silver_schema}.{data_source}").select(
    "customer_id", "customer_name", "city", "customer", "market", "platform", "channel"
)
(df_gold.write.format("delta")
 .option("mergeSchema", "true")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}"))

# MERGE into parent dimension
parent = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_customers")
child = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}").select(
    F.col("customer_id").alias("customer_code"),
    "customer", "market", "platform", "channel"
)
(parent.alias("target")
 .merge(child.alias("source"), "target.customer_code = source.customer_code")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
