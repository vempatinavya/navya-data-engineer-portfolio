# Databricks notebook source
# Bronze layer: ingest source CSV datasets into Delta tables.
# Create the Unity Catalog Volume and upload authorized sample data first.

from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType
)
from pyspark.sql import functions as F

CATALOG_NAME = "ecommerce"
RAW_ROOT = "/Volumes/ecommerce/source_data/raw_data"


def read_csv_with_metadata(path, schema=None):
    reader = spark.read.option("header", "true").option("delimiter", ",")
    if schema is not None:
        reader = reader.schema(schema)
    return (
        reader.csv(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze(df, table_name):
    (df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG_NAME}.bronze.{table_name}"))


spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.gold")

# Brands
brands_schema = StructType([
    StructField("brand", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True),
])
brands_df = read_csv_with_metadata(f"{RAW_ROOT}/brands", brands_schema)
write_bronze(brands_df, "brands")
display(brands_df)

# Categories
category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True),
])
category_df = read_csv_with_metadata(f"{RAW_ROOT}/category/", category_schema)
write_bronze(category_df, "category")
display(category_df)

# Customers: phone is STRING to preserve leading zeros.
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), False),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
])
customers_df = read_csv_with_metadata(f"{RAW_ROOT}/customers/", customers_schema)
write_bronze(customers_df, "customers")
display(customers_df)

# Calendar / date dimension
date_schema = StructType([
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True),
])
calendar_df = read_csv_with_metadata(f"{RAW_ROOT}/date/", date_schema)
write_bronze(calendar_df, "date")
display(calendar_df.limit(5))

# Products
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm", StringType(), True),
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
])
products_df = read_csv_with_metadata(f"{RAW_ROOT}/products/", products_schema)
write_bronze(products_df, "products")
display(products_df.limit(5))

# Order items
order_items_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("item_seq", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price_currency", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_pct", StringType(), True),
    StructField("tax_amount", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("coupon_code", StringType(), True),
])
order_items_df = read_csv_with_metadata(
    f"{RAW_ROOT}/order_items/landing/", order_items_schema
)
write_bronze(order_items_df, "order_items")
display(order_items_df.limit(5))
