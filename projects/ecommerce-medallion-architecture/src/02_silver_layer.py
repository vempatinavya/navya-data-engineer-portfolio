# Databricks notebook source
# Silver layer: clean and standardize Bronze Delta tables.

from pyspark.sql import functions as F

CATALOG_NAME = "ecommerce"


def bronze_table(name):
    return spark.table(f"{CATALOG_NAME}.bronze.{name}")


def write_silver(df, name):
    (df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG_NAME}.silver.{name}"))


# Brands
brands_df = bronze_table("brands")
brands_df = (
    brands_df
    .withColumn("brand_name", F.trim(F.col("brand_name")))
    .withColumn("brand", F.regexp_replace(F.col("brand"), r"[^A-Za-z0-9]", ""))
    .replace(
        {"GROCERY": "GRCY", "TOYS": "TOY", "BOOKS": "BKS"},
        subset=["category_code"]
    )
)
write_silver(brands_df, "brands")
display(brands_df)

# Categories: inspect duplicate combinations, then retain one row per code.
category_df = bronze_table("category")
display(category_df.groupBy("category_code", "category_name").count())
category_df = category_df.dropDuplicates(["category_code"])
write_silver(category_df, "category")
display(category_df)

# Customers
customers_df = bronze_table("customers").dropna(subset=["customer_id"])
customers_df = customers_df.withColumn("phone", F.col("phone").cast("string"))
customers_df = customers_df.fillna("Not Available", subset=["phone"])
write_silver(customers_df, "customers")
display(customers_df)

# Dates
date_df = bronze_table("date")
date_df = (
    date_df
    .withColumn("date", F.to_date(F.col("date"), "dd-MM-yyyy"))
    .withColumn("week", F.abs(F.col("week_of_year")))
    .withColumn("day_name", F.initcap(F.col("day_name")))
    .drop("week_of_year")
)
display(date_df.groupBy("date").count().filter(F.col("count") > 1))
date_df = date_df.dropDuplicates(["date"])
date_df = date_df.withColumn(
    "quarter",
    F.concat(
        F.lit("Q"), F.col("quarter").cast("string"),
        F.lit("-"), F.col("year").cast("string")
    )
)
write_silver(date_df, "date")
display(date_df)

# Products
products_df = bronze_table("products")
products_df = (
    products_df
    .withColumn("category_code", F.upper(F.col("category_code")))
    .withColumn("brand_code", F.upper(F.col("brand_code")))
    .withColumn("length_cm", F.regexp_replace(F.col("length_cm"), ",", ".").cast("double"))
    .withColumn("width_cm", F.round(F.col("width_cm"), 2))
    .withColumn("height_cm", F.round(F.col("height_cm"), 2))
    .withColumn("rating_count", F.abs(F.col("rating_count")))
    .withColumn("weight_grams", F.regexp_replace(F.col("weight_grams"), "g", "").cast("double"))
)
write_silver(products_df, "products")
display(products_df)

# Order items: normalize fields, parse timestamps and cast measures.
order_items_df = bronze_table("order_items")
order_items_df = order_items_df.replace({"Two": "2"}, subset=["quantity"])
order_items_df = (
    order_items_df
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("discount_pct", F.regexp_replace(F.col("discount_pct"), "%", "").cast("double"))
    .withColumn("coupon_code", F.trim(F.col("coupon_code")))
    .withColumn("unit_price", F.regexp_replace(F.col("unit_price"), r"\$", "").cast("double"))
    .withColumn(
        "order_ts",
        F.coalesce(
            F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")
        )
    )
    .withColumn("item_seq", F.col("item_seq").cast("int"))
    .withColumn(
        "tax_amount",
        F.regexp_replace(F.col("tax_amount"), r"[^0-9.\-]", "").cast("double")
    )
    .withColumn(
        "channel",
        F.when(F.lower(F.col("channel")) == "web", "Website")
         .when(F.lower(F.col("channel")) == "app", "Mobile")
         .otherwise(F.col("channel"))
    )
    .withColumn("dt", F.to_date("dt", "yyyy-MM-dd"))
    .withColumn("processed_time", F.current_timestamp())
)
write_silver(order_items_df, "order_items")
display(order_items_df)
