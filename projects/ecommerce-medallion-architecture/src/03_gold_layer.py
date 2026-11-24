# Databricks notebook source
# Gold layer: create an analytics-ready order fact table.

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

CATALOG_NAME = "ecommerce"
df = spark.table(f"{CATALOG_NAME}.silver.order_items")

# Cast calculation columns to numeric types.
df = (
    df
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("discount_pct", F.col("discount_pct").cast("double"))
    .withColumn("tax_amount", F.col("tax_amount").cast("double"))
)

df = df.withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
df = df.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / F.lit(100.0)))
)
df = df.withColumn(
    "sale_amount",
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)
df = df.withColumn(
    "date_id",
    F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType())
)
df = df.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1)).otherwise(F.lit(0))
)

# Demonstration reference rates from the original example.
# Replace with an approved, dated FX reference table for production use.
fx_rates = {
    "INR": 1.00, "AED": 24.18, "AUD": 57.55, "CAD": 62.93,
    "GBP": 117.98, "SGD": 68.18, "USD": 88.29,
}
rates_df = spark.createDataFrame(
    [(currency, float(rate)) for currency, rate in fx_rates.items()],
    ["currency", "inr_rate"]
)

df = (
    df.join(
        rates_df,
        F.upper(F.trim(F.col("unit_price_currency"))) == rates_df.currency,
        "left"
    )
    .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount") * F.col("inr_rate")))
)

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr"),
)

(orders_gold_df.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.gold.fact_orders"))

display(orders_gold_df)
