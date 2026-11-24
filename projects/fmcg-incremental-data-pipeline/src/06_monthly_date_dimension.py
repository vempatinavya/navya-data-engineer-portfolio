# Databricks notebook source
# Generate a monthly-grain date dimension.

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("start_date", "2024-01-01", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2025-12-01", "End date (YYYY-MM-DD)")

catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

df = spark.sql(f"""
    SELECT explode(
        sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 month
        )
    ) AS month_start_date
""")

df = (
    df
    .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
    .withColumn("year", F.year("month_start_date"))
    .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
    .withColumn(
        "year_quarter",
        F.concat(F.col("year").cast("string"), F.lit("-Q"), F.quarter("month_start_date"))
    )
)

(df.write.format("delta")
 .mode("overwrite")
 .saveAsTable(f"{catalog}.gold.dim_date"))

display(df)
