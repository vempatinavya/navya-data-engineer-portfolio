# Databricks notebook source
# Amazon S3 data lake exploration and Delta table creation.
# Configure secure S3 access through an approved IAM role / instance profile.
# Never hard-code AWS access keys or secrets in this notebook.

display(dbutils.fs.ls("s3://amzn-companystocks-practice/"))

stocks_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3://amzn-companystocks-practice/company_stocks.csv")
)
display(stocks_df)

# Separate company-stocks Delta/time-travel demonstration.
# Adjust the source path and target location to an authorized test bucket.
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.company_stocks
USING DELTA
LOCATION 's3://amzn-companystocks-practice/company_stocks/bronze/'
AS
SELECT
    *,
    current_timestamp() AS ingest_time,
    _metadata.file_path AS source_file,
    uuid() AS bronze_id
FROM read_files(
    's3://amzn-companystocks-practice/',
    format => 'csv',
    header => true,
    inferSchema => true
)
""")

display(spark.sql("""
SELECT * FROM workspace.default.company_stocks LIMIT 1
"""))

# Demo only: uncomment to update a test row and inspect Delta time travel.
# spark.sql("""
# UPDATE workspace.default.company_stocks
# SET Stock_Price = 70
# WHERE Company = 'Apple'
# """)
# display(spark.sql("""
# SELECT * FROM workspace.default.company_stocks
# WHERE Company = 'Apple'
# """))
# display(spark.sql("""
# SELECT * FROM workspace.default.company_stocks VERSION AS OF 0
# WHERE Company = 'Apple'
# """))
