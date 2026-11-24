# Databricks notebook source
# FMCG catalog and schema initialization.
# Run this once before executing the dimension and fact pipelines.

catalog = "fmcg"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# Create catalog if it does not already exist.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

# Create the Medallion schemas.
for schema_name in (bronze_schema, silver_schema, gold_schema):
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}"
    )

# Display the schemas created in the catalog.
spark.sql(f"SHOW SCHEMAS IN {catalog}").show(truncate=False)

# Reusable variables for downstream notebooks:
# bronze_schema = "bronze"
# silver_schema = "silver"
# gold_schema = "gold"
