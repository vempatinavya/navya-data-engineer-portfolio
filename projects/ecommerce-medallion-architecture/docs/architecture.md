# Architecture notes

## Data flow

1. Source CSV files are read from Amazon S3 and Unity Catalog Volumes.
2. Bronze notebooks preserve source attributes and add ingestion metadata.
3. Silver notebooks apply data cleansing, normalization, type conversion and deduplication.
4. Gold notebook derives order-level financial measures and writes `ecommerce.gold.fact_orders`.
5. A Databricks multi-task Job runs Bronze, Silver and Gold sequentially.

## Important distinction

The company-stocks S3 example creates a separate table in `workspace.default`. The E-commerce sample uses Unity Catalog Volumes as its raw-data paths. Keep these examples separate unless you intentionally consolidate the storage design.
