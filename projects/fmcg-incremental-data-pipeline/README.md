# FMCG Incremental Data Pipeline

## Overview
A multi-source FMCG data integration project built with PySpark, Delta Lake, Amazon S3 and Databricks Workflows. It ingests child-business datasets, applies Bronze/Silver/Gold transformations, and merges curated dimensions and facts into parent-company Delta tables.

> Portfolio-safe reconstruction from notebook code supplied by the author. Workspace-specific utilities, user paths and identifiers have been removed. The scripts have not been executed in the author's Databricks environment.

## Architecture
```text
Amazon S3 CSV datasets
  |-- customers.csv
  |-- products.csv
  |-- gross_price.csv
  |-- orders / landing/*.csv
          |
          v
       Bronze
  raw Delta + ingestion metadata
          |
          v
       Silver
 cleaning, standardization, validation,
 product/customer key enrichment
          |
          v
        Gold
 child dimensions + child fact table
          |
          v
 Delta MERGE into parent-company
 dimensions and monthly fact table
```

## Project files
- `src/01_products_pipeline.py` — product cleansing, product code generation, child dimension and parent dimension MERGE.
- `src/02_customers_pipeline.py` — customer standardization, city corrections, derived attributes and parent dimension MERGE.
- `src/03_gross_price_pipeline.py` — month parsing, price validation, product enrichment, annual latest-price selection and parent dimension MERGE.
- `src/04_fact_orders_full_load.py` — initial/full fact load, deduplication, product enrichment and monthly parent aggregation.
- `src/05_fact_orders_incremental.py` — incremental landing-file ingestion, staging, Delta MERGE and recalculation of impacted parent months.
- `src/06_monthly_date_dimension.py` — monthly date dimension generation.
- `resources/fmcg_workflow.yml` — sample Databricks multi-task workflow.

## Technology
PySpark, Python, Delta Lake, Delta MERGE, Change Data Feed, Amazon S3, Databricks Widgets, Databricks Workflows, Spark SQL, Window functions.

## Data layers
**Bronze:** reads CSV data from S3, adds ingestion timestamp and source-file metadata, and stores Delta tables with Change Data Feed enabled.

**Silver:** cleans and standardizes product, customer, price and order attributes; removes duplicates; parses dates; enriches facts with product codes.

**Gold:** creates child dimension/fact tables and merges them into parent-company tables. The fact pipeline aggregates daily child transactions to monthly grain before updating the parent fact table.

## Configuration before execution
1. Configure S3 access through an approved IAM role / instance profile. Never store keys in source code.
2. Create the `fmcg` catalog and the Bronze, Silver and Gold schemas.
3. Create or load the parent tables referenced in the scripts: `dim_products`, `dim_customers`, `dim_gross_price`, and `fact_orders`.
4. Set the S3 landing and processed paths in the widgets or script configuration.
5. Confirm the actual CSV schemas, column names and date formats.
6. Ensure each source has a deterministic deduplication rule before using `dropDuplicates`.
7. Validate that parent-table MERGE keys match the intended grain.
8. Deploy the workflow YAML only after confirming the notebook paths match the deployed project layout.

## Important review notes
- The original gross-price MERGE matched only on product code while also carrying a year attribute. This reconstruction uses product code + year as the natural key; confirm that this matches the actual parent dimension grain.
- The original fact pipeline uses a monthly parent grain. The incremental process re-aggregates all child fact rows for each month touched by the incoming batch before merging.
- File movement from landing to processed is operationally sensitive. The sample incremental script includes an explicit switch and moves files only after staging succeeds; add stronger retry/idempotency controls before production use.
- Unknown customer/product IDs are mapped to fallback values in the original approach. Validate referential-integrity and reporting implications before adopting this rule.
- Price conversion and negative-price handling are business rules that require data-owner approval.
- Change Data Feed is enabled on selected Delta tables; this project does not consume CDF with `table_changes`.
- No throughput, performance or business-impact metrics are claimed.

## Security
Only use synthetic or approved sample data. Do not publish company data, credentials, internal workspace URLs, personal email addresses or production storage paths.
