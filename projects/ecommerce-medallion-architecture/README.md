# E-commerce Medallion Architecture with Amazon S3 and Azure Databricks

## Overview

A portfolio implementation of an E-commerce Lakehouse using Amazon S3, Azure Databricks, PySpark, Delta Lake and Unity Catalog. The project organizes data into Bronze, Silver and Gold layers and creates an analytics-ready order fact table.

> This repository is a sanitized portfolio adaptation of notebook code supplied by the author. It has not been executed or validated against the original Databricks workspace. Review paths, permissions, schemas and runtime compatibility before running.

## Architecture

```text
CSV source data
      |
      v
Amazon S3 / Unity Catalog Volumes
      |
      v
Bronze Delta tables
(raw ingestion + source metadata)
      |
      v
Silver Delta tables
(cleaning + standardization + deduplication)
      |
      v
Gold Delta table
(ecommerce.gold.fact_orders)
      |
      v
Downstream analytics
```

## Technology
- Amazon S3
- Azure Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Jobs / Workflows
- YAML

## Source datasets
Brands, category, customers, date, products and order_items.

## Processing details

### Bronze
- Reads CSV datasets using Spark with explicit schemas.
- Adds source-file path and ingestion timestamp metadata.
- Writes Delta tables under `ecommerce.bronze`.

### Silver
- Trims and standardizes brand and category values.
- Removes duplicate category and date records.
- Filters customers with missing customer IDs.
- Cleans product dimensions and order item fields.
- Parses dates and timestamps and casts measures.
- Writes cleansed Delta tables under `ecommerce.silver`.

### Gold
Creates `ecommerce.gold.fact_orders` with gross amount, discount amount, net amount including tax, date key, coupon indicator and INR-converted net amount using demonstration FX rates.

## Workflow orchestration
The YAML job defines three sequential tasks:
1. `Dimension_Bronze`
2. `Dimension_Silver` (depends on Bronze)
3. `Dimension_Gold` (depends on Silver)

The job enables queueing and uses the `PERFORMANCE_OPTIMIZED` performance target.

## Before running
1. Configure S3 access securely using an approved IAM role or instance profile. Do not put credentials in code.
2. Create the Unity Catalog catalog, schemas and source-data Volume.
3. Upload authorized sample CSV files to the expected Volume paths, or update `RAW_ROOT`.
4. Confirm that your Databricks Runtime supports the APIs used in the notebooks.
5. Review the S3 `read_files` example and ensure its source path does not include unrelated files.
6. Replace demonstration FX rates with a dated, approved source if used beyond a demo.
7. Deploy the job only after confirming the YAML notebook paths match the deployed notebook locations.

## Limitations and validation notes
- The example uses overwrite writes for simplicity; it is not an incremental ingestion implementation.
- The S3 company-stocks example is a separate Delta/time-travel demonstration and is not the source for the E-commerce catalog tables.
- Validate duplicate-resolution rules before using `dropDuplicates` in production.
- Validate malformed records and failed numeric casts before publishing Gold data.
- No performance metrics are claimed because no benchmark results were provided.
- Do not upload confidential company data, private workspace URLs, account identifiers or credentials.

## Learning outcomes
- Building a multi-layer Lakehouse
- Reading CSV data from object storage
- Creating Delta tables
- Applying PySpark data-quality transformations
- Creating a Gold fact table
- Orchestrating notebook tasks with Databricks Jobs
