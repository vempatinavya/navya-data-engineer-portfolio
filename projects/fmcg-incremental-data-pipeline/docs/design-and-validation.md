# Design and validation notes

## Pipeline behavior
- Product, customer and gross-price dimensions use full refreshes for the child Silver/Gold staging outputs, then MERGE into parent dimensions.
- Order fact processing has a separate full-load/bootstrap script and an incremental script.
- Incremental fact processing isolates the incoming batch in staging, merges it into the child Silver and Gold tables, then recalculates monthly totals for months affected by that batch.
- Monthly date dimension uses one row per month start.

## Items to validate
1. **Deduplication:** `dropDuplicates` does not choose the latest row deterministically. Use a Window ordered by source update timestamp or ingestion timestamp if duplicate keys can have different values.
2. **Product code:** SHA-256 of product name means identical product names share a code, and name changes create a new code. Confirm the desired business key.
3. **Customer corrections:** the supplied customer-to-city mapping is sample-specific. Verify identifiers and city rules before using.
4. **Fallback IDs:** `999999` can combine unrelated invalid IDs. Prefer an explicit unknown dimension member and retain the original invalid value for auditing.
5. **Price rule:** negative gross prices are converted to absolute values and invalid values become zero in the sample. Confirm this is the approved business rule.
6. **Annual price grain:** the MERGE uses product_code + year because the child result contains annual rows. Confirm the parent table has the same composite key.
7. **Fact grain:** verify uniqueness of order_id + date + product_code + customer_code, and verify that parent fact grain is month_start + product_code + customer_code.
8. **Incremental file handling:** moving landing files after a run can create gaps or duplicates if a run partially fails. Production design should use Auto Loader/checkpoints or a manifest-driven idempotent ingestion process.
9. **Late arriving updates:** monthly recalculation is designed to include all child rows for each impacted month. Confirm the parent MERGE updates the exact monthly grain.
10. **Date parsing:** validate all source date patterns and reject or quarantine rows that cannot be parsed.
11. **S3 permissions:** use IAM roles / instance profiles and least-privilege access.
12. **Workflow:** verify task dependencies and paths in the Databricks Asset Bundle deployment.

## Privacy
All workspace paths and user identifiers from the original notebooks have been removed. Use only approved synthetic/sample data in this public repository.
