## Goal

Set up a Medallion Architecture pipeline using Azure Databricks, Azure Storage, and dbt for Airbnb datasets.

### Data Source

- Azure Storage Account: airbnbdbtlearnstorage
- Container: src
- Files:
  - listings.csv
  - reviews.csv
  - seed_full_moon_dates.csv

### Architecture

- Bronze Layer: Raw data ingested from CSVs in Azure Storage into Databricks managed tables
- Silver Layer: Cleaned and enriched data using dbt models
- Gold Layer: Final curated data products for analytics and BI

### Steps

1. Mount Azure Storage container to Databricks workspace
2. Create Unity Catalog and schema for bronze, silver, gold layers
3. Ingest CSV files into Bronze tables using PySpark
4. Initialize dbt project in Databricks
5. Define dbt sources pointing to Bronze tables
6. Create dbt models to transform Bronze → Silver → Gold
7. Run and schedule dbt models using Databricks workflows
