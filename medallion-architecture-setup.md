# Medallion Architecture Pipeline Setup Guide

## Goal

Set up a Medallion Architecture pipeline using Azure Databricks, Azure Storage, and dbt for Airbnb datasets.

### Data Source

- Azure Data Lake Storage Gen2 Account: airbnbstorage
- Container: src
- Files:
  - listings.csv
  - reviews.csv
  - seed_full_moon_dates.csv (dbt seed file - to be placed in dbt project's `seeds/` directory)

### Architecture

- Bronze Layer: Raw data ingested from CSVs in Azure Data Lake Storage Gen2 into Databricks managed tables
- Silver Layer: Cleaned and enriched data using dbt models
- Gold Layer: Final curated data products for analytics and BI

**Note**: The `seed_full_moon_dates.csv` file is managed as a dbt seed and should be placed in the dbt project's `seeds/` directory. It will be loaded as a managed table using the `dbt seed` command rather than being ingested from external storage.

---

## 1. Mount Azure Data Lake Storage Gen2 to Databricks

Create a notebook named `01_mount_storage.py` in Databricks:

```python
# Mount Azure Data Lake Storage Gen2 container to Databricks

# Configure storage account details
storage_account_name = "airbnbstorage"
container_name = "src"
mount_point = "/mnt/airbnb-data"

# Mount the container using Service Principal authentication
client_id = dbutils.secrets.get(scope="airbnb-scope", key="client-id")
client_secret = dbutils.secrets.get(scope="airbnb-scope", key="client-secret")
tenant_id = dbutils.secrets.get(scope="airbnb-scope", key="tenant-id")

dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
    mount_point=mount_point,
    extra_configs={
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
)

# Verify mount
display(dbutils.fs.ls(mount_point))
```

---

## 2. Unity Catalog Setup

Create a notebook named `02_unity_catalog_setup.sql` in Databricks:

```sql
-- Create Unity Catalog and schemas
CREATE CATALOG IF NOT EXISTS airbnb_medallion;
USE CATALOG airbnb_medallion;

-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS airbnb_bronze;
CREATE SCHEMA IF NOT EXISTS airbnb_silver;
CREATE SCHEMA IF NOT EXISTS airbnb_gold;

-- Show created schemas
SHOW SCHEMAS;
```

---

## 3. Bronze Layer Data Ingestion

Create a notebook named `03_bronze_ingestion.py` in Databricks:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# Set catalog and schema
spark.sql("USE CATALOG airbnb_medallion")
spark.sql("USE SCHEMA airbnb_bronze")

# Define schemas for each dataset
listings_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("host_id", StringType(), True),
    StructField("host_name", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("room_type", StringType(), True),
    StructField("price", StringType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("last_review", StringType(), True),
    StructField("reviews_per_month", DoubleType(), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True)
])

reviews_schema = StructType([
    StructField("listing_id", StringType(), True),
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("reviewer_id", StringType(), True),
    StructField("reviewer_name", StringType(), True),
    StructField("comments", StringType(), True)
])

# Ingest listings data
listings_df = (spark.read
               .option("header", "true")
               .schema(listings_schema)
               .csv("/mnt/airbnb-data/listings.csv")
               .withColumn("ingested_at", current_timestamp())
               .withColumn("source_file", input_file_name()))

listings_df.write.mode("overwrite").saveAsTable("airbnb_bronze.listings")

# Ingest reviews data
reviews_df = (spark.read
              .option("header", "true")
              .schema(reviews_schema)
              .csv("/mnt/airbnb-data/reviews.csv")
              .withColumn("ingested_at", current_timestamp())
              .withColumn("source_file", input_file_name()))

reviews_df.write.mode("overwrite").saveAsTable("airbnb_bronze.reviews")

# Note: seed_full_moon_dates.csv is handled as a dbt seed file
# It should be placed in the dbt project's seeds/ directory and loaded using 'dbt seed'
# Do not ingest it here as it will be managed by dbt

print("Bronze layer ingestion completed successfully!")
print("Remember to run 'dbt seed' to load the full moon dates seed data")
```

---

## 4. dbt Project Structure

### dbt_project.yml

```yaml
name: "airbnb_medallion"
version: "1.0.0"
config-version: 2

profile: "airbnb_medallion"

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  airbnb_medallion:
    +materialized: table
    bronze:
      +materialized: view
    silver:
      +materialized: view
    gold:
      +materialized: table

seeds:
  airbnb_medallion:
    +materialized: table
    +schema: bronze
    seed_full_moon_dates:
      +column_types:
        date: date
```

### dbt Seed File Setup

Create a `seeds/` directory in your dbt project and place the `seed_full_moon_dates.csv` file there:

```
dbt_project/
├── seeds/
│   └── seed_full_moon_dates.csv
├── models/
├── tests/
└── dbt_project.yml
```

The seed file will be loaded into the `bronze` schema as `seed_full_moon_dates` table when you run `dbt seed`.

### profiles.yml

```yaml
airbnb_medallion:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: airbnb_medallion
      schema: silver
      host: <your-databricks-workspace-url>
      http_path: <your-cluster-http-path>
      token: <your-access-token>
      threads: 4
```

---

## 5. dbt Sources Configuration

### models/sources.yml

```yaml
version: 2

sources:
  - name: bronze
    description: "Bronze layer raw data from Azure Data Lake Storage Gen2"
    database: airbnb_medallion
    schema: airbnb_bronze
    tables:
      - name: listings
        description: "Raw Airbnb listings data"
        columns:
          - name: id
            description: "Unique listing identifier"
            tests:
              - unique
              - not_null
          - name: host_id
            description: "Host identifier"
          - name: price
            description: "Price per night as string"
          - name: last_review
            description: "Date of last review"

      - name: reviews
        description: "Raw Airbnb reviews data"
        columns:
          - name: listing_id
            description: "Reference to listing"
          - name: id
            description: "Unique review identifier"
            tests:
              - unique
              - not_null
          - name: date
            description: "Review date"

      - name: full_moon_dates
        description: "Full moon dates for analysis (dbt seed)"
        columns:
          - name: date
            description: "Full moon date"
            tests:
              - unique
              - not_null
```

---

## 6. Silver Layer Models

### models/silver/stg_listings.sql

```sql
{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('bronze', 'listings') }}
),

cleaned_data as (
    select
        cast(id as bigint) as listing_id,
        name as listing_name,
        cast(host_id as bigint) as host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude,
        room_type,
        cast(regexp_replace(price, '[$,]', '') as decimal(10,2)) as price_per_night,
        minimum_nights,
        number_of_reviews,
        case
            when last_review = '' then null
            else cast(last_review as date)
        end as last_review_date,
        reviews_per_month,
        calculated_host_listings_count,
        availability_365,
        ingested_at
    from source_data
    where id is not null
)

select * from cleaned_data
```

### models/silver/stg_reviews.sql

```sql
{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('bronze', 'reviews') }}
),

cleaned_data as (
    select
        cast(listing_id as bigint) as listing_id,
        cast(id as bigint) as review_id,
        cast(date as date) as review_date,
        cast(reviewer_id as bigint) as reviewer_id,
        reviewer_name,
        comments as review_comments,
        ingested_at
    from source_data
    where listing_id is not null
    and id is not null
    and date is not null
)

select * from cleaned_data
```

### models/silver/stg_full_moon_dates.sql

```sql
{{ config(materialized='table') }}

with source_data as (
    select * from {{ ref('seed_full_moon_dates') }}
),

cleaned_data as (
    select
        cast(date as date) as full_moon_date
    from source_data
    where date is not null
)

select * from cleaned_data
```

---

## 7. Gold Layer Models

### models/gold/dim_listings.sql

```sql
{{ config(materialized='table') }}

with listings as (
    select * from {{ ref('stg_listings') }}
),

listing_metrics as (
    select
        listing_id,
        listing_name,
        host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude,
        room_type,
        price_per_night,
        minimum_nights,
        number_of_reviews,
        last_review_date,
        reviews_per_month,
        calculated_host_listings_count,
        availability_365,
        case
            when price_per_night < 50 then 'Budget'
            when price_per_night between 50 and 150 then 'Mid-range'
            when price_per_night between 150 and 300 then 'Premium'
            else 'Luxury'
        end as price_category,
        case
            when number_of_reviews = 0 then 'No Reviews'
            when number_of_reviews between 1 and 10 then 'Few Reviews'
            when number_of_reviews between 11 and 50 then 'Some Reviews'
            else 'Many Reviews'
        end as review_category
    from listings
)

select * from listing_metrics
```

### models/gold/fact_reviews.sql

```sql
{{ config(materialized='table') }}

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

review_facts as (
    select
        r.review_id,
        r.listing_id,
        r.review_date,
        r.reviewer_id,
        r.reviewer_name,
        r.review_comments,
        l.neighbourhood,
        l.room_type,
        l.price_per_night,
        extract(year from r.review_date) as review_year,
        extract(month from r.review_date) as review_month,
        extract(dayofweek from r.review_date) as review_day_of_week
    from reviews r
    left join listings l on r.listing_id = l.listing_id
)

select * from review_facts
```

### models/gold/agg_neighbourhood_metrics.sql

```sql
{{ config(materialized='table') }}

with listings as (
    select * from {{ ref('dim_listings') }}
),

reviews as (
    select * from {{ ref('fact_reviews') }}
),

neighbourhood_stats as (
    select
        l.neighbourhood,
        l.neighbourhood_group,
        count(distinct l.listing_id) as total_listings,
        avg(l.price_per_night) as avg_price,
        min(l.price_per_night) as min_price,
        max(l.price_per_night) as max_price,
        avg(l.number_of_reviews) as avg_reviews_per_listing,
        sum(l.number_of_reviews) as total_reviews,
        count(distinct l.host_id) as unique_hosts,
        avg(l.availability_365) as avg_availability
    from listings l
    group by l.neighbourhood, l.neighbourhood_group
)

select * from neighbourhood_stats
```

---

## 8. Databricks Workflow Setup

### Notebook: 04_run_dbt_pipeline.py

```python
# Databricks notebook to run dbt pipeline

# Install dbt-databricks if not already installed
%pip install dbt-databricks

# Change to dbt project directory
import os
os.chdir("/Workspace/Repos/<your-repo>/dbt-azuredatabricks-training")

# Run dbt commands
%sh dbt deps
%sh dbt debug
%sh dbt seed
%sh dbt run
%sh dbt test

print("dbt pipeline completed successfully!")
```

### Workflow JSON Configuration

```json
{
  "name": "Airbnb Medallion Pipeline",
  "tasks": [
    {
      "task_key": "mount_storage",
      "notebook_task": {
        "notebook_path": "/Repos/<your-repo>/notebooks/01_mount_storage"
      },
      "job_cluster_key": "default_cluster"
    },
    {
      "task_key": "setup_catalog",
      "depends_on": [{ "task_key": "mount_storage" }],
      "notebook_task": {
        "notebook_path": "/Repos/<your-repo>/notebooks/02_unity_catalog_setup"
      },
      "job_cluster_key": "default_cluster"
    },
    {
      "task_key": "bronze_ingestion",
      "depends_on": [{ "task_key": "setup_catalog" }],
      "notebook_task": {
        "notebook_path": "/Repos/<your-repo>/notebooks/03_bronze_ingestion"
      },
      "job_cluster_key": "default_cluster"
    },
    {
      "task_key": "dbt_pipeline",
      "depends_on": [{ "task_key": "bronze_ingestion" }],
      "notebook_task": {
        "notebook_path": "/Repos/<your-repo>/notebooks/04_run_dbt_pipeline"
      },
      "job_cluster_key": "default_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "default_cluster",
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.preview.enabled": "true"
        }
      }
    }
  ]
}
```

---

## 9. Data Quality Tests

### tests/assert_positive_prices.sql

```sql
select *
from {{ ref('dim_listings') }}
where price_per_night <= 0
```

### tests/assert_valid_coordinates.sql

```sql
select *
from {{ ref('dim_listings') }}
where latitude < -90 or latitude > 90
   or longitude < -180 or longitude > 180
```

---

## 10. Implementation Steps

### Prerequisites

1. Set up Azure Databricks workspace
2. Create Unity Catalog enabled cluster
3. Configure Databricks secrets for Data Lake Storage Gen2 Service Principal access
4. Set up Git integration in Databricks

### Step-by-step Implementation

1. **Create Databricks Secrets**

   ```bash
   databricks secrets create-scope --scope airbnb-scope
   databricks secrets put --scope airbnb-scope --key client-id
   databricks secrets put --scope airbnb-scope --key client-secret
   databricks secrets put --scope airbnb-scope --key tenant-id
   ```

2. **Clone Repository**

   - Set up Git integration in Databricks
   - Clone this repository to your Databricks workspace

3. **Run Setup Notebooks**

   - Execute `01_mount_storage.py`
   - Execute `02_unity_catalog_setup.sql`
   - Execute `03_bronze_ingestion.py`

4. **Initialize dbt Project**

   - Create dbt project structure
   - Place `seed_full_moon_dates.csv` in the `seeds/` directory
   - Configure `profiles.yml` with your Databricks connection details
   - Run `dbt deps` and `dbt debug`

5. **Execute dbt Pipeline**

   - Run `dbt seed` to load seed data (full moon dates)
   - Run `dbt run` to build Silver and Gold layers
   - Run `dbt test` to validate data quality

6. **Schedule Workflow**
   - Create Databricks job using the workflow configuration
   - Set up appropriate scheduling (daily, hourly, etc.)

### Monitoring and Maintenance

1. **Data Quality Monitoring**

   - Set up alerts for dbt test failures
   - Monitor data freshness and volume
   - Track pipeline execution times

2. **Performance Optimization**

   - Monitor query performance
   - Optimize table clustering and partitioning
   - Review and update warehouse sizing

3. **Documentation**
   - Generate dbt docs: `dbt docs generate` and `dbt docs serve`
   - Maintain data lineage documentation
   - Update business glossary

---

## Architecture Diagram

```
Azure Data Lake Storage Gen2 (airbnbstorage)
├── src/
│   ├── listings.csv
│   └── reviews.csv
│
↓ (PySpark Ingestion)
│
Unity Catalog: airbnb_medallion
├── bronze/ (Raw Data)
│   ├── listings
│   ├── reviews
│   └── seed_full_moon_dates (from dbt seed)
│
↓ (dbt Transformations)
│
├── silver/ (Cleaned Data)
│   ├── stg_listings
│   ├── stg_reviews
│   └── stg_full_moon_dates
│
↓ (dbt Transformations)
│
└── gold/ (Analytics Ready)
    ├── dim_listings
    ├── fact_reviews
    └── agg_neighbourhood_metrics
```

This comprehensive setup provides a robust Medallion Architecture with proper data governance, quality controls, and scalability for your Airbnb analytics use case.
