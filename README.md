# Transportation Lakehouse Pipeline

**Databricks | PySpark | SQL | Google Cloud Storage | Delta Lake | Lakeflow**

## Overview

This project implements a transportation analytics pipeline using Databricks and Google Cloud Storage (GCS). It organizes GoodCabs city and trip data into Bronze, Silver, and Gold layers using Lakeflow Spark Declarative Pipelines, PySpark transformations, Delta tables, and SQL views.

The project follows the GoodCabs data engineering tutorial and adapts its original Amazon S3 storage setup to Google Cloud Storage and Databricks Free Edition.

## Project at a Glance

| Metric | Implementation |
| :--- | :--- |
| Source data groups | 2: city and trips |
| Data processing layers | 3: Bronze, Silver, Gold |
| Silver datasets | 3: city, trips, calendar |
| Gold analytical views | 11: one combined fact view and 10 city views |
| Cities represented by SQL views | 10 |
| Incremental ingestion | Databricks Auto Loader for trips |
| Change processing | SCD Type 1 using Lakeflow Auto CDC |

These are project structure counts, not claims about row volume or performance gains.

## Architecture

```text
Google Cloud Storage
  gs://goodcabs-mk1/data-store/city/
  gs://goodcabs-mk1/data-store/trips/
                 ↓
File transfer into a Databricks Unity Catalog volume
  /Volumes/transportation/default/goodcabs_raw/
                 ↓
BRONZE
  transportation.bronze.city_raw
  transportation.bronze.trips_raw
                 ↓
SILVER
  transportation.silver.city
  transportation.silver.trips
  transportation.silver.calendar
                 ↓
GOLD
  transportation.gold.fact_trips
                 ↓
10 city specific analytical views
```

**Storage note:** GCS is the original source. In this implementation, files are copied into a Databricks Unity Catalog volume before ingestion. The current design does not claim a live GCS connection or automatic synchronization between GCS and Databricks.

## Technology Stack

| Technology | Role |
| :--- | :--- |
| Google Cloud Storage | Original storage for GoodCabs CSV files |
| Databricks | Data engineering workspace and pipeline execution |
| Lakeflow Spark Declarative Pipelines | Pipeline definitions and dependency management |
| PySpark | Ingestion, data cleaning, and transformations |
| Auto Loader | Incremental ingestion of trip files |
| Delta Lake | Storage format for pipeline tables |
| Unity Catalog | Organization of catalogs, schemas, tables, and volumes |
| SQL | Gold analytics views and validation queries |
| GitHub | Source control and project documentation |

## Pipeline Design

### Bronze: Raw Ingestion

The Bronze layer loads CSV files from the `goodcabs_raw` Unity Catalog volume.

* `city_raw` uses a materialized view to read city reference data.
* `trips_raw` uses Auto Loader to process incoming trip files incrementally.
* Ingestion metadata records file paths and processing timestamps.
* The source column `distance_travelled(km)` is renamed to `distance_travelled_km` to avoid unsupported characters in Delta column names.

### Silver: Cleaning and Change Processing

The Silver layer contains three datasets:

| Dataset | Transformation |
| :--- | :--- |
| `city` | Selects city IDs, city names, and ingestion metadata |
| `trips` | Standardizes trip fields, converts dates and numeric values, and applies data quality expectations |
| `calendar` | Generates date attributes, weekday and weekend flags, and selected Indian national holiday flags |

The trip pipeline includes expectations for dates and ratings, followed by **SCD Type 1 Auto CDC** keyed by trip ID. The tutorial implementation sequences changes with a processing timestamp.

The configured date range for the calendar is controlled by `start_date` and `end_date` pipeline settings.

### Gold: Analytics Ready Views

The main view, `transportation.gold.fact_trips`, joins the Silver trips, city, and calendar datasets. It brings together trip IDs, dates, city names, passenger categories, distances, fares, ratings, and calendar attributes.

The project also defines ten city specific views:

| City | SQL view |
| :--- | :--- |
| Chandigarh | `fact_trips_chandigarh` |
| Coimbatore | `fact_trips_coimbatore` |
| Indore | `fact_trips_indore` |
| Jaipur | `fact_trips_jaipur` |
| Kochi | `fact_trips_kochi` |
| Lucknow | `fact_trips_lucknow` |
| Mysore | `fact_trips_mysore` |
| Surat | `fact_trips_surat` |
| Vadodara | `fact_trips_vadodara` |
| Visakhapatnam | `fact_trips_visakhapatnam` |

These views support questions about ride volumes, revenue, trip distances, driver ratings, and passenger ratings by city.

## Repository Organization

A suggested organization for the project source files is:

```text
README.md
project_setup.ipynb
bronze/
    city_bronze.py
    trips_bronze.py
silver/
    city_silver.py
    trips_silver.py
    calendar_silver.py
gold/
    trips_gold.sql
    trips_chandigarh.sql
    trips_coimbatore.sql
    trips_indore.sql
    trips_jaipur.sql
    trips_kochi.sql
    trips_lucknow.sql
    trips_mysore.sql
    trips_surat.sql
    trips_vadodara.sql
    trips_visakhapatnam.sql
```

This is a suggested folder layout. Match these paths and names to the files actually committed to the repository. The original tutorial Python files used different filenames and S3 paths, so the committed Bronze and Silver scripts should contain the Databricks volume paths and current table names.

## How to Run

### 1. Create the Databricks catalog and schemas

Run `project_setup.ipynb`, or execute:

```sql
CREATE CATALOG IF NOT EXISTS transportation;
CREATE SCHEMA IF NOT EXISTS transportation.bronze;
CREATE SCHEMA IF NOT EXISTS transportation.silver;
CREATE SCHEMA IF NOT EXISTS transportation.gold;
```

### 2. Create the source volume and upload the data

```sql
CREATE VOLUME IF NOT EXISTS transportation.default.goodcabs_raw;
```

Copy the city and trip CSVs from GCS into these paths:

```text
/Volumes/transportation/default/goodcabs_raw/city/
/Volumes/transportation/default/goodcabs_raw/trips/
```

### 3. Configure the Bronze source paths

In the Bronze Python scripts, use:

```python
CITY_SOURCE = "/Volumes/transportation/default/goodcabs_raw/city"
TRIPS_SOURCE = "/Volumes/transportation/default/goodcabs_raw/trips"
```

Ensure that the Bronze output tables are named `transportation.bronze.city_raw` and `transportation.bronze.trips_raw` and that the Silver scripts read those names.

### 4. Configure and run the Lakeflow pipeline

Create or open a Lakeflow ETL pipeline. Include the Bronze and Silver Python source files. Use the `transportation` catalog and configure the calendar date range to cover the source trip dates.

Example pipeline configuration:

```text
start_date = 2025-01-01
end_date   = 2025-12-31
```

The dates above are examples. Adjust them to match your data, then run the pipeline and verify the Bronze and Silver outputs.

### 5. Create the Gold views

Run `gold/trips_gold.sql` first. After the combined `fact_trips` view is available, run the ten city SQL files.

### 6. Validate the outputs

```sql
SELECT COUNT(*) AS city_count
FROM transportation.silver.city;

SELECT COUNT(*) AS trip_count
FROM transportation.silver.trips;

SELECT
    city_name,
    COUNT(*) AS total_trips,
    ROUND(SUM(sales_amt), 2) AS total_revenue,
    ROUND(AVG(distance_kms), 2) AS average_distance_km,
    ROUND(AVG(passenger_rating), 2) AS average_passenger_rating
FROM transportation.gold.fact_trips
GROUP BY city_name
ORDER BY total_revenue DESC;
```

## Key Engineering Concepts

* Medallion architecture across Bronze, Silver, and Gold layers.
* Streaming file ingestion using Auto Loader.
* Managed transformations and dependencies using Lakeflow.
* Type conversion, schema standardization, and data quality expectations.
* SCD Type 1 change processing for trip records.
* Dimensional enrichment through city and calendar joins.
* Reusable SQL views for regional analysis.

## Current Scope and Possible Improvements

The project demonstrates a working design for processing GCS sourced files in Databricks. A future enhancement would automate the transfer from Google Cloud Storage to the Unity Catalog volume. Other improvements include source event timestamps for deterministic CDC ordering, automated deployment through GitHub, and measured quality and performance reporting.

## Credits

Adapted as a personal data engineering learning project from the [GoodCabs Databricks tutorial on YouTube](https://www.youtube.com/watch?v=bIIC44n2Dss). The GCS storage workflow and Databricks volume adaptation reflect this project's environment.
