
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="transportation.silver.city",
    comment="Cleaned and standardized city dimension",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():

    df_bronze = spark.read.table(
        "transportation.bronze.city_raw"
    )

    timestamp_col = (
        "ingest_datetime"
        if "ingest_datetime" in df_bronze.columns
        else "ingested_at"
    )

    df_silver = df_bronze.select(
        F.col("city_id"),
        F.col("city_name"),
        F.col(timestamp_col).alias(
            "bronze_ingest_timestamp"
        )
    )

    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

    return df_silver
