
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_PATH = "/Volumes/transportation/default/goodcabs_raw/city"

@dp.materialized_view(
    name="transportation.bronze.city_raw",
    comment="City Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(SOURCE_PATH)
    )

    df = (
        df.withColumn("file_name", col("_metadata.file_path"))
          .withColumn("ingest_datetime", current_timestamp())
    )

    return df
