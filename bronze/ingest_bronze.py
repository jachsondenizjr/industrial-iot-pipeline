"""
Bronze Layer Ingestion — Industrial IoT Pipeline
Reads raw sensor data and saves to Delta Lake Bronze layer
Author: Jachson Deniz Junior
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema definition — explicit schema for data quality
SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("equipment_id", StringType(), False),
    StructField("equipment_type", StringType(), True),
    StructField("equipment_model", StringType(), True),
    StructField("equipment_year", IntegerType(), True),
    StructField("timestamp", StringType(), False),
    StructField("rpm", DoubleType(), True),
    StructField("temp_coolant_c", DoubleType(), True),
    StructField("temp_oil_c", DoubleType(), True),
    StructField("pressure_oil_bar", DoubleType(), True),
    StructField("fuel_rate_lh", DoubleType(), True),
    StructField("nox_ppm", DoubleType(), True),
    StructField("pm25_mg_m3", DoubleType(), True),
    StructField("co2_pct", DoubleType(), True),
    StructField("load_pct", DoubleType(), True),
    StructField("is_anomaly", BooleanType(), True),
    StructField("location_lat", DoubleType(), True),
    StructField("location_lon", DoubleType(), True),
    StructField("anomaly_flags", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("IndustrialIoT_Bronze_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def ingest_to_bronze(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    checkpoint_path: str
) -> None:
    """
    Ingest raw sensor data to Bronze Delta Lake layer.
    Preserves raw data without modifications.
    """
    logger.info(f"Starting Bronze ingestion from: {source_path}")

    # Read raw JSON data with explicit schema
    df_raw = spark.read \
        .schema(SENSOR_SCHEMA) \
        .json(source_path)

    # Add ingestion metadata — do NOT transform data in Bronze
    df_bronze = df_raw \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_path", F.lit(source_path)) \
        .withColumn("_pipeline_version", F.lit("1.0.0"))

    # Log basic stats
    total_records = df_bronze.count()
    logger.info(f"Records to ingest: {total_records}")

    # Write to Delta Lake Bronze — append mode (idempotent with event_id)
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date", "equipment_type") \
        .save(bronze_path)

    logger.info(f"Bronze ingestion complete. Records written: {total_records}")
    logger.info(f"Bronze path: {bronze_path}")


def validate_bronze(spark: SparkSession, bronze_path: str) -> dict:
    """Basic validation after Bronze ingestion."""
    df = spark.read.format("delta").load(bronze_path)

    stats = {
        "total_records": df.count(),
        "null_event_id": df.filter(F.col("event_id").isNull()).count(),
        "null_equipment_id": df.filter(F.col("equipment_id").isNull()).count(),
        "distinct_equipment": df.select("equipment_id").distinct().count(),
        "anomaly_records": df.filter(F.col("is_anomaly") == True).count(),
    }

    logger.info(f"Bronze validation: {stats}")
    return stats


if __name__ == "__main__":
    spark = create_spark_session()

    SOURCE_PATH = "data/sensor_readings_sample.json"
    BRONZE_PATH = "delta/bronze/sensor_readings"
    CHECKPOINT_PATH = "checkpoints/bronze"

    ingest_to_bronze(spark, SOURCE_PATH, BRONZE_PATH, CHECKPOINT_PATH)
    validate_bronze(spark, BRONZE_PATH)
