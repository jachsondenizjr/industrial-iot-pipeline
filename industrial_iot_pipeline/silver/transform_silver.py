"""
Silver Layer Transformation — Industrial IoT Pipeline
Cleans, validates and standardizes sensor data
Author: Jachson Deniz Junior
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data quality thresholds based on CONAMA/Euro VI standards
QUALITY_RULES = {
    "rpm":              {"min": 0,    "max": 3000},
    "temp_coolant_c":   {"min": 50,   "max": 130},
    "temp_oil_c":       {"min": 50,   "max": 150},
    "pressure_oil_bar": {"min": 0.5,  "max": 10.0},
    "fuel_rate_lh":     {"min": 0,    "max": 100},
    "nox_ppm":          {"min": 0,    "max": 1000},
    "pm25_mg_m3":       {"min": 0,    "max": 50},
    "co2_pct":          {"min": 0,    "max": 20},
    "load_pct":         {"min": 0,    "max": 100},
}


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("IndustrialIoT_Silver_Transform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def apply_quality_filters(df: DataFrame) -> DataFrame:
    """Apply data quality rules and flag out-of-range values."""
    logger.info("Applying data quality filters...")

    df_validated = df
    quality_conditions = []

    for col_name, rules in QUALITY_RULES.items():
        if col_name in df.columns:
            condition = (
                F.col(col_name).isNotNull() &
                F.col(col_name).between(rules["min"], rules["max"])
            )
            quality_conditions.append(condition)

    # Mark records that pass all quality checks
    if quality_conditions:
        combined = quality_conditions[0]
        for c in quality_conditions[1:]:
            combined = combined & c
        df_validated = df_validated.withColumn("_quality_passed", combined)
    else:
        df_validated = df_validated.withColumn("_quality_passed", F.lit(True))

    failed = df_validated.filter(F.col("_quality_passed") == False).count()
    logger.info(f"Records failing quality checks: {failed}")

    return df_validated.filter(F.col("_quality_passed") == True)


def enrich_sensor_data(df: DataFrame) -> DataFrame:
    """Add calculated fields and business logic."""
    logger.info("Enriching sensor data...")

    # Window for lag calculations per equipment
    window_eq = Window.partitionBy("equipment_id").orderBy("timestamp")

    df_enriched = df \
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
        .withColumn("hour_of_day", F.hour(F.col("timestamp"))) \
        .withColumn("day_of_week", F.dayofweek(F.col("timestamp"))) \
        .withColumn("rpm_prev", F.lag("rpm", 1).over(window_eq)) \
        .withColumn("rpm_delta", F.col("rpm") - F.col("rpm_prev")) \
        .withColumn("temp_coolant_prev", F.lag("temp_coolant_c", 1).over(window_eq)) \
        .withColumn("temp_coolant_delta", F.col("temp_coolant_c") - F.col("temp_coolant_prev")) \
        .withColumn("emissions_severity",
            F.when(F.col("nox_ppm") > 400, "CRITICAL")
            .when(F.col("nox_ppm") > 350, "HIGH")
            .when(F.col("nox_ppm") > 200, "MEDIUM")
            .otherwise("NORMAL")
        ) \
        .withColumn("engine_status",
            F.when(F.col("temp_coolant_c") > 105, "OVERHEATING")
            .when(F.col("pressure_oil_bar") < 2.5, "LOW_OIL_PRESSURE")
            .when(F.col("rpm") > 2000, "HIGH_RPM")
            .otherwise("NORMAL")
        ) \
        .withColumn("_transformed_at", F.current_timestamp())

    return df_enriched


def transform_bronze_to_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str
) -> None:
    """Main transformation from Bronze to Silver."""
    logger.info(f"Starting Silver transformation from: {bronze_path}")

    # Read Bronze
    df_bronze = spark.read.format("delta").load(bronze_path)
    total_bronze = df_bronze.count()
    logger.info(f"Bronze records: {total_bronze}")

    # Apply quality filters
    df_clean = apply_quality_filters(df_bronze)

    # Enrich data
    df_silver = enrich_sensor_data(df_clean)

    # Remove duplicate events (idempotent)
    df_silver = df_silver.dropDuplicates(["event_id"])

    total_silver = df_silver.count()
    logger.info(f"Silver records after transformation: {total_silver}")
    logger.info(f"Records filtered: {total_bronze - total_silver}")

    # Write to Silver Delta Lake
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("ingestion_date", "equipment_type") \
        .save(silver_path)

    logger.info(f"Silver transformation complete. Path: {silver_path}")


if __name__ == "__main__":
    spark = create_spark_session()

    BRONZE_PATH = "delta/bronze/sensor_readings"
    SILVER_PATH = "delta/silver/sensor_readings_clean"

    transform_bronze_to_silver(spark, BRONZE_PATH, SILVER_PATH)
