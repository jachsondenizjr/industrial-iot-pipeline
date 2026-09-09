#!/usr/bin/env bash
# Aplica a correcao do CI/CD do industrial-iot-pipeline.
# Rode este script a partir da RAIZ do repositorio clonado localmente
# (o mesmo diretorio onde fica a pasta .git).
set -e

if [ ! -d ".git" ]; then
  echo "ERRO: rode este script a partir da raiz do repositorio (onde esta a pasta .git)."
  exit 1
fi

echo "1) Removendo venv/ do controle de versao (mantendo os arquivos localmente)..."
git rm -r -q --cached venv 2>/dev/null || echo "   (venv ja nao estava rastreado, ok)"

echo "2) Removendo a pasta duplicada industrial_iot_pipeline/ (copia inteira do projeto)..."
if [ -d "industrial_iot_pipeline" ]; then
  git rm -r -q industrial_iot_pipeline
else
  echo "   (pasta duplicada ja nao existe, ok)"
fi

echo "3) Reescrevendo bronze/ingest_bronze.py..."
cat > bronze/ingest_bronze.py << 'FILE_EOF'
"""
Bronze Layer Ingestion — Industrial IoT Pipeline
Reads raw sensor data and saves to Delta Lake Bronze layer
Author: Jachson Deniz Junior
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)
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
        "anomaly_records": df.filter(F.col("is_anomaly")).count(),
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
FILE_EOF

echo "4) Reescrevendo silver/transform_silver.py..."
cat > silver/transform_silver.py << 'FILE_EOF'
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

    failed = df_validated.filter(~F.col("_quality_passed")).count()
    logger.info(f"Records failing quality checks: {failed}")

    return df_validated.filter(F.col("_quality_passed"))


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
        .withColumn(
            "emissions_severity",
            F.when(F.col("nox_ppm") > 400, "CRITICAL")
            .when(F.col("nox_ppm") > 350, "HIGH")
            .when(F.col("nox_ppm") > 200, "MEDIUM")
            .otherwise("NORMAL")
        ) \
        .withColumn(
            "engine_status",
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
FILE_EOF

echo "5) Reescrevendo data_generator/sensor_simulator.py..."
python3 - << 'PYEOF'
import re
path = "data_generator/sensor_simulator.py"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()
content = content.replace(
    "import json\nimport random\nimport time\nimport uuid\nfrom datetime import datetime, timezone\nfrom typing import Generator\n",
    "import json\nimport random\nimport uuid\nfrom datetime import datetime, timezone\n",
)
with open(path, "w", encoding="utf-8") as f:
    f.write(content)
print("ok")
PYEOF

echo "6) Reescrevendo tests/test_sensor_simulator.py..."
cat > tests/test_sensor_simulator.py << 'FILE_EOF'
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_generator.sensor_simulator import (  # noqa: E402
    generate_sensor_reading,
    save_batch_to_json,
    EQUIPMENT_FLEET,
)


def test_equipment_fleet_not_empty():
    assert len(EQUIPMENT_FLEET) > 0


def test_sensor_reading_has_required_fields():
    equipment = EQUIPMENT_FLEET[0]
    reading = generate_sensor_reading(equipment)
    required = ["event_id", "equipment_id", "timestamp", "rpm",
                "temp_coolant_c", "nox_ppm", "is_anomaly"]
    for field in required:
        assert field in reading


def test_sensor_values_within_range():
    equipment = EQUIPMENT_FLEET[0]
    for _ in range(50):
        reading = generate_sensor_reading(equipment)
        assert reading["rpm"] >= 0
        assert reading["temp_coolant_c"] >= 50
        assert reading["nox_ppm"] >= 0
        assert 0 <= reading["load_pct"] <= 100


def test_event_id_is_unique():
    equipment = EQUIPMENT_FLEET[0]
    ids = [generate_sensor_reading(equipment)["event_id"] for _ in range(100)]
    assert len(set(ids)) == 100


def test_save_batch_creates_file(tmp_path):
    import json
    output = str(tmp_path / "test.json")
    save_batch_to_json(output, num_events=10)
    assert os.path.exists(output)
    with open(output) as f:
        data = json.load(f)
    assert len(data) == 10
FILE_EOF

echo "7) Criando .flake8 (config unica de lint, exclui notebooks/)..."
cat > .flake8 << 'FILE_EOF'
[flake8]
max-line-length = 120
exclude =
    .git,
    __pycache__,
    venv,
    .venv,
    *.egg-info,
    dist,
    build,
    notebooks
FILE_EOF

echo "8) Simplificando .github/workflows/ci.yml (usa o .flake8 acima)..."
cat > .github/workflows/ci.yml << 'FILE_EOF'
name: CI/CD — Industrial IoT Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install pandas pytest python-dotenv

      - name: Run data generator tests
        run: |
          python -m pytest tests/ -v

      - name: Lint with flake8
        run: |
          pip install flake8
          flake8 .
FILE_EOF

echo ""
echo "Tudo aplicado. Rodando pytest + flake8 localmente para conferir antes do commit..."
if command -v flake8 >/dev/null 2>&1 && command -v pytest >/dev/null 2>&1; then
  python -m pytest tests/ -v
  flake8 .
  echo "OK: pytest e flake8 passaram limpos."
else
  echo "(pytest/flake8 nao encontrados neste ambiente local — sem problema, o GitHub Actions vai rodar de qualquer forma)"
fi

echo ""
echo "Arquivos prontos. Revise no painel Source Control do VS Code, escreva a mensagem de commit e faca o push."
