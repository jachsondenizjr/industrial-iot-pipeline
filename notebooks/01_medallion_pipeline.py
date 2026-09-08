# Databricks notebook source
import urllib.request
import json

url = "https://raw.githubusercontent.com/jachsondenizjr/industrial-iot-pipeline/main/data/sensor_readings_sample.json"

# Download direto para memória
with urllib.request.urlopen(url) as response:
    data = json.loads(response.read().decode('utf-8'))

print(f"Arquivo carregado com sucesso!")
print(f"Total de registros: {len(data)}")
print(f"Anomalias: {sum(1 for r in data if r['is_anomaly'])}")
print(f"\nPrimeiro registro:")
print(json.dumps(data[0], indent=2))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Converter lista para DataFrame Spark
df_raw = spark.createDataFrame(data)

print(f"Schema:")
df_raw.printSchema()

print(f"\nTotal de registros: {df_raw.count()}")
print(f"\nAmostra dos dados:")
display(df_raw.limit(5))

# COMMAND ----------

# Adicionar metadados de ingestão — Bronze não modifica os dados
df_bronze = df_raw \
    .withColumn("_ingested_at", F.current_timestamp()) \
    .withColumn("_source", F.lit("github/sensor_simulator")) \
    .withColumn("_pipeline_version", F.lit("1.0.0"))

# Salvar como tabela Delta no Catalog
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.bronze_sensor_readings")

print("Bronze layer criada com sucesso!")
print(f"Total de registros: {df_bronze.count()}")

# Verificar tabela criada
display(spark.sql("SHOW TABLES IN default"))

# COMMAND ----------

from pyspark.sql.window import Window

# Ler da Bronze
df_bronze = spark.table("default.bronze_sensor_readings")

# Aplicar regras de qualidade baseadas em CONAMA/Euro VI
df_silver = df_bronze \
    .filter(F.col("rpm").between(0, 3000)) \
    .filter(F.col("temp_coolant_c").between(50, 130)) \
    .filter(F.col("temp_oil_c").between(50, 150)) \
    .filter(F.col("pressure_oil_bar").between(0.5, 10.0)) \
    .filter(F.col("nox_ppm").between(0, 1000)) \
    .filter(F.col("pm25_mg_m3").between(0, 50)) \
    .filter(F.col("event_id").isNotNull()) \
    .dropDuplicates(["event_id"]) \
    .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
    .withColumn("hour_of_day", F.hour(F.col("timestamp"))) \
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

# Salvar Silver
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.silver_sensor_readings")

total = df_silver.count()
print(f"Silver layer criada! Registros: {total}")
display(df_silver.select("equipment_id", "rpm", "temp_coolant_c", "nox_ppm", "emissions_severity", "engine_status").limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table("default.silver_sensor_readings")

df_gold = df_silver.groupBy(
    "equipment_id",
    "equipment_type",
    "equipment_model",
    "equipment_year",
    "ingestion_date"
).agg(
    F.count("event_id").alias("total_readings"),
    F.sum(F.col("is_anomaly").cast("int")).alias("total_anomalies"),
    F.round(F.avg("rpm"), 1).alias("avg_rpm"),
    F.round(F.max("rpm"), 1).alias("max_rpm"),
    F.round(F.avg("temp_coolant_c"), 1).alias("avg_temp_coolant_c"),
    F.round(F.max("temp_coolant_c"), 1).alias("max_temp_coolant_c"),
    F.round(F.avg("nox_ppm"), 1).alias("avg_nox_ppm"),
    F.round(F.max("nox_ppm"), 1).alias("max_nox_ppm"),
    F.round(F.avg("pm25_mg_m3"), 2).alias("avg_pm25"),
    F.round(F.sum("fuel_rate_lh"), 2).alias("total_fuel_lh"),
    F.count(F.when(F.col("nox_ppm") > 350, 1)).alias("nox_violations"),
    F.count(F.when(F.col("temp_coolant_c") > 105, 1)).alias("overheating_events"),
    F.count(F.when(F.col("pressure_oil_bar") < 2.5, 1)).alias("low_pressure_events"),
    F.count(F.when(F.col("emissions_severity") == "CRITICAL", 1)).alias("critical_emissions"),
    F.count(F.when(F.col("engine_status") != "NORMAL", 1)).alias("engine_issues"),
    F.current_timestamp().alias("_updated_at")
)

print("Aggregation OK!")
df_gold.show(5)

# COMMAND ----------

df_gold_final = df_gold \
    .withColumn("anomaly_rate_pct",
        F.round(F.col("total_anomalies") * 100.0 / F.col("total_readings"), 2)
    ) \
    .withColumn("equipment_health",
        F.when(F.col("anomaly_rate_pct") > 20, "CRITICAL")
        .when(F.col("anomaly_rate_pct") > 10, "WARNING")
        .when(F.col("anomaly_rate_pct") > 5, "ATTENTION")
        .otherwise("HEALTHY")
    )

df_gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.gold_equipment_kpis")

print("Gold layer criada com sucesso!")
display(df_gold_final.orderBy("equipment_id"))

# COMMAND ----------

display(spark.sql("""
    SELECT 
        equipment_id,
        equipment_type,
        equipment_model,
        total_readings,
        total_anomalies,
        anomaly_rate_pct,
        avg_nox_ppm,
        max_nox_ppm,
        nox_violations,
        overheating_events,
        equipment_health
    FROM default.gold_equipment_kpis
    ORDER BY equipment_id
"""))