-- Gold Layer: Equipment KPIs and Performance Metrics
-- Author: Jachson Deniz Junior
-- Description: Aggregated KPIs per equipment for analytics consumption

{{ config(
    materialized='table',
    partition_by={
        "field": "ingestion_date",
        "data_type": "date"
    },
    cluster_by=["equipment_id", "equipment_type"]
) }}

WITH silver_base AS (
    SELECT * FROM {{ ref('silver_sensor_readings') }}
),

equipment_daily_kpis AS (
    SELECT
        equipment_id,
        equipment_type,
        equipment_model,
        equipment_year,
        ingestion_date,

        -- Volume metrics
        COUNT(event_id)                                 AS total_readings,
        COUNT(CASE WHEN is_anomaly THEN 1 END)          AS total_anomalies,
        ROUND(COUNT(CASE WHEN is_anomaly THEN 1 END) * 100.0 / COUNT(event_id), 2) AS anomaly_rate_pct,

        -- Engine performance
        ROUND(AVG(rpm), 1)                              AS avg_rpm,
        ROUND(MAX(rpm), 1)                              AS max_rpm,
        ROUND(MIN(rpm), 1)                              AS min_rpm,
        ROUND(AVG(load_pct), 1)                         AS avg_load_pct,

        -- Temperature metrics
        ROUND(AVG(temp_coolant_c), 1)                   AS avg_temp_coolant_c,
        ROUND(MAX(temp_coolant_c), 1)                   AS max_temp_coolant_c,
        ROUND(AVG(temp_oil_c), 1)                       AS avg_temp_oil_c,
        ROUND(MAX(temp_oil_c), 1)                       AS max_temp_oil_c,

        -- Pressure metrics
        ROUND(AVG(pressure_oil_bar), 2)                 AS avg_pressure_oil_bar,
        ROUND(MIN(pressure_oil_bar), 2)                 AS min_pressure_oil_bar,

        -- Fuel consumption
        ROUND(SUM(fuel_rate_lh), 2)                     AS total_fuel_consumed_l,
        ROUND(AVG(fuel_rate_lh), 2)                     AS avg_fuel_rate_lh,

        -- Emissions KPIs (CONAMA/Euro VI compliance)
        ROUND(AVG(nox_ppm), 1)                          AS avg_nox_ppm,
        ROUND(MAX(nox_ppm), 1)                          AS max_nox_ppm,
        ROUND(AVG(pm25_mg_m3), 2)                       AS avg_pm25_mg_m3,
        ROUND(MAX(pm25_mg_m3), 2)                       AS max_pm25_mg_m3,
        ROUND(AVG(co2_pct), 2)                          AS avg_co2_pct,

        -- Compliance flags
        COUNT(CASE WHEN nox_ppm > 350 THEN 1 END)       AS nox_violations,
        COUNT(CASE WHEN pm25_mg_m3 > 8.0 THEN 1 END)    AS pm25_violations,
        COUNT(CASE WHEN temp_coolant_c > 105 THEN 1 END) AS overheating_events,
        COUNT(CASE WHEN pressure_oil_bar < 2.5 THEN 1 END) AS low_pressure_events,

        -- Severity distribution
        COUNT(CASE WHEN emissions_severity = 'CRITICAL' THEN 1 END) AS critical_emissions,
        COUNT(CASE WHEN emissions_severity = 'HIGH' THEN 1 END)     AS high_emissions,
        COUNT(CASE WHEN emissions_severity = 'MEDIUM' THEN 1 END)   AS medium_emissions,
        COUNT(CASE WHEN emissions_severity = 'NORMAL' THEN 1 END)   AS normal_emissions,

        -- Metadata
        CURRENT_TIMESTAMP()                             AS _updated_at

    FROM silver_base
    GROUP BY
        equipment_id,
        equipment_type,
        equipment_model,
        equipment_year,
        ingestion_date
)

SELECT
    *,
    -- Compliance score (0-100)
    ROUND(
        100 - (
            (nox_violations * 2.0 / NULLIF(total_readings, 0) * 100) +
            (pm25_violations * 1.5 / NULLIF(total_readings, 0) * 100) +
            (overheating_events * 1.0 / NULLIF(total_readings, 0) * 100)
        ), 1
    ) AS compliance_score,

    -- Health status
    CASE
        WHEN anomaly_rate_pct > 20 THEN 'CRITICAL'
        WHEN anomaly_rate_pct > 10 THEN 'WARNING'
        WHEN anomaly_rate_pct > 5  THEN 'ATTENTION'
        ELSE 'HEALTHY'
    END AS equipment_health_status

FROM equipment_daily_kpis
