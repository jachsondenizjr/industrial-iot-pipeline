# Industrial IoT Monitoring Pipeline

## Overview
End-to-end data pipeline for industrial equipment monitoring using real sensor data patterns from diesel engines and heavy machinery. Built with Databricks, Delta Lake, PySpark and dbt following Medallion Architecture.

## Architecture
```
Sensors (Simulated) 
    → Python Generator 
    → Bronze Layer (Delta Lake - Raw) 
    → Silver Layer (PySpark - Clean & Validated) 
    → Gold Layer (dbt - KPIs & Analytics)
```

## Tech Stack
- **Platform:** Databricks + Delta Lake
- **Processing:** PySpark
- **Transformation:** dbt
- **Orchestration:** Databricks Workflows
- **Storage:** AWS S3 / DBFS
- **CI/CD:** GitHub Actions
- **Quality:** dbt tests + Great Expectations
- **Language:** Python + SQL

## Project Structure
```
industrial_iot_pipeline/
├── data_generator/          # Sensor data simulator
├── bronze/                  # Raw ingestion scripts
├── silver/                  # PySpark transformation jobs
├── gold/                    # Aggregations and KPIs
├── dbt_project/             # dbt models and tests
├── notebooks/               # Databricks notebooks
├── workflows/               # Databricks Workflows config
└── .github/workflows/       # CI/CD pipelines
```

## Domain Knowledge
Based on real industrial experience with:
- Diesel engine sensor data (J1939/CAN Bus protocol)
- Emissions monitoring (NOx, PM2.5, CO2)
- Equipment performance metrics (RPM, temperature, pressure, torque)
- Regulatory compliance (CONAMA/PROCONVE, Euro VI)

## Getting Started
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure Databricks connection in `.env`
4. Run data generator: `python data_generator/sensor_simulator.py`
5. Execute Bronze ingestion
6. Run Silver transformation
7. Execute dbt for Gold layer

## Author
Jachson Deniz Junior | Data Engineer
github.com/jachsondenizjr | linkedin.com/in/jachson-deniz-junior
