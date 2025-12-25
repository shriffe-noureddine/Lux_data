# Lux_data

# Lux Mobility & Energy Insights

Stack: Apache Airflow (ETL/Forecast) + PostgreSQL + Spring Boot API + Power BI.

## Structure

- infra: docker-compose, DB init scripts
- etl/airflow: DAGs for ingestion + KPIs + forecasts
- analytics: Python features/models
- api: Spring Boot (added later)
- bi/powerbi: dashboards (exclude .pbix from git)

## Workflow

1. Start with Airflow + Postgres
2. Build ingestion + KPIs + forecast
3. Add Spring Boot API
4. Connect Power BI
