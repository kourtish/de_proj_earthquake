import logging
import os

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "g.kourtish"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("access_key", default_var=None)
SECRET_KEY = Variable.get("secret_key", default_var=None)
BUCKET = "prod"

# DAG settings
DAG_START_DATE = pendulum.datetime(2026, 3, 15, tz="Europe/Moscow")

LONG_DESCRIPTION = """
# Raw Earthquake Data Pipeline

This DAG orchestrates the extraction of earthquake data from the USGS (United States Geological Survey) API
and stores it in the MinIO S3-compatible storage in the raw layer.

## Workflow

1. **Extract**: Fetches earthquake event data from the USGS FDSNWS Event API for the specified date range
2. **Transform**: Processes the CSV data into optimized Parquet format for efficient storage and querying
3. **Load**: Uploads the processed data to MinIO S3 bucket under the raw layer path structure

## Schedule

Runs daily at 05:00 AM (Moscow Time) with historical backfill support.

## Data Details

- **Source**: https://earthquake.usgs.gov/fdsnws/event/1/query
- **Format**: CSV → Parquet (gzip compressed)
- **Storage**: S3 (MinIO) - prod bucket, raw layer
- **Path Pattern**: s3://prod/raw/earthquake/{date}/
"""

SHORT_DESCRIPTION = "Extract earthquake data from USGS API and load to S3 raw layer"

args = {
    "owner": OWNER,
    "start_date": DAG_START_DATE,
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """"""

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) TO 's3://{BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';

        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end