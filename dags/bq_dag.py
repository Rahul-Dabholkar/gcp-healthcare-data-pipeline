from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import logging

PROJECT_ID = "gcp-healthcare-etl-2025"
LOCATION = "US"
BUCKET_NAME = "us-central1-gcp-healthcare--e353934f-bucket"

# Paths inside the bucket
SQL_PATHS = {
    "bronze": "pipelines/loaders/bronze.sql",
    "silver": "pipelines/transforms/silver.sql",
    "gold": "pipelines/transforms/gold.sql",
}


def read_sql_from_gcs(file_path: str) -> str:
    """Read SQL file content directly from GCS with logging."""
    hook = GCSHook()
    logging.info(f"📥 Reading SQL file from GCS: gs://{BUCKET_NAME}/{file_path}")
    try:
        content = hook.download(bucket_name=BUCKET_NAME, object_name=file_path).decode("utf-8")
        logging.info(f"✅ Successfully read SQL file: {file_path}")
        logging.debug(f"SQL Preview ({file_path}):\n{content[:400]}...")
        return content
    except Exception as e:
        logging.error(f"❌ Error reading SQL from GCS ({file_path}): {e}", exc_info=True)
        raise


default_args = {
    "owner": "RAHUL DEV",
    "depends_on_past": False,
    "email": ["rahultbeast@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 11, 1),
}

with DAG(
    dag_id="bigquery_dag",
    description="Run staged SQL transformations (bronze → silver → gold) in BigQuery",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["bq", "gcs", "composer"],
) as dag:

    bronze_sql = read_sql_from_gcs(SQL_PATHS["bronze"])
    silver_sql = read_sql_from_gcs(SQL_PATHS["silver"])
    gold_sql = read_sql_from_gcs(SQL_PATHS["gold"])

    bronze_task = BigQueryInsertJobOperator(
        task_id="run_bronze_sql",
        configuration={
            "query": {
                "query": bronze_sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    silver_task = BigQueryInsertJobOperator(
        task_id="run_silver_sql",
        configuration={
            "query": {
                "query": silver_sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    gold_task = BigQueryInsertJobOperator(
        task_id="run_gold_sql",
        configuration={
            "query": {
                "query": gold_sql,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    bronze_task >> silver_task >> gold_task
