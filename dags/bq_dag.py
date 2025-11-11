import airflow
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Define constants
PROJECT_ID = "gcp-healthcare-etl-2025"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/pipelines/loaders/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/pipelines/transforms/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/pipelines/transforms/gold.sql"

# Helper to read SQL at runtime
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

# Default arguments
ARGS = {
    "owner": "RAHUL DEV",
    "start_date": datetime(2025, 1, 1),  # must be a valid date
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["rahultbeast@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run the BigQuery jobs",
    default_args=ARGS,
    catchup=False,
    tags=["gcs", "bq", "etl"]
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": read_sql_file(SQL_FILE_PATH_1),  # Read at runtime
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": read_sql_file(SQL_FILE_PATH_2),
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": read_sql_file(SQL_FILE_PATH_3),
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables