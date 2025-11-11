from google.cloud import bigquery
import logging

"""
Initialization script for creating required BigQuery tables
for the GCP Healthcare ETL pipeline (audit_log + pipeline_logs).

Usage:
    python init_bq_tables.py
or in Airflow:
    PythonOperator(task_id="init_bq_tables", python_callable=init_bq_tables)
"""

BQ_PROJECT = "gcp-healthcare-etl-2025"
BQ_DATASET = "config_dataset"
BQ_LOG_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.pipeline_logs"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.audit_log"

# Setup
bq_client = bigquery.Client(project=BQ_PROJECT)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Helper Functions
def ensure_dataset_exists():
    """Create dataset if it doesn't exist."""
    try:
        bq_client.get_dataset(BQ_DATASET)
        logging.info(f"✅ Dataset exists: {BQ_DATASET}")
    except Exception:
        dataset = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
        dataset.location = "US"
        bq_client.create_dataset(dataset)
        logging.info(f"🆕 Created dataset: {BQ_DATASET}")


def ensure_table_exists(table_id: str, schema: list):
    """Create table if it doesn't exist."""
    try:
        bq_client.get_table(table_id)
        logging.info(f"✅ Table exists: {table_id}")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)
        logging.info(f"🆕 Created table: {table_id}")


def init_bq_tables():
    """Initialize dataset and required tables."""
    logging.info("🔍 Checking and initializing BigQuery tables...")

    ensure_dataset_exists()

    # pipeline_logs schema
    log_schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("message", "STRING"),
        bigquery.SchemaField("step", "STRING"),
        bigquery.SchemaField("table", "STRING"),
        bigquery.SchemaField("error_trace", "STRING"),
    ]
    ensure_table_exists(BQ_LOG_TABLE, log_schema)

    # audit_log schema
    audit_schema = [
        bigquery.SchemaField("data_source", "STRING"),
        bigquery.SchemaField("tablename", "STRING"),
        bigquery.SchemaField("load_type", "STRING"),
        bigquery.SchemaField("record_count", "INTEGER"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
    ]
    ensure_table_exists(BQ_AUDIT_TABLE, audit_schema)

    logging.info("✅ BigQuery environment is ready!")

if __name__ == "__main__":
    exit_code = init_bq_tables()
    exit(exit_code)
    
