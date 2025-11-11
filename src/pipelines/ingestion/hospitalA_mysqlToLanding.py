from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import pandas as pd
import datetime
import json
import logging
import traceback
from typing import Optional, Dict, Any
import os
import tempfile

# -----------------------------------------------
# Initialize Clients
# -----------------------------------------------
storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("HospitalA_MySQL_To_Landing")
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
    .getOrCreate()
)

# -----------------------------------------------
# Google Cloud Configuration
# -----------------------------------------------
GCS_BUCKET = "gcp-healthcare-2025"
HOSPITAL_NAME = "hospital-a"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/config/load_config.csv"

# -----------------------------------------------
# BigQuery Configuration
# -----------------------------------------------
BQ_PROJECT = "gcp-healthcare-etl-2025"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.config_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.config_dataset.pipeline_logs"
BQ_TEMP_BUCKET = GCS_BUCKET

# -----------------------------------------------
# MySQL Configuration
# -----------------------------------------------
MYSQL_CONFIG = {
    "url": "jdbc:mysql://35.244.55.185:3306/hospital_a_db",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "admin-rahul",
    "password": "Rahul@742001"
}

# -----------------------------------------------
# Enhanced Logging Setup
# -----------------------------------------------
class PipelineLogger:
    """Handles structured logs and BigQuery inserts"""

    def __init__(self, pipeline_run_id: str):
        self.pipeline_run_id = pipeline_run_id
        self.start_time = datetime.datetime.now()
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = logging.getLogger(__name__)

    def log(self, level: str, message: str, step: str = None, table: str = None, error: Exception = None):
        """Log event locally and to BigQuery pipeline log table"""
        timestamp = datetime.datetime.utcnow().isoformat()
        log_entry = {
            "timestamp": timestamp,
            "event_type": level,
            "message": message,
            "step": step or "general",
            "table": table or "",
            "error_trace": traceback.format_exc() if error else None
        }

        # Print to console
        icon = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🚨"}.get(level, "ℹ️")
        self.logger.info(f"{icon} [{level}] {message} | Table: {table or '-'} | Step: {step or '-'}")

        # Insert to BigQuery pipeline_logs table
        try:
            rows_to_insert = [log_entry]
            errors = bq_client.insert_rows_json(BQ_LOG_TABLE, rows_to_insert)
            if errors:
                self.logger.error(f"Failed to insert log in BigQuery: {errors}")
        except Exception as e:
            self.logger.error(f"BigQuery pipeline log insert failed: {str(e)}")


# -----------------------------------------------
# Move existing files to archive
# -----------------------------------------------
def move_existing_files_to_archive(table: str, logger: PipelineLogger):
    try:
        bucket = storage_client.bucket(GCS_BUCKET)
        prefix = f"landing/{HOSPITAL_NAME}/{table}/"
        blobs = list(bucket.list_blobs(prefix=prefix))

        existing_files = [b for b in blobs if b.name.endswith(".json")]
        if not existing_files:
            logger.log("INFO", "No existing files to archive", "archive", table)
            return

        today = datetime.datetime.today()
        for blob in existing_files:
            archive_path = (
                f"landing/{HOSPITAL_NAME}/archive/{table}/{today.year}/{today.month:02}/{today.day:02}/"
                f"{blob.name.split('/')[-1]}"
            )
            bucket.copy_blob(blob, bucket, archive_path)
            blob.delete()

        logger.log("INFO", f"Archived {len(existing_files)} existing file(s)", "archive", table)
    except Exception as e:
        logger.log("WARNING", "Archiving failed", "archive", table, e)


# -----------------------------------------------
# Watermark retrieval
# -----------------------------------------------
def get_latest_watermark(table_name: str, logger: PipelineLogger) -> str:
    try:
        query = f"""
            SELECT MAX(load_timestamp) AS latest_timestamp
            FROM `{BQ_AUDIT_TABLE}`
            WHERE tablename = '{table_name}' AND data_source = 'hospital_a_db'
        """
        rows = list(bq_client.query(query).result())
        if rows and rows[0].latest_timestamp:
            return str(rows[0].latest_timestamp)
        return "1900-01-01 00:00:00"
    except Exception as e:
        logger.log("WARNING", "Failed to retrieve watermark", "watermark", table_name, e)
        return "1900-01-01 00:00:00"


# -----------------------------------------------
# Extract, Save JSON to GCS, Write Audit + Log
# -----------------------------------------------
def extract_and_save_to_landing(table: str, load_type: str, watermark_col: str, logger: PipelineLogger):
    start_time = datetime.datetime.now()
    try:
        logger.log("INFO", "Starting extraction", "extract", table)
        # Build query
        if load_type.lower() == "incremental":
            last_watermark = get_latest_watermark(table, logger)
            query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        else:
            query = f"(SELECT * FROM {table}) AS t"

        # Extract
        df = (
            spark.read.format("jdbc")
            .option("url", MYSQL_CONFIG["url"])
            .option("user", MYSQL_CONFIG["user"])
            .option("password", MYSQL_CONFIG["password"])
            .option("driver", MYSQL_CONFIG["driver"])
            .option("dbtable", query)
            .load()
        )

        record_count = df.count()

        today = datetime.datetime.today().strftime("%d%m%Y")
        gcs_path = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        # If no records found
        if record_count == 0:
            logger.log("WARNING", "No new records found", "extract", table)
            insert_audit_record(table, load_type, 0, "SUCCESS", logger)
            return

        # Convert to Pandas and save JSON
        pdf = df.toPandas()
        local_file = os.path.join(tempfile.gettempdir(), f"{table}_{today}.json")
        pdf.to_json(local_file, orient="records", lines=True, date_format="iso")

        # Upload JSON to GCS
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json")
        blob.upload_from_filename(local_file, content_type="application/json")
        os.remove(local_file)

        logger.log("INFO", f"Data written to landing zone at {gcs_path}", "write", table)
        insert_audit_record(table, load_type, record_count, "SUCCESS", logger)

    except Exception as e:
        logger.log("ERROR", "Extraction failed", "extract", table, e)
        insert_audit_record(table, load_type, 0, "FAILED", logger)
        raise


# -----------------------------------------------
# Insert Audit Record to BigQuery
# -----------------------------------------------
def insert_audit_record(table: str, load_type: str, record_count: int, status: str, logger: PipelineLogger):
    """Insert one record into audit_log table"""
    try:
        row = [{
            "data_source": "hospital_a_db",
            "tablename": table,
            "load_type": load_type,
            "record_count": record_count,
            "load_timestamp": datetime.datetime.utcnow().isoformat(),
            "status": status
        }]
        errors = bq_client.insert_rows_json(BQ_AUDIT_TABLE, row)
        if errors:
            logger.log("ERROR", f"Audit log insert failed: {errors}", "audit", table)
        else:
            logger.log("INFO", f"Audit record inserted ({status})", "audit", table)
    except Exception as e:
        logger.log("ERROR", "Audit record insert failed", "audit", table, e)


# -----------------------------------------------
# Read Config File
# -----------------------------------------------
def read_config_file(logger: PipelineLogger):
    try:
        df = spark.read.csv(CONFIG_FILE_PATH, header=True)
        count = df.count()
        logger.log("INFO", f"Loaded {count} config entries", "config")
        return df
    except Exception as e:
        logger.log("CRITICAL", "Failed to read config file", "config", error=e)
        raise


# -----------------------------------------------
# Main Pipeline
# -----------------------------------------------
def main():
    pipeline_run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = PipelineLogger(pipeline_run_id)
    logger.log("INFO", "Pipeline started", "start")

    try:
        config_df = read_config_file(logger)
        for row in config_df.collect():
            if row["is_active"] == "1" and row["datasource"] == "hospital_a_db":
                _, _, table, load_type, watermark_col, _, _ = row
                try:
                    move_existing_files_to_archive(table, logger)
                    extract_and_save_to_landing(table, load_type, watermark_col, logger)
                except Exception as e:
                    logger.log("ERROR", "Table processing failed", "process", table, e)
                    continue

        logger.log("SUCCESS", "Pipeline completed successfully", "end")
        return 0
    except Exception as e:
        logger.log("CRITICAL", "Pipeline failed", "end", error=e)
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)


# Read config file - to know what tables are incremental load and what tables are full load
# Move existing files to archive - move existing files to archive (if any)
    # Generate pipeline logs (STEP - EVENT TYPE - TABLE - MESSAGE - ERROR) 
# Extract and save to landing - extract data from SQL to GCS landing according to config file
    # Generates audit logs ( DB - TABLE - PASS/FAIL)