# Healthcare Revenue Cycle Management (RCM) - Data Engineering Pipeline

A GCP-based data engineering solution for managing the financial aspects of healthcare revenue cycles using **Apache Airflow** (on Cloud Composer), **PySpark**, and **BigQuery**.

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Data Sources](#data-sources)
- [Pipeline Stages (Medallion Architecture)](#pipeline-stages-medallion-architecture)
- [Quick Start](#quick-start)
- [Development Setup](#development-setup)
- [Deployment](#deployment)
- [Configuration](#configuration)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## 🏥 Project Overview

**Healthcare Revenue Cycle Management (RCM)** is the process hospitals use to manage financial aspects from patient appointment scheduling through provider payment. This data pipeline:

1. **Ingests** data from multiple sources (EMR systems, insurance claims, reference codes).
2. **Transforms** raw data through staging layers (bronze → silver → gold).
3. **Produces** fact and dimension tables for business intelligence and KPI reporting.

### The RCM Process
1. **Patient Visit**: Patient details and insurance are collected to ensure the provider knows who will pay.
2. **Services Provided**: Daily checkups, treatments, surgeries.
3. **Billing**: Hospital creates a bill.
4. **Claims Review**: Insurance company reviews and pays in full, partial, or declines.
5. **Payments & Follow-up**: Provider may follow up on partial payments or patient portions.
6. **Tracking & Improvement**: RCM ensures quality care while maintaining financial health.

### Key Stakeholders
- **Finance teams**: Track claim denials, payments, and revenue cycles.
- **Reporting teams**: Generate KPIs and dashboards.
- **Operations teams**: Monitor data quality and pipeline performance.

---

## 🏗️ Architecture

![Airflow Dag](https://github.com/Rahul-Dabholkar/gcp-healthcare-data-pipeline/blob/main/docs/dag_diagram.png)

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources (Multi-tenant)               │
├─────────────────────────────────────────────────────────────┤
│ • EMR (Cloud SQL): Hospital A & B                           │
│   - Patients, Providers, Departments, Encounters, Xactions  │
│ • Claims (Flat Files): Insurance data (GCS Landing)         │
│ • Reference APIs: CPT, ICD, NPI codes                       │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│            Ingestion Layer (src/pipelines/ingestion)         │
├─────────────────────────────────────────────────────────────┤
│ • hospitalA_mysqlToLanding.py: Read Hospital A DB           │
│ • hospitalB_mysqlToLanding.py: Read Hospital B DB           │
│ • claims.py: Load claim flat files                          │
│ • cpt_codes.py, icd_codes.py, npi_codes.py: Ref data       │
│ Orchestrated by: parent_dag.py (via pyspark_dag.py)        │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│         Processing Layer (PySpark)                           │
├─────────────────────────────────────────────────────────────┤
│ • Landing → Bronze (light cleaning, type casting)           │
│ • Orchestrated by: pyspark_dag.py                           │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│         Transformation Layer (BigQuery SQL)                  │
├─────────────────────────────────────────────────────────────┤
│ • Bronze → Silver: Business logic, deduplication, joins     │
│ • Silver → Gold: Fact/Dimension tables for BI               │
│ • Orchestrated by: bq_dag.py (BigQueryInsertJobOperator)   │
└──────────┬──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│              Analytics / BI Reporting                        │
├─────────────────────────────────────────────────────────────┤
│ • Looker/Tableau dashboards consume Gold tables             │
│ • KPI metrics: claim denial rates, payment cycles, etc.     │
└─────────────────────────────────────────────────────────────┘
```
![Pipeline Diagram](https://github.com/Rahul-Dabholkar/gcp-healthcare-data-pipeline/blob/main/docs/pipeline_diagram.png)

---

## 📁 Repository Structure

```
healthcare-revenue-cycle-management/
├── dags/                              # Airflow DAG definitions
│   ├── parent_dag.py                  # Master orchestrator (triggers pyspark & bq dags)
│   ├── pyspark_dag.py                 # PySpark ingestion & landing → bronze
│   ├── bq_dag.py                      # BigQuery bronze → silver → gold
│   └── operators/                     # Custom Airflow operators (future)
│
├── src/                               # Main pipeline code (installable package)
│   ├── pipelines/
│   │   ├── ingestion/                 # Data source connectors
│   │   │   ├── hospitalA_mysqlToLanding.py
│   │   │   ├── hospitalB_mysqlToLanding.py
│   │   │   ├── claims.py
│   │   │   ├── cpt_codes.py
│   │   │   ├── icd_codes.py
│   │   │   └── npi_codes.py
│   │   ├── transforms/                # Data transformation logic (future)
│   │   └── loaders/                   # BigQuery/GCS write operations
│   ├── utils/                         # Shared utilities
│   │   └── add_dags_to_composer.py    # Deployment helper
│   └── config/                        # Configuration parsers
│
├── configs/                           # Non-code configuration
│   ├── audit_table_ddl.sql
│   └── load_config.csv
│
├── schemas/                           # Database schemas & DDLs
│   └── emr/
│       ├── hospital-a/
│       │   └── ddl.sql                # Hospital A EMR schema
│       └── hospital-b/
│           └── ddl.sql                # Hospital B EMR schema
│
├── data/                              # Sample data & references
│   ├── claims/
│   │   ├── hospital1_claim_data.csv
│   │   └── hospital2_claim_data.csv
│   ├── cptcodes/                      # CPT code reference files
│   ├── emr/
│   │   ├── hospital-a/
│   │   └── hospital-b/
│   └── configs/
│       └── load_config.csv
│
├── infra/                             # Infrastructure & deployment
│   └── scripts/
│       └── add_dags_to_composer.py    # Deploy DAGs to Composer
│
├── tests/                             # Unit & integration tests
│   ├── unit/
│   │   └── test_ingestion.py
│   └── integration/
│
├── cloudbuild.yaml                    # GCP Cloud Build CI/CD pipeline
├── requirements.txt                   # Python dependencies (Airflow, clients)
├── pyproject.toml                     # Package metadata (src/ as installable)
├── ProjectNotes.md                    # Project context & business requirements
└── README.md                          # This file
```

### Directory Purposes

| Directory | Purpose | Owner |
|-----------|---------|-------|
| `dags/` | Airflow orchestration layer | Data Engineer (DAG author) |
| `src/pipelines/` | Business logic & ETL code | Data Engineer (core logic) |
| `src/utils/` | Shared helpers (logging, GCS/BQ clients) | Data Engineer (infra) |
| `configs/` | SQL templates, loader configs | Data Analyst / Engineer |
| `schemas/` | EMR DDLs, reference schemas | Database Engineer |
| `data/` | Test data, sample CSVs | Data owner (EMR, Claims team) |
| `infra/` | CI/CD, deployment scripts | DevOps / Data Engineer |
| `tests/` | Unit tests, integration tests | QA / Data Engineer |

---

## 💾 Data Sources

### 1. **EMR (Electronic Medical Records) - Cloud SQL**
- **Hospital A**: `hospital_a_db` (MySQL)
- **Hospital B**: `hospital_b_db` (MySQL)
- **Tables**:
  - `patients`: Demographic data
  - `providers`: Healthcare practitioners
  - `departments`: Hospital departments
  - `encounters`: Patient visits/admissions
  - `transactions`: Financial transactions (charges, payments)
- **Ingestion**: `src/pipelines/ingestion/hospitalA_mysqlToLanding.py` & `hospitalB_mysqlToLanding.py`
- **Frequency**: Daily incremental pull

### 2. **Claims Data - Flat Files (GCS Landing)**
- **Source**: Insurance companies upload monthly claim files
- **Format**: CSV
- **Files**: `hospital1_claim_data.csv`, `hospital2_claim_data.csv`
- **Key fields**: `claim_id`, `patient_id`, `amount`, `status`, `denial_reason` (if applicable)
- **Ingestion**: `src/pipelines/ingestion/claims.py`
- **Frequency**: Monthly batch load

### 3. **Reference Data - Public APIs**

#### CPT (Current Procedural Terminology)
- **Source**: Public API or flat file (local copy)
- **Purpose**: Map procedure codes to descriptions
- **Ingestion**: `src/pipelines/ingestion/cpt_codes.py`
- **Refresh**: As needed or quarterly

#### ICD (International Classification of Diseases)
- **Source**: Public API or flat file
- **Purpose**: Map diagnosis codes to descriptions
- **Ingestion**: `src/pipelines/ingestion/icd_codes.py`
- **Refresh**: As needed or quarterly

#### NPI (National Provider Identifier)
- **Source**: CMS Public API
- **Purpose**: Validate & enrich provider identities
- **Ingestion**: `src/pipelines/ingestion/npi_codes.py`
- **Refresh**: Quarterly

---

## 🎯 Pipeline Stages (Medallion Architecture)

### **Stage 1: Landing (Raw)**
- **Source**: Direct copy of data from external systems.
- **Location**: GCS landing bucket (or Airflow temp storage).
- **Characteristics**: Unvalidated, undeduped, original format.
- **Owner**: Ingestion DAG (`pyspark_dag.py`)
- **Retention**: 7 days (can be purged after Bronze validation)

### **Stage 2: Bronze (Validated)**
- **Transformation**: Light cleaning, type casting, null handling.
- **Processing**: PySpark (via `pyspark_dag.py`).
- **Location**: BigQuery `bronze` dataset.
- **Schema**: Mirrors source structure, adds metadata (ingestion_date, source, record_hash, etc.).
- **Characteristics**: 1:1 mapping to sources, audit columns added.

### **Stage 3: Silver (Business Logic)**
- **Transformation**: Deduplication, joins across sources, business rules, SCD Type 2.
- **Processing**: BigQuery SQL (via `bq_dag.py`).
- **Location**: BigQuery `silver` dataset.
- **Example Joins**:
  - Patients + Encounters + Transactions (from EMR)
  - Claims joined to Patients (match on patient_id, encounter_date)
  - CPT/ICD codes enriched onto procedures
- **Characteristics**: Conformed dimensions, conformed facts, historical tracking.

### **Stage 4: Gold (Analytics-Ready)**
- **Transformation**: Fact tables, dimension tables, aggregations for BI.
- **Processing**: BigQuery SQL.
- **Location**: BigQuery `gold` dataset.
- **Outputs**:
  - **Fact tables**: 
    - `fact_claims`: One row per claim (with claim_status, denial_reason, revenue_recognized)
    - `fact_payments`: Claim payment history (payment_date, payment_amount, payment_method)
    - `fact_encounters`: Patient encounters with clinical & financial data
  - **Dimension tables**: 
    - `dim_patients`: Patient master (demographics, current status)
    - `dim_providers`: Provider master (credentials, department)
    - `dim_claims_status`: Claim status reference
    - `dim_date`: Time dimension for easy time-based joins
- **Aggregates**: Revenue by department, denial rates by provider, days to pay metrics

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- GCP project with:
  - Cloud Composer (managed Airflow)
  - BigQuery
  - Cloud SQL (MySQL)
  - GCS buckets (landing, staging)
- Service account with permissions: BigQuery Editor, Storage Editor, Cloud SQL Client

### Local Development

#### 1. Clone & Setup
```bash
git clone <repo-url>
cd healthcare-revenue-cycle-management
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

#### 2. Install the `src` package for imports
```bash
pip install -e .
```

#### 3. Run a quick unit test
```bash
python -m pytest tests/unit/test_ingestion.py -v
```

#### 4. Dry-run a DAG locally (requires Airflow init)
```bash
airflow db init
airflow dags test parent_dag 2025-01-01
```

---

## 🛠️ Development Setup

### Configuration

#### Environment Variables
Create a `.env` file (not committed):
```env
GCP_PROJECT_ID=gcp-healthcare-etl-2025
GCP_LOCATION=US
BIGQUERY_DATASET_BRONZE=bronze
BIGQUERY_DATASET_SILVER=silver
BIGQUERY_DATASET_GOLD=gold
GCS_LANDING_BUCKET=us-central1-healthcare-landing
CLOUD_SQL_INSTANCE=project:region:instance-name
CLOUD_SQL_USER=admin
CLOUD_SQL_PASSWORD=<secret>
CLOUD_SQL_DATABASE=hospital_a_db
```

#### DAG Configuration
Edit `dags/parent_dag.py` to set:
- `schedule_interval`: e.g., `"0 5 * * *"` (daily at 5am UTC)
- `retries`: How many times to retry on failure
- `email`: Notification recipients
- `tags`: For filtering in Airflow UI

#### BigQuery Constants (in `dags/bq_dag.py`)
```python
PROJECT_ID = "gcp-healthcare-etl-2025"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/dags/../src/pipeline/loaders/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/dags/../src/pipeline/transforms/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/dags/../src/pipeline/transforms/gold.sql"
```

### Adding a New Ingestion Source

Example: Add a new hospital C.

1. **Create a new ingestion module**:
   ```python
   # src/pipelines/ingestion/hospitalC_mysqlToLanding.py
   def run_hospitalC_ingest(conn_str: str, landing_path: str) -> None:
       """Fetch data from Hospital C MySQL DB and write to GCS landing."""
       import pandas as pd
       from google.cloud import storage
       
       # Connect to MySQL, fetch data
       df = pd.read_sql("SELECT * FROM patients", con=create_engine(conn_str))
       
       # Upload to GCS landing
       client = storage.Client()
       bucket = client.bucket("us-central1-healthcare-landing")
       blob = bucket.blob(f"{landing_path}/hospital_c/patients.csv")
       blob.upload_from_string(df.to_csv(index=False))
   ```

2. **Update the ingestion DAG** (`dags/pyspark_dag.py`):
   ```python
   from src.pipelines.ingestion import hospitalC_mysqlToLanding
   
   task_hospital_c = PythonOperator(
       task_id="ingest_hospital_c",
       python_callable=hospitalC_mysqlToLanding.run_hospitalC_ingest,
       op_kwargs={
           "conn_str": "mysql+pymysql://admin:password@cloudsql/hospital_c_db",
           "landing_path": "landing/2025-01-15"
       }
   )
   task_hospital_a >> task_hospital_c >> transform_to_bronze
   ```

3. **Test locally**:
   ```bash
   python -c "from src.pipelines.ingestion import hospitalC_mysqlToLanding; hospitalC_mysqlToLanding.run_hospitalC_ingest(...)"
   ```

---

## 📤 Deployment

### Prerequisites
- GCP Service Account with appropriate roles
- Cloud Composer environment provisioned
- DAGs synced to Composer's GCS bucket

### Steps

#### 1. Update DAGs in Composer
```bash
python infra/scripts/add_dags_to_composer.py \
  --dags_directory="dags/" \
  --dags_bucket="us-central1-gcp-healthcare--e353934f-bucket" \
  --data_directory="data/"
```

#### 2. Deploy via Cloud Build (CI/CD)
```bash
gcloud builds submit . \
  --config=cloudbuild.yaml \
  --substitutions=_DAGS_DIRECTORY="dags/",_DAGS_BUCKET="us-central1-gcp-healthcare--e353934f-bucket"
```

The Cloud Build pipeline:
- Installs Python dependencies
- Runs `add_dags_to_composer.py` to sync DAGs and data to the Composer bucket
- Logs to Cloud Logging

#### 3. Verify in Composer
- Navigate to Cloud Composer > [Environment Name] > DAGs folder (GCS)
- Refresh Airflow web UI: `https://your-composer-webserver-url`
- Check that `parent_dag`, `pyspark_dag`, `bq_dag` appear and are enabled

#### 4. Trigger a Manual Run
```bash
gcloud composer environments run <composer-env> \
  --location us-central1 \
  dags trigger -- parent_dag
```

#### 5. Monitor Execution
```bash
gcloud composer environments storage logs list \
  --environment <env-name> --location us-central1 | head -20
```

---

## ⚙️ Configuration

### SQL Queries (BigQuery)

#### Bronze Layer Query
`src/pipeline/loaders/bronze.sql` — Load landing data into bronze tables (example):
```sql
CREATE OR REPLACE TABLE bronze.patients AS
SELECT 
  patient_id,
  name,
  dob,
  gender,
  CURRENT_TIMESTAMP() AS _ingestion_timestamp,
  FARM_FINGERPRINT(CONCAT(patient_id, name)) AS _record_hash
FROM `gcp-healthcare-etl-2025.landing.patients`;
```

#### Silver Layer Query
`src/pipeline/transforms/silver.sql` — Apply business logic & joins:
```sql
CREATE OR REPLACE TABLE silver.patients_enriched AS
SELECT
  p.patient_id,
  p.name,
  p.dob,
  COUNT(DISTINCT e.encounter_id) AS total_encounters,
  MAX(e.encounter_date) AS last_encounter_date,
  p._ingestion_timestamp
FROM bronze.patients p
LEFT JOIN bronze.encounters e ON p.patient_id = e.patient_id
WHERE p._ingestion_timestamp = (SELECT MAX(_ingestion_timestamp) FROM bronze.patients)
GROUP BY p.patient_id, p.name, p.dob, p._ingestion_timestamp;
```

#### Gold Layer Query
`src/pipeline/transforms/gold.sql` — Create fact & dimension tables:
```sql
CREATE OR REPLACE TABLE gold.fact_claims AS
SELECT
  c.claim_id,
  p.patient_id,
  pr.provider_id,
  c.amount,
  c.claim_date,
  c.claim_status,
  c.denial_reason,
  EXTRACT(DAY FROM CURRENT_DATE() - c.claim_date) AS days_outstanding
FROM silver.claims c
LEFT JOIN silver.patients_enriched p ON c.patient_id = p.patient_id
LEFT JOIN silver.providers pr ON c.provider_id = pr.provider_id;
```

### Loader Config
`configs/load_config.csv` — Specifies which tables to load and from where (example):
```csv
target,source,format,partition_column
bronze.patients,gcs-landing/patients,csv,
bronze.encounters,gcs-landing/encounters,csv,encounter_date
bronze.transactions,gcs-landing/transactions,csv,transaction_date
bronze.claims,gcs-landing/claims,csv,claim_date
```

---

## ✅ Testing

### Unit Tests
```bash
python -m pytest tests/unit/ -v --cov=src
```

### Integration Tests (requires GCP credentials)
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
python -m pytest tests/integration/ -v
```

### Dry-run DAG Locally
```bash
airflow dags test parent_dag 2025-01-15
```

### Validate Airflow DAG Syntax
```bash
python -m py_compile dags/parent_dag.py dags/bq_dag.py dags/pyspark_dag.py
```

### Test Ingestion Module
```bash
python -m pytest tests/unit/test_ingestion.py::test_claims_ingestion -v
```

---

## 🔧 Troubleshooting

### Issue: "Import 'airflow' could not be resolved"
**Solution**: Install Apache Airflow:
```bash
pip install apache-airflow==2.6.3
```

### Issue: BigQuery query fails with "Table not found"
**Solution**: 
1. Verify the table exists in BigQuery (check dataset & table names).
2. Ensure the Airflow service account has `BigQuery Editor` role.
3. Check the SQL file paths in `bq_dag.py`.
4. Verify dataset exists: `bq ls --dataset_id=bronze`

### Issue: Cloud SQL connection times out
**Solution**:
1. Verify Cloud SQL Public IP is whitelisted (or use Cloud SQL Proxy).
2. Check credentials in connection string.
3. Ensure the service account has `Cloud SQL Client` role.
4. Test connection: `gcloud sql connect hospital-a-db --user=admin`

### Issue: DAG not appearing in Composer UI
**Solution**:
1. Run `add_dags_to_composer.py` to sync DAG files to GCS.
2. Refresh the Composer environment:
   ```bash
   gcloud composer environments update <env-name> \
     --location us-central1 \
     --update-airflow-configs core-dags_folder=/home/airflow/gcs/dags
   ```
3. Check DAGs folder in GCS: `gsutil ls gs://us-central1-gcp-healthcare--e353934f-bucket/dags/`

### Debugging a Failed Task
1. **View logs in Cloud Composer**:
   ```bash
   gcloud composer environments storage logs list \
     --environment <env-name> --location us-central1
   ```
2. **Rerun a specific task**:
   ```bash
   airflow tasks run parent_dag trigger_pyspark_dag 2025-01-15
   ```
3. **Check Airflow web UI logs**: Navigate to DAG run → Task → Logs tab

### Issue: Permission denied writing to GCS
**Solution**:
1. Verify service account has `Storage Editor` role on bucket.
2. Check bucket policy: `gsutil iam ch serviceAccount:...:objectEditor gs://bucket-name`
3. Ensure Composer environment is attached to the service account.

---

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [BigQuery SQL Reference](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [GCP Healthcare Solutions](https://cloud.google.com/solutions/healthcare)
- [Healthcare RCM Best Practices](https://en.wikipedia.org/wiki/Medical_billing)

---

## 📝 License & Contact

- **Project Owner**: Rahul Dev (rahultbeast@gmail.com)
- **Domain**: Healthcare Revenue Cycle Management (RCM)
- **Tech Stack**: GCP (Composer, BigQuery, Cloud SQL, GCS), Apache Airflow, PySpark
- **Last Updated**: November 12, 2025

---

### Quick Command Reference

```bash
# Local development
pip install -r requirements.txt
pip install -e .
python -m pytest tests/unit/ -v

# Deploy to Composer
gcloud builds submit . --config=cloudbuild.yaml

# Trigger DAG
gcloud composer environments run <env> --location us-central1 dags trigger -- parent_dag

# View logs
gcloud composer environments storage logs list --environment <env> --location us-central1

# Dry-run DAG
airflow dags test parent_dag 2025-01-15
```
