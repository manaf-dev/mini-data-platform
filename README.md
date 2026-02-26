# Healthcare Operations and Patient Flow Analytics Platform

## About This Project

Healthcare facilities generate large volumes of operational data every day — patient registrations, clinical visits, hospital admissions, treatments performed, and billing transactions. In many settings this data sits in flat files and disconnected systems, making it difficult for hospital administrators, operations managers, and finance teams to answer basic questions: Which departments are under the most pressure? How long are patients staying? What is the hospital actually collecting versus what is billed?

This project builds a self-contained data platform that addresses that problem. It ingests raw healthcare data, runs it through an automated validation and transformation pipeline, loads clean records into a structured warehouse, and surfaces the results through operational dashboards — all running locally with a single command.

The platform was built as a demonstration of modern data engineering principles applied to a healthcare context. It is not connected to any real patient data. All records are synthetically generated and are designed to reflect realistic patterns, including common data quality issues such as missing values, invalid entries, duplicate records, and referential integrity violations. The pipeline detects and isolates these problems automatically, ensuring that only validated data reaches the reporting layer.

## What the Platform Does

The platform covers four core capabilities.

**Data Collection.** A data generator produces five interrelated CSV datasets representing patients, visits, admissions, treatments, and billing. These files are uploaded to MinIO, an S3-compatible object storage service, which acts as the raw data landing zone.

**Data Processing.** Apache Airflow orchestrates three sequential pipelines. The first ingests raw files into storage. The second reads those files, applies validation rules, rejects bad rows with documented reasons, and writes clean data back to a separate storage path. The third loads validated data into PostgreSQL and builds pre-aggregated tables that power the dashboards.

**Data Storage.** PostgreSQL holds the structured warehouse. It contains five core tables populated directly from the pipeline, plus three mart tables that pre-aggregate metrics by day, by department, and by patient. These mart tables are what Metabase queries to produce charts.

**Data Visualisation.** Metabase connects to PostgreSQL and provides four operational dashboards covering executive KPIs, department operations, financial performance, and patient demographics.

## Why Healthcare

Healthcare operations is a domain where data quality genuinely matters. A patient record with a missing admission timestamp, a billing entry where the insurance amount exceeds the total billed, or a treatment linked to a visit that does not exist — these are not just data annoyances, they represent real operational and financial risk. Building the platform around healthcare data made it possible to demonstrate meaningful validation logic rather than trivial checks.

The use case also produces visually informative dashboards. Metrics like average length of stay, department load over time, payment collection rates, and patient return rates tell a coherent operational story that is immediately understandable to a non-technical audience.

---

## Architecture

```
+-----------------+    +---------------------------------------------+
|  Data Generator |    |              Apache Airflow                  |
|  (Python CLI)   |--->|  DAG 1: Bronze Ingestion                    |
|  5 CSV files    |    |  DAG 2: Silver Pipeline (clean + validate)  |
+-----------------+    |  DAG 3: Gold Pipeline  (load + marts)       |
                       +--------------------+------------------------+
                                            |
                    +-----------------------+---------------------+
                    |                                             |
           +--------+--------+                      +------------+--------+
           |   MinIO          |                      |   PostgreSQL         |
           |  (Object Storage)|                      |  (Warehouse)         |
           |                  |                      |                      |
           |  bronze/         |                      |  patients            |
           |  raw CSVs        |                      |  visits              |
           |  partitioned     |                      |  admissions          |
           |  by date         |                      |  treatments          |
           |                  |                      |  billing             |
           |  silver/         |                      |                      |
           |  clean rows      |                      |  mart_daily_kpis     |
           |                  |                      |  mart_dept_stats     |
           |  silver/         |                      |  mart_patient_stats  |
           |  rejected/       |                      |                      |
           |  bad rows +      |                      +----------+-----------+
           |  reject reasons  |                                 |
           +------------------+                                 v
                                                   +-----------+-----------+
                                                   |  Metabase             |
                                                   |  (Dashboards)         |
                                                   |                       |
                                                   |  Executive Overview   |
                                                   |  Dept Operations      |
                                                   |  Financial Summary    |
                                                   |  Patient Analytics    |
                                                   +-----------------------+
```

### Data Flow Step by Step

```
generate.py
    Creates 5 synthetic CSVs with intentional data quality issues
    |
    v
MinIO  healthcare-bronze/
    Raw files stored as-is, partitioned by date: {dataset}/YYYY/MM/DD/
    Triggered by: healthcare_bronze_ingestion_v1
    |
    v
MinIO  healthcare-silver/
    Cleaned and validated rows only
    Bad rows written to silver/rejected/{dataset}/ with _reject_reason column
    Triggered by: healthcare_silver_pipeline_v1
    |
    v
PostgreSQL  ecomdb
    Rows upserted using ON CONFLICT DO UPDATE — reruns are safe
    Mart tables rebuilt on each run to reflect current warehouse state
    Triggered by: healthcare_gold_pipeline_v1
    |
    v
Metabase  localhost:3000
    4 dashboards reading from mart tables via SQL
```

---

## Tech Stack

| Component      | Technology     | Version   | Port        |
|----------------|----------------|-----------|-------------|
| Database       | PostgreSQL     | 16-alpine | 5432        |
| Orchestration  | Apache Airflow | 2.11.1    | 8080        |
| Object Storage | MinIO          | latest    | 9000 / 9001 |
| Dashboards     | Metabase       | latest    | 3000        |
| Language       | Python         | 3.11      |             |
| Containers     | Docker Compose | v2        |             |

---

## Project Structure

```
mini-healthcare-platform/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── README.md
│
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── bronze_ingestion_dag.py     DAG 1: raw CSV to MinIO bronze
│       ├── silver_dag.py               DAG 2: clean and validate to MinIO silver
│       └── gold_dag.py                 DAG 3: load to PostgreSQL and build marts
│
├── src/
│   ├── data_generator/
│   │   ├── config.py                   Volume settings, departments, treatment codes
│   │   ├── generator.py                Synthetic data generator with quality issues
│   │   └── main.py                     CLI entry point
│   ├── ingestion/
│   │   └── bronze_ingestion.py         Uploads CSVs to MinIO bronze
│   ├── silver/
│   │   ├── base.py                     Shared cleaner base class
│   │   ├── patients.py
│   │   ├── visits.py                   Includes patient FK check
│   │   ├── admissions.py               Includes visit FK check
│   │   ├── treatments.py               Includes visit FK check
│   │   └── billing.py                  Includes visit FK check
│   ├── gold/
│   │   ├── base.py                     Shared loader base class
│   │   ├── loader.py                   One upsert class per table
│   │   └── marts.py                    Pre-aggregated mart SQL
│   └── utils/
│       └── minio_client.py             MinIO client wrapper
│
├── sql/
│   ├── init.sql                        Creates airflow and metabase databases
│   └── schema.sql                      All warehouse table DDL
│
├── tests/
│   └── test_pipeline.py
│
├── docs/
│   ├── architecture.svg
│   └── screenshots/
│       ├── dashboard_executive.png
│       ├── dashboard_operations.png
│       ├── dashboard_financial.png
│       └── dashboard_patients.png
│
└── .github/
    └── workflows/
        ├── ci.yml                      Lint, validate, build on every push
        ├── cd.yml                      Full deploy and health checks on main
        └── validate.yml                End-to-end data flow validation
```

---

## Quick Start

### Prerequisites

- Docker Desktop with at least 4 GB RAM allocated
- Git
- WSL2 if running on Windows

### 1. Clone and configure

```bash
git clone https://github.com/<your-username>/mini-healthcare-platform.git
cd mini-healthcare-platform
cp .env.example .env
```

### 2. Start the platform

```bash
docker compose up -d --build
```

This starts all services, creates the PostgreSQL schema, creates the MinIO buckets, and initialises Airflow. First run takes approximately three minutes.

### 3. Verify services are healthy

```bash
docker compose ps
```

All services should show as healthy or running before proceeding.

### 4. Service URLs

| Service        | URL                   | Credentials                         |
|----------------|-----------------------|--------------------------------------|
| Airflow        | http://localhost:8080 | admin / admin                        |
| MinIO Console  | http://localhost:9001 | minioadmin / minioadmin123           |
| Metabase       | http://localhost:3000 | admin@healthcare.local / admin1234! |
| PostgreSQL     | localhost:5432        | ecom / ecom_secret  (db: ecomdb)    |

---

## Running the Pipeline

### Step 1 — Generate data

```bash
docker compose exec airflow-scheduler \
  python /opt/airflow/src/data_generator/main.py
```

Produces five CSV files in `data/raw/` with the following volumes and intentional quality issues:

| File           | Records | Quality Issues Injected                                               |
|----------------|---------|------------------------------------------------------------------------|
| patients.csv   | 5,000   | Missing gender, duplicate IDs, birth date after registration date      |
| visits.csv     | 15,000  | Invalid status values, missing department, end before start timestamp  |
| admissions.csv | ~3,000  | Discharge before admit, references to non-existent visits              |
| treatments.csv | ~25,000 | Negative costs, missing treatment codes, orphaned visit references     |
| billing.csv    | ~13,000 | Negative amounts, insurance exceeding total, orphaned visit references |

### Step 2 — Run the three DAGs in order

```bash
# DAG 1: Upload raw CSVs to MinIO bronze
docker compose exec airflow-scheduler \
  airflow dags trigger healthcare_bronze_ingestion_v1

# DAG 2: Clean and validate, write clean rows to MinIO silver
docker compose exec airflow-scheduler \
  airflow dags trigger healthcare_silver_pipeline_v1

# DAG 3: Load to PostgreSQL and build mart tables
docker compose exec airflow-scheduler \
  airflow dags trigger healthcare_gold_pipeline_v1
```

All three DAGs can also be triggered from the Airflow UI at http://localhost:8080.

### Step 3 — Verify data loaded

```bash
docker compose exec postgres psql -U ecom -d ecomdb -c "
SELECT 'patients'             AS table_name, COUNT(*) AS rows FROM patients
UNION ALL SELECT 'visits',                   COUNT(*) FROM visits
UNION ALL SELECT 'admissions',               COUNT(*) FROM admissions
UNION ALL SELECT 'treatments',               COUNT(*) FROM treatments
UNION ALL SELECT 'billing',                  COUNT(*) FROM billing
UNION ALL SELECT 'mart_daily_kpis',          COUNT(*) FROM mart_daily_kpis
UNION ALL SELECT 'mart_department_stats',    COUNT(*) FROM mart_department_stats
UNION ALL SELECT 'mart_patient_stats',       COUNT(*) FROM mart_patient_stats;
"
```

---

## Data Quality and Validation

The silver pipeline applies the following checks to every dataset. Rows that fail any check are written to `healthcare-silver/rejected/{dataset}/` with a `_reject_reason` column that documents exactly which rule was violated. Only rows that pass all checks proceed to PostgreSQL.

| Validation Rule                                       | Applied To                              |
|-------------------------------------------------------|-----------------------------------------|
| Required fields must not be null or blank             | All datasets                            |
| Gender must be M, F, Other, or Unknown                | patients                                |
| Visit type must be Outpatient, Emergency, or Inpatient| visits                                  |
| Status must be Completed, Cancelled, or No-show       | visits                                  |
| Room type must be a recognised value                  | admissions                              |
| visit_end_ts must be after visit_start_ts             | visits                                  |
| discharge_ts must be after admit_ts                   | admissions                              |
| treatment_cost must be zero or greater                | treatments                              |
| total_amount must be zero or greater                  | billing                                 |
| insurance_amount must not exceed total_amount         | billing                                 |
| Paid bills must have a payment timestamp              | billing                                 |
| patient_id must exist in validated patients file      | visits (referential integrity)          |
| visit_id must exist in validated visits file          | admissions, treatments, billing         |

All pipeline runs are idempotent. Re-triggering any DAG for the same partition date will overwrite existing files and upsert existing rows without creating duplicates.

---

## Database Schema

### Core Tables

```
patients        patient demographics                         PK: patient_id
visits          hospital visits                              PK: visit_id      FK -> patients
admissions      inpatient stays with computed LOS days       PK: admission_id  FK -> visits
treatments      procedures performed during a visit          PK: treatment_id  FK -> visits
billing         payment records per visit                    PK: bill_id       FK -> visits
```

### Mart Tables

```
mart_daily_kpis
    One row per calendar day.
    Columns: total_visits, completed_visits, total_admissions,
             avg_length_of_stay_days, total_revenue, insurance_revenue,
             patient_revenue, paid_bills, pending_bills,
             payment_rate_pct, new_patients

mart_department_stats
    One row per department per day.
    Columns: total_visits, emergency/inpatient/outpatient counts,
             avg_visit_duration_hrs, total_admissions,
             avg_los_days, total_revenue

mart_patient_stats
    One row per patient.
    Columns: age_years, total_visits, first/last_visit_date,
             total_billed, total_paid, is_returning
```

---

## Dashboards

To connect Metabase to the warehouse, use these settings when adding a database:

- Host: `postgres` | Port: `5432` | Database: `ecomdb`
- Username: `ecom` | Password: `ecom_secret`

| Dashboard             | What It Shows                                                                           |
|-----------------------|------------------------------------------------------------------------------------------|
| Executive Overview    | Total visits, revenue, and payment rate as headline numbers. Daily visit and revenue trends. Admissions rate over time. |
| Department Operations | Visit volume per department by type (emergency, inpatient, outpatient). Average visit duration and length of stay. Department load trend over time. |
| Financial Summary     | Insurance versus patient revenue split. Bill payment status breakdown. Monthly revenue by source. Top 10 treatments by revenue. Monthly payment rate trend. |
| Patient Analytics     | New versus returning patient counts and rate. Patients by city. Age group distribution. Gender split. Visit frequency distribution. |

---

## CI/CD Pipeline

| Workflow      | Trigger            | What It Does                                                                                                                      |
|---------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| ci.yml        | Every push and PR  | Lints Python source and DAGs, validates docker-compose syntax, builds all custom Docker images, tests that all DAGs import cleanly |
| cd.yml        | Push to main       | Builds the full stack, starts all services, runs health checks against all four service endpoints                                  |
| validate.yml  | After CD completes | Generates data, triggers all three DAGs in sequence, asserts MinIO bronze and silver contain files, asserts all eight PostgreSQL tables have data, runs data quality sanity checks on the gold layer, confirms Metabase API is responding |

---

## Stopping the Platform

```bash
# Stop all containers, keep data volumes intact
docker compose down

# Full teardown including all data volumes
docker compose down -v
```

---

## Troubleshooting

**Airflow webserver not becoming healthy**
```bash
docker compose logs airflow-init
docker compose restart airflow-init
```

**MinIO buckets not created**
```bash
docker compose logs minio-init
docker compose up minio-init
```

**Schema not applied or tables missing on Windows**
```powershell
docker compose cp sql/schema.sql postgres:/tmp/schema.sql
docker compose exec postgres psql -U ecom -d ecomdb -f /tmp/schema.sql
```

**Silver pipeline reporting zero rows processed**
Confirm that `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` are set in your `.env` file. These are separate from `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` and must both be present for the Python MinIO client to authenticate correctly.
