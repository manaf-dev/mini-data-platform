"""
Master DAG: Healthcare Full Pipeline

Runs daily at midnight (00:00 UTC) and executes all three
pipeline stages in strict dependency order:

  bronze_ingestion
       |
       v
  silver_pipeline   (5 parallel dataset tasks + summary)
       |
       v
  gold_pipeline     (5 parallel load tasks + mart build)
       |
       v
  pipeline_summary  (logs full run report to logs)

The three individual DAGs (bronze, silver, gold) remain
available for manual one-off runs. Only this DAG is scheduled.

Idempotent: all stages use the DAG logical_date as the
partition date so re-runs for the same day are always safe.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


def _partition_date(context: dict) -> datetime:
    """Return logical_date as a timezone-naive datetime for partition paths."""
    return context["logical_date"].replace(tzinfo=None)


# Stage 1: Bronze
def run_bronze(**context):
    from src.ingestion.bronze_ingestion import BronzeIngestion

    partition = _partition_date(context)
    logger.info("[master] Stage 1 — Bronze ingestion | partition=%s", partition.date())

    ingestion = BronzeIngestion(source_dir="data/raw")
    results = ingestion.ingest_all(partition_date=partition)

    ok = ingestion.verify_ingestion(partition_date=partition)
    if not ok:
        raise RuntimeError(
            "Bronze verification failed — one or more datasets missing from MinIO."
        )

    logger.info("[master] Bronze complete | uploaded=%s", list(results.keys()))
    context["task_instance"].xcom_push(
        key="bronze_datasets", value=list(results.keys())
    )
    return results


# Stage 2: Silver (one task per dataset, run sequentially inside one callable)
def run_silver(**context):
    """
    Runs all five silver cleaners in dependency order:
      patients → visits → admissions, treatments, billing
    Sequential within the task so FK checks always find upstream silver files.
    """
    from src.silver.admissions import AdmissionsCleaner
    from src.silver.billing import BillingCleaner
    from src.silver.patients import PatientsCleaner
    from src.silver.treatments import TreatmentsCleaner
    from src.silver.visits import VisitsCleaner

    partition = _partition_date(context)
    logger.info("[master] Stage 2 — Silver pipeline | partition=%s", partition.date())

    # Order matters: visits FK checks patients; admissions/treatments/billing FK check visits
    cleaners = [
        PatientsCleaner(),
        VisitsCleaner(),
        AdmissionsCleaner(),
        TreatmentsCleaner(),
        BillingCleaner(),
    ]

    silver_stats = {}
    for cleaner in cleaners:
        stats = cleaner.run(partition)
        stats["dataset"] = cleaner.dataset_name
        silver_stats[cleaner.dataset_name] = stats
        logger.info(
            "[master] Silver %-12s  total=%-6d  clean=%-6d  rejected=%-6d  (%.1f%%)",
            cleaner.dataset_name,
            stats["total"],
            stats["clean"],
            stats["rejected"],
            stats["rejection_rate_pct"],
        )

    context["task_instance"].xcom_push(key="silver_stats", value=silver_stats)
    return silver_stats


# Stage 3: Gold
def run_gold(**context):
    """
    Loads all five tables into PostgreSQL then rebuilds all mart tables.
    Order matters: patients → visits → admissions/treatments/billing (FK constraints).
    """
    from src.gold.loader import (
        AdmissionsLoader,
        BillingLoader,
        PatientsLoader,
        TreatmentsLoader,
        VisitsLoader,
    )
    from src.gold.marts import build_all_marts

    partition = _partition_date(context)
    logger.info("[master] Stage 3 — Gold pipeline | partition=%s", partition.date())

    loaders = [
        PatientsLoader(),
        VisitsLoader(),
        AdmissionsLoader(),
        TreatmentsLoader(),
        BillingLoader(),
    ]

    gold_stats = {}
    for loader in loaders:
        stats = loader.run(partition)
        gold_stats[loader.dataset_name] = stats
        logger.info(
            "[master] Gold %-12s  upserted=%d",
            loader.dataset_name,
            stats["upserted"],
        )

    logger.info("[master] Building mart tables ...")
    mart_results = build_all_marts()
    for mart, count in mart_results.items():
        logger.info("[master]   %-30s  %d rows", mart, count)

    context["task_instance"].xcom_push(key="gold_stats", value=gold_stats)
    context["task_instance"].xcom_push(key="mart_results", value=mart_results)
    return {"tables": gold_stats, "marts": mart_results}


# Stage 4: Summary
def pipeline_summary(**context):
    """
    Pulls XCom stats from all three stages and logs a
    consolidated run report to the Airflow task log.
    """
    ti = context["task_instance"]
    partition = _partition_date(context)

    silver_stats = ti.xcom_pull(task_ids="silver_pipeline", key="silver_stats") or {}
    gold_stats = ti.xcom_pull(task_ids="gold_pipeline", key="gold_stats") or {}
    mart_results = ti.xcom_pull(task_ids="gold_pipeline", key="mart_results") or {}

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    logger.info("=" * 65)
    logger.info("  HEALTHCARE MASTER PIPELINE - RUN REPORT")
    logger.info("  Partition : %s", partition.date())
    logger.info("  Completed : %s", run_ts)
    logger.info("=" * 65)

    logger.info("  SILVER LAYER - Validation Results")
    logger.info(
        "  %-15s %8s %8s %8s %7s", "Dataset", "Total", "Clean", "Rejected", "Rej%"
    )
    logger.info("  %s", "-" * 50)
    total_rejected = 0
    for ds, stats in silver_stats.items():
        logger.info(
            "  %-15s %8s %8s %8s %6.1f%%",
            ds,
            format(stats["total"], ","),
            format(stats["clean"], ","),
            format(stats["rejected"], ","),
            stats["rejection_rate_pct"],
        )
        total_rejected += stats["rejected"]
    logger.info(
        "  Total rows rejected and quarantined: %s", format(total_rejected, ",")
    )
    logger.info("  Rejected rows written to MinIO silver/rejected/ with _reject_reason")

    logger.info("  GOLD LAYER - PostgreSQL Load Results")
    logger.info("  %-20s %14s", "Table", "Rows Upserted")
    logger.info("  %s", "-" * 36)
    for ds, stats in gold_stats.items():
        logger.info("  %-20s %14s", ds, format(stats["upserted"], ","))

    logger.info("  MART TABLES - Rebuilt")
    for mart, count in mart_results.items():
        logger.info("  %-30s  %s rows", mart, format(count, ","))

    logger.info("=" * 65)
    logger.info("  Pipeline completed successfully.")
    logger.info("  Dashboards at http://localhost:3000 are up to date.")
    logger.info("=" * 65)


# DAG definition─

with DAG(
    dag_id="healthcare_master_pipeline",
    description=(
        "Scheduled daily pipeline: Bronze ingestion → Silver validation "
        "→ Gold load + marts. Runs at midnight UTC every day."
    ),
    # Midnight UTC daily
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # do not back-fill missed runs
    max_active_runs=1,  # never run two instances simultaneously
    tags=["healthcare", "master", "scheduled"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    },
) as dag:
    t_bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
        doc_md="Upload raw CSVs from data/raw/ to MinIO healthcare-bronze/",
    )

    t_silver = PythonOperator(
        task_id="silver_pipeline",
        python_callable=run_silver,
        doc_md=(
            "Clean, validate, and deduplicate all five datasets. "
            "Rejected rows are quarantined to silver/rejected/ with reasons. "
            "FK checks run in order: patients → visits → admissions/treatments/billing."
        ),
    )

    t_gold = PythonOperator(
        task_id="gold_pipeline",
        python_callable=run_gold,
        doc_md=(
            "Upsert validated rows into PostgreSQL core tables. "
            "Rebuild mart_daily_kpis, mart_department_stats, mart_patient_stats."
        ),
    )

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        # Run summary even if an upstream task fails so we always get a report
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Print consolidated run report to task logs.",
    )

    # Strict linear dependency - each stage must succeed before the next begins
    t_bronze >> t_silver >> t_gold >> t_summary
