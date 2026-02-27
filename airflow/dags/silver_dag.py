"""
Reads raw CSVs from MinIO bronze, cleans + validates each dataset,
writes clean rows to MinIO silver, and rejected rows to silver/rejected/.

"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


def _partition_date(context: dict) -> datetime:
    """Return the DAG logical date as a plain datetime (no timezone)."""
    return context["logical_date"].replace(tzinfo=None)


def run_patients_silver(**context):
    from src.silver.patients import PatientsCleaner

    stats = PatientsCleaner().run(_partition_date(context))
    stats["dataset"] = "patients"
    logger.info("patients silver stats: %s", stats)
    context["task_instance"].xcom_push(key="patients_stats", value=stats)


def run_visits_silver(**context):
    from src.silver.visits import VisitsCleaner

    stats = VisitsCleaner().run(_partition_date(context))
    stats["dataset"] = "visits"
    logger.info("visits silver stats: %s", stats)
    context["task_instance"].xcom_push(key="visits_stats", value=stats)


def run_admissions_silver(**context):
    from src.silver.admissions import AdmissionsCleaner

    stats = AdmissionsCleaner().run(_partition_date(context))
    stats["dataset"] = "admissions"
    logger.info("admissions silver stats: %s", stats)
    context["task_instance"].xcom_push(key="admissions_stats", value=stats)


def run_treatments_silver(**context):
    from src.silver.treatments import TreatmentsCleaner

    stats = TreatmentsCleaner().run(_partition_date(context))
    stats["dataset"] = "treatments"
    logger.info("treatments silver stats: %s", stats)
    context["task_instance"].xcom_push(key="treatments_stats", value=stats)


def run_billing_silver(**context):
    from src.silver.billing import BillingCleaner

    stats = BillingCleaner().run(_partition_date(context))
    stats["dataset"] = "billing"
    logger.info("billing silver stats: %s", stats)
    context["task_instance"].xcom_push(key="billing_stats", value=stats)


def log_silver_summary(**context):
    """Pull all stats from XCom and log a clean summary table."""
    ti = context["task_instance"]
    datasets = ["patients", "visits", "admissions", "treatments", "billing"]

    logger.info("=" * 65)
    logger.info(
        "  SILVER PIPELINE SUMMARY  |  partition=%s",
        _partition_date(context).date(),
    )
    logger.info("=" * 65)
    logger.info(
        "  %-15s %8s %8s %8s %7s", "Dataset", "Total", "Clean", "Rejected", "Rej%"
    )
    logger.info("  %s", "-" * 50)

    for ds in datasets:
        stats = ti.xcom_pull(key=f"{ds}_stats", task_ids=f"silver_{ds}")
        if stats:
            logger.info(
                "  %-15s %8s %8s %8s %6.1f%%",
                stats["dataset"],
                format(stats["total"], ","),
                format(stats["clean"], ","),
                format(stats["rejected"], ","),
                stats["rejection_rate_pct"],
            )
    logger.info("=" * 65)


# DAG definition

with DAG(
    dag_id="healthcare_silver_pipeline_v1",
    description="Bronze → Silver: clean, validate, reject bad rows",
    start_date=datetime(2026, 2, 24),
    schedule=None,  # Trigger manually or chain from bronze DAG
    catchup=False,
    tags=["healthcare", "silver"],
    default_args={
        "retries": 1,
        "owner": "data-engineering",
    },
) as dag:
    # Individual dataset tasks
    t_patients = PythonOperator(
        task_id="silver_patients",
        python_callable=run_patients_silver,
    )

    t_visits = PythonOperator(
        task_id="silver_visits",
        python_callable=run_visits_silver,
    )

    t_admissions = PythonOperator(
        task_id="silver_admissions",
        python_callable=run_admissions_silver,
    )

    t_treatments = PythonOperator(
        task_id="silver_treatments",
        python_callable=run_treatments_silver,
    )

    t_billing = PythonOperator(
        task_id="silver_billing",
        python_callable=run_billing_silver,
    )

    t_summary = PythonOperator(
        task_id="silver_summary",
        python_callable=log_silver_summary,
    )

    [t_patients, t_visits] >> t_admissions
    t_visits >> [t_treatments, t_billing]
    [t_admissions, t_treatments, t_billing] >> t_summary
