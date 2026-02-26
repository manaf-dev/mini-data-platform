"""
Airflow DAG: Healthcare Gold Pipeline

Loads clean silver CSVs into PostgreSQL, then builds mart tables.

Task order:
  load_patients
  load_visits        (after patients — FK dependency)
  load_admissions  ──┐
  load_treatments    ├── (all after visits — FK dependency)
  load_billing     ──┘
  build_marts        (after all loads complete)

Idempotent: yes — all inserts use ON CONFLICT DO UPDATE
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
    return context["data_interval_start"].replace(tzinfo=None)


def load_patients(**context):
    from src.gold.loader import PatientsLoader

    stats = PatientsLoader().run(_partition_date(context))
    logger.info("patients gold stats: %s", stats)
    context["task_instance"].xcom_push(key="patients_gold", value=stats)


def load_visits(**context):
    from src.gold.loader import VisitsLoader

    stats = VisitsLoader().run(_partition_date(context))
    logger.info("visits gold stats: %s", stats)
    context["task_instance"].xcom_push(key="visits_gold", value=stats)


def load_admissions(**context):
    from src.gold.loader import AdmissionsLoader

    stats = AdmissionsLoader().run(_partition_date(context))
    logger.info("admissions gold stats: %s", stats)
    context["task_instance"].xcom_push(key="admissions_gold", value=stats)


def load_treatments(**context):
    from src.gold.loader import TreatmentsLoader

    stats = TreatmentsLoader().run(_partition_date(context))
    logger.info("treatments gold stats: %s", stats)
    context["task_instance"].xcom_push(key="treatments_gold", value=stats)


def load_billing(**context):
    from src.gold.loader import BillingLoader

    stats = BillingLoader().run(_partition_date(context))
    logger.info("billing gold stats: %s", stats)
    context["task_instance"].xcom_push(key="billing_gold", value=stats)


def run_build_marts(**context):
    from src.gold.marts import build_all_marts

    results = build_all_marts()
    logger.info("Mart build results: %s", results)
    context["task_instance"].xcom_push(key="mart_results", value=results)


def log_gold_summary(**context):
    ti = context["task_instance"]
    datasets = ["patients", "visits", "admissions", "treatments", "billing"]

    logger.info("=" * 55)
    logger.info(
        "  GOLD PIPELINE SUMMARY  |  partition=%s",
        _partition_date(context).date(),
    )
    logger.info("=" * 55)
    logger.info("  %-15s %14s", "Dataset", "Rows Upserted")
    logger.info("  %s", "-" * 35)
    for ds in datasets:
        stats = ti.xcom_pull(key=f"{ds}_gold", task_ids=f"load_{ds}")
        if stats:
            logger.info(
                "  %-15s %14s",
                stats["dataset"],
                format(stats["upserted"], ","),
            )

    logger.info("  Marts rebuilt:")
    mart_results = ti.xcom_pull(key="mart_results", task_ids="build_marts")
    if mart_results:
        for mart, count in mart_results.items():
            logger.info("    - %-28s %8s rows", mart, format(count, ","))
    logger.info("=" * 55)


# DAG

with DAG(
    dag_id="healthcare_gold_pipeline_v1",
    description="Silver → Gold: load Postgres tables + build marts",
    start_date=datetime(2026, 2, 24),
    schedule=None,
    catchup=False,
    tags=["healthcare", "gold"],
    default_args={
        "retries": 1,
        "owner": "data-engineering",
    },
) as dag:
    t_patients = PythonOperator(task_id="load_patients", python_callable=load_patients)
    t_visits = PythonOperator(task_id="load_visits", python_callable=load_visits)
    t_admissions = PythonOperator(
        task_id="load_admissions", python_callable=load_admissions
    )
    t_treatments = PythonOperator(
        task_id="load_treatments", python_callable=load_treatments
    )
    t_billing = PythonOperator(task_id="load_billing", python_callable=load_billing)
    t_marts = PythonOperator(task_id="build_marts", python_callable=run_build_marts)
    t_summary = PythonOperator(task_id="gold_summary", python_callable=log_gold_summary)

    # patients must load before visits (FK)
    # visits must load before admissions/treatments/billing (FK)
    (
        t_patients
        >> t_visits
        >> [t_admissions, t_treatments, t_billing]
        >> t_marts
        >> t_summary
    )
