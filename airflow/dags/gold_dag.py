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

sys.path.insert(0, str(Path(__file__).parent.parent))
from airflow import DAG  # noqa: E402

logger = logging.getLogger(__name__)


def _partition_date(context: dict) -> datetime:
    return context["logical_date"].replace(tzinfo=None)


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

    print("\n" + "=" * 55)
    print(f"  GOLD PIPELINE SUMMARY  |  partition={_partition_date(context).date()}")
    print("=" * 55)
    print(f"  {'Dataset':<15} {'Rows Upserted':>14}")
    print("  " + "-" * 35)
    for ds in datasets:
        stats = ti.xcom_pull(key=f"{ds}_gold", task_ids=f"load_{ds}")
        if stats:
            print(f"  {stats['dataset']:<15} {stats['upserted']:>14,}")

    print("\n  Marts rebuilt:")
    mart_results = ti.xcom_pull(key="mart_results", task_ids="build_marts")
    if mart_results:
        for mart, count in mart_results.items():
            print(f"    • {mart:<28} {count:>8,} rows")
    print("=" * 55 + "\n")


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
