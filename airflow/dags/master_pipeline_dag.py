"""Master DAG with task-level visibility across bronze, silver, and gold."""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

SILVER_DATASETS = ["patients", "visits", "admissions", "treatments", "billing"]
GOLD_DATASETS = ["patients", "visits", "admissions", "treatments", "billing"]


def _partition_date(context: dict) -> datetime:
    return context["data_interval_start"].replace(tzinfo=None)


def run_data_generator(**context):
    from src.data_generator.generator import main as generate_data

    partition = _partition_date(context).date()
    logger.info("[master] Data generation start | partition=%s", partition)
    generate_data(for_date=partition)
    logger.info("[master] Data generation done | partition=%s", partition)


def run_bronze_ingestion(**context):
    from src.ingestion.bronze_ingestion import BronzeIngestion

    partition = _partition_date(context)
    logger.info("[master] Bronze ingestion start | partition=%s", partition.date())

    ingestion = BronzeIngestion()
    results = ingestion.ingest_all(partition_date=partition)
    ok = ingestion.verify_ingestion(partition_date=partition)

    if not ok:
        raise RuntimeError("Bronze verification failed: one or more datasets are missing.")

    logger.info("[master] Bronze ingestion done | uploaded=%s", list(results.keys()))
    context["task_instance"].xcom_push(key="bronze_datasets", value=list(results.keys()))


def run_patients_silver(**context):
    from src.silver.patients import PatientsCleaner

    stats = PatientsCleaner().run(_partition_date(context))
    stats["dataset"] = "patients"
    logger.info("[master] silver patients stats: %s", stats)
    context["task_instance"].xcom_push(key="patients_stats", value=stats)


def run_visits_silver(**context):
    from src.silver.visits import VisitsCleaner

    stats = VisitsCleaner().run(_partition_date(context))
    stats["dataset"] = "visits"
    logger.info("[master] silver visits stats: %s", stats)
    context["task_instance"].xcom_push(key="visits_stats", value=stats)


def run_admissions_silver(**context):
    from src.silver.admissions import AdmissionsCleaner

    stats = AdmissionsCleaner().run(_partition_date(context))
    stats["dataset"] = "admissions"
    logger.info("[master] silver admissions stats: %s", stats)
    context["task_instance"].xcom_push(key="admissions_stats", value=stats)


def run_treatments_silver(**context):
    from src.silver.treatments import TreatmentsCleaner

    stats = TreatmentsCleaner().run(_partition_date(context))
    stats["dataset"] = "treatments"
    logger.info("[master] silver treatments stats: %s", stats)
    context["task_instance"].xcom_push(key="treatments_stats", value=stats)


def run_billing_silver(**context):
    from src.silver.billing import BillingCleaner

    stats = BillingCleaner().run(_partition_date(context))
    stats["dataset"] = "billing"
    logger.info("[master] silver billing stats: %s", stats)
    context["task_instance"].xcom_push(key="billing_stats", value=stats)


def log_silver_summary(**context):
    ti = context["task_instance"]

    logger.info("=" * 65)
    logger.info("  SILVER PIPELINE SUMMARY  |  partition=%s", _partition_date(context).date())
    logger.info("=" * 65)
    logger.info("  %-15s %8s %8s %8s %7s", "Dataset", "Total", "Clean", "Rejected", "Rej%")
    logger.info("  %s", "-" * 50)

    for dataset in SILVER_DATASETS:
        stats = ti.xcom_pull(key=f"{dataset}_stats", task_ids=f"silver_{dataset}")
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


def load_patients_gold(**context):
    from src.gold.loader import PatientsLoader

    stats = PatientsLoader().run(_partition_date(context))
    logger.info("[master] gold patients stats: %s", stats)
    context["task_instance"].xcom_push(key="patients_gold", value=stats)


def load_visits_gold(**context):
    from src.gold.loader import VisitsLoader

    stats = VisitsLoader().run(_partition_date(context))
    logger.info("[master] gold visits stats: %s", stats)
    context["task_instance"].xcom_push(key="visits_gold", value=stats)


def load_admissions_gold(**context):
    from src.gold.loader import AdmissionsLoader

    stats = AdmissionsLoader().run(_partition_date(context))
    logger.info("[master] gold admissions stats: %s", stats)
    context["task_instance"].xcom_push(key="admissions_gold", value=stats)


def load_treatments_gold(**context):
    from src.gold.loader import TreatmentsLoader

    stats = TreatmentsLoader().run(_partition_date(context))
    logger.info("[master] gold treatments stats: %s", stats)
    context["task_instance"].xcom_push(key="treatments_gold", value=stats)


def load_billing_gold(**context):
    from src.gold.loader import BillingLoader

    stats = BillingLoader().run(_partition_date(context))
    logger.info("[master] gold billing stats: %s", stats)
    context["task_instance"].xcom_push(key="billing_gold", value=stats)


def run_build_marts(**context):
    from src.gold.marts import build_all_marts

    results = build_all_marts()
    logger.info("[master] mart build results: %s", results)
    context["task_instance"].xcom_push(key="mart_results", value=results)


def log_gold_summary(**context):
    ti = context["task_instance"]

    logger.info("=" * 55)
    logger.info("  GOLD PIPELINE SUMMARY  |  partition=%s", _partition_date(context).date())
    logger.info("=" * 55)
    logger.info("  %-15s %14s", "Dataset", "Rows Upserted")
    logger.info("  %s", "-" * 35)

    for dataset in GOLD_DATASETS:
        stats = ti.xcom_pull(key=f"{dataset}_gold", task_ids=f"load_{dataset}")
        if stats:
            logger.info("  %-15s %14s", dataset, format(stats["upserted"], ","))

    logger.info("  Marts rebuilt:")
    mart_results = ti.xcom_pull(key="mart_results", task_ids="build_marts")
    if mart_results:
        for mart, count in mart_results.items():
            logger.info("    - %-28s %8s rows", mart, format(count, ","))

    logger.info("=" * 55)


def pipeline_summary(**context):
    ti = context["task_instance"]
    partition = _partition_date(context).date()
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    logger.info("=" * 65)
    logger.info("  HEALTHCARE MASTER PIPELINE - RUN REPORT")
    logger.info("  Partition : %s", partition)
    logger.info("  Completed : %s", run_ts)
    logger.info("=" * 65)

    logger.info("  SILVER LAYER")
    total_rejected = 0
    for dataset in SILVER_DATASETS:
        stats = ti.xcom_pull(key=f"{dataset}_stats", task_ids=f"silver_{dataset}")
        if stats:
            total_rejected += stats["rejected"]
            logger.info(
                "  %-15s total=%s clean=%s rejected=%s",
                dataset,
                format(stats["total"], ","),
                format(stats["clean"], ","),
                format(stats["rejected"], ","),
            )
    logger.info("  Total rejected rows: %s", format(total_rejected, ","))

    logger.info("  GOLD LAYER")
    for dataset in GOLD_DATASETS:
        stats = ti.xcom_pull(key=f"{dataset}_gold", task_ids=f"load_{dataset}")
        if stats:
            logger.info("  %-15s upserted=%s", dataset, format(stats["upserted"], ","))

    mart_results = ti.xcom_pull(key="mart_results", task_ids="build_marts") or {}
    logger.info("  MART TABLES")
    for mart, count in mart_results.items():
        logger.info("  %-30s %s rows", mart, format(count, ","))

    logger.info("=" * 65)


with DAG(
    dag_id="healthcare_master_pipeline",
    description="Daily end-to-end pipeline with task-level visibility for bronze, silver, and gold.",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "master", "scheduled"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
    },
) as dag:
    t_generate = PythonOperator(task_id="generate_raw_data", python_callable=run_data_generator)
    t_bronze = PythonOperator(task_id="bronze_ingestion", python_callable=run_bronze_ingestion)

    t_silver_patients = PythonOperator(task_id="silver_patients", python_callable=run_patients_silver)
    t_silver_visits = PythonOperator(task_id="silver_visits", python_callable=run_visits_silver)
    t_silver_admissions = PythonOperator(task_id="silver_admissions", python_callable=run_admissions_silver)
    t_silver_treatments = PythonOperator(task_id="silver_treatments", python_callable=run_treatments_silver)
    t_silver_billing = PythonOperator(task_id="silver_billing", python_callable=run_billing_silver)
    t_silver_summary = PythonOperator(task_id="silver_summary", python_callable=log_silver_summary)

    t_gold_patients = PythonOperator(task_id="load_patients", python_callable=load_patients_gold)
    t_gold_visits = PythonOperator(task_id="load_visits", python_callable=load_visits_gold)
    t_gold_admissions = PythonOperator(task_id="load_admissions", python_callable=load_admissions_gold)
    t_gold_treatments = PythonOperator(task_id="load_treatments", python_callable=load_treatments_gold)
    t_gold_billing = PythonOperator(task_id="load_billing", python_callable=load_billing_gold)
    t_build_marts = PythonOperator(task_id="build_marts", python_callable=run_build_marts)
    t_gold_summary = PythonOperator(task_id="gold_summary", python_callable=log_gold_summary)

    t_pipeline_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_generate >> t_bronze

    t_bronze >> t_silver_patients >> t_silver_visits
    t_silver_visits >> [t_silver_admissions, t_silver_treatments, t_silver_billing]
    [t_silver_admissions, t_silver_treatments, t_silver_billing] >> t_silver_summary

    t_silver_summary >> t_gold_patients >> t_gold_visits
    t_gold_visits >> [t_gold_admissions, t_gold_treatments, t_gold_billing]
    [t_gold_admissions, t_gold_treatments, t_gold_billing] >> t_build_marts >> t_gold_summary

    t_gold_summary >> t_pipeline_summary
