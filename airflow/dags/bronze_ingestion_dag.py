import logging
import sys
from datetime import datetime
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)


def run_bronze_ingestion(**context):
    """
    Runs inside the Airflow container.
    Assumes:
      - ./data is available (we’ll mount it below)
      - src package imports work (ensure __init__.py files exist)
      - MINIO_* env vars available in Airflow container
    """
    from src.ingestion.bronze_ingestion import BronzeIngestion

    # Partition date: use DAG logical date
    logical_date = context["logical_date"].to_datetime_string()
    logger.info("Bronze ingestion start | logical_date=%s", logical_date)

    ingestion = BronzeIngestion(source_dir="data/raw")
    results = ingestion.ingest_all(partition_date=context["logical_date"])
    ok = ingestion.verify_ingestion(partition_date=context["logical_date"])

    if not ok:
        raise RuntimeError("Bronze verification failed (some datasets missing).")

    logger.info("Bronze ingestion done | uploaded=%s", results)


with DAG(
    dag_id="healthcare_bronze_ingestion_v1",
    start_date=datetime(2026, 2, 24),
    schedule=None,  # manual trigger for now
    catchup=False,
    tags=["healthcare", "bronze"],
) as dag:
    bronze_ingestion = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
    )

    bronze_ingestion
