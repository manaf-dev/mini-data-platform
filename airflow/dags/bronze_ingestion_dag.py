import logging
import sys
from datetime import datetime
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, str(Path(__file__).parent.parent))
logger = logging.getLogger(__name__)


def _partition_date(**context):
    return context["logical_date"].replace(tzinfo=None).date()


def run_data_generator(**context):
    """Generate partitioned raw CSV files before bronze ingestion."""
    from src.data_generator.generator import main as generate_data

    partition = _partition_date(context)
    logger.info("Data generation start | partition=%s", partition)
    generate_data(for_date=partition)
    logger.info("Data generation done | partition=%s", partition)


def run_bronze_ingestion(**context):
    """
    Runs inside the Airflow container.
    """
    from src.ingestion.bronze_ingestion import BronzeIngestion

    # Partition date: use DAG logical date
    logical_date = _partition_date(context)
    logger.info("Bronze ingestion start | logical_date=%s", logical_date)

    ingestion = BronzeIngestion()
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
    data_generator = PythonOperator(
        task_id="generate_raw_data",
        python_callable=run_data_generator,
    )

    bronze_ingestion = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
    )

    data_generator >> bronze_ingestion
