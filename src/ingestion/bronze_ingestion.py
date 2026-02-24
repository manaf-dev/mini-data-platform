"""Bronze layer ingestion - upload raw CSV files to MinIO."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.minio_client import MinIOClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BronzeIngestion:
    """Handle ingestion of raw CSV files to bronze layer."""

    def __init__(self, source_dir: str = "data/raw"):
        """
        Initialize bronze ingestion.

        Args:
            source_dir: Directory containing raw CSV files
        """
        self.source_dir = Path(source_dir)
        self.minio_client = MinIOClient()

        # Dataset configuration
        self.datasets = ["patients", "visits", "admissions", "treatments", "billing"]

    def ingest_all(self, partition_date: datetime = None) -> Dict[str, str]:
        """
        Ingest all CSV files to bronze layer.

        Args:
            partition_date: Date for partitioning (default: today)

        Returns:
            Dictionary mapping dataset names to MinIO paths
        """
        if partition_date is None:
            partition_date = datetime.now()

        logger.info(
            f"Starting bronze layer ingestion for date: {partition_date.date()}"
        )

        results = {}

        for dataset in self.datasets:
            csv_file = self.source_dir / f"{dataset}.csv"

            if not csv_file.exists():
                logger.warning(f"CSV file not found: {csv_file}")
                continue

            try:
                logger.info(f"Ingesting {dataset}...")

                object_path = self.minio_client.upload_csv_to_bronze(
                    file_path=str(csv_file),
                    dataset_name=dataset,
                    partition_date=partition_date,
                )

                results[dataset] = object_path
                logger.info(f"Successfully ingested {dataset} to {object_path}")

            except Exception as e:
                logger.error(f"Failed to ingest {dataset}: {e}")
                raise

        logger.info(f"Bronze ingestion complete. Ingested {len(results)} datasets.")
        return results

    def verify_ingestion(self, partition_date: datetime = None) -> bool:
        """
        Verify all datasets were ingested successfully.

        Args:
            partition_date: Date to verify (default: today)

        Returns:
            True if all datasets present in bronze layer
        """
        if partition_date is None:
            partition_date = datetime.now()

        bucket_name = "healthcare-bronze"
        all_present = True

        logger.info("Verifying bronze layer ingestion...")

        for dataset in self.datasets:
            prefix = (
                f"{dataset}/"
                f"{partition_date.year:04d}/"
                f"{partition_date.month:02d}/"
                f"{partition_date.day:02d}/"
            )

            objects = self.minio_client.list_objects(
                bucket_name=bucket_name, prefix=prefix, recursive=True
            )

            if objects:
                logger.info(f"✓ {dataset}: {len(objects)} file(s) found")
            else:
                logger.error(f"✗ {dataset}: No files found")
                all_present = False

        return all_present


def main():
    """Main execution function."""
    ingestion = BronzeIngestion(source_dir="data/raw")

    # Ingest all datasets
    results = ingestion.ingest_all()

    # Print summary
    print("\n" + "=" * 60)
    print("BRONZE LAYER INGESTION SUMMARY")
    print("=" * 60)
    for dataset, path in results.items():
        print(f"{dataset:15s}: {path}")
    print("=" * 60)

    # Verify ingestion
    if ingestion.verify_ingestion():
        print("\n✓ All datasets successfully ingested to bronze layer")
        return 0
    else:
        print("\n✗ Some datasets missing from bronze layer")
        return 1


if __name__ == "__main__":
    sys.exit(main())
