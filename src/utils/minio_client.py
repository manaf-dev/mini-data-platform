"""MinIO client wrapper for object storage operations."""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOClient:
    """Wrapper for MinIO operations with healthcare data platform conventions."""

    def __init__(
        self,
        endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        secure: bool = False,
    ):
        """
        Initialize MinIO client.

        Args:
            endpoint: MinIO server endpoint (default from env)
            access_key: Access key (default from env)
            secret_key: Secret key (default from env)
            secure: Use HTTPS (default False for local dev)
        """
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin123")

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure,
        )

        logger.info(f"MinIO client initialized for endpoint: {self.endpoint}")

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            else:
                logger.debug(f"Bucket already exists: {bucket_name}")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: str = "application/octet-stream",
    ) -> bool:
        """
        Upload a file to MinIO.

        Args:
            bucket_name: Target bucket
            object_name: Object path in bucket
            file_path: Local file path
            content_type: MIME type

        Returns:
            True if successful
        """
        try:
            self.ensure_bucket_exists(bucket_name)

            self.client.fput_object(
                bucket_name, object_name, file_path, content_type=content_type
            )

            logger.info(f"Uploaded {file_path} to {bucket_name}/{object_name}")
            return True

        except S3Error as e:
            logger.error(f"Error uploading file: {e}")
            return False

    def upload_csv_to_bronze(
        self,
        file_path: str,
        dataset_name: str,
        partition_date: Optional[datetime] = None,
    ) -> str:
        """
        Upload CSV file to bronze layer with date partitioning.

        Args:
            file_path: Local CSV file path
            dataset_name: Dataset name (e.g., 'patients', 'visits')
            partition_date: Date for partitioning (default: today)

        Returns:
            Object path in MinIO
        """
        if partition_date is None:
            partition_date = datetime.now()

        bucket_name = os.getenv("MINIO_BUCKET_BRONZE", "healthcare-bronze")

        # Create partitioned path: dataset/YYYY/MM/DD/filename.csv
        object_path = (
            f"{dataset_name}/"
            f"{partition_date.year:04d}/"
            f"{partition_date.month:02d}/"
            f"{partition_date.day:02d}/"
            f"{Path(file_path).name}"
        )

        success = self.upload_file(
            bucket_name=bucket_name,
            object_name=object_path,
            file_path=file_path,
            content_type="text/csv",
        )

        if success:
            return f"s3://{bucket_name}/{object_path}"
        else:
            raise Exception(f"Failed to upload {file_path} to bronze layer")

    def list_objects(
        self, bucket_name: str, prefix: str = "", recursive: bool = True
    ) -> List[str]:
        """
        List objects in bucket.

        Args:
            bucket_name: Bucket to list
            prefix: Object prefix filter
            recursive: Recursive listing

        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                bucket_name, prefix=prefix, recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> bool:
        """
        Download file from MinIO.

        Args:
            bucket_name: Source bucket
            object_name: Object path in bucket
            file_path: Local destination path

        Returns:
            True if successful
        """
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            logger.info(f"Downloaded {bucket_name}/{object_name} to {file_path}")
            return True
        except S3Error as e:
            logger.error(f"Error downloading file: {e}")
            return False

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """Check if object exists in bucket."""
        try:
            self.client.stat_object(bucket_name, object_name)
            return True
        except S3Error:
            return False
