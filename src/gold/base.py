"""
Shared base for all gold-layer table loaders.

Design principles
 - Reads clean CSV from MinIO silver bucket
 - Loads into Postgres using UPSERT (ON CONFLICT DO UPDATE) — idempotent
 - Uses execute_values for batch inserts (fast, single round-trip)
 - Logs row counts at each stage
 - processed_at is always refreshed on upsert so you can audit reloads
"""

import io
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import pandas as pd
import psycopg2
from minio import Minio

logger = logging.getLogger(__name__)

SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "healthcare-silver")


def _pg_conn():
    """Return a live psycopg2 connection from env vars."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def _minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")

    return Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )


class GoldLoader(ABC):
    """Abstract base for loading one dataset from silver into Postgres."""

    dataset_name: str = ""

    def __init__(self) -> None:
        if not self.dataset_name:
            raise NotImplementedError("Subclass must set dataset_name")

    @abstractmethod
    def upsert(self, df: pd.DataFrame, cur: psycopg2.extensions.cursor) -> int:
        """
        Upsert DataFrame rows into the target Postgres table.
        Returns number of rows upserted.
        """

    def run(self, partition_date: datetime) -> dict:
        """
        Full gold pipeline for this dataset:
          MinIO silver read → upsert Postgres → return stats
        """
        logger.info(
            "[gold:%s] Load start | partition=%s",
            self.dataset_name,
            partition_date.date(),
        )

        df = self._read_silver(partition_date)
        if df is None or df.empty:
            logger.warning("[gold:%s] No silver data — skipping.", self.dataset_name)
            return {"dataset": self.dataset_name, "upserted": 0}

        logger.info("[gold:%s] Read %d rows from silver.", self.dataset_name, len(df))

        with _pg_conn() as conn:
            with conn.cursor() as cur:
                upserted = self.upsert(df, cur)
            conn.commit()

        logger.info(
            "[gold:%s] Upserted %d rows into Postgres.", self.dataset_name, upserted
        )
        return {"dataset": self.dataset_name, "upserted": upserted}

    def _read_silver(self, partition_date: datetime) -> Optional[pd.DataFrame]:
        d = partition_date
        key = (
            f"{self.dataset_name}/"
            f"{d.year:04d}/{d.month:02d}/{d.day:02d}/"
            f"{self.dataset_name}.csv"
        )
        client = _minio_client()
        try:
            resp = client.get_object(SILVER_BUCKET, key)
            df = pd.read_csv(io.BytesIO(resp.read()))
            resp.close()
            resp.release_conn()
            return df
        except Exception as exc:
            logger.error(
                "[gold:%s] Cannot read silver key '%s': %s",
                self.dataset_name,
                key,
                exc,
            )
            return None
