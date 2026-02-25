"""
Base class for all silver-layer cleaners.

Every dataset cleaner inherits from SilverCleaner and implements:
  - clean(df) cleaned DataFrame
  - validate(df) (valid_df, rejected_df)

"""

import io
import logging
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.minio_client import MinIOClient

logger = logging.getLogger(__name__)

BRONZE_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "healthcare-bronze")
SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "healthcare-silver")


class SilverCleaner(ABC):
    """Abstract base for all silver-layer dataset cleaners."""

    # Subclasses must declare their dataset name, e.g. "patients"
    dataset_name: str = ""

    def __init__(self) -> None:
        if not self.dataset_name:
            raise NotImplementedError("Subclass must set dataset_name")
        self.minio = MinIOClient()
        self.minio.ensure_bucket_exists(SILVER_BUCKET)

    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply type casting and normalisation to raw DataFrame.
        Must NOT drop rows — just fix/cast/normalise values.
        Rows with unfixable issues get a populated '_reject_reason' column.
        """

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split cleaned DataFrame into (valid, rejected).
        valid   → rows that pass all rules
        rejected → rows that failed at least one rule (with '_reject_reason')
        """

    def run(self, partition_date: datetime) -> dict:
        logger.info(
            "[%s] Silver pipeline start | partition=%s",
            self.dataset_name,
            partition_date.date(),
        )

        raw_df = self._read_bronze(partition_date)
        if raw_df is None or raw_df.empty:
            logger.warning("[%s] No data in bronze — skipping.", self.dataset_name)
            return self._stats(partition_date, 0, 0, 0)

        total = len(raw_df)
        logger.info("[%s] Loaded %d rows from bronze.", self.dataset_name, total)

        # Step 1 — clean (cast / normalise, no drops)
        cleaned_df = self.clean(raw_df.copy())

        # Step 2 — validate FIRST (reasons aligned with current index)
        valid_df, rejected_df = self.validate(cleaned_df)
        logger.info(
            "[%s] Post-validation | valid=%d rejected=%d",
            self.dataset_name,
            len(valid_df),
            len(rejected_df),
        )

        # Step 3 — dedupe only the valid rows
        valid_df = self._dedupe(valid_df, self._id_col())
        logger.info("[%s] Post-dedupe valid rows=%d", self.dataset_name, len(valid_df))

        # Step 4 — write to silver
        self._write_silver(valid_df, partition_date, rejected=False)
        self._write_silver(rejected_df, partition_date, rejected=True)

        stats = self._stats(partition_date, total, len(valid_df), len(rejected_df))
        stats["dataset"] = self.dataset_name
        logger.info("[%s] Done | %s", self.dataset_name, stats)
        return stats

    def _id_col(self) -> str:
        """Primary key column for this dataset — override if different."""
        return f"{self.dataset_name.rstrip('s')}_id"

    @staticmethod
    def flag_nulls(df: pd.DataFrame, cols: list[str]) -> pd.Series:
        """True where any of cols is null or blank string."""
        mask = pd.Series(False, index=df.index)
        for col in cols:
            if col in df.columns:
                is_null = df[col].isna()
                is_blank = df[col].astype(str).str.strip().eq("")
                mask |= is_null | is_blank
        return mask

    @staticmethod
    def flag_not_in_set(df: pd.DataFrame, col: str, allowed: set[str]) -> pd.Series:
        """
        True where col value is NOT in allowed.

        Normalisation: both the column values and the allowed set are
        compared as uppercase strings, so casing in allowed set doesn't matter
        and .title() bugs can't creep in.
        """
        if col not in df.columns:
            return pd.Series(False, index=df.index)
        allowed_upper = {v.upper() for v in allowed}
        col_upper = df[col].astype(str).str.strip().str.upper()
        return ~col_upper.isin(allowed_upper)

    @staticmethod
    def flag_negative(df: pd.DataFrame, col: str) -> pd.Series:
        """True where col is a valid number AND negative."""
        if col not in df.columns:
            return pd.Series(False, index=df.index)
        numeric = pd.to_numeric(df[col], errors="coerce")
        return numeric.notna() & (numeric < 0)

    @staticmethod
    def flag_ts_order(df: pd.DataFrame, start_col: str, end_col: str) -> pd.Series:
        """True where both timestamps present AND end < start."""
        if start_col not in df.columns or end_col not in df.columns:
            return pd.Series(False, index=df.index)
        start = pd.to_datetime(df[start_col], errors="coerce", utc=True)
        end = pd.to_datetime(df[end_col], errors="coerce", utc=True)
        both = start.notna() & end.notna()
        return both & (end < start)

    @staticmethod
    def flag_foreign_key(df: pd.DataFrame, fk_col: str, valid_keys: set) -> pd.Series:
        """
        True where fk_col value is NOT in valid_keys (referential integrity).
        Handles nulls: a null FK is flagged only if the column is required
        — callers should combine with flag_nulls if the FK is mandatory.
        """
        if fk_col not in df.columns:
            return pd.Series(False, index=df.index)
        return df[fk_col].notna() & ~df[fk_col].isin(valid_keys)

    def load_valid_keys(
        self, partition_date: datetime, ref_dataset: str, key_col: str
    ) -> set:
        """
        Load the set of valid primary keys from a silver clean file.
        Used for referential integrity checks.
        Returns empty set (with warning) if the ref file doesn't exist yet.
        """
        key = (
            f"{ref_dataset}/"
            f"{partition_date.year:04d}/{partition_date.month:02d}/{partition_date.day:02d}/"
            f"{ref_dataset}.csv"
        )
        try:
            response = self.minio.client.get_object(SILVER_BUCKET, key)
            ref_df = pd.read_csv(io.BytesIO(response.read()))
            response.close()
            response.release_conn()
            keys = set(ref_df[key_col].dropna().astype(str).unique())
            logger.info(
                "[%s] Loaded %d valid keys from silver/%s",
                self.dataset_name,
                len(keys),
                key,
            )
            return keys
        except Exception as exc:
            logger.warning(
                "[%s] Could not load ref keys from silver/%s: %s — "
                "skipping FK check for this run.",
                self.dataset_name,
                key,
                exc,
            )
            return set()

    @staticmethod
    def _dedupe(
        df: pd.DataFrame, key_col: str, ts_col: str | None = None
    ) -> pd.DataFrame:
        """
        Keep the last occurrence of each key.
        Does NOT call reset_index() — preserves original index labels
        so any downstream index operations stay consistent.
        """
        if key_col not in df.columns:
            return df
        if ts_col and ts_col in df.columns:
            df = df.sort_values(ts_col, na_position="first")
        return df.drop_duplicates(subset=[key_col], keep="last")

    def _bronze_key(self, partition_date: datetime) -> str:
        d = partition_date
        return (
            f"{self.dataset_name}/"
            f"{d.year:04d}/{d.month:02d}/{d.day:02d}/"
            f"{self.dataset_name}.csv"
        )

    def _silver_key(self, partition_date: datetime, rejected: bool) -> str:
        d = partition_date
        prefix = f"rejected/{self.dataset_name}" if rejected else self.dataset_name
        return (
            f"{prefix}/{d.year:04d}/{d.month:02d}/{d.day:02d}/{self.dataset_name}.csv"
        )

    def _read_bronze(self, partition_date: datetime) -> pd.DataFrame | None:
        key = self._bronze_key(partition_date)
        try:
            resp = self.minio.client.get_object(BRONZE_BUCKET, key)
            df = pd.read_csv(io.BytesIO(resp.read()))
            resp.close()
            resp.release_conn()
            return df
        except Exception as exc:
            logger.error(
                "[%s] Cannot read bronze '%s': %s", self.dataset_name, key, exc
            )
            return None

    def _write_silver(
        self, df: pd.DataFrame, partition_date: datetime, rejected: bool
    ) -> None:
        key = self._silver_key(partition_date, rejected)
        label = "rejected" if rejected else "clean"
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        self.minio.client.put_object(
            SILVER_BUCKET,
            key,
            data=io.BytesIO(csv_bytes),
            length=len(csv_bytes),
            content_type="text/csv",
        )
        logger.info(
            "[%s] Wrote %d %s rows → silver/%s",
            self.dataset_name,
            len(df),
            label,
            key,
        )

    @staticmethod
    def _stats(partition_date, total, clean, rejected) -> dict:
        return {
            "dataset": "",
            "partition": str(partition_date.date()),
            "total": total,
            "clean": clean,
            "rejected": rejected,
            "rejection_rate_pct": (
                round(rejected / total * 100, 2) if total > 0 else 0.0
            ),
        }
