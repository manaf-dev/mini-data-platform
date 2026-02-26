"""Silver cleaner — visits dataset."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from silver.base import SilverCleaner  # noqa: E402

logger = logging.getLogger(__name__)

# Stored as-is in generator; flag_not_in_set compares uppercase internally
VALID_VISIT_TYPES = {"OUTPATIENT", "EMERGENCY", "INPATIENT"}
VALID_STATUSES = {"COMPLETED", "CANCELLED", "NO-SHOW"}
REQUIRED_COLS = ["visit_id", "patient_id", "visit_type", "visit_start_ts", "status"]


class VisitsCleaner(SilverCleaner):
    dataset_name = "visits"

    def _id_col(self) -> str:
        return "visit_id"

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        # Strip whitespace only — do NOT title-case or upper-case enum cols
        # flag_not_in_set() handles case normalisation internally
        for col in ("visit_type", "department", "status", "patient_id", "doctor_id"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

        # Parse timestamps
        for col in ("visit_start_ts", "visit_end_ts"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        return df

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        reasons = pd.Series("", index=df.index)

        # R1: required fields not null
        null_mask = self.flag_nulls(df, REQUIRED_COLS)
        reasons[null_mask] += "missing_required_field;"

        # R2: visit_start_ts must parse
        bad_start = df["visit_start_ts"].isna()
        reasons[bad_start] += "invalid_visit_start_ts;"

        # R3: visit_end_ts must be after start (when present)
        bad_order = self.flag_ts_order(df, "visit_start_ts", "visit_end_ts")
        reasons[bad_order] += "visit_end_before_start;"

        # R4: visit_type must be in allowed set
        bad_type = self.flag_not_in_set(df, "visit_type", VALID_VISIT_TYPES)
        reasons[bad_type] += "invalid_visit_type;"

        # R5: status must be in allowed set
        bad_status = self.flag_not_in_set(df, "status", VALID_STATUSES)
        reasons[bad_status] += "invalid_status;"

        # R6: department must not be null
        if "department" in df.columns:
            missing_dept = df["department"].isna() | df["department"].eq("None")
            reasons[missing_dept] += "missing_department;"

        # Split BEFORE touching index
        is_rejected = reasons.str.len() > 0
        rejected = df[is_rejected].copy()
        rejected["_reject_reason"] = reasons[is_rejected].str.rstrip(";")
        valid = df[~is_rejected].copy()

        return valid, rejected

    def run(self, partition_date: datetime) -> dict:
        """
        Override run() to inject the FK check against silver patients.
        The FK check happens after validate() so invalid rows are already
        excluded before we look up patient_ids.
        """

        logger.info(
            "[visits] Silver pipeline start | partition=%s", partition_date.date()
        )

        raw_df = self._read_bronze(partition_date)
        if raw_df is None or raw_df.empty:
            logger.warning("[visits] No data in bronze — skipping.")
            return self._stats(partition_date, 0, 0, 0)

        total = len(raw_df)
        cleaned_df = self.clean(raw_df.copy())

        # Primary validation
        valid_df, rejected_df = self.validate(cleaned_df)

        # ── Referential integrity: patient_id must exist in silver patients ──
        valid_patient_ids = self.load_valid_keys(
            partition_date, ref_dataset="patients", key_col="patient_id"
        )
        if valid_patient_ids:
            orphan_mask = self.flag_foreign_key(
                valid_df, fk_col="patient_id", valid_keys=valid_patient_ids
            )
            orphaned = valid_df[orphan_mask].copy()
            orphaned["_reject_reason"] = "orphan_patient_id"
            # Move orphaned rows to rejected
            rejected_df = pd.concat([rejected_df, orphaned], ignore_index=True)
            valid_df = valid_df[~orphan_mask].copy()
            logger.info(
                "[visits] FK check: %d orphaned patient_ids moved to rejected.",
                len(orphaned),
            )

        # Dedupe valid rows only
        valid_df = self._dedupe(valid_df, "visit_id", "visit_start_ts")

        self._write_silver(valid_df, partition_date, rejected=False)
        self._write_silver(rejected_df, partition_date, rejected=True)

        stats = self._stats(partition_date, total, len(valid_df), len(rejected_df))
        stats["dataset"] = "visits"
        logger.info("[visits] Done | %s", stats)
        return stats
