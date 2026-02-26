"""Silver cleaner — billing dataset."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from silver.base import SilverCleaner  # noqa: E402

logger = logging.getLogger(__name__)

VALID_PAYMENT_STATUSES = {"PAID", "PENDING", "PARTIAL"}
REQUIRED_COLS = ["bill_id", "visit_id", "total_amount", "payment_status"]


class BillingCleaner(SilverCleaner):
    dataset_name = "billing"

    def _id_col(self) -> str:
        return "bill_id"

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ("payment_status", "visit_id"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

        for col in ("total_amount", "insurance_amount", "patient_amount"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "payment_ts" in df.columns:
            df["payment_ts"] = pd.to_datetime(
                df["payment_ts"], errors="coerce", utc=True
            )

        return df

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        reasons = pd.Series("", index=df.index)

        # R1: required not null
        null_mask = self.flag_nulls(df, REQUIRED_COLS)
        reasons[null_mask] += "missing_required_field;"

        # R2: total_amount non-negative
        neg_total = self.flag_negative(df, "total_amount")
        reasons[neg_total] += "negative_total_amount;"

        # R3: insurance_amount cannot exceed total_amount
        if "insurance_amount" in df.columns and "total_amount" in df.columns:
            both_valid = df["insurance_amount"].notna() & df["total_amount"].notna()
            over_insured = both_valid & (df["insurance_amount"] > df["total_amount"])
            reasons[over_insured] += "insurance_exceeds_total;"

        # R4: patient_amount non-negative
        neg_patient = self.flag_negative(df, "patient_amount")
        reasons[neg_patient] += "negative_patient_amount;"

        # R5: payment_status in allowed set
        bad_status = self.flag_not_in_set(df, "payment_status", VALID_PAYMENT_STATUSES)
        reasons[bad_status] += "invalid_payment_status;"

        # R6: Paid bills must have a payment_ts
        if "payment_ts" in df.columns:
            paid_no_ts = (
                df["payment_status"].astype(str).str.upper().eq("PAID")
                & df["payment_ts"].isna()
            )
            reasons[paid_no_ts] += "paid_bill_missing_payment_ts;"

        is_rejected = reasons.str.len() > 0
        rejected = df[is_rejected].copy()
        rejected["_reject_reason"] = reasons[is_rejected].str.rstrip(";")
        valid = df[~is_rejected].copy()
        return valid, rejected

    def run(self, partition_date: datetime) -> dict:
        logger.info(
            "[billing] Silver pipeline start | partition=%s", partition_date.date()
        )

        raw_df = self._read_bronze(partition_date)
        if raw_df is None or raw_df.empty:
            logger.warning("[billing] No data in bronze — skipping.")
            return self._stats(partition_date, 0, 0, 0)

        total = len(raw_df)
        cleaned_df = self.clean(raw_df.copy())
        valid_df, rejected_df = self.validate(cleaned_df)

        # ── FK check: visit_id must exist in silver visits ────────────────
        valid_visit_ids = self.load_valid_keys(
            partition_date, ref_dataset="visits", key_col="visit_id"
        )
        if valid_visit_ids:
            orphan_mask = self.flag_foreign_key(
                valid_df, fk_col="visit_id", valid_keys=valid_visit_ids
            )
            orphaned = valid_df[orphan_mask].copy()
            orphaned["_reject_reason"] = "orphan_visit_id"
            rejected_df = pd.concat([rejected_df, orphaned], ignore_index=True)
            valid_df = valid_df[~orphan_mask].copy()
            logger.info(
                "[billing] FK check: %d orphaned visit_ids moved to rejected.",
                len(orphaned),
            )

        valid_df = self._dedupe(valid_df, "bill_id", "payment_ts")
        self._write_silver(valid_df, partition_date, rejected=False)
        self._write_silver(rejected_df, partition_date, rejected=True)

        stats = self._stats(partition_date, total, len(valid_df), len(rejected_df))
        stats["dataset"] = "billing"
        return stats
