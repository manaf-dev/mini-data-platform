"""Silver cleaner — treatments dataset."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from silver.base import SilverCleaner  # noqa: E402

logger = logging.getLogger(__name__)

REQUIRED_COLS = ["treatment_id", "visit_id", "treatment_code", "treatment_cost"]


class TreatmentsCleaner(SilverCleaner):
    dataset_name = "treatments"

    def _id_col(self) -> str:
        return "treatment_id"

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in ("treatment_code", "treatment_name", "visit_id"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

        if "treatment_cost" in df.columns:
            df["treatment_cost"] = pd.to_numeric(df["treatment_cost"], errors="coerce")

        if "performed_ts" in df.columns:
            df["performed_ts"] = pd.to_datetime(
                df["performed_ts"], errors="coerce", utc=True
            )

        return df

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        reasons = pd.Series("", index=df.index)

        # R1: required not null
        null_mask = self.flag_nulls(df, REQUIRED_COLS)
        reasons[null_mask] += "missing_required_field;"

        # R2: treatment_cost must be non-negative
        neg_cost = self.flag_negative(df, "treatment_cost")
        reasons[neg_cost] += "negative_treatment_cost;"

        # R3: performed_ts must parse
        bad_ts = df["performed_ts"].isna()
        reasons[bad_ts] += "invalid_performed_ts;"

        is_rejected = reasons.str.len() > 0
        rejected = df[is_rejected].copy()
        rejected["_reject_reason"] = reasons[is_rejected].str.rstrip(";")
        valid = df[~is_rejected].copy()
        return valid, rejected

    def run(self, partition_date: datetime) -> dict:
        logger.info(
            "[treatments] Silver pipeline start | partition=%s", partition_date.date()
        )

        raw_df = self._read_bronze(partition_date)
        if raw_df is None or raw_df.empty:
            logger.warning("[treatments] No data in bronze — skipping.")
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
                "[treatments] FK check: %d orphaned visit_ids moved to rejected.",
                len(orphaned),
            )

        valid_df = self._dedupe(valid_df, "treatment_id", "performed_ts")
        self._write_silver(valid_df, partition_date, rejected=False)
        self._write_silver(rejected_df, partition_date, rejected=True)

        stats = self._stats(partition_date, total, len(valid_df), len(rejected_df))
        stats["dataset"] = "treatments"
        return stats
