"""Silver cleaner — admissions dataset."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from silver.base import SilverCleaner

logger = logging.getLogger(__name__)

VALID_ROOM_TYPES = {"GENERAL WARD", "SEMI-PRIVATE", "PRIVATE", "ICU", "CCU"}
REQUIRED_COLS = ["admission_id", "visit_id", "admit_ts"]


class AdmissionsCleaner(SilverCleaner):
    dataset_name = "admissions"

    def _id_col(self) -> str:
        return "admission_id"

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if "room_type" in df.columns:
            df["room_type"] = df["room_type"].astype(str).str.strip()
            df["room_type"] = df["room_type"].replace(
                {"nan": None, "NaN": None, "": None}
            )

        for col in ("visit_id",):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

        for col in ("admit_ts", "discharge_ts"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        return df

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        reasons = pd.Series("", index=df.index)

        # R1: required not null
        null_mask = self.flag_nulls(df, REQUIRED_COLS)
        reasons[null_mask] += "missing_required_field;"

        # R2: admit_ts must parse
        bad_admit = df["admit_ts"].isna()
        reasons[bad_admit] += "invalid_admit_ts;"

        # R3: discharge must be after admit (when present)
        bad_order = self.flag_ts_order(df, "admit_ts", "discharge_ts")
        reasons[bad_order] += "discharge_before_admit;"

        # R4: room_type must be in allowed set (when present)
        if "room_type" in df.columns:
            has_room = df["room_type"].notna() & df["room_type"].ne("None")
            bad_room = has_room & self.flag_not_in_set(
                df, "room_type", VALID_ROOM_TYPES
            )
            reasons[bad_room] += "invalid_room_type;"

        is_rejected = reasons.str.len() > 0
        rejected = df[is_rejected].copy()
        rejected["_reject_reason"] = reasons[is_rejected].str.rstrip(";")
        valid = df[~is_rejected].copy()
        return valid, rejected

    def run(self, partition_date: datetime) -> dict:
        logger.info(
            "[admissions] Silver pipeline start | partition=%s", partition_date.date()
        )

        raw_df = self._read_bronze(partition_date)
        if raw_df is None or raw_df.empty:
            logger.warning("[admissions] No data in bronze — skipping.")
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
                "[admissions] FK check: %d orphaned visit_ids moved to rejected.",
                len(orphaned),
            )

        valid_df = self._dedupe(valid_df, "admission_id", "admit_ts")
        self._write_silver(valid_df, partition_date, rejected=False)
        self._write_silver(rejected_df, partition_date, rejected=True)

        stats = self._stats(partition_date, total, len(valid_df), len(rejected_df))
        stats["dataset"] = "admissions"
        return stats
