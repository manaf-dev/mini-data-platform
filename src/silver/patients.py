"""Silver cleaner — patients dataset."""

import logging
import sys
from pathlib import Path
from typing import Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
import pandas as pd  # noqa: E402

from silver.base import SilverCleaner  # noqa: E402

logger = logging.getLogger(__name__)

# Uppercase — flag_not_in_set() compares uppercase internally
VALID_GENDERS = {"M", "F", "OTHER", "UNKNOWN"}
REQUIRED_COLS = ["patient_id", "birth_date", "registration_date"]


class PatientsCleaner(SilverCleaner):
    dataset_name = "patients"

    def _id_col(self) -> str:
        return "patient_id"

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        # Strip whitespace on string cols — do NOT title-case enum cols
        for col in ("gender", "city"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({"nan": None, "NaN": None, "": None})

        # Parse dates
        for col in ("birth_date", "registration_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        return df

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # reasons Series shares the SAME index as df throughout
        reasons = pd.Series("", index=df.index)

        # R1: required fields not null
        null_mask = self.flag_nulls(df, REQUIRED_COLS)
        reasons[null_mask] += "missing_required_field;"

        # R2: gender must be in valid set (when present)
        if "gender" in df.columns:
            has_gender = df["gender"].notna()
            bad_gender = has_gender & self.flag_not_in_set(df, "gender", VALID_GENDERS)
            reasons[bad_gender] += "invalid_gender;"

        # R3: birth_date must parse
        bad_birth = df["birth_date"].isna()
        reasons[bad_birth] += "invalid_birth_date;"

        # R4: registration_date must parse
        bad_reg = df["registration_date"].isna()
        reasons[bad_reg] += "invalid_registration_date;"

        # R5: birth must be before registration
        both = df["birth_date"].notna() & df["registration_date"].notna()
        bad_order = both & (df["birth_date"] > df["registration_date"])
        reasons[bad_order] += "birth_after_registration;"

        # Split — index alignment is guaranteed (no reset yet)
        is_rejected = reasons.str.len() > 0
        rejected = df[is_rejected].copy()
        rejected["_reject_reason"] = reasons[is_rejected].str.rstrip(";")

        valid = df[~is_rejected].copy()
        return valid, rejected
