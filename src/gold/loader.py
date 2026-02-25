"""
Each class implements upsert() with an explicit ON CONFLICT clause
so reruns never create duplicates.

Column order in execute_values tuples must exactly match
the INSERT column list.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))
from gold.base import GoldLoader

logger = logging.getLogger(__name__)

NOW_UTC = datetime.now(timezone.utc)


def _safe(val):
    """Convert pandas NA / NaT / float nan to Python None for psycopg2."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


class PatientsLoader(GoldLoader):
    dataset_name = "patients"

    def upsert(self, df: pd.DataFrame, cur) -> int:
        sql = """
            INSERT INTO patients
                (patient_id, gender, birth_date, city, registration_date, processed_at)
            VALUES %s
            ON CONFLICT (patient_id) DO UPDATE SET
                gender            = EXCLUDED.gender,
                birth_date        = EXCLUDED.birth_date,
                city              = EXCLUDED.city,
                registration_date = EXCLUDED.registration_date,
                processed_at        = EXCLUDED.processed_at
        """
        rows = [
            (
                row.patient_id,
                _safe(row.gender),
                _safe(row.birth_date),
                _safe(row.city),
                _safe(row.registration_date),
                NOW_UTC,
            )
            for row in df.itertuples(index=False)
        ]
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        return len(rows)


class VisitsLoader(GoldLoader):
    dataset_name = "visits"

    def upsert(self, df: pd.DataFrame, cur) -> int:
        sql = """
            INSERT INTO visits
                (visit_id, patient_id, visit_type, department,
                 doctor_id, visit_start_ts, visit_end_ts, status, processed_at)
            VALUES %s
            ON CONFLICT (visit_id) DO UPDATE SET
                patient_id     = EXCLUDED.patient_id,
                visit_type     = EXCLUDED.visit_type,
                department     = EXCLUDED.department,
                doctor_id      = EXCLUDED.doctor_id,
                visit_start_ts = EXCLUDED.visit_start_ts,
                visit_end_ts   = EXCLUDED.visit_end_ts,
                status         = EXCLUDED.status,
                processed_at     = EXCLUDED.processed_at
        """
        rows = [
            (
                row.visit_id,
                _safe(row.patient_id),
                _safe(row.visit_type),
                _safe(row.department),
                _safe(row.doctor_id),
                _safe(row.visit_start_ts),
                _safe(row.visit_end_ts),
                _safe(row.status),
                NOW_UTC,
            )
            for row in df.itertuples(index=False)
        ]
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        return len(rows)


class AdmissionsLoader(GoldLoader):
    dataset_name = "admissions"

    def upsert(self, df: pd.DataFrame, cur) -> int:
        sql = """
            INSERT INTO admissions
                (admission_id, visit_id, admit_ts, discharge_ts, room_type, processed_at)
            VALUES %s
            ON CONFLICT (admission_id) DO UPDATE SET
                visit_id     = EXCLUDED.visit_id,
                admit_ts     = EXCLUDED.admit_ts,
                discharge_ts = EXCLUDED.discharge_ts,
                room_type    = EXCLUDED.room_type,
                processed_at   = EXCLUDED.processed_at
        """
        rows = [
            (
                row.admission_id,
                _safe(row.visit_id),
                _safe(row.admit_ts),
                _safe(row.discharge_ts),
                _safe(row.room_type),
                NOW_UTC,
            )
            for row in df.itertuples(index=False)
        ]
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        return len(rows)


class TreatmentsLoader(GoldLoader):
    dataset_name = "treatments"

    def upsert(self, df: pd.DataFrame, cur) -> int:
        sql = """
            INSERT INTO treatments
                (treatment_id, visit_id, treatment_code, treatment_name,
                 treatment_cost, performed_ts, processed_at)
            VALUES %s
            ON CONFLICT (treatment_id) DO UPDATE SET
                visit_id       = EXCLUDED.visit_id,
                treatment_code = EXCLUDED.treatment_code,
                treatment_name = EXCLUDED.treatment_name,
                treatment_cost = EXCLUDED.treatment_cost,
                performed_ts   = EXCLUDED.performed_ts,
                processed_at     = EXCLUDED.processed_at
        """
        rows = [
            (
                row.treatment_id,
                _safe(row.visit_id),
                _safe(row.treatment_code),
                _safe(row.treatment_name),
                _safe(row.treatment_cost),
                _safe(row.performed_ts),
                NOW_UTC,
            )
            for row in df.itertuples(index=False)
        ]
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        return len(rows)


class BillingLoader(GoldLoader):
    dataset_name = "billing"

    def upsert(self, df: pd.DataFrame, cur) -> int:
        sql = """
            INSERT INTO billing
                (bill_id, visit_id, total_amount, insurance_amount,
                 patient_amount, payment_status, payment_ts, processed_at)
            VALUES %s
            ON CONFLICT (bill_id) DO UPDATE SET
                visit_id         = EXCLUDED.visit_id,
                total_amount     = EXCLUDED.total_amount,
                insurance_amount = EXCLUDED.insurance_amount,
                patient_amount   = EXCLUDED.patient_amount,
                payment_status   = EXCLUDED.payment_status,
                payment_ts       = EXCLUDED.payment_ts,
                processed_at       = EXCLUDED.processed_at
        """
        rows = [
            (
                row.bill_id,
                _safe(row.visit_id),
                _safe(row.total_amount),
                _safe(row.insurance_amount),
                _safe(row.patient_amount),
                _safe(row.payment_status),
                _safe(row.payment_ts),
                NOW_UTC,
            )
            for row in df.itertuples(index=False)
        ]
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
        return len(rows)
