"""
marts.py — Builds pre-aggregated mart tables from core gold tables.

All three marts are TRUNCATE + INSERT (full rebuild each run).
This is intentional: marts are cheap to recompute and always
reflect the complete current state of the warehouse.

Pure SQL — no pandas needed here.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from gold.base import _pg_conn

logger = logging.getLogger(__name__)


_MART_DAILY_KPIS_SQL = """
INSERT INTO mart_daily_kpis (
    kpi_date,
    total_visits,
    completed_visits,
    total_admissions,
    avg_length_of_stay_days,
    total_revenue,
    insurance_revenue,
    patient_revenue,
    paid_bills,
    pending_bills,
    payment_rate_pct,
    new_patients
)
SELECT
    v.visit_date                                        AS kpi_date,
    COUNT(DISTINCT v.visit_id)                          AS total_visits,
    COUNT(DISTINCT v.visit_id)
        FILTER (WHERE v.status ILIKE 'Completed')       AS completed_visits,
    COUNT(DISTINCT a.admission_id)                      AS total_admissions,
    ROUND(AVG(a.length_of_stay_days)::NUMERIC, 2)       AS avg_length_of_stay_days,
    COALESCE(SUM(b.total_amount),      0)               AS total_revenue,
    COALESCE(SUM(b.insurance_amount),  0)               AS insurance_revenue,
    COALESCE(SUM(b.patient_amount),    0)               AS patient_revenue,
    COUNT(b.bill_id)
        FILTER (WHERE b.payment_status ILIKE 'Paid')    AS paid_bills,
    COUNT(b.bill_id)
        FILTER (WHERE b.payment_status ILIKE 'Pending') AS pending_bills,
    CASE
        WHEN COUNT(b.bill_id) > 0
        THEN ROUND(
            (COUNT(b.bill_id) FILTER (WHERE b.payment_status ILIKE 'Paid'))::NUMERIC
            / COUNT(b.bill_id) * 100, 2)
        ELSE 0
    END                                                 AS payment_rate_pct,
    COUNT(DISTINCT p.patient_id)
        FILTER (WHERE p.registration_date = v.visit_date) AS new_patients
FROM (
    SELECT
        visit_id, patient_id, status,
        visit_start_ts::DATE AS visit_date
    FROM visits
    WHERE visit_start_ts IS NOT NULL
) v
LEFT JOIN admissions  a ON a.visit_id    = v.visit_id
LEFT JOIN billing     b ON b.visit_id    = v.visit_id
LEFT JOIN patients    p ON p.patient_id  = v.patient_id
GROUP BY v.visit_date
ORDER BY v.visit_date
ON CONFLICT (kpi_date) DO UPDATE SET
    total_visits            = EXCLUDED.total_visits,
    completed_visits        = EXCLUDED.completed_visits,
    total_admissions        = EXCLUDED.total_admissions,
    avg_length_of_stay_days = EXCLUDED.avg_length_of_stay_days,
    total_revenue           = EXCLUDED.total_revenue,
    insurance_revenue       = EXCLUDED.insurance_revenue,
    patient_revenue         = EXCLUDED.patient_revenue,
    paid_bills              = EXCLUDED.paid_bills,
    pending_bills           = EXCLUDED.pending_bills,
    payment_rate_pct        = EXCLUDED.payment_rate_pct,
    new_patients            = EXCLUDED.new_patients
;
"""


_MART_DEPT_STATS_SQL = """
INSERT INTO mart_department_stats (
    department,
    stat_date,
    total_visits,
    completed_visits,
    emergency_visits,
    inpatient_visits,
    outpatient_visits,
    avg_visit_duration_hrs,
    total_admissions,
    avg_los_days,
    total_revenue
)
SELECT
    v.department,
    v.visit_date                                                AS stat_date,
    COUNT(DISTINCT v.visit_id)                                  AS total_visits,
    COUNT(DISTINCT v.visit_id) FILTER (WHERE v.status ILIKE 'Completed')    AS completed_visits,
    COUNT(DISTINCT v.visit_id) FILTER (WHERE v.visit_type ILIKE 'Emergency') AS emergency_visits,
    COUNT(DISTINCT v.visit_id) FILTER (WHERE v.visit_type ILIKE 'Inpatient') AS inpatient_visits,
    COUNT(DISTINCT v.visit_id) FILTER (WHERE v.visit_type ILIKE 'Outpatient') AS outpatient_visits,
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM (v.visit_end_ts - v.visit_start_ts)) / 3600.0
        )::NUMERIC, 2
    )                                                           AS avg_visit_duration_hrs,
    COUNT(DISTINCT a.admission_id)                              AS total_admissions,
    ROUND(AVG(a.length_of_stay_days)::NUMERIC, 2)               AS avg_los_days,
    COALESCE(SUM(b.total_amount), 0)                            AS total_revenue
FROM (
    SELECT
        visit_id, department, status, visit_type,
        visit_start_ts, visit_end_ts,
        visit_start_ts::DATE AS visit_date
    FROM visits
    WHERE department IS NOT NULL
      AND visit_start_ts IS NOT NULL
) v
LEFT JOIN admissions  a ON a.visit_id = v.visit_id
LEFT JOIN billing     b ON b.visit_id = v.visit_id
GROUP BY v.department, v.visit_date
ON CONFLICT (department, stat_date) DO UPDATE SET
    total_visits           = EXCLUDED.total_visits,
    completed_visits       = EXCLUDED.completed_visits,
    emergency_visits       = EXCLUDED.emergency_visits,
    inpatient_visits       = EXCLUDED.inpatient_visits,
    outpatient_visits      = EXCLUDED.outpatient_visits,
    avg_visit_duration_hrs = EXCLUDED.avg_visit_duration_hrs,
    total_admissions       = EXCLUDED.total_admissions,
    avg_los_days           = EXCLUDED.avg_los_days,
    total_revenue          = EXCLUDED.total_revenue
;
"""


_MART_PATIENT_STATS_SQL = """
INSERT INTO mart_patient_stats (
    patient_id,
    gender,
    city,
    age_years,
    total_visits,
    first_visit_date,
    last_visit_date,
    total_billed,
    total_paid,
    is_returning
)
SELECT
    p.patient_id,
    p.gender,
    p.city,
    DATE_PART('year', AGE(CURRENT_DATE, p.birth_date))::INT  AS age_years,
    COUNT(DISTINCT v.visit_id)                               AS total_visits,
    MIN(v.visit_start_ts)::DATE                              AS first_visit_date,
    MAX(v.visit_start_ts)::DATE                              AS last_visit_date,
    COALESCE(SUM(b.total_amount),   0)                       AS total_billed,
    COALESCE(SUM(
        CASE WHEN b.payment_status ILIKE 'Paid'
             THEN b.total_amount ELSE 0 END
    ), 0)                                                    AS total_paid,
    COUNT(DISTINCT v.visit_id) > 1                           AS is_returning
FROM patients p
LEFT JOIN visits  v ON v.patient_id = p.patient_id
LEFT JOIN billing b ON b.visit_id   = v.visit_id
GROUP BY p.patient_id, p.gender, p.city, p.birth_date
ON CONFLICT (patient_id) DO UPDATE SET
    gender           = EXCLUDED.gender,
    city             = EXCLUDED.city,
    age_years        = EXCLUDED.age_years,
    total_visits     = EXCLUDED.total_visits,
    first_visit_date = EXCLUDED.first_visit_date,
    last_visit_date  = EXCLUDED.last_visit_date,
    total_billed     = EXCLUDED.total_billed,
    total_paid       = EXCLUDED.total_paid,
    is_returning     = EXCLUDED.is_returning
;
"""


def build_all_marts() -> dict:
    """
    Rebuild all three mart tables.
    Uses UPSERT so reruns are safe.
    Returns row counts for each mart.
    """
    marts = {
        "mart_daily_kpis": _MART_DAILY_KPIS_SQL,
        "mart_department_stats": _MART_DEPT_STATS_SQL,
        "mart_patient_stats": _MART_PATIENT_STATS_SQL,
    }

    results = {}

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            for mart_name, sql in marts.items():
                logger.info("[marts] Building %s ...", mart_name)
                cur.execute(sql)
                row_count = cur.rowcount
                results[mart_name] = row_count
                logger.info("[marts] %s → %d rows upserted.", mart_name, row_count)
        conn.commit()

    return results
