

CREATE TABLE IF NOT EXISTS patients (
    patient_id          TEXT        PRIMARY KEY,
    gender              TEXT,
    birth_date          DATE,
    city                TEXT,
    registration_date   DATE,
    processed_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id            TEXT        PRIMARY KEY,
    patient_id          TEXT        REFERENCES patients(patient_id) ON DELETE SET NULL,
    visit_type          TEXT,
    department          TEXT,
    doctor_id           TEXT,
    visit_start_ts      TIMESTAMPTZ,
    visit_end_ts        TIMESTAMPTZ,
    status              TEXT,
    processed_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admissions (
    admission_id        TEXT        PRIMARY KEY,
    visit_id            TEXT        REFERENCES visits(visit_id) ON DELETE SET NULL,
    admit_ts            TIMESTAMPTZ,
    discharge_ts        TIMESTAMPTZ,
    room_type           TEXT,
    length_of_stay_days NUMERIC(8,2) GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (discharge_ts - admit_ts)) / 86400.0
    ) STORED,
    processed_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS treatments (
    treatment_id        TEXT        PRIMARY KEY,
    visit_id            TEXT        REFERENCES visits(visit_id) ON DELETE SET NULL,
    treatment_code      TEXT,
    treatment_name      TEXT,
    treatment_cost      NUMERIC(12,4),
    performed_ts        TIMESTAMPTZ,
    processed_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS billing (
    bill_id             TEXT        PRIMARY KEY,
    visit_id            TEXT        REFERENCES visits(visit_id) ON DELETE SET NULL,
    total_amount        NUMERIC(12,4),
    insurance_amount    NUMERIC(12,4),
    patient_amount      NUMERIC(12,4),
    payment_status      TEXT,
    payment_ts          TIMESTAMPTZ,
    processed_at          TIMESTAMPTZ DEFAULT NOW()
);

--Mart tables (rebuilt fully on each DAG run)

CREATE TABLE IF NOT EXISTS mart_daily_kpis (
    kpi_date                DATE        PRIMARY KEY,
    total_visits            INT,
    completed_visits        INT,
    total_admissions        INT,
    avg_length_of_stay_days NUMERIC(8,2),
    total_revenue           NUMERIC(14,4),
    insurance_revenue       NUMERIC(14,4),
    patient_revenue         NUMERIC(14,4),
    paid_bills              INT,
    pending_bills           INT,
    payment_rate_pct        NUMERIC(8,2),
    new_patients            INT
);

CREATE TABLE IF NOT EXISTS mart_department_stats (
    department              TEXT,
    stat_date               DATE,
    total_visits            INT,
    completed_visits        INT,
    emergency_visits        INT,
    inpatient_visits        INT,
    outpatient_visits       INT,
    avg_visit_duration_hrs  NUMERIC(8,2),
    total_admissions        INT,
    avg_los_days            NUMERIC(8,2),
    total_revenue           NUMERIC(14,4),
    PRIMARY KEY (department, stat_date)
);

CREATE TABLE IF NOT EXISTS mart_patient_stats (
    patient_id              TEXT        PRIMARY KEY,
    gender                  TEXT,
    city                    TEXT,
    age_years               INT,
    total_visits            INT,
    first_visit_date        DATE,
    last_visit_date         DATE,
    total_billed            NUMERIC(14,4),
    total_paid              NUMERIC(14,4),
    is_returning            BOOLEAN
);

-- Indexes for Metabase query speed 

CREATE INDEX IF NOT EXISTS idx_visits_patient    ON visits(patient_id);
CREATE INDEX IF NOT EXISTS idx_visits_dept       ON visits(department);
CREATE INDEX IF NOT EXISTS idx_visits_start      ON visits(visit_start_ts);
CREATE INDEX IF NOT EXISTS idx_admissions_visit  ON admissions(visit_id);
CREATE INDEX IF NOT EXISTS idx_treatments_visit  ON treatments(visit_id);
CREATE INDEX IF NOT EXISTS idx_billing_visit     ON billing(visit_id);
CREATE INDEX IF NOT EXISTS idx_billing_status    ON billing(payment_status);
CREATE INDEX IF NOT EXISTS idx_daily_kpis_date   ON mart_daily_kpis(kpi_date);
CREATE INDEX IF NOT EXISTS idx_dept_stats_date   ON mart_department_stats(stat_date);