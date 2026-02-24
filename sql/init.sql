-- Creates:
--   airflow    Airflow metadata DB
--   metabase   Metabase internal DB

CREATE DATABASE airflow;
CREATE DATABASE metabase;

-- Grant user access to all three
GRANT ALL PRIVILEGES ON DATABASE airflow  TO healthcare_admin;
GRANT ALL PRIVILEGES ON DATABASE metabase TO healthcare_admin;

