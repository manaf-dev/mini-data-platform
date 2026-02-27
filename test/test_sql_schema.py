from pathlib import Path
import re


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_ddl_contains_required_core_and_mart_tables():
    ddl = _read("sql/ddl.sql")
    required_tables = [
        "patients",
        "visits",
        "admissions",
        "treatments",
        "billing",
        "mart_daily_kpis",
        "mart_department_stats",
        "mart_patient_stats",
    ]

    for table in required_tables:
        pattern = rf"CREATE TABLE IF NOT EXISTS\s+{table}\s*\("
        assert re.search(pattern, ddl, re.IGNORECASE), f"Missing table: {table}"


def test_init_sql_creates_required_databases():
    init_sql = _read("sql/init.sql")
    assert "CREATE DATABASE airflow;" in init_sql
    assert "CREATE DATABASE metabase;" in init_sql
