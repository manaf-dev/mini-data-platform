from datetime import datetime
from pathlib import Path

import src.ingestion.bronze_ingestion as bronze_module


class FakeMinioClient:
    def __init__(self):
        self.upload_calls = []

    def upload_csv_to_bronze(self, file_path, dataset_name, partition_date):
        self.upload_calls.append((file_path, dataset_name, partition_date))
        return f"s3://healthcare-bronze/{dataset_name}/{Path(file_path).name}"

    def list_objects(self, bucket_name, prefix, recursive=True):
        if prefix.startswith("billing/"):
            return []
        return [f"{prefix}file.csv"]


def test_ingest_all_uploads_only_existing_partition_files(tmp_path, monkeypatch):
    fake = FakeMinioClient()
    monkeypatch.setattr(bronze_module, "MinIOClient", lambda: fake)
    monkeypatch.chdir(tmp_path)

    partition = datetime(2026, 2, 27)
    partition_dir = tmp_path / "data" / "raw" / "2026" / "02" / "27"
    partition_dir.mkdir(parents=True, exist_ok=True)
    (partition_dir / "patients.csv").write_text("patient_id\nP001\n", encoding="utf-8")
    (partition_dir / "visits.csv").write_text("visit_id\nV001\n", encoding="utf-8")

    ingestion = bronze_module.BronzeIngestion()
    result = ingestion.ingest_all(partition)

    assert set(result.keys()) == {"patients", "visits"}
    assert len(fake.upload_calls) == 2


def test_verify_ingestion_returns_false_when_any_dataset_missing(monkeypatch):
    fake = FakeMinioClient()
    monkeypatch.setattr(bronze_module, "MinIOClient", lambda: fake)

    ingestion = bronze_module.BronzeIngestion()
    ok = ingestion.verify_ingestion(datetime(2026, 2, 27))

    assert ok is False
