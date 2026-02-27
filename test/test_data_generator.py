from datetime import date
from pathlib import Path

from src.data_generator.config import GeneratorConfig
from src.data_generator.generator import HealthcareDataGenerator, partition_path


def test_partition_path_uses_yyyy_mm_dd():
    root = Path("data/raw")
    d = date(2026, 2, 27)
    assert partition_path(root, d).as_posix() == "data/raw/2026/02/27"


def test_generator_creates_partition_output_dir(tmp_path):
    config = GeneratorConfig()
    config.output_dir = str(tmp_path / "raw")
    gen = HealthcareDataGenerator(config, date(2026, 2, 27))

    expected = tmp_path / "raw" / "2026" / "02" / "27"
    assert gen.output_dir == expected
    assert expected.exists()
