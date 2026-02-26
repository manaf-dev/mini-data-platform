"""Standalone script to run data generator."""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_generator.generator import main  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare synthetic data generator — writes one day's data to data/raw/YYYY/MM/DD/"
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="Partition date to generate (default: today). Format: YYYY-MM-DD",
    )
    args = parser.parse_args()
    main(args.date)
