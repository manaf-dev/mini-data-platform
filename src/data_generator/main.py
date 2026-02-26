"""Standalone script to run data generator."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_generator.generator import main  # noqa: E402

if __name__ == "__main__":
    main()
