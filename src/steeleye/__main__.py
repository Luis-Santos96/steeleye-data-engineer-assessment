"""Entry point for running the pipeline as a module: python -m steeleye."""

import argparse
import logging

from steeleye.config import PipelineConfig
from steeleye.pipeline import Pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def main() -> None:
    """Parse arguments and run the ESMA FIRDS ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="ESMA FIRDS ETL Pipeline - Download, parse, and store data.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Local output path for the CSV file.",
    )
    parser.add_argument(
        "--storage-path",
        default=None,
        help="Remote storage path (e.g. s3://bucket/file.csv).",
    )
    parser.add_argument(
        "--backend",
        choices=["s3", "az", "file"],
        default=None,
        help="Storage backend: s3, az (Azure), or file (local).",
    )
    args = parser.parse_args()

    config = PipelineConfig.from_env()
    if args.output:
        config = PipelineConfig(
            esma_url=config.esma_url,
            output_path=args.output,
            storage_path=args.storage_path or config.storage_path,
            storage_backend=args.backend or config.storage_backend,
        )

    pipeline = Pipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
