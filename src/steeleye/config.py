"""Pipeline configuration with environment variable overrides."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for the ESMA FIRDS ETL pipeline.

    Attributes:
        esma_url: URL to fetch the ESMA FIRDS XML registry.
        output_path: Local path for the output CSV file.
        storage_path: Remote path for cloud storage upload.
        storage_backend: fsspec-compatible protocol (s3, az, file).
    """

    esma_url: str = (
        "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
        "?q=*"
        "&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D"
        "&wt=xml&indent=true&start=0&rows=100"
    )
    output_path: str = "output/firds_data.csv"
    storage_path: str = "s3://steeleye-bucket/firds_data.csv"
    storage_backend: str = "s3"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Create configuration from environment variables.

        Falls back to default values when variables are not set.

        Returns:
            PipelineConfig with values from environment or defaults.
        """
        return cls(
            esma_url=os.getenv("STEELEYE_ESMA_URL", cls.esma_url),
            output_path=os.getenv("STEELEYE_OUTPUT_PATH", cls.output_path),
            storage_path=os.getenv("STEELEYE_STORAGE_PATH", cls.storage_path),
            storage_backend=os.getenv("STEELEYE_STORAGE_BACKEND", cls.storage_backend),
        )
