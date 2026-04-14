"""Module for storing output files using fsspec-compatible backends."""

import logging
from pathlib import Path

import fsspec
import pandas as pd

from steeleye.exceptions import StorageError

logger = logging.getLogger(__name__)


class DataStorage:
    """Stores DataFrames to local or cloud storage using fsspec.

    Supports any fsspec-compatible backend including local filesystem,
    AWS S3 (s3://), and Azure Blob Storage (az://).

    Args:
        storage_path: Destination path (e.g. s3://bucket/file.csv).
        storage_options: Extra options passed to the fsspec filesystem
            (e.g. credentials, endpoint URLs).
    """

    def __init__(
        self,
        storage_path: str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self.storage_path = storage_path
        self.storage_options = storage_options or {}

    def save_csv(self, df: pd.DataFrame) -> str:
        """Save a DataFrame as CSV to the configured storage path.

        Args:
            df: DataFrame to save.

        Returns:
            The storage path where the file was saved.

        Raises:
            StorageError: If writing to the storage backend fails.
        """
        logger.info("Saving CSV to %s", self.storage_path)
        try:
            with fsspec.open(
                self.storage_path,
                mode="w",
                **self.storage_options,
            ) as f:
                df.to_csv(f, index=False)
        except Exception as exc:
            raise StorageError(
                f"Failed to save CSV to {self.storage_path}: {exc}"
            ) from exc

        logger.info("CSV saved successfully to %s", self.storage_path)
        return self.storage_path

    def save_local(self, df: pd.DataFrame, output_path: str) -> str:
        """Save a DataFrame as CSV to the local filesystem.

        Args:
            df: DataFrame to save.
            output_path: Local file path for the CSV.

        Returns:
            The local path where the file was saved.

        Raises:
            StorageError: If writing to disk fails.
        """
        logger.info("Saving CSV locally to %s", output_path)
        try:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=False)
        except Exception as exc:
            raise StorageError(
                f"Failed to save CSV to {output_path}: {exc}"
            ) from exc

        logger.info("CSV saved locally to %s", output_path)
        return output_path
