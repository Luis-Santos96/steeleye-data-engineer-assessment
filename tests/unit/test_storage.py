"""Unit tests for the data storage module."""

import os
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from steeleye.storage import DataStorage


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "FinInstrmGnlAttrbts.Id": ["ID1", "ID2"],
            "FinInstrmGnlAttrbts.FullNm": ["Alpha", "Beta"],
            "a_count": [1, 0],
            "contains_a": ["YES", "NO"],
        }
    )


class TestSaveLocal:
    """Tests for saving CSV to local filesystem."""

    def test_creates_csv_file(self, sample_df: pd.DataFrame, tmp_path: Path) -> None:
        """Should create a CSV file at the given path."""
        output_path = str(tmp_path / "output" / "test.csv")
        storage = DataStorage(storage_path="s3://unused")

        result = storage.save_local(sample_df, output_path)

        assert os.path.exists(result)

    def test_csv_content_matches(self, sample_df: pd.DataFrame, tmp_path: Path) -> None:
        """Should write correct CSV content."""
        output_path = str(tmp_path / "test.csv")
        storage = DataStorage(storage_path="s3://unused")

        storage.save_local(sample_df, output_path)
        loaded = pd.read_csv(output_path)

        assert len(loaded) == 2
        assert loaded.iloc[0]["FinInstrmGnlAttrbts.Id"] == "ID1"

    def test_creates_parent_directories(
        self, sample_df: pd.DataFrame, tmp_path: Path
    ) -> None:
        """Should create parent directories if they don't exist."""
        output_path = str(tmp_path / "deep" / "nested" / "test.csv")
        storage = DataStorage(storage_path="s3://unused")

        storage.save_local(sample_df, output_path)

        assert os.path.exists(output_path)


class TestSaveCsvToS3:
    """Tests for saving CSV to S3 using moto mock."""

    @mock_aws
    def test_uploads_to_s3(self, sample_df: pd.DataFrame) -> None:
        """Should upload CSV to the specified S3 path."""
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")

        storage = DataStorage(storage_path="s3://test-bucket/output.csv")
        result = storage.save_csv(sample_df)

        assert result == "s3://test-bucket/output.csv"

        obj = conn.get_object(Bucket="test-bucket", Key="output.csv")
        content = obj["Body"].read().decode("utf-8")
        assert "ID1" in content
        assert "Alpha" in content

    @mock_aws
    def test_csv_has_correct_columns(self, sample_df: pd.DataFrame) -> None:
        """Should write CSV with correct headers."""
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")

        storage = DataStorage(storage_path="s3://test-bucket/data.csv")
        storage.save_csv(sample_df)

        obj = conn.get_object(Bucket="test-bucket", Key="data.csv")
        header = obj["Body"].read().decode("utf-8").split("\n")[0]
        assert "FinInstrmGnlAttrbts.Id" in header
        assert "a_count" in header
