"""Unit tests for the data transformer module."""

import pandas as pd
import pytest

from steeleye.transformer import DataTransformer


@pytest.fixture
def transformer() -> DataTransformer:
    """Create a DataTransformer instance."""
    return DataTransformer()


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "FinInstrmGnlAttrbts.Id": ["ID1", "ID2", "ID3"],
            "FinInstrmGnlAttrbts.FullNm": [
                "Alpha Bond",
                "XYZ Fund",
                None,
            ],
            "FinInstrmGnlAttrbts.ClssfctnTp": ["A", "B", "C"],
            "FinInstrmGnlAttrbts.CmmdtyDerivInd": [
                "false",
                "true",
                "false",
            ],
            "FinInstrmGnlAttrbts.NtnlCcy": ["EUR", "USD", "GBP"],
            "Issr": ["ISS1", "ISS2", "ISS3"],
        }
    )


class TestTransform:
    """Tests for the a_count and contains_a transformations."""

    def test_a_count_counts_lowercase_a(
        self, transformer: DataTransformer, sample_df: pd.DataFrame
    ) -> None:
        """Should count only lowercase 'a' occurrences."""
        result = transformer.transform(sample_df)

        assert result.iloc[0]["a_count"] == 1  # "Alpha Bond" -> 1 'a'
        assert result.iloc[1]["a_count"] == 0  # "XYZ Fund" -> 0 'a'

    def test_a_count_handles_missing_values(
        self, transformer: DataTransformer, sample_df: pd.DataFrame
    ) -> None:
        """Should return 0 for missing FullNm values."""
        result = transformer.transform(sample_df)

        assert result.iloc[2]["a_count"] == 0

    def test_contains_a_yes_when_present(
        self, transformer: DataTransformer, sample_df: pd.DataFrame
    ) -> None:
        """Should return YES when a_count is greater than 0."""
        result = transformer.transform(sample_df)

        assert result.iloc[0]["contains_a"] == "YES"

    def test_contains_a_no_when_absent(
        self, transformer: DataTransformer, sample_df: pd.DataFrame
    ) -> None:
        """Should return NO when a_count is 0."""
        result = transformer.transform(sample_df)

        assert result.iloc[1]["contains_a"] == "NO"
        assert result.iloc[2]["contains_a"] == "NO"

    def test_preserves_original_columns(
        self, transformer: DataTransformer, sample_df: pd.DataFrame
    ) -> None:
        """Should keep all original columns intact."""
        result = transformer.transform(sample_df)

        assert "FinInstrmGnlAttrbts.Id" in result.columns
        assert "Issr" in result.columns
        assert len(result.columns) == 8  # 6 original + 2 new
