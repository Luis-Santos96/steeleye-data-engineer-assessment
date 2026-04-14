"""Module for transforming financial instrument DataFrames."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Applies transformations to the parsed instrument DataFrame.

    Adds derived columns based on the assessment requirements.
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add a_count and contains_a columns to the DataFrame.

        Args:
            df: DataFrame with financial instrument data.

        Returns:
            DataFrame with added a_count and contains_a columns.
        """
        logger.info("Applying transformations to %d rows", len(df))

        full_nm = df["FinInstrmGnlAttrbts.FullNm"].fillna("")
        df["a_count"] = full_nm.str.count("a")
        df["contains_a"] = df["a_count"].apply(lambda x: "YES" if x > 0 else "NO")

        logger.info("Transformations applied successfully")
        return df
