"""Orchestrator for the ESMA FIRDS ETL pipeline."""

import logging

from steeleye.config import PipelineConfig
from steeleye.downloader import ESMADownloader
from steeleye.parser import DLTINSParser
from steeleye.storage import DataStorage
from steeleye.transformer import DataTransformer

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates the full ETL pipeline: download, parse, transform, store.

    Args:
        config: Pipeline configuration instance.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def run(self) -> None:
        """Execute the full ETL pipeline.

        Steps:
            1. Download ESMA registry XML and extract DLTINS file.
            2. Parse instrument records into a DataFrame.
            3. Add a_count and contains_a columns.
            4. Save CSV locally and to cloud storage.
        """
        logger.info("Starting ESMA FIRDS ETL pipeline")

        downloader = ESMADownloader(url=self.config.esma_url)
        dltins_xml = downloader.download_dltins_xml(index=1)

        parser = DLTINSParser()
        df = parser.parse(dltins_xml)

        transformer = DataTransformer()
        df = transformer.transform(df)

        storage = DataStorage(storage_path=self.config.storage_path)
        storage.save_local(df, self.config.output_path)
        storage.save_csv(df)

        logger.info("Pipeline finished successfully")
