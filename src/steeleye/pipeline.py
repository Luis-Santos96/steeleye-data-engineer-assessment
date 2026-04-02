"""Orchestrator for the ESMA FIRDS ETL pipeline."""

import logging

from steeleye.config import PipelineConfig

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates the full ETL pipeline: download, parse, transform, store.

    Args:
        config: Pipeline configuration instance.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def run(self) -> None:
        """Execute the full ETL pipeline."""
        logger.info("Starting ESMA FIRDS ETL pipeline")
        logger.info("Pipeline finished successfully")
