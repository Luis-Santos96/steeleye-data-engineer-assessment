"""Custom exceptions for the SteelEye ETL pipeline."""


class SteelEyeError(Exception):
    """Base exception for all pipeline errors."""


class DownloadError(SteelEyeError):
    """Raised when a file download fails."""


class ParsingError(SteelEyeError):
    """Raised when XML parsing fails."""


class StorageError(SteelEyeError):
    """Raised when storing output to a backend fails."""
