"""Module for downloading and extracting ESMA FIRDS data files."""

import io
import logging
import zipfile

import requests
from lxml import etree

from steeleye.exceptions import DownloadError, ParsingError

logger = logging.getLogger(__name__)


class ESMADownloader:
    """Downloads and extracts ESMA FIRDS instrument files.

    Args:
        url: ESMA FIRDS registry URL to fetch.
        timeout: Request timeout in seconds.
    """

    def __init__(self, url: str, timeout: int = 30) -> None:
        self.url = url
        self.timeout = timeout

    def fetch_registry_xml(self) -> bytes:
        """Fetch the ESMA FIRDS registry XML.

        Returns:
            Raw XML content as bytes.

        Raises:
            DownloadError: If the HTTP request fails.
        """
        logger.info("Fetching ESMA registry XML from %s", self.url)
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise DownloadError(f"Failed to fetch registry XML: {exc}") from exc

        logger.info(
            "Registry XML fetched successfully (%d bytes)", len(response.content)
        )
        return response.content

    def parse_dltins_links(self, xml_content: bytes) -> list[str]:
        """Parse download links for DLTINS file types from the registry XML.

        Args:
            xml_content: Raw XML content from the ESMA registry.

        Returns:
            List of download URLs for DLTINS files.

        Raises:
            ParsingError: If XML parsing fails or no DLTINS links are found.
        """
        logger.info("Parsing DLTINS download links from registry XML")
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as exc:
            raise ParsingError(f"Failed to parse registry XML: {exc}") from exc

        links: list[str] = []
        for doc in root.xpath("//doc"):
            file_type = doc.xpath("str[@name='file_type']/text()")
            download_link = doc.xpath("str[@name='download_link']/text()")

            if file_type and file_type[0] == "DLTINS" and download_link:
                links.append(download_link[0])

        if not links:
            raise ParsingError("No DLTINS download links found in registry XML")

        logger.info("Found %d DLTINS download links", len(links))
        return links

    def download_zip(self, url: str) -> bytes:
        """Download a ZIP file from the given URL.

        Args:
            url: URL of the ZIP file to download.

        Returns:
            Raw ZIP content as bytes.

        Raises:
            DownloadError: If the download fails.
        """
        logger.info("Downloading ZIP file from %s", url)
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise DownloadError(f"Failed to download ZIP file: {exc}") from exc

        logger.info(
            "ZIP file downloaded successfully (%d bytes)", len(response.content)
        )
        return response.content

    def extract_xml_from_zip(self, zip_content: bytes) -> bytes:
        """Extract the first XML file from a ZIP archive.

        Args:
            zip_content: Raw ZIP file content.

        Returns:
            Raw XML content extracted from the ZIP.

        Raises:
            ParsingError: If the ZIP is invalid or contains no XML files.
        """
        logger.info("Extracting XML from ZIP archive")
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                xml_files = [f for f in zf.namelist() if f.endswith(".xml")]
                if not xml_files:
                    raise ParsingError("No XML files found in ZIP archive")

                xml_filename = xml_files[0]
                logger.info("Extracting %s from ZIP", xml_filename)
                return zf.read(xml_filename)
        except zipfile.BadZipFile as exc:
            raise ParsingError(f"Invalid ZIP archive: {exc}") from exc

    def download_dltins_xml(self, index: int = 1) -> bytes:
        """Execute the full download flow for a DLTINS file.

        Args:
            index: Zero-based index of the DLTINS link to download.
                Defaults to 1 (second link).

        Returns:
            Raw XML content of the DLTINS instrument file.

        Raises:
            DownloadError: If any download step fails.
            ParsingError: If any parsing step fails.
        """
        registry_xml = self.fetch_registry_xml()
        dltins_links = self.parse_dltins_links(registry_xml)

        if index >= len(dltins_links):
            raise ParsingError(
                f"Requested DLTINS link at index {index}, "
                f"but only {len(dltins_links)} found"
            )

        target_url = dltins_links[index]
        logger.info("Selected DLTINS link at index %d: %s", index, target_url)

        zip_content = self.download_zip(target_url)
        return self.extract_xml_from_zip(zip_content)
