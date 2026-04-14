"""Unit tests for the ESMA downloader module."""

import zipfile
from io import BytesIO

import pytest
import responses

from steeleye.downloader import ESMADownloader
from steeleye.exceptions import DownloadError, ParsingError

SAMPLE_REGISTRY_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<response>
  <result name="response" numFound="2" start="0">
    <doc>
      <str name="file_type">DLTINS</str>
      <str name="download_link">https://example.com/first.zip</str>
    </doc>
    <doc>
      <str name="file_type">DLTINS</str>
      <str name="download_link">https://example.com/second.zip</str>
    </doc>
  </result>
</response>
"""

SAMPLE_REGISTRY_NO_DLTINS = b"""<?xml version="1.0" encoding="UTF-8"?>
<response>
  <result name="response" numFound="1" start="0">
    <doc>
      <str name="file_type">OTHER</str>
      <str name="download_link">https://example.com/other.zip</str>
    </doc>
  </result>
</response>
"""


def _create_test_zip(xml_content: bytes) -> bytes:
    """Create an in-memory ZIP file containing a test XML file."""
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test_data.xml", xml_content)
    return buffer.getvalue()


@pytest.fixture
def downloader() -> ESMADownloader:
    """Create an ESMADownloader instance with a test URL."""
    return ESMADownloader(url="https://example.com/registry", timeout=10)


class TestFetchRegistryXml:
    """Tests for fetching the ESMA registry XML."""

    @responses.activate
    def test_success(self, downloader: ESMADownloader) -> None:
        """Should return XML content on successful request."""
        responses.add(
            responses.GET,
            "https://example.com/registry",
            body=SAMPLE_REGISTRY_XML,
            status=200,
        )

        result = downloader.fetch_registry_xml()

        assert result == SAMPLE_REGISTRY_XML

    @responses.activate
    def test_http_error_raises_download_error(self, downloader: ESMADownloader) -> None:
        """Should raise DownloadError on HTTP failure."""
        responses.add(
            responses.GET,
            "https://example.com/registry",
            status=500,
        )

        with pytest.raises(DownloadError, match="Failed to fetch registry XML"):
            downloader.fetch_registry_xml()


class TestParseDltinsLinks:
    """Tests for parsing DLTINS download links from XML."""

    def test_extracts_dltins_links(self, downloader: ESMADownloader) -> None:
        """Should extract all DLTINS download links."""
        links = downloader.parse_dltins_links(SAMPLE_REGISTRY_XML)

        assert len(links) == 2
        assert links[0] == "https://example.com/first.zip"
        assert links[1] == "https://example.com/second.zip"

    def test_no_dltins_raises_parsing_error(self, downloader: ESMADownloader) -> None:
        """Should raise ParsingError when no DLTINS links exist."""
        with pytest.raises(ParsingError, match="No DLTINS download links"):
            downloader.parse_dltins_links(SAMPLE_REGISTRY_NO_DLTINS)

    def test_invalid_xml_raises_parsing_error(self, downloader: ESMADownloader) -> None:
        """Should raise ParsingError on malformed XML."""
        with pytest.raises(ParsingError, match="Failed to parse registry XML"):
            downloader.parse_dltins_links(b"<invalid>xml")


class TestDownloadZip:
    """Tests for downloading ZIP files."""

    @responses.activate
    def test_success(self, downloader: ESMADownloader) -> None:
        """Should return ZIP content on successful download."""
        zip_bytes = _create_test_zip(b"<xml>test</xml>")
        responses.add(
            responses.GET,
            "https://example.com/test.zip",
            body=zip_bytes,
            status=200,
        )

        result = downloader.download_zip("https://example.com/test.zip")

        assert result == zip_bytes

    @responses.activate
    def test_http_error_raises_download_error(self, downloader: ESMADownloader) -> None:
        """Should raise DownloadError on HTTP failure."""
        responses.add(
            responses.GET,
            "https://example.com/test.zip",
            status=404,
        )

        with pytest.raises(DownloadError, match="Failed to download ZIP"):
            downloader.download_zip("https://example.com/test.zip")


class TestExtractXmlFromZip:
    """Tests for extracting XML from ZIP archives."""

    def test_extracts_xml_file(self, downloader: ESMADownloader) -> None:
        """Should extract the first XML file from the archive."""
        xml_content = b"<xml>test data</xml>"
        zip_bytes = _create_test_zip(xml_content)

        result = downloader.extract_xml_from_zip(zip_bytes)

        assert result == xml_content

    def test_no_xml_raises_parsing_error(self, downloader: ESMADownloader) -> None:
        """Should raise ParsingError when ZIP contains no XML files."""
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("data.csv", "col1,col2")
        zip_bytes = buffer.getvalue()

        with pytest.raises(ParsingError, match="No XML files found"):
            downloader.extract_xml_from_zip(zip_bytes)

    def test_invalid_zip_raises_parsing_error(self, downloader: ESMADownloader) -> None:
        """Should raise ParsingError on invalid ZIP data."""
        with pytest.raises(ParsingError, match="Invalid ZIP archive"):
            downloader.extract_xml_from_zip(b"not a zip file")


class TestDownloadDltinsXml:
    """Tests for the full download flow."""

    @responses.activate
    def test_full_flow(self, downloader: ESMADownloader) -> None:
        """Should fetch registry, select second link, download and extract."""
        xml_content = b"<xml>instrument data</xml>"
        zip_bytes = _create_test_zip(xml_content)

        responses.add(
            responses.GET,
            "https://example.com/registry",
            body=SAMPLE_REGISTRY_XML,
            status=200,
        )
        responses.add(
            responses.GET,
            "https://example.com/second.zip",
            body=zip_bytes,
            status=200,
        )

        result = downloader.download_dltins_xml(index=1)

        assert result == xml_content

    @responses.activate
    def test_index_out_of_range_raises_parsing_error(
        self, downloader: ESMADownloader
    ) -> None:
        """Should raise ParsingError when index exceeds available links."""
        responses.add(
            responses.GET,
            "https://example.com/registry",
            body=SAMPLE_REGISTRY_XML,
            status=200,
        )

        with pytest.raises(ParsingError, match="Requested DLTINS link at index"):
            downloader.download_dltins_xml(index=5)
