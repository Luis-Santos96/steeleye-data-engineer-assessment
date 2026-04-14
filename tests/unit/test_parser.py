"""Unit tests for the DLTINS XML parser module."""

import pytest

from steeleye.exceptions import ParsingError
from steeleye.parser import DLTINSParser

SAMPLE_DLTINS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<BizData xmlns="urn:iso:std:iso:20022:tech:xsd:head.003.001.01">
  <Pyld>
    <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
      <FinInstrmRptgRefDataDltaRpt>
        <FinInstrm>
          <TermntdRcrd>
            <FinInstrmGnlAttrbts>
              <Id>ISIN123456</Id>
              <FullNm>Test Instrument Alpha</FullNm>
              <ClssfctnTp>ABCD</ClssfctnTp>
              <CmmdtyDerivInd>false</CmmdtyDerivInd>
              <NtnlCcy>EUR</NtnlCcy>
            </FinInstrmGnlAttrbts>
            <Issr>ISSUER001</Issr>
          </TermntdRcrd>
        </FinInstrm>
        <FinInstrm>
          <NewRcrd>
            <FinInstrmGnlAttrbts>
              <Id>ISIN789012</Id>
              <FullNm>Bond Beta</FullNm>
              <ClssfctnTp>EFGH</ClssfctnTp>
              <CmmdtyDerivInd>true</CmmdtyDerivInd>
              <NtnlCcy>USD</NtnlCcy>
            </FinInstrmGnlAttrbts>
            <Issr>ISSUER002</Issr>
          </NewRcrd>
        </FinInstrm>
      </FinInstrmRptgRefDataDltaRpt>
    </Document>
  </Pyld>
</BizData>
"""

SAMPLE_NO_RECORDS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<BizData xmlns="urn:iso:std:iso:20022:tech:xsd:head.003.001.01">
  <Pyld>
    <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
      <FinInstrmRptgRefDataDltaRpt>
      </FinInstrmRptgRefDataDltaRpt>
    </Document>
  </Pyld>
</BizData>
"""


@pytest.fixture
def parser() -> DLTINSParser:
    """Create a DLTINSParser instance."""
    return DLTINSParser()


class TestParse:
    """Tests for parsing DLTINS XML into a DataFrame."""

    def test_parses_records_correctly(self, parser: DLTINSParser) -> None:
        """Should extract all instrument records into a DataFrame."""
        df = parser.parse(SAMPLE_DLTINS_XML)

        assert len(df) == 2
        assert df.iloc[0]["FinInstrmGnlAttrbts.Id"] == "ISIN123456"
        assert df.iloc[0]["FinInstrmGnlAttrbts.FullNm"] == "Test Instrument Alpha"
        assert df.iloc[0]["FinInstrmGnlAttrbts.ClssfctnTp"] == "ABCD"
        assert df.iloc[0]["FinInstrmGnlAttrbts.CmmdtyDerivInd"] == "false"
        assert df.iloc[0]["FinInstrmGnlAttrbts.NtnlCcy"] == "EUR"
        assert df.iloc[0]["Issr"] == "ISSUER001"

    def test_parses_different_record_types(
        self, parser: DLTINSParser
    ) -> None:
        """Should handle both TermntdRcrd and NewRcrd types."""
        df = parser.parse(SAMPLE_DLTINS_XML)

        assert df.iloc[1]["FinInstrmGnlAttrbts.Id"] == "ISIN789012"
        assert df.iloc[1]["Issr"] == "ISSUER002"

    def test_has_expected_columns(self, parser: DLTINSParser) -> None:
        """Should produce DataFrame with all required columns."""
        df = parser.parse(SAMPLE_DLTINS_XML)
        expected_columns = [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]

        for col in expected_columns:
            assert col in df.columns

    def test_no_records_raises_parsing_error(
        self, parser: DLTINSParser
    ) -> None:
        """Should raise ParsingError when no records are found."""
        with pytest.raises(ParsingError, match="No instrument records"):
            parser.parse(SAMPLE_NO_RECORDS_XML)

    def test_invalid_xml_raises_parsing_error(
        self, parser: DLTINSParser
    ) -> None:
        """Should raise ParsingError on malformed XML."""
        with pytest.raises(ParsingError, match="Failed to parse DLTINS XML"):
            parser.parse(b"<broken>xml")
