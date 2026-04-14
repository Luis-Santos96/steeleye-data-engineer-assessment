"""Module for parsing DLTINS XML into a pandas DataFrame."""

import logging

import pandas as pd
from lxml import etree

from steeleye.exceptions import ParsingError

logger = logging.getLogger(__name__)

NAMESPACES = {
    "head": "urn:iso:std:iso:20022:tech:xsd:head.003.001.01",
    "doc": "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02",
}

RECORD_TAGS = [
    "TermntdRcrd",
    "NewRcrd",
    "ModfdRcrd",
]

COLUMN_MAP = {
    "FinInstrmGnlAttrbts.Id": "Id",
    "FinInstrmGnlAttrbts.FullNm": "FullNm",
    "FinInstrmGnlAttrbts.ClssfctnTp": "ClssfctnTp",
    "FinInstrmGnlAttrbts.CmmdtyDerivInd": "CmmdtyDerivInd",
    "FinInstrmGnlAttrbts.NtnlCcy": "NtnlCcy",
}


class DLTINSParser:
    """Parses DLTINS XML files into a structured pandas DataFrame.

    Extracts financial instrument attributes and issuer information
    from ESMA FIRDS DLTINS delta report XML files.
    """

    def _find_text(
        self, element: etree._Element, tag: str, ns: str
    ) -> str:
        """Find text content of a child element.

        Args:
            element: Parent XML element.
            tag: Tag name to search for.
            ns: Namespace prefix key.

        Returns:
            Text content of the element, or empty string if not found.
        """
        node = element.find(f"{{{NAMESPACES[ns]}}}{tag}")
        if node is not None and node.text:
            return node.text
        return ""

    def _parse_record(
        self, record: etree._Element, ns: str
    ) -> dict[str, str]:
        """Parse a single instrument record into a flat dictionary.

        Args:
            record: XML element for a single instrument record.
            ns: Namespace prefix key for the document.

        Returns:
            Dictionary with column names as keys and text values.
        """
        attrs = record.find(
            f"{{{NAMESPACES[ns]}}}FinInstrmGnlAttrbts"
        )

        row: dict[str, str] = {}
        if attrs is not None:
            for col_name, tag in COLUMN_MAP.items():
                row[col_name] = self._find_text(attrs, tag, ns)
        else:
            for col_name in COLUMN_MAP:
                row[col_name] = ""

        row["Issr"] = self._find_text(record, "Issr", ns)
        return row

    def parse(self, xml_content: bytes) -> pd.DataFrame:
        """Parse DLTINS XML content into a pandas DataFrame.

        Args:
            xml_content: Raw XML content from a DLTINS file.

        Returns:
            DataFrame with financial instrument attributes.

        Raises:
            ParsingError: If XML parsing fails or no records are found.
        """
        logger.info("Parsing DLTINS XML content")
        try:
            root = etree.fromstring(xml_content)
        except etree.XMLSyntaxError as exc:
            raise ParsingError(
                f"Failed to parse DLTINS XML: {exc}"
            ) from exc

        ns = "doc"
        records: list[dict[str, str]] = []

        for tag in RECORD_TAGS:
            for record in root.iter(f"{{{NAMESPACES[ns]}}}{tag}"):
                records.append(self._parse_record(record, ns))

        if not records:
            raise ParsingError("No instrument records found in XML")

        df = pd.DataFrame(records)
        logger.info("Parsed %d instrument records", len(df))
        return df
