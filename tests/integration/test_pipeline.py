"""Integration tests for the full ETL pipeline."""

import zipfile
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import responses
from moto import mock_aws

from steeleye.config import PipelineConfig
from steeleye.pipeline import Pipeline

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

SAMPLE_DLTINS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<BizData xmlns="urn:iso:std:iso:20022:tech:xsd:head.003.001.01">
  <Pyld>
    <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
      <FinInstrmRptgRefDataDltaRpt>
        <FinInstrm>
          <TermntdRcrd>
            <FinInstrmGnlAttrbts>
              <Id>ISIN001</Id>
              <FullNm>Test Alpha Instrument</FullNm>
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
              <Id>ISIN002</Id>
              <FullNm>XYZ Bond</FullNm>
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


def _create_test_zip(xml_content: bytes) -> bytes:
    """Create an in-memory ZIP containing an XML file."""
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("DLTINS_20210119_01of02.xml", xml_content)
    return buffer.getvalue()


class TestPipelineEndToEnd:
    """End-to-end tests for the full ETL pipeline."""

    @mock_aws
    @responses.activate
    def test_full_pipeline(self, tmp_path: Path) -> None:
        """Should download, parse, transform, and store data."""
        responses.add(
            responses.GET,
            "https://example.com/registry",
            body=SAMPLE_REGISTRY_XML,
            status=200,
        )
        responses.add(
            responses.GET,
            "https://example.com/second.zip",
            body=_create_test_zip(SAMPLE_DLTINS_XML),
            status=200,
        )

        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")

        output_path = str(tmp_path / "output.csv")
        config = PipelineConfig(
            esma_url="https://example.com/registry",
            output_path=output_path,
            storage_path="s3://test-bucket/output.csv",
            storage_backend="s3",
        )

        pipeline = Pipeline(config)
        pipeline.run()

        # Verify local CSV
        assert Path(output_path).exists()
        df = pd.read_csv(output_path)
        assert len(df) == 2

        # Verify columns
        assert "FinInstrmGnlAttrbts.Id" in df.columns
        assert "a_count" in df.columns
        assert "contains_a" in df.columns

        # Verify transformations
        alpha_row = df[df["FinInstrmGnlAttrbts.Id"] == "ISIN001"]
        assert alpha_row.iloc[0]["a_count"] == 1
        assert alpha_row.iloc[0]["contains_a"] == "YES"

        xyz_row = df[df["FinInstrmGnlAttrbts.Id"] == "ISIN002"]
        assert xyz_row.iloc[0]["a_count"] == 0
        assert xyz_row.iloc[0]["contains_a"] == "NO"

        # Verify S3 upload
        s3_obj = conn.get_object(Bucket="test-bucket", Key="output.csv")
        s3_content = s3_obj["Body"].read().decode("utf-8")
        assert "ISIN001" in s3_content
