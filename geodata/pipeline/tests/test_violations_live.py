"""Live smoke tests — hit real CMS/CDPH endpoints.

Excluded from the default `pytest` run. To invoke:
  uv run pytest -m live tests/test_violations_live.py -v
"""

import pytest

from pipeline.ingest.cdph_sea import discover_latest_xlsx_url
from pipeline.ingest.cms_nh_compare import (
    discover_latest_csv_url,
    download_csv,
    filter_to_ca,
    parse_csv,
)


@pytest.mark.live
def test_cms_metadata_endpoint_resolves_csv_url():
    url = discover_latest_csv_url()
    assert url.endswith(".csv")
    assert "NH_" in url or "Health" in url


@pytest.mark.live
def test_cms_csv_parses_and_has_ca_rows():
    url = discover_latest_csv_url()
    raw = download_csv(url)
    df = parse_csv(raw)
    assert len(df) > 1000
    ca = filter_to_ca(df)
    assert len(ca) > 100
    assert "scope_severity_code" in ca.columns


@pytest.mark.live
def test_cdph_sea_metadata_endpoint_resolves_xlsx_url():
    url = discover_latest_xlsx_url()
    assert url.endswith(".xlsx")
