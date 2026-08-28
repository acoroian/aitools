import io
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from house import parse_ptr_pdf, fetch_trades

FIXTURE = Path(__file__).parent / "fixtures" / "house_ptr_sample.pdf"


def test_parses_real_house_ptr_pdf():
    pdf_bytes = FIXTURE.read_bytes()
    trades = parse_ptr_pdf(
        pdf_bytes,
        person="Hon. Mark Alford",
        filed_date=date(2026, 3, 31),
        link="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034201.pdf",
    )

    tickers = [t.ticker for t in trades]
    assert tickers == ["AMZN", "AAPL", "T", "BRK.B", "DIA", "QQQ", "PYPL", "SPYB", "SPYB"]

    for t in trades:
        assert t.source == "house"
        assert t.person == "Hon. Mark Alford"
        assert t.transaction_type == "sell"
        assert t.trade_date == date(2026, 3, 16)
        assert t.amount_low == 1001
        assert t.amount_high == 15000
        assert t.link.endswith("20034201.pdf")


XML_INDEX = """<?xml version="1.0" encoding="utf-8"?>
<FinancialDisclosure>
  <Member>
    <Last>Alford</Last>
    <First>Mark</First>
    <FilingType>P</FilingType>
    <StateDst>MO04</StateDst>
    <Year>2026</Year>
    <FilingDate>{filed}</FilingDate>
    <DocID>20034201</DocID>
  </Member>
  <Member>
    <Last>Someone</Last>
    <First>Else</First>
    <FilingType>C</FilingType>
    <StateDst>TX01</StateDst>
    <Year>2026</Year>
    <FilingDate>{filed}</FilingDate>
    <DocID>99999</DocID>
  </Member>
</FinancialDisclosure>
"""


def _zip_bytes(xml_text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("2026FD.xml", xml_text)
    return buf.getvalue()


def test_fetch_trades_filters_to_ptr_filings_and_parses_pdf():
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    zip_bytes = _zip_bytes(XML_INDEX.format(filed=today))
    pdf_bytes = FIXTURE.read_bytes()

    session = MagicMock()

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        if url.endswith(".zip"):
            response.content = zip_bytes
        elif url.endswith("20034201.pdf"):
            response.content = pdf_bytes
        return response

    session.get.side_effect = fake_get

    trades = fetch_trades(session, days=1)

    assert len(trades) == 9
    assert all(t.person == "Mark Alford" for t in trades)
    assert all(t.source == "house" for t in trades)
