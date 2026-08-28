import io
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from house import parse_ptr_pdf, fetch_trades, _parse_ptr_text

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


def test_row_with_out_of_format_amount_is_dropped_not_fabricated_from_next_row():
    """An open-ended amount band like "$50,000,001 +" doesn't match _AMOUNT_RE
    at all. The row's [ST] code must not get paired with some other, distant
    row's amount/date — it should be dropped instead."""
    malformed_row = (
        "XYZ Corp - Common Stock (XYZ) S (partial) 03/16/2026 03/16/2026 "
        "$50,000,001 +\n[ST]\n"
    )
    # Padding pushes the only real amount match (on the well-formed row
    # below) more than _MAX_PAIRING_DISTANCE chars away from XYZ's bracket.
    padding = "Filler description text about unrelated matters. " * 12
    well_formed_row = (
        "ABC Corp - Common Stock (ABC) S (partial) 03/17/2026 03/17/2026 "
        "$1,001 - $15,000\n[ST]\n"
    )
    text = malformed_row + padding + well_formed_row

    trades = _parse_ptr_text(
        text, person="Test Person", filed_date=date(2026, 3, 31), link="https://example.com/x.pdf"
    )

    tickers = [t.ticker for t in trades]
    assert "XYZ" not in tickers
    assert tickers == ["ABC"]
    assert trades[0].trade_date == date(2026, 3, 17)
    assert trades[0].amount_low == 1001
    assert trades[0].amount_high == 15000


def test_row_with_no_parenthesized_ticker_and_ambiguous_prose_is_dropped():
    """With no parenthesized ticker, and the asset-type-code bracket on its
    own line (not sharing a line with any uppercase word), _find_ticker must
    not fabricate a ticker from unrelated preceding prose (e.g. "LLC")."""
    text = (
        "Some Private LLC Interest\n"
        "[OT] S (partial) 03/16/2026 03/16/2026 $1,001 - $15,000\n"
    )

    trades = _parse_ptr_text(
        text, person="Test Person", filed_date=date(2026, 3, 31), link="https://example.com/x.pdf"
    )

    assert trades == []


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


TWO_FILINGS_XML_INDEX = """<?xml version="1.0" encoding="utf-8"?>
<FinancialDisclosure>
  <Member>
    <Last>Broken</Last>
    <First>Rep</First>
    <FilingType>P</FilingType>
    <StateDst>MO04</StateDst>
    <Year>2026</Year>
    <FilingDate>{filed}</FilingDate>
    <DocID>10001</DocID>
  </Member>
  <Member>
    <Last>Alford</Last>
    <First>Mark</First>
    <FilingType>P</FilingType>
    <StateDst>MO04</StateDst>
    <Year>2026</Year>
    <FilingDate>{filed}</FilingDate>
    <DocID>20034201</DocID>
  </Member>
</FinancialDisclosure>
"""


def test_fetch_trades_skips_one_bad_filing_and_still_returns_the_next():
    """One filing's PDF fetch raises; a second, good filing must still make
    it through — and the failure must be logged, not silently swallowed."""
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    zip_bytes = _zip_bytes(TWO_FILINGS_XML_INDEX.format(filed=today))
    pdf_bytes = FIXTURE.read_bytes()

    session = MagicMock()

    def fake_get(url, **kwargs):
        if url.endswith("10001.pdf"):
            raise ConnectionError("simulated fetch failure")
        response = MagicMock()
        response.status_code = 200
        if url.endswith(".zip"):
            response.content = zip_bytes
        elif url.endswith("20034201.pdf"):
            response.content = pdf_bytes
        return response

    session.get.side_effect = fake_get

    with patch("house.courtesy_delay"), \
         patch("house.logger") as mock_logger:
        trades = fetch_trades(session, days=1)

    assert len(trades) == 9
    assert all(t.person == "Mark Alford" for t in trades)
    mock_logger.warning.assert_called_once()
    assert "10001.pdf" in mock_logger.warning.call_args.args[1]
