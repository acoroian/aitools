from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

from insiders import parse_form4_xml, fetch_trades

FIXTURE = Path(__file__).parent / "fixtures" / "form4_sample.xml"


def test_parses_real_form4_sell_transaction():
    xml_bytes = FIXTURE.read_bytes()
    trades = parse_form4_xml(xml_bytes, source_url="https://example.com/form4.xml")

    assert len(trades) == 1  # the file also has a nonDerivativeHolding, which is not a transaction
    trade = trades[0]
    assert trade.source == "insider"
    assert trade.person == "Mears Robert J"
    assert trade.role == "Chief Technology Officer"
    assert trade.ticker == "ATOM"
    assert trade.company == "Atomera Inc"
    assert trade.transaction_type == "sell"
    assert trade.trade_date == date(2026, 8, 3)
    assert trade.shares == 1000.0
    assert trade.price == 5.05
    assert trade.link == "https://example.com/form4.xml"


def test_role_combines_director_and_officer_when_both_true():
    xml_bytes = FIXTURE.read_bytes().replace(
        b"<isDirector>0</isDirector>", b"<isDirector>1</isDirector>"
    )
    trades = parse_form4_xml(xml_bytes, source_url="https://example.com/form4.xml")
    assert trades[0].role == "Director, Chief Technology Officer"


ATOM_FEED = """<?xml version="1.0" encoding="ISO-8859-1"?>
<feed xmlns="http://www.w3.org/2005/Atom">
<entry>
<title>4 - Example Corp (0001111111) (Issuer)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/1111111/000222222226000001/0002222222-26-000001-index.htm"/>
<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; {filed} &lt;b&gt;AccNo:&lt;/b&gt; 0002222222-26-000001 &lt;b&gt;Size:&lt;/b&gt; 5 KB</summary>
<id>urn:tag:sec.gov,2008:accession-number=0002222222-26-000001</id>
</entry>
<entry>
<title>4 - Someone Q (0002222222) (Reporting)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/2222222/000222222226000001/0002222222-26-000001-index.htm"/>
<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; {filed} &lt;b&gt;AccNo:&lt;/b&gt; 0002222222-26-000001 &lt;b&gt;Size:&lt;/b&gt; 5 KB</summary>
<id>urn:tag:sec.gov,2008:accession-number=0002222222-26-000001</id>
</entry>
</feed>
"""

INDEX_JSON = """{"directory": {"item": [
  {"name": "0002222222-26-000001-index.html"},
  {"name": "form4.xml"}
]}}"""


def test_fetch_trades_walks_feed_then_index_then_xml():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    feed_xml = ATOM_FEED.format(filed=today)
    form4_xml = FIXTURE.read_bytes()

    session = MagicMock()

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        if "getcurrent" in url:
            response.text = feed_xml
            response.content = feed_xml.encode()
        elif url.endswith("index.json"):
            response.text = INDEX_JSON
            response.content = INDEX_JSON.encode()
        elif url.endswith("form4.xml"):
            response.content = form4_xml
        return response

    session.get.side_effect = fake_get

    trades = fetch_trades(session, days=1)

    assert len(trades) == 1
    assert trades[0].ticker == "ATOM"
    assert "form4.xml" in trades[0].link
