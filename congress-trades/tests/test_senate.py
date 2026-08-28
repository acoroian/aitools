import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

from senate import SenateUnavailableError, fetch_trades, parse_ptr_html

SAMPLE_PTR_HTML = """
<html><body>
<table class="table">
<thead><tr>
<th>Transaction Date</th><th>Owner</th><th>Ticker</th><th>Asset Name</th>
<th>Asset Type</th><th>Type</th><th>Amount</th><th>Comment</th>
</tr></thead>
<tbody>
<tr>
<td>08/12/2026</td><td>Self</td><td>NVDA</td><td>NVIDIA Corporation</td>
<td>Stock</td><td>Purchase</td><td>$15,001 - $50,000</td><td>--</td>
</tr>
<tr>
<td>08/12/2026</td><td>Spouse</td><td>MSFT</td><td>Microsoft Corporation</td>
<td>Stock</td><td>Sale (Full)</td><td>$1,001 - $15,000</td><td>--</td>
</tr>
</tbody>
</table>
</body></html>
"""


def test_parses_transaction_table_by_header_name():
    trades = parse_ptr_html(
        SAMPLE_PTR_HTML,
        person="Jane Example",
        filed_date=date(2026, 8, 13),
        link="https://efdsearch.senate.gov/search/view/ptr/example/",
    )

    assert len(trades) == 2
    buy, sell = trades
    assert buy.ticker == "NVDA"
    assert buy.transaction_type == "buy"
    assert buy.amount_low == 15001
    assert buy.amount_high == 50000
    assert buy.trade_date == date(2026, 8, 12)
    assert buy.source == "senate"
    assert buy.company == "NVIDIA Corporation"

    assert sell.ticker == "MSFT"
    assert sell.transaction_type == "sell"


SEARCH_PAGE_HTML = """
<html><body><form>
<input type="hidden" name="csrfmiddlewaretoken" value="testtoken123">
<input type="checkbox" name="prohibition_agreement" value="1">
</form></body></html>
"""

SEARCH_RESULTS_JSON = json.dumps(
    {
        "data": [
            [
                "<a href='/search/view/annual/x/'>Jane Example</a>",
                "Senator",
                "<a href='/search/view/ptr/example/'>Periodic Transaction Report</a>",
                "08/13/2026",
            ]
        ],
        "recordsTotal": 1,
        "recordsFiltered": 1,
    }
)


def test_fetch_trades_does_handshake_then_search_then_parses_ptr():
    session = MagicMock()
    session.cookies = {"csrftoken": "cookievalue"}

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        if url == "https://efdsearch.senate.gov/search/":
            response.text = SEARCH_PAGE_HTML
        elif "view/ptr/example" in url:
            response.text = SAMPLE_PTR_HTML
        return response

    def fake_post(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        response.headers = {"Content-Type": "application/json"}
        if url == "https://efdsearch.senate.gov/search/home/":
            response.text = "ok"
        elif url == "https://efdsearch.senate.gov/search/report/data/":
            response.text = SEARCH_RESULTS_JSON
            response.json.return_value = json.loads(SEARCH_RESULTS_JSON)
        return response

    session.get.side_effect = fake_get
    session.post.side_effect = fake_post

    trades = fetch_trades(session, days=7)

    assert len(trades) == 2
    assert trades[0].person == "Jane Example"
    assert trades[0].source == "senate"


def test_fetch_trades_raises_when_site_is_under_maintenance():
    session = MagicMock()
    session.cookies = {"csrftoken": "cookievalue"}

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        response.text = SEARCH_PAGE_HTML
        return response

    def fake_post(url, **kwargs):
        response = MagicMock()
        response.status_code = 503
        response.headers = {"Content-Type": "text/html"}
        response.text = "<title>U.S. Senate: Site Under Maintenance</title>"
        return response

    session.get.side_effect = fake_get
    session.post.side_effect = fake_post

    try:
        fetch_trades(session, days=7)
        assert False, "expected SenateUnavailableError"
    except SenateUnavailableError:
        pass
