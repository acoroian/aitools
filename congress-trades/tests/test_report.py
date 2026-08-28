from datetime import date

from models import Trade
from report import generate_html

SAMPLE_TRADES = [
    Trade(
        source="senate",
        person="Jane Example",
        role="Senator",
        ticker="NVDA",
        company="NVIDIA Corporation",
        transaction_type="buy",
        trade_date=date(2026, 8, 12),
        filed_date=date(2026, 8, 13),
        amount_low=15001,
        amount_high=50000,
        shares=None,
        price=None,
        link="https://efdsearch.senate.gov/search/view/ptr/example/",
    ),
    Trade(
        source="insider",
        person="Mears Robert J",
        role="Chief Technology Officer",
        ticker="ATOM",
        company="Atomera Inc",
        transaction_type="sell",
        trade_date=date(2026, 8, 3),
        filed_date=date(2026, 8, 4),
        amount_low=None,
        amount_high=None,
        shares=1000,
        price=5.05,
        link="https://www.sec.gov/example.xml",
    ),
]


def test_generates_html_with_all_trades_and_source_error_notice():
    html = generate_html(
        SAMPLE_TRADES,
        meta={"days": 30, "generated_at": "2026-08-27 12:00"},
        source_errors={"house": "network timeout after 3 retries"},
    )

    assert "<html" in html
    assert "NVDA" in html
    assert "ATOM" in html
    assert "Jane Example" in html
    assert "Mears Robert J" in html
    assert "house" in html.lower()
    assert "network timeout" in html


def test_generates_valid_html_with_no_trades():
    html = generate_html([], meta={"days": 30, "generated_at": "2026-08-27 12:00"}, source_errors={})
    assert "<html" in html
    assert "No trades" in html
