from datetime import date

from models import Trade


def test_trade_to_dict_and_back_round_trips():
    trade = Trade(
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
        shares=1000.0,
        price=5.05,
        link="https://www.sec.gov/Archives/edgar/data/1420520/example.xml",
    )

    d = trade.to_dict()
    assert d["trade_date"] == "2026-08-03"
    assert d["filed_date"] == "2026-08-04"

    restored = Trade.from_dict(d)
    assert restored == trade
