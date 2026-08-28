from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from models import Trade
from trades import run

SAMPLE_TRADE = Trade(
    source="house", person="Test Rep", role="Representative", ticker="AAPL",
    company="", transaction_type="buy", trade_date=date(2026, 8, 1),
    filed_date=date(2026, 8, 2), amount_low=1001, amount_high=15000,
    shares=None, price=None, link="https://example.com",
)


def test_run_writes_report_and_returns_zero_when_trades_found(tmp_path):
    with patch("trades.house.fetch_trades", return_value=[SAMPLE_TRADE]), \
         patch("trades.senate.fetch_trades", return_value=[]), \
         patch("trades.insiders.fetch_trades", return_value=[]), \
         patch("trades.cache.get_cached_trades", return_value=None), \
         patch("trades.cache.set_cached_trades"):
        exit_code = run(days=30, sources=["house", "senate", "insider"], no_cache=True,
                         output_dir=tmp_path, quiet=True)

    assert exit_code == 0
    assert (tmp_path / "congress_trades_report.html").exists()


def test_run_returns_one_when_no_trades_found(tmp_path):
    with patch("trades.house.fetch_trades", return_value=[]), \
         patch("trades.senate.fetch_trades", return_value=[]), \
         patch("trades.insiders.fetch_trades", return_value=[]), \
         patch("trades.cache.get_cached_trades", return_value=None), \
         patch("trades.cache.set_cached_trades"):
        exit_code = run(days=30, sources=["house", "senate", "insider"], no_cache=True,
                         output_dir=tmp_path, quiet=True)

    assert exit_code == 1


def test_run_returns_two_when_every_source_fails(tmp_path):
    with patch("trades.house.fetch_trades", side_effect=RuntimeError("boom")), \
         patch("trades.senate.fetch_trades", side_effect=RuntimeError("boom")), \
         patch("trades.insiders.fetch_trades", side_effect=RuntimeError("boom")), \
         patch("trades.cache.get_cached_trades", return_value=None), \
         patch("trades.cache.set_cached_trades"):
        exit_code = run(days=30, sources=["house", "senate", "insider"], no_cache=True,
                         output_dir=tmp_path, quiet=True)

    assert exit_code == 2


def test_run_continues_when_one_source_fails(tmp_path):
    with patch("trades.house.fetch_trades", return_value=[SAMPLE_TRADE]), \
         patch("trades.senate.fetch_trades", side_effect=RuntimeError("boom")), \
         patch("trades.insiders.fetch_trades", return_value=[]), \
         patch("trades.cache.get_cached_trades", return_value=None), \
         patch("trades.cache.set_cached_trades"):
        exit_code = run(days=30, sources=["house", "senate", "insider"], no_cache=True,
                         output_dir=tmp_path, quiet=True)

    assert exit_code == 0
    report_text = (tmp_path / "congress_trades_report.html").read_text()
    assert "senate" in report_text.lower()
    assert "boom" in report_text
