import time
from datetime import date
from pathlib import Path

import cache
from models import Trade


SAMPLE_TRADE = Trade(
    source="insider",
    person="Test Person",
    role="Director",
    ticker="TEST",
    company="Test Co",
    transaction_type="buy",
    trade_date=date(2026, 1, 1),
    filed_date=date(2026, 1, 2),
    amount_low=None,
    amount_high=None,
    shares=10.0,
    price=1.0,
    link="https://example.com",
)


def test_round_trips_trades_through_cache_file(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    cache.set_cached_trades("insider", [SAMPLE_TRADE])
    result = cache.get_cached_trades("insider")
    assert result == [SAMPLE_TRADE]


def test_returns_none_when_no_cache_file_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    assert cache.get_cached_trades("house") is None


def test_returns_none_when_cache_is_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(cache, "CACHE_TTL", 1)
    cache.set_cached_trades("senate", [SAMPLE_TRADE])
    time.sleep(1.1)
    assert cache.get_cached_trades("senate") is None
