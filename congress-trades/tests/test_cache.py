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
    cache.set_cached_trades("insider", [SAMPLE_TRADE], 30)
    result = cache.get_cached_trades("insider", 30)
    assert result == [SAMPLE_TRADE]


def test_returns_none_when_no_cache_file_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    assert cache.get_cached_trades("house", 30) is None


def test_returns_none_when_cache_is_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(cache, "CACHE_TTL", 1)
    cache.set_cached_trades("senate", [SAMPLE_TRADE], 30)
    time.sleep(1.1)
    assert cache.get_cached_trades("senate", 30) is None


def test_cache_built_for_7_days_does_not_satisfy_90_day_request(tmp_path, monkeypatch):
    """A narrower cached window must not silently serve a wider request —
    it may be missing filings that fall outside the 7-day window it covers."""
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    cache.set_cached_trades("house", [SAMPLE_TRADE], 7)
    assert cache.get_cached_trades("house", 90) is None


def test_cache_built_for_90_days_does_satisfy_7_day_request(tmp_path, monkeypatch):
    """A cache built for a wider or equal window can still serve a narrower
    request — it necessarily covers the narrower window too."""
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    cache.set_cached_trades("house", [SAMPLE_TRADE], 90)
    result = cache.get_cached_trades("house", 7)
    assert result == [SAMPLE_TRADE]
