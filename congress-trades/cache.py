"""
cache.py — Per-source JSON cache with TTL, mirroring analysis/cache.py.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

from models import Trade

CACHE_DIR = Path(__file__).parent / "cache"
CACHE_TTL = 3600 * 6  # 6 hours — trades are disclosed daily, not intraday


def _cache_file(source: str) -> Path:
    return CACHE_DIR / f"{source}_cache.json"


def load_cache(source: str) -> dict[str, Any]:
    """Load the raw cache envelope for a source. Empty dict if missing/corrupt."""
    try:
        return json.loads(_cache_file(source).read_text())
    except Exception:
        return {}


def save_cache(source: str, envelope: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_file(source).write_text(json.dumps(envelope, indent=2))


def get_cached_trades(source: str, days: int) -> Optional[list[Trade]]:
    """Return cached trades for a source if present, within TTL, and built for
    a lookback window at least as wide as the one requested — else None.

    A cache built for a narrower window (e.g. --days 7) must not silently
    serve a wider request (e.g. --days 90): it may be missing filings from
    the extra window. A cache built for an equal or wider window can still
    serve a narrower request.
    """
    envelope = load_cache(source)
    if not envelope:
        return None
    if time.time() - envelope.get("ts", 0) > CACHE_TTL:
        return None
    if envelope.get("days", 0) < days:
        return None
    return [Trade.from_dict(d) for d in envelope.get("trades", [])]


def set_cached_trades(source: str, trades: list[Trade], days: int) -> None:
    save_cache(source, {"ts": time.time(), "days": days, "trades": [t.to_dict() for t in trades]})
