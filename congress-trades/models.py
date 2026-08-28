"""
models.py — Shared Trade data model for congress-trades.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Optional


@dataclass
class Trade:
    """One disclosed buy/sell, normalized across House, Senate, and SEC insider sources."""

    source: str              # "house" | "senate" | "insider"
    person: str
    role: str
    ticker: str
    company: str
    transaction_type: str    # "buy" | "sell" | "other"
    trade_date: date
    filed_date: date
    amount_low: Optional[float]
    amount_high: Optional[float]
    shares: Optional[float]
    price: Optional[float]
    link: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["trade_date"] = self.trade_date.isoformat()
        d["filed_date"] = self.filed_date.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Trade":
        d = dict(d)
        d["trade_date"] = date.fromisoformat(d["trade_date"])
        d["filed_date"] = date.fromisoformat(d["filed_date"])
        return cls(**d)
