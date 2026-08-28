"""
insiders.py — Fetch and normalize SEC EDGAR Form 4 insider-trading filings.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional
from xml.etree import ElementTree as ET

from models import Trade

EDGAR_BASE = "https://www.sec.gov"
GETCURRENT_URL = (
    f"{EDGAR_BASE}/cgi-bin/browse-edgar"
    "?action=getcurrent&type=4&company=&dateb=&owner=include&count=100&output=atom"
)
CODE_LABELS = {"P": "buy", "S": "sell"}
ACCESSION_RE = re.compile(r"accession-number=([\d-]+)")
FILED_RE = re.compile(r"Filed:</b>\s*(\d{4}-\d{2}-\d{2})")
CIK_RE = re.compile(r"\((\d+)\)")


def _text(el: Optional[ET.Element], default: str = "") -> str:
    return el.text.strip() if el is not None and el.text else default


def _role(relationship: ET.Element) -> str:
    parts = []
    if _text(relationship.find("isDirector")) == "1":
        parts.append("Director")
    title = _text(relationship.find("officerTitle"))
    if _text(relationship.find("isOfficer")) == "1" and title:
        parts.append(title)
    if _text(relationship.find("isTenPercentOwner")) == "1":
        parts.append("10% Owner")
    return ", ".join(parts) if parts else "Other"


def parse_form4_xml(xml_bytes: bytes, source_url: str) -> list[Trade]:
    """Normalize one Form 4 ownershipDocument into zero or more non-derivative Trades."""
    root = ET.fromstring(xml_bytes)

    issuer = root.find("issuer")
    ticker = _text(issuer.find("issuerTradingSymbol")) if issuer is not None else ""
    company = _text(issuer.find("issuerName")) if issuer is not None else ""

    owner = root.find("reportingOwner")
    person = ""
    role = "Other"
    if owner is not None:
        owner_id = owner.find("reportingOwnerId")
        if owner_id is not None:
            person = _text(owner_id.find("rptOwnerName"))
        relationship = owner.find("reportingOwnerRelationship")
        if relationship is not None:
            role = _role(relationship)

    trades: list[Trade] = []
    table = root.find("nonDerivativeTable")
    if table is None:
        return trades

    for txn in table.findall("nonDerivativeTransaction"):
        coding = txn.find("transactionCoding")
        code = _text(coding.find("transactionCode")) if coding is not None else ""
        amounts = txn.find("transactionAmounts")
        shares_el = amounts.find("transactionShares") if amounts is not None else None
        price_el = amounts.find("transactionPricePerShare") if amounts is not None else None
        trade_date_text = _text(txn.find("transactionDate/value"))

        trades.append(
            Trade(
                source="insider",
                person=person,
                role=role,
                ticker=ticker,
                company=company,
                transaction_type=CODE_LABELS.get(code, "other"),
                trade_date=date.fromisoformat(trade_date_text) if trade_date_text else date.today(),
                filed_date=date.today(),
                amount_low=None,
                amount_high=None,
                shares=float(_text(shares_el.find("value"))) if shares_el is not None and _text(shares_el.find("value")) else None,
                price=float(_text(price_el.find("value"))) if price_el is not None and _text(price_el.find("value")) else None,
                link=source_url,
            )
        )
    return trades


def _recent_reporting_entries(feed_text: str, days: int) -> list[dict]:
    """Parse the EDGAR getcurrent atom feed into one dict per (Reporting) filing."""
    root = ET.fromstring(feed_text)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    cutoff = date.today() - timedelta(days=days)
    entries = []
    for entry in root.findall("a:entry", ns):
        title = _text(entry.find("a:title", ns))
        if "(Reporting)" not in title:
            continue
        link_el = entry.find("a:link", ns)
        index_url = link_el.get("href") if link_el is not None else ""
        summary = _text(entry.find("a:summary", ns))
        filed_match = FILED_RE.search(summary)
        if not filed_match:
            continue
        filed_date = date.fromisoformat(filed_match.group(1))
        if filed_date < cutoff:
            continue
        id_text = _text(entry.find("a:id", ns))
        accession_match = ACCESSION_RE.search(id_text)
        if not accession_match:
            continue
        entries.append(
            {
                "index_url": index_url,
                "accession": accession_match.group(1),
                "filed_date": filed_date,
            }
        )
    return entries


def _xml_filename_from_index(index_json_text: str) -> Optional[str]:
    import json

    data = json.loads(index_json_text)
    for item in data.get("directory", {}).get("item", []):
        name = item.get("name", "")
        if name.endswith(".xml") and "index" not in name.lower():
            return name
    return None


def fetch_trades(session, days: int = 30) -> list[Trade]:
    """Fetch recent Form 4 filings from EDGAR and normalize to Trades."""
    feed_response = session.get(GETCURRENT_URL)
    entries = _recent_reporting_entries(feed_response.text, days)

    trades: list[Trade] = []
    for entry in entries:
        index_url = entry["index_url"].replace("-index.htm", "").rsplit("/", 1)[0] + "/index.json"
        try:
            index_response = session.get(index_url)
            xml_filename = _xml_filename_from_index(index_response.text)
            if not xml_filename:
                continue
            xml_url = index_url.rsplit("/", 1)[0] + f"/{xml_filename}"
            xml_response = session.get(xml_url)
            filing_trades = parse_form4_xml(xml_response.content, source_url=xml_url)
            for t in filing_trades:
                t.filed_date = entry["filed_date"]
            trades.extend(filing_trades)
        except Exception:
            continue  # one bad filing must not sink the whole fetch
    return trades
