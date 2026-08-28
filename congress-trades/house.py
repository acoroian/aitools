"""
house.py — Fetch and normalize U.S. House Periodic Transaction Reports (PTRs)
from the Clerk of the House's official financial disclosure index.

PTRs are PDFs with no machine-readable transaction table — text extraction
order varies row to row, so tickers and the transaction-type/date/amount
block are paired by proximity rather than assumed to be adjacent. This is
best-effort: it was validated against a real filing during planning, not
against every possible PTR layout.
"""

from __future__ import annotations

import io
import re
import zipfile
from datetime import date, timedelta
from typing import Optional
from xml.etree import ElementTree as ET

import pdfplumber

from models import Trade

CLERK_BASE = "https://disclosures-clerk.house.gov/public_disc"
TYPE_LABELS = {"P": "buy", "S": "sell", "E": "other"}

_AMOUNT_RE = re.compile(
    r"(?P<txn_type>P|S\s*\(partial\)|S\s*\(full\)|S|E)\s+"
    r"(?P<trade_date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<notif_date>\d{2}/\d{2}/\d{4})\s+"
    r"\$(?P<amt_low>[\d,]+)\s*-\s*\$(?P<amt_high>[\d,]+)"
)
_ASSET_CODE_RE = re.compile(r"\[(ST|OT|GS|MF|CO|CT|OP|PS|RP)\]")
_PAREN_TICKER_RE = re.compile(r"\(([A-Z][A-Z.&]{0,6})\)")
_BARE_TICKER_RE = re.compile(r"(?<![\w$])([A-Z]{2,6})(?![\w])")


def _find_ticker(text: str, code_match: re.Match) -> Optional[str]:
    start = max(0, code_match.start() - 200)
    window = text[start : code_match.start()]
    paren_matches = _PAREN_TICKER_RE.findall(window)
    if paren_matches:
        return paren_matches[-1]
    bare_matches = _BARE_TICKER_RE.findall(window)
    if bare_matches:
        return bare_matches[-1]
    return None


def parse_ptr_pdf(pdf_bytes: bytes, person: str, filed_date: date, link: str) -> list[Trade]:
    """Best-effort extraction of transaction line items from a House PTR PDF."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    amount_matches = list(_AMOUNT_RE.finditer(text))
    code_matches = list(_ASSET_CODE_RE.finditer(text))
    if not amount_matches or not code_matches:
        return []

    trades: list[Trade] = []
    for code_match in code_matches:
        amount_match = min(amount_matches, key=lambda m: abs(m.start() - code_match.start()))
        ticker = _find_ticker(text, code_match)
        if not ticker:
            continue

        txn_type_raw = amount_match.group("txn_type").split()[0]  # "S" from "S (partial)"
        trades.append(
            Trade(
                source="house",
                person=person,
                role="Representative",
                ticker=ticker,
                company="",
                transaction_type=TYPE_LABELS.get(txn_type_raw, "other"),
                trade_date=_parse_mmddyyyy(amount_match.group("trade_date")),
                filed_date=filed_date,
                amount_low=float(amount_match.group("amt_low").replace(",", "")),
                amount_high=float(amount_match.group("amt_high").replace(",", "")),
                shares=None,
                price=None,
                link=link,
            )
        )
    return trades


def _parse_mmddyyyy(text: str) -> date:
    month, day, year = text.split("/")
    return date(int(year), int(month), int(day))


def _recent_ptr_filings(zip_bytes: bytes, days: int, year: int) -> list[dict]:
    """Parse the Clerk's annual index ZIP into PTR filings within the lookback window."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        xml_text = zf.read(f"{year}FD.xml").decode("utf-8", errors="replace")

    root = ET.fromstring(xml_text)
    cutoff = date.today() - timedelta(days=days)
    filings = []
    for member in root.findall("Member"):
        if (member.findtext("FilingType") or "").strip() != "P":
            continue
        filing_date_text = (member.findtext("FilingDate") or "").strip()
        try:
            month, day, filing_year = filing_date_text.split("/")
            filing_date = date(int(filing_year), int(month), int(day))
        except ValueError:
            continue
        if filing_date < cutoff:
            continue
        doc_id = (member.findtext("DocID") or "").strip()
        if not doc_id:
            continue
        last = (member.findtext("Last") or "").strip()
        first = (member.findtext("First") or "").strip()
        filings.append(
            {
                "person": f"{first} {last}".strip(),
                "filed_date": filing_date,
                "doc_id": doc_id,
            }
        )
    return filings


def fetch_trades(session, days: int = 30, year: Optional[int] = None) -> list[Trade]:
    """Fetch recent House PTR filings and normalize to Trades.

    Only covers the given year's index (default: current year) — a lookback
    window that crosses a year boundary will miss filings from the prior
    year. Acceptable for the default 30-day window; a known limitation for
    wider windows near January.
    """
    year = year or date.today().year
    zip_response = session.get(f"{CLERK_BASE}/financial-pdfs/{year}FD.zip")
    filings = _recent_ptr_filings(zip_response.content, days, year)

    trades: list[Trade] = []
    for filing in filings:
        pdf_url = f"{CLERK_BASE}/ptr-pdfs/{year}/{filing['doc_id']}.pdf"
        try:
            pdf_response = session.get(pdf_url)
            trades.extend(
                parse_ptr_pdf(
                    pdf_response.content,
                    person=filing["person"],
                    filed_date=filing["filed_date"],
                    link=pdf_url,
                )
            )
        except Exception:
            continue  # one bad filing must not sink the whole fetch
    return trades
