"""
senate.py — Fetch and normalize Senate Periodic Transaction Reports (PTRs)
from the official efdsearch.senate.gov system.

efdsearch.senate.gov requires a short session handshake before any search:
GET /search/ for a CSRF token + session cookie, then POST that token plus
prohibition_agreement=1 to /search/home/. Only after that does
/search/report/data/ (a DataTables endpoint) accept search POSTs.
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from bs4 import BeautifulSoup

from http_client import courtesy_delay
from models import Trade

logger = logging.getLogger(__name__)

EFD_BASE = "https://efdsearch.senate.gov"
SEARCH_PAGE_URL = f"{EFD_BASE}/search/"
AGREEMENT_URL = f"{EFD_BASE}/search/home/"
SEARCH_DATA_URL = f"{EFD_BASE}/search/report/data/"
PTR_REPORT_TYPE = 11

TYPE_LABELS = {"purchase": "buy", "sale": "sell", "sale (partial)": "sell", "sale (full)": "sell"}

_HEADER_TO_FIELD = {
    "transaction date": "trade_date",
    "owner": "owner",
    "ticker": "ticker",
    "asset name": "company",
    "asset type": "asset_type",
    "type": "transaction_type",
    "amount": "amount",
    "comment": "comment",
}

_AMOUNT_RE = re.compile(r"\$([\d,]+)\s*-\s*\$([\d,]+)")


class SenateUnavailableError(RuntimeError):
    """Raised when efdsearch.senate.gov doesn't return the expected search UI/JSON."""


def parse_ptr_html(html: str, person: str, filed_date: date, link: str) -> list[Trade]:
    """Parse one PTR view page's transaction table into Trades, by header name."""
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for candidate in soup.find_all("table"):
        header_cells = [th.get_text(strip=True).lower() for th in candidate.find_all("th")]
        if "ticker" in header_cells and "transaction date" in header_cells:
            table = candidate
            break
    if table is None:
        return []

    # html.parser (unlike a browser or lxml) does not synthesize missing
    # <thead>/<tbody> wrapper tags — fall back to searching the whole table
    # for <th>/<tr> elements directly when the wrapper is absent.
    header_container = table.find("thead") or table
    headers = [th.get_text(strip=True).lower() for th in header_container.find_all("th")]
    fields = [_HEADER_TO_FIELD.get(h) for h in headers]

    body_container = table.find("tbody") or table
    trades: list[Trade] = []
    for row in body_container.find_all("tr"):
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cells:
            continue  # header row (or other non-data row) picked up by the fallback search
        row_data = {field: value for field, value in zip(fields, cells) if field}

        ticker = row_data.get("ticker", "").strip()
        if not ticker or ticker == "--":
            continue

        amount_match = _AMOUNT_RE.search(row_data.get("amount", ""))
        amount_low = float(amount_match.group(1).replace(",", "")) if amount_match else None
        amount_high = float(amount_match.group(2).replace(",", "")) if amount_match else None

        trade_date_text = row_data.get("trade_date", "")
        try:
            month, day, year = trade_date_text.split("/")
            trade_date = date(int(year), int(month), int(day))
        except ValueError:
            continue

        txn_type_raw = row_data.get("transaction_type", "").strip().lower()

        trades.append(
            Trade(
                source="senate",
                person=person,
                role="Senator",
                ticker=ticker,
                company=row_data.get("company", ""),
                transaction_type=TYPE_LABELS.get(txn_type_raw, "other"),
                trade_date=trade_date,
                filed_date=filed_date,
                amount_low=amount_low,
                amount_high=amount_high,
                shares=None,
                price=None,
                link=link,
            )
        )
    return trades


def _start_session(session) -> str:
    """GET the search page for a CSRF token, then accept the prohibition agreement."""
    page = session.get(SEARCH_PAGE_URL)
    match = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', page.text)
    if not match:
        raise SenateUnavailableError("could not find CSRF token on efdsearch.senate.gov")
    csrf_token = match.group(1)

    session.post(
        AGREEMENT_URL,
        data={"csrfmiddlewaretoken": csrf_token, "prohibition_agreement": "1"},
        headers={"Referer": SEARCH_PAGE_URL},
    )
    return csrf_token


def _search_ptrs(session, csrf_token: str, start_date: date, end_date: date) -> list[dict]:
    """POST to the DataTables search endpoint for Periodic Transaction filings."""
    csrf_cookie = session.cookies.get("csrftoken", csrf_token)
    response = session.post(
        SEARCH_DATA_URL,
        headers={"Referer": SEARCH_PAGE_URL, "X-CSRFToken": csrf_cookie},
        data={
            "report_types": f"[{PTR_REPORT_TYPE}]",
            "filer_types": "[]",
            "submitted_start_date": start_date.strftime("%m/%d/%Y"),
            "submitted_end_date": end_date.strftime("%m/%d/%Y"),
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
            "draw": "1",
            "start": "0",
            "length": "100",
        },
    )
    if "json" not in response.headers.get("Content-Type", ""):
        raise SenateUnavailableError(
            f"efdsearch.senate.gov did not return JSON (status {response.status_code}) — "
            "likely a maintenance page"
        )
    payload = response.json()

    results = []
    for row in payload.get("data", []):
        name_html, office, report_html, date_received = row
        name_match = re.search(r">([^<]+)<", name_html)
        report_match = re.search(r"href='([^']+)'", report_html)
        if not name_match or not report_match:
            continue
        month, day, year = date_received.split("/")
        results.append(
            {
                "person": name_match.group(1),
                "ptr_url": EFD_BASE + report_match.group(1),
                "filed_date": date(int(year), int(month), int(day)),
            }
        )
    return results


def fetch_trades(session, days: int = 30) -> list[Trade]:
    """Fetch recent Senate PTR filings and normalize to Trades."""
    csrf_token = _start_session(session)
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    filings = _search_ptrs(session, csrf_token, start_date, end_date)

    trades: list[Trade] = []
    for filing in filings:
        try:
            ptr_response = session.get(filing["ptr_url"])
            trades.extend(
                parse_ptr_html(
                    ptr_response.text,
                    person=filing["person"],
                    filed_date=filing["filed_date"],
                    link=filing["ptr_url"],
                )
            )
        except Exception as exc:
            logger.warning("skipping filing %s: %s", filing["ptr_url"], exc)
            continue  # one bad filing must not sink the whole fetch
        courtesy_delay()
    return trades
