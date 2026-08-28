# Congress & Insider Trading Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `congress-trades/`, a CLI tool that fetches recent stock trades disclosed by the House, the Senate, and corporate insiders (SEC Form 4), and renders them as one filterable, self-contained HTML report.

**Architecture:** One fetcher module per source (`house.py`, `senate.py`, `insiders.py`), each exposing `fetch_trades(session, days) -> list[Trade]` against a shared `Trade` dataclass. A shared `http_client.py` provides a `requests.Session` with the descriptive User-Agent all three government sites require, plus retry/backoff. `cache.py` mirrors `analysis/cache.py`'s JSON-file-with-TTL pattern, keyed per source. `report.py` renders one static HTML table with client-side JS filters, styled like `analysis/report.py`. `trades.py` is the argparse CLI that orchestrates fetch → cache → report, tolerating individual source failures.

**Tech Stack:** Python 3.11+, `requests` (HTTP), `pdfplumber` (House PTR PDF text extraction), `beautifulsoup4` (Senate PTR HTML + EDGAR index parsing), `pytest`. No web framework — output is a static HTML file, same as `analysis/screener.py`.

**Spec:** `docs/superpowers/specs/2026-08-27-congress-trades-design.md`

## Global Constraints

- Every HTTP request to `disclosures-clerk.house.gov`, `efdsearch.senate.gov`, or `sec.gov` MUST set a descriptive `User-Agent` header (`"congress-trades <contact>"`) via `http_client.get_session()` — these sites block the default/blank UA. The contact address comes from the `CONGRESS_TRADES_CONTACT` env var, defaulting to a clearly-fake placeholder (never the developer's personal email hardcoded into source).
- A failure in one source (network error, "Site Under Maintenance" page, one unparseable filing) must not abort the other sources or the whole run — catch per-source, log a warning, continue. This is exercised by tests, not just mentioned in a docstring.
- `Trade.trade_date` and `Trade.filed_date` are `datetime.date` objects everywhere except at the cache-serialization boundary (ISO `YYYY-MM-DD` strings in JSON) and the report-rendering boundary (formatted strings in HTML).
- Follow `analysis/`'s conventions: module docstring header, `from __future__ import annotations`, dataclasses via `dataclasses.dataclass`, cache under `<project>/cache/*.json` (gitignored).
- Fixtures already captured and verified against live sources during planning live at `congress-trades/tests/fixtures/`: `house_ptr_sample.pdf` (real filing #20034201, Rep. Mark Alford, 9 line-item sales) and `form4_sample.xml` (real Atomera Inc. Form 4, one sell transaction + one holding). Tests must use these, not invented data.

---

### Task 1: Project scaffold, `models.py`, `http_client.py`

**Files:**
- Create: `congress-trades/models.py`
- Create: `congress-trades/http_client.py`
- Create: `congress-trades/requirements.txt`
- Create: `congress-trades/.gitignore`
- Create: `congress-trades/tests/__init__.py`
- Create: `congress-trades/tests/test_models.py`
- Create: `congress-trades/tests/test_http_client.py`
- (Fixtures already exist: `congress-trades/tests/fixtures/house_ptr_sample.pdf`, `congress-trades/tests/fixtures/form4_sample.xml`)

**Interfaces:**
- Produces: `Trade` dataclass (`source: str`, `person: str`, `role: str`, `ticker: str`, `company: str`, `transaction_type: str`, `trade_date: date`, `filed_date: date`, `amount_low: float | None`, `amount_high: float | None`, `shares: float | None`, `price: float | None`, `link: str`), `Trade.to_dict() -> dict`, `Trade.from_dict(d: dict) -> Trade` (both used by `cache.py` in Task 2).
- Produces: `http_client.get_session() -> requests.Session`, `http_client.fetch(session, url, method="GET", **kwargs) -> requests.Response` (retries transient failures; raises `requests.HTTPError` / `requests.ConnectionError` after 3 attempts).

- [ ] **Step 1: Create the project skeleton and requirements**

```bash
mkdir -p congress-trades/tests/fixtures congress-trades/cache
```

`congress-trades/requirements.txt`:
```
requests>=2.31
beautifulsoup4>=4.12
pdfplumber>=0.11
pytest>=8.0
```

`congress-trades/.gitignore`:
```
cache/*.json
__pycache__/
*.pyc
.venv/
```

- [ ] **Step 2: Write the failing test for `Trade` serialization**

`congress-trades/tests/test_models.py`:
```python
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
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'models'`

- [ ] **Step 4: Implement `models.py`**

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd congress-trades && python3 -m pytest tests/test_models.py -v`
Expected: PASS

- [ ] **Step 6: Write the failing test for `http_client`**

`congress-trades/tests/test_http_client.py`:
```python
import requests

from http_client import get_session


def test_session_sets_descriptive_user_agent():
    session = get_session()
    ua = session.headers.get("User-Agent", "")
    assert "congress-trades" in ua
    assert ua != ""


def test_session_is_a_requests_session():
    session = get_session()
    assert isinstance(session, requests.Session)
```

- [ ] **Step 7: Run test to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_http_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'http_client'`

- [ ] **Step 8: Implement `http_client.py`**

```python
"""
http_client.py — Shared requests.Session with the descriptive User-Agent that
disclosures-clerk.house.gov, efdsearch.senate.gov, and sec.gov all require for
automated access, plus basic retry/backoff for transient failures.
"""

from __future__ import annotations

import os
import time

import requests

DEFAULT_CONTACT = "set-CONGRESS_TRADES_CONTACT-env-var@example.com"
MAX_RETRIES = 3
BACKOFF_SECONDS = 1.5


def get_session() -> requests.Session:
    """Build a requests.Session with the required identifying User-Agent."""
    contact = os.environ.get("CONGRESS_TRADES_CONTACT", DEFAULT_CONTACT)
    session = requests.Session()
    session.headers.update({"User-Agent": f"congress-trades {contact}"})
    return session


def fetch(session: requests.Session, url: str, method: str = "GET", **kwargs) -> requests.Response:
    """GET/POST with retry+backoff on network errors and 5xx responses.

    Raises the last exception (or the last response's HTTPError) if every
    attempt fails.
    """
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.request(method, url, timeout=30, **kwargs)
            if response.status_code >= 500:
                response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_SECONDS * (attempt + 1))
    assert last_exc is not None
    raise last_exc
```

- [ ] **Step 9: Run test to verify it passes**

Run: `cd congress-trades && python3 -m pytest tests/test_http_client.py -v`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
cd congress-trades
git add models.py http_client.py requirements.txt .gitignore tests/
git commit -m "feat(congress-trades): add Trade model and shared HTTP client"
```

---

### Task 2: `cache.py`

**Files:**
- Create: `congress-trades/cache.py`
- Create: `congress-trades/tests/test_cache.py`

**Interfaces:**
- Consumes: `Trade.to_dict()`, `Trade.from_dict()` from Task 1.
- Produces: `cache.load_cache(source: str) -> dict`, `cache.save_cache(source: str, cache: dict) -> None`, `cache.get_cached_trades(source: str) -> list[Trade] | None` (None if no cache file or stale), `cache.set_cached_trades(source: str, trades: list[Trade]) -> None`. Used by `house.py`, `senate.py`, `insiders.py`, and `trades.py` in later tasks.

- [ ] **Step 1: Write the failing tests**

`congress-trades/tests/test_cache.py`:
```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_cache.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'cache'`

- [ ] **Step 3: Implement `cache.py`**

```python
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


def get_cached_trades(source: str) -> Optional[list[Trade]]:
    """Return cached trades for a source if present and within TTL, else None."""
    envelope = load_cache(source)
    if not envelope:
        return None
    if time.time() - envelope.get("ts", 0) > CACHE_TTL:
        return None
    return [Trade.from_dict(d) for d in envelope.get("trades", [])]


def set_cached_trades(source: str, trades: list[Trade]) -> None:
    save_cache(source, {"ts": time.time(), "trades": [t.to_dict() for t in trades]})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_cache.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add cache.py tests/test_cache.py
git commit -m "feat(congress-trades): add per-source JSON cache with TTL"
```

---

### Task 3: `insiders.py` — SEC EDGAR Form 4

**Files:**
- Create: `congress-trades/insiders.py`
- Create: `congress-trades/tests/test_insiders.py`
- Uses fixture: `congress-trades/tests/fixtures/form4_sample.xml`

**Interfaces:**
- Consumes: `http_client.get_session()`, `http_client.fetch()` from Task 1; `Trade` from Task 1.
- Produces: `insiders.parse_form4_xml(xml_bytes: bytes, source_url: str) -> list[Trade]`, `insiders.fetch_trades(session, days: int) -> list[Trade]`. `fetch_trades` is called by `trades.py` in Task 7.

**Real fixture schema** (verified against a live SEC filing, 2026-08-27 — `tests/fixtures/form4_sample.xml`): root `<ownershipDocument>` with `<issuer><issuerName>`/`<issuerTradingSymbol>`, `<reportingOwner><reportingOwnerId><rptOwnerName>`, `<reportingOwner><reportingOwnerRelationship>` with `<isOfficer>`/`<isDirector>`/`<isTenPercentOwner>`/`<officerTitle>` (each `0`/`1` or text), and `<nonDerivativeTable>` containing zero or more `<nonDerivativeTransaction>` (has `<transactionDate><value>`, `<transactionCoding><transactionCode>`, `<transactionAmounts><transactionShares><value>`, `<transactionPricePerShare><value>`, `<transactionAcquiredDisposedCode><value>`) interleaved with `<nonDerivativeHolding>` elements (no transaction data — must be skipped, not just elements with missing fields).

- [ ] **Step 1: Write the failing test for XML parsing**

`congress-trades/tests/test_insiders.py`:
```python
from datetime import date
from pathlib import Path

from insiders import parse_form4_xml

FIXTURE = Path(__file__).parent / "fixtures" / "form4_sample.xml"


def test_parses_real_form4_sell_transaction():
    xml_bytes = FIXTURE.read_bytes()
    trades = parse_form4_xml(xml_bytes, source_url="https://example.com/form4.xml")

    assert len(trades) == 1  # the file also has a nonDerivativeHolding, which is not a transaction
    trade = trades[0]
    assert trade.source == "insider"
    assert trade.person == "Mears Robert J"
    assert trade.role == "Chief Technology Officer"
    assert trade.ticker == "ATOM"
    assert trade.company == "Atomera Inc"
    assert trade.transaction_type == "sell"
    assert trade.trade_date == date(2026, 8, 3)
    assert trade.shares == 1000.0
    assert trade.price == 5.05
    assert trade.link == "https://example.com/form4.xml"


def test_role_combines_director_and_officer_when_both_true():
    xml_bytes = FIXTURE.read_bytes().replace(
        b"<isDirector>0</isDirector>", b"<isDirector>1</isDirector>"
    )
    trades = parse_form4_xml(xml_bytes, source_url="https://example.com/form4.xml")
    assert trades[0].role == "Director, Chief Technology Officer"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_insiders.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'insiders'`

- [ ] **Step 3: Implement the XML parsing half of `insiders.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_insiders.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Write the failing test for the live-fetch orchestration, using a stub session**

Add to `congress-trades/tests/test_insiders.py`:
```python
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from insiders import fetch_trades


ATOM_FEED = """<?xml version="1.0" encoding="ISO-8859-1"?>
<feed xmlns="http://www.w3.org/2005/Atom">
<entry>
<title>4 - Example Corp (0001111111) (Issuer)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/1111111/000222222226000001/0002222222-26-000001-index.htm"/>
<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; {filed} &lt;b&gt;AccNo:&lt;/b&gt; 0002222222-26-000001 &lt;b&gt;Size:&lt;/b&gt; 5 KB</summary>
<id>urn:tag:sec.gov,2008:accession-number=0002222222-26-000001</id>
</entry>
<entry>
<title>4 - Someone Q (0002222222) (Reporting)</title>
<link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/2222222/000222222226000001/0002222222-26-000001-index.htm"/>
<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; {filed} &lt;b&gt;AccNo:&lt;/b&gt; 0002222222-26-000001 &lt;b&gt;Size:&lt;/b&gt; 5 KB</summary>
<id>urn:tag:sec.gov,2008:accession-number=0002222222-26-000001</id>
</entry>
</feed>
"""

INDEX_JSON = """{"directory": {"item": [
  {"name": "0002222222-26-000001-index.html"},
  {"name": "form4.xml"}
]}}"""


def test_fetch_trades_walks_feed_then_index_then_xml():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    feed_xml = ATOM_FEED.format(filed=today)
    form4_xml = FIXTURE.read_bytes()

    session = MagicMock()

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        if "getcurrent" in url:
            response.text = feed_xml
            response.content = feed_xml.encode()
        elif url.endswith("index.json"):
            response.text = INDEX_JSON
            response.content = INDEX_JSON.encode()
        elif url.endswith("form4.xml"):
            response.content = form4_xml
        return response

    session.get.side_effect = fake_get

    trades = fetch_trades(session, days=1)

    assert len(trades) == 1
    assert trades[0].ticker == "ATOM"
    assert "form4.xml" in trades[0].link
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_insiders.py -v`
Expected: FAIL — `fetch_trades` not defined

- [ ] **Step 7: Implement the fetch/orchestration half of `insiders.py`**

Append to `congress-trades/insiders.py`:
```python
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
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_insiders.py -v`
Expected: PASS (4 passed)

- [ ] **Step 9: Commit**

```bash
git add insiders.py tests/test_insiders.py
git commit -m "feat(congress-trades): add SEC EDGAR Form 4 fetcher and parser"
```

---

### Task 4: `house.py` — House Clerk PTR filings

**Files:**
- Create: `congress-trades/house.py`
- Create: `congress-trades/tests/test_house.py`
- Uses fixture: `congress-trades/tests/fixtures/house_ptr_sample.pdf`

**Interfaces:**
- Consumes: `Trade` from Task 1.
- Produces: `house.parse_ptr_pdf(pdf_bytes: bytes, person: str, filed_date: date, link: str) -> list[Trade]`, `house.fetch_trades(session, days: int) -> list[Trade]`. `fetch_trades` is called by `trades.py` in Task 7.

**Real fixture** (verified live 2026-08-27, `tests/fixtures/house_ptr_sample.pdf` — Rep. Mark Alford, filing #20034201, 9 sale line items, all `S (partial)`, trade+notification date `03/16/2026`, amount band `$1,001 - $15,000`): tickers in filing order are `AMZN, AAPL, T, BRK.B, DIA, QQQ, PYPL, SPYB, SPYB` (the last ticker is genuinely duplicated in the source PDF — verified by extracting and printing the raw text during planning, not a parsing artifact to paper over).

- [ ] **Step 1: Write the failing test for PDF parsing**

`congress-trades/tests/test_house.py`:
```python
from datetime import date
from pathlib import Path

from house import parse_ptr_pdf

FIXTURE = Path(__file__).parent / "fixtures" / "house_ptr_sample.pdf"


def test_parses_real_house_ptr_pdf():
    pdf_bytes = FIXTURE.read_bytes()
    trades = parse_ptr_pdf(
        pdf_bytes,
        person="Hon. Mark Alford",
        filed_date=date(2026, 3, 31),
        link="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034201.pdf",
    )

    tickers = [t.ticker for t in trades]
    assert tickers == ["AMZN", "AAPL", "T", "BRK.B", "DIA", "QQQ", "PYPL", "SPYB", "SPYB"]

    for t in trades:
        assert t.source == "house"
        assert t.person == "Hon. Mark Alford"
        assert t.transaction_type == "sell"
        assert t.trade_date == date(2026, 3, 16)
        assert t.amount_low == 1001
        assert t.amount_high == 15000
        assert t.link.endswith("20034201.pdf")
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_house.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'house'`

- [ ] **Step 3: Implement the PDF-parsing half of `house.py`**

This regex-pairing approach was validated during planning against the real fixture (all 9 rows extracted correctly) — see the design spec's House data-source note for how the layout was reverse-engineered.

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_house.py -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for the index-fetch orchestration, using a stub session**

Add to `congress-trades/tests/test_house.py`:
```python
from datetime import datetime, timezone
from unittest.mock import MagicMock

from house import fetch_trades

XML_INDEX = """<?xml version="1.0" encoding="utf-8"?>
<FinancialDisclosure>
  <Member>
    <Last>Alford</Last>
    <First>Mark</First>
    <FilingType>P</FilingType>
    <StateDst>MO04</StateDst>
    <Year>2026</Year>
    <FilingDate>{filed}</FilingDate>
    <DocID>20034201</DocID>
  </Member>
  <Member>
    <Last>Someone</Last>
    <First>Else</First>
    <FilingType>C</FilingType>
    <StateDst>TX01</StateDst>
    <Year>2026</Year>
    <FilingDate>{filed}</FilingDate>
    <DocID>99999</DocID>
  </Member>
</FinancialDisclosure>
"""


def _zip_bytes(xml_text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("2026FD.xml", xml_text)
    return buf.getvalue()


def test_fetch_trades_filters_to_ptr_filings_and_parses_pdf():
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    zip_bytes = _zip_bytes(XML_INDEX.format(filed=today))
    pdf_bytes = FIXTURE.read_bytes()

    session = MagicMock()

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        if url.endswith(".zip"):
            response.content = zip_bytes
        elif url.endswith("20034201.pdf"):
            response.content = pdf_bytes
        return response

    session.get.side_effect = fake_get

    trades = fetch_trades(session, days=1)

    assert len(trades) == 9
    assert all(t.person == "Mark Alford" for t in trades)
    assert all(t.source == "house" for t in trades)
```

(add `import io, zipfile` to the top of `tests/test_house.py`)

- [ ] **Step 6: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_house.py -v`
Expected: FAIL — `fetch_trades` not defined

- [ ] **Step 7: Implement the fetch/orchestration half of `house.py`**

Append to `congress-trades/house.py`:
```python
from xml.etree import ElementTree as ET


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
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_house.py -v`
Expected: PASS (2 passed)

- [ ] **Step 9: Commit**

```bash
git add house.py tests/test_house.py
git commit -m "feat(congress-trades): add House Clerk PTR fetcher and PDF parser"
```

---

### Task 5: `senate.py` — Senate eFD PTR filings

**Files:**
- Create: `congress-trades/senate.py`
- Create: `congress-trades/tests/test_senate.py`

**Interfaces:**
- Consumes: `Trade` from Task 1.
- Produces: `senate.parse_ptr_html(html: str, person: str, filed_date: date, link: str) -> list[Trade]`, `senate.fetch_trades(session, days: int) -> list[Trade]` (raises `senate.SenateUnavailableError` — caught by `trades.py` in Task 7 — when the site returns its "Site Under Maintenance" page instead of the expected search results, which was observed live during planning).

**Note on this task's fixture:** unlike Task 3 and Task 4, no live PTR view page could be captured during planning — `efdsearch.senate.gov` was showing "Site Under Maintenance" for the search endpoint at the time (confirmed via curl; the session-handshake steps before it — CSRF token fetch and prohibition-agreement POST — were confirmed working). The HTML fixture below is built from the documented eFD PTR table schema (verified via the field names in the archived Senate Stock Watcher dataset, which scraped these same pages: `transaction_date, owner, ticker, asset_description, asset_type, type, amount, comment`). Parse by table header text, not fixed column position, so real markup differences in class names/wrapper elements don't break it. **Before treating this task as done, manually fetch one live PTR URL** (search https://efdsearch.senate.gov/search/ once the maintenance window is over, for `report_type=11`) **and confirm the header names match** — adjust `_HEADER_TO_FIELD` if they don't.

- [ ] **Step 1: Write the failing test for HTML parsing**

`congress-trades/tests/test_senate.py`:
```python
from datetime import date

from senate import parse_ptr_html

SAMPLE_PTR_HTML = """
<html><body>
<table class="table">
<thead><tr>
<th>Transaction Date</th><th>Owner</th><th>Ticker</th><th>Asset Name</th>
<th>Asset Type</th><th>Type</th><th>Amount</th><th>Comment</th>
</tr></thead>
<tbody>
<tr>
<td>08/12/2026</td><td>Self</td><td>NVDA</td><td>NVIDIA Corporation</td>
<td>Stock</td><td>Purchase</td><td>$15,001 - $50,000</td><td>--</td>
</tr>
<tr>
<td>08/12/2026</td><td>Spouse</td><td>MSFT</td><td>Microsoft Corporation</td>
<td>Stock</td><td>Sale (Full)</td><td>$1,001 - $15,000</td><td>--</td>
</tr>
</tbody>
</table>
</body></html>
"""


def test_parses_transaction_table_by_header_name():
    trades = parse_ptr_html(
        SAMPLE_PTR_HTML,
        person="Jane Example",
        filed_date=date(2026, 8, 13),
        link="https://efdsearch.senate.gov/search/view/ptr/example/",
    )

    assert len(trades) == 2
    buy, sell = trades
    assert buy.ticker == "NVDA"
    assert buy.transaction_type == "buy"
    assert buy.amount_low == 15001
    assert buy.amount_high == 50000
    assert buy.trade_date == date(2026, 8, 12)
    assert buy.source == "senate"
    assert buy.company == "NVIDIA Corporation"

    assert sell.ticker == "MSFT"
    assert sell.transaction_type == "sell"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_senate.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'senate'`

- [ ] **Step 3: Implement the HTML-parsing half of `senate.py`**

```python
"""
senate.py — Fetch and normalize Senate Periodic Transaction Reports (PTRs)
from the official efdsearch.senate.gov system.

efdsearch.senate.gov requires a short session handshake before any search:
GET /search/ for a CSRF token + session cookie, then POST that token plus
prohibition_agreement=1 to /search/home/. Only after that does
/search/report/data/ (a DataTables endpoint) accept search POSTs.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

from bs4 import BeautifulSoup

from models import Trade

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

    headers = [th.get_text(strip=True).lower() for th in table.find("thead").find_all("th")]
    fields = [_HEADER_TO_FIELD.get(h) for h in headers]

    trades: list[Trade] = []
    for row in table.find("tbody").find_all("tr"):
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_senate.py -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for the session-handshake + search orchestration, using a stub session**

Add to `congress-trades/tests/test_senate.py`:
```python
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from senate import SenateUnavailableError, fetch_trades

SEARCH_PAGE_HTML = """
<html><body><form>
<input type="hidden" name="csrfmiddlewaretoken" value="testtoken123">
<input type="checkbox" name="prohibition_agreement" value="1">
</form></body></html>
"""

SEARCH_RESULTS_JSON = json.dumps(
    {
        "data": [
            [
                "<a href='/search/view/annual/x/'>Jane Example</a>",
                "Senator",
                "<a href='/search/view/ptr/example/'>Periodic Transaction Report</a>",
                "08/13/2026",
            ]
        ],
        "recordsTotal": 1,
        "recordsFiltered": 1,
    }
)


def test_fetch_trades_does_handshake_then_search_then_parses_ptr():
    session = MagicMock()
    session.cookies = {"csrftoken": "cookievalue"}

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        if url == "https://efdsearch.senate.gov/search/":
            response.text = SEARCH_PAGE_HTML
        elif "view/ptr/example" in url:
            response.text = SAMPLE_PTR_HTML
        return response

    def fake_post(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        response.headers = {"Content-Type": "application/json"}
        if url == "https://efdsearch.senate.gov/search/home/":
            response.text = "ok"
        elif url == "https://efdsearch.senate.gov/search/report/data/":
            response.text = SEARCH_RESULTS_JSON
            response.json.return_value = json.loads(SEARCH_RESULTS_JSON)
        return response

    session.get.side_effect = fake_get
    session.post.side_effect = fake_post

    trades = fetch_trades(session, days=7)

    assert len(trades) == 2
    assert trades[0].person == "Jane Example"
    assert trades[0].source == "senate"


def test_fetch_trades_raises_when_site_is_under_maintenance():
    session = MagicMock()
    session.cookies = {"csrftoken": "cookievalue"}

    def fake_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        response.text = SEARCH_PAGE_HTML
        return response

    def fake_post(url, **kwargs):
        response = MagicMock()
        response.status_code = 503
        response.headers = {"Content-Type": "text/html"}
        response.text = "<title>U.S. Senate: Site Under Maintenance</title>"
        return response

    session.get.side_effect = fake_get
    session.post.side_effect = fake_post

    try:
        fetch_trades(session, days=7)
        assert False, "expected SenateUnavailableError"
    except SenateUnavailableError:
        pass
```

- [ ] **Step 6: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_senate.py -v`
Expected: FAIL — `fetch_trades` not defined

- [ ] **Step 7: Implement the handshake/search/orchestration half of `senate.py`**

Append to `congress-trades/senate.py`:
```python
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
        except Exception:
            continue  # one bad filing must not sink the whole fetch
    return trades
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_senate.py -v`
Expected: PASS (4 passed)

- [ ] **Step 9: Commit**

```bash
git add senate.py tests/test_senate.py
git commit -m "feat(congress-trades): add Senate eFD PTR fetcher and parser

Note: HTML fixture is built from the documented eFD schema, not a live
capture — efdsearch.senate.gov was under maintenance during planning.
Verify header names against a live PTR page before relying on this in
production; adjust _HEADER_TO_FIELD if they differ."
```

---

### Task 6: `report.py`

**Files:**
- Create: `congress-trades/report.py`
- Create: `congress-trades/tests/test_report.py`

**Interfaces:**
- Consumes: `Trade` from Task 1.
- Produces: `report.generate_html(trades: list[Trade], meta: dict, source_errors: dict[str, str]) -> str`, `report.write_and_open(html: str, output_dir: Path, quiet: bool = False) -> Path`. Both called by `trades.py` in Task 7.

- [ ] **Step 1: Write the failing test**

`congress-trades/tests/test_report.py`:
```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_report.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'report'`

- [ ] **Step 3: Implement `report.py`**

```python
"""
report.py — Self-contained HTML report generator for congress-trades,
styled to match analysis/report.py.
"""

from __future__ import annotations

import html as html_lib
import webbrowser
from pathlib import Path
from typing import Any

from models import Trade

SOURCE_LABELS = {"house": "House", "senate": "Senate", "insider": "Insider"}


def _format_amount(trade: Trade) -> str:
    if trade.amount_low is not None and trade.amount_high is not None:
        return f"${trade.amount_low:,.0f} - ${trade.amount_high:,.0f}"
    if trade.shares is not None and trade.price is not None:
        return f"{trade.shares:,.0f} sh @ ${trade.price:,.2f}"
    return "—"


def _row_html(trade: Trade) -> str:
    badge_color = {"buy": "#10b981", "sell": "#ef4444"}.get(trade.transaction_type, "#64748b")
    return f"""<tr data-source="{trade.source}" data-type="{trade.transaction_type}"
    data-search="{html_lib.escape(f'{trade.person} {trade.ticker} {trade.company}'.lower())}">
  <td>{trade.trade_date.isoformat()}</td>
  <td>{html_lib.escape(trade.person)}</td>
  <td>{html_lib.escape(trade.role)}</td>
  <td>{SOURCE_LABELS.get(trade.source, trade.source)}</td>
  <td><strong>{html_lib.escape(trade.ticker)}</strong></td>
  <td>{html_lib.escape(trade.company)}</td>
  <td><span style="color: {badge_color}; font-weight: 600;">{trade.transaction_type.upper()}</span></td>
  <td>{_format_amount(trade)}</td>
  <td><a href="{html_lib.escape(trade.link)}" target="_blank" rel="noopener">filing</a></td>
</tr>"""


def generate_html(trades: list[Trade], meta: dict[str, Any], source_errors: dict[str, str]) -> str:
    trades_sorted = sorted(trades, key=lambda t: t.trade_date, reverse=True)
    rows_html = "\n".join(_row_html(t) for t in trades_sorted)

    errors_html = ""
    if source_errors:
        items = "".join(
            f"<li><strong>{SOURCE_LABELS.get(src, src)}</strong>: {html_lib.escape(msg)}</li>"
            for src, msg in source_errors.items()
        )
        errors_html = f"""<div class="errors">⚠️ Some sources did not load:<ul>{items}</ul></div>"""

    empty_state = "" if trades_sorted else '<p class="empty">No trades found in this window.</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Congress &amp; Insider Trading Tracker</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f8fafc;
    color: #1e293b;
    margin: 0;
    padding: 24px;
    font-size: 14px;
  }}
  h1 {{ font-size: 22px; font-weight: 700; margin: 0 0 4px; color: #0f172a; }}
  .meta {{ color: #64748b; font-size: 13px; margin-bottom: 16px; }}
  .errors {{
    background: #fffbeb; border: 1px solid #f59e0b; border-radius: 6px;
    padding: 10px 14px; font-size: 13px; color: #92400e; margin-bottom: 16px;
  }}
  .controls {{ display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; }}
  .controls input, .controls select {{
    padding: 6px 10px; border: 1px solid #cbd5e1; border-radius: 6px; font-size: 13px;
  }}
  table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }}
  th, td {{ text-align: left; padding: 8px 12px; border-bottom: 1px solid #e2e8f0; font-size: 13px; }}
  th {{ background: #f1f5f9; font-weight: 600; color: #475569; }}
  tr:hover {{ background: #f8fafc; }}
  .empty {{ color: #64748b; padding: 24px; }}
</style>
</head>
<body>
<h1>Congress &amp; Insider Trading Tracker</h1>
<div class="meta">Last {meta.get('days', 30)} days &middot; generated {meta.get('generated_at', '')} &middot; {len(trades_sorted)} trades</div>
{errors_html}
<div class="controls">
  <input type="text" id="searchBox" placeholder="Search person, ticker, company...">
  <select id="sourceFilter">
    <option value="">All sources</option>
    <option value="house">House</option>
    <option value="senate">Senate</option>
    <option value="insider">Insider</option>
  </select>
  <select id="typeFilter">
    <option value="">Buy or sell</option>
    <option value="buy">Buy</option>
    <option value="sell">Sell</option>
  </select>
</div>
{empty_state}
<table id="tradesTable" style="{'display:none' if not trades_sorted else ''}">
<thead><tr>
  <th>Date</th><th>Person</th><th>Role</th><th>Source</th><th>Ticker</th>
  <th>Company</th><th>Type</th><th>Amount</th><th>Filing</th>
</tr></thead>
<tbody>
{rows_html}
</tbody>
</table>
<script>
  const searchBox = document.getElementById('searchBox');
  const sourceFilter = document.getElementById('sourceFilter');
  const typeFilter = document.getElementById('typeFilter');
  const rows = Array.from(document.querySelectorAll('#tradesTable tbody tr'));

  function applyFilters() {{
    const query = searchBox.value.toLowerCase();
    const source = sourceFilter.value;
    const type = typeFilter.value;
    rows.forEach(row => {{
      const matchesQuery = !query || row.dataset.search.includes(query);
      const matchesSource = !source || row.dataset.source === source;
      const matchesType = !type || row.dataset.type === type;
      row.style.display = (matchesQuery && matchesSource && matchesType) ? '' : 'none';
    }});
  }}

  searchBox.addEventListener('input', applyFilters);
  sourceFilter.addEventListener('change', applyFilters);
  typeFilter.addEventListener('change', applyFilters);
</script>
</body>
</html>"""


def write_and_open(html: str, output_dir: Path, quiet: bool = False) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "congress_trades_report.html"
    output_path.write_text(html)
    if not quiet:
        webbrowser.open(f"file://{output_path.resolve()}")
    return output_path
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_report.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add report.py tests/test_report.py
git commit -m "feat(congress-trades): add self-contained HTML report generator"
```

---

### Task 7: `trades.py` — CLI orchestration

**Files:**
- Create: `congress-trades/trades.py`
- Create: `congress-trades/tests/test_trades.py`
- Create: `congress-trades/README.md`

**Interfaces:**
- Consumes: `http_client.get_session()` (Task 1), `cache.get_cached_trades()`/`cache.set_cached_trades()` (Task 2), `house.fetch_trades()` (Task 4), `senate.fetch_trades()`/`senate.SenateUnavailableError` (Task 5), `insiders.fetch_trades()` (Task 3), `report.generate_html()`/`report.write_and_open()` (Task 6).
- Produces: `trades.run(days, sources, no_cache, output_dir, quiet) -> int` (the exit code), used by the `if __name__ == "__main__":` block and by tests directly.

- [ ] **Step 1: Write the failing tests**

`congress-trades/tests/test_trades.py`:
```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd congress-trades && python3 -m pytest tests/test_trades.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'trades'`

- [ ] **Step 3: Implement `trades.py`**

```python
#!/usr/bin/env python3
"""
trades.py — Congress & Insider Trading Tracker CLI.

Fetches recent stock trades disclosed by the House, the Senate, and
corporate insiders (SEC Form 4), and renders them as one filterable,
self-contained HTML report.

Usage:
    python trades.py [options]

Options:
    --days N            Lookback window in days (default: 30)
    --sources LIST       Comma-separated subset of house,senate,insider (default: all)
    --no-cache            Force re-fetch all data
    --output-dir PATH     Directory for HTML output (default: current directory)
    --quiet                Suppress progress output

Exit codes:
    0  Success — report written with at least one trade
    1  No trades found in the window
    2  Fatal — every source failed
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import cache
import house
import insiders
import report
import senate
from http_client import get_session

ALL_SOURCES = ["house", "senate", "insider"]
# Modules, not bound function references — looked up at call time so
# unittest.mock.patch("trades.house.fetch_trades", ...) actually takes effect.
FETCHER_MODULES = {"house": house, "senate": senate, "insider": insiders}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="trades", description="Congress & Insider Trading Tracker")
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days")
    parser.add_argument("--sources", type=str, default="house,senate,insider",
                         help="Comma-separated subset of house,senate,insider")
    parser.add_argument("--no-cache", action="store_true", help="Force re-fetch all data")
    parser.add_argument("--output-dir", type=Path, default=Path("."), help="Directory for HTML output")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    return parser.parse_args()


def run(days: int, sources: list[str], no_cache: bool, output_dir: Path, quiet: bool) -> int:
    session = get_session()
    all_trades = []
    source_errors: dict[str, str] = {}

    for source in sources:
        if not no_cache:
            cached = cache.get_cached_trades(source)
            if cached is not None:
                all_trades.extend(cached)
                if not quiet:
                    print(f"{source}: {len(cached)} trades (cached)")
                continue

        try:
            fetched = FETCHER_MODULES[source].fetch_trades(session, days=days)
            all_trades.extend(fetched)
            cache.set_cached_trades(source, fetched)
            if not quiet:
                print(f"{source}: {len(fetched)} trades")
        except Exception as exc:
            source_errors[source] = str(exc)
            if not quiet:
                print(f"{source}: FAILED — {exc}", file=sys.stderr)

    if len(source_errors) == len(sources):
        return 2

    html = report.generate_html(
        all_trades,
        meta={"days": days, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M")},
        source_errors=source_errors,
    )
    report.write_and_open(html, output_dir, quiet=quiet)

    return 0 if all_trades else 1


def main() -> int:
    args = parse_args()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    return run(
        days=args.days,
        sources=sources,
        no_cache=args.no_cache,
        output_dir=args.output_dir,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd congress-trades && python3 -m pytest tests/test_trades.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Run the full test suite**

Run: `cd congress-trades && python3 -m pytest -v`
Expected: all tests across all modules PASS

- [ ] **Step 6: Write `README.md`**

```markdown
# congress-trades

Fetches recent stock trades disclosed by the House, the Senate, and
corporate insiders (SEC Form 4), and renders them as one filterable,
self-contained HTML report.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export CONGRESS_TRADES_CONTACT="you@example.com"  # required by SEC/House/Senate fair-access policy
```

## Usage

```bash
python trades.py --days 30
```

Options: `--sources house,senate,insider` (default: all three), `--no-cache`,
`--output-dir PATH`, `--quiet`. See `docs/superpowers/specs/2026-08-27-congress-trades-design.md`
for the data-source details and known limitations (disclosure amounts
are bands, not exact dollars; House PDF parsing is best-effort; Senate
parsing was built against the documented schema, not a live capture —
see the note at the top of `senate.py`).
```

- [ ] **Step 7: Commit**

```bash
git add trades.py tests/test_trades.py README.md
git commit -m "feat(congress-trades): add CLI orchestration and README"
```

---

## Post-plan verification (not a task — a note for whoever runs this)

1. Before relying on this in production, run `python trades.py --days 7` for real once `efdsearch.senate.gov` is out of its maintenance window, and confirm `senate.py`'s `_HEADER_TO_FIELD` mapping matches the live page (see Task 5's fixture note).
2. `house.py`'s PDF parser is best-effort regex pairing, validated against one real filing. Filings with denser or differently-ordered tables may drop or mis-pair line items — worth spot-checking a handful of real PTRs after the first live run.
3. Cross-year lookback windows (e.g. `--days 400` in January) will miss House filings from the prior year — `house.py`'s `fetch_trades` only fetches the current year's index.
