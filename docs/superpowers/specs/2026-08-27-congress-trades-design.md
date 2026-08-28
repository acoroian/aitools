# Congress & Insider Trading Tracker — Design Spec

**Date**: 2026-08-27
**Status**: Approved for planning

## Purpose

A tool to answer "what have members of Congress and corporate insiders
been buying and selling recently?" Combines three disclosure sources
(House, Senate, SEC insiders) into one browsable report, following the
same self-contained-HTML-report pattern already used by
`analysis/screener.py` and `swing-scanner/`.

## Background: data sources

Congressional trades are **not** filed with the SEC. Members of
Congress report under the STOCK Act to the Clerk of the House / Senate
Office of Public Records, not EDGAR. So this tool pulls from three
independent pipelines.

**Revision note (verified 2026-08-27):** The original plan was to use
House Stock Watcher and Senate Stock Watcher, two free community
JSON aggregators. Live verification during planning found **both are
dead**: `housestockwatcher.com` no longer resolves and its S3 data
bucket returns Access Denied; the Senate Stock Watcher GitHub data
repo's last commit was 2021-03-16 and its aggregate file stops at
2020-12-02. Neither is usable for "recent trades." The sources below
replace them — verified reachable and current as of 2026-08-27:

1. **House** — official [Clerk of the House financial disclosure
   index](https://disclosures-clerk.house.gov/FinancialDisclosure):
   `https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.zip`,
   republished daily, contains `{YEAR}FD.xml` — one `<Member>` entry
   per filing with `Last`, `First`, `FilingType`, `StateDst`, `Year`,
   `FilingDate`, `DocID`. Periodic Transaction Reports have
   `FilingType == "P"`. The filing itself is a PDF at
   `https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/{DocID}.pdf`
   — confirmed live (sample: filing #20034201, Rep. Mark Alford, 3
   pages, real March 2026 transactions). No index-level ticker/amount
   data — must be extracted from the PDF text per transaction line.
   Amounts are disclosure *bands* (e.g. $1,001–$15,000), not exact
   dollars — a limitation of the underlying filing, not a parsing gap.
2. **Senate** — official [efdsearch.senate.gov](https://efdsearch.senate.gov/search/):
   a Django app requiring a short session handshake — GET `/search/`
   for a CSRF token and session cookie, POST that token plus
   `prohibition_agreement=1` to `/search/home/` — then POST to the
   DataTables endpoint `/search/report/data/` with
   `report_types=[11]` (Periodic Transaction), a submitted-date range,
   and paging params (`X-CSRFToken` header + CSRF cookie required on
   this POST). Confirmed live and reachable through the full handshake
   during planning (the search endpoint itself briefly returned an
   HTML "Site Under Maintenance" page rather than JSON — a real
   possibility the fetcher must treat as a transient failure, not a
   parse error). Each result row links to a PTR view page whose HTML
   contains a transaction table (columns: Transaction Date, Owner,
   Ticker, Asset Name, Asset Type, Type, Amount, Comment) — parsed by
   table header name, not column position, since some older filings
   omit columns.
3. **Corporate insiders** — SEC EDGAR Form 4 filings. The public
   recent-filings feed
   `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom`
   lists two entries per filing (issuer + reporting owner) with an
   accession number and an `-index.htm` link. Confirmed live. For each
   accession, `https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodashes}/index.json`
   lists that filing's files; the one non-index `.xml` file is the
   `ownershipDocument` — confirmed schema (real sample fetched
   2026-08-27): `issuer.issuerName`/`issuerTradingSymbol`,
   `reportingOwner.reportingOwnerId.rptOwnerName`,
   `reportingOwnerRelationship.isOfficer`/`isDirector`/`isTenPercentOwner`/`officerTitle`,
   and per-transaction
   `nonDerivativeTable.nonDerivativeTransaction` entries with
   `transactionDate.value`, `transactionCoding.transactionCode` (P/S/A/etc.),
   `transactionAmounts.transactionShares.value`,
   `transactionAmounts.transactionPricePerShare.value`,
   `transactionAmounts.transactionAcquiredDisposedCode.value` (A=buy,
   D=sell).

**SEC/House/Senate fair-access requirement**: all three government
sources require a descriptive `User-Agent` header identifying the
requester (e.g. `"congress-trades research you@example.com"`) — SEC
explicitly blocks the default/blank UA, and House/Senate are
courtesy-rate-limited the same way. Every fetcher must set this header
and keep requests modest (small delay between PDF/XML fetches).

## Project layout

New sibling project: `congress-trades/` (alongside `analysis/`,
`swing-scanner/`), following the `analysis/` module split — one module
per data source, since the three fetch mechanics (PDF scraping,
session-based scraping, XML API) are unrelated:

```
congress-trades/
  models.py       # shared Trade dataclass
  house.py        # fetch + normalize House Clerk PTR filings (XML index + PDF parse)
  senate.py       # fetch + normalize Senate eFD PTR filings (session handshake + HTML parse)
  insiders.py     # fetch + normalize SEC EDGAR Form 4 filings
  http_client.py  # shared requests.Session with the required User-Agent + retry/backoff
  cache.py        # daily JSON cache per source (mirrors analysis/cache.py)
  report.py       # renders the self-contained HTML report
  trades.py       # CLI entry point / orchestration
  requirements.txt
  .gitignore
```

## Data model

`models.py` defines one shared dataclass used by all three sources:

```python
@dataclass
class Trade:
    source: str               # "house" | "senate" | "insider"
    person: str                # member name or insider name
    role: str                   # e.g. "Rep. (R-TX)" or "Chief Technology Officer"
    ticker: str
    company: str
    transaction_type: str       # "buy" | "sell" | "other"
    trade_date: date
    filed_date: date
    amount_low: float | None    # house/senate: band low
    amount_high: float | None   # house/senate: band high
    shares: float | None        # insider only
    price: float | None         # insider only
    link: str                   # URL to original filing
```

`house.py`, `senate.py`, and `insiders.py` each expose a
`fetch_trades(days: int) -> list[Trade]` normalizing their respective
raw responses into this shape.

## Caching

`cache.py` mirrors `analysis/cache.py`'s approach: a simple JSON file
cache keyed by source + date, so repeated runs within the same day
don't re-hit the APIs. `--no-cache` forces a re-fetch, matching
`screener.py`'s existing flag.

## Report

`report.py` renders one self-contained HTML file (no server, no
external JS dependencies fetched at runtime) containing a single table
merging all three sources, sorted by trade date descending by default.
Columns: date, person, role, source, ticker, company, buy/sell, amount.
Client-side JS filters: free-text search (person/ticker/company),
source toggle (house/senate/insider/all), buy/sell toggle. Same
self-contained-file convention as the existing `screener.py` report
output.

## CLI

`trades.py`, argparse-based, matching `screener.py`'s style:

```
--days N              Lookback window in days (default: 30)
--sources LIST         Comma-separated subset of house,senate,insider (default: all)
--no-cache             Force re-fetch all data
--output-dir PATH      Directory for HTML output (default: current directory)
--quiet                Suppress progress output
```

Exit codes follow `screener.py`'s convention: 0 = success with results,
1 = no trades matched, 2 = fatal (every source failed).

## Error handling

If a source fails (network error, site maintenance, schema change,
PDF that won't parse), the other sources still render and the report
includes a visible notice naming which source is missing/stale, rather
than failing the whole run. A single unparseable filing (e.g. one PTR
PDF with a layout the regex can't handle) is skipped with a logged
warning, not treated as a fatal error for that whole source. HTTP
calls use basic retry with backoff (matching the pattern already used
for yfinance fetches in `analysis/data.py`), and every request sets
the descriptive `User-Agent` required by SEC/House/Senate.

## Testing

Unit tests for the normalization functions in `house.py`, `senate.py`,
and `insiders.py`, each using a real captured fixture (a sample PTR
PDF, a hand-built PTR HTML table matching the confirmed eFD schema,
and a real Form 4 `ownershipDocument` XML) rather than invented data,
plus a smoke test that `report.py` produces valid HTML from a small
fixture list of `Trade` objects. No live-network tests in the default
test run.

## Out of scope (for this pass)

- Historical trend tracking across multiple runs (would need SQLite
  instead of JSON cache — noted as a natural future upgrade, not built
  now).
- Watchlist-specific filtering (this pass shows all recent trades,
  filterable client-side, not scoped to a ticker list).
- Paid aggregator APIs (Quiver Quant, Unusual Whales, etc.) — free
  sources only for this pass.
