# Congress & Insider Trading Tracker — Design Spec

**Date**: 2026-08-27
**Status**: Approved for planning

## Purpose

A tool to answer "what have members of Congress and corporate insiders
been buying and selling recently?" Combines two disclosure sources into
one browsable report, following the same self-contained-HTML-report
pattern already used by `analysis/screener.py` and `swing-scanner/`.

## Background: data sources

Congressional trades are **not** filed with the SEC. Members of
Congress report under the STOCK Act to the Clerk of the House / Senate
Office of Public Records, not EDGAR. So this tool pulls from two
independent pipelines:

1. **Congress** — [House Stock Watcher](https://housestockwatcher.com/api)
   and [Senate Stock Watcher](https://senatestockwatcher.com/api): free,
   community-maintained JSON dumps scraped from the official House
   Clerk PTR disclosures and Senate eFD filings. No API key required.
   Amounts are reported in disclosure *bands* (e.g. $1,001–$15,000),
   not exact dollars — a real limitation of the underlying filings, not
   a parsing gap.
2. **Corporate insiders** — SEC EDGAR Form 4 filings, via EDGAR's
   public recent-filings feed. Real insider trading: officers,
   directors, 10%+ owners of public companies. Free, no key required.

## Project layout

New sibling project: `congress-trades/` (alongside `analysis/`,
`swing-scanner/`), following the `analysis/` module split:

```
congress-trades/
  models.py       # shared Trade dataclass
  congress.py     # fetch + normalize House/Senate Stock Watcher data
  insiders.py     # fetch + normalize SEC EDGAR Form 4 filings
  cache.py        # daily JSON cache per source (mirrors analysis/cache.py)
  report.py       # renders the self-contained HTML report
  trades.py       # CLI entry point / orchestration
  requirements.txt
  .gitignore
```

## Data model

`models.py` defines one shared dataclass used by both sources:

```python
@dataclass
class Trade:
    source: str            # "congress" | "insider"
    person: str             # member name or insider name
    role: str                # e.g. "Senator (R-TX)" or "Director"
    ticker: str
    company: str
    transaction_type: str    # "buy" | "sell" | "other"
    trade_date: date
    filed_date: date
    amount_low: float | None   # congress: band low; insider: shares*price low estimate or None
    amount_high: float | None  # congress: band high
    shares: float | None       # insider only
    price: float | None        # insider only
    link: str                  # URL to original filing
```

Both `congress.py` and `insiders.py` expose a `fetch_trades(days: int) -> list[Trade]` normalizing their respective raw API/feed responses into this shape.

## Caching

`cache.py` mirrors `analysis/cache.py`'s approach: a simple JSON file
cache keyed by source + date, so repeated runs within the same day
don't re-hit the APIs. `--no-cache` forces a re-fetch, matching
`screener.py`'s existing flag.

## Report

`report.py` renders one self-contained HTML file (no server, no
external JS dependencies fetched at runtime) containing a single table
merging both sources, sorted by trade date descending by default.
Columns: date, person, role, source, ticker, company, buy/sell, amount.
Client-side JS filters: free-text search (person/ticker/company),
source toggle (congress/insider/both), buy/sell toggle. Same
self-contained-file convention as the existing `screener.py` report
output.

## CLI

`trades.py`, argparse-based, matching `screener.py`'s style:

```
--days N            Lookback window in days (default: 30)
--congress-only      Skip the insider-trading fetch
--insiders-only      Skip the congress fetch
--no-cache            Force re-fetch all data
--output-dir PATH     Directory for HTML output (default: current directory)
--quiet                Suppress progress output
```

Exit codes follow `screener.py`'s convention: 0 = success with results,
1 = no trades matched, 2 = fatal (neither source could be fetched).

## Error handling

If one source fails (network error, API schema change, etc.), the
other source still renders and the report includes a visible notice
that one source's data is missing/stale, rather than failing the whole
run. HTTP calls use basic retry with backoff (matching the pattern
already used for yfinance fetches in `analysis/data.py`).

## Testing

Unit tests for the normalization functions in `congress.py` and
`insiders.py` (raw API/feed sample → expected `Trade` objects), plus a
smoke test that `report.py` produces valid HTML from a small fixture
list of `Trade` objects. No live-network tests in the default test run.

## Out of scope (for this pass)

- Historical trend tracking across multiple runs (would need SQLite
  instead of JSON cache — noted as a natural future upgrade, not built
  now).
- Watchlist-specific filtering (this pass shows all recent trades,
  filterable client-side, not scoped to a ticker list).
- Paid aggregator APIs (Quiver Quant, Unusual Whales, etc.) — free
  sources only for this pass.
