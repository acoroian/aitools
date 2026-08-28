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
