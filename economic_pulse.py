#!/usr/bin/env python3
"""
Economic Pulse - Real-time indicator dashboard
Data sources: FRED (Federal Reserve) + Yahoo Finance
No financial advice — just data.
"""

import sys
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("Run: pip3 install requests yfinance")
    sys.exit(1)

try:
    import yfinance as yf
except ImportError:
    yf = None

# ── FRED API (free, no key needed for these endpoints via FRED's public data)
# We'll use the FRED API with a free key, OR fall back to their public JSON feeds.
# Sign up free at https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = "7308fcf67e4235271ea0a6910e2f4b05"

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

INDICATORS = {
    # Macro
    "UNRATE":    ("Unemployment Rate",          "%",    "lower = healthier"),
    "CPIAUCSL":  ("CPI (Inflation YoY proxy)",  "idx",  "rising = inflation pressure"),
    "FEDFUNDS":  ("Fed Funds Rate",             "%",    "higher = tighter money"),
    "GDP":       ("Real GDP Growth",            "B$",   "quarterly"),
    # Yield curve (recession predictor)
    "T10Y2Y":    ("10Y-2Y Yield Spread",        "%pts", "negative = inverted = recession signal"),
    "T10YFF":    ("10Y Treasury - Fed Funds",   "%pts", "context for rate pressure"),
    # Consumer & credit
    "UMCSENT":   ("Consumer Sentiment (U of M)","idx",  "below 80 = worry zone"),
    "DRCCLACBS": ("Credit Card Delinquency",    "%",    "rising = consumer stress"),
    # Labor
    "PAYEMS":    ("Nonfarm Payrolls",           "K",    "monthly change"),
    "JTSJOL":    ("Job Openings (JOLTS)",       "K",    "falling = cooling labor"),
}

CRASH_THRESHOLDS = {
    "UNRATE":    (">=", 5.5,  "Unemployment above 5.5% — historically recessionary"),
    "T10Y2Y":    ("<=", 0.0,  "Yield curve inverted — strong recession predictor"),
    "UMCSENT":   ("<=", 70,   "Consumer sentiment collapse territory"),
    "DRCCLACBS": (">=", 3.0,  "Credit card delinquency elevated"),
    "FEDFUNDS":  (">=", 5.0,  "Rates at restrictive levels"),
}

# ── News RSS feeds (no key needed) ───────────────────────────
NEWS_FEEDS = [
    ("BBC",         "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("BBC",         "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("CNBC",        "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC",        "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ("FT",          "https://www.ft.com/rss/home/us"),
    ("NYT",         "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ("NYT",         "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml"),
]

# Keywords that signal market-moving events — grouped by category
NEWS_KEYWORDS = {
    "war/conflict":   ["iran", "war", "military", "strike", "missile", "sanctions", "conflict",
                       "troops", "attack", "nuclear", "airstrike", "invasion"],
    "trade/tariffs":  ["tariff", "trade war", "import duty", "export ban", "trade deal",
                       "china trade", "embargo"],
    "fed/rates":      ["federal reserve", "fed rate", "interest rate", "powell", "rate hike",
                       "rate cut", "inflation", "cpi", "fomc"],
    "recession":      ["recession", "gdp", "layoffs", "unemployment", "downturn", "contraction",
                       "bear market", "crash", "default"],
    "energy/oil":     ["oil price", "crude", "opec", "energy crisis", "gas price", "brent", "wti"],
    "banks/finance":  ["bank failure", "credit crisis", "debt ceiling", "treasury", "yield curve",
                       "liquidity", "svb", "lehman"],
}

MARKET_TICKERS = {
    "^GSPC":  "S&P 500",
    "^VIX":   "VIX (Fear Index)",
    "^TNX":   "10Y Treasury Yield",
    "GLD":    "Gold ETF",
    "^DXY":   "US Dollar Index",
}


def fred_get(series_id, limit=2):
    """Fetch latest FRED observations."""
    if FRED_API_KEY == "your_fred_api_key_here":
        return None  # Demo mode

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }
    try:
        r = requests.get(FRED_BASE, params=params, timeout=8)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        # Filter out missing values
        valid = [o for o in obs if o["value"] != "."]
        return valid[0] if valid else None
    except Exception as e:
        return None


def get_market_data():
    """Fetch market data via yfinance."""
    if yf is None:
        return {}
    results = {}
    for ticker, name in MARKET_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if hist.empty:
                continue
            latest = hist["Close"].iloc[-1]
            prev = hist["Close"].iloc[-2] if len(hist) > 1 else latest
            pct_chg = ((latest - prev) / prev) * 100
            results[ticker] = {
                "name": name,
                "value": latest,
                "change_pct": pct_chg,
            }
        except Exception:
            pass
    return results


def get_news():
    """Fetch and filter market-moving headlines from RSS feeds."""
    seen_titles = set()
    hits = []

    for source, url in NEWS_FEEDS:
        try:
            r = requests.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            root = ET.fromstring(r.content)
            # Handle both RSS and Atom
            items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
            for item in items[:20]:
                def _find(el, *tags):
                    for tag in tags:
                        found = el.find(tag)
                        if found is not None:
                            return found
                    return None
                title_el = _find(item, "title", "{http://www.w3.org/2005/Atom}title")
                link_el  = _find(item, "link",  "{http://www.w3.org/2005/Atom}link")
                desc_el  = _find(item, "description", "{http://www.w3.org/2005/Atom}summary")
                pub_el   = _find(item, "pubDate", "{http://www.w3.org/2005/Atom}published")

                if title_el is None:
                    continue
                title = (title_el.text or "").strip()
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)

                desc  = (desc_el.text  or "") if desc_el is not None else ""
                text  = (title + " " + desc).lower()

                matched_cats = []
                for cat, keywords in NEWS_KEYWORDS.items():
                    if any(kw in text for kw in keywords):
                        matched_cats.append(cat)

                if not matched_cats:
                    continue

                pub = ""
                if pub_el is not None and pub_el.text:
                    try:
                        # Try common RSS date formats
                        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                                    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
                            try:
                                pub = datetime.strptime(pub_el.text.strip(), fmt).strftime("%b %d %H:%M")
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass

                hits.append({
                    "source":     source,
                    "title":      title,
                    "categories": matched_cats,
                    "pub":        pub,
                })
        except Exception:
            continue

    # Sort: war/conflict first, then by source
    priority = ["war/conflict", "trade/tariffs", "recession", "energy/oil", "fed/rates", "banks/finance"]
    hits.sort(key=lambda h: min((priority.index(c) if c in priority else 99) for c in h["categories"]))
    return hits


def score_crash_risk(readings):
    """Simple signal counter — not a forecast."""
    warnings = []
    for series_id, (op, threshold, msg) in CRASH_THRESHOLDS.items():
        if series_id not in readings:
            continue
        val = readings[series_id]
        triggered = False
        if op == ">=" and val >= threshold:
            triggered = True
        elif op == "<=" and val <= threshold:
            triggered = True
        if triggered:
            warnings.append(f"  ⚠  {msg} (current: {val})")
    return warnings


def color(text, code):
    return f"\033[{code}m{text}\033[0m"

def red(t):    return color(t, "91")
def green(t):  return color(t, "92")
def yellow(t): return color(t, "93")
def cyan(t):   return color(t, "96")
def bold(t):   return color(t, "1")


def main():
    print()
    print(bold("=" * 60))
    print(bold("  ECONOMIC PULSE  —  " + datetime.now().strftime("%Y-%m-%d %H:%M")))
    print(bold("=" * 60))
    print(color("  Data: Federal Reserve (FRED) + Yahoo Finance", "90"))
    print(color("  NOT financial advice. Verify before acting.", "90"))
    print()

    # ── Market Data (works without API key) ──────────────────────
    print(bold(cyan("MARKETS (live)")))
    print("-" * 40)
    market = get_market_data()
    if market:
        for ticker, d in market.items():
            chg = d["change_pct"]
            chg_str = f"{chg:+.2f}%"
            chg_colored = green(chg_str) if chg >= 0 else red(chg_str)
            val = d["value"]

            # Special formatting
            if ticker == "^VIX":
                vix_note = red(" ← FEAR") if val > 25 else (yellow(" ← elevated") if val > 18 else green(" ← calm"))
                print(f"  {d['name']:25s}  {val:>8.2f}  {chg_colored}{vix_note}")
            elif ticker in ("^GSPC",):
                print(f"  {d['name']:25s}  {val:>8.0f}  {chg_colored}")
            else:
                print(f"  {d['name']:25s}  {val:>8.2f}  {chg_colored}")
    else:
        print(red("  yfinance not available — run: pip3 install yfinance"))
    print()

    # ── FRED Macro Indicators ─────────────────────────────────────
    print(bold(cyan("MACRO INDICATORS (FRED)")))
    print("-" * 40)

    if FRED_API_KEY == "your_fred_api_key_here":
        print(yellow("  [Demo mode] Get a free API key at fred.stlouisfed.org"))
        print(yellow("  Then set FRED_API_KEY in this script."))
        print()
        print("  Key indicators to watch manually:")
        for sid, (name, unit, note) in INDICATORS.items():
            print(f"  {name:35s} ({unit})  — {note}")
        readings = {}
    else:
        readings = {}
        for series_id, (name, unit, note) in INDICATORS.items():
            obs = fred_get(series_id)
            if obs:
                try:
                    val = float(obs["value"])
                    readings[series_id] = val
                    date = obs["date"]
                    # Flag worrying values
                    flag = ""
                    if series_id in CRASH_THRESHOLDS:
                        op, threshold, _ = CRASH_THRESHOLDS[series_id]
                        if (op == ">=" and val >= threshold) or (op == "<=" and val <= threshold):
                            flag = red(" ◄ WARNING")
                    print(f"  {name:35s}  {val:>8.2f} {unit:<6}  ({date}){flag}")
                except ValueError:
                    print(f"  {name:35s}  n/a")
            else:
                print(f"  {name:35s}  [no data]")
        print()

    # ── Crash Risk Score ──────────────────────────────────────────
    if readings:
        print(bold(cyan("RECESSION / CRASH SIGNALS")))
        print("-" * 40)
        warnings = score_crash_risk(readings)
        total = len(CRASH_THRESHOLDS)
        fired = len(warnings)

        if fired == 0:
            print(green(f"  0/{total} warning signals active — no obvious immediate crash signals"))
        elif fired <= 2:
            print(yellow(f"  {fired}/{total} warning signals active — elevated caution warranted"))
        else:
            print(red(f"  {fired}/{total} warning signals active — significant stress indicators"))

        for w in warnings:
            print(red(w))
        print()

    # ── News ──────────────────────────────────────────────────────
    print(bold(cyan("MARKET-MOVING NEWS (live)")))
    print("-" * 40)
    CAT_COLORS = {
        "war/conflict":  "91",   # red
        "trade/tariffs": "93",   # yellow
        "recession":     "91",   # red
        "energy/oil":    "93",   # yellow
        "fed/rates":     "96",   # cyan
        "banks/finance": "93",   # yellow
    }
    news = get_news()
    if news:
        for item in news[:12]:
            cats = item["categories"]
            cat_str = ", ".join(color(c, CAT_COLORS.get(c, "97")) for c in cats)
            pub = f"  [{item['pub']}]" if item["pub"] else ""
            print(f"  {color(item['source'], '90'):>10}  {item['title']}")
            print(f"             {cat_str}{color(pub, '90')}")
            print()
    else:
        print(yellow("  No market-moving headlines found (feeds may be slow)"))
        print()

    # ── Interpretation Guide ──────────────────────────────────────
    print(bold(cyan("HOW TO READ THIS")))
    print("-" * 40)
    rows = [
        ("VIX > 25",         "Fear/volatility spike — markets pricing in stress"),
        ("VIX > 40",         "Panic territory (COVID hit 85, 2008 hit 80)"),
        ("Yield curve < 0",  "Inverted — preceded every recession since 1970"),
        ("Unemployment +1%", "Fast rise = recession already started"),
        ("CPI still high",   "Fed keeps rates high = growth squeeze"),
        ("S&P down >20%",    "Official bear market"),
    ]
    for signal, meaning in rows:
        print(f"  {signal:25s}  {meaning}")

    print()
    print(color("  Sources: fred.stlouisfed.org  |  finance.yahoo.com", "90"))
    print(bold("=" * 60))
    print()


if __name__ == "__main__":
    main()
