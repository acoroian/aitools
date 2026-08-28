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
