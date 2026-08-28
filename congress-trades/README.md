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
`--output-dir PATH`, `--quiet`. See [`../docs/superpowers/specs/2026-08-27-congress-trades-design.md`](../docs/superpowers/specs/2026-08-27-congress-trades-design.md)
for the data-source details and known limitations (disclosure amounts
are bands, not exact dollars; House PDF parsing is best-effort; Senate
parsing was built against the documented schema, not a live capture —
see the note at the top of `senate.py`).
