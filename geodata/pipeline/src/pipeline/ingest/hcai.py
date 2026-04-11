"""
CA HCAI Annual Financial Disclosure ingest — Skilled Nursing Facilities.

HCAI (Health Care Access and Information, formerly OSHPD) publishes
annual financial data for CA licensed facilities as Excel workbooks.

The SNF workbook has one row per facility per year with columns including:
  OSHPD_ID (or Facility ID), Facility Name, Gross Patient Revenue,
  Net Patient Revenue, Total Revenue, Total Expenses, Net Income,
  Medicare Revenue, Medi-Cal Revenue.

Join key: OSHPD_ID → facilities.oshpd_id

Download page:
  https://hcai.ca.gov/data-and-reports/research-data/annual-financial-data/

Note: HCAI changed their URL structure; if the default URL fails, update
HCAI_SNF_URL in settings/.env.
"""

from __future__ import annotations

import io
import logging

import httpx
import pandas as pd

from pipeline.config import settings
from pipeline.db import get_session
from pipeline.models import Facility, FacilityFinancial

log = logging.getLogger(__name__)

# Known column aliases across HCAI Excel releases (they rename columns between years)
_COL_ALIASES: dict[str, list[str]] = {
    "oshpd_id":        ["OSHPD_ID", "Oshpd Id", "FACILITY_ID", "Facility ID", "License Number"],
    "gross_revenue":   ["GROSS_PATIENT_REV", "Gross Patient Revenue", "Gross Revenue"],
    "net_revenue":     ["NET_PATIENT_REV", "Net Patient Revenue", "Net Revenue"],
    "total_expenses":  ["TOTAL_EXPENSES", "Total Expenses", "Total Operating Expense"],
    "medicare_revenue":["MEDICARE_REV", "Medicare Revenue", "Medicare Net Revenue"],
    "medicaid_revenue":["MEDI_CAL_REV", "Medi-Cal Revenue", "Medicaid Revenue"],
    "net_income":      ["NET_INCOME", "Net Income", "Total Net Income"],
}


def _resolve_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Return the first alias that is a column in df, or None."""
    for alias in aliases:
        if alias in df.columns:
            return alias
    # Try case-insensitive match
    lower = {c.lower(): c for c in df.columns}
    for alias in aliases:
        if alias.lower() in lower:
            return lower[alias.lower()]
    return None


def _download(url: str) -> bytes:
    log.info("Downloading HCAI Excel from %s", url)
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def _parse_excel(raw: bytes) -> pd.DataFrame:
    """Read the HCAI Excel, find the data sheet, normalize columns."""
    xl = pd.ExcelFile(io.BytesIO(raw), engine="openpyxl")
    # Use the first sheet that has >10 rows (skip cover/notes sheets)
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, dtype=str)
        if len(df) > 10:
            log.info("Using sheet '%s' with %d rows", sheet, len(df))
            df.columns = [str(c).strip() for c in df.columns]
            return df
    raise ValueError(f"No data sheet found in HCAI Excel. Sheets: {xl.sheet_names}")


def _load_oshpd_index() -> dict[str, str]:
    """Return {oshpd_id: facility_id} for all CA facilities with oshpd_id set."""
    with get_session() as session:
        rows = (
            session.query(Facility.oshpd_id, Facility.id)
            .filter(Facility.oshpd_id.isnot(None))
            .all()
        )
    return {r.oshpd_id.strip(): str(r.id) for r in rows}


def run() -> dict[str, int]:
    """Ingest HCAI SNF annual financial data. Returns {upserted, skipped, no_match}."""
    raw = _download(settings.hcai_snf_url)
    df = _parse_excel(raw)

    # Resolve column names
    col = {}
    missing = []
    for field_name, aliases in _COL_ALIASES.items():
        resolved = _resolve_col(df, aliases)
        if resolved:
            col[field_name] = resolved
        else:
            missing.append(field_name)

    if "oshpd_id" not in col:
        raise ValueError(
            f"Could not find OSHPD_ID column. Available columns: {list(df.columns)[:20]}"
        )
    if missing:
        log.warning("Could not resolve columns (will be NULL): %s", missing)

    oshpd_index = _load_oshpd_index()
    log.info("CA facilities with OSHPD ID in DB: %d", len(oshpd_index))

    if not oshpd_index:
        log.warning("No OSHPD IDs loaded — run crosswalk ingest first")
        return {"upserted": 0, "skipped": 0, "no_match": 0}

    year = settings.hcai_year
    source_tag = "hcai"
    upserted = skipped = no_match = 0

    def _dollars(val: object) -> int | None:
        """Parse a dollar string like '$1,234,567' or '1234567' to integer cents."""
        if val is None or str(val).strip() in ("", "nan", "N/A", "--"):
            return None
        cleaned = str(val).replace("$", "").replace(",", "").strip()
        try:
            # HCAI reports in dollars; store as dollars (not cents)
            return int(float(cleaned))
        except (ValueError, OverflowError):
            return None

    with get_session() as session:
        for _, row in df.iterrows():
            raw_id = _resolve_col(df, _COL_ALIASES["oshpd_id"])
            oshpd_id = str(row.get(col["oshpd_id"], "")).strip() if "oshpd_id" in col else None
            if not oshpd_id or oshpd_id in ("", "nan"):
                skipped += 1
                continue

            facility_id = oshpd_index.get(oshpd_id)
            if not facility_id:
                no_match += 1
                continue

            existing = (
                session.query(FacilityFinancial)
                .filter_by(facility_id=facility_id, year=year, source=source_tag)
                .first()
            )
            if existing is None:
                existing = FacilityFinancial(
                    facility_id=facility_id, year=year, source=source_tag
                )
                session.add(existing)

            if "gross_revenue" in col:
                existing.gross_revenue = _dollars(row.get(col["gross_revenue"]))
            if "net_revenue" in col:
                existing.net_revenue = _dollars(row.get(col["net_revenue"]))
            if "total_expenses" in col:
                existing.total_expenses = _dollars(row.get(col["total_expenses"]))
            if "medicare_revenue" in col:
                existing.medicare_revenue = _dollars(row.get(col["medicare_revenue"]))
            if "medicaid_revenue" in col:
                existing.medicaid_revenue = _dollars(row.get(col["medicaid_revenue"]))

            upserted += 1

    log.info("HCAI SNF upserted=%d skipped=%d no_match=%d", upserted, skipped, no_match)
    return {"upserted": upserted, "skipped": skipped, "no_match": no_match}
